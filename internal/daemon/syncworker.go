package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
)

type hostSyncWorkerSessions struct {
	manager *pool.Manager
}

func (s hostSyncWorkerSessions) Busy(ctx context.Context, uuid string) (bool, string, error) {
	rows, err := s.manager.Store.AccountsByUUID(uuid)
	if err != nil {
		return false, "", fmt.Errorf("resolve accounts for %s: %w", uuid, err)
	}
	if len(rows) == 0 {
		return false, "", nil
	}
	sessions, err := s.manager.ScanSessions(ctx)
	if err != nil {
		return false, "", fmt.Errorf("scan sessions: %w", err)
	}
	for _, account := range rows {
		active, err := s.manager.Store.ActiveSessionCount(account.ID)
		if err != nil {
			return false, "", fmt.Errorf("read acct-%02d active sessions: %w", account.ID, err)
		}
		if active > 0 {
			return true, fmt.Sprintf("acct-%02d has %d active session(s)", account.ID, active), nil
		}
		if count := procscan.CountByConfigDir(sessions, account.ConfigDir); count > 0 {
			return true, fmt.Sprintf("acct-%02d has %d live process session(s)", account.ID, count), nil
		}
	}
	return false, "", nil
}

type hostSyncWorkerRemover struct {
	lifecycle context.Context
	manager   *pool.Manager

	mu          sync.Mutex
	client      *tenantfs.ControlClient
	coordinator *tenantCoordinator
	cancel      context.CancelCauseFunc
}

type hostSyncWorkerRemoval struct {
	remover *hostSyncWorkerRemover
	intent  store.AccountRemoval
}

func (r *hostSyncWorkerRemover) BeginAccountRemoval(id int, deleteCredential bool) (hostsync.AccountRemoval, error) {
	intent, err := r.manager.Store.BeginAccountRemoval(id, deleteCredential)
	if err != nil {
		return nil, err
	}
	return hostSyncWorkerRemoval{remover: r, intent: intent}, nil
}

func (r hostSyncWorkerRemoval) Finish(ctx context.Context) error {
	coordinator, err := r.remover.runtime(ctx)
	if err != nil {
		return err
	}
	return coordinator.finishRemoval(ctx, r.intent)
}

func (r *hostSyncWorkerRemover) PrepareReservedAccount(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	label string,
) (store.FileProviderPresentationIdentity, error) {
	coordinator, err := r.runtime(ctx)
	if err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	configDir, err := pool.AccountConfigDir(reservation.InstanceID)
	if err != nil {
		return store.FileProviderPresentationIdentity{}, fmt.Errorf("derive stable account config dir: %w", err)
	}
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID,
		Generation: reservation.Generation, ConfigDir: configDir, Label: label,
	}
	tenantAccount := pool.TenantAccount(account)
	if err := coordinator.ensureTenant(ctx, account, tenantAccount); err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	identity, err := expectedPresentationIdentity(account)
	if err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	if err := store.ValidateReservedPresentationIdentity(reservation, identity); err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	return identity, nil
}

func (r *hostSyncWorkerRemover) runtime(ctx context.Context) (*tenantCoordinator, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.coordinator != nil {
		return r.coordinator, nil
	}
	client, err := tenantfs.NewControlClient(ctx, pool.FuseKitSocketPath())
	if err != nil {
		return nil, fmt.Errorf("connect FuseKit runtime: %w", err)
	}
	server := &Server{m: r.manager}
	lifecycle, cancel := contextWithoutCancelUntil(ctx, r.lifecycle.Done())
	r.client = client
	r.cancel = cancel
	r.coordinator = newTenantCoordinator(lifecycle, server, nil, client)
	return r.coordinator, nil
}

func (r *hostSyncWorkerRemover) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel(context.Canceled)
		r.cancel = nil
	}
	if r.client == nil {
		return nil
	}
	err := r.client.Close()
	r.client = nil
	r.coordinator = nil
	return err
}

// RunHostSyncWorker reconstructs and executes one complete host-sync operation
// inside one daemonkit-owned disposable command.
func RunHostSyncWorker(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
) (err error) {
	manager, err := pool.OpenHostSyncWorker(ctx)
	if err != nil {
		return err
	}
	remover := &hostSyncWorkerRemover{lifecycle: ctx, manager: manager}
	defer func(ctx context.Context) {
		err = errors.Join(err, remover.Close(), manager.Close(ctx))
	}(ctx)

	manifestPath, err := hostsync.ManifestPath()
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, "[cc-pool-hostsync] ", log.LstdFlags)
	return hostsync.RunWorker(ctx, input, output, func(
		scopeCtx context.Context,
		synckitdExecutable string,
		run func(hostsync.WorkerRuntime) error,
	) error {
		runtime, err := newHostSyncWorkerRuntime(
			scopeCtx, manager, remover, manifestPath, synckitdExecutable, logger,
		)
		if err != nil {
			return err
		}
		return run(runtime)
	})
}

func newHostSyncWorkerRuntime(
	ctx context.Context,
	manager *pool.Manager,
	remover *hostSyncWorkerRemover,
	manifestPath string,
	synckitdExecutable string,
	logger *log.Logger,
) (hostsync.WorkerRuntime, error) {
	self, err := (&Server{m: manager, log: logger}).resolveSyncSelf(ctx)
	if err != nil {
		return hostsync.WorkerRuntime{}, fmt.Errorf("resolve host-sync worker identity: %w", err)
	}
	manager.BuildCredentialWritePublication = credentialWritePublicationBuilder(self)
	manager.SettleCredentialWrite = func(
		_ context.Context,
		settlement pool.CredentialWriteSettlement,
	) error {
		publication, err := decodeCredentialWritePublication(settlement.PublicationPayload)
		if err != nil {
			return err
		}
		if publication.Chain != nil {
			return errors.New("host-sync worker cannot publish an owned credential chain")
		}
		return nil
	}
	service := &hostsync.Service{
		M:        manager,
		Registry: hostsync.NewRegistryFile(pool.SyncDir()),
		StampDir: pool.SyncStampsDir(),
		Log:      logger,
		Locals:   hostsync.ManagerLocals(manager, self, time.Now),
		Mesh:     hostsync.SynckitMesh{},
		Sessions: hostSyncWorkerSessions{manager: manager},
		Remover:  remover,
		Preparer: remover,
		Run: func(ctx context.Context, _ string, args ...string) error {
			return manager.RunHostSyncCommand(ctx, synckitdExecutable, args...)
		},
	}
	service.CredentialSnapshot = func(ctx context.Context, registry hostsync.Registry) (map[string]hostsync.CredentialEnvelope, error) {
		return hostsync.BuildCredentialSnapshot(
			ctx, registry, self, manager.Store.GetAccountByUUID,
			func(ctx context.Context, account store.Account) (*creds.Credential, error) {
				credential, _, err := manager.ReadCredential(ctx, account)
				return credential, err
			},
		)
	}
	service.Driver = hostsync.NewDriver(service, hostsync.DriverDeps{
		Store: manager.Store,
		Cred:  manager,
		Materialize: func(ctx context.Context, value hostsync.AccountValue, credential *creds.Credential) (hostsync.MaterializeResult, error) {
			return service.Materialize(ctx, value, credential, manifestPath)
		},
		Admit:   service.AdmitSyncedAccount,
		Resolve: hostsync.ResolveAppliedCredential,
	})
	enabled := func() (bool, error) {
		value, ok, err := manager.Store.GetMeta(metaSyncEnabled)
		if err != nil {
			return false, fmt.Errorf("read %s meta: %w", metaSyncEnabled, err)
		}
		return ok && value == "1", nil
	}
	consumer := hostsync.NewConsumer(service, enabled)
	return hostsync.WorkerRuntime{
		Consumer: consumer,
		AuthKind: func(ctx context.Context, accountID int, uuid string) (store.AuthKind, error) {
			value, ok, err := manager.Store.GetMeta(metaSyncEnabled)
			if err != nil {
				return "", err
			}
			if !ok || value != "1" {
				return store.AuthKindOwned, nil
			}
			account, err := manager.Store.GetAccount(accountID)
			if err != nil {
				return "", err
			}
			if account.AccountUUID != uuid {
				return "", errors.New("hostsync: auth-kind account identity changed")
			}
			registry, err := service.Registry.Load()
			if err != nil {
				return "", err
			}
			entry, present := registry[uuid]
			if !present || !entry.Present() {
				return store.AuthKindOwned, nil
			}
			meshSelf, peers, err := service.Mesh.Resolve(ctx)
			if err != nil {
				return "", fmt.Errorf("resolve auth-kind mesh: %w", err)
			}
			if meshSelf != self {
				return "", fmt.Errorf("auth-kind mesh identity changed from %q to %q", self, meshSelf)
			}
			return classifyAuthKindOwner(entry.Value.Chain.Origin, meshSelf, peers)
		},
	}, nil
}

func classifyAuthKindOwner(origin, self string, peers []string) (store.AuthKind, error) {
	if origin == "" {
		return "", hostsync.ErrAuthKindOriginMissing
	}
	if origin == self {
		return store.AuthKindOwned, nil
	}
	for _, peer := range peers {
		if origin == peer {
			return store.AuthKindAwaitingOrigin, nil
		}
	}
	return "", fmt.Errorf("%w: %q is outside the exact mesh", hostsync.ErrAuthKindOriginForeign, origin)
}

var (
	_ hostsync.Sessions        = hostSyncWorkerSessions{}
	_ hostsync.AccountRemover  = (*hostSyncWorkerRemover)(nil)
	_ hostsync.AccountRemoval  = hostSyncWorkerRemoval{}
	_ hostsync.AccountPreparer = (*hostSyncWorkerRemover)(nil)
)
