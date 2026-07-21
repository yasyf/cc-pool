package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// setupSync constructs the host-sync engine and wires it onto the daemon.
// Always constructed — every acting path re-reads the sync_enabled meta, so
// enable needs no daemon restart — and it must run before serve admits any
// handler: handlers read s.syncPull / s.authKind unlocked.
func (s *Server) setupSync(ctx context.Context) error {
	if s.syncSocket == "" {
		return fmt.Errorf("no sync socket path configured")
	}
	self, err := s.resolveSyncSelf(ctx)
	if err != nil {
		return err
	}
	if s.disposableWorkers == nil {
		return errors.New("sync requires daemon disposable workers")
	}
	workerExecutable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve credential settlement worker executable: %w", err)
	}

	svc := &hostsync.Service{
		Registry: hostsync.NewRegistryFile(pool.SyncDir()),
		StampDir: pool.SyncStampsDir(),
		Log:      s.log,
	}
	settler := newCredentialWriteSettler(
		s.disposableWorkers,
		workerExecutable,
		s.syncEnabled,
		*svc.Registry,
		svc.StampDir,
		self,
	)
	previousBuilder := s.m.BuildCredentialWritePublication
	previousSettler := s.m.SettleCredentialWrite
	wired := false
	defer func() {
		if wired {
			return
		}
		s.m.BuildCredentialWritePublication = previousBuilder
		s.m.SettleCredentialWrite = previousSettler
	}()
	s.m.BuildCredentialWritePublication = credentialWritePublicationBuilder(self)
	s.m.SettleCredentialWrite = settler.Settle
	if err := s.m.SettlePendingCredentialWrites(ctx); err != nil {
		return fmt.Errorf("settle pending credential writes: %w", err)
	}

	worker, err := hostsync.NewWorkerClient(s.disposableWorkers, workerExecutable)
	if err != nil {
		return err
	}
	if err := s.startSyncServer(ctx, worker, worker.FetchCredentialHandler); err != nil {
		return err
	}
	s.syncSvc = svc
	s.syncSelf = self
	s.syncAuthKind = worker.AuthKind
	s.syncPull = func(ctx context.Context) error {
		_, err := worker.Reconcile(ctx, "")
		if errors.Is(err, hostsync.ErrSyncDisabled) {
			return nil
		}
		return err
	}
	wired = true
	return nil
}

// authKind classifies a needs-login at persist time in the disposable worker.
// An absent worker or an unprovable registry owner is an error, never Owned.
func (s *Server) authKind(ctx context.Context, a store.Account) (store.AuthKind, error) {
	if a.AccountUUID == "" {
		return store.AuthKindOwned, nil
	}
	if s.syncAuthKind == nil {
		return "", errors.New("auth-kind worker is unavailable")
	}
	return s.syncAuthKind(ctx, a.ID, a.AccountUUID)
}

// syncEnabledBool adapts syncEnabled for acting paths; a meta read
// failure reads disabled — sync must never take down single-host pooling.
func (s *Server) syncEnabledBool() bool {
	on, err := s.syncEnabled()
	if err != nil {
		s.log.Printf("read %s meta: %v", metaSyncEnabled, err)
		return false
	}
	return on
}

// resolveSyncSelf names this host in the registry: the mesh ssh target when
// joined — peers dial the chain holder by this name — else os.Hostname().
func (s *Server) resolveSyncSelf(ctx context.Context) (string, error) {
	self, _, merr := (hostsync.SynckitMesh{}).Resolve(ctx)
	if merr == nil {
		return self, nil
	}
	if s.syncEnabledBool() {
		s.log.Printf("sync self falls back to the hostname: %v", merr)
	}
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve sync self: %w", err)
	}
	if host == "" {
		return "", fmt.Errorf("resolve sync self: kernel hostname is empty")
	}
	return host, nil
}
