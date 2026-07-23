package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

// Refresher is the slice of *oauth.Client the Manager needs.
type Refresher interface {
	Refresh(ctx context.Context, flightKey, refreshToken string) (*oauth.TokenResponse, error)
	Usage(ctx context.Context, accessToken string) (*oauth.Usage, error)
}

func (m *Manager) credentialOwnerRecord() (proc.Record, error) {
	if m.workerAuthority == nil {
		return proc.Record{}, errors.New("credential mutation requires exact worker authority")
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		return proc.Record{}, err
	}
	if err := validateCurrentWorkerOwner(m.workerAuthority.owner, identity); err != nil {
		return proc.Record{}, err
	}
	if m.workerAuthority.owner.RecoveryClass != proc.RecoveryTask ||
		m.workerAuthority.owner.ProcessGroup {
		return proc.Record{}, errors.New("credential worker authority kind changed")
	}
	return m.workerAuthority.owner, nil
}

// MutationOwner returns the singleton daemon's durable credential authority.
func (m *Manager) MutationOwner() (proc.Record, error) {
	if m.workers == nil && m.workerAuthority == nil {
		return proc.Record{}, errors.New("credential mutation requires daemon worker ownership")
	}
	return m.credentialOwnerRecord()
}

// Credentials resolves an account's candidate credential stores; injectable
// for tests.
type Credentials interface {
	// Store returns a's Keychain credential store.
	Store(a store.Account, src creds.Source) creds.Store
	// Stores returns a's sole Keychain credential store.
	Stores(a store.Account) []creds.Store
	// Discover resolves the account (-a) label actually stored on a service's
	// Keychain item, or creds.ErrNotFound: `claude /login` items carry whatever
	// label claude derived then, which a later recompute may not match, so
	// deleting or adopting a claude-written item must Discover first.
	Discover(context.Context, string) (string, error)
}

// sysCredentials resolves the account's sole Keychain credential item.
type sysCredentials struct {
	runner creds.TaskRunner
}

func (c sysCredentials) Store(a store.Account, src creds.Source) creds.Store {
	if src != creds.SourceKeychain {
		panic(fmt.Sprintf("unknown credential source %d", src))
	}
	return creds.KeychainItem{
		Service: a.KeychainService, Account: a.KeychainAccount, Runner: c.runner,
	}
}

func (c sysCredentials) Stores(a store.Account) []creds.Store {
	return []creds.Store{c.Store(a, creds.SourceKeychain)}
}

func (c sysCredentials) Discover(ctx context.Context, service string) (string, error) {
	return creds.DiscoverAccount(ctx, c.runner, service)
}

// Manager is the high-level façade over the store, the OAuth client, and the
// Keychain/overlay machinery.
type Manager struct {
	Store *store.Store
	OAuth Refresher
	Creds Credentials
	// ScanSessions is the process-inspection boundary. The daemon installs a
	// killable worker scanner; tests may inject a deterministic implementation.
	ScanSessions func(context.Context) ([]procscan.Session, error)
	// ScanProcesses returns Claude sessions and every process identity from one
	// killable, atomic process-table observation.
	ScanProcesses func(context.Context) (procscan.Snapshot, error)

	// SettleCredentialWrite durably publishes one terminal credential write.
	// Implementations must be exact-idempotent by OperationID and worker-backed.
	SettleCredentialWrite func(context.Context, CredentialWriteSettlement) error
	// BuildCredentialWritePublication captures immutable, non-secret publication
	// bytes before the terminal receipt commits. It must perform no I/O.
	BuildCredentialWritePublication CredentialWritePublicationBuilder
	// ClaimCredentialMutation serializes credential writes with daemon selection
	// reservations. The returned release must be called after the durable lane settles.
	ClaimCredentialMutation func(accountID int) (release func(), err error)

	credentialMu      sync.Mutex
	credentialFlights map[int]*credentialFlight

	workers          *workerRuntime
	workerAuthority  *WorkerAuthority
	taskRunner       supervise.TaskRunner
	workerExecutable string
	credentialCAS    credentialCASFunc
	recoveryMu       sync.Mutex
	recoveryCancel   context.CancelFunc
	recoveryDone     chan struct{}
}

// OpenDaemon ensures state exists and creates the singleton daemon's durable worker runtime.
func OpenDaemon(ctx context.Context) (*Manager, error) {
	if err := EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("ensure state dir: %w", err)
	}
	st, err := store.Open(DBPath())
	if err != nil {
		return nil, err
	}
	workers, scanner, err := newWorkerRuntime(ctx)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	authority, err := NewWorkerAuthority(workers.pool, workers.executable, workers.owner)
	if err != nil {
		_ = workers.close(ctx)
		_ = st.Close()
		return nil, err
	}
	manager, err := NewManager(st, oauth.New(), scanner.Scan, authority)
	if err != nil {
		_ = workers.close(ctx)
		_ = st.Close()
		return nil, err
	}
	manager.ScanProcesses = scanner.Snapshot
	manager.workers = workers
	return manager, nil
}

// OpenLocal opens local derived/state inspection without credential stores,
// process recovery, or disposable workers. Credential mutation is daemon-only.
func OpenLocal() (*Manager, error) {
	if err := EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("ensure state dir: %w", err)
	}
	st, err := store.Open(DBPath())
	if err != nil {
		return nil, err
	}
	return &Manager{Store: st}, nil
}

func (m *Manager) scanSessions(ctx context.Context) ([]procscan.Session, error) {
	if m.ScanSessions == nil {
		return nil, errors.New("session scan requires worker authority")
	}
	return m.ScanSessions(ctx)
}

// Close releases resources within a cleanup context derived from ctx.
func (m *Manager) Close(ctx context.Context) error {
	var result error
	m.stopCredentialOwnerRecovery()
	if m.workers != nil {
		result = m.workers.close(ctx)
	}
	if m.Store != nil {
		result = errors.Join(result, m.Store.Close())
	}
	return result
}

// DisposableWorkers returns the manager-owned daemonkit worker pool.
func (m *Manager) DisposableWorkers() *supervise.Pool {
	if m.workers == nil {
		return nil
	}
	return m.workers.pool
}

// Meta keys recording pool-level state in the store's meta table.
const (
	// metaInitialized marks that the pool was set up via `ccp init` (or add's
	// auto-init) — distinct from "the DB file exists", which any read-only command
	// creates just by opening the Manager.
	metaInitialized = "initialized"
)

// Initialized reports whether the pool has been set up (`ccp init` or `ccp
// add`'s auto-init).
func (m *Manager) Initialized() (bool, error) {
	_, ok, err := m.Store.GetMeta(metaInitialized)
	if err != nil {
		return false, err
	}
	return ok, nil
}
