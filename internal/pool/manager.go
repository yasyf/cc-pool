package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit"
)

// Refresher is the slice of *oauth.Client the Manager needs.
type Refresher interface {
	Refresh(ctx context.Context, flightKey, refreshToken string) (*oauth.TokenResponse, error)
	Usage(ctx context.Context, accessToken string) (*oauth.Usage, error)
}

// MutationOwner returns the singleton daemon generation's credential authority.
func (m *Manager) MutationOwner() (store.OwnerRecord, error) {
	if err := m.owner.Validate(); err != nil {
		return nil, errors.New("credential mutation requires daemon worker ownership")
	}
	return m.owner, nil
}

// Credentials resolves an account's candidate credential stores; injectable
// for tests.
type Credentials interface {
	// Store returns a's Keychain credential store.
	Store(a store.Account, src creds.Source) creds.Store
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
	// reservations. A caller-owned reservation may replace this through context;
	// the returned release must be called after the durable lane settles.
	ClaimCredentialMutation CredentialMutationClaim
	// RetirePendingAdd obtains exact tenant-generation and File Provider absence
	// proof before dead-owner pending state is cleaned up.
	RetirePendingAdd func(
		context.Context,
		store.PendingAccountReservation,
	) (PendingAddRetirementProof, error)

	credentialMu      sync.Mutex
	credentialFlights map[int]*credentialFlight

	owner            store.OwnerRecord
	workers          *workerRuntime
	workerAuthority  *WorkerAuthority
	taskRunner       workerexec.Runner
	workerExecutable string
	credentialCAS    credentialCASFunc
}

// CredentialMutationClaim acquires one account's credential-write exclusion.
type CredentialMutationClaim func(accountID int) (release func(), err error)

type credentialMutationClaimContextKey struct{}

// WithCredentialMutationClaim binds one exact caller-owned claim to credential work.
func WithCredentialMutationClaim(ctx context.Context, claim CredentialMutationClaim) context.Context {
	return context.WithValue(ctx, credentialMutationClaimContextKey{}, claim)
}

func credentialMutationClaim(ctx context.Context, fallback CredentialMutationClaim) CredentialMutationClaim {
	if claim, ok := ctx.Value(credentialMutationClaimContextKey{}).(CredentialMutationClaim); ok && claim != nil {
		return claim
	}
	return fallback
}

// OpenDaemon ensures state exists and binds the singleton daemon's worker
// runtime to the process scope Serve handed Start.
func OpenDaemon(scope daemonkit.Ctx) (*Manager, error) {
	if err := EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("ensure state dir: %w", err)
	}
	st, err := store.Open(DBPath())
	if err != nil {
		return nil, err
	}
	workers, err := newWorkerRuntime(scope)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	scanner, err := procscan.NewWorkerScanner(workers, workers.executable)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	owner, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	authority, err := NewWorkerAuthority(workers, workers.executable, owner)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	manager, err := NewManager(st, oauth.New(), scanner.Scan, authority)
	if err != nil {
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

// Close releases the store. Disposable workers run in the process scope Serve
// owns, so their settlement is the daemon's drain, not the manager's.
func (m *Manager) Close() error {
	if m.Store == nil {
		return nil
	}
	return m.Store.Close()
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
