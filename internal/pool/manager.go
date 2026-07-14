package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/proc"
)

// Refresher is the slice of *oauth.Client the Manager needs.
type Refresher interface {
	Refresh(ctx context.Context, flightKey, refreshToken string) (*oauth.TokenResponse, error)
	Usage(ctx context.Context, accessToken string) (*oauth.Usage, error)
}

// Credentials resolves an account's candidate credential stores; injectable
// for tests.
type Credentials interface {
	// Store returns a's store for the backend src names.
	Store(a store.Account, src creds.Source) creds.Store
	// Stores returns a's candidate stores in resolution order: Keychain first
	// (as claude prefers), then the plaintext file fallback.
	Stores(a store.Account) []creds.Store
	// Discover resolves the account (-a) label actually stored on a service's
	// Keychain item, or creds.ErrNotFound: `claude /login` items carry whatever
	// label claude derived then, which a later recompute may not match, so
	// deleting or adopting a claude-written item must Discover first.
	Discover(service string) (string, error)
}

// sysCredentials is the production Credentials: the account's own Keychain
// item and the plaintext .credentials.json inside its config dir.
type sysCredentials struct{}

func (sysCredentials) Store(a store.Account, src creds.Source) creds.Store {
	switch src {
	case creds.SourceKeychain:
		return creds.KeychainItem{Service: a.KeychainService, Account: a.KeychainAccount}
	case creds.SourceFile:
		return creds.FileStore{ConfigDir: a.ConfigDir}
	}
	panic(fmt.Sprintf("unknown credential source %d", src))
}

func (c sysCredentials) Stores(a store.Account) []creds.Store {
	return []creds.Store{c.Store(a, creds.SourceKeychain), c.Store(a, creds.SourceFile)}
}

func (sysCredentials) Discover(service string) (string, error) {
	return creds.DiscoverAccount(service)
}

// Manager is the high-level façade over the store, the OAuth client, and the
// Keychain/overlay machinery.
type Manager struct {
	Store *store.Store
	OAuth Refresher
	Creds Credentials

	// OverlayFor resolves an overlay backend to a fusekit/overlay provider; nil
	// means pool.OverlayProviderFor.
	OverlayFor func(fkoverlay.Backend) (fkoverlay.Provider, error)

	// DetectOverlay resolves the overlay backend for new accounts when none is
	// recorded yet; nil means pool.DetectOverlayBackend.
	DetectOverlay func() (fkoverlay.Backend, string)

	// CanHostFuse reports whether fuse may be recorded as the new-account
	// default; nil means pool.CanHostFuse.
	CanHostFuse func() bool

	// FPProbe classifies an account dir's File Provider domain data-plane verdict
	// through the signed companion app's control op (never a through-domain read),
	// returning nil (healthy) or one of overlay.ErrFPProbe{Missing,Empty,Wedged,
	// NoVerdict}. The daemon injects it so the convert gate rides the same bounded
	// probe as the heal loop; nil derives the probe from the freshly-registered
	// target provider at the convert gate.
	FPProbe func(ctx context.Context, accountDir string) error

	// LockDir holds the per-account cross-process refresh lock files; tests point
	// it at a temp dir so they never touch real state.
	LockDir string

	// OnCredWrite, when non-nil, fires after every successful credential write.
	// Runs under the per-account lock: never block, never take the registry
	// lock — see ccn 10bf17d.
	OnCredWrite func(a store.Account, cred *creds.Credential) error

	// Warnf surfaces a non-fatal warning (an overlay teardown's journal
	// persist-warning) loudly instead of dropping it; the daemon wires it to its
	// logger, the CLI to stderr. Nil discards.
	Warnf func(format string, args ...any)

	// LeaseRoot resolves the fleet session-lease root the destructive-op fence
	// seizes under; nil uses lease.DefaultRoot (~/.fusekit). Tests point it at a
	// temp dir so a Seize/Probe never touches real state.
	LeaseRoot func() (string, error)

	// ContentSource is the merged-content seam wired into the memoized File
	// Provider provider so its enumerator signal is fingerprint-gated (see
	// overlay.FileProviderSpec.Source). The daemon injects the same PoolContentSource
	// its bridge serves; nil (the CLI) leaves the FP signal unconditional.
	ContentSource content.Source

	// fpProvMu guards fpProv, the memoized File Provider provider — memoized so its
	// fingerprint-signal cache (lastSignal) survives across the daemon's polls
	// instead of being rebuilt (and re-signalling) every resolve.
	fpProvMu sync.Mutex
	fpProv   fkoverlay.Provider

	// muMap guards locks (map access only); locks holds one mutex per account ID
	// serializing that account's credential read→refresh→write cycle in-process.
	// That mutex is DELIBERATELY held across Keychain and OAuth I/O — the sanctioned
	// exception to the no-locks-across-I/O rule, since a double-spent single-use
	// refresh token gets invalid_grant. See ccn doc 935d323.
	muMap sync.Mutex
	locks map[int]*sync.Mutex
}

// warnf surfaces a non-fatal warning through the Warnf seam; a nil seam discards.
func (m *Manager) warnf(format string, args ...any) {
	if m.Warnf != nil {
		m.Warnf(format, args...)
	}
}

// leaseRoot resolves the session-lease root: the LeaseRoot seam when set, else
// lease.DefaultRoot.
func (m *Manager) leaseRoot() (string, error) {
	if m.LeaseRoot != nil {
		return m.LeaseRoot()
	}
	return lease.DefaultRoot()
}

func (m *Manager) acctLock(id int) *sync.Mutex {
	m.muMap.Lock()
	defer m.muMap.Unlock()
	if m.locks == nil {
		m.locks = map[int]*sync.Mutex{}
	}
	if m.locks[id] == nil {
		m.locks[id] = &sync.Mutex{}
	}
	return m.locks[id]
}

// lockAccount serializes an account's credential cycle by taking its in-process
// mutex then its cross-process flock; the returned release reverses that order
// and must be called exactly once. If the flock isn't taken before ctx is done
// it returns an error, and the caller falls back to the existing (possibly
// stale) credential rather than racing a refresh.
func (m *Manager) lockAccount(ctx context.Context, id int) (func(), error) {
	mu := m.acctLock(id)
	mu.Lock()
	h, err := proc.Flock(ctx, filepath.Join(m.LockDir, AccountDirName(id)+".lock"))
	if err != nil {
		mu.Unlock()
		return nil, fmt.Errorf("acct-%d refresh lock: %w", id, err)
	}
	return func() {
		h.Release()
		mu.Unlock()
	}, nil
}

// LockAccount serializes an account's credential cycle for out-of-package
// writers (hostsync's materialize install); the returned release must be
// called exactly once.
func (m *Manager) LockAccount(ctx context.Context, id int) (func(), error) {
	return m.lockAccount(ctx, id)
}

// Open ensures the state dir exists, opens the database, and returns a Manager.
func Open() (*Manager, error) {
	if err := EnsureStateDir(); err != nil {
		return nil, fmt.Errorf("ensure state dir: %w", err)
	}
	st, err := store.Open(DBPath())
	if err != nil {
		return nil, err
	}
	return &Manager{
		Store:   st,
		OAuth:   oauth.New(),
		Creds:   sysCredentials{},
		LockDir: filepath.Join(StateDir(), "locks"),
		Warnf:   func(format string, args ...any) { fmt.Fprintf(os.Stderr, "cc-pool: "+format+"\n", args...) },
	}, nil
}

// Close releases resources.
func (m *Manager) Close() error {
	if m.Store != nil {
		return m.Store.Close()
	}
	return nil
}

// Meta keys recording pool-level state in the store's meta table.
const (
	// metaInitialized marks that the pool was set up via `ccp init` (or add's
	// auto-init) — distinct from "the DB file exists", which any read-only command
	// creates just by opening the Manager.
	metaInitialized = "initialized"
	// metaOverlayKind records the overlay provider chosen at init, so new
	// accounts keep using it and a re-init never flips providers under live
	// accounts.
	metaOverlayKind = "overlay_kind"
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
