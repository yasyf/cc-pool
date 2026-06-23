package pool

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// Refresher is the slice of *oauth.Client the Manager needs: token refresh and
// usage sampling. Consumer-defined so tests can fake provider behavior.
type Refresher interface {
	Refresh(ctx context.Context, flightKey, refreshToken string) (*oauth.TokenResponse, error)
	Usage(ctx context.Context, accessToken string) (*oauth.Usage, error)
}

// CredentialStore is the slice of package keychain the Manager needs for
// credential reads, writes, and deletes. Discover resolves the account (-a)
// label actually stored on a service's item — items written by `claude /login`
// carry whatever label claude derived at the time, which may not match a label
// recomputed later, so deletions of claude-written items must discover first.
type CredentialStore interface {
	Read(service, account string) (*keychain.Credential, error)
	Write(service, account string, cred *keychain.Credential) error
	Delete(service, account string) error
	Discover(service string) (string, error)
}

// sysKeychain adapts package keychain's process-global functions to
// CredentialStore.
type sysKeychain struct{}

func (sysKeychain) Read(service, account string) (*keychain.Credential, error) {
	return keychain.Read(service, account)
}

func (sysKeychain) Write(service, account string, cred *keychain.Credential) error {
	return keychain.Write(service, account, cred)
}

func (sysKeychain) Delete(service, account string) error {
	return keychain.Delete(service, account)
}

func (sysKeychain) Discover(service string) (string, error) {
	return keychain.DiscoverAccount(service)
}

// Manager is the high-level façade over the store, the OAuth client, and the
// Keychain/overlay machinery. CLI commands, the TUI wizards, and the daemon all
// go through it.
type Manager struct {
	Store    *store.Store
	OAuth    Refresher
	Keychain CredentialStore

	// OverlayFor resolves an overlay backend to a fusekit/overlay provider; nil
	// means pool.OverlayProviderFor. Tests inject fakes here so conversion logic
	// runs without a live mount.
	OverlayFor func(fkoverlay.Backend) (fkoverlay.Provider, error)

	// DetectOverlay resolves the overlay backend for new accounts when none is
	// recorded yet; nil means pool.DetectOverlayBackend. Tests inject verdicts so
	// Init never spawns a mount holder.
	DetectOverlay func() (fkoverlay.Backend, string)

	// CanHostFuse reports whether fuse may be recorded as the new-account
	// default; nil means pool.CanHostFuse. Tests inject true so conversion
	// flows run on fake providers in builds that cannot host mounts.
	CanHostFuse func() bool

	// LockDir holds the per-account cross-process refresh lock files. Open sets
	// it under the state dir; tests point it at a temp dir so they never touch
	// real state.
	LockDir string

	// muMap guards locks; locks holds one mutex per account ID serializing that
	// account's credential read→refresh→write cycle WITHIN this process. These
	// per-account mutexes are DELIBERATELY held across Keychain and OAuth I/O — a
	// documented exception to the no-locks-across-I/O rule: concurrent mutation
	// of one account's single-use refresh token is never safe (double-spend gets
	// invalid_grant; a stale write-back clobbers the rotated token), so
	// serializing the whole cycle is the point. muMap itself is only ever held
	// for the map access, never across an op. Cross-process serialization (the
	// daemon vs a concurrent `ccp` invocation) is layered on top by lockAccount's
	// per-account flock — the mutex alone cannot span processes.
	muMap sync.Mutex
	locks map[int]*sync.Mutex
}

// acctLock returns the mutex serializing credential operations for an account
// within this process.
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

// lockAccount serializes an account's credential read→refresh→write cycle both
// in-process (the per-account mutex) and across processes (a per-account flock),
// so the daemon and a concurrent `ccp` invocation can never both refresh one
// account and double-spend its single-use refresh token. Acquire order is mutex
// then flock; the returned release reverses it and must be called exactly once.
// A flock that cannot be taken before ctx is done is returned as an error; the
// caller falls back to the existing (possibly stale) credential rather than
// racing a refresh.
func (m *Manager) lockAccount(ctx context.Context, id int) (func(), error) {
	mu := m.acctLock(id)
	mu.Lock()
	h, err := flockAcquire(ctx, filepath.Join(m.LockDir, AccountDirName(id)+".lock"))
	if err != nil {
		mu.Unlock()
		return nil, fmt.Errorf("acct-%d refresh lock: %w", id, err)
	}
	return func() {
		h.release()
		mu.Unlock()
	}, nil
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
		Store:    st,
		OAuth:    oauth.New(),
		Keychain: sysKeychain{},
		LockDir:  filepath.Join(StateDir(), "locks"),
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
	// auto-init) — deliberately distinct from "the DB file exists", which any
	// read-only command creates as a side effect of opening the Manager.
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
