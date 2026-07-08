package pool

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// ErrNotInitialized means the pool has not been set up yet.
var ErrNotInitialized = errors.New("pool not initialized")

// InitResult summarizes what `ccp init` did.
type InitResult struct {
	OverlayKind fkoverlay.Backend
	// OverlayFallbackReason says why detection settled on symlink; non-empty only
	// when THIS Init ran detection and ruled fuse out.
	OverlayFallbackReason string
	Already               bool
}

// Init prepares the pool's ~/.cc-pool state dirs, overlay choice, and initialized
// marker. Never touches ~/.claude or the Keychain — accounts join via Add, each
// with its own `claude auth login`. Idempotent.
func (m *Manager) Init() (*InitResult, error) {
	if err := EnsureStateDir(); err != nil {
		return nil, err
	}
	if err := EnsureAccountsDir(); err != nil {
		return nil, err
	}
	already, err := m.Initialized()
	if err != nil {
		return nil, err
	}
	kind, reason, err := m.ensureOverlayKind()
	if err != nil {
		return nil, err
	}
	if err := m.Store.SetMeta(metaInitialized, "1"); err != nil {
		return nil, err
	}
	return &InitResult{OverlayKind: kind, OverlayFallbackReason: reason, Already: already}, nil
}

// PendingAdd describes a half-created account awaiting interactive login.
type PendingAdd struct {
	Index           int
	ConfigDir       string
	KeychainService string
	OverlayKind     fkoverlay.Backend
	// FallbackReason says why fuse was ruled out: Setup fell back to symlinks, or
	// detection did. OverlayKind then records symlink; "" when fuse held or was
	// never in play.
	FallbackReason string
	LoginCommand   string
	ClaudeJSONSeed SeedOutcome
}

// DuplicateIdentity returns an existing pool account sharing want's accountUuid
// (the same Claude subscription), or nil. Unreadable accounts are skipped so one
// broken dir never blocks the check.
func (m *Manager) DuplicateIdentity(want Identity) (*store.Account, error) {
	accounts, err := m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	for i := range accounts {
		a := accounts[i]
		backend, err := fkoverlay.Parse(a.OverlayKind)
		if err != nil {
			continue
		}
		id, err := AccountIdentity(backend, a.ConfigDir)
		if err != nil {
			continue
		}
		if id.AccountUUID == want.AccountUUID {
			return &a, nil
		}
	}
	return nil, nil
}

// PrepareAdd allocates the next account dir, establishes its overlay, and seeds
// its private .claude.json from ~/.claude.json so login inherits onboarding
// state instead of the first-run wizard. Returns the login command to run.
// Unless the dir is reused (SeedKeptExisting), stale credentials from a dead
// attempt — the Keychain item under its service and the plaintext file — are
// deleted. The index is held by an atomic pending_adds reservation (two
// concurrent PrepareAdds can never collide) that FinalizeAdd promotes and
// AbandonAdd/ReleaseAdd release; no account row or Keychain item exists until
// FinalizeAdd.
func (m *Manager) PrepareAdd() (pending *PendingAdd, err error) {
	ok, err := m.Initialized()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotInitialized
	}
	n, err := m.Store.ReserveAccountIndex()
	if err != nil {
		return nil, err
	}
	// Every failure below must free the reservation, else the index stays
	// blocked until the daemon's TTL sweep.
	defer func() {
		if err != nil {
			err = errors.Join(err, m.Store.ReleaseAccountIndex(n))
		}
	}()
	acctDir := AccountDir(n)
	backend, detectReason, err := m.ensureOverlayKind()
	if err != nil {
		return nil, err
	}
	prov, err := m.overlayFor(backend)
	if err != nil {
		return nil, fmt.Errorf("resolve overlay provider for %s: %w", acctDir, err)
	}
	// Seed before Setup: File Provider's readiness probe reads .claude.json.
	// privFreshlyCreated is true only when THIS call's atomic Mkdir claimed the
	// backing dir, so a Setup failure below can clean up its own mess without
	// touching a kept/resume dir. A stat-then-MkdirAll would misread a dir born in
	// that gap (a concurrent add) as ours and RemoveAll it. The parent (AccountsDir)
	// is guaranteed by Init's EnsureAccountsDir, which the Initialized gate above proves ran.
	privFreshlyCreated := false
	if backend == fkoverlay.BackendFileProvider {
		priv := prov.PrivateRoot(acctDir)
		switch err := os.Mkdir(priv, 0o700); {
		case err == nil:
			privFreshlyCreated = true
		case errors.Is(err, fs.ErrExist):
			// A pre-existing dir (kept/resume attempt, or a racing add) — proceed, never claim it.
		default:
			return nil, fmt.Errorf("prepare private store for %s: %w", acctDir, err)
		}
		if _, err := seedClaudeJSON(prov, acctDir, ClaudeJSONPath()); err != nil {
			return nil, fmt.Errorf("seed .claude.json for %s: %w", acctDir, err)
		}
	}
	fallbackReason := detectReason
	if setupErr := prov.Setup(ClaudeDir(), acctDir); setupErr != nil {
		if !backend.IsFuse() {
			// A File Provider domain that never came up leaves the private backing dir
			// we just created (seedClaudeJSON always fills it), which a retry would then
			// adopt as SeedKeptExisting. Drop it — but only when we created it, never a
			// pre-existing kept/resume dir.
			if privFreshlyCreated {
				if rmErr := os.RemoveAll(prov.PrivateRoot(acctDir)); rmErr != nil {
					setupErr = errors.Join(setupErr, fmt.Errorf("clean up fresh private store for %s: %w", acctDir, rmErr))
				}
			}
			return nil, fmt.Errorf("set up overlay for %s: %w", acctDir, setupErr)
		}
		// Fuse Setup failure = holder unavailable, not fatal: fall back to
		// symlinks; the reason rides along so `ccp add` names it.
		fallbackReason = setupErr.Error()
		prov, err = m.overlayFor(fkoverlay.BackendSymlink)
		if err != nil {
			return nil, fmt.Errorf("resolve fallback symlink provider for %s (after fuse setup failed: %w): %w", acctDir, setupErr, err)
		}
		if err := prov.Setup(ClaudeDir(), acctDir); err != nil {
			// Wrap both causes: callers match either with errors.Is, and the
			// symlink error alone would mask the fuse failure (e.g. ErrForeignMount).
			return nil, fmt.Errorf("set up fallback symlink overlay for %s (after fuse setup failed: %w): %w", acctDir, setupErr, err)
		}
		// Fuse may leave an empty backing dir (holder creates it pre-mount); drop
		// it only if empty — contents are unclassified state, never destroy them.
		removePrivateRootIfEmpty(fkoverlay.FusePrivateRoot(acctDir), m.overlaySpec())
	}
	seed, err := seedClaudeJSON(prov, acctDir, ClaudeJSONPath())
	if err != nil {
		return nil, fmt.Errorf("seed .claude.json for %s: %w", acctDir, err)
	}
	svc := creds.ServiceName(acctDir)
	if seed != SeedKeptExisting {
		// A leftover item is garbage from a dead attempt that FinalizeAdd would
		// register. Discover by service, not a recomputed label — the item carries
		// whatever -a label claude stored at login.
		stale := store.Account{ConfigDir: acctDir, KeychainService: svc}
		account, err := m.Creds.Discover(svc)
		switch {
		case errors.Is(err, creds.ErrNotFound):
		case err != nil:
			return nil, fmt.Errorf("probe stale credential for %s: %w", acctDir, err)
		default:
			stale.KeychainAccount = account
			if derr := m.Creds.Store(stale, creds.SourceKeychain).Delete(); derr != nil {
				return nil, fmt.Errorf("purge stale credential for %s: %w", acctDir, derr)
			}
		}
		// A dead headless attempt leaves an identity-less .credentials.json the
		// fresh login would later diverge from; purge it too.
		if err := m.Creds.Store(stale, creds.SourceFile).Delete(); err != nil {
			return nil, fmt.Errorf("purge stale file credential for %s: %w", acctDir, err)
		}
	}
	return &PendingAdd{
		Index:           n,
		ConfigDir:       acctDir,
		KeychainService: svc,
		// The provider actually established, not the requested backend: recording
		// the request would promise a mirror a fallen-back dir doesn't have.
		OverlayKind:    prov.Backend(),
		FallbackReason: fallbackReason,
		// Pin claude's plugin root to the shared base so login writes canonical
		// ~/.claude plugin paths, not acct-anchored ones claude's marketplace
		// validator later rejects; see cli.execEnv.
		LoginCommand: fmt.Sprintf("CLAUDE_CODE_PLUGIN_CACHE_DIR=%s CLAUDE_CONFIG_DIR=%s claude auth login",
			filepath.Join(ClaudeDir(), "plugins"), acctDir),
		ClaudeJSONSeed: seed,
	}, nil
}

// FinalizeAdd confirms the credential landed after the interactive login,
// re-asserts ACL ownership, validates with one usage call, and records the
// account. label is an optional human note.
func (m *Manager) FinalizeAdd(ctx context.Context, p *PendingAdd, label string) (*store.Account, error) {
	// A completed `claude auth login` writes the account's own oauthAccount identity; a
	// copied credential writes none. Missing identity means login never completed —
	// refuse it, so the pool never registers a copy of plain claude's session
	// (Max/Pro OAuth only; Console/3rd-party logins write none and are likewise
	// refused). Precedes any credential read.
	if _, err := AccountIdentity(p.OverlayKind, p.ConfigDir); err != nil {
		if errors.Is(err, ErrNoIdentity) {
			return nil, fmt.Errorf("login didn't complete for %s — cc-pool pools Max/Pro (OAuth) logins only and won't register an unverified copy of your main login: %w", p.ConfigDir, ErrNoIdentity)
		}
		return nil, fmt.Errorf("read account identity for %s: %w", p.ConfigDir, err)
	}

	// The row FinalizeAdd is about to persist; the credential backend is
	// resolved onto it (KeychainAccount) before any row exists, so store ops
	// below run through the seam against this value.
	acct := store.Account{
		ID:              p.Index,
		ConfigDir:       p.ConfigDir,
		KeychainService: p.KeychainService,
		Label:           label,
		OverlayKind:     string(p.OverlayKind),
		CreatedAt:       time.Now(),
	}
	src := creds.SourceKeychain
	account, err := m.Creds.Discover(p.KeychainService)
	switch {
	case err == nil:
		acct.KeychainAccount = account
	case errors.Is(err, creds.ErrNotFound):
		// No Keychain item: a headless login wrote the plaintext fallback. The
		// file carries no -a label, so record today's computed one.
		if _, ferr := m.Creds.Store(acct, creds.SourceFile).Read(); ferr != nil {
			if errors.Is(ferr, creds.ErrNotFound) {
				return nil, fmt.Errorf("no credential found for %s — was the login completed?", p.ConfigDir)
			}
			return nil, ferr
		}
		acct.KeychainAccount = creds.AccountLabel()
		src = creds.SourceFile
	default:
		return nil, err
	}

	// Read the item claude wrote and write it back so our tooling owns the ACL
	// for prompt-free refresh. Only the Keychain backend has an ACL; the plaintext
	// file is read directly.
	if src == creds.SourceKeychain {
		item := m.Creds.Store(acct, creds.SourceKeychain)
		cred, err := item.Read()
		if err != nil {
			return nil, fmt.Errorf("re-assert keychain item: %w", err)
		}
		if err := item.Write(cred); err != nil {
			return nil, fmt.Errorf("re-assert keychain item: %w", err)
		}
	}

	// Consume the reservation before registering: once a sweep or release
	// re-opened the index, a concurrent add may hold it, and a blind upsert
	// would silently collide on the same index/dir/Keychain service. A crash
	// between consume and upsert briefly frees the index while the dir remains
	// — the pre-reservation semantics — and fail-loud beats silent collision.
	if err := m.Store.ConsumeAccountIndex(p.Index); err != nil {
		return nil, fmt.Errorf("finalize %s: %w", p.ConfigDir, err)
	}
	if err := m.Store.UpsertAccount(acct); err != nil {
		return nil, err
	}

	// Best-effort usage check: a failure returns the added account with the error,
	// it does not unwind the add.
	if _, _, _, err := m.SampleUsage(ctx, acct, SampleOpts{AllowRefresh: true}); err != nil {
		return &acct, fmt.Errorf("account added but usage validation failed: %w", err)
	}
	return &acct, nil
}

// ReleaseAdd frees a pending add's index reservation while keeping its dir and
// any login state it captured, so rerunning `ccp add` resumes the attempt:
// PrepareAdd re-reserves the same lowest-free index and SeedKeptExisting adopts
// the kept login. Call it on every keep-dir exit; AbandonAdd is the tear-down
// counterpart, and the daemon's TTL sweep remains the crash-only backstop.
func (m *Manager) ReleaseAdd(p *PendingAdd) error {
	return m.Store.ReleaseAccountIndex(p.Index)
}

// AbandonAdd cleans up a prepared-but-not-finalized account dir (no store row
// yet) and any credential its login wrote — the Keychain item and the plaintext
// file, each deleted explicitly so the rollback never depends on the dir
// removal succeeding. The index reservation is released last but
// unconditionally (even when cleanup partly fails): a concurrent PrepareAdd
// must never be handed the index while its dir is mid-teardown, and a
// lingering reservation would block the index until the daemon's TTL sweep.
// p must be non-nil, from PrepareAdd. Idempotent.
func (m *Manager) AbandonAdd(p *PendingAdd) error {
	var errs error
	pend := store.Account{ConfigDir: p.ConfigDir, KeychainService: p.KeychainService}
	account, err := m.Creds.Discover(p.KeychainService)
	switch {
	case errors.Is(err, creds.ErrNotFound):
	case err != nil:
		errs = fmt.Errorf("probe credential for %s: %w", p.ConfigDir, err)
	default:
		pend.KeychainAccount = account
		errs = m.Creds.Store(pend, creds.SourceKeychain).Delete()
	}
	errs = errors.Join(errs, m.Creds.Store(pend, creds.SourceFile).Delete())
	prov, err := m.overlayFor(p.OverlayKind)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("resolve overlay provider for %s: %w", p.ConfigDir, err))
	} else {
		errs = errors.Join(errs, m.removeAccountDir(prov, p.ConfigDir))
	}
	return errors.Join(errs, m.Store.ReleaseAccountIndex(p.Index))
}

// Remove deletes an account from the pool: tears down its overlay, removes its
// Keychain item, and deletes its rows. ~/.claude is never touched (it is not
// an account). Keeping the credential (deleteCredential=false) is refused when
// it is file-backed — the file lives inside the account dir being removed.
func (m *Manager) Remove(id int, deleteCredential bool) error {
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	if !deleteCredential {
		_, src, err := m.ReadCredential(a)
		switch {
		case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrUnavailable):
			// Nothing found to keep (or the Keychain — which removal won't touch —
			// is unsearchable): proceed, removal loses nothing.
		case err != nil:
			return fmt.Errorf("resolve acct-%02d's credential backend: %w", id, err)
		case src == creds.SourceFile:
			return fmt.Errorf("cannot keep acct-%02d's credential: it is file-backed (%s), which lives inside the account dir being removed; run `ccp cred move --to keychain --account %d` first",
				id, m.Creds.Store(a, creds.SourceFile), id)
		}
	}
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return fmt.Errorf("remove acct-%02d: parse stored backend: %w", id, err)
	}
	prov, err := m.overlayFor(backend)
	if err != nil {
		return fmt.Errorf("remove acct-%02d: resolve overlay provider: %w", id, err)
	}
	if err := m.removeAccountDir(prov, a.ConfigDir); err != nil {
		return err
	}
	if deleteCredential {
		if err := m.Creds.Store(a, creds.SourceKeychain).Delete(); err != nil {
			return fmt.Errorf("delete keychain item %q: %w", a.KeychainService, err)
		}
	}
	return m.Store.DeleteAccount(id)
}

// removeAccountDir tears down the overlay, then removes the dir and its private
// backing. A refused Teardown (e.g. a wedged unmount) aborts removal so we never
// RemoveAll through a live mount into the base.
func (m *Manager) removeAccountDir(prov fkoverlay.Provider, configDir string) error {
	if err := prov.Teardown(ClaudeDir(), configDir); err != nil {
		return fmt.Errorf("teardown overlay: %w", err)
	}
	if err := os.RemoveAll(configDir); err != nil {
		return fmt.Errorf("remove account dir: %w", err)
	}
	if priv := prov.PrivateRoot(configDir); priv != configDir {
		if err := os.RemoveAll(priv); err != nil {
			return fmt.Errorf("remove private backing dir: %w", err)
		}
	}
	return nil
}

// SyncOverlay re-asserts an account's overlay against the current ~/.claude: the
// symlink provider links any new top-level entry, the fuse provider (a live
// mirror) just health-checks. Run at launch and periodically by the daemon.
func (m *Manager) SyncOverlay(a store.Account) error {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return fmt.Errorf("sync overlay for acct-%02d: parse stored backend: %w", a.ID, err)
	}
	prov, err := m.overlayFor(backend)
	if err != nil {
		return fmt.Errorf("sync overlay for acct-%02d: resolve provider: %w", a.ID, err)
	}
	return prov.Sync(ClaudeDir(), a.ConfigDir)
}

// ensureOverlayKind returns the new-account overlay backend: the one recorded at
// init, else detects and records one. The reason string is non-empty only when
// detection just ran and ruled fuse out, so callers can surface it.
func (m *Manager) ensureOverlayKind() (fkoverlay.Backend, string, error) {
	if v, ok, err := m.Store.GetMeta(metaOverlayKind); err != nil {
		return "", "", err
	} else if ok {
		b, perr := fkoverlay.Parse(v)
		if perr != nil {
			return "", "", fmt.Errorf("read recorded overlay backend: %w", perr)
		}
		return b, "", nil
	}
	backend, reason := m.detectOverlay()
	if err := m.Store.SetMeta(metaOverlayKind, string(backend)); err != nil {
		return "", "", err
	}
	return backend, reason, nil
}
