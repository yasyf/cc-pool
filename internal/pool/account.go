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
	"github.com/yasyf/fusekit/fileproviderd"
	"github.com/yasyf/fusekit/mountd"
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
	kind, reason, err := m.ensureOverlayKind(context.Background())
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

// PrepareAdd allocates the next account dir, establishes its overlay, and seeds its
// private .claude.json from ~/.claude.json so login inherits onboarding state.
// Returns the login command to run. Stale credentials from a dead attempt are
// deleted unless the dir is reused (SeedKeptExisting). The index is held by an
// atomic pending_adds reservation FinalizeAdd promotes; no account row or Keychain
// item exists until then. See ccn doc 935d323.
func (m *Manager) PrepareAdd(ctx context.Context) (pending *PendingAdd, err error) {
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
	// blocked until the daemon's TTL sweep — UNLESS a live fuse mount could not be
	// reclaimed (keepReservation): releasing then would leave the mount rowless and
	// nameless, so the reservation is held to keep its name until `ccp add` retries.
	keepReservation := false
	defer func() {
		if err != nil && !keepReservation {
			err = errors.Join(err, m.Store.ReleaseAccountIndex(n))
		}
	}()
	acctDir := AccountDir(n)
	backend, detectReason, err := m.ensureOverlayKind(ctx)
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
		// ANY post-mount Setup failure LEAVES the fresh mount up: a post-Setup capability
		// refusal (ErrHolderUnsupported), or a lost ack / protocol mismatch after the
		// holder mounted (ErrMountedUnverified — the feature gate probed the holder's
		// mount list to tell it from a clean pre-mount miss). The symlink fallback below
		// refuses a live mount and would strand a rowless mount that poisons later adds,
		// so reclaim it through the holder first — teardownWithRetry is ungated,
		// idempotent, and closes the journal-resurrection gap. A hard reclaim failure
		// KEEPS the reservation so the live mount is never orphaned nameless; the user
		// retries `ccp add`. A live-session ErrBusy is near-impossible on a fresh mount.
		if errors.Is(setupErr, ErrHolderUnsupported) || errors.Is(setupErr, ErrMountedUnverified) {
			if terr := m.teardownWithRetry(prov, ClaudeDir(), acctDir, n); terr != nil {
				keepReservation = true
				if errors.Is(terr, mountd.ErrBusy) {
					return nil, fmt.Errorf("set up overlay for %s: the fresh fuse mount is busy and cannot be reclaimed for the symlink fallback (%w) — its reservation is kept; retry `ccp add` once the holding session ends", acctDir, terr)
				}
				return nil, fmt.Errorf("reclaim the unverified fuse mount for %s before the symlink fallback failed (after fuse setup failed: %w): %w — its reservation is kept; retry `ccp add`", acctDir, setupErr, terr)
			}
		}
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

	// Atomic consume+upsert: fails loud if the reservation was swept or released.
	if err := m.Store.PromoteReservedAccount(acct); err != nil {
		return nil, fmt.Errorf("finalize %s: %w", p.ConfigDir, err)
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

// AbandonAdd cleans up a prepared-but-not-finalized account dir and any credential
// its login wrote (the Keychain item and the plaintext file, each deleted
// explicitly). The index reservation is released last but unconditionally. p must be
// non-nil, from PrepareAdd. Idempotent. See ccn doc 935d323.
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
		pend.ID, pend.OverlayKind = p.Index, string(p.OverlayKind)
		errs = errors.Join(errs, m.removeAccountDir(pend, prov))
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
	if err := m.removeAccountDir(a, prov); err != nil {
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
// backing, sequenced per the lease contract by WHO performs the destructive step:
//
//   - A FUSE row's Teardown is holder-delegated (the holder's lease-ladder Seizes
//     the key), so it runs FIRST — its mountd.ErrBusy is the gate, and a consumer
//     seize of the same key would self-bounce forever. Only AFTER it confirms is
//     the (now-plain) dir fenced for the local RemoveAll.
//   - A symlink/File Provider row's Teardown is a LOCAL destructive op (delete
//     links / deregister + unlink the domain), so it runs UNDER the fence — a held
//     lease must defer it, not discover the overlay already destroyed afterward.
func (m *Manager) removeAccountDir(a store.Account, prov fkoverlay.Provider) error {
	configDir := a.ConfigDir
	fuse := prov.Backend().IsFuse()
	teardown := func() error {
		if err := m.teardownWithRetry(prov, ClaudeDir(), configDir, a.ID); err != nil {
			return fmt.Errorf("teardown overlay: %w", err)
		}
		return nil
	}
	if fuse {
		if err := teardown(); err != nil {
			return err
		}
	}
	fence, err := m.SeizeSessionLease(a)
	if err != nil {
		return fmt.Errorf("acct-%02d dir is in use (a live session or launch holds it); relaunch or close it, then retry `ccp remove`: %w", a.ID, err)
	}
	defer func() { _ = fence.Release() }()
	if !fuse {
		if err := teardown(); err != nil {
			return err
		}
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
	// Re-check just before Sync's MkdirAll: a remove racing the poll must not
	// recreate the dir. A gone row is not an error — nothing left to sync.
	if _, err := m.Store.GetAccount(a.ID); err != nil {
		if errors.Is(err, store.ErrAccountNotFound) {
			return nil
		}
		return fmt.Errorf("sync overlay for acct-%02d: %w", a.ID, err)
	}
	return prov.Sync(ClaudeDir(), a.ConfigDir)
}

type contextOverlaySyncer interface {
	SyncContext(ctx context.Context, base, accountDir string) error
}

// SyncOverlayContext is the launch-bound form of SyncOverlay. File Provider
// control calls receive ctx directly instead of the provider's background
// context; injected providers may implement contextOverlaySyncer themselves.
func (m *Manager) SyncOverlayContext(ctx context.Context, a store.Account) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return fmt.Errorf("sync overlay for acct-%02d: parse stored backend: %w", a.ID, err)
	}
	prov, err := m.overlayFor(backend)
	if err != nil {
		return fmt.Errorf("sync overlay for acct-%02d: resolve provider: %w", a.ID, err)
	}
	if syncer, ok := prov.(contextOverlaySyncer); ok {
		return syncer.SyncContext(ctx, ClaudeDir(), a.ConfigDir)
	}
	if backend == fkoverlay.BackendFileProvider && m.OverlayFor == nil {
		return m.syncFileProviderContext(ctx, a)
	}
	if err := prov.Sync(ClaudeDir(), a.ConfigDir); err != nil {
		return err
	}
	return ctx.Err()
}

func (m *Manager) syncFileProviderContext(ctx context.Context, a store.Account) error {
	spec := m.overlaySpec().FileProvider
	host := &fileproviderd.RemoteDomainHost{
		AppPath:       spec.AppPath,
		ControlSocket: spec.ControlSocket,
		SpawnTimeout:  spec.SpawnTimeout,
		LaunchTimeout: spec.LaunchTimeout,
	}
	domain := filepath.Base(a.ConfigDir)
	root, err := host.Ensure(ctx, domain)
	if err != nil {
		return fmt.Errorf("file provider sync %s: %w", a.ConfigDir, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fileproviderd.AtomicSymlink(a.ConfigDir, root); err != nil {
		return fmt.Errorf("file provider sync %s: %w", a.ConfigDir, err)
	}
	if err := host.Signal(ctx, domain); err != nil {
		return fmt.Errorf("file provider sync %s: signal: %w", a.ConfigDir, err)
	}
	return ctx.Err()
}

// ensureOverlayKind returns the new-account overlay backend: the one recorded at
// init, else detects and records one. The reason string is non-empty only when
// detection just ran and ruled fuse out, so callers can surface it.
func (m *Manager) ensureOverlayKind(ctx context.Context) (fkoverlay.Backend, string, error) {
	if v, ok, err := m.Store.GetMeta(metaOverlayKind); err != nil {
		return "", "", err
	} else if ok {
		b, perr := fkoverlay.Parse(v)
		if perr != nil {
			return "", "", fmt.Errorf("read recorded overlay backend: %w", perr)
		}
		return b, "", nil
	}
	backend, reason := m.detectOverlay(ctx)
	if err := m.Store.SetMeta(metaOverlayKind, string(backend)); err != nil {
		return "", "", err
	}
	return backend, reason, nil
}
