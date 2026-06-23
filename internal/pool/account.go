package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// ErrNotInitialized means the pool has not been set up yet (`ccp init` or
// `ccp add`'s auto-init).
var ErrNotInitialized = errors.New("pool not initialized")

// InitResult summarizes what `ccp init` did.
type InitResult struct {
	OverlayKind fkoverlay.Backend
	// OverlayFallbackReason says why detection settled on symlink when THIS
	// Init ran the detection; "" when fuse was chosen or a kind was already
	// recorded.
	OverlayFallbackReason string
	Already               bool // the pool was already initialized
}

// Init prepares the pool: ~/.cc-pool state dirs, the overlay provider choice
// for new accounts, and the initialized marker. It never touches ~/.claude or
// the Keychain — accounts (including the user's main subscription) join via
// Add, each with its own independent `claude /login`. Idempotent.
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
	// FallbackReason says why fuse was ruled out for this account: a requested
	// fuse overlay fell back to symlinks at Setup time, or detection ran inside
	// this PrepareAdd (legacy pools with no recorded kind) and ruled fuse out.
	// OverlayKind then records symlink. "" when fuse was established or never
	// in play.
	FallbackReason string
	LoginCommand   string
	ClaudeJSONSeed SeedOutcome
}

// DuplicateIdentity returns an existing pool account that shares want's
// accountUuid (the same Claude subscription), or nil if none does. Accounts
// whose identity cannot be read are skipped, so one broken account dir never
// blocks the check.
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

// PrepareAdd allocates the next account dir, establishes its overlay, and
// seeds its private .claude.json from plain claude's (~/.claude.json) so the
// login session inherits onboarding state and settings instead of running the
// first-run wizard. It returns the login command the user must run. Unless the
// dir is being reused (SeedKeptExisting), any stale Keychain item left under
// the dir's service by an earlier dead attempt is deleted. No account row or
// new Keychain item is created yet — FinalizeAdd does that once the login
// lands.
func (m *Manager) PrepareAdd() (*PendingAdd, error) {
	ok, err := m.Initialized()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotInitialized
	}
	n, err := m.Store.NextAccountIndex()
	if err != nil {
		return nil, err
	}
	acctDir := AccountDir(n)
	backend, detectReason, err := m.ensureOverlayKind()
	if err != nil {
		return nil, err
	}
	prov, err := m.overlayFor(backend)
	if err != nil {
		return nil, fmt.Errorf("resolve overlay provider for %s: %w", acctDir, err)
	}
	// A detection that just ran here (legacy pool with no recorded backend) and
	// ruled fuse out is surfaced exactly like a Setup-time fallback.
	fallbackReason := detectReason
	if setupErr := prov.Setup(ClaudeDir(), acctDir); setupErr != nil {
		if !backend.IsFuse() {
			return nil, fmt.Errorf("set up overlay for %s: %w", acctDir, setupErr)
		}
		// A fuse Setup failure means the mount holder is unavailable right
		// now, not that the add must die: fall back to symlinks EXPLICITLY.
		// The backend actually established is recorded below and the reason
		// rides along so `ccp add` says the substitution out loud.
		fallbackReason = setupErr.Error()
		prov, err = m.overlayFor(fkoverlay.BackendSymlink)
		if err != nil {
			return nil, fmt.Errorf("resolve fallback symlink provider for %s (after fuse setup failed: %w): %w", acctDir, setupErr, err)
		}
		if err := prov.Setup(ClaudeDir(), acctDir); err != nil {
			// Both causes ride the chain: the symlink complaint alone would
			// mask the fuse failure that started this (e.g. ErrForeignMount),
			// and callers match on either with errors.Is.
			return nil, fmt.Errorf("set up fallback symlink overlay for %s (after fuse setup failed: %w): %w", acctDir, setupErr, err)
		}
		// The fuse attempt may have left an empty backing dir behind (the
		// holder creates it before mounting); drop it so the symlink account
		// doesn't accrete an inert acct-NN.private. Only-if-empty: anything
		// inside is unclassified state that must not be destroyed.
		removePrivateRootIfEmpty(fkoverlay.FusePrivateRoot(acctDir))
	}
	seed, err := seedClaudeJSON(prov, acctDir, ClaudeJSONPath())
	if err != nil {
		return nil, fmt.Errorf("seed .claude.json for %s: %w", acctDir, err)
	}
	svc := keychain.ServiceName(acctDir)
	if seed != SeedKeptExisting {
		// A leftover item under this service is garbage from a dead attempt
		// (an abandoned add, or `ccp remove --keep-credential` followed by
		// index reuse); left in place, FinalizeAdd would register the stale
		// credential. Probe by service alone (Discover), not a recomputed
		// account label — the item carries whatever -a label claude stored at
		// its login. SeedKeptExisting keeps it — the documented reuse path.
		account, err := m.Keychain.Discover(svc)
		switch {
		case errors.Is(err, keychain.ErrNotFound):
			// nothing to purge
		case err != nil:
			return nil, fmt.Errorf("probe stale credential for %s: %w", acctDir, err)
		default:
			if derr := m.Keychain.Delete(svc, account); derr != nil {
				return nil, fmt.Errorf("purge stale credential for %s: %w", acctDir, derr)
			}
		}
	}
	return &PendingAdd{
		Index:           n,
		ConfigDir:       acctDir,
		KeychainService: svc,
		// The provider actually established, not the requested backend: a failed
		// fuse Setup fell back to symlinks above, and recording the request
		// would mint a row promising a mirror the dir doesn't have.
		OverlayKind:    prov.Backend(),
		FallbackReason: fallbackReason,
		// The plugin var pins claude's plugin root to the shared base so the
		// login session (where first-launch marketplace auto-install can run)
		// writes canonical ~/.claude plugin paths, not acct-anchored ones that
		// claude's marketplace validator later rejects; see cli.execEnv.
		LoginCommand: fmt.Sprintf("CLAUDE_CODE_PLUGIN_CACHE_DIR=%s CLAUDE_CONFIG_DIR=%s claude /login",
			filepath.Join(ClaudeDir(), "plugins"), acctDir),
		ClaudeJSONSeed: seed,
	}, nil
}

// FinalizeAdd is called after the user completes the interactive login. It
// confirms the credential landed, re-asserts ACL ownership, validates with one
// usage call, and records the account. label is an optional human note.
func (m *Manager) FinalizeAdd(ctx context.Context, p *PendingAdd, label string) (*store.Account, error) {
	// A completed `claude /login` writes the account's own oauthAccount identity.
	// A startup adoption of the global credential copies the secret (the Keychain
	// item, or the plaintext .credentials.json headless over SSH) but writes NO
	// identity — a missing identity here means the login never completed. Refuse
	// to register what would be a copy of plain claude's session (invariant: the
	// pool never adopts plain claude's credential). cc-pool pools Max/Pro OAuth
	// logins only; a Console/3rd-party login writes no oauthAccount and is
	// likewise (correctly) refused. The check precedes any credential read.
	if _, err := AccountIdentity(p.OverlayKind, p.ConfigDir); err != nil {
		if errors.Is(err, ErrNoIdentity) {
			return nil, fmt.Errorf("login didn't complete for %s — cc-pool pools Max/Pro (OAuth) logins only and won't register an unverified copy of your main login: %w", p.ConfigDir, ErrNoIdentity)
		}
		return nil, fmt.Errorf("read account identity for %s: %w", p.ConfigDir, err)
	}

	account, src, err := keychain.LocateCredential(p.ConfigDir, p.KeychainService)
	if errors.Is(err, keychain.ErrNotFound) {
		return nil, fmt.Errorf("no credential found for %s — was the login completed?", p.ConfigDir)
	} else if err != nil {
		return nil, err
	}

	// Re-assert: read the item Claude wrote and write it straight back so our
	// tooling owns the ACL for prompt-free refresh thereafter. Only the Keychain
	// item has an ACL; the plaintext file backend is read directly.
	if src == keychain.SourceKeychain {
		if _, err := keychain.Reassert(p.KeychainService, account); err != nil {
			return nil, fmt.Errorf("re-assert keychain item: %w", err)
		}
	}

	acct := store.Account{
		ID:              p.Index,
		ConfigDir:       p.ConfigDir,
		KeychainService: p.KeychainService,
		KeychainAccount: account,
		Label:           label,
		OverlayKind:     string(p.OverlayKind),
		CreatedAt:       time.Now(),
	}
	if err := m.Store.UpsertAccount(acct); err != nil {
		return nil, err
	}

	// Validate end-to-end with a single usage fetch (best-effort; a transient
	// failure here does not unwind the add).
	if _, _, err := m.SampleUsage(ctx, acct, SampleOpts{AllowRefresh: true}); err != nil {
		return &acct, fmt.Errorf("account added but usage validation failed: %w", err)
	}
	return &acct, nil
}

// AbandonAdd cleans up a prepared-but-not-finalized account dir (no store row
// exists at this stage), including any credential a completed-but-unfinalized
// login wrote to its Keychain item. The item's account label is discovered,
// not recomputed — claude wrote it. p must be a non-nil PendingAdd returned by
// PrepareAdd.
func (m *Manager) AbandonAdd(p *PendingAdd) error {
	var credErr error
	account, err := m.Keychain.Discover(p.KeychainService)
	switch {
	case errors.Is(err, keychain.ErrNotFound):
		// no credential to roll back
	case err != nil:
		credErr = fmt.Errorf("probe credential for %s: %w", p.ConfigDir, err)
	default:
		credErr = m.Keychain.Delete(p.KeychainService, account)
	}
	prov, err := m.overlayFor(p.OverlayKind)
	if err != nil {
		return errors.Join(credErr, fmt.Errorf("resolve overlay provider for %s: %w", p.ConfigDir, err))
	}
	return errors.Join(credErr, m.removeAccountDir(prov, p.ConfigDir))
}

// Remove deletes an account from the pool: tears down its overlay, removes its
// Keychain item, and deletes its rows. ~/.claude is never touched (it is not
// an account).
func (m *Manager) Remove(id int, deleteCredential bool) error {
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return err
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
		if err := m.Keychain.Delete(a.KeychainService, a.KeychainAccount); err != nil {
			return fmt.Errorf("delete keychain item %q: %w", a.KeychainService, err)
		}
	}
	return m.Store.DeleteAccount(id)
}

// removeAccountDir tears down an account dir's overlay and removes the dir and
// its private backing (when the provider keeps one beside the dir, as fuse
// does). Teardown refusing to operate (e.g. a wedged unmount) aborts the
// removal so we never RemoveAll through a live mount into the base.
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

// SyncOverlay re-asserts an account's overlay so it reflects the current
// ~/.claude (the symlink provider links any new top-level entry; the fuse
// provider is a live mirror, so this just health-checks). Called at launch
// time and periodically by the daemon, which is why explicit `ccp sync` is
// unnecessary.
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

// ensureOverlayKind returns the overlay provider backend for new accounts: the
// backend recorded at init, detecting and recording one first if absent. The
// returned reason is non-empty only when detection just ran and ruled out
// fuse; it says why, so callers can surface the substitution to the user.
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
