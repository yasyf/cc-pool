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
// with its own `claude /login`. Idempotent.
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
// Unless the dir is reused (SeedKeptExisting), a stale Keychain item under its
// service is deleted. No account row or Keychain item exists until FinalizeAdd.
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
	fallbackReason := detectReason
	if setupErr := prov.Setup(ClaudeDir(), acctDir); setupErr != nil {
		if !backend.IsFuse() {
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
		removePrivateRootIfEmpty(fkoverlay.FusePrivateRoot(acctDir))
	}
	seed, err := seedClaudeJSON(prov, acctDir, ClaudeJSONPath())
	if err != nil {
		return nil, fmt.Errorf("seed .claude.json for %s: %w", acctDir, err)
	}
	svc := keychain.ServiceName(acctDir)
	if seed != SeedKeptExisting {
		// A leftover item is garbage from a dead attempt that FinalizeAdd would
		// register. Discover by service, not a recomputed label — the item carries
		// whatever -a label claude stored at login.
		account, err := m.Keychain.Discover(svc)
		switch {
		case errors.Is(err, keychain.ErrNotFound):
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
		// The provider actually established, not the requested backend: recording
		// the request would promise a mirror a fallen-back dir doesn't have.
		OverlayKind:    prov.Backend(),
		FallbackReason: fallbackReason,
		// Pin claude's plugin root to the shared base so login writes canonical
		// ~/.claude plugin paths, not acct-anchored ones claude's marketplace
		// validator later rejects; see cli.execEnv.
		LoginCommand: fmt.Sprintf("CLAUDE_CODE_PLUGIN_CACHE_DIR=%s CLAUDE_CONFIG_DIR=%s claude /login",
			filepath.Join(ClaudeDir(), "plugins"), acctDir),
		ClaudeJSONSeed: seed,
	}, nil
}

// FinalizeAdd, called after the interactive login, confirms the credential
// landed, re-asserts ACL ownership, validates with one usage call, and records
// the account. label is an optional human note.
func (m *Manager) FinalizeAdd(ctx context.Context, p *PendingAdd, label string) (*store.Account, error) {
	// A completed `claude /login` writes the account's own oauthAccount identity; a
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

	account, src, err := keychain.LocateCredential(p.ConfigDir, p.KeychainService)
	if errors.Is(err, keychain.ErrNotFound) {
		return nil, fmt.Errorf("no credential found for %s — was the login completed?", p.ConfigDir)
	} else if err != nil {
		return nil, err
	}

	// Read the item claude wrote and write it back so our tooling owns the ACL
	// for prompt-free refresh. Only the Keychain backend has an ACL; the plaintext
	// file is read directly.
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

	// Best-effort usage check: a failure returns the added account with the error,
	// it does not unwind the add.
	if _, _, err := m.SampleUsage(ctx, acct, SampleOpts{AllowRefresh: true}); err != nil {
		return &acct, fmt.Errorf("account added but usage validation failed: %w", err)
	}
	return &acct, nil
}

// AbandonAdd cleans up a prepared-but-not-finalized account dir (no store row yet)
// and any credential its login wrote to the Keychain. p must be non-nil, from
// PrepareAdd.
func (m *Manager) AbandonAdd(p *PendingAdd) error {
	var credErr error
	account, err := m.Keychain.Discover(p.KeychainService)
	switch {
	case errors.Is(err, keychain.ErrNotFound):
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
