package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func (m *Manager) overlayFor(b fkoverlay.Backend) (fkoverlay.Provider, error) {
	if m.OverlayFor != nil {
		return m.OverlayFor(b)
	}
	return OverlayProviderFor(b)
}

// detectOverlay defaults to unbounded detection: init/add paths carry no context
// (Select bounds its own dial/spawn).
func (m *Manager) detectOverlay() (fkoverlay.Backend, string) {
	if m.DetectOverlay != nil {
		return m.DetectOverlay()
	}
	return DetectOverlayBackend(context.Background())
}

func (m *Manager) canHostFuse() bool {
	if m.CanHostFuse != nil {
		return m.CanHostFuse()
	}
	return CanHostFuse()
}

// ErrConvertUnsupported means a resolved provider does not report the backend it was
// resolved for; the Backend() fences fail closed because a fuse-side conversion on
// symlink paths destroys account state.
var ErrConvertUnsupported = errors.New("overlay backend unavailable")

// ConvertOverlay switches an account's overlay provider, persisting the row last so
// an interrupted run re-converges. MUST run inside the daemon, which alone gates
// against live sessions; a failed fuse mount rolls back to symlink. ctx bounds the
// fuse-side conversion: callers detach it from request cancellation
// (context.WithoutCancel) so a daemon shutdown mid-conversion finishes or rolls
// back instead of abandoning a half-converted account.
func (m *Manager) ConvertOverlay(ctx context.Context, a store.Account, to fkoverlay.Backend) (store.Account, error) {
	from, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return a, fmt.Errorf("convert acct-%02d: parse stored backend: %w", a.ID, err)
	}
	if from == to {
		return a, nil
	}
	fromProv, err := m.overlayFor(from)
	if err != nil {
		return a, fmt.Errorf("convert acct-%02d: resolve source provider: %w", a.ID, err)
	}
	toProv, err := m.overlayFor(to)
	if err != nil {
		return a, fmt.Errorf("convert acct-%02d: resolve target provider: %w", a.ID, err)
	}
	if fromProv.Backend() != from {
		return a, fmt.Errorf("convert acct-%02d: source %q: %w", a.ID, from, ErrConvertUnsupported)
	}
	if toProv.Backend() != to {
		return a, fmt.Errorf("convert acct-%02d: target %q: %w", a.ID, to, ErrConvertUnsupported)
	}
	if to.IsFuse() {
		return m.convertToFuse(ctx, a, fromProv, toProv)
	}
	return m.convertToSymlink(a, fromProv, toProv)
}

func (m *Manager) convertToFuse(ctx context.Context, a store.Account, symProv, fuseProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	if overlay.Mounted(dir) {
		return a, fmt.Errorf("convert acct-%02d: %s is already a mountpoint but the row says %s; refusing", a.ID, dir, a.OverlayKind)
	}
	// A mux bridge symlink resolves INTO the shared mirror; moving private files
	// through it (MovePrivateEntries below) would write into the live mount, not
	// the account dir. The row says symlink, so a bridge here is wreckage — refuse.
	if IsBridgeSymlink(dir) {
		return a, fmt.Errorf("convert acct-%02d: %s is a mux bridge symlink into %s but the row says %s; refusing to move files through the mirror", a.ID, dir, MuxRootDir(), a.OverlayKind)
	}

	// An account that never completed a login legitimately has no identity.
	pre, preErr := readIdentity(filepath.Join(dir, ".claude.json"))
	if preErr != nil && !errors.Is(preErr, ErrNoIdentity) {
		return a, fmt.Errorf("convert acct-%02d: read identity before conversion: %w", a.ID, preErr)
	}
	// Nothing moved yet: a spent budget aborts cleanly, no rollback needed.
	if err := ctx.Err(); err != nil {
		return a, fmt.Errorf("convert acct-%02d: %w", a.ID, err)
	}

	// STRAND WINDOW: from here until SetAccountOverlayKind the private files
	// live in priv while the row still says symlink; every error return below
	// must go through rollbackToSymlink, or the account is stranded until
	// HealStrandedPrivate — the recovery of last resort — finds it.
	if err := fkoverlay.MovePrivateEntries(dir, priv, m.overlaySpec()); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("move private files: %w", err))
	}
	if err := symProv.Teardown(base, dir); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("tear down symlinks: %w", err))
	}
	// A spent budget must not start a mount it has no time to verify.
	if err := ctx.Err(); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, err)
	}
	if err := fuseProv.Setup(base, dir); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("mount: %w", err))
	}

	// Verify the identity we moved into the private root survived the move intact.
	// We read the backing file, NOT back through the fresh mount: a through-mount
	// os.ReadFile is unbounded and stalls at the macOS-NFS/fuse-t transport layer
	// when --force converts a dir a live session still holds — hanging the migrate
	// and stranding the account. The mirror serves this exact file (ReadSynth merges
	// priv/.claude.json with base), and mirror liveness is already vouched by Setup's
	// bounded MountAlive stat, the mitigation gate's post-mount health re-check, and
	// the heal loop — so the backing-file read preserves the only invariant that
	// matters (the moved identity is intact and unchanged) without the stall.
	if preErr == nil {
		post, err := readIdentity(filepath.Join(priv, ".claude.json"))
		if err != nil {
			return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("identity not readable in private root after move: %w", err))
		}
		if post.AccountUUID != pre.AccountUUID {
			return a, m.rollbackToSymlink(a, symProv, fuseProv,
				fmt.Errorf("identity in private root is %s, expected %s", post.AccountUUID, pre.AccountUUID))
		}
	}

	if err := m.Store.SetAccountOverlayKind(a.ID, string(fuseProv.Backend())); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("persist row: %w", err))
	}
	a.OverlayKind = string(fuseProv.Backend())
	return a, nil
}

// rollbackToSymlink restores a symlink overlay after a failed fuse setup. If the
// unmount does not take it stops — laying symlinks into a live mirror would write
// through to the real ~/.claude — leaving recovery to the daemon's reconcile.
func (m *Manager) rollbackToSymlink(a store.Account, symProv, fuseProv fkoverlay.Provider, cause error) error {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := fuseProv.Teardown(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and rollback unmount failed: %w; private files remain in %s until the daemon reconciles)",
			a.ID, cause, err, priv)
	}
	// Both moves run regardless (disjoint name sets); Setup is sequenced after them
	// so it never lays links over an un-swept dir.
	spec := m.overlaySpec()
	if err := errors.Join(
		fkoverlay.MovePrivateEntries(priv, dir, spec),
		fkoverlay.MoveSharedOrphans(dir, base, spec),
	); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and symlink rollback failed: %w)", a.ID, cause, err)
	}
	if err := symProv.Setup(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and symlink rollback failed: %w)", a.ID, cause, err)
	}
	removePrivateRootIfEmpty(priv, spec)
	return fmt.Errorf("convert acct-%02d: %w (rolled back to symlink)", a.ID, cause)
}

// convertToSymlink turns a fuse account into a symlink one. With nothing mounted
// Teardown is a no-op, so even a build that cannot host fuse can retreat from a
// stale fuse row — pure file moves.
func (m *Manager) convertToSymlink(a store.Account, fuseProv, symProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	spec := m.overlaySpec()
	if err := fuseProv.Teardown(base, dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: unmount: %w", a.ID, err)
	}
	if _, err := os.Stat(priv); err == nil {
		if err := fkoverlay.MovePrivateEntries(priv, dir, spec); err != nil {
			return a, fmt.Errorf("convert acct-%02d: restore private files: %w", a.ID, err)
		}
	} else if !os.IsNotExist(err) {
		return a, fmt.Errorf("convert acct-%02d: stat private root: %w", a.ID, err)
	}
	// claude may have written real shared entries into the bare mountpoint after a
	// force-unmount; move them to base first or Setup's assertSymlink refuses to
	// clobber them and the retreat fails.
	if err := fkoverlay.MoveSharedOrphans(dir, base, spec); err != nil {
		return a, fmt.Errorf("convert acct-%02d: relocate orphaned shared entries: %w", a.ID, err)
	}
	if err := symProv.Setup(base, dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: lay symlinks: %w", a.ID, err)
	}
	if err := m.Store.SetAccountOverlayKind(a.ID, string(fkoverlay.BackendSymlink)); err != nil {
		return a, fmt.Errorf("convert acct-%02d: persist row: %w", a.ID, err)
	}
	a.OverlayKind = string(fkoverlay.BackendSymlink)
	removePrivateRootIfEmpty(priv, spec)
	return a, nil
}

// removePrivateRootIfEmpty removes an emptied fuse private backing dir, first
// clearing entries the spec classifies as skip litter (.DS_Store, AppleDouble
// "._*" sidecars from pre-mitigation fuse mounts). A dir holding anything else
// is left in place — its contents are unclassified data deleting could destroy.
func removePrivateRootIfEmpty(priv string, spec fkoverlay.Spec) {
	entries, err := os.ReadDir(priv)
	if err != nil {
		return
	}
	for _, e := range entries {
		if spec.Skipped(e.Name()) {
			_ = os.Remove(filepath.Join(priv, e.Name()))
		}
	}
	_ = os.Remove(priv)
}

// HealStrandedPrivate recovers a symlink account whose private files are stranded in
// a fuse private backing dir (an interrupted conversion), moving them back and
// re-asserting the symlink overlay; reports whether anything was healed.
func (m *Manager) HealStrandedPrivate(a store.Account) (bool, error) {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return false, fmt.Errorf("heal acct-%02d: parse stored backend: %w", a.ID, err)
	}
	if backend.IsFuse() {
		return false, fmt.Errorf("heal acct-%02d: account is fuse-backed; its private root is in use, not stranded", a.ID)
	}
	dir := a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	spec := m.overlaySpec()
	has, err := fkoverlay.HasPrivateEntries(priv, spec)
	if err != nil {
		return false, fmt.Errorf("heal acct-%02d: %w", a.ID, err)
	}
	if !has {
		return false, nil
	}
	if overlay.Mounted(dir) {
		return false, fmt.Errorf("heal acct-%02d: %s is a live mountpoint but the row says symlink; refusing to move files under a mirror", a.ID, dir)
	}
	// A mux bridge symlink is a live-mirror stand-in the row must not carry: moving
	// the stranded files back through it (MovePrivateEntries) would write into the
	// mount. Refuse loudly for `ccp doctor` rather than corrupt the mirror.
	if IsBridgeSymlink(dir) {
		return false, fmt.Errorf("heal acct-%02d: %s is a mux bridge symlink but the row says symlink; refusing to move files through the mirror — run `ccp doctor`", a.ID, dir)
	}
	symProv, err := m.overlayFor(fkoverlay.BackendSymlink)
	if err != nil {
		return false, fmt.Errorf("heal acct-%02d: resolve symlink provider: %w", a.ID, err)
	}
	if err := errors.Join(
		fkoverlay.MovePrivateEntries(priv, dir, spec),
		symProv.Setup(ClaudeDir(), dir),
	); err != nil {
		return false, fmt.Errorf("heal acct-%02d: %w", a.ID, err)
	}
	removePrivateRootIfEmpty(priv, spec)
	return true, nil
}

// SetDefaultOverlayKind records backend as the default for accounts added later. Fuse
// is refused when this build cannot host mounts, else new accounts' rows would promise
// a mirror their dirs cannot have.
func (m *Manager) SetDefaultOverlayKind(backend fkoverlay.Backend) error {
	switch {
	case backend == fkoverlay.BackendSymlink:
	case backend.IsFuse():
		if !m.canHostFuse() {
			return fmt.Errorf("set default overlay %q: this build cannot host fuse mounts — run `ccp fuse enable`: %w", backend, ErrConvertUnsupported)
		}
	default:
		return fmt.Errorf("set default overlay: unknown backend %q", backend)
	}
	if err := m.Store.SetMeta(metaOverlayKind, string(backend)); err != nil {
		return fmt.Errorf("set default overlay: %w", err)
	}
	return nil
}

// ConfiguredOverlayKind returns the pool's recorded default overlay backend and
// whether one has been recorded. Pure read — never detects or persists — so callers
// like doctor can compare without side effects.
func (m *Manager) ConfiguredOverlayKind() (fkoverlay.Backend, bool, error) {
	v, ok, err := m.Store.GetMeta(metaOverlayKind)
	if err != nil {
		return "", false, fmt.Errorf("read default overlay backend: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	b, err := fkoverlay.Parse(v)
	if err != nil {
		return "", false, fmt.Errorf("read default overlay backend: %w", err)
	}
	return b, true, nil
}
