package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// overlayFor resolves a backend to a fusekit/overlay provider through the
// Manager's injectable seam.
func (m *Manager) overlayFor(b fkoverlay.Backend) (fkoverlay.Provider, error) {
	if m.OverlayFor != nil {
		return m.OverlayFor(b)
	}
	return OverlayProviderFor(b)
}

// detectOverlay resolves overlay-backend detection through the Manager's
// injectable seam.
func (m *Manager) detectOverlay() (fkoverlay.Backend, string) {
	if m.DetectOverlay != nil {
		return m.DetectOverlay()
	}
	return DetectOverlayBackend()
}

// canHostFuse resolves the fuse-hosting capability check through the
// Manager's injectable seam.
func (m *Manager) canHostFuse() bool {
	if m.CanHostFuse != nil {
		return m.CanHostFuse()
	}
	return CanHostFuse()
}

// ErrConvertUnsupported means the provider resolved for a conversion's source
// or target backend does not actually report that backend. The real resolver
// cannot produce this (a fuse backend always maps to the holder-backed
// RemoteFuseProvider, which always reports its own backend), so the Backend()
// fences guard against wrong-backend INJECTED fakes — a conversion that *thinks*
// it is operating fuse-side while running symlink code paths is exactly how
// account state gets destroyed. It also fences fuse as the new-account default
// in builds that cannot host mounts (SetDefaultOverlayKind).
var ErrConvertUnsupported = errors.New("overlay backend unavailable")

// ConvertOverlay switches an account's overlay provider: it relocates the
// account's private files between the providers' private roots, tears down the
// old overlay, establishes the new one, and persists the row — in that order,
// with the row flip last, so an interrupted conversion always leaves a re-run
// that converges. The fuse direction mounts through the detached mount holder
// but still MUST run inside the daemon, which alone gates the conversion
// against live sessions and its own reservations. A failed fuse mount is
// rolled back to a byte-identical symlink overlay before returning.
// Converting to the backend the account already has is a no-op.
func (m *Manager) ConvertOverlay(a store.Account, to fkoverlay.Backend) (store.Account, error) {
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
		return m.convertToFuse(a, fromProv, toProv)
	}
	return m.convertToSymlink(a, fromProv, toProv)
}

// convertToFuse turns a symlink account into a fuse one: private files move to
// the sibling backing dir, the links come down, the mirror mounts over the
// (now link-free) account dir, and only then does the row flip.
func (m *Manager) convertToFuse(a store.Account, symProv, fuseProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	if overlay.Mounted(dir) {
		return a, fmt.Errorf("convert acct-%02d: %s is already a mountpoint but the row says %s; refusing", a.ID, dir, a.OverlayKind)
	}

	// The identity to re-verify through the mount. An account that never
	// completed a login legitimately has none.
	pre, preErr := readIdentity(filepath.Join(dir, ".claude.json"))
	if preErr != nil && !errors.Is(preErr, ErrNoIdentity) {
		return a, fmt.Errorf("convert acct-%02d: read identity before conversion: %w", a.ID, preErr)
	}

	if err := fkoverlay.MovePrivateEntries(dir, priv, m.overlaySpec()); err != nil {
		return a, fmt.Errorf("convert acct-%02d: move private files: %w", a.ID, err)
	}
	if err := symProv.Teardown(base, dir); err != nil {
		// Links may be half-removed; private files are already safe in the
		// backing dir. Heal/re-run converges from here.
		return a, fmt.Errorf("convert acct-%02d: tear down symlinks: %w", a.ID, err)
	}
	if err := fuseProv.Setup(base, dir); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("mount: %w", err))
	}

	// The mount is live — verify the account's identity survived the trip
	// before committing the row. A mismatch means the mirror is not serving
	// the backing dir we populated.
	if preErr == nil {
		post, err := readIdentity(filepath.Join(dir, ".claude.json"))
		if err != nil {
			return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("identity not readable through mount: %w", err))
		}
		if post.AccountUUID != pre.AccountUUID {
			return a, m.rollbackToSymlink(a, symProv, fuseProv,
				fmt.Errorf("identity through mount is %s, expected %s", post.AccountUUID, pre.AccountUUID))
		}
	}

	if err := m.Store.SetAccountOverlayKind(a.ID, string(fuseProv.Backend())); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("persist row: %w", err))
	}
	a.OverlayKind = string(fuseProv.Backend())
	return a, nil
}

// rollbackToSymlink restores a working symlink overlay after a failed fuse
// setup: unmount (verified), move private files back, re-link. If the unmount
// did not take, it stops there — laying symlinks "into" a live mirror would
// write them through to the real ~/.claude — and leaves recovery to the
// daemon's startup reconcile. The returned error always carries cause.
func (m *Manager) rollbackToSymlink(a store.Account, symProv, fuseProv fkoverlay.Provider, cause error) error {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := fuseProv.Teardown(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and rollback unmount failed: %w; private files remain in %s until the daemon reconciles)",
			a.ID, cause, err, priv)
	}
	// Restore private files into dir and sweep orphaned shared entries out to
	// base before re-linking. Both moves run regardless (disjoint name sets), but
	// Setup is sequenced AFTER them so it never lays links over an un-swept dir.
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
	removePrivateRootIfEmpty(priv)
	return fmt.Errorf("convert acct-%02d: %w (rolled back to symlink)", a.ID, cause)
}

// convertToSymlink turns a fuse account into a symlink one: unmount (verified
// — never lay links into a live mirror), move private files back beside the
// links, re-link, flip the row. With nothing mounted, the fuse provider's
// Teardown is an immediate no-op (RemoteFuseProvider contacts no holder), which
// is exactly the retreat path for a machine whose fuse rows outlived their
// mounts: the dir is already link-free and unmounted, so the retreat is pure
// file moves — in every build.
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
	// Relocate any shared entries claude wrote as real dirs/files into the bare
	// mountpoint while its mirror was force-unmounted: they sit at shared names
	// that Setup is about to symlink into base, so move them into base first or
	// assertSymlink refuses to clobber them and the retreat fails.
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
	removePrivateRootIfEmpty(priv)
	return a, nil
}

// removePrivateRootIfEmpty removes a fuse private backing dir once its private
// contents have been moved out. Anything still inside is data we did not
// classify — deleting it could destroy real user state, so a non-empty dir is
// deliberately left in place (inert; doctor does not flag it because it holds
// no private entries).
func removePrivateRootIfEmpty(priv string) {
	_ = os.Remove(filepath.Join(priv, ".DS_Store"))
	_ = os.Remove(priv)
}

// HealStrandedPrivate recovers a symlink account whose private files are
// stranded in a fuse private backing dir — the aftermath of a conversion
// interrupted before its rollback completed (or of a pre-fix mount fallback).
// It moves the files back into the account dir and re-asserts the symlink
// overlay, reporting whether anything was healed.
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
	removePrivateRootIfEmpty(priv)
	return true, nil
}

// SetDefaultOverlayKind records backend as the provider for accounts added
// later (the meta key ensureOverlayKind consults). Fuse is refused when this
// build cannot host fuse mounts (CanHostFuse): the RemoteFuseProvider always
// reports its own backend, so a provider-backend fence would always pass, while
// recording a default whose mount holder this machine cannot spawn would mint
// accounts whose rows promise a mirror their dirs don't have.
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

// ConfiguredOverlayKind returns the pool's recorded default overlay backend (the
// provider new accounts are minted with and that `ccp migrate` converts toward)
// and whether one has been recorded yet. Unlike ensureOverlayKind it is a pure
// read — it never detects or persists — so callers like `doctor` can compare an
// account's live backend against the configured default without side effects.
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
