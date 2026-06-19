package pool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
)

// overlayFor resolves a kind through the Manager's injectable seam.
func (m *Manager) overlayFor(kind overlay.Kind) overlay.Provider {
	if m.OverlayFor != nil {
		return m.OverlayFor(kind)
	}
	return OverlayProviderFor(kind)
}

// detectOverlay resolves overlay-kind detection through the Manager's
// injectable seam.
func (m *Manager) detectOverlay() (overlay.Kind, string) {
	if m.DetectOverlay != nil {
		return m.DetectOverlay()
	}
	return DetectOverlayKind()
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
// or target kind does not actually report that kind. The real resolver cannot
// produce this (KindFuse always maps to the holder-backed RemoteProvider,
// which always reports KindFuse), so the Kind() fences guard against
// wrong-kind INJECTED fakes — a conversion that *thinks* it is operating
// fuse-side while running symlink code paths is exactly how account state
// gets destroyed. It also fences fuse as the new-account default in builds
// that cannot host mounts (SetDefaultOverlayKind).
var ErrConvertUnsupported = errors.New("overlay kind unavailable")

// ConvertOverlay switches an account's overlay provider: it relocates the
// account's private files between the providers' private roots, tears down the
// old overlay, establishes the new one, and persists the row — in that order,
// with the row flip last, so an interrupted conversion always leaves a re-run
// that converges. The fuse direction mounts through the detached mount holder
// but still MUST run inside the daemon, which alone gates the conversion
// against live sessions and its own reservations. A failed fuse mount is
// rolled back to a byte-identical symlink overlay before returning.
// Converting to the kind the account already has is a no-op.
func (m *Manager) ConvertOverlay(a store.Account, to overlay.Kind) (store.Account, error) {
	from := overlay.Kind(a.OverlayKind)
	if from == to {
		return a, nil
	}
	fromProv, toProv := m.overlayFor(from), m.overlayFor(to)
	if fromProv.Kind() != from {
		return a, fmt.Errorf("convert acct-%02d: source %q: %w", a.ID, from, ErrConvertUnsupported)
	}
	if toProv.Kind() != to {
		return a, fmt.Errorf("convert acct-%02d: target %q: %w", a.ID, to, ErrConvertUnsupported)
	}
	switch to {
	case overlay.KindFuse:
		return m.convertToFuse(a, fromProv, toProv)
	case overlay.KindSymlink:
		return m.convertToSymlink(a, fromProv, toProv)
	default:
		// Unreachable: OverlayProviderFor maps unknown kinds to symlink, so
		// the Kind() equality fences above already rejected them.
		return a, fmt.Errorf("convert acct-%02d: unknown target %q", a.ID, to)
	}
}

// convertToFuse turns a symlink account into a fuse one: private files move to
// the sibling backing dir, the links come down, the mirror mounts over the
// (now link-free) account dir, and only then does the row flip.
func (m *Manager) convertToFuse(a store.Account, symProv, fuseProv overlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := overlay.FusePrivateRoot(dir)
	if overlay.Mounted(dir) {
		return a, fmt.Errorf("convert acct-%02d: %s is already a mountpoint but the row says %s; refusing", a.ID, dir, a.OverlayKind)
	}

	// The identity to re-verify through the mount. An account that never
	// completed a login legitimately has none.
	pre, preErr := readIdentity(filepath.Join(dir, ".claude.json"))
	if preErr != nil && !errors.Is(preErr, ErrNoIdentity) {
		return a, fmt.Errorf("convert acct-%02d: read identity before conversion: %w", a.ID, preErr)
	}

	if err := overlay.MovePrivateEntries(dir, priv); err != nil {
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

	if err := m.Store.SetAccountOverlayKind(a.ID, string(overlay.KindFuse)); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, fmt.Errorf("persist row: %w", err))
	}
	a.OverlayKind = string(overlay.KindFuse)
	return a, nil
}

// rollbackToSymlink restores a working symlink overlay after a failed fuse
// setup: unmount (verified), move private files back, re-link. If the unmount
// did not take, it stops there — laying symlinks "into" a live mirror would
// write them through to the real ~/.claude — and leaves recovery to the
// daemon's startup reconcile. The returned error always carries cause.
func (m *Manager) rollbackToSymlink(a store.Account, symProv, fuseProv overlay.Provider, cause error) error {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := overlay.FusePrivateRoot(dir)
	if err := fuseProv.Teardown(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and rollback unmount failed: %v; private files remain in %s until the daemon reconciles)",
			a.ID, cause, err, priv)
	}
	if err := errors.Join(
		overlay.MovePrivateEntries(priv, dir),
		symProv.Setup(base, dir),
	); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and symlink rollback failed: %v)", a.ID, cause, err)
	}
	removePrivateRootIfEmpty(priv)
	return fmt.Errorf("convert acct-%02d: %w (rolled back to symlink)", a.ID, cause)
}

// convertToSymlink turns a fuse account into a symlink one: unmount (verified
// — never lay links into a live mirror), move private files back beside the
// links, re-link, flip the row. With nothing mounted, the fuse provider's
// Teardown is an immediate no-op (RemoteProvider contacts no holder), which is
// exactly the retreat path for a machine whose fuse rows outlived their
// mounts: the dir is already link-free and unmounted, so the retreat is pure
// file moves — in every build.
func (m *Manager) convertToSymlink(a store.Account, fuseProv, symProv overlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := overlay.FusePrivateRoot(dir)
	if err := fuseProv.Teardown(base, dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: unmount: %w", a.ID, err)
	}
	if _, err := os.Stat(priv); err == nil {
		if err := overlay.MovePrivateEntries(priv, dir); err != nil {
			return a, fmt.Errorf("convert acct-%02d: restore private files: %w", a.ID, err)
		}
	} else if !os.IsNotExist(err) {
		return a, fmt.Errorf("convert acct-%02d: stat private root: %w", a.ID, err)
	}
	if err := symProv.Setup(base, dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: lay symlinks: %w", a.ID, err)
	}
	if err := m.Store.SetAccountOverlayKind(a.ID, string(overlay.KindSymlink)); err != nil {
		return a, fmt.Errorf("convert acct-%02d: persist row: %w", a.ID, err)
	}
	a.OverlayKind = string(overlay.KindSymlink)
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
	if overlay.Kind(a.OverlayKind) == overlay.KindFuse {
		return false, fmt.Errorf("heal acct-%02d: account is fuse-kind; its private root is in use, not stranded", a.ID)
	}
	dir := a.ConfigDir
	priv := overlay.FusePrivateRoot(dir)
	has, err := overlay.HasPrivateEntries(priv)
	if err != nil {
		return false, fmt.Errorf("heal acct-%02d: %w", a.ID, err)
	}
	if !has {
		return false, nil
	}
	if overlay.Mounted(dir) {
		return false, fmt.Errorf("heal acct-%02d: %s is a live mountpoint but the row says symlink; refusing to move files under a mirror", a.ID, dir)
	}
	if err := errors.Join(
		overlay.MovePrivateEntries(priv, dir),
		(&overlay.SymlinkProvider{}).Setup(ClaudeDir(), dir),
	); err != nil {
		return false, fmt.Errorf("heal acct-%02d: %w", a.ID, err)
	}
	removePrivateRootIfEmpty(priv)
	return true, nil
}

// SetDefaultOverlayKind records kind as the provider for accounts added later
// (the meta key ensureOverlayKind consults). Fuse is refused when this build
// cannot host fuse mounts (CanHostFuse): the RemoteProvider always reports
// KindFuse, so a provider-kind fence would always pass, while recording a
// default whose mount holder this machine cannot spawn would mint accounts
// whose rows promise a mirror their dirs don't have.
func (m *Manager) SetDefaultOverlayKind(kind overlay.Kind) error {
	switch kind {
	case overlay.KindSymlink:
	case overlay.KindFuse:
		if !m.canHostFuse() {
			return fmt.Errorf("set default overlay %q: this build cannot host fuse mounts — install fuse-t (brew install macos-fuse-t/cask/fuse-t), then brew reinstall cc-pool: %w", kind, ErrConvertUnsupported)
		}
	default:
		return fmt.Errorf("set default overlay: unknown kind %q", kind)
	}
	if err := m.Store.SetMeta(metaOverlayKind, string(kind)); err != nil {
		return fmt.Errorf("set default overlay: %w", err)
	}
	return nil
}

// ConfiguredOverlayKind returns the pool's recorded default overlay kind (the
// provider new accounts are minted with and that `ccp migrate` converts toward)
// and whether one has been recorded yet. Unlike ensureOverlayKind it is a pure
// read — it never detects or persists — so callers like `doctor` can compare an
// account's live kind against the configured default without side effects.
func (m *Manager) ConfiguredOverlayKind() (overlay.Kind, bool, error) {
	v, ok, err := m.Store.GetMeta(metaOverlayKind)
	if err != nil {
		return "", false, fmt.Errorf("read default overlay kind: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	return overlay.Kind(v), true, nil
}
