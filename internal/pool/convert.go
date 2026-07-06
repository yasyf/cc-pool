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

// ErrDirIsOverlaySymlink means a symlink-row file move found the account dir to be
// a symlink (a mux bridge, a File Provider domain bridge, or any overlay stand-in)
// rather than the real directory a symlink row must hold. Moving files through it
// would traverse a live mirror or domain — the exact loss that destroyed three
// accounts' identities when a crashed convert left an FP-bridge symlink behind. Every
// guarded flow refuses it, naming the link target, and moves nothing.
var ErrDirIsOverlaySymlink = errors.New("account dir is an overlay symlink, not a real directory")

// ErrIdentityLost means a rollback's restore move completed but the account's
// identity is not readable at its row-implied .claude.json afterward — the
// divergence the fresher-wins resolver could cause. Its message names the recovery
// sources in order so the identity can be restored by hand.
var ErrIdentityLost = errors.New("account identity lost after rollback")

// requireRealDir fails with ErrDirIsOverlaySymlink (naming the link target) when dir
// is a symlink, so no symlink-row file move ever traverses a live domain or mirror.
// Lstat never follows the link, so it cannot hang on a wedged mount. A real dir, an
// absent dir, or a non-symlink stat error passes through — the caller's own move
// surfaces a genuine fault.
func requireRealDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return nil
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		target, rerr := os.Readlink(dir)
		if rerr != nil {
			target = fmt.Sprintf("<unreadable: %v>", rerr)
		}
		return fmt.Errorf("%w: %s -> %s", ErrDirIsOverlaySymlink, dir, target)
	}
	return nil
}

// verifyIdentityRestored confirms a rollback's restore move landed a readable
// identity back at dir/.claude.json; pre is the identity read before the conversion
// (nil when the account had none, so there is nothing to restore and nothing to
// verify). It checks readability, NOT that the bytes match pre: the rollback
// faithfully restores whatever the private root held, and a genuinely divergent
// identity there is the convert's own reported cause, preserved for inspection —
// never destroyed. An UNREADABLE identity (absent or corrupt) is the loss this
// guards against — the fresher-wins EXDEV window that left the identity gone from
// every read path — so the error names, in recovery order, where a surviving copy
// may be found: the resolver's conflict siblings, the private-root backups, then a
// fresh login.
func verifyIdentityRestored(a store.Account, dir string, pre *Identity) error {
	if pre == nil {
		return nil
	}
	_, err := readIdentity(filepath.Join(dir, ".claude.json"))
	if err == nil {
		return nil
	}
	priv := fkoverlay.FusePrivateRoot(dir)
	return fmt.Errorf("%w: acct-%02d %v; recover it, in order, from %s/.claude.json.conflict-* or %s/.claude.json.conflict-* → %s/.claude.json.backup.* → re-run `claude /login` for this account",
		ErrIdentityLost, a.ID, err, dir, priv, filepath.Join(priv, "backups"))
}

// fpProbe classifies the account dir's File Provider domain verdict through the
// companion app's control op (never a through-domain read). Setup (fusekit)
// already blocks until the domain enumerates; this proves the bridge data plane
// end to end before the row flips — the readiness the FP-migrate-storm incident
// lacked. The daemon injects Manager.FPProbe; otherwise it derives the probe from
// the freshly-registered target provider, which exposes ProbeDomain. A NoVerdict
// (app busy/unreachable/too old) is a non-nil error, so the convert gate rolls
// back rather than flipping an unverified row.
func (m *Manager) fpProbe(ctx context.Context, fpProv fkoverlay.Provider, accountDir string) error {
	if m.FPProbe != nil {
		return m.FPProbe(ctx, accountDir)
	}
	prober, ok := fpProv.(overlay.FPDomainProber)
	if !ok {
		return fmt.Errorf("%w: provider %T lacks the app control-op probe", overlay.ErrFPProbeNoVerdict, fpProv)
	}
	return overlay.FPDomainProbe(ctx, prober, accountDir)
}

// ConvertOverlay switches an account's overlay provider, persisting the row last so
// an interrupted run re-converges. MUST run inside the daemon, which alone gates
// against live sessions; a failed conversion rolls back to the source backend's
// shape. ctx bounds the fuse- and file-provider-side conversions: callers detach
// it from request cancellation (context.WithoutCancel) so a daemon shutdown
// mid-conversion finishes or rolls back instead of abandoning a half-converted
// account.
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
	switch {
	case to.IsFuse():
		// A File Provider source never enters convertToFuse: its early steps
		// (readIdentity, MovePrivateEntries) would traverse the account-dir
		// symlink INTO the domain — an unbounded read through the appex.
		if from == fkoverlay.BackendFileProvider {
			return m.convertFileProviderToFuse(ctx, a, fromProv, toProv)
		}
		return m.convertToFuse(ctx, a, fromProv, toProv)
	case to == fkoverlay.BackendSymlink:
		return m.convertToSymlink(a, fromProv, toProv)
	case to == fkoverlay.BackendFileProvider:
		return m.convertToFileProvider(ctx, a, fromProv, toProv)
	default:
		return a, fmt.Errorf("convert acct-%02d: no conversion arm for target backend %q", a.ID, to)
	}
}

func (m *Manager) convertToFuse(ctx context.Context, a store.Account, symProv, fuseProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	// ROOT GUARD: a symlink row must hold a REAL account dir. A symlink here (a mux
	// bridge into the shared mirror, or a File Provider domain bridge a crashed
	// convert left behind) would make MovePrivateEntries below write THROUGH the
	// live mirror/domain — the identity-loss window this pass closes. Lstat never
	// follows the link, so it precedes the mount check (which would). Refuse, naming
	// the target, and move nothing.
	if err := requireRealDir(dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: row says %s: %w", a.ID, a.OverlayKind, err)
	}
	if overlay.Mounted(dir) {
		return a, fmt.Errorf("convert acct-%02d: %s is already a mountpoint but the row says %s; refusing", a.ID, dir, a.OverlayKind)
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
		return a, m.rollbackToSymlink(a, symProv, fuseProv, pre, fmt.Errorf("move private files: %w", err))
	}
	if err := symProv.Teardown(base, dir); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, pre, fmt.Errorf("tear down symlinks: %w", err))
	}
	// A spent budget must not start a mount it has no time to verify.
	if err := ctx.Err(); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, pre, err)
	}
	if err := fuseProv.Setup(base, dir); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, pre, fmt.Errorf("mount: %w", err))
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
			return a, m.rollbackToSymlink(a, symProv, fuseProv, pre, fmt.Errorf("identity not readable in private root after move: %w", err))
		}
		if post.AccountUUID != pre.AccountUUID {
			return a, m.rollbackToSymlink(a, symProv, fuseProv, pre,
				fmt.Errorf("identity in private root is %s, expected %s", post.AccountUUID, pre.AccountUUID))
		}
	}

	if err := m.Store.SetAccountOverlayKind(a.ID, string(fuseProv.Backend())); err != nil {
		return a, m.rollbackToSymlink(a, symProv, fuseProv, pre, fmt.Errorf("persist row: %w", err))
	}
	a.OverlayKind = string(fuseProv.Backend())
	return a, nil
}

// rollbackToSymlink restores a symlink overlay after a failed fuse setup. If the
// unmount does not take it stops — laying symlinks into a live mirror would write
// through to the real ~/.claude — leaving recovery to the daemon's reconcile.
func (m *Manager) rollbackToSymlink(a store.Account, symProv, fuseProv fkoverlay.Provider, pre *Identity, cause error) error {
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
	// IDENTITY INVARIANT: the restore moved the identity back to dir/.claude.json;
	// verify it survived intact before reporting a clean rollback. A miss surfaces
	// the recovery sources rather than a silent "rolled back".
	if err := verifyIdentityRestored(a, dir, pre); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (rollback re-asserted the symlink overlay but %w)", a.ID, cause, err)
	}
	return fmt.Errorf("convert acct-%02d: %w (rolled back to symlink)", a.ID, cause)
}

// convertToFileProvider switches an account onto the File Provider overlay,
// leaving the account dir a symlink into the OS-surfaced domain root. Two
// source shapes, split by the mux cutover: a symlink row holds a REAL dir —
// drain its private files into the shared backing root, tear the links down,
// and remove the emptied dir so Setup's fail-closed AtomicSymlink can lay the
// bridge (anything unclassified left inside fails that removal ENOTEMPTY and
// rolls back, never clobbered — the same stance as the mux cutover's
// clearAccountDirForBridge) — while a post-mux fuse row is ALREADY a bridge
// symlink with its private files in the backing root, so the fuse Teardown
// detaches the subtree and Setup retargets the symlink (a real dir on a fuse
// row is legacy wreckage AtomicSymlink refuses). Identity is verified from the
// private BACKING file, never through the fresh domain: a through-domain read
// is unbounded while the appex materializes, and the domain serves that exact
// backing file anyway. Every failure past the first move routes through a
// rollback restoring the source shape; the row flips last.
func (m *Manager) convertToFileProvider(ctx context.Context, a store.Account, fromProv, fpProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	fromFuse := fromProv.Backend().IsFuse()
	// ROOT GUARD (symlink source only): a symlink row must hold a REAL account dir;
	// a symlink there (a mux bridge, or a File Provider domain bridge a crashed
	// convert left behind) would make MovePrivateEntries below drain files THROUGH
	// the live mirror/domain — the identity-loss window this pass closes. Lstat
	// never follows the link, so it precedes the mount check (which would). A fuse
	// source's dir is legitimately a bridge symlink and its arm moves nothing, so it
	// is exempt.
	if !fromFuse {
		if err := requireRealDir(dir); err != nil {
			return a, fmt.Errorf("convert acct-%02d: row says %s: %w", a.ID, a.OverlayKind, err)
		}
	}
	if overlay.Mounted(dir) {
		return a, fmt.Errorf("convert acct-%02d: %s is a live mountpoint; refusing to convert while anything is mounted there", a.ID, dir)
	}

	// The identity that must survive the conversion: a symlink row holds it in
	// the account dir (about to move), a fuse row already in the backing root.
	// An account that never completed a login legitimately has no identity.
	identityAt := dir
	if fromFuse {
		identityAt = priv
	}
	pre, preErr := readIdentity(filepath.Join(identityAt, ".claude.json"))
	if preErr != nil && !errors.Is(preErr, ErrNoIdentity) {
		return a, fmt.Errorf("convert acct-%02d: read identity before conversion: %w", a.ID, preErr)
	}
	// Nothing changed yet: a spent budget aborts cleanly, no rollback needed.
	if err := ctx.Err(); err != nil {
		return a, fmt.Errorf("convert acct-%02d: %w", a.ID, err)
	}

	if fromFuse {
		// Detach the subtree. The mux teardown retracts the bridge symlink
		// fail-closed (RemoveSymlink refuses a real dir), so nothing is
		// half-done on failure: plain error, no rollback, files never moved.
		if err := fromProv.Teardown(base, dir); err != nil {
			return a, fmt.Errorf("convert acct-%02d: tear down fuse overlay: %w", a.ID, err)
		}
	} else {
		// STRAND WINDOW: from here until SetAccountOverlayKind the private files
		// live in priv while the row still says symlink; every error return below
		// must go through a rollback, or the account is stranded until
		// HealStrandedPrivate — the recovery of last resort — finds it.
		if err := fkoverlay.MovePrivateEntries(dir, priv, m.overlaySpec()); err != nil {
			return a, m.rollbackFileProviderToSymlink(a, fromProv, fpProv, pre, fmt.Errorf("move private files: %w", err))
		}
		if err := fromProv.Teardown(base, dir); err != nil {
			return a, m.rollbackFileProviderToSymlink(a, fromProv, fpProv, pre, fmt.Errorf("tear down symlinks: %w", err))
		}
		if err := os.Remove(dir); err != nil {
			return a, m.rollbackFileProviderToSymlink(a, fromProv, fpProv, pre, fmt.Errorf("remove drained account dir: %w", err))
		}
	}
	rollback := func(cause error) error {
		if fromFuse {
			return m.rollbackFileProviderToFuse(a, fromProv, fpProv, cause)
		}
		return m.rollbackFileProviderToSymlink(a, fromProv, fpProv, pre, cause)
	}

	// A spent budget must not register a domain it has no time to verify.
	if err := ctx.Err(); err != nil {
		return a, rollback(err)
	}
	if err := fpProv.Setup(base, dir); err != nil {
		return a, rollback(fmt.Errorf("register domain: %w", err))
	}

	// An identity-bearing account: prove the domain serves reads and the moved
	// identity is intact before the row flips. Identity-less accounts skip both —
	// FPFS skips fetchContents at size 0, so a Missing/Empty domain read is
	// expected and benign there, and there is no identity to verify.
	if preErr == nil {
		// Setup proved the appex enumerator; this proves the bridge data plane
		// over the app control op — the readiness the wedge incident lacked. A
		// NoVerdict (busy/unreachable/too old) rolls back too: never flip an
		// unverified row.
		if err := m.fpProbe(ctx, fpProv, dir); err != nil {
			return a, rollback(fmt.Errorf("domain registered but does not serve reads: %w", err))
		}
		post, err := readIdentity(filepath.Join(priv, ".claude.json"))
		if err != nil {
			return a, rollback(fmt.Errorf("identity not readable in private root after conversion: %w", err))
		}
		if post.AccountUUID != pre.AccountUUID {
			return a, rollback(fmt.Errorf("identity in private root is %s, expected %s", post.AccountUUID, pre.AccountUUID))
		}
	}

	if err := m.Store.SetAccountOverlayKind(a.ID, string(fkoverlay.BackendFileProvider)); err != nil {
		return a, rollback(fmt.Errorf("persist row: %w", err))
	}
	a.OverlayKind = string(fkoverlay.BackendFileProvider)
	return a, nil
}

// retractFileProviderIfLaid undoes whatever a failed File Provider Setup laid at
// the account dir. An absent path or a symlink takes the full Teardown (retract the
// link AND deregister the domain — deregistering a never-registered domain is a
// no-op). A REAL dir means Setup never swapped the bridge symlink in — Teardown's
// fail-closed RemoveSymlink would refuse it — but Setup may still have registered
// the domain before AtomicSymlink refused the real dir (the fuse→FP legacy shape),
// which returning nil here leaked forever. So on a real dir it deregisters the
// domain, but ONLY when the zero-spawn registration check finds one: a rollback
// that never reached Setup (a drain/teardown fault) leaves no registration and must
// touch nothing — never spawn the app to deregister a domain that was never laid.
func retractFileProviderIfLaid(base, dir string, fpProv fkoverlay.Provider) error {
	fi, err := os.Lstat(dir)
	if err != nil || fi.Mode()&os.ModeSymlink != 0 {
		return fpProv.Teardown(base, dir)
	}
	registry, ok := fpProv.(overlay.FPDomainRegistry)
	remover, isRemover := fpProv.(overlay.FPDomainRemover)
	if !ok || !isRemover {
		return fmt.Errorf("convert: provider %T cannot deregister a leaked file provider domain: %w", fpProv, ErrConvertUnsupported)
	}
	if _, err := registry.DomainRoot(context.Background(), dir); err != nil {
		return nil // no registration (ErrNoDomain) or app down (ErrAppUnavailable): nothing laid to retract
	}
	return remover.RemoveDomain(dir)
}

// rollbackFileProviderToSymlink restores the symlink overlay after a failed
// symlink→fileprovider conversion. If retracting what Setup laid fails it stops
// — moving private files back through a live domain symlink would write into
// the domain — leaving them in the backing root for HealStrandedPrivate.
func (m *Manager) rollbackFileProviderToSymlink(a store.Account, symProv, fpProv fkoverlay.Provider, pre *Identity, cause error) error {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := retractFileProviderIfLaid(base, dir, fpProv); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and rollback teardown failed: %w; private files remain in %s until the daemon reconciles)",
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
	// IDENTITY INVARIANT: the restore moved the identity back to dir/.claude.json;
	// verify it survived before reporting a clean rollback, else name the recovery
	// sources.
	if err := verifyIdentityRestored(a, dir, pre); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (rollback re-asserted the symlink overlay but %w)", a.ID, cause, err)
	}
	return fmt.Errorf("convert acct-%02d: %w (rolled back to symlink)", a.ID, cause)
}

// rollbackFileProviderToFuse restores the fuse overlay after a failed
// fuse→fileprovider conversion: retract whatever Setup laid, then let the fuse
// Setup re-attach the subtree and re-lay its own bridge symlink. The private
// files never moved (both backends share the backing root), so a failure here
// leaves them intact for the daemon's fuse reconcile.
func (m *Manager) rollbackFileProviderToFuse(a store.Account, fuseProv, fpProv fkoverlay.Provider, cause error) error {
	base, dir := ClaudeDir(), a.ConfigDir
	if err := retractFileProviderIfLaid(base, dir, fpProv); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and rollback teardown failed: %w; the daemon's reconcile will remount the %s row)",
			a.ID, cause, err, a.OverlayKind)
	}
	if err := fuseProv.Setup(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and fuse rollback failed: %w)", a.ID, cause, err)
	}
	return fmt.Errorf("convert acct-%02d: %w (rolled back to %s)", a.ID, cause, a.OverlayKind)
}

// convertFileProviderToFuse turns a File Provider account into a fuse one.
// Nothing moves: both backends keep private files in the same backing root, so
// the FP Teardown retracts the account-dir symlink and deregisters the domain,
// the fuse Setup lays its own bridge symlink over the vacated path, and the
// identity is verified untouched in the backing root — never through a mount —
// before the row flips.
func (m *Manager) convertFileProviderToFuse(ctx context.Context, a store.Account, fpProv, fuseProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)

	pre, preErr := readIdentity(filepath.Join(priv, ".claude.json"))
	if preErr != nil && !errors.Is(preErr, ErrNoIdentity) {
		return a, fmt.Errorf("convert acct-%02d: read identity before conversion: %w", a.ID, preErr)
	}
	// Nothing changed yet: a spent budget aborts cleanly.
	if err := ctx.Err(); err != nil {
		return a, fmt.Errorf("convert acct-%02d: %w", a.ID, err)
	}
	// Fail-closed on both arms (RemoveSymlink refuses a real dir), so a failure
	// here leaves the FP row consistent for the daemon's reconcile: plain error.
	if err := fpProv.Teardown(base, dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: retract file provider overlay: %w", a.ID, err)
	}
	// A spent budget must not start a mount it has no time to verify.
	if err := ctx.Err(); err != nil {
		return a, m.rollbackToFileProvider(a, fuseProv, fpProv, err)
	}
	if err := fuseProv.Setup(base, dir); err != nil {
		return a, m.rollbackToFileProvider(a, fuseProv, fpProv, fmt.Errorf("mount: %w", err))
	}
	if preErr == nil {
		post, err := readIdentity(filepath.Join(priv, ".claude.json"))
		if err != nil {
			return a, m.rollbackToFileProvider(a, fuseProv, fpProv, fmt.Errorf("identity not readable in private root after conversion: %w", err))
		}
		if post.AccountUUID != pre.AccountUUID {
			return a, m.rollbackToFileProvider(a, fuseProv, fpProv,
				fmt.Errorf("identity in private root is %s, expected %s", post.AccountUUID, pre.AccountUUID))
		}
	}
	if err := m.Store.SetAccountOverlayKind(a.ID, string(fuseProv.Backend())); err != nil {
		return a, m.rollbackToFileProvider(a, fuseProv, fpProv, fmt.Errorf("persist row: %w", err))
	}
	a.OverlayKind = string(fuseProv.Backend())
	return a, nil
}

// rollbackToFileProvider restores the File Provider overlay after a failed
// fileprovider→fuse conversion. The fuse teardown is fail-closed (it refuses a
// real dir) and a no-op with nothing mounted; if it fails, it stops — re-laying
// the domain symlink over live fuse state would divert the account — leaving
// the fuse wreckage for the daemon to reconcile against the fileprovider row.
func (m *Manager) rollbackToFileProvider(a store.Account, fuseProv, fpProv fkoverlay.Provider, cause error) error {
	base, dir := ClaudeDir(), a.ConfigDir
	if err := fuseProv.Teardown(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and rollback unmount failed: %w)", a.ID, cause, err)
	}
	if err := fpProv.Setup(base, dir); err != nil {
		return fmt.Errorf("convert acct-%02d: %w (and file provider rollback failed: %w)", a.ID, cause, err)
	}
	return fmt.Errorf("convert acct-%02d: %w (rolled back to fileprovider)", a.ID, cause)
}

// convertToSymlink turns a fuse or File Provider account into a symlink one.
// The source Teardown vacates the account dir without crossing it (with nothing
// mounted the fuse teardown is a no-op; the FP teardown retracts the domain
// symlink and deregisters the domain), so even a build that cannot host the
// source backend can retreat from a stale row — pure file moves.
func (m *Manager) convertToSymlink(a store.Account, fromProv, symProv fkoverlay.Provider) (store.Account, error) {
	base, dir := ClaudeDir(), a.ConfigDir
	priv := fkoverlay.FusePrivateRoot(dir)
	spec := m.overlaySpec()
	if err := fromProv.Teardown(base, dir); err != nil {
		return a, fmt.Errorf("convert acct-%02d: tear down %s overlay: %w", a.ID, fromProv.Backend(), err)
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
	// Only a symlink row can strand: fuse AND fileprovider rows keep their
	// private root in active use, and healing one would move live files.
	if backend != fkoverlay.BackendSymlink {
		return false, fmt.Errorf("heal acct-%02d: account is %s-backed; its private root is in use, not stranded", a.ID, backend)
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
	// ROOT GUARD: the stranded files move back INTO dir, so dir must be the real
	// account directory. A symlink there (a mux bridge, or a File Provider domain
	// bridge a crashed convert left behind) would send MovePrivateEntries writing
	// through the live mirror/domain. Lstat never follows the link, so this precedes
	// the mount check (which would). Refuse loudly for `ccp doctor` rather than
	// corrupt the mirror; the stranded copy stays intact.
	if err := requireRealDir(dir); err != nil {
		return false, fmt.Errorf("heal acct-%02d: %w — run `ccp doctor`", a.ID, err)
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
	removePrivateRootIfEmpty(priv, spec)
	return true, nil
}

// SetDefaultOverlayKind records backend as the default for accounts added later. Fuse
// is refused when this build cannot host mounts, else new accounts' rows would promise
// a mirror their dirs cannot have. File Provider is recorded as-is: its availability
// (extension enabled, companion app reachable) is gated at the migrate entry points
// (the daemon's fpGate, the CLI precheck), which run before this is reached.
func (m *Manager) SetDefaultOverlayKind(backend fkoverlay.Backend) error {
	switch {
	case backend == fkoverlay.BackendSymlink, backend == fkoverlay.BackendFileProvider:
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
