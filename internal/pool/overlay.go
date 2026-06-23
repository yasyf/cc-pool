package pool

import (
	"context"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// overlaySpec builds the fusekit/overlay Spec from cc-pool's classification and
// holder wiring: the per-account-private / excluded / shared / skipped entry
// sets (cc-pool POLICY, owned by internal/overlay), plus the detached
// mount-holder seam. PassthroughOnly is false because cc-pool's mirror serves
// synthetic content (the merged /.claude.json), so FuseBackend always lands on
// fuse-t's NFS backend. Every fusekit/overlay entry point (ProviderFor, Select,
// the migration primitives) takes this Spec.
func overlaySpec() fkoverlay.Spec {
	return fkoverlay.Spec{
		IsPrivate:       overlay.PrivateEntry,
		Excluded:        overlay.ExcludedEntries,
		Shared:          overlay.SharedEntries,
		Skip:            overlay.SkipEntries,
		PassthroughOnly: false,
		Holder: &fkoverlay.HolderSpec{
			Socket:         MountsSocketPath(),
			LogPath:        MountHolderLogPath(),
			Args:           []string{"mount-holder", "--socket", MountsSocketPath()},
			CannotHostHint: cannotHostHint,
			StableExecDir:  HolderBinDir(),
			// No Version: cc-pool drives holder version-skew convergence through the
			// fusekit/proc Supervisor (the daemon's holder supervision, fed
			// MyVersion at daemon startup), NOT through mountd.RemoteHost.Converge —
			// which cc-pool never calls. RemoteHost reads Version only in Converge,
			// so setting it here would be inert at best and a double-converge footgun
			// if that path were ever invoked alongside the proc Supervisor's.
			SpawnTimeout: mountd.DefaultSpawnTimeout,
		},
	}
}

// overlaySpec is the package-level spec builder; the Manager method delegates to
// it so seams and tests share one definition.
func (m *Manager) overlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlaySpec exposes cc-pool's fusekit/overlay Spec to other packages (the
// daemon's mount-sweep path) that drive the migration primitives directly.
func (m *Manager) OverlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlayProviderFor returns the fusekit/overlay provider for a stored backend,
// wired with cc-pool's Spec. It never silently substitutes backends: a fuse
// backend maps to the holder-backed RemoteFuseProvider (which always reports its
// own backend, even in a build that could not host the mounts itself); symlink
// maps to the symlink provider. A bad stored backend fails loud.
func OverlayProviderFor(b fkoverlay.Backend) (fkoverlay.Provider, error) {
	return fkoverlay.ProviderFor(b, overlaySpec())
}

// FuseBackend is the fuse backend cc-pool realizes for its mirror — fusekit
// derives it from the Spec (PassthroughOnly=false → always NFS). Callers that
// need "the fuse provider" without a stored row (the daemon's mount/heal paths,
// the `ccp migrate --to fuse` mapping) resolve through this rather than naming a
// concrete backend, so cc-pool never branches nfs-vs-fskit itself.
func FuseBackend() fkoverlay.Backend { return fkoverlay.FuseBackend(overlaySpec()) }

// CanHostFuse reports whether THIS binary can host fuse mounts (built with
// -tags fuse). A running holder spawned from a fuse build is usable by any
// build regardless.
func CanHostFuse() bool { return fusekit.Built() }

// DetectOverlayBackend chooses the overlay backend for this machine via
// fusekit/overlay's Select: a fuse backend when this build can host fuse mounts,
// a mount holder is reachable (auto-spawned), and the holder's probe mount
// succeeds; else symlink. The probe runs in the holder, not here — mount
// capability and the macOS grant are per-process. A symlink verdict carries a
// generic human-readable reason from fusekit (no cc-pool CLI verbs); callers
// append their own `ccp ...` hints at the edge.
//
// A holder spawned here lingers after a symlink verdict: it keeps serving the
// socket with zero mounts (supervision never retires a same-version idle
// holder; `ccp doctor` flags it as an orphan and `ccp service uninstall` stops
// it), and a later fuse Setup reuses it.
func DetectOverlayBackend() (fkoverlay.Backend, string) {
	_, backend, reason, _ := fkoverlay.Select(context.Background(), overlaySpec())
	return backend, reason
}
