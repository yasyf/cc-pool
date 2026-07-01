package pool

import (
	"context"
	"os"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// HolderOwner is cc-pool's tenant identity on the shared multi-tenant
// fusekit-holder: every mount cc-pool registers is tagged with it, so the daemon
// lists/reclaims only its own mounts and never disturbs another consumer's (e.g.
// cc-notes).
const HolderOwner = "cc-pool"

// cannotHostHint is the user-facing guidance appended to mountd.ErrCannotHost when
// the shared holder cannot be spawned (the cask is not installed). It points at
// the one-step setup command.
const cannotHostHint = "run `ccp fuse enable` to install the fusekit-holder cask"

// overlaySpec builds the fusekit/overlay Spec from cc-pool's classification and
// the shared-holder wiring: the per-account-private / excluded / shared / skipped
// entry sets (cc-pool POLICY, owned by internal/overlay), plus the content-mount
// seam onto the shared fusekit-holder. PassthroughOnly is false because cc-pool
// serves synthetic content (the merged /.claude.json), so its fuse backend is
// always fuse-t's NFS backend. The Holder points at the cask binary and the shared
// holder socket, and carries the CONTENT wiring (BridgeSocket/ContentMode/
// ProbePath/PrivatePrefixes) so RemoteFuseProvider.Setup registers a synth-serving
// mount over RPC rather than a passthrough. Every fusekit/overlay entry point
// (ProviderFor, Select, the migration primitives) takes this Spec.
func overlaySpec() fkoverlay.Spec {
	socket := mountd.DefaultHolderSocket()
	return fkoverlay.Spec{
		IsPrivate:       overlay.PrivateEntry,
		Excluded:        overlay.ExcludedEntries,
		Shared:          overlay.SharedEntries,
		Skip:            overlay.SkipEntries,
		PassthroughOnly: false,
		Holder: &fkoverlay.HolderSpec{
			Socket:         socket,
			LogPath:        MountHolderLogPath(),
			Args:           []string{"--socket", socket},
			ExecPath:       holderExe,
			Owner:          HolderOwner,
			CannotHostHint: cannotHostHint,
			SpawnTimeout:   mountd.DefaultSpawnTimeout,
			// Content wiring: the daemon's BridgeServer socket, the source content
			// mode, the wedge-probe path, and the private-name prefixes. With these
			// set, the provider's Setup registers an AddMount (synth over RPC), not a
			// passthrough. No Version: cc-pool must NEVER version-replace the shared
			// holder — that would tear down another tenant's mounts (the cask's
			// launchd owns the holder's lifecycle and upgrades).
			BridgeSocket:    BridgeSocketPath(),
			ContentMode:     "source",
			ProbePath:       "/" + overlay.ProbeFileName,
			PrivatePrefixes: overlay.PrivatePrefixes,
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

// holderExe is the cask holder binary CanHostFuse stats; a var (not the const
// mountd.HolderExe directly) so a hermetic test can point it at an absent/present
// path instead of depending on /Applications on the test machine.
var holderExe = mountd.HolderExe

// CanHostFuse reports whether this machine can host fuse mounts via the shared
// holder: the signed fusekit-holder cask is installed (mountd.HolderExe exists) or
// a holder is already serving the shared socket. Capability is no longer a
// build-tag property — a pure-Go cc-pool drives the cask holder, which is the fuse
// build — so this gates on the cask, not fusekit.Built().
func CanHostFuse() bool {
	if _, err := os.Stat(holderExe); err == nil {
		return true
	}
	return mountd.NewClient(mountd.DefaultHolderSocket()).Available()
}

// DetectOverlayBackend chooses the overlay backend for this machine via
// fusekit/overlay's Select: a fuse backend when the shared holder is reachable
// (auto-spawned from the cask) and its probe mount succeeds; else symlink. The
// probe runs in the holder, not here — mount capability and the macOS grant are
// per-process. A symlink verdict carries a generic human-readable reason from
// fusekit (no cc-pool CLI verbs); callers append their own `ccp ...` hints.
//
// A holder spawned here lingers after a symlink verdict: it keeps serving the
// socket with zero mounts (`ccp doctor` flags it as an orphan), and a later fuse
// Setup reuses it.
func DetectOverlayBackend(ctx context.Context) (fkoverlay.Backend, string) {
	_, backend, reason, _ := fkoverlay.Select(ctx, overlaySpec())
	return backend, reason
}
