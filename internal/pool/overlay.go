package pool

import (
	"context"
	"os"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// HolderOwner tags cc-pool's mounts on the shared fusekit-holder so the daemon
// reclaims only its own.
const HolderOwner = "cc-pool"

// cannotHostHint is appended to mountd.ErrCannotHost when the holder cask is absent.
const cannotHostHint = "run `ccp fuse enable` to install the fusekit-holder cask"

// overlaySpec builds cc-pool's fusekit/overlay Spec. PassthroughOnly is false
// because cc-pool serves synthetic content (the merged /.claude.json), forcing
// fuse-t's NFS backend.
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
			// No Version: cc-pool must NEVER version-replace the shared holder — that
			// tears down another tenant's mounts (the cask's launchd owns holder
			// upgrades).
			BridgeSocket:    BridgeSocketPath(),
			ContentMode:     "source",
			ProbePath:       "/" + overlay.ProbeFileName,
			PrivatePrefixes: overlay.PrivatePrefixes,
		},
	}
}

func (m *Manager) overlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlaySpec exposes cc-pool's fusekit/overlay Spec to packages that drive the
// migration primitives directly.
func (m *Manager) OverlaySpec() fkoverlay.Spec { return overlaySpec() }

// OverlayProviderFor returns the fusekit/overlay provider for a stored backend,
// wired with cc-pool's Spec; a bad backend fails loud.
func OverlayProviderFor(b fkoverlay.Backend) (fkoverlay.Provider, error) {
	return fkoverlay.ProviderFor(b, overlaySpec())
}

// holderExe is the cask holder binary CanHostFuse stats; a var so tests can point
// it at an absent/present path.
var holderExe = mountd.HolderExe

// CanHostFuse reports whether this machine can host fuse mounts via the shared
// holder: the cask is installed or a holder already serves the socket.
func CanHostFuse() bool {
	if _, err := os.Stat(holderExe); err == nil {
		return true
	}
	return mountd.NewClient(mountd.DefaultHolderSocket()).Available()
}

// DetectOverlayBackend picks fuse via fusekit/overlay's Select when the shared
// holder's probe mount succeeds, else symlink. The probe runs in the holder (the
// macOS mount grant is per-process); a symlink verdict can leave an orphan holder.
func DetectOverlayBackend(ctx context.Context) (fkoverlay.Backend, string) {
	_, backend, reason, _ := fkoverlay.Select(ctx, overlaySpec())
	return backend, reason
}
