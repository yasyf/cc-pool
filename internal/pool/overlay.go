package pool

import (
	"fmt"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
)

// OverlayProviderFor returns the provider for a stored overlay kind. It is
// the pool's one resolver and never silently substitutes kinds: KindFuse maps
// to the mount-holder-backed remoteFuse adapter (which always reports KindFuse,
// even in a build that could not host the mounts itself); everything else
// maps to the symlink provider.
func OverlayProviderFor(kind overlay.Kind) overlay.Provider {
	if kind == overlay.KindFuse {
		return newRemoteFuse()
	}
	return &overlay.SymlinkProvider{}
}

// CanHostFuse reports whether THIS binary can host fuse mounts (built with
// -tags fuse). A running holder spawned from a fuse build is usable by any
// build regardless.
func CanHostFuse() bool { return overlay.FuseBuilt() }

// DetectOverlayKind chooses the overlay kind for this machine: fuse when this
// build can host fuse mounts (CanHostFuse), a mount holder is reachable
// (auto-spawned), and the holder's probe mount succeeds; else symlink. A build
// that cannot host mounts gets the symlink verdict without probing — even a
// reachable leftover holder is deliberately not adopted, because the recorded
// default must survive that holder's death, the same policy
// SetDefaultOverlayKind enforces. The probe MUST run in the holder, not here —
// mount capability and the macOS "Network Volumes" TCC grant are per-process,
// and the holder is the process that will host the mounts. A symlink verdict
// carries a human-readable reason saying why fuse was ruled out; a fuse
// verdict carries "".
//
// A holder spawned here lingers after a symlink verdict: it keeps serving the
// socket with zero mounts (supervision never retires a same-version idle
// holder; `ccp doctor` flags it as an orphan and `ccp service uninstall`
// stops it), and a later fuse Setup reuses it.
func DetectOverlayKind() (overlay.Kind, string) {
	if !CanHostFuse() {
		return overlay.KindSymlink, "this build cannot host fuse mounts; install fuse-t (brew install macos-fuse-t/cask/fuse-t), then brew reinstall cc-pool"
	}
	if err := SpawnHolder(MountsSocketPath(), MountHolderLogPath(), mountd.DefaultSpawnTimeout); err != nil {
		return overlay.KindSymlink, fmt.Sprintf("mount holder did not start: %v", err)
	}
	ok, err := mountd.NewClient(MountsSocketPath()).Probe()
	if err != nil {
		return overlay.KindSymlink, fmt.Sprintf("mount holder probe failed: %v", err)
	}
	if !ok {
		return overlay.KindSymlink, "probe mount declined — install fuse-t (brew install macos-fuse-t/cask/fuse-t) if it is missing, or grant \"Network Volumes\" access in System Settings ▸ Privacy & Security (the failed attempt creates the toggle)"
	}
	return overlay.KindFuse, ""
}
