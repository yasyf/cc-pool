//go:build !fuse

package pool

import (
	"strings"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// CanHostFuse is pinned against the build tag itself — not against
// fusekit.Built(), which would be tautological. The fuse-build counterpart
// lives in overlay_fuse_test.go.
func TestCanHostFusePureBuild(t *testing.T) {
	if CanHostFuse() {
		t.Fatal("CanHostFuse() = true in a pure (non-fuse) build, want false")
	}
}

// TestDetectOverlayBackendPureBuild pins the pure-build short-circuit: the
// verdict is symlink with a short reason, decided without touching the holder
// socket — no spawn, no probe, and no adoption of a leftover holder this binary
// could never respawn (the same policy SetDefaultOverlayKind enforces). A
// regression that probes anyway surfaces as a different reason (or a fuse
// verdict) here. The reason is fusekit's generic copy ("cannot host fuse
// mounts") with no cc-pool CLI verb baked in.
func TestDetectOverlayBackendPureBuild(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	backend, reason := DetectOverlayBackend()
	if backend != fkoverlay.BackendSymlink {
		t.Fatalf("backend = %q, want symlink in a pure build", backend)
	}
	if !strings.Contains(reason, "cannot host fuse mounts") {
		t.Fatalf("reason = %q, want it to say this build cannot host fuse mounts", reason)
	}
}
