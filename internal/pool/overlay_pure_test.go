//go:build !fuse

package pool

import (
	"path/filepath"
	"strings"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestDetectOverlayBackendCaskAbsent pins the cask-absent verdict: with no
// fusekit-holder cask installed (holderExe at an absent path), the binary cannot
// host fuse mounts, so DetectOverlayBackend resolves to symlink — canHost fails
// on the missing ExecPath before any holder spawn, never adopting a leftover
// holder this binary could not respawn. Hermetic via the holderExe seam (which
// overlaySpec threads into the probe ExecPath), so the verdict never depends on
// whether this machine happens to have the cask installed.
func TestDetectOverlayBackendCaskAbsent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	orig := holderExe
	t.Cleanup(func() { holderExe = orig })
	holderExe = filepath.Join(t.TempDir(), "absent", "fusekit-holder")
	backend, reason := DetectOverlayBackend()
	if backend != fkoverlay.BackendSymlink {
		t.Fatalf("backend = %q, want symlink with the cask absent", backend)
	}
	if !strings.Contains(reason, "cannot host fuse mounts") {
		t.Fatalf("reason = %q, want it to say this binary cannot host fuse mounts", reason)
	}
}
