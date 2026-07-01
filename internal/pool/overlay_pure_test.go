//go:build !fuse

package pool

import (
	"path/filepath"
	"strings"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestDetectOverlayBackendCaskAbsent pins that with no fusekit-holder cask
// (holderExe at an absent path) DetectOverlayBackend resolves to symlink: canHost
// fails on the missing ExecPath before any holder spawn, never adopting a holder
// this binary could not respawn.
func TestDetectOverlayBackendCaskAbsent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	orig := holderExe
	t.Cleanup(func() { holderExe = orig })
	holderExe = filepath.Join(t.TempDir(), "absent", "fusekit-holder")
	backend, reason := DetectOverlayBackend(t.Context())
	if backend != fkoverlay.BackendSymlink {
		t.Fatalf("backend = %q, want symlink with the cask absent", backend)
	}
	if !strings.Contains(reason, "cannot host fuse mounts") {
		t.Fatalf("reason = %q, want it to say this binary cannot host fuse mounts", reason)
	}
}
