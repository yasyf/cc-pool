package pool

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
)

// TestSpawnHolderPureBuildRefusal pins that on a pure (non-fuse) build,
// SpawnHolder refuses with mountd.ErrCannotHost — NOT ErrHolderUnavailable —
// carrying cc-pool's brew hint verbatim. The two sentinels must never
// errors.Is-match: a binary that can never host is a permanent condition that
// drives cc-pool's gated fuse→symlink retreat, while ErrHolderUnavailable is a
// transient blip that drives retry. Collapsing them would make an additive
// holder blip trigger the one irreversible action.
func TestSpawnHolderPureBuildRefusal(t *testing.T) {
	if fusekit.Built() {
		t.Skip("fuse build can host: the pure-build refusal path is not exercised here")
	}
	// A socket nothing serves, so EnsureRunning falls past its Available()
	// short-circuit to the build check.
	sock := filepath.Join(t.TempDir(), "mounts.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")

	err := SpawnHolder(sock, logPath, 100*time.Millisecond)
	if err == nil {
		t.Fatal("SpawnHolder on a pure build = nil, want a cannot-host refusal")
	}
	if !errors.Is(err, mountd.ErrCannotHost) {
		t.Errorf("errors.Is(err, ErrCannotHost) = false, want true; err = %v", err)
	}
	if errors.Is(err, mountd.ErrHolderUnavailable) {
		t.Errorf("errors.Is(err, ErrHolderUnavailable) = true, want false (the sentinels must stay disjoint); err = %v", err)
	}
	for _, want := range []string{"cannot host fuse mounts", "ccp fuse enable"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	}
}
