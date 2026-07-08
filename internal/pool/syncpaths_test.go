package pool

import (
	"path/filepath"
	"testing"
)

// TestSyncPaths pins the host-sync path helpers to their exact strings rooted
// at ~/.cc-pool, mirroring the existing stateDir.Path one-liner layout.
func TestSyncPaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := filepath.Join(home, ".cc-pool")

	cases := map[string]struct {
		got  string
		want string
	}{
		"SyncDir":        {SyncDir(), filepath.Join(root, "sync")},
		"SyncSocketPath": {SyncSocketPath(), filepath.Join(root, "sync.sock")},
		"SyncStampsDir":  {SyncStampsDir(), filepath.Join(root, "sync", "stamps")},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s = %q, want %q", name, tc.got, tc.want)
			}
		})
	}
}
