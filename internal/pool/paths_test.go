package pool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
)

func TestFuseKitPathsArePrivate(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	runtime := filepath.Join(StateDir(), "fusekit")
	if got := FuseKitRuntimeDir(); got != runtime {
		t.Fatalf("FuseKitRuntimeDir() = %q, want %q", got, runtime)
	}
	if got := FuseKitSocketPath(); got != filepath.Join(runtime, "fusekit.sock") {
		t.Fatalf("FuseKitSocketPath() = %q", got)
	}
	backing := filepath.Join(runtime, "backing")
	if got := FuseKitBackingRoot(); got != backing {
		t.Fatalf("FuseKitBackingRoot() = %q, want %q", got, backing)
	}
	if got := AccountBackingDir(7); got != filepath.Join(backing, "acct-07") {
		t.Fatalf("AccountBackingDir(7) = %q", got)
	}
}

func TestWidgetAppPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if got, want := WidgetAppPath(), filepath.Join(home, "Applications", "CCPoolStatus.app"); got != want {
		t.Fatalf("WidgetAppPath() = %q, want %q", got, want)
	}
}

func TestDBPathIsV1Only(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if got, want := DBPath(), filepath.Join(StateDir(), "pool-v1.db"); got != want {
		t.Fatalf("DBPath() = %q, want %q", got, want)
	}
}

func TestV1StoreIgnoresOldPoolDatabase(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	oldPath := statePath("pool.db")
	oldBytes := []byte("unrecognized old database")
	if err := os.WriteFile(oldPath, oldBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := store.Open(DBPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	// #nosec G304 -- oldPath is a fixed filename inside this test's temporary HOME.
	got, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(oldBytes) {
		t.Fatalf("old pool database changed: got %q, want %q", got, oldBytes)
	}
	if _, err := os.Stat(DBPath()); err != nil {
		t.Fatalf("v1 database missing: %v", err)
	}
}
