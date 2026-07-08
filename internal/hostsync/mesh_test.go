package hostsync

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// writeMeshState writes the shared synckit state.json under a fresh
// XDG_CONFIG_HOME; hostregistry keys off that env, so this seams the mesh.
func writeMeshState(t *testing.T, body string) {
	t.Helper()
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	if body == "" {
		return
	}
	dir := filepath.Join(xdg, "synckit")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestSynckitMeshResolve pins the mesh source of truth: self and peers come
// from the shared synckit state.json, and an unjoined host fails loud.
func TestSynckitMeshResolve(t *testing.T) {
	ctx := context.Background()

	t.Run("resolves self and peers from the shared state", func(t *testing.T) {
		writeMeshState(t, `{"self":"me@a.ts.net","hosts":["you@b.ts.net","exec:true"]}`)
		self, peers, err := (SynckitMesh{}).Resolve(ctx)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if self != "me@a.ts.net" {
			t.Errorf("self = %q, want me@a.ts.net", self)
		}
		if want := []string{"you@b.ts.net", "exec:true"}; !reflect.DeepEqual(peers, want) {
			t.Errorf("peers = %v, want %v", peers, want)
		}
	})

	t.Run("unjoined host fails loud", func(t *testing.T) {
		writeMeshState(t, "") // no state.json at all
		if _, _, err := (SynckitMesh{}).Resolve(ctx); err == nil || !strings.Contains(err.Error(), "joined") {
			t.Fatalf("Resolve on an unjoined host = %v, want a loud not-joined error", err)
		}
	})

	t.Run("empty self with peers still fails loud", func(t *testing.T) {
		writeMeshState(t, `{"hosts":["you@b.ts.net"]}`)
		if _, _, err := (SynckitMesh{}).Resolve(ctx); err == nil {
			t.Fatal("Resolve without a self must not return a mesh it cannot converge")
		}
	})
}

// TestManifestPath pins synckitd's discovery path for cc-pool's manifest.
func TestManifestPath(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	got, err := ManifestPath()
	if err != nil {
		t.Fatalf("ManifestPath: %v", err)
	}
	if want := filepath.Join(xdg, "synckit", "manifests", "cc-pool.json"); got != want {
		t.Fatalf("ManifestPath = %q, want %q", got, want)
	}
}
