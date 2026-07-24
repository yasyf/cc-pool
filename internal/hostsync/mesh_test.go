package hostsync

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/synckit/hostregistry"
)

// writeMeshState writes the shared synckit state.json under a fresh
// XDG_CONFIG_HOME; hostregistry keys off that env, so this seams the mesh.
func writeMeshState(ctx context.Context, t *testing.T, reg *hostregistry.Registry) {
	t.Helper()
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	if reg == nil {
		return
	}
	if err := hostregistry.Mesh.InitializeState(ctx); err != nil {
		t.Fatal(err)
	}
	for _, identity := range reg.Hosts {
		fact, err := hostregistry.NewSSHHostFact(identity, "/opt/homebrew/bin/synckitd", nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := hostregistry.Mesh.RegisterHost(ctx, fact); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hostregistry.Mesh.Update(ctx, func(state *hostregistry.Registry) error {
		state.Self = reg.Self
		state.Hosts = append([]string{}, reg.Hosts...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSynckitMeshResolve pins the mesh source of truth: self and peers come
// from the shared synckit state.json, and an unjoined host fails loud.
func TestSynckitMeshResolve(t *testing.T) {
	ctx := context.Background()

	t.Run("resolves self and peers from the shared state", func(t *testing.T) {
		writeMeshState(ctx, t, &hostregistry.Registry{Self: "me@a.ts.net", Hosts: []string{"you@b.ts.net", "zed@c.ts.net"}})
		self, peers, err := (SynckitMesh{}).Resolve(ctx)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if self != "me@a.ts.net" {
			t.Errorf("self = %q, want me@a.ts.net", self)
		}
		if want := []string{"you@b.ts.net", "zed@c.ts.net"}; !reflect.DeepEqual(peers, want) {
			t.Errorf("peers = %v, want %v", peers, want)
		}
	})

	t.Run("unjoined host fails loud", func(t *testing.T) {
		writeMeshState(ctx, t, nil) // no state.json at all
		if _, _, err := (SynckitMesh{}).Resolve(ctx); !errors.Is(err, hostregistry.ErrStateMissing) {
			t.Fatalf("Resolve on an unjoined host = %v, want a loud not-joined error", err)
		}
	})

	t.Run("empty self with peers still fails loud", func(t *testing.T) {
		writeMeshState(ctx, t, &hostregistry.Registry{Hosts: []string{"you@b.ts.net"}})
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
