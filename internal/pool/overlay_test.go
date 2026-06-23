package pool

import (
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func TestOverlayProviderFor(t *testing.T) {
	tests := []struct {
		name     string
		backend  fkoverlay.Backend
		wantFuse bool // RemoteFuseProvider wired to the pool paths; else symlink
	}{
		{name: "nfs maps to the remote fuse provider", backend: fkoverlay.BackendNFS, wantFuse: true},
		{name: "fskit maps to the remote fuse provider", backend: fkoverlay.BackendFSKit, wantFuse: true},
		{name: "symlink maps to the symlink provider", backend: fkoverlay.BackendSymlink},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := OverlayProviderFor(tc.backend)
			if err != nil {
				t.Fatalf("OverlayProviderFor(%q) error: %v", tc.backend, err)
			}
			if !tc.wantFuse {
				if _, ok := p.(*fkoverlay.SymlinkProvider); !ok {
					t.Fatalf("OverlayProviderFor(%q) = %T, want *fkoverlay.SymlinkProvider", tc.backend, p)
				}
				if got := p.Backend(); got != fkoverlay.BackendSymlink {
					t.Errorf("Backend() = %q, want %q", got, fkoverlay.BackendSymlink)
				}
				return
			}
			// A fuse provider always reports its stored backend — even in a build
			// that cannot host mounts itself — so stored-backend fences never
			// silently flip.
			if got := p.Backend(); got != tc.backend {
				t.Errorf("Backend() = %q, want %q", got, tc.backend)
			}
			if got := p.PrivateRoot("/p/acct-01"); got != fkoverlay.FusePrivateRoot("/p/acct-01") {
				t.Errorf("PrivateRoot = %q, want %q", got, fkoverlay.FusePrivateRoot("/p/acct-01"))
			}
		})
	}
}

// TestOverlayProviderForUnknownBackend pins that a bad stored backend string
// fails loud rather than silently degrading to symlink.
func TestOverlayProviderForUnknownBackend(t *testing.T) {
	if _, err := OverlayProviderFor(fkoverlay.Backend("bogus")); err == nil {
		t.Fatal("OverlayProviderFor(\"bogus\") = nil error, want a loud failure")
	}
}

// TestOverlaySpecHolderWiring pins that the Spec cc-pool hands fusekit carries
// the pool socket/log/holder argv — the wiring a RemoteFuseProvider drives.
func TestOverlaySpecHolderWiring(t *testing.T) {
	spec := overlaySpec()
	if spec.PassthroughOnly {
		t.Error("PassthroughOnly = true; cc-pool's mirror serves synthetic content, want false (always NFS)")
	}
	if spec.Holder == nil {
		t.Fatal("Holder is nil; fuse selection would be disabled")
	}
	if spec.Holder.Socket != MountsSocketPath() {
		t.Errorf("Holder.Socket = %q, want %q", spec.Holder.Socket, MountsSocketPath())
	}
	if spec.Holder.LogPath != MountHolderLogPath() {
		t.Errorf("Holder.LogPath = %q, want %q", spec.Holder.LogPath, MountHolderLogPath())
	}
	if spec.Holder.StableExecDir != HolderBinDir() {
		t.Errorf("Holder.StableExecDir = %q, want %q", spec.Holder.StableExecDir, HolderBinDir())
	}
	wantArgs := []string{"mount-holder", "--socket", MountsSocketPath()}
	if len(spec.Holder.Args) != len(wantArgs) {
		t.Fatalf("Holder.Args = %v, want %v", spec.Holder.Args, wantArgs)
	}
	for i := range wantArgs {
		if spec.Holder.Args[i] != wantArgs[i] {
			t.Fatalf("Holder.Args = %v, want %v", spec.Holder.Args, wantArgs)
		}
	}
	// cc-pool's mirror always lands on NFS (PassthroughOnly=false).
	if got := FuseBackend(); got != fkoverlay.BackendNFS {
		t.Errorf("FuseBackend() = %q, want %q", got, fkoverlay.BackendNFS)
	}
}
