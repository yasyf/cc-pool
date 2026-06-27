package pool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestCanHostFuse pins CanHostFuse's real, build-tag-independent logic via the
// holderExe seam (and a temp HOME so DefaultHolderSocket points at nothing
// serving): no cask installed and no holder reachable → cannot host; the cask
// binary present → can host without a running holder. This replaces the old
// per-build-tag tests, which only passed when the test machine happened to match
// the tag's assumed cask state.
func TestCanHostFuse(t *testing.T) {
	t.Setenv("HOME", t.TempDir()) // DefaultHolderSocket → temp/.fusekit; nothing listening
	orig := holderExe
	t.Cleanup(func() { holderExe = orig })

	holderExe = filepath.Join(t.TempDir(), "absent", "fusekit-holder")
	if CanHostFuse() {
		t.Fatal("CanHostFuse() = true with no cask and no holder, want false")
	}

	exe := filepath.Join(t.TempDir(), "fusekit-holder")
	if err := os.WriteFile(exe, []byte("stub"), 0o755); err != nil {
		t.Fatal(err)
	}
	holderExe = exe
	if !CanHostFuse() {
		t.Fatal("CanHostFuse() = false with the cask installed, want true")
	}
}

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

// TestOverlaySpecHolderWiring pins that the Spec cc-pool hands fusekit drives the
// SHARED holder over RPC: the cask socket/binary, cc-pool's Owner, and the content
// wiring (bridge socket, source mode, probe path, private prefixes) that makes the
// provider register a synth-serving AddMount. It must NOT self-exec (no
// StableExecDir, no "mount-holder" argv) and must NOT version-replace the holder
// (no Version).
func TestOverlaySpecHolderWiring(t *testing.T) {
	spec := overlaySpec()
	if spec.PassthroughOnly {
		t.Error("PassthroughOnly = true; cc-pool's mirror serves synthetic content, want false (always NFS)")
	}
	h := spec.Holder
	if h == nil {
		t.Fatal("Holder is nil; fuse selection would be disabled")
	}
	socket := mountd.DefaultHolderSocket()
	switch {
	case h.Socket != socket:
		t.Errorf("Holder.Socket = %q, want the shared socket %q", h.Socket, socket)
	case h.ExecPath != mountd.HolderExe:
		t.Errorf("Holder.ExecPath = %q, want the cask binary %q", h.ExecPath, mountd.HolderExe)
	case h.Owner != HolderOwner:
		t.Errorf("Holder.Owner = %q, want %q", h.Owner, HolderOwner)
	case h.BridgeSocket != BridgeSocketPath():
		t.Errorf("Holder.BridgeSocket = %q, want %q", h.BridgeSocket, BridgeSocketPath())
	case h.ContentMode != "source":
		t.Errorf("Holder.ContentMode = %q, want \"source\"", h.ContentMode)
	case h.ProbePath != "/"+overlay.ProbeFileName:
		t.Errorf("Holder.ProbePath = %q, want %q", h.ProbePath, "/"+overlay.ProbeFileName)
	case h.StableExecDir != "":
		t.Errorf("Holder.StableExecDir = %q, want empty (no self-exec onto the shared holder)", h.StableExecDir)
	case h.Version != "":
		t.Errorf("Holder.Version = %q, want empty (cc-pool must not version-replace a shared holder)", h.Version)
	}
	wantArgs := []string{"--socket", socket}
	if len(h.Args) != len(wantArgs) || h.Args[0] != wantArgs[0] || h.Args[1] != wantArgs[1] {
		t.Fatalf("Holder.Args = %v, want %v (the cask binary's own flags, not a subcommand)", h.Args, wantArgs)
	}
	if len(h.PrivatePrefixes) != len(overlay.PrivatePrefixes) {
		t.Fatalf("Holder.PrivatePrefixes = %v, want %v", h.PrivatePrefixes, overlay.PrivatePrefixes)
	}
}
