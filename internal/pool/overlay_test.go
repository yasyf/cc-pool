package pool

import (
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
)

func TestOverlayProviderFor(t *testing.T) {
	tests := []struct {
		name     string
		kind     overlay.Kind
		wantFuse bool // RemoteProvider wired to the pool paths; else symlink
	}{
		{name: "fuse maps to the remote provider", kind: overlay.KindFuse, wantFuse: true},
		{name: "symlink maps to the symlink provider", kind: overlay.KindSymlink},
		{name: "empty kind maps to the symlink provider", kind: overlay.Kind("")},
		{name: "unknown kind maps to the symlink provider", kind: overlay.Kind("bogus")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := OverlayProviderFor(tc.kind)
			if !tc.wantFuse {
				if _, ok := p.(*overlay.SymlinkProvider); !ok {
					t.Fatalf("OverlayProviderFor(%q) = %T, want *overlay.SymlinkProvider", tc.kind, p)
				}
				if got := p.Kind(); got != overlay.KindSymlink {
					t.Errorf("Kind() = %q, want %q", got, overlay.KindSymlink)
				}
				return
			}
			rp, ok := p.(remoteFuse)
			if !ok {
				t.Fatalf("OverlayProviderFor(%q) = %T, want pool.remoteFuse", tc.kind, p)
			}
			// KindFuse always — even in a build that cannot host mounts itself —
			// so stored-kind fences never silently flip.
			if got := rp.Kind(); got != overlay.KindFuse {
				t.Errorf("Kind() = %q, want %q", got, overlay.KindFuse)
			}
			if rp.Socket != MountsSocketPath() {
				t.Errorf("Socket = %q, want %q", rp.Socket, MountsSocketPath())
			}
			if rp.LogPath != MountHolderLogPath() {
				t.Errorf("LogPath = %q, want %q", rp.LogPath, MountHolderLogPath())
			}
		})
	}
}
