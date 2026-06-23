package pool

import (
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
)

// TestNewRemoteFuse pins the holder-backed adapter wiring that OverlayProviderFor
// hands out for KindFuse: it always reports KindFuse, routes PrivateRoot to the
// fuse private backing dir, and carries the pool socket, holder log, mount-holder
// argv, and pure-build brew hint that drive the detached holder. The
// Setup/Teardown/Sync/Health half is inherited from fusekit's mountd.RemoteHost
// (covered by fusekit's own tests); this pins only the cc-pool adapter surface.
func TestNewRemoteFuse(t *testing.T) {
	p := newRemoteFuse()

	if got := p.Kind(); got != overlay.KindFuse {
		t.Errorf("Kind() = %q, want %q", got, overlay.KindFuse)
	}
	if got, want := p.PrivateRoot("/p/acct-01"), overlay.FusePrivateRoot("/p/acct-01"); got != want {
		t.Errorf("PrivateRoot = %q, want %q", got, want)
	}

	// Fields promoted from the embedded *mountd.RemoteHost.
	if p.Socket != MountsSocketPath() {
		t.Errorf("Socket = %q, want %q", p.Socket, MountsSocketPath())
	}
	if p.LogPath != MountHolderLogPath() {
		t.Errorf("LogPath = %q, want %q", p.LogPath, MountHolderLogPath())
	}
	wantArgs := []string{"mount-holder", "--socket", MountsSocketPath()}
	if !reflect.DeepEqual(p.Args, wantArgs) {
		t.Errorf("Args = %v, want %v", p.Args, wantArgs)
	}
	for _, want := range []string{"ccp fuse enable"} {
		if !strings.Contains(p.CannotHostHint, want) {
			t.Errorf("CannotHostHint %q does not contain %q", p.CannotHostHint, want)
		}
	}
}

// TestRemoteFuseImplementsProvider is a runtime restatement of the compile-time
// assertion in remotefuse.go: the adapter satisfies overlay.Provider.
func TestRemoteFuseImplementsProvider(_ *testing.T) {
	var _ overlay.Provider = newRemoteFuse()
}
