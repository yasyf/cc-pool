package daemon

import (
	"testing"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
)

func TestRuntimeHealthReflectsHolderSessionState(t *testing.T) {
	server := &Server{}
	if state := server.runtimeHealthState(); state != dkdaemon.StateDegraded || !server.runtimeBusy() {
		t.Fatalf("unactivated health = %q busy=%t", state, server.runtimeBusy())
	}
	server.holderActive.Store(true)
	if state := server.runtimeHealthState(); state != dkdaemon.StateHealthy || server.runtimeBusy() {
		t.Fatalf("active health = %q busy=%t", state, server.runtimeBusy())
	}
	server.holderActive.Store(false)
	server.holderLost.Store(true)
	if state := server.runtimeHealthState(); state != dkdaemon.StateFailed || !server.runtimeBusy() {
		t.Fatalf("lost health = %q busy=%t", state, server.runtimeBusy())
	}
}
