package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
)

// setRunTakeover swaps the takeover seam so listen's Evict closure can be driven
// to a chosen Outcome without a live incumbent.
func setRunTakeover(t *testing.T, fn func(context.Context, daemon.TakeoverConfig) (daemon.Outcome, error)) {
	t.Helper()
	old := runTakeover
	runTakeover = fn
	t.Cleanup(func() { runTakeover = old })
}

// setSocketPeerPID swaps the kernel-attested-pid seam so tests aim the takeover
// ladder at a chosen pid instead of the in-process test socket (whose peer is
// the test itself, and whose getsockopt is darwin-only).
func setSocketPeerPID(t *testing.T, fn func(string) (int, error)) {
	t.Helper()
	old := socketPeerPID
	socketPeerPID = fn
	t.Cleanup(func() { socketPeerPID = old })
}

// TestDaemonPeerHealth: Health reports the exact build/protocol and the
// kernel-attested socket peer pid (the Response itself carries no pid).
func TestDaemonPeerHealth(t *testing.T) {
	f := newFakeDaemon(t, "1.2.3", false)
	setSocketPeerPID(t, func(socket string) (int, error) {
		if socket != f.socket {
			t.Errorf("PeerPID read socket %q, want %q", socket, f.socket)
		}
		return 4242, nil
	})
	h, err := (&daemonPeer{socket: f.socket}).Health(t.Context())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if h.Build != "1.2.3" || h.Protocol != ProtocolVersion || h.PID != 4242 || h.State != daemon.StateHealthy {
		t.Fatalf("Health = %+v, want {Build:1.2.3 Protocol:%d PID:4242 State:healthy}", h, ProtocolVersion)
	}
}

// TestDaemonPeerHealthUnreachable: a dead socket surfaces as an error so
// daemon.Run treats it as no incumbent (bind), not a false healthy peer.
func TestDaemonPeerHealthUnreachable(t *testing.T) {
	p := &daemonPeer{socket: filepath.Join(t.TempDir(), "missing.sock")}
	if _, err := p.Health(t.Context()); err == nil {
		t.Fatal("Health on a dead socket returned a nil error")
	}
}

// TestDaemonPeerShutdown sends the frozen OpShutdown handshake over the wire.
func TestDaemonPeerShutdown(t *testing.T) {
	f := newFakeDaemon(t, "1.2.3", false)
	if err := (&daemonPeer{socket: f.socket}).Shutdown(t.Context()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !f.received(OpShutdown) {
		t.Fatal("incumbent never received OpShutdown")
	}
}

// TestDaemonPeerHandoffRefused: cc-pool advertises no handoff, so Handoff errs.
func TestDaemonPeerHandoffRefused(t *testing.T) {
	if err := (&daemonPeer{socket: "unused.sock"}).Handoff(t.Context()); !errors.Is(err, errNoHandoff) {
		t.Fatalf("Handoff err = %v, want errNoHandoff", err)
	}
}

// TestTakeoverVersionPolicy golden-pins newer-wins through the real daemon.Run
// over the daemonPeer: a downgrade or a tie steps aside (ExitSelf); only a
// strictly-newer self evicts (Bind). The evict case aims the ladder at a dead
// pid so no live process is signalled and no shutdown handshake is needed.
func TestTakeoverVersionPolicy(t *testing.T) {
	const deadPID = 0x7ffffffe // beyond pid_max: proc.Probe → ErrNoProcess
	tests := []struct {
		name string
		self string
		peer string
		want daemon.Outcome
	}{
		{"downgrade steps aside for a newer release", "1.0.0", "2.0.0", daemon.ExitSelf},
		{"equal version ties to step-aside", "1.2.3", "1.2.3", daemon.ExitSelf},
		{"dev build evicts a release", "9999.1.0-dev", "1.2.3", daemon.Bind},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeDaemon(t, tt.peer, false)
			setSocketPeerPID(t, func(string) (int, error) { return deadPID, nil })
			got, err := daemon.Run(t.Context(), daemon.TakeoverConfig{
				Self:     tt.self,
				Protocol: ProtocolVersion,
				Peer:     &daemonPeer{socket: f.socket},
				Contract: daemon.RequestDaemon,
				WaitMode: daemon.SocketRelease,
				Grace:    50 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got != tt.want {
				t.Fatalf("outcome = %s, want %s", got, tt.want)
			}
			if tt.want == daemon.ExitSelf && f.received(OpShutdown) {
				t.Error("step-aside sent OpShutdown to the incumbent")
			}
		})
	}
}
