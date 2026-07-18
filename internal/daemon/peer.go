package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/fusekit/mountd"
)

// socketPeerPID reads the daemon socket's kernel-attested peer pid via
// getsockopt(LOCAL_PEERPID) — stronger than any wire field (the Response carries
// no PID). A test seam so eviction tests can aim the ladder at a chosen victim.
var socketPeerPID = func(socket string) (int, error) { return mountd.NewClient(socket).PeerPID() }

// runTakeover is a test seam over daemon.Run so listen's Evict closure can be
// driven to a chosen Outcome without a live incumbent.
var runTakeover = daemon.Run

// errNoHandoff rejects a handoff request: cc-pool advertises no FeatureHandoff.
var errNoHandoff = errors.New("cc-pool daemon advertises no socket handoff")

// daemonPeer adapts the frozen daemon wire to daemonkit's daemon.Peer for the
// successor-initiated takeover.
type daemonPeer struct {
	socket string
}

// request sends a lifecycle request without rejecting the incumbent's reply
// protocol. Takeover must read OK, Version, and Error across an upgrade skew;
// public clients and work operations remain strict.
func (p *daemonPeer) request(ctx context.Context, req Request) (*Response, error) {
	resp, err := (&Client{socket: p.socket}).roundTripContext(ctx, req, 2*time.Second)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, fmt.Errorf("takeover %s rejected: %s", req.Op, resp.Error)
	}
	return resp, nil
}

// Health dials the socket for the incumbent's version and reads its
// kernel-attested pid; State is healthy and Features nil (no handoff).
func (p *daemonPeer) Health(ctx context.Context) (daemon.Health, error) {
	resp, err := p.request(ctx, Request{Op: OpHealth})
	if err != nil {
		return daemon.Health{}, err
	}
	pid, err := socketPeerPID(p.socket)
	if err != nil {
		return daemon.Health{}, err
	}
	return daemon.Health{Version: resp.Version, PID: pid, State: daemon.StateHealthy}, nil
}

// Shutdown sends the frozen OpShutdown handshake, asking the incumbent to step
// down and release the socket.
func (p *daemonPeer) Shutdown(ctx context.Context) error {
	_, err := p.request(ctx, Request{Op: OpShutdown})
	return err
}

// Handoff always errs: cc-pool never advertises FeatureHandoff, so the takeover
// evicts an older incumbent via the RequestDaemon kill ladder, never a handoff.
func (p *daemonPeer) Handoff(context.Context) error { return errNoHandoff }
