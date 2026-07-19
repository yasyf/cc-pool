package daemon

import (
	"context"
	"os"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

// EnsureRunning returns true if the daemon is reachable, spawning a detached
// `cc-pool daemon` and waiting up to timeout for its socket if not. A second
// instance is harmless: the daemon refuses to start if the socket is owned.
func (c *Client) EnsureRunning(ctx context.Context, timeout time.Duration) bool {
	peer := c.lifecyclePeer()
	defer func() { _ = peer.Close() }()
	spawn := proc.Spawn{
		Socket:  c.socket,
		Args:    []string{"daemon"},
		Timeout: timeout,
		LogPath: os.DevNull,
		Available: func() bool {
			probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			health, err := peer.Health(probeCtx)
			return err == nil && health.Build == c.clientBuild() && health.Protocol == int(wire.ProtocolVersion)
		},
		CanHost: func() error { return nil },
	}
	err := dkdaemon.EnsureCurrent(ctx, dkdaemon.EnsureConfig{
		Peer:     peer,
		Protocol: int(wire.ProtocolVersion),
		LockPath: c.socket + ".start.lock",
		Ensure:   spawn.EnsureRunning,
		Timeout:  timeout,
	}, c.clientBuild())
	return err == nil
}
