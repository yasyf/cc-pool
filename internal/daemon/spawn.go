package daemon

import (
	"context"
	"os"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// EnsureRunning returns true if the daemon is reachable, spawning a detached
// `cc-pool daemon` and waiting up to timeout for its socket if not. A second
// instance is harmless: the daemon refuses to start if the socket is owned.
func (c *Client) EnsureRunning(ctx context.Context, timeout time.Duration) bool {
	return proc.Spawn{
		Socket:    c.socket,
		Args:      []string{"daemon"},
		Timeout:   timeout,
		LogPath:   os.DevNull,
		Available: c.Available,
		CanHost:   func() error { return nil },
	}.EnsureRunning(ctx) == nil
}
