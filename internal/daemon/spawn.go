package daemon

import (
	"os"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// EnsureRunning returns true if the daemon is reachable, auto-spawning a
// detached `cc-pool daemon` and waiting up to timeout for its socket if it
// is not. A second instance is harmless: the daemon refuses to start if the
// socket is already owned. Used by the `select` hot path so it stays fast when
// the daemon is up and self-heals when it is not.
//
// The spawn is proc.Spawn: the daemon can always spawn itself (CanHost returns
// nil), and the child's stdout/stderr are discarded (LogPath=os.DevNull, the
// old Stdout/Stderr=nil → /dev/null behavior). proc.Spawn also reaps the
// detached child in the background, so its exit never strands a zombie on the
// select hot path.
func (c *Client) EnsureRunning(timeout time.Duration) bool {
	return proc.Spawn{
		Socket:    c.socket,
		Args:      []string{"daemon"},
		Timeout:   timeout,
		LogPath:   os.DevNull,
		Available: c.Available,
		CanHost:   func() error { return nil },
	}.EnsureRunning() == nil
}
