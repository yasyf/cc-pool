package daemon

import "github.com/yasyf/fusekit/mountd"

// killSocketPeer is overridable in tests so eviction paths can be exercised
// without dialing a real socket or signalling a real process.
var killSocketPeer = func(socket string) (int, error) { return mountd.NewClient(socket).Kill() }

// KillSocketPeer force-terminates the process currently holding the DAEMON
// socket — the daemon itself or an orphaned predecessor, never the
// mount-holder, which lives behind its own socket. It delegates to
// mountd.Client.Kill: the target is identified by peer credentials (LOCAL_PEERPID),
// never by name, pid<=1 and the caller's own process are spared, and the
// killed pid is returned (0 if the peer is gone or is us).
func (c *Client) KillSocketPeer() (int, error) {
	return killSocketPeer(c.socket)
}
