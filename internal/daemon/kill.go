package daemon

import "github.com/yasyf/fusekit/mountd"

// killSocketPeer is a test seam.
var killSocketPeer = func(socket string) (int, error) { return mountd.NewClient(socket).Kill() }

// KillSocketPeer force-terminates the process holding the daemon socket —
// never the mount-holder, which lives behind its own socket. The peer is
// identified by LOCAL_PEERPID, never by name; pid<=1 and self are spared;
// returns the killed pid (0 if the peer is gone or is us).
func (c *Client) KillSocketPeer() (int, error) {
	return killSocketPeer(c.socket)
}
