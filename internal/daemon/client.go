package daemon

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
)

// ErrDaemonUnavailable means the daemon socket could not be reached.
var ErrDaemonUnavailable = errors.New("daemon not running")

// Client is a short-lived connection to the daemon socket.
type Client struct {
	socket string
}

// NewClient returns a client for the default socket path.
func NewClient() *Client { return &Client{socket: pool.SocketPath()} }

// Available reports whether the daemon socket accepts a connection.
func (c *Client) Available() bool {
	conn, err := net.DialTimeout("unix", c.socket, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// do sends one request and reads one response.
func (c *Client) do(req Request, timeout time.Duration) (*Response, error) {
	conn, err := net.DialTimeout("unix", c.socket, 500*time.Millisecond)
	if err != nil {
		return nil, ErrDaemonUnavailable
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	req.Proto = ProtocolVersion
	enc := json.NewEncoder(conn)
	if err := enc.Encode(req); err != nil {
		return nil, err
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Select asks the daemon for the best account dir. cwd keys best-effort
// session stickiness; empty disables it. noFallback rejects a least-bad
// exhausted pick (set by --wait callers, which would discard it). ok=false
// means the caller should fall back to a live, daemonless selection.
func (c *Client) Select(account *int, pid int, noMark bool, cwd string, noFallback bool) (resp *Response, ok bool) {
	r, err := c.do(Request{Op: OpSelect, Account: account, PID: pid, NoMark: noMark, Cwd: cwd, NoFallback: noFallback}, 3*time.Second)
	if err != nil {
		return nil, false
	}
	return r, true
}

// Status asks the daemon for all account statuses.
func (c *Client) Status() (*Response, error) {
	return c.do(Request{Op: OpStatus}, 5*time.Second)
}

// Checkin releases a checkout for pid.
func (c *Client) Checkin(pid int) (*Response, error) {
	return c.do(Request{Op: OpCheckin, PID: pid}, 3*time.Second)
}

// Health probes the daemon.
func (c *Client) Health() (*Response, error) {
	return c.do(Request{Op: OpHealth}, 2*time.Second)
}

// migrateTimeout bounds one migrate request: a probe mount, then per account
// up to an 8s mount wait plus a bounded rollback teardown, across a pool.
const migrateTimeout = 150 * time.Second

// Migrate asks the daemon to convert accounts to the given overlay kind.
// account nil means every account not already at the kind; busy accounts are
// skipped and reported, so re-running sweeps the stragglers. force skips the
// live-session gate (reservations still refuse).
func (c *Client) Migrate(account *int, to string, force bool) (*Response, error) {
	return c.do(Request{Op: OpMigrate, Account: account, To: to, Force: force}, migrateTimeout)
}

// Shutdown asks the daemon to step down. An OK reply means it accepted and will
// release the socket shortly; use WaitGone to confirm. This evicts a daemon
// regardless of launchd tracking — the only way to clear an orphan a `brew
// services stop` (launchctl bootout) cannot kill.
func (c *Client) Shutdown() (*Response, error) {
	return c.do(Request{Op: OpShutdown}, 2*time.Second)
}

// WaitGone polls until the socket stops accepting connections or timeout
// elapses, reporting whether it went dead. Reused by the CLI upgrade path and a
// successor daemon's bind-time eviction.
func (c *Client) WaitGone(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", c.socket, 200*time.Millisecond)
		if err != nil {
			return true
		}
		_ = conn.Close()
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
