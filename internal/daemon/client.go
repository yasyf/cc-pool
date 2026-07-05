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
// session stickiness (empty disables); noFallback rejects a least-bad
// exhausted pick; ok=false means fall back to a live, daemonless selection.
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

// migrateTimeout covers a probe mount plus, per account, an 8s mount wait and rollback teardown.
const migrateTimeout = 150 * time.Second

// Migrate asks the daemon to convert accounts to the given overlay kind:
// nil account means all not already at it; busy accounts are skipped and
// reported; force skips the live-session gate (reservations still refuse).
func (c *Client) Migrate(account *int, to string, force bool) (*Response, error) {
	return c.do(Request{Op: OpMigrate, Account: account, To: to, Force: force}, migrateTimeout)
}

// CredMove asks the daemon to move account credentials to the given backend
// ("keychain" or "file"): nil account means all accounts; busy accounts are
// skipped and reported per account. It shares migrateTimeout because both ops
// answer under the server's extended 140s conn deadline.
func (c *Client) CredMove(account *int, to string) (*Response, error) {
	return c.do(Request{Op: OpCredMove, Account: account, To: to}, migrateTimeout)
}

// FPRepair asks the daemon to re-register wedged File Provider domains: nil
// account repairs every currently-wedged domain, a set account repairs that one
// regardless of its verdict. The daemon owns the select gate a CLI-side
// re-register would race, so this routes through it; the CLI falls back to a
// direct provider repair only when the daemon is down. It shares migrateTimeout:
// each re-register is a Teardown+Setup that can take seconds to materialize.
func (c *Client) FPRepair(account *int) (*Response, error) {
	return c.do(Request{Op: OpFPRepair, Account: account}, migrateTimeout)
}

// Shutdown asks the daemon to step down; OK means it will release the socket
// shortly (confirm with WaitGone). Works even on an orphan launchctl bootout
// cannot kill.
func (c *Client) Shutdown() (*Response, error) {
	return c.do(Request{Op: OpShutdown}, 2*time.Second)
}

// WaitGone reports whether the socket stopped accepting connections within timeout.
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
