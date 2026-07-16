package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
)

// ErrDaemonUnavailable means the daemon socket could not be reached.
var ErrDaemonUnavailable = errors.New("daemon not running")

// ErrProtocolMismatch means the socket peer speaks an incompatible daemon protocol.
var ErrProtocolMismatch = errors.New("daemon protocol mismatch")

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
	return c.doContext(context.Background(), req, timeout)
}

func (c *Client) doContext(ctx context.Context, req Request, timeout time.Duration) (*Response, error) {
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "unix", c.socket)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrDaemonUnavailable
	}
	defer func() { _ = conn.Close() }()
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)

	req.Proto = ProtocolVersion
	enc := json.NewEncoder(conn)
	if err := enc.Encode(req); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if resp.Proto != ProtocolVersion {
		return nil, fmt.Errorf("%w: daemon=%d client=%d", ErrProtocolMismatch, resp.Proto, ProtocolVersion)
	}
	return &resp, nil
}

// Select asks the daemon for a provisional account selection. cwd keys
// best-effort session stickiness (empty disables); noFallback rejects a
// least-bad exhausted pick; ok=false means fall back to a live, daemonless
// selection. A successful response must be committed or aborted by token.
func (c *Client) Select(ctx context.Context, account *int, pid int, noMark bool, cwd string, noFallback bool, excludeIDs []int) (resp *Response, ok bool) {
	r, err := c.doContext(ctx, Request{Op: OpSelect, Account: account, PID: pid, NoMark: noMark, Cwd: cwd, NoFallback: noFallback, ExcludeIDs: excludeIDs}, 3*time.Second)
	if err != nil {
		return nil, false
	}
	return r, true
}

// CommitSelection consumes a provisional selection and records its session and
// sticky state. The server caches the terminal token result, so transport-ambiguous
// attempts are safe to retry within one bounded commit window.
func (c *Client) CommitSelection(ctx context.Context, token string) error {
	const commitWindow = 3 * time.Second
	if err := ctx.Err(); err != nil {
		return err
	}
	commitDeadline := time.Now().Add(commitWindow)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(commitDeadline) {
		return context.DeadlineExceeded
	}
	// Once the first commit byte can be sent, cancellation cannot make the result
	// ambiguous: keep confirming the token's terminal result for the full reserved
	// window. Its deadline is no later than the caller's checked deadline.
	commitCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), commitDeadline)
	defer cancel()
	var lastErr error
	for {
		r, err := c.doContext(commitCtx, Request{Op: OpSelectCommit, ReservationToken: token}, time.Second)
		if err == nil {
			if !r.OK {
				return errors.New(r.Error)
			}
			return nil
		}
		lastErr = err
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-commitCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return lastErr
		case <-timer.C:
		}
	}
}

// AbortSelection releases a provisional selection. Abort is idempotent.
func (c *Client) AbortSelection(ctx context.Context, token string) error {
	r, err := c.doContext(ctx, Request{Op: OpSelectAbort, ReservationToken: token}, 3*time.Second)
	if err != nil {
		return err
	}
	if !r.OK {
		return errors.New(r.Error)
	}
	return nil
}

// Status asks the daemon for all account statuses.
func (c *Client) Status() (*Response, error) {
	return c.do(Request{Op: OpStatus}, 5*time.Second)
}

// StatusContext asks the daemon for all account statuses within ctx's deadline.
func (c *Client) StatusContext(ctx context.Context) (*Response, error) {
	return c.doContext(ctx, Request{Op: OpStatus}, 5*time.Second)
}

// Checkin releases a checkout for pid.
func (c *Client) Checkin(pid int) (*Response, error) {
	return c.do(Request{Op: OpCheckin, PID: pid}, 3*time.Second)
}

// Health probes the daemon.
func (c *Client) Health() (*Response, error) {
	return c.do(Request{Op: OpHealth}, 2*time.Second)
}

// HealthContext probes the daemon within ctx's remaining deadline.
func (c *Client) HealthContext(ctx context.Context) (*Response, error) {
	return c.doContext(ctx, Request{Op: OpHealth}, 2*time.Second)
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
// regardless of its verdict. When retreat is true the daemon retreats the target
// domain(s) to the symlink floor instead of re-registering — the ONLY path that
// reaches the automatic-retreat-removed convertFPToSymlinkHeld, gated to explicit
// operator request. The daemon owns the select gate a CLI-side re-register would
// race, so this routes through it and refuses when it is down. It shares
// migrateTimeout: each re-register is a Teardown+Setup that can take seconds to
// materialize.
func (c *Client) FPRepair(account *int, retreat bool) (*Response, error) {
	return c.do(Request{Op: OpFPRepair, Account: account, Retreat: retreat}, migrateTimeout)
}

// fpBridgeCheckTimeout bounds the on-demand bridge self-test: a Manifest+Read
// round-trip over the group-container socket.
const fpBridgeCheckTimeout = 15 * time.Second

// FPBridgeCheck asks the daemon to self-test the File Provider content bridge and
// return its data-plane verdict — the bound-but-dead / consent-parked / down
// distinction the dial-only FPBridgeUp cannot make. Also refreshes the daemon's
// fp.bridge ledger row.
func (c *Client) FPBridgeCheck() (*Response, error) {
	return c.do(Request{Op: OpFPBridgeCheck}, fpBridgeCheckTimeout)
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
