package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/lifeproto"
)

// ErrDaemonUnavailable means the daemon socket could not be reached.
var ErrDaemonUnavailable = errors.New("daemon not running")

// CallError preserves daemonkit's delivery proof for one failed operation.
type CallError struct {
	Op      wire.Op
	Outcome wire.Outcome
	Reason  string
	Err     error
}

// Error describes the failed operation and its exact delivery outcome.
func (e *CallError) Error() string {
	detail := e.Reason
	if detail == "" && e.Err != nil {
		detail = e.Err.Error()
	}
	if detail == "" {
		detail = "no terminal response"
	}
	return fmt.Sprintf("daemon call %s %s: %s", e.Op, e.Outcome, detail)
}

// Unwrap returns the underlying transport error, when present.
func (e *CallError) Unwrap() error { return e.Err }

type clientSession struct {
	wire       *wire.Client
	generation uint64
	active     int
	stale      bool
}

// Client maintains one generation-aware persistent daemonkit session. A
// failed operation is never replayed; the next operation opens a new session.
type Client struct {
	socket string
	build  string

	mu         sync.Mutex
	current    *clientSession
	sessions   map[*clientSession]struct{}
	dialing    chan struct{}
	generation uint64
	closed     bool
}

// NewClient returns a lazy client for the default daemon socket and exact build.
func NewClient() *Client {
	return &Client{
		socket:   pool.SocketPath(),
		build:    version.String(),
		sessions: make(map[*clientSession]struct{}),
	}
}

// Close permanently closes every current or retiring daemon session.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	sessions := make([]*clientSession, 0, len(c.sessions))
	for session := range c.sessions {
		sessions = append(sessions, session)
	}
	c.current = nil
	clear(c.sessions)
	c.mu.Unlock()
	var errs []error
	for _, session := range sessions {
		if err := session.wire.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Available reports whether an exact lifecycle session can reach the daemon.
func (c *Client) Available() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.HealthContext(ctx)
	return err == nil
}

func (c *Client) do(req Request, timeout time.Duration) (*Response, error) {
	return c.doContext(context.Background(), req, timeout)
}

func (c *Client) doContext(ctx context.Context, req Request, timeout time.Duration) (*Response, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", req.Op, err)
	}
	result, err := c.call(ctx, wire.Op(req.Op), payload)
	if err != nil {
		return nil, err
	}
	if result.Response.Err != "" {
		return nil, errors.New(result.Response.Err)
	}
	var response Response
	if err := decodeStrict(result.Response.Payload, &response); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", req.Op, err)
	}
	return &response, nil
}

// Select asks the daemon for a provisional account selection. cwd keys
// best-effort session stickiness (empty disables); noFallback rejects a
// least-bad exhausted pick. PID 0 is an inspect-only request; PID > 0 returns
// a launch reservation that must be committed or aborted by token.
func (c *Client) Select(ctx context.Context, account *int, process store.ProcessIdentity, cwd string, noFallback bool, excludeIDs []int) (*Response, error) {
	var processStartedAt int64
	if !process.StartedAt.IsZero() {
		processStartedAt = process.StartedAt.UnixMicro()
	}
	return c.doContext(ctx, Request{
		Op: OpSelect, Account: account, PID: process.PID, ProcessStartedAt: processStartedAt,
		Cwd: cwd, NoFallback: noFallback, ExcludeIDs: excludeIDs,
	}, 13*time.Second)
}

// CommitSelection consumes a provisional selection and records its session and
// sticky state. daemonkit's terminal acknowledgement makes the single call's
// delivery outcome exact; post-send failures are never replayed.
func (c *Client) CommitSelection(ctx context.Context, token string) error {
	r, err := c.doContext(ctx, Request{Op: OpSelectCommit, ReservationToken: token}, 3*time.Second)
	if err != nil {
		return err
	}
	if !r.OK {
		return errors.New(r.Error)
	}
	return nil
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

// Health probes the daemon lifecycle.
func (c *Client) Health() (*Response, error) {
	return c.HealthContext(context.Background())
}

// HealthContext probes the daemon lifecycle within ctx's remaining deadline.
func (c *Client) HealthContext(ctx context.Context) (*Response, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	var response lifeproto.HealthResponse
	if err := c.lifecycle(ctx, wire.Op(lifeproto.OpHealth), lifeproto.NewHealthRequest(), &response); err != nil {
		return nil, err
	}
	return &Response{OK: true, Version: response.Build}, nil
}

// migrateTimeout covers a probe mount plus, per account, an 8s mount wait and rollback teardown.
const migrateTimeout = 150 * time.Second

// Migrate asks the daemon to convert accounts to the given overlay kind.
func (c *Client) Migrate(account *int, to string, force bool) (*Response, error) {
	return c.do(Request{Op: OpMigrate, Account: account, To: to, Force: force}, migrateTimeout)
}

// CredMove asks the daemon to move account credentials to the given backend.
func (c *Client) CredMove(account *int, to string) (*Response, error) {
	return c.do(Request{Op: OpCredMove, Account: account, To: to}, migrateTimeout)
}

// FPRepair asks the daemon to re-register or explicitly retreat File Provider domains.
func (c *Client) FPRepair(account *int, retreat bool) (*Response, error) {
	return c.do(Request{Op: OpFPRepair, Account: account, Retreat: retreat}, migrateTimeout)
}

const fpBridgeCheckTimeout = 15 * time.Second

// FPBridgeCheck asks the daemon to self-test the File Provider content bridge.
func (c *Client) FPBridgeCheck() (*Response, error) {
	return c.do(Request{Op: OpFPBridgeCheck}, fpBridgeCheckTimeout)
}

// Shutdown asks the daemon runtime to step down gracefully.
func (c *Client) Shutdown() (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var response lifeproto.ShutdownResponse
	if err := c.lifecycle(ctx, wire.Op(lifeproto.OpShutdown), lifeproto.NewShutdownRequest(), &response); err != nil {
		return nil, err
	}
	if !response.OK {
		return nil, errors.New("daemon shutdown not acknowledged")
	}
	return &Response{OK: true}, nil
}

func (c *Client) lifecycle(ctx context.Context, op wire.Op, message, response any) error {
	payload, err := lifeproto.Encode(message)
	if err != nil {
		return err
	}
	result, err := c.call(ctx, op, payload)
	if err != nil {
		return err
	}
	if result.Response.Err != "" {
		return errors.New(result.Response.Err)
	}
	if err := decodeStrict(result.Response.Payload, response); err != nil {
		return fmt.Errorf("decode lifecycle %s: %w", op, err)
	}
	return nil
}

func (c *Client) call(ctx context.Context, op wire.Op, payload []byte) (wire.Result, error) {
	session, err := c.acquire(ctx)
	if err != nil {
		return wire.Result{Outcome: wire.PreSendFailure}, &CallError{Op: op, Outcome: wire.PreSendFailure, Err: err}
	}
	result, callErr := session.wire.Call(ctx, op, "", payload)
	if callErr != nil || result.Outcome != wire.Delivered {
		c.retire(session)
	}
	c.release(session)
	if callErr != nil || result.Outcome != wire.Delivered {
		return result, &CallError{Op: op, Outcome: result.Outcome, Reason: result.Response.Reason, Err: callErr}
	}
	return result, nil
}

func (c *Client) acquire(ctx context.Context) (*clientSession, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errors.New("daemon client closed")
		}
		if c.current != nil && !c.current.stale {
			session := c.current
			session.active++
			c.mu.Unlock()
			return session, nil
		}
		if c.dialing != nil {
			dialing := c.dialing
			c.mu.Unlock()
			select {
			case <-dialing:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		dialing := make(chan struct{})
		c.dialing = dialing
		c.mu.Unlock()

		session, err := c.dial(ctx)
		c.mu.Lock()
		c.dialing = nil
		if err == nil && !c.closed {
			if c.sessions == nil {
				c.sessions = make(map[*clientSession]struct{})
			}
			c.generation++
			session.generation = c.generation
			session.active = 1
			c.current = session
			c.sessions[session] = struct{}{}
		}
		closed := c.closed
		close(dialing)
		c.mu.Unlock()
		if err != nil {
			if provesNoListener(err) {
				return nil, ErrDaemonUnavailable
			}
			return nil, err
		}
		if closed {
			_ = session.wire.Close()
			return nil, errors.New("daemon client closed")
		}
		return session, nil
	}
}

func (c *Client) dial(ctx context.Context) (*clientSession, error) {
	ladder, err := operationLadder()
	if err != nil {
		return nil, err
	}
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:   wire.UnixDialer(c.socket),
		Build:  c.clientBuild(),
		Ladder: ladder,
	})
	if err != nil {
		return nil, fmt.Errorf("connect daemon: %w", err)
	}
	return &clientSession{wire: client}, nil
}

func (c *Client) clientBuild() string {
	if c.build != "" {
		return c.build
	}
	return version.String()
}

func (c *Client) retire(session *clientSession) {
	c.mu.Lock()
	session.stale = true
	if c.current == session && c.current.generation == session.generation {
		c.current = nil
	}
	c.mu.Unlock()
}

func (c *Client) release(session *clientSession) {
	var closeSession bool
	c.mu.Lock()
	session.active--
	if session.active < 0 {
		c.mu.Unlock()
		panic("daemon: negative client session references")
	}
	if session.stale && session.active == 0 {
		delete(c.sessions, session)
		closeSession = true
	}
	c.mu.Unlock()
	if closeSession {
		_ = session.wire.Close()
	}
}

func decodeStrict(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func provesNoListener(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
}

func (c *Client) lifecyclePeer() *wire.LifecyclePeer {
	return &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial:  wire.UnixDialer(c.socket),
		Build: c.clientBuild(),
	}}
}

var _ dkdaemon.Peer = (*wire.LifecyclePeer)(nil)
