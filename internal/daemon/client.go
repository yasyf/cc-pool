package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/lifeproto"
)

// ErrDaemonUnavailable means the daemon socket could not be reached.
var ErrDaemonUnavailable = errors.New("daemon not running")

// ErrDaemonBuildMismatch means the authenticated transport peer is not this
// exact cc-pool build.
var ErrDaemonBuildMismatch = errors.New("daemon build mismatch")

// ErrAccountMutationCancelled means the daemon durably aborted the operation.
var ErrAccountMutationCancelled = errors.New("account mutation cancelled")

// ErrAccountMutationSuperseded means account/removal state invalidated the operation.
var ErrAccountMutationSuperseded = errors.New("account mutation superseded")

// ErrAccountMutationQuarantined means external credential state needs manual recovery.
var ErrAccountMutationQuarantined = errors.New("account mutation quarantined")

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
	socket         string
	build          string
	lifecycleBuild string

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
		socket:         pool.SocketPath(),
		build:          BusinessBuild,
		lifecycleBuild: version.String(),
		sessions:       make(map[*clientSession]struct{}),
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

// CredMove asks the daemon to move account credentials to the given backend.
func (c *Client) CredMove(account *int, to string) (*Response, error) {
	return c.do(Request{Op: OpCredMove, Account: account, To: to}, 60*time.Second)
}

// RemoveAccount durably deprovisions one account before deleting its source state.
func (c *Client) RemoveAccount(ctx context.Context, account int, deleteCredential bool) error {
	r, err := c.doContext(ctx, Request{
		Op: OpAccountRemove, Account: &account, DeleteCredential: deleteCredential,
	}, 4*time.Minute)
	if err != nil {
		return err
	}
	if !r.OK {
		return errors.New(r.Error)
	}
	return nil
}

// AccountIdentity returns worker-validated identity metadata for one stored account.
func (c *Client) AccountIdentity(ctx context.Context, account int) (AccountIdentityResult, error) {
	if account <= 0 {
		return AccountIdentityResult{}, errors.New("account identity requires a positive account ID")
	}
	response, err := c.doContext(ctx, Request{
		Op: OpAccountIdentity, Account: &account,
	}, 32*time.Second)
	if err != nil {
		return AccountIdentityResult{}, err
	}
	if !response.OK {
		return AccountIdentityResult{}, errors.New(response.Error)
	}
	if response.AccountIdentity == nil {
		return AccountIdentityResult{}, errors.New("daemon returned no account identity")
	}
	identity := *response.AccountIdentity
	if identity.AccountID != account {
		return AccountIdentityResult{}, fmt.Errorf(
			"daemon returned account identity %d, want %d", identity.AccountID, account,
		)
	}
	if identity.AccountUUID == "" {
		return AccountIdentityResult{}, errors.New("daemon returned account identity without an account UUID")
	}
	return identity, nil
}

// AccountHealth verifies one stored account's backing identity and credential stores.
func (c *Client) AccountHealth(ctx context.Context, account int) error {
	if account <= 0 {
		return errors.New("account health requires a positive account ID")
	}
	response, err := c.doContext(ctx, Request{
		Op: OpAccountHealth, Account: &account,
	}, 62*time.Second)
	if err != nil {
		return err
	}
	if !response.OK {
		return errors.New(response.Error)
	}
	if response.AccountHealth == nil {
		return errors.New("daemon returned no account health proof")
	}
	if response.AccountHealth.AccountID != account {
		return fmt.Errorf(
			"daemon returned account health %d, want %d", response.AccountHealth.AccountID, account,
		)
	}
	return nil
}

// AccountMutation executes one exact daemon-owned mutation transition.
func (c *Client) AccountMutation(
	ctx context.Context,
	request AccountMutationRequest,
) (AccountMutationResult, error) {
	response, err := c.doContext(ctx, Request{
		Op: OpAccountMutation, Mutation: &request,
	}, 6*time.Minute)
	if err != nil {
		return AccountMutationResult{}, err
	}
	if !response.OK {
		return AccountMutationResult{}, errors.New(response.Error)
	}
	if response.AccountMutation == nil {
		return AccountMutationResult{}, errors.New("daemon returned no account mutation result")
	}
	return *response.AccountMutation, nil
}

// AccountMutationTerminal attaches this terminal to the daemon-supervised
// interactive writer. The daemon owns the child, journal, verification, and
// compensation; the client carries only terminal bytes.
func (c *Client) AccountMutationTerminal(
	ctx context.Context,
	request AccountMutationRequest,
	stdin *os.File,
	stdout io.Writer,
	onURL func(context.Context, string) error,
) (AccountMutationResult, error) {
	if request.Action != AccountMutationStartOrAttach ||
		request.Fence != (AccountMutationFence{}) || request.TerminalCursor != nil {
		return AccountMutationResult{}, errors.New("terminal mutation must start or attach without a fence or terminal cursor")
	}
	if stdin == nil {
		return AccountMutationResult{}, errors.New("terminal mutation stdin is required")
	}
	if stdout == nil {
		return AccountMutationResult{}, errors.New("terminal mutation stdout is required")
	}
	initial, err := c.AccountMutation(ctx, request)
	if err != nil {
		return AccountMutationResult{}, err
	}
	if err := validateAccountMutationTerminalResult(initial, request.Kind, request.AccountID, nil); err != nil {
		return AccountMutationResult{}, err
	}
	if accountMutationTerminalState(initial.State) {
		return c.settleAccountMutationResult(ctx, initial)
	}
	if initial.Fence.CanonicalOperationID == ([32]byte{}) {
		return AccountMutationResult{}, errors.New("daemon returned no account mutation fence")
	}
	request.Action = AccountMutationProvideInput
	request.Fence = initial.Fence
	endpoint, err := newAccountMutationTerminalEndpoint(ctx, c, request)
	if err != nil {
		return AccountMutationResult{}, err
	}
	defer endpoint.Close()
	if initial.State == AccountMutationAwaitingInput {
		const prompt = "Press enter to begin the daemon-owned login session.\r\n"
		if count, writeErr := io.WriteString(stdout, prompt); writeErr != nil {
			return AccountMutationResult{}, writeErr
		} else if count != len(prompt) {
			return AccountMutationResult{}, io.ErrShortWrite
		}
	}
	resizeSource := stdin
	if outputFile, ok := stdout.(*os.File); ok {
		resizeSource = outputFile
	}
	if err := supervise.RunTerminalClient(ctx, supervise.TerminalClientConfig{
		Endpoint: endpoint, Stdin: stdin, Stdout: stdout, ResizeSource: resizeSource, OnURL: onURL,
	}); err != nil {
		return AccountMutationResult{}, err
	}
	mutation, ok := endpoint.Result()
	if !ok {
		return AccountMutationResult{}, errors.New("daemon terminal stream ended without a mutation result")
	}
	return c.settleAccountMutationResult(ctx, mutation)
}

type accountMutationTerminalEndpoint struct {
	client  *Client
	request AccountMutationRequest

	mu           sync.Mutex
	session      *clientSession
	call         *wire.ClientCall
	nextSequence uint64
	haveCursor   bool
	result       AccountMutationResult
	settled      bool
	closed       bool
	stateChanged chan struct{}
}

func newAccountMutationTerminalEndpoint(
	ctx context.Context,
	client *Client,
	request AccountMutationRequest,
) (*accountMutationTerminalEndpoint, error) {
	endpoint := &accountMutationTerminalEndpoint{
		client: client, request: request, stateChanged: make(chan struct{}),
	}
	if err := endpoint.connect(ctx); err != nil {
		return nil, err
	}
	return endpoint, nil
}

func (e *accountMutationTerminalEndpoint) Send(
	ctx context.Context,
	input supervise.TerminalInput,
) error {
	payload, err := encodeAccountTerminalInput(input)
	if err != nil {
		return err
	}
	var call *wire.ClientCall
	for call == nil {
		e.mu.Lock()
		if e.settled {
			e.mu.Unlock()
			return nil
		}
		if e.closed {
			e.mu.Unlock()
			return errors.New("account mutation terminal is closed")
		}
		call = e.call
		changed := e.stateChanged
		e.mu.Unlock()
		if call == nil {
			select {
			case <-changed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	err = call.SendChunk(ctx, payload)
	if err == nil || !errors.Is(err, wire.ErrCallDone) {
		return err
	}
	for {
		e.mu.Lock()
		if e.settled {
			e.mu.Unlock()
			return nil
		}
		if e.call != call || e.closed {
			e.mu.Unlock()
			return err
		}
		changed := e.stateChanged
		e.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (e *accountMutationTerminalEndpoint) Receive(
	ctx context.Context,
) (supervise.TerminalOutput, error) {
	for {
		e.mu.Lock()
		call := e.call
		e.mu.Unlock()
		if call == nil {
			return supervise.TerminalOutput{}, errors.New("account mutation terminal is detached")
		}
		select {
		case chunk, ok := <-call.Chunks():
			if ok {
				return e.decodeOutput(chunk.Payload)
			}
		case <-ctx.Done():
			return supervise.TerminalOutput{}, ctx.Err()
		}
		result, err := call.Response(ctx)
		if err != nil || result.Outcome != wire.Delivered {
			if ctx.Err() != nil {
				return supervise.TerminalOutput{}, ctx.Err()
			}
			failure := &CallError{
				Op: wire.Op(OpAccountMutation), Outcome: result.Outcome,
				Reason: result.Response.Reason, Err: err,
			}
			if !retryableTerminalCall(result, err) {
				e.drop(call, false)
				return supervise.TerminalOutput{}, failure
			}
			e.drop(call, true)
			if reconnectErr := e.reconnect(ctx); reconnectErr != nil {
				return supervise.TerminalOutput{}, errors.Join(failure, reconnectErr)
			}
			continue
		}
		mutation, decodeErr := decodeAccountMutationTerminalResult(result)
		if decodeErr != nil {
			e.drop(call, false)
			return supervise.TerminalOutput{}, decodeErr
		}
		if validateErr := validateAccountMutationTerminalResult(
			mutation, e.request.Kind, e.request.AccountID, &e.request.Fence,
		); validateErr != nil {
			e.drop(call, false)
			return supervise.TerminalOutput{}, validateErr
		}
		if !accountMutationTerminalState(mutation.State) {
			e.drop(call, false)
			return supervise.TerminalOutput{}, errors.New("daemon terminal stream ended before the mutation settled")
		}
		e.complete(call, mutation)
		return supervise.TerminalOutput{}, io.EOF
	}
}

func (e *accountMutationTerminalEndpoint) Result() (AccountMutationResult, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.result, e.settled
}

func (e *accountMutationTerminalEndpoint) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	call := e.call
	session := e.session
	e.call = nil
	e.session = nil
	e.signalStateLocked()
	e.mu.Unlock()
	if call != nil {
		call.Cancel()
	}
	if session != nil {
		e.client.release(session)
	}
}

func (e *accountMutationTerminalEndpoint) connect(ctx context.Context) error {
	e.mu.Lock()
	request := e.request
	if e.haveCursor {
		cursor := e.nextSequence
		request.TerminalCursor = &cursor
	}
	e.mu.Unlock()
	payload, err := json.Marshal(Request{Op: OpAccountMutation, Mutation: &request})
	if err != nil {
		return err
	}
	session, err := e.client.acquire(ctx)
	if err != nil {
		return err
	}
	call, err := session.wire.Open(ctx, wire.Op(OpAccountMutation), "", payload, false)
	if err != nil {
		e.client.retire(session)
		e.client.release(session)
		return err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		call.Cancel()
		e.client.release(session)
		return errors.New("account mutation terminal is closed")
	}
	e.session = session
	e.call = call
	e.signalStateLocked()
	e.mu.Unlock()
	return nil
}

func (e *accountMutationTerminalEndpoint) reconnect(ctx context.Context) error {
	delay := 25 * time.Millisecond
	for {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
		if err := e.connect(ctx); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else if errors.Is(err, ErrDaemonBuildMismatch) || errors.Is(err, wire.ErrProtectedSessionRequired) {
			return err
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func (e *accountMutationTerminalEndpoint) drop(call *wire.ClientCall, retire bool) {
	e.mu.Lock()
	if e.call != call {
		e.mu.Unlock()
		return
	}
	session := e.session
	e.call = nil
	e.session = nil
	e.signalStateLocked()
	e.mu.Unlock()
	if session == nil {
		return
	}
	if retire {
		e.client.retire(session)
	}
	e.client.release(session)
}

func (e *accountMutationTerminalEndpoint) complete(
	call *wire.ClientCall,
	mutation AccountMutationResult,
) {
	e.mu.Lock()
	if e.call != call {
		e.mu.Unlock()
		return
	}
	session := e.session
	e.call = nil
	e.session = nil
	e.result = mutation
	e.settled = true
	e.signalStateLocked()
	e.mu.Unlock()
	if session != nil {
		e.client.release(session)
	}
}

func (e *accountMutationTerminalEndpoint) signalStateLocked() {
	close(e.stateChanged)
	e.stateChanged = make(chan struct{})
}

func (e *accountMutationTerminalEndpoint) decodeOutput(payload []byte) (supervise.TerminalOutput, error) {
	if len(payload) <= 8 || len(payload) > supervise.TerminalChunkSize+8 {
		return supervise.TerminalOutput{}, errors.New("daemon terminal output frame is empty or oversized")
	}
	sequence := binary.BigEndian.Uint64(payload[:8])
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.haveCursor && sequence != e.nextSequence {
		return supervise.TerminalOutput{}, fmt.Errorf(
			"daemon terminal output sequence %d, want %d", sequence, e.nextSequence,
		)
	}
	if sequence == ^uint64(0) {
		return supervise.TerminalOutput{}, errors.New("daemon terminal output sequence exhausted")
	}
	e.nextSequence = sequence + 1
	e.haveCursor = true
	return supervise.TerminalOutput{
		Sequence: sequence, Data: append([]byte(nil), payload[8:]...),
	}, nil
}

func decodeAccountMutationTerminalResult(result wire.Result) (AccountMutationResult, error) {
	if result.Response.Err != "" {
		return AccountMutationResult{}, errors.New(result.Response.Err)
	}
	var response Response
	if err := decodeStrict(result.Response.Payload, &response); err != nil {
		return AccountMutationResult{}, err
	}
	if !response.OK {
		return AccountMutationResult{}, errors.New(response.Error)
	}
	if response.AccountMutation == nil {
		return AccountMutationResult{}, errors.New("daemon returned no account mutation result")
	}
	return *response.AccountMutation, nil
}

func retryableTerminalCall(result wire.Result, err error) bool {
	if errors.Is(err, ErrDaemonBuildMismatch) || errors.Is(err, wire.ErrProtectedSessionRequired) {
		return false
	}
	switch result.Outcome {
	case wire.PreSendFailure, wire.PostSendFailure, wire.DeliveryUnknown:
		return true
	case wire.Rejected:
		return result.Response.Reason == wire.ErrQueueFull.Error() ||
			result.Response.Reason == wire.ErrDraining.Error()
	default:
		return false
	}
}

func validateAccountMutationTerminalResult(
	result AccountMutationResult,
	kind AccountMutationKind,
	accountID int,
	expectedFence *AccountMutationFence,
) error {
	if result.OperationID == ([32]byte{}) || result.Fence.CanonicalOperationID != result.OperationID {
		return errors.New("daemon returned an invalid account mutation operation fence")
	}
	if result.Kind != kind {
		return fmt.Errorf("daemon returned account mutation kind %q, want %q", result.Kind, kind)
	}
	if result.AccountID <= 0 {
		return errors.New("daemon returned an account mutation without an account ID")
	}
	if accountID > 0 && result.AccountID != accountID {
		return fmt.Errorf("daemon returned account %d, want %d", result.AccountID, accountID)
	}
	if expectedFence != nil && result.Fence != *expectedFence {
		return errors.New("daemon returned a different account mutation fence")
	}
	terminal := accountMutationTerminalState(result.State)
	if !terminal && result.State != AccountMutationAwaitingInput && result.State != AccountMutationApplying {
		return fmt.Errorf("daemon returned unknown account mutation state %q", result.State)
	}
	if result.Completed != (result.State == AccountMutationCompleted) {
		return errors.New("daemon returned inconsistent account mutation completion state")
	}
	return nil
}

func accountMutationTerminalState(state AccountMutationState) bool {
	switch state {
	case AccountMutationCompleted, AccountMutationCancelled,
		AccountMutationSuperseded, AccountMutationQuarantined:
		return true
	default:
		return false
	}
}

func (c *Client) settleAccountMutationResult(
	ctx context.Context,
	mutation AccountMutationResult,
) (AccountMutationResult, error) {
	if err := c.ackAccountMutation(ctx, mutation.OperationID); err != nil {
		return mutation, err
	}
	switch mutation.State {
	case AccountMutationCompleted:
		return mutation, nil
	case AccountMutationCancelled:
		return mutation, ErrAccountMutationCancelled
	case AccountMutationSuperseded:
		return mutation, ErrAccountMutationSuperseded
	case AccountMutationQuarantined:
		return mutation, ErrAccountMutationQuarantined
	default:
		return AccountMutationResult{}, errors.New("account mutation result is not terminal")
	}
}

func (c *Client) ackAccountMutation(ctx context.Context, operationID [32]byte) error {
	ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	delay := 25 * time.Millisecond
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ackCtx, 3*time.Second)
		ack, err := c.doContext(attemptCtx, Request{
			Op: OpAccountMutationAck, MutationReceipt: &operationID,
		}, 3*time.Second)
		attemptCancel()
		if err == nil {
			if ack.OK {
				return nil
			}
			err = errors.New(ack.Error)
		} else if !retryableAccountMutationAck(err) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ackCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return errors.Join(err, ackCtx.Err())
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func retryableAccountMutationAck(err error) bool {
	var callErr *CallError
	if !errors.As(err, &callErr) || errors.Is(err, ErrDaemonBuildMismatch) {
		return false
	}
	if callErr.Outcome == wire.Rejected {
		return callErr.Reason == wire.ErrQueueFull.Error() || callErr.Reason == wire.ErrDraining.Error()
	}
	return callErr.Outcome == wire.PreSendFailure ||
		callErr.Outcome == wire.PostSendFailure || callErr.Outcome == wire.DeliveryUnknown
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
		Dial: wire.UnixDialer(c.socket), Build: c.clientBuild(),
		LifecycleBuild: c.clientLifecycleBuild(), Ladder: ladder,
	})
	if err != nil {
		return nil, fmt.Errorf("connect daemon: %w", err)
	}
	peer := client.PeerBuild()
	if peer.Build != c.clientBuild() {
		mismatch := fmt.Errorf(
			"%w: peer %q, client %q",
			ErrDaemonBuildMismatch,
			peer.Build,
			c.clientBuild(),
		)
		return nil, errors.Join(mismatch, client.Close())
	}
	return &clientSession{wire: client}, nil
}

func (c *Client) clientBuild() string {
	if c.build != "" {
		return c.build
	}
	return BusinessBuild
}

func (c *Client) clientLifecycleBuild() string {
	if c.lifecycleBuild != "" {
		return c.lifecycleBuild
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
