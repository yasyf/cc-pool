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
	"time"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
)

// ErrDaemonUnavailable means the daemon socket could not be reached.
var ErrDaemonUnavailable = errors.New("daemon not running")

// ErrDaemonBuildMismatch means the serving daemon is not this exact cc-pool
// build. The wire-era rejection moved to business-attach schema membership;
// this sentinel now names only a health report whose runtime build disagrees.
var ErrDaemonBuildMismatch = errors.New("daemon build mismatch")

// ErrAccountMutationCancelled means the daemon durably aborted the operation.
var ErrAccountMutationCancelled = errors.New("account mutation cancelled")

// ErrAccountMutationSuperseded means account/removal state invalidated the operation.
var ErrAccountMutationSuperseded = errors.New("account mutation superseded")

// ErrAccountMutationQuarantined means external credential state needs manual recovery.
var ErrAccountMutationQuarantined = errors.New("account mutation quarantined")

// pollWaitMillis is the wait every client poll requests, under the protocol's
// MaxPollWaitMillis ceiling.
const pollWaitMillis = 25_000

// Client reaches the daemon over one persistent daemonkit business lane; the
// lane re-acquires its session after any failure and never replays a request.
type Client struct {
	client       *daemonkit.Client
	business     *daemonkit.Business
	runtimeBuild string

	mu     sync.Mutex
	closed bool
}

// NewClient returns a lazy client for the daemon's own identity. The spec is a
// fixed value, so a construction failure is a build defect, not a condition.
func NewClient() *Client {
	client, err := daemonkit.Open(Spec(daemonkit.Program{}, nil))
	if err != nil {
		panic(fmt.Sprintf("daemon: open client for the fixed spec: %v", err))
	}
	return &Client{
		client:       client,
		business:     client.Business(),
		runtimeBuild: version.String(),
	}
}

// Close permanently releases the business lane.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.business.Close(ctx); err != nil && !errors.Is(err, daemonkit.ErrLaneClosed) {
		return err
	}
	return nil
}

// Available reports whether a ready daemon of this exact build answers.
func (c *Client) Available() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.HealthContext(ctx)
	return err == nil
}

func (c *Client) do(req Request, timeout time.Duration) (*Response, error) {
	return c.doContext(context.Background(), req, timeout)
}

// doContext sends one unary op. timeout is this package's default budget for
// the op — a caller that stated its own deadline keeps it, per the fleet
// deadline-budget convention.
func (c *Client) doContext(ctx context.Context, req Request, timeout time.Duration) (*Response, error) {
	if _, stated := ctx.Deadline(); !stated {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", req.Op, err)
	}
	reply, err := c.business.Call(ctx, string(req.Op), payload)
	if err != nil {
		if errors.Is(err, daemonkit.ErrAbsent) {
			return nil, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
		}
		return nil, err
	}
	var response Response
	if err := decodeStrict(reply.Body, &response); err != nil {
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
// sticky state. A post-send failure is never replayed.
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

// Health probes daemon readiness and requires this exact build.
func (c *Client) Health() (*HealthResponse, error) {
	return c.HealthContext(context.Background())
}

// HealthContext probes daemon readiness within ctx's remaining deadline.
func (c *Client) HealthContext(ctx context.Context) (*HealthResponse, error) {
	response, err := c.ObserveHealthContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateDaemonHealth(*response, c.runtimeBuild); err != nil {
		return nil, err
	}
	return response, nil
}

// ObserveHealthContext reads the product half of Health.Detail through the
// control lane, which answers during drain and without readiness. daemonkit
// itself pins the serving PID and generation at attach. Lifecycle readiness
// derives from the live Health.Phase, which daemonkit recomputes on every
// call, folded over the decoded detail: the detail replays the last report,
// so on its own it can say a draining daemon still serves.
func (c *Client) ObserveHealthContext(ctx context.Context) (*HealthResponse, error) {
	if _, stated := ctx.Deadline(); !stated {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	control, err := c.client.Control(ctx)
	if err != nil {
		if errors.Is(err, daemonkit.ErrAbsent) {
			return nil, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
		}
		return nil, err
	}
	defer func() { _ = control.Close(ctx) }()
	health, err := control.Health(ctx)
	if err != nil {
		return nil, err
	}
	return decodeDaemonHealthDetail(health)
}

func decodeDaemonHealthDetail(health daemonkit.Health) (*HealthResponse, error) {
	if len(health.Detail) == 0 {
		return nil, errors.New("daemon health carries no product detail")
	}
	var response HealthResponse
	if err := decodeStrict(health.Detail, &response); err != nil {
		return nil, fmt.Errorf("decode daemon health detail: %w", err)
	}
	if response.Schema != DaemonHealthSchema || response.RuntimeBuild == "" ||
		!validDaemonRuntimeState(response.State) {
		return nil, fmt.Errorf(
			"daemon runtime identity is not exact: schema=%d build=%q state=%q",
			response.Schema, response.RuntimeBuild, response.State,
		)
	}
	response.Ready = response.Ready && health.Phase == daemonkit.PhaseReady
	response.Draining = response.Draining || health.Phase == daemonkit.PhaseDraining
	return &response, nil
}

func validDaemonRuntimeState(state RuntimeState) bool {
	switch state {
	case RuntimeStateHealthy, RuntimeStateDegraded, RuntimeStateFailed:
		return true
	default:
		return false
	}
}

func validateDaemonHealth(response HealthResponse, expectedBuild string) error {
	if response.RuntimeBuild != expectedBuild {
		return fmt.Errorf(
			"%w: build=%q want=%q", ErrDaemonBuildMismatch, response.RuntimeBuild, expectedBuild,
		)
	}
	if response.State != RuntimeStateHealthy || response.Draining || response.Busy || !response.Ready {
		return fmt.Errorf(
			"daemon runtime is not ready: state=%q draining=%t busy=%t ready=%t",
			response.State, response.Draining, response.Busy, response.Ready,
		)
	}
	return nil
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

// AccountMutationTerminal attaches this terminal to the daemon-owned
// interactive writer: input pumps as unary ProvideInput calls, output pages
// through OpAccountMutationPoll. The daemon owns the child, journal,
// verification, and compensation; the client carries only terminal bytes.
func (c *Client) AccountMutationTerminal(
	ctx context.Context,
	request AccountMutationRequest,
	stdin *os.File,
	stdout io.Writer,
	onURL func(context.Context, string) error,
) (AccountMutationResult, error) {
	if request.Action != AccountMutationStartOrAttach ||
		request.Fence != (AccountMutationFence{}) || len(request.Input) != 0 {
		return AccountMutationResult{}, errors.New("terminal mutation must start or attach without a fence or input")
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
	endpoint := &accountMutationTerminalEndpoint{
		client: c,
		request: AccountMutationRequest{
			Kind: request.Kind, Action: AccountMutationProvideInput,
			AccountID: initial.AccountID, Fence: initial.Fence,
		},
	}
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
	if err := accountterminal.RunTerminalClient(ctx, accountterminal.TerminalClientConfig{
		Endpoint: endpoint, Stdin: stdin, Stdout: stdout, ResizeSource: resizeSource, OnURL: onURL,
	}); err != nil {
		return AccountMutationResult{}, err
	}
	final, err := c.AccountMutation(ctx, request)
	if err != nil {
		return AccountMutationResult{}, err
	}
	if err := validateAccountMutationTerminalResult(
		final, request.Kind, request.AccountID, &initial.Fence,
	); err != nil {
		return AccountMutationResult{}, err
	}
	if !accountMutationTerminalState(final.State) {
		return AccountMutationResult{}, errors.New("daemon terminal ended before the mutation settled")
	}
	return c.settleAccountMutationResult(ctx, final)
}

// accountMutationTerminalEndpoint pumps the TUI over the poll-unary lane:
// Send is one ProvideInput per event, Receive pages the replay cursor.
type accountMutationTerminalEndpoint struct {
	client  *Client
	request AccountMutationRequest

	mu       sync.Mutex
	next     uint64
	buffered [][]byte
	done     bool
}

func (e *accountMutationTerminalEndpoint) Send(
	ctx context.Context,
	input accountterminal.TerminalInput,
) error {
	payload, err := encodeAccountTerminalInput(input)
	if err != nil {
		return err
	}
	request := e.request
	request.Input = payload
	response, err := e.client.doContext(ctx, Request{
		Op: OpAccountMutation, Mutation: &request,
	}, 10*time.Second)
	if err != nil {
		return err
	}
	if !response.OK {
		e.mu.Lock()
		done := e.done
		e.mu.Unlock()
		if done {
			return nil
		}
		return errors.New(response.Error)
	}
	return nil
}

func (e *accountMutationTerminalEndpoint) Receive(
	ctx context.Context,
) (accountterminal.TerminalOutput, error) {
	for {
		e.mu.Lock()
		if len(e.buffered) > 0 {
			data := e.buffered[0]
			e.buffered = e.buffered[1:]
			sequence := e.next
			e.next++
			e.mu.Unlock()
			return accountterminal.TerminalOutput{Sequence: sequence, Data: data}, nil
		}
		if e.done {
			e.mu.Unlock()
			return accountterminal.TerminalOutput{}, io.EOF
		}
		cursor := e.next
		e.mu.Unlock()

		page, err := e.client.pollAccountMutation(ctx, AccountMutationPollRequest{
			Fence: e.request.Fence, TerminalCursor: cursor, WaitMillis: pollWaitMillis,
		})
		if err != nil {
			return accountterminal.TerminalOutput{}, err
		}
		e.mu.Lock()
		e.buffered = append(e.buffered, page.Chunks...)
		e.done = e.done || page.Done
		e.mu.Unlock()
	}
}

// pollAccountMutation pages one attachment's replay cursor. The reply is the
// bare typed page — poll failures cross as *daemonkit.ProductError, never an
// in-band envelope.
func (c *Client) pollAccountMutation(
	ctx context.Context,
	poll AccountMutationPollRequest,
) (AccountMutationPollResponse, error) {
	if _, stated := ctx.Deadline(); !stated {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pollWaitMillis*time.Millisecond+5*time.Second)
		defer cancel()
	}
	payload, err := json.Marshal(Request{Op: OpAccountMutationPoll, MutationPoll: &poll})
	if err != nil {
		return AccountMutationPollResponse{}, fmt.Errorf("encode account mutation poll: %w", err)
	}
	reply, err := c.business.Call(ctx, string(OpAccountMutationPoll), payload)
	if err != nil {
		if errors.Is(err, daemonkit.ErrAbsent) {
			return AccountMutationPollResponse{}, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
		}
		return AccountMutationPollResponse{}, err
	}
	var page AccountMutationPollResponse
	if err := decodeStrict(reply.Body, &page); err != nil {
		return AccountMutationPollResponse{}, fmt.Errorf("decode account mutation poll page: %w", err)
	}
	if err := validateAccountMutationPollPage(page, poll.TerminalCursor); err != nil {
		return AccountMutationPollResponse{}, err
	}
	return page, nil
}

func validateAccountMutationPollPage(page AccountMutationPollResponse, cursor uint64) error {
	if page.NextCursor != cursor+uint64(len(page.Chunks)) {
		return fmt.Errorf(
			"daemon poll page cursor %d does not follow %d over %d chunks",
			page.NextCursor, cursor, len(page.Chunks),
		)
	}
	for _, chunk := range page.Chunks {
		if len(chunk) == 0 || len(chunk) > accountterminal.TerminalChunkSize {
			return errors.New("daemon poll chunk is empty or oversized")
		}
	}
	return nil
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
		} else if !daemonkit.Undispatched(err) {
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
