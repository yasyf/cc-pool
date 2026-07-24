package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
)

type accountMutationTestTaskRunner struct {
	credentials pool.Credentials
	refresher   pool.Refresher
}

func (r accountMutationTestTaskRunner) Run(
	ctx context.Context,
	task worker.CommandRequest,
) (worker.CommandResult, error) {
	input := bytes.NewReader(task.Stdin)
	var output bytes.Buffer
	var err error
	switch {
	case pool.IsBackingWorkerInvocation(task.Args):
		err = pool.RunBackingWorker(ctx, input, &output)
	case pool.IsCredentialCASWorkerInvocation(task.Args):
		err = runDaemonTestCredentialCAS(ctx, input, &output, r.credentials, r.refresher)
	case procscan.IsWorkerInvocation(task.Args):
		err = procscan.RunWorker(ctx, input, &output)
	default:
		err = errors.New("unexpected account mutation test worker task")
	}
	return worker.CommandResult{Stdout: output.Bytes()}, err
}

func runDaemonTestCredentialCAS(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
	credentials pool.Credentials,
	refresher pool.Refresher,
) error {
	if credentials == nil {
		return errors.New("daemon test credential CAS requires credentials")
	}
	request, err := pool.DecodeCredentialCASRequest(input)
	if err != nil {
		return err
	}
	account := store.Account{
		ID: request.AccountID, ConfigDir: request.ConfigDir,
		KeychainService: request.KeychainService, KeychainAccount: request.KeychainAccount,
	}
	before, err := daemonTestCredentialState(ctx, credentials, account)
	if err != nil {
		return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{ErrorCode: "io", Error: err.Error()})
	}
	beforeDigest, err := before.Digest()
	if err != nil {
		return err
	}
	expectedDigest, err := request.Expected.Digest()
	if err != nil {
		return err
	}
	if beforeDigest != expectedDigest {
		return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{
			Before: before, After: before, ErrorCode: "conflict", Error: "credential changed before compare-and-swap",
		})
	}
	if request.Refresh {
		return runDaemonTestCredentialRefresh(ctx, output, credentials, refresher, account, request, before)
	}
	if request.Delete {
		err = credentials.Store(account, creds.SourceKeychain).Delete(ctx)
		after, observeErr := daemonTestCredentialState(ctx, credentials, account)
		if err != nil || observeErr != nil {
			return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{
				Before: before, After: after, ErrorCode: "io", Error: errors.Join(err, observeErr).Error(),
			})
		}
		return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{Before: before, After: after})
	}
	target := credentials.Store(account, creds.SourceKeychain)
	var credential creds.Credential
	if err = json.Unmarshal(request.Credential, &credential); err == nil {
		err = target.Write(ctx, &credential)
	}
	after, observeErr := daemonTestCredentialState(ctx, credentials, account)
	if err != nil || observeErr != nil {
		return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{
			Before: before, After: after, ErrorCode: "io", Error: errors.Join(err, observeErr).Error(),
		})
	}
	return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{Before: before, After: after})
}

func runDaemonTestCredentialRefresh(
	ctx context.Context,
	output io.Writer,
	credentials pool.Credentials,
	refresher pool.Refresher,
	account store.Account,
	request pool.CredentialCASRequest,
	before store.CredentialExternalState,
) error {
	if refresher == nil {
		return errors.New("daemon test credential CAS refresh requires refresher")
	}
	target := credentials.Store(account, creds.SourceKeychain)
	previous, err := target.Read(ctx)
	if err != nil {
		return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	response, err := refresher.Refresh(
		ctx, fmt.Sprintf("acct-%d", request.AccountID), previous.ClaudeAiOauth.RefreshToken,
	)
	if err != nil {
		result := pool.CredentialCASResponse{Before: before, After: before, ErrorCode: "io", Error: err.Error()}
		var refreshErr *oauth.RefreshError
		switch {
		case errors.As(err, &refreshErr):
			result.ErrorCode = "refresh"
			result.RefreshStatus = refreshErr.Status
			result.RefreshDigest = refreshErr.ResponseDigest
			result.RefreshInvalidGrant = refreshErr.ConfirmedInvalidGrant
		case errors.Is(err, oauth.ErrNetwork):
			result.ErrorCode = "network"
		}
		return pool.WriteCredentialCASResponse(output, result)
	}
	next := *previous
	next.ClaudeAiOauth.AccessToken = response.AccessToken
	if response.RefreshToken != "" {
		next.ClaudeAiOauth.RefreshToken = response.RefreshToken
	}
	next.ClaudeAiOauth.ExpiresAt = response.Expiry(time.Now()).UnixMilli()
	payload, err := next.Marshal()
	if err == nil {
		err = target.Write(ctx, &next)
	}
	after, observeErr := daemonTestCredentialState(ctx, credentials, account)
	if err != nil || observeErr != nil {
		return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{
			Before: before, After: after, ErrorCode: "io", Error: errors.Join(err, observeErr).Error(),
		})
	}
	return pool.WriteCredentialCASResponse(output, pool.CredentialCASResponse{
		Before: before, After: after, Credential: payload,
	})
}

func daemonTestCredentialState(
	ctx context.Context,
	credentials pool.Credentials,
	account store.Account,
) (store.CredentialExternalState, error) {
	credential, err := credentials.Store(account, creds.SourceKeychain).Read(ctx)
	var slot store.CredentialSlotObservation
	switch creds.ClassifyRead(err) {
	case creds.ReadEmpty:
		slot.State = store.CredentialSlotEmpty
	case creds.ReadPresent:
		payload, marshalErr := credential.Marshal()
		if marshalErr != nil {
			return store.CredentialExternalState{}, marshalErr
		}
		digest := store.CredentialDigest(sha256.Sum256(payload))
		slot = store.CredentialSlotObservation{State: store.CredentialSlotPresent, Digest: &digest}
	case creds.ReadUnsearchable:
		slot.State = store.CredentialSlotUnsearchable
	case creds.ReadFatal:
		slot.State = store.CredentialSlotUnreadable
	}
	return store.CredentialExternalState{Keychain: slot}, nil
}

type accountMutationTestRefresher struct{}

func (accountMutationTestRefresher) Refresh(
	context.Context,
	string,
	string,
) (*oauth.TokenResponse, error) {
	return &oauth.TokenResponse{}, nil
}

func (accountMutationTestRefresher) Usage(context.Context, string) (*oauth.Usage, error) {
	return &oauth.Usage{}, nil
}

type accountMutationTerminalRunnerFunc func(
	context.Context,
	store.AccountMutation,
	accountterminal.TerminalInput,
	accountterminal.TerminalSize,
	<-chan wire.Chunk,
	func(context.Context, []byte) error,
) error

type accountMutationTerminalStarter struct {
	terminal *testAccountMutationTerminal
	started  chan struct{}
}

func (s accountMutationTerminalStarter) Start(
	context.Context,
	store.AccountMutation,
	accountterminal.TerminalSize,
) (accountMutationTerminal, error) {
	s.terminal.start = func(accountterminal.TerminalInput) { close(s.started) }
	return s.terminal, nil
}

func (accountMutationTerminalStarter) LoginReady(
	context.Context,
	store.AccountMutation,
) (bool, error) {
	return false, nil
}

func (f accountMutationTerminalRunnerFunc) Start(
	ctx context.Context,
	mutation store.AccountMutation,
	size accountterminal.TerminalSize,
) (accountMutationTerminal, error) {
	terminal := newTestAccountMutationTerminal()
	terminal.start = func(first accountterminal.TerminalInput) {
		input := make(chan wire.Chunk)
		close(input)
		err := f(
			ctx, mutation, first, size, input,
			func(_ context.Context, payload []byte) error {
				terminal.publish(payload)
				return nil
			},
		)
		outcome := accountterminal.TerminalOutcome{
			Kind: accountterminal.TerminalExited, Digest: [32]byte{1},
		}
		if err != nil {
			outcome.ExitCode = 1
		}
		terminal.settle(outcome, err)
	}
	return terminal, nil
}

func (f accountMutationTerminalRunnerFunc) LoginReady(
	context.Context,
	store.AccountMutation,
) (bool, error) {
	return false, nil
}

type testAccountMutationTerminal struct {
	mu          sync.Mutex
	startOnce   sync.Once
	settleOnce  sync.Once
	retireOnce  sync.Once
	start       func(accountterminal.TerminalInput)
	outputs     []accountterminal.TerminalOutput
	notify      chan struct{}
	settled     chan struct{}
	retired     chan struct{}
	outcome     accountterminal.TerminalOutcome
	err         error
	inputs      []accountterminal.TerminalInput
	controller  *testAccountMutationAttachment
	attachments map[*testAccountMutationAttachment]struct{}
}

type testAccountMutationAttachment struct {
	terminal *testAccountMutationTerminal
	cursor   uint64
	closed   bool
}

func newTestAccountMutationTerminal() *testAccountMutationTerminal {
	return &testAccountMutationTerminal{
		notify: make(chan struct{}, 1), settled: make(chan struct{}), retired: make(chan struct{}),
		attachments: make(map[*testAccountMutationAttachment]struct{}),
	}
}

func (t *testAccountMutationTerminal) Attach(
	ctx context.Context,
	spec accountterminal.TerminalAttachmentSpec,
) (accountMutationTerminalAttachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cursor := uint64(0)
	if spec.Cursor != nil {
		cursor = spec.Cursor.NextSequence
		if cursor > uint64(len(t.outputs)) {
			return nil, accountterminal.ErrTerminalOutputCursor
		}
	}
	attachment := &testAccountMutationAttachment{terminal: t, cursor: cursor}
	t.attachments[attachment] = struct{}{}
	return attachment, nil
}

func (t *testAccountMutationTerminal) Cancel(context.Context) error {
	t.settle(accountterminal.TerminalOutcome{
		Kind: accountterminal.TerminalCanceled, Digest: [32]byte{1},
	}, nil)
	return nil
}

func (t *testAccountMutationTerminal) Wait(ctx context.Context) (accountterminal.TerminalOutcome, error) {
	select {
	case <-t.settled:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.outcome, t.err
	case <-ctx.Done():
		return accountterminal.TerminalOutcome{}, ctx.Err()
	}
}

func (t *testAccountMutationTerminal) Acknowledge(
	_ context.Context,
	digest [32]byte,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.settled:
	default:
		return accountterminal.ErrTerminalSettled
	}
	if digest != t.outcome.Digest {
		return accountterminal.ErrTerminalOutcomeMismatch
	}
	if len(t.attachments) != 0 {
		return errors.New("terminal attachments remain during acknowledgement")
	}
	t.retireOnce.Do(func() { close(t.retired) })
	return nil
}

func (t *testAccountMutationTerminal) Retired() <-chan struct{} {
	return t.retired
}

func (t *testAccountMutationTerminal) publish(payload []byte) {
	t.mu.Lock()
	t.outputs = append(t.outputs, accountterminal.TerminalOutput{
		Sequence: uint64(len(t.outputs)), Data: append([]byte(nil), payload...),
	})
	t.mu.Unlock()
	select {
	case t.notify <- struct{}{}:
	default:
	}
}

func (t *testAccountMutationTerminal) settle(outcome accountterminal.TerminalOutcome, err error) {
	t.settleOnce.Do(func() {
		t.mu.Lock()
		t.outcome, t.err = outcome, err
		t.mu.Unlock()
		close(t.settled)
		select {
		case t.notify <- struct{}{}:
		default:
		}
	})
}

func (a *testAccountMutationAttachment) ClaimControl(
	_ context.Context,
	_ accountterminal.TerminalDisconnectPolicy,
	_ time.Duration,
) (accountterminal.TerminalControllerLease, error) {
	a.terminal.mu.Lock()
	defer a.terminal.mu.Unlock()
	if a.closed {
		return accountterminal.TerminalControllerLease{}, accountterminal.ErrTerminalDetached
	}
	select {
	case <-a.terminal.settled:
		return accountterminal.TerminalControllerLease{}, accountterminal.ErrTerminalSettled
	default:
	}
	if a.terminal.controller != nil && a.terminal.controller != a {
		return accountterminal.TerminalControllerLease{}, accountterminal.ErrTerminalControllerAttached
	}
	a.terminal.controller = a
	return accountterminal.TerminalControllerLease{Fence: 1, Expires: time.Now().Add(time.Minute)}, nil
}

func (a *testAccountMutationAttachment) RenewControl(
	context.Context,
) (accountterminal.TerminalControllerLease, error) {
	a.terminal.mu.Lock()
	defer a.terminal.mu.Unlock()
	if a.closed || a.terminal.controller != a {
		return accountterminal.TerminalControllerLease{}, accountterminal.ErrTerminalNotController
	}
	return accountterminal.TerminalControllerLease{Fence: 1, Expires: time.Now().Add(time.Minute)}, nil
}

func (a *testAccountMutationAttachment) Send(
	_ context.Context,
	event accountterminal.TerminalInput,
) error {
	a.terminal.mu.Lock()
	controller := !a.closed && a.terminal.controller == a
	a.terminal.mu.Unlock()
	if !controller {
		return accountterminal.ErrTerminalNotController
	}
	a.terminal.mu.Lock()
	a.terminal.inputs = append(a.terminal.inputs, event)
	a.terminal.mu.Unlock()
	a.terminal.startOnce.Do(func() { go a.terminal.start(event) })
	return nil
}

func (a *testAccountMutationAttachment) Receive(
	ctx context.Context,
) (accountterminal.TerminalOutput, error) {
	for {
		a.terminal.mu.Lock()
		if a.closed {
			a.terminal.mu.Unlock()
			return accountterminal.TerminalOutput{}, accountterminal.ErrTerminalDetached
		}
		if a.cursor < uint64(len(a.terminal.outputs)) {
			output := a.terminal.outputs[a.cursor]
			a.cursor++
			a.terminal.mu.Unlock()
			return output, nil
		}
		settled := a.terminal.settled
		notify := a.terminal.notify
		select {
		case <-settled:
			a.terminal.mu.Unlock()
			return accountterminal.TerminalOutput{}, io.EOF
		default:
		}
		a.terminal.mu.Unlock()
		select {
		case <-settled:
			continue
		case <-notify:
		case <-ctx.Done():
			return accountterminal.TerminalOutput{}, ctx.Err()
		}
	}
}

func (a *testAccountMutationAttachment) Close() error {
	a.terminal.mu.Lock()
	defer a.terminal.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	delete(a.terminal.attachments, a)
	if a.terminal.controller == a {
		a.terminal.controller = nil
	}
	return nil
}
