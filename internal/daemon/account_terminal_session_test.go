package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
)

type accountMutationTestTaskRunner struct {
	credentials pool.Credentials
}

func (r accountMutationTestTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	switch {
	case creds.IsFileWorkerInvocation(task.Args):
		return creds.RunFileWorker(ctx, task.Stdin, task.Stdout)
	case pool.IsBackingWorkerInvocation(task.Args):
		return pool.RunBackingWorker(ctx, task.Stdin, task.Stdout)
	case pool.IsCredentialCASWorkerInvocation(task.Args):
		return runDaemonTestCredentialCAS(ctx, task, r.credentials)
	case procscan.IsWorkerInvocation(task.Args):
		return procscan.RunWorker(ctx, task.Stdin, task.Stdout)
	}
	return errors.New("unexpected account mutation test worker task")
}

type daemonTestCredentialCASRequest struct {
	AccountID       int                           `json:"account_id"`
	ConfigDir       string                        `json:"config_dir"`
	KeychainService string                        `json:"keychain_service"`
	KeychainAccount string                        `json:"keychain_account"`
	Source          creds.Source                  `json:"source"`
	Expected        store.CredentialExternalState `json:"expected"`
	Credential      []byte                        `json:"credential"`
	DeleteOther     bool                          `json:"delete_other"`
	DeleteTarget    bool                          `json:"delete_target"`
	RollbackTarget  []byte                        `json:"rollback_target,omitempty"`
}

type daemonTestCredentialCASResponse struct {
	Before    store.CredentialExternalState `json:"before"`
	After     store.CredentialExternalState `json:"after"`
	ErrorCode string                        `json:"error_code,omitempty"`
	Error     string                        `json:"error,omitempty"`
}

func runDaemonTestCredentialCAS(
	ctx context.Context,
	task supervise.Task,
	credentials pool.Credentials,
) error {
	if credentials == nil {
		return errors.New("daemon test credential CAS requires credentials")
	}
	var request daemonTestCredentialCASRequest
	decoder := json.NewDecoder(task.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return err
	}
	account := store.Account{
		ID: request.AccountID, ConfigDir: request.ConfigDir,
		KeychainService: request.KeychainService, KeychainAccount: request.KeychainAccount,
	}
	before, err := daemonTestCredentialState(ctx, credentials, account)
	if err != nil {
		return json.NewEncoder(task.Stdout).Encode(daemonTestCredentialCASResponse{ErrorCode: "io", Error: err.Error()})
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
		return json.NewEncoder(task.Stdout).Encode(daemonTestCredentialCASResponse{
			Before: before, After: before, ErrorCode: "conflict", Error: "credential changed before compare-and-swap",
		})
	}
	target := credentials.Store(account, request.Source)
	if request.DeleteTarget {
		err = target.Delete(ctx)
	} else {
		var credential creds.Credential
		if err = json.Unmarshal(request.Credential, &credential); err == nil {
			err = target.Write(ctx, &credential)
		}
		if err == nil && request.DeleteOther {
			err = credentials.Store(account, daemonTestOtherCredentialSource(request.Source)).Delete(ctx)
			if err != nil {
				if len(request.RollbackTarget) == 0 {
					_ = target.Delete(ctx)
				} else {
					var rollback creds.Credential
					if decodeErr := json.Unmarshal(request.RollbackTarget, &rollback); decodeErr == nil {
						_ = target.Write(ctx, &rollback)
					}
				}
			}
		}
	}
	after, observeErr := daemonTestCredentialState(ctx, credentials, account)
	if err != nil || observeErr != nil {
		return json.NewEncoder(task.Stdout).Encode(daemonTestCredentialCASResponse{
			Before: before, After: after, ErrorCode: "io", Error: errors.Join(err, observeErr).Error(),
		})
	}
	return json.NewEncoder(task.Stdout).Encode(daemonTestCredentialCASResponse{Before: before, After: after})
}

func daemonTestCredentialState(
	ctx context.Context,
	credentials pool.Credentials,
	account store.Account,
) (store.CredentialExternalState, error) {
	var state store.CredentialExternalState
	for _, source := range []creds.Source{creds.SourceKeychain, creds.SourceFile} {
		credential, err := credentials.Store(account, source).Read(ctx)
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
		if source == creds.SourceKeychain {
			state.Keychain = slot
		} else {
			state.File = slot
		}
	}
	return state, nil
}

func daemonTestOtherCredentialSource(source creds.Source) creds.Source {
	if source == creds.SourceKeychain {
		return creds.SourceFile
	}
	return creds.SourceKeychain
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
	supervise.TerminalInput,
	supervise.TerminalSize,
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
	supervise.TerminalSize,
) (accountMutationTerminal, error) {
	s.terminal.start = func(supervise.TerminalInput) { close(s.started) }
	return s.terminal, nil
}

func (accountMutationTerminalStarter) LoginReady(
	context.Context,
	store.AccountMutation,
) (bool, error) {
	return false, nil
}

func (f accountMutationTerminalRunnerFunc) Start(
	_ context.Context,
	mutation store.AccountMutation,
	size supervise.TerminalSize,
) (accountMutationTerminal, error) {
	terminal := newTestAccountMutationTerminal()
	terminal.start = func(first supervise.TerminalInput) {
		input := make(chan wire.Chunk)
		close(input)
		err := f(
			context.Background(), mutation, first, size, input,
			func(_ context.Context, payload []byte) error {
				terminal.publish(payload)
				return nil
			},
		)
		outcome := supervise.TerminalOutcome{
			Kind: supervise.TerminalExited, Digest: [32]byte{1},
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
	start       func(supervise.TerminalInput)
	outputs     []supervise.TerminalOutput
	notify      chan struct{}
	settled     chan struct{}
	retired     chan struct{}
	outcome     supervise.TerminalOutcome
	err         error
	inputs      []supervise.TerminalInput
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
	spec supervise.TerminalAttachmentSpec,
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
			return nil, supervise.ErrTerminalOutputCursor
		}
	}
	attachment := &testAccountMutationAttachment{terminal: t, cursor: cursor}
	t.attachments[attachment] = struct{}{}
	return attachment, nil
}

func (t *testAccountMutationTerminal) Cancel(context.Context) error {
	t.settle(supervise.TerminalOutcome{
		Kind: supervise.TerminalCanceled, Digest: [32]byte{1},
	}, nil)
	return nil
}

func (t *testAccountMutationTerminal) Wait(ctx context.Context) (supervise.TerminalOutcome, error) {
	select {
	case <-t.settled:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.outcome, t.err
	case <-ctx.Done():
		return supervise.TerminalOutcome{}, ctx.Err()
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
		return supervise.ErrTerminalSettled
	}
	if digest != t.outcome.Digest {
		return supervise.ErrTerminalOutcomeMismatch
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
	t.outputs = append(t.outputs, supervise.TerminalOutput{
		Sequence: uint64(len(t.outputs)), Data: append([]byte(nil), payload...),
	})
	t.mu.Unlock()
	select {
	case t.notify <- struct{}{}:
	default:
	}
}

func (t *testAccountMutationTerminal) settle(outcome supervise.TerminalOutcome, err error) {
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
	_ supervise.TerminalDisconnectPolicy,
	_ time.Duration,
) (supervise.TerminalControllerLease, error) {
	a.terminal.mu.Lock()
	defer a.terminal.mu.Unlock()
	if a.closed {
		return supervise.TerminalControllerLease{}, supervise.ErrTerminalDetached
	}
	select {
	case <-a.terminal.settled:
		return supervise.TerminalControllerLease{}, supervise.ErrTerminalSettled
	default:
	}
	if a.terminal.controller != nil && a.terminal.controller != a {
		return supervise.TerminalControllerLease{}, supervise.ErrTerminalControllerAttached
	}
	a.terminal.controller = a
	return supervise.TerminalControllerLease{Fence: 1, Expires: time.Now().Add(time.Minute)}, nil
}

func (a *testAccountMutationAttachment) RenewControl(
	context.Context,
) (supervise.TerminalControllerLease, error) {
	a.terminal.mu.Lock()
	defer a.terminal.mu.Unlock()
	if a.closed || a.terminal.controller != a {
		return supervise.TerminalControllerLease{}, supervise.ErrTerminalNotController
	}
	return supervise.TerminalControllerLease{Fence: 1, Expires: time.Now().Add(time.Minute)}, nil
}

func (a *testAccountMutationAttachment) Send(
	_ context.Context,
	event supervise.TerminalInput,
) error {
	a.terminal.mu.Lock()
	controller := !a.closed && a.terminal.controller == a
	a.terminal.mu.Unlock()
	if !controller {
		return supervise.ErrTerminalNotController
	}
	a.terminal.mu.Lock()
	a.terminal.inputs = append(a.terminal.inputs, event)
	a.terminal.mu.Unlock()
	a.terminal.startOnce.Do(func() { go a.terminal.start(event) })
	return nil
}

func (a *testAccountMutationAttachment) Receive(
	ctx context.Context,
) (supervise.TerminalOutput, error) {
	for {
		a.terminal.mu.Lock()
		if a.closed {
			a.terminal.mu.Unlock()
			return supervise.TerminalOutput{}, supervise.ErrTerminalDetached
		}
		if a.cursor < uint64(len(a.terminal.outputs)) {
			output := a.terminal.outputs[a.cursor]
			a.cursor++
			a.terminal.mu.Unlock()
			return output, nil
		}
		settled := a.terminal.settled
		notify := a.terminal.notify
		a.terminal.mu.Unlock()
		select {
		case <-settled:
			return supervise.TerminalOutput{}, io.EOF
		case <-notify:
		case <-ctx.Done():
			return supervise.TerminalOutput{}, ctx.Err()
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
