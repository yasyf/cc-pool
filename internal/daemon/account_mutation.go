package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
)

const (
	accountMutationStreamQueue = 32
	accountMutationInputWait   = 5 * time.Minute
	accountMutationReceiptTTL  = 24 * time.Hour
	accountMutationProbeWait   = 5 * time.Second
	accountMutationSettleWait  = 30 * time.Second
)

type accountMutationRun struct {
	ready    chan struct{}
	done     chan struct{}
	terminal accountMutationTerminal
	result   AccountMutationResult
	err      error
	outcome  supervise.TerminalOutcome
}

func (s *Server) handleAccountMutationAck(request Request) Response {
	if request.MutationReceipt == nil || *request.MutationReceipt == ([32]byte{}) {
		return Response{Error: "account mutation receipt is required"}
	}
	operationID := store.AccountMutationID(*request.MutationReceipt)
	s.accountMutationMu.Lock()
	running := s.accountMutationRuns[operationID]
	s.accountMutationMu.Unlock()
	if running != nil {
		select {
		case <-running.ready:
		default:
			return Response{Error: "account mutation terminal is not initialized"}
		}
		select {
		case <-running.done:
		default:
			return Response{Error: "account mutation terminal is not settled"}
		}
		if running.terminal != nil {
			ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := running.terminal.Acknowledge(ackCtx, running.outcome.Digest)
			cancel()
			if err != nil && !errors.Is(err, supervise.ErrTerminalRetentionExpired) {
				return Response{Error: err.Error()}
			}
		}
	}
	if err := s.m.Store.AcknowledgeAccountMutationReceipt(operationID); err != nil {
		return Response{Error: err.Error()}
	}
	s.forgetAccountMutationRun(operationID, running)
	return Response{OK: true}
}

func (s *Server) handleAccountMutationWire(
	ctx context.Context,
	wireRequest wire.Request,
	request Request,
) (any, error) {
	if request.Mutation == nil {
		return nil, errors.New("account mutation request is required")
	}
	if err := validateAccountMutationCommand(*request.Mutation); err != nil {
		return nil, err
	}
	output := make(chan []byte, accountMutationStreamQueue)
	response := &Response{}
	go func() {
		defer close(output)
		result, err := s.runAccountMutation(ctx, *request.Mutation, wireRequest.Chunks, output)
		if err != nil {
			response.Error = err.Error()
			return
		}
		response.OK = true
		response.AccountMutation = &result
	}()
	return wire.StreamResponse{Chunks: output, Value: response}, nil
}

func validateAccountMutationCommand(request AccountMutationRequest) error {
	switch request.Kind {
	case AccountMutationAdd:
		if request.AccountID != 0 {
			return errors.New("add mutation cannot name an existing account")
		}
	case AccountMutationRelogin:
		if request.AccountID <= 0 {
			return errors.New("relogin mutation requires an account")
		}
	case AccountMutationSyncInstall:
		return errors.New("sync installation is daemon-internal")
	default:
		return errors.New("unknown account mutation kind")
	}
	switch request.Action {
	case AccountMutationStartOrAttach:
		if request.Fence != (AccountMutationFence{}) {
			return errors.New("start-or-attach mutation cannot carry a fence")
		}
		if request.TerminalCursor != nil {
			return errors.New("start-or-attach mutation cannot carry a terminal cursor")
		}
	case AccountMutationProvideInput, AccountMutationCancel:
		if request.Fence == (AccountMutationFence{}) || request.Fence.CanonicalOperationID == ([32]byte{}) {
			return errors.New("account mutation fence is required")
		}
		if request.Action == AccountMutationCancel && request.TerminalCursor != nil {
			return errors.New("cancel mutation cannot carry a terminal cursor")
		}
	default:
		return errors.New("unknown account mutation action")
	}
	return nil
}

func (s *Server) runAccountMutation(
	ctx context.Context,
	request AccountMutationRequest,
	input <-chan wire.Chunk,
	output chan<- []byte,
) (AccountMutationResult, error) {
	var active store.AccountMutation
	var receipt *store.AccountMutationReceipt
	var err error
	if request.Action == AccountMutationStartOrAttach {
		active, receipt, err = s.startOrAttachAccountMutation(ctx, request)
	} else {
		active, receipt, err = s.exactAccountMutation(request)
	}
	if err != nil {
		return AccountMutationResult{}, err
	}
	if receipt != nil {
		s.accountMutationMu.Lock()
		running := s.accountMutationRuns[receipt.OperationID]
		s.accountMutationMu.Unlock()
		if running != nil {
			switch request.Action {
			case AccountMutationStartOrAttach:
				return accountMutationRetainedResult(*receipt), nil
			case AccountMutationProvideInput:
				return s.attachAccountMutationRun(
					ctx, running, request.TerminalCursor, input, output,
				)
			}
		}
		return accountMutationReceiptResult(*receipt), nil
	}
	if request.Action == AccountMutationStartOrAttach {
		return accountMutationActiveResult(active), nil
	}
	if request.Action == AccountMutationCancel {
		if active.State != store.AccountMutationAwaitingInput {
			return AccountMutationResult{}, errors.New("account mutation already crossed the input boundary")
		}
		receipt, err := s.m.Store.ResolveAccountMutation(
			active.Fence(), store.AccountMutationAborted,
			active.ExpectedCredentialDigest, nil, time.Now().Add(accountMutationReceiptTTL),
		)
		if err != nil {
			return AccountMutationResult{}, err
		}
		return accountMutationReceiptResult(receipt), nil
	}
	return s.runAttachedAccountMutation(ctx, active, request.TerminalCursor, input, output)
}

func (s *Server) runAttachedAccountMutation(
	ctx context.Context,
	active store.AccountMutation,
	cursor *uint64,
	input <-chan wire.Chunk,
	output chan<- []byte,
) (AccountMutationResult, error) {
	operationID := active.OperationID
	s.accountMutationMu.Lock()
	if s.accountMutationRuns == nil {
		s.accountMutationRuns = make(map[store.AccountMutationID]*accountMutationRun)
	}
	if running := s.accountMutationRuns[active.OperationID]; running != nil {
		s.accountMutationMu.Unlock()
		return s.attachAccountMutationRun(ctx, running, cursor, input, output)
	}
	if cursor != nil {
		s.accountMutationMu.Unlock()
		return AccountMutationResult{}, supervise.ErrTerminalOutputCursor
	}
	running := &accountMutationRun{ready: make(chan struct{}), done: make(chan struct{})}
	s.accountMutationRuns[active.OperationID] = running
	s.accountMutationMu.Unlock()

	active, terminal, attachment, result, err := s.startAccountMutationRun(ctx, active, input)
	if terminal == nil {
		running.result, running.err = result, err
		close(running.ready)
		close(running.done)
		s.forgetAccountMutationRun(operationID, running)
		return result, err
	}
	if s.accountMutationLifetime == nil {
		_ = attachment.Close()
		cancelErr := terminal.Cancel(context.WithoutCancel(ctx))
		result, settleErr := s.settleAccountMutationTerminal(
			ctx, active, errors.Join(errors.New("account mutation lifetime is unavailable"), cancelErr),
		)
		running.result, running.err = result, settleErr
		close(running.ready)
		close(running.done)
		s.forgetAccountMutationRun(operationID, running)
		return result, settleErr
	}
	running.terminal = terminal
	close(running.ready)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.watchAccountMutationRun(active, running)
	}()
	return s.relayAccountMutationRun(ctx, running, attachment, true, input, output)
}

func (s *Server) startAccountMutationRun(
	ctx context.Context,
	active store.AccountMutation,
	input <-chan wire.Chunk,
) (
	store.AccountMutation,
	accountMutationTerminal,
	accountMutationTerminalAttachment,
	AccountMutationResult,
	error,
) {
	if active.State == store.AccountMutationApplying {
		recovered, rearmed, err := s.rearmUnchangedAccountMutation(ctx, active)
		if err != nil {
			return active, nil, nil, AccountMutationResult{}, err
		}
		if !rearmed {
			result, err := s.finishOrQuarantineAccountMutation(ctx, active)
			return active, nil, nil, result, err
		}
		active = recovered
	}

	if active.State == store.AccountMutationAwaitingInput {
		first, size, ok, err := firstAccountMutationInput(ctx, input)
		if err != nil {
			return active, nil, nil, AccountMutationResult{}, err
		}
		if !ok {
			return active, nil, nil, accountMutationActiveResult(active), nil
		}
		fence, err := s.m.Store.MarkAccountMutationInputProvided(
			active.Fence(), accountMutationInputDigest(active.OperationID),
		)
		if err != nil {
			return active, nil, nil, AccountMutationResult{}, err
		}
		active, err = s.m.Store.AccountMutation(fence.OperationID)
		if err != nil {
			return active, nil, nil, AccountMutationResult{}, err
		}
		fence, err = s.m.Store.MarkAccountMutationApplying(active.Fence())
		if err != nil {
			return active, nil, nil, AccountMutationResult{}, err
		}
		active, err = s.m.Store.AccountMutation(fence.OperationID)
		if err != nil {
			return active, nil, nil, AccountMutationResult{}, err
		}
		if active.Kind == store.AccountMutationAdd {
			if _, err := s.m.PrepareReservedAdd(ctx, accountMutationReservation(active)); err != nil {
				result, settleErr := s.settleAccountMutationTerminal(ctx, active, err)
				return active, nil, nil, result, settleErr
			}
		}
		if s.accountMutationTerminal == nil {
			result, err := s.settleAccountMutationTerminal(
				ctx, active, errors.New("daemonkit terminal worker is unavailable"),
			)
			return active, nil, nil, result, err
		}
		startupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		terminal, err := s.accountMutationTerminal.Start(startupCtx, active, size)
		cancel()
		if err != nil {
			result, settleErr := s.settleAccountMutationTerminal(ctx, active, err)
			return active, nil, nil, result, settleErr
		}
		attachment, err := terminal.Attach(context.WithoutCancel(ctx), supervise.TerminalAttachmentSpec{
			Role: supervise.TerminalObserver, DisconnectPolicy: supervise.DetachOnDisconnect,
		})
		if err == nil {
			startupCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_, err = attachment.ClaimControl(
				startupCtx, supervise.DetachOnDisconnect, supervise.DefaultTerminalControlLease,
			)
			if err == nil {
				err = attachment.Send(startupCtx, first)
			}
			cancel()
		}
		if err != nil {
			if attachment != nil {
				_ = attachment.Close()
			}
			cancelErr := terminal.Cancel(context.WithoutCancel(ctx))
			_, waitErr := terminal.Wait(context.WithoutCancel(ctx))
			result, settleErr := s.settleAccountMutationTerminal(
				ctx, active, errors.Join(err, cancelErr, waitErr),
			)
			return active, nil, nil, result, settleErr
		}
		return active, terminal, attachment, AccountMutationResult{}, nil
	}
	result, err := s.finishOrQuarantineAccountMutation(ctx, active)
	return active, nil, nil, result, err
}

func (s *Server) attachAccountMutationRun(
	ctx context.Context,
	running *accountMutationRun,
	cursor *uint64,
	input <-chan wire.Chunk,
	output chan<- []byte,
) (AccountMutationResult, error) {
	select {
	case <-running.ready:
	case <-ctx.Done():
		return AccountMutationResult{}, ctx.Err()
	}
	if running.terminal == nil {
		select {
		case <-running.done:
			return running.result, running.err
		case <-ctx.Done():
			return AccountMutationResult{}, ctx.Err()
		}
	}
	var replay *supervise.TerminalOutputCursor
	if cursor != nil {
		replay = &supervise.TerminalOutputCursor{NextSequence: *cursor}
	}
	attachment, controller, err := claimAccountMutationAttachment(ctx, running, replay)
	if err != nil {
		return AccountMutationResult{}, err
	}
	return s.relayAccountMutationRun(ctx, running, attachment, controller, input, output)
}

func claimAccountMutationAttachment(
	ctx context.Context,
	running *accountMutationRun,
	cursor *supervise.TerminalOutputCursor,
) (accountMutationTerminalAttachment, bool, error) {
	attachment, err := running.terminal.Attach(context.WithoutCancel(ctx), supervise.TerminalAttachmentSpec{
		Role: supervise.TerminalObserver, DisconnectPolicy: supervise.DetachOnDisconnect, Cursor: cursor,
	})
	if err != nil {
		return nil, false, err
	}
	_, err = attachment.ClaimControl(
		ctx, supervise.DetachOnDisconnect, supervise.DefaultTerminalControlLease,
	)
	switch {
	case err == nil:
		return attachment, true, nil
	case errors.Is(err, supervise.ErrTerminalControllerAttached),
		errors.Is(err, supervise.ErrTerminalSettled):
		return attachment, false, nil
	default:
		_ = attachment.Close()
		return nil, false, err
	}
}

func (s *Server) relayAccountMutationRun(
	ctx context.Context,
	running *accountMutationRun,
	attachment accountMutationTerminalAttachment,
	controller bool,
	input <-chan wire.Chunk,
	output chan<- []byte,
) (AccountMutationResult, error) {
	ioCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { _ = attachment.Close() }()
	errorsOut := make(chan error, 2)
	controlReady := make(chan struct{})
	if controller {
		close(controlReady)
	}
	var pumps sync.WaitGroup
	pumps.Add(1)
	go func() {
		defer pumps.Done()
		select {
		case <-controlReady:
		case <-ioCtx.Done():
			return
		}
		for {
			select {
			case <-ioCtx.Done():
				return
			case chunk, ok := <-input:
				if !ok {
					return
				}
				event, err := decodeAccountTerminalInput(chunk)
				if err == nil && event.Kind != 0 {
					err = attachment.Send(ioCtx, event)
				}
				if err != nil {
					select {
					case errorsOut <- err:
					default:
					}
					cancel()
					return
				}
				if event.Kind == supervise.TerminalInputEOF {
					return
				}
			}
		}
	}()
	pumps.Add(1)
	go func() {
		defer pumps.Done()
		if !controller {
			retry := time.NewTicker(25 * time.Millisecond)
			defer retry.Stop()
			for {
				_, err := attachment.ClaimControl(
					ioCtx, supervise.DetachOnDisconnect, supervise.DefaultTerminalControlLease,
				)
				switch {
				case err == nil:
					close(controlReady)
					controller = true
				case errors.Is(err, supervise.ErrTerminalControllerAttached):
				case errors.Is(err, supervise.ErrTerminalSettled):
					return
				default:
					select {
					case errorsOut <- err:
					default:
					}
					cancel()
					return
				}
				if controller {
					break
				}
				select {
				case <-retry.C:
				case <-ioCtx.Done():
					return
				}
			}
		}
		renew := time.NewTicker(supervise.DefaultTerminalControlLease / 3)
		defer renew.Stop()
		for {
			select {
			case <-ioCtx.Done():
				return
			case <-renew.C:
				if _, err := attachment.RenewControl(ioCtx); err != nil {
					select {
					case errorsOut <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	var relayErr error
	for {
		terminalOutput, err := attachment.Receive(ioCtx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			relayErr = err
			break
		}
		payload, err := encodeAccountTerminalOutput(terminalOutput)
		if err == nil {
			err = sendAccountMutationOutput(ioCtx, output, payload)
		}
		if err != nil {
			relayErr = err
			break
		}
	}
	cancel()
	pumps.Wait()
	select {
	case err := <-errorsOut:
		relayErr = errors.Join(relayErr, err)
	default:
	}
	if relayErr != nil {
		return AccountMutationResult{}, relayErr
	}
	select {
	case <-running.done:
		return running.result, running.err
	case <-ctx.Done():
		return AccountMutationResult{}, ctx.Err()
	}
}

func (s *Server) watchAccountMutationRun(
	mutation store.AccountMutation,
	running *accountMutationRun,
) {
	running.outcome, running.err = waitAccountMutationTerminal(
		s.accountMutationLifetime, s.accountMutationTerminal, running.terminal, mutation,
	)
	settleCtx, cancel := context.WithTimeout(
		context.WithoutCancel(s.accountMutationLifetime), accountMutationSettleWait,
	)
	running.result, running.err = s.settleAccountMutationTerminal(
		settleCtx, mutation, running.err,
	)
	cancel()
	close(running.done)
	if !accountMutationTerminalState(running.result.State) {
		s.forgetAccountMutationRun(mutation.OperationID, running)
		return
	}
	select {
	case <-running.terminal.Retired():
	case <-s.accountMutationLifetime.Done():
	}
	s.forgetAccountMutationRun(mutation.OperationID, running)
}

func (s *Server) forgetAccountMutationRun(
	operationID store.AccountMutationID,
	running *accountMutationRun,
) {
	if running == nil {
		return
	}
	s.accountMutationMu.Lock()
	if s.accountMutationRuns[operationID] == running {
		delete(s.accountMutationRuns, operationID)
	}
	s.accountMutationMu.Unlock()
}

func (s *Server) settleAccountMutationTerminal(
	ctx context.Context,
	mutation store.AccountMutation,
	terminalErr error,
) (AccountMutationResult, error) {
	rearmed, unchanged, err := s.rearmUnchangedAccountMutation(ctx, mutation)
	if err != nil {
		return AccountMutationResult{}, errors.Join(terminalErr, err)
	}
	if unchanged {
		if terminalErr == nil {
			terminalErr = errors.New("login exited without changing the credential")
		}
		return accountMutationActiveResult(rearmed), terminalErr
	}
	return s.finishOrQuarantineAccountMutation(ctx, mutation)
}

func (s *Server) rearmUnchangedAccountMutation(
	ctx context.Context,
	mutation store.AccountMutation,
) (store.AccountMutation, bool, error) {
	state, err := s.m.CredentialExternalState(ctx, accountMutationAccount(mutation))
	if err != nil {
		return store.AccountMutation{}, false, err
	}
	observed, err := state.Digest()
	if err != nil {
		return store.AccountMutation{}, false, err
	}
	if observed != mutation.ExpectedCredentialDigest {
		return mutation, false, nil
	}
	fence, err := s.m.Store.RearmAccountMutationInput(mutation.Fence(), observed)
	if err != nil {
		return store.AccountMutation{}, false, err
	}
	rearmed, err := s.m.Store.AccountMutation(fence.OperationID)
	return rearmed, true, err
}

func (s *Server) finishOrQuarantineAccountMutation(
	ctx context.Context,
	mutation store.AccountMutation,
) (AccountMutationResult, error) {
	result, finishErr := s.finishAccountMutation(ctx, mutation)
	if finishErr == nil {
		return result, nil
	}
	current, err := s.m.Store.AccountMutation(mutation.OperationID)
	if err != nil {
		return AccountMutationResult{}, errors.Join(finishErr, err)
	}
	if current.Kind == store.AccountMutationAdd && current.State == store.AccountMutationCompensating {
		return AccountMutationResult{}, errors.Join(finishErr, store.ErrAccountMutationRecoveryRequired)
	}
	state, err := s.m.CredentialExternalState(ctx, accountMutationAccount(current))
	if err != nil {
		return AccountMutationResult{}, errors.Join(finishErr, err)
	}
	observed, err := state.Digest()
	if err != nil {
		return AccountMutationResult{}, errors.Join(finishErr, err)
	}
	receipt, err := s.m.Store.ResolveAccountMutation(
		current.Fence(), store.AccountMutationQuarantined, observed,
		&store.AccountMutationQuarantine{
			FileLocatorDigest: store.CredentialFileLocatorDigest(
				creds.FileCredentialPath(current.ConfigDir),
			),
			Observation: state,
			Reason:      store.CredentialResultAmbiguous,
		},
		time.Now().Add(accountMutationReceiptTTL),
	)
	if err != nil {
		return AccountMutationResult{}, errors.Join(finishErr, err)
	}
	return accountMutationReceiptResult(receipt), nil
}

func (s *Server) exactAccountMutation(
	request AccountMutationRequest,
) (store.AccountMutation, *store.AccountMutationReceipt, error) {
	operationID := store.AccountMutationID(request.Fence.CanonicalOperationID)
	if receipt, err := s.m.Store.AccountMutationReceipt(operationID); err == nil {
		if err := validateAccountMutationReceiptRequest(receipt, request); err != nil {
			return store.AccountMutation{}, nil, err
		}
		return store.AccountMutation{}, &receipt, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return store.AccountMutation{}, nil, err
	}
	active, err := s.m.Store.AccountMutation(operationID)
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	if err := validateAccountMutationActiveRequest(active, request); err != nil {
		return store.AccountMutation{}, nil, err
	}
	if err := s.requireCurrentAccountMutationOwner(active); err != nil {
		return store.AccountMutation{}, nil, err
	}
	return active, nil, nil
}

func (s *Server) startOrAttachAccountMutation(
	ctx context.Context,
	request AccountMutationRequest,
) (store.AccountMutation, *store.AccountMutationReceipt, error) {
	kind := store.AccountMutationKind(request.Kind)
	if receipt, err := s.m.Store.UnacknowledgedAccountMutationReceipt(kind, request.AccountID); err == nil {
		if err := validateAccountMutationReceiptIntent(receipt, request); err != nil {
			return store.AccountMutation{}, nil, err
		}
		return store.AccountMutation{}, &receipt, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return store.AccountMutation{}, nil, err
	}
	var active store.AccountMutation
	var err error
	if kind == store.AccountMutationAdd {
		active, err = s.m.Store.ActiveAccountMutationByKind(kind)
	} else {
		active, err = s.m.Store.ActiveAccountMutation(request.AccountID)
	}
	if err == nil {
		if err := validateAccountMutationActiveIntent(active, request); err != nil {
			return store.AccountMutation{}, nil, err
		}
		if err := s.requireCurrentAccountMutationOwner(active); err != nil {
			return store.AccountMutation{}, nil, err
		}
		return active, nil, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.AccountMutation{}, nil, err
	}

	account, release, err := s.accountMutationSubject(request)
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	committed := false
	defer func() {
		if !committed && release != nil {
			_ = release()
		}
	}()
	expectedState, err := s.m.CredentialExternalState(ctx, account)
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	expected, err := expectedState.Digest()
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	locator := store.CredentialLocatorDigest(
		account.KeychainService, account.KeychainAccount,
		creds.FileCredentialPath(account.ConfigDir),
	)
	intent := accountMutationIntentDigest(kind, account.ID, request.Label)
	operationID, err := store.NewAccountMutationID(
		account.ID, account.InstanceID, account.Generation, kind, locator, expected, intent,
	)
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	owner, err := s.currentAccountMutationOwner()
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	begin, err := s.m.Store.BeginAccountMutation(ctx, store.BeginAccountMutationRequest{
		OperationID: operationID, AccountID: account.ID, Kind: kind,
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest: locator, ExpectedCredentialDigest: expected, IntentDigest: intent,
		ConfigDir: account.ConfigDir, KeychainService: account.KeychainService,
		KeychainAccount: account.KeychainAccount, Label: request.Label,
		AccountUUID: account.AccountUUID, Owner: owner,
	})
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	committed = true
	if begin.Receipt != nil {
		return store.AccountMutation{}, begin.Receipt, nil
	}
	if begin.Active == nil {
		return store.AccountMutation{}, nil, errors.New("store returned no account mutation")
	}
	return *begin.Active, nil, nil
}

func (s *Server) currentAccountMutationOwner() (proc.Record, error) {
	if s.accountMutationOwner != nil {
		return s.accountMutationOwner()
	}
	return s.m.MutationOwner()
}

func (s *Server) requireCurrentAccountMutationOwner(mutation store.AccountMutation) error {
	owner, err := s.currentAccountMutationOwner()
	if err != nil {
		return err
	}
	if mutation.Owner != owner {
		return store.ErrAccountMutationRecoveryRequired
	}
	return nil
}

func validateAccountMutationActiveIntent(
	mutation store.AccountMutation,
	request AccountMutationRequest,
) error {
	want := accountMutationIntentDigest(mutation.Kind, mutation.AccountID, request.Label)
	if mutation.Kind != store.AccountMutationKind(request.Kind) || mutation.IntentDigest != want ||
		(request.Kind != AccountMutationAdd && mutation.AccountID != request.AccountID) {
		return errors.New("active account mutation intent does not match request")
	}
	return nil
}

func validateAccountMutationReceiptIntent(
	receipt store.AccountMutationReceipt,
	request AccountMutationRequest,
) error {
	want := accountMutationIntentDigest(receipt.Kind, receipt.AccountID, request.Label)
	if receipt.Kind != store.AccountMutationKind(request.Kind) || receipt.IntentDigest != want ||
		(request.Kind != AccountMutationAdd && receipt.AccountID != request.AccountID) {
		return errors.New("unacknowledged account mutation intent does not match request")
	}
	return nil
}

func validateAccountMutationActiveRequest(
	mutation store.AccountMutation,
	request AccountMutationRequest,
) error {
	if err := validateAccountMutationActiveIntent(mutation, request); err != nil {
		return err
	}
	want := accountMutationActiveResult(mutation).Fence
	if request.Fence != want {
		return errors.New("account mutation fence does not match active operation")
	}
	return nil
}

func validateAccountMutationReceiptRequest(
	receipt store.AccountMutationReceipt,
	request AccountMutationRequest,
) error {
	if err := validateAccountMutationReceiptIntent(receipt, request); err != nil {
		return err
	}
	want := accountMutationReceiptResult(receipt).Fence
	if request.Fence != want {
		return errors.New("account mutation fence does not match receipt")
	}
	return nil
}

func (s *Server) accountMutationSubject(
	request AccountMutationRequest,
) (store.Account, func() error, error) {
	if request.Kind == AccountMutationRelogin {
		account, err := s.m.Store.GetAccount(request.AccountID)
		return account, nil, err
	}
	reservation, err := s.m.ReserveAdd()
	if err != nil {
		return store.Account{}, nil, err
	}
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation,
		ConfigDir:       pool.AccountDir(reservation.ID),
		KeychainService: creds.ServiceName(pool.AccountDir(reservation.ID)),
		KeychainAccount: creds.AccountLabel(), Label: request.Label,
	}
	return account, func() error { return s.m.Store.ReleaseAccountIndex(reservation) }, nil
}

func (s *Server) finishAccountMutation(
	ctx context.Context,
	mutation store.AccountMutation,
) (AccountMutationResult, error) {
	if mutation.State == store.AccountMutationApplying {
		account := accountMutationAccount(mutation)
		state, err := s.m.CredentialExternalState(ctx, account)
		if err != nil {
			return AccountMutationResult{}, err
		}
		written, err := state.Digest()
		if err != nil {
			return AccountMutationResult{}, err
		}
		if written == mutation.ExpectedCredentialDigest {
			return AccountMutationResult{}, errors.New("login exited without changing the credential")
		}
		credential, _, err := s.m.ReadCredential(ctx, account)
		if err != nil {
			return AccountMutationResult{}, err
		}
		if !credential.HasRefreshToken() || credential.Expired() {
			return AccountMutationResult{}, errors.New("login left no usable credential")
		}
		if err := s.m.AdoptRotatedToken(ctx, account); err != nil {
			return AccountMutationResult{}, err
		}
		if err := s.m.DropDivergentCopy(ctx, account); err != nil {
			return AccountMutationResult{}, err
		}
		state, err = s.m.CredentialExternalState(ctx, account)
		if err != nil {
			return AccountMutationResult{}, err
		}
		written, err = state.Digest()
		if err != nil {
			return AccountMutationResult{}, err
		}
		_, identity, err := s.m.AccountOAuth(ctx, mutation.AccountID, mutation.ConfigDir)
		if err != nil {
			return AccountMutationResult{}, err
		}
		label := mutation.Label
		if label == "" {
			label = pool.LabelForEmail(identity.EmailAddress)
		}
		fence, err := s.m.Store.SetAccountMutationMetadata(
			mutation.Fence(), label, identity.AccountUUID,
		)
		if err != nil {
			return AccountMutationResult{}, err
		}
		fence, err = s.m.Store.MarkAccountMutationApplied(fence, written)
		if err != nil {
			return AccountMutationResult{}, err
		}
		mutation, err = s.m.Store.AccountMutation(fence.OperationID)
		if err != nil {
			return AccountMutationResult{}, err
		}
	}
	if mutation.State == store.AccountMutationApplied {
		fence, err := s.m.Store.MarkAccountMutationPublishing(mutation.Fence())
		if err != nil {
			return AccountMutationResult{}, err
		}
		mutation, err = s.m.Store.AccountMutation(fence.OperationID)
		if err != nil {
			return AccountMutationResult{}, err
		}
	}
	if mutation.State != store.AccountMutationPublishing {
		return AccountMutationResult{}, fmt.Errorf("account mutation cannot finish from %s", mutation.State)
	}
	receipt, err := s.m.Store.CommitAccountMutation(
		mutation.Fence(), time.Now().Add(accountMutationReceiptTTL),
	)
	if err != nil {
		if errors.Is(err, store.ErrAccountMutationSuperseded) {
			current, readErr := s.m.Store.AccountMutation(mutation.OperationID)
			if readErr != nil {
				return AccountMutationResult{}, errors.Join(err, readErr)
			}
			result, resolveErr := s.resolveSupersededAccountMutation(ctx, current)
			return result, errors.Join(resolveErr)
		}
		return AccountMutationResult{}, err
	}
	return accountMutationReceiptResult(receipt), nil
}

func (s *Server) resolveSupersededAccountMutation(
	ctx context.Context,
	current store.AccountMutation,
) (AccountMutationResult, error) {
	if current.Kind != store.AccountMutationAdd {
		state, err := s.m.CredentialExternalState(ctx, accountMutationAccount(current))
		if err != nil {
			return AccountMutationResult{}, err
		}
		observed, err := state.Digest()
		if err != nil {
			return AccountMutationResult{}, err
		}
		receipt, err := s.m.Store.ResolveAccountMutation(
			current.Fence(), store.AccountMutationQuarantined,
			observed, &store.AccountMutationQuarantine{
				FileLocatorDigest: store.CredentialFileLocatorDigest(
					creds.FileCredentialPath(current.ConfigDir),
				),
				Observation: state,
				Reason:      store.CredentialResultChangedUnderfoot,
			}, time.Now().Add(accountMutationReceiptTTL),
		)
		if err != nil {
			return AccountMutationResult{}, err
		}
		return accountMutationReceiptResult(receipt), nil
	}
	if current.CredentialWritten {
		if err := s.m.CompensateCredentialState(
			ctx, accountMutationAccount(current), current.WrittenCredentialDigest,
		); err != nil {
			return AccountMutationResult{}, err
		}
	}
	state, err := s.m.CredentialExternalState(ctx, accountMutationAccount(current))
	if err != nil {
		return AccountMutationResult{}, err
	}
	observed, err := state.Digest()
	if err != nil {
		return AccountMutationResult{}, err
	}
	receipt, err := s.m.Store.ResolveAccountMutation(
		current.Fence(), store.AccountMutationSuperseded,
		observed, nil, time.Now().Add(accountMutationReceiptTTL),
	)
	if err != nil {
		return AccountMutationResult{}, err
	}
	return accountMutationReceiptResult(receipt), nil
}

func (s *Server) recoverRetiredAccountMutations(ctx context.Context) error {
	for {
		mutations, more, err := s.m.TakeoverRetiredAccountMutationPage(ctx)
		if err != nil {
			return err
		}
		for _, mutation := range mutations {
			if err := s.recoverAccountMutation(ctx, mutation); err != nil {
				s.log.Printf(
					"account mutation recovery deferred: account=%d operation=%x state=%s: %v",
					mutation.AccountID, mutation.OperationID, mutation.State, err,
				)
			}
		}
		if !more {
			return nil
		}
	}
}

func (s *Server) recoverAccountMutation(
	ctx context.Context,
	mutation store.AccountMutation,
) error {
	switch mutation.State {
	case store.AccountMutationAwaitingInput:
		return nil
	case store.AccountMutationReserved:
		fence, err := s.m.Store.MarkAccountMutationApplying(mutation.Fence())
		if err != nil {
			return err
		}
		mutation, err = s.m.Store.AccountMutation(fence.OperationID)
		if err != nil {
			return err
		}
		fallthrough
	case store.AccountMutationApplying:
		_, unchanged, err := s.rearmUnchangedAccountMutation(ctx, mutation)
		if err != nil || unchanged {
			return err
		}
		_, err = s.finishOrQuarantineAccountMutation(ctx, mutation)
		return err
	case store.AccountMutationApplied, store.AccountMutationPublishing:
		_, err := s.finishOrQuarantineAccountMutation(ctx, mutation)
		return err
	case store.AccountMutationCompensating:
		_, err := s.resolveSupersededAccountMutation(ctx, mutation)
		return err
	default:
		return fmt.Errorf("recover account mutation from %s: %w", mutation.State, store.ErrAccountMutationState)
	}
}

func accountMutationReservation(mutation store.AccountMutation) store.PendingAccountReservation {
	return store.PendingAccountReservation{
		ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
		Generation: mutation.AccountGeneration, Owner: mutation.Owner,
	}
}

func accountMutationAccount(mutation store.AccountMutation) store.Account {
	return store.Account{
		ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
		Generation: mutation.AccountGeneration, ConfigDir: mutation.ConfigDir,
		KeychainService: mutation.KeychainService, KeychainAccount: mutation.KeychainAccount,
		Label: mutation.Label, AccountUUID: mutation.AccountUUID,
	}
}

func accountMutationIntentDigest(kind store.AccountMutationKind, accountID int, label string) store.CredentialDigest {
	return store.CredentialDigest(sha256.Sum256([]byte(fmt.Sprintf("cc-pool:account-mutation-intent:v1\x00%s\x00%d\x00%s", kind, accountID, label))))
}

func accountMutationInputDigest(operationID store.AccountMutationID) store.CredentialDigest {
	return store.CredentialDigest(sha256.Sum256(append([]byte("cc-pool:terminal-input:v1\x00"), operationID[:]...)))
}

func firstAccountMutationInput(
	ctx context.Context,
	input <-chan wire.Chunk,
) (supervise.TerminalInput, supervise.TerminalSize, bool, error) {
	var size supervise.TerminalSize
	timer := time.NewTimer(accountMutationInputWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return supervise.TerminalInput{}, size, false, nil
		case <-timer.C:
			return supervise.TerminalInput{}, size, false, nil
		case chunk, ok := <-input:
			if !ok {
				return supervise.TerminalInput{}, size, false, nil
			}
			event, err := decodeAccountTerminalInput(chunk)
			if err != nil {
				return supervise.TerminalInput{}, size, false, err
			}
			switch event.Kind {
			case supervise.TerminalInputResize:
				size = event.Size
			case supervise.TerminalInputBytes:
				return event, size, true, nil
			case supervise.TerminalInputEOF:
				return supervise.TerminalInput{}, size, false, nil
			}
		}
	}
}

func sendAccountMutationOutput(ctx context.Context, output chan<- []byte, payload []byte) error {
	select {
	case output <- payload:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func accountMutationActiveResult(mutation store.AccountMutation) AccountMutationResult {
	state := AccountMutationApplying
	if mutation.State == store.AccountMutationAwaitingInput {
		state = AccountMutationAwaitingInput
	}
	return AccountMutationResult{
		OperationID: [32]byte(mutation.OperationID), Kind: AccountMutationKind(mutation.Kind),
		State: state, AccountID: mutation.AccountID, ConfigDir: mutation.ConfigDir,
		Label: mutation.Label, Fence: AccountMutationFence{
			CanonicalOperationID: [32]byte(mutation.OperationID),
			AccountInstanceID:    mutation.AccountInstanceID,
			AccountGeneration:    mutation.AccountGeneration,
			RegistrySequence:     mutation.RegistrySequence,
			CredentialDigest:     [32]byte(mutation.ExpectedCredentialDigest),
		},
	}
}

func accountMutationReceiptResult(receipt store.AccountMutationReceipt) AccountMutationResult {
	state := AccountMutationCancelled
	completed := receipt.Terminal == store.AccountMutationCommitted
	switch receipt.Terminal {
	case store.AccountMutationCommitted:
		state = AccountMutationCompleted
	case store.AccountMutationSuperseded:
		state = AccountMutationSuperseded
	case store.AccountMutationQuarantined:
		state = AccountMutationQuarantined
	}
	return AccountMutationResult{
		OperationID: [32]byte(receipt.OperationID), Kind: AccountMutationKind(receipt.Kind),
		State: state, AccountID: receipt.AccountID, ConfigDir: receipt.ConfigDir,
		Label: receipt.Label, Completed: completed,
		Fence: AccountMutationFence{
			CanonicalOperationID: [32]byte(receipt.OperationID),
			AccountInstanceID:    receipt.AccountInstanceID,
			AccountGeneration:    receipt.AccountGeneration,
			RegistrySequence:     receipt.RegistrySequence,
			CredentialDigest:     [32]byte(receipt.ExpectedCredentialDigest),
		},
	}
}

func accountMutationRetainedResult(receipt store.AccountMutationReceipt) AccountMutationResult {
	result := accountMutationReceiptResult(receipt)
	result.State = AccountMutationApplying
	result.Completed = false
	return result
}
