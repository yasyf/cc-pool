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

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

const (
	accountMutationStreamQueue = 32
	accountMutationInputWait   = 5 * time.Minute
	accountMutationReceiptTTL  = 24 * time.Hour
	accountMutationProbeWait   = 5 * time.Second
	accountMutationSettleWait  = 30 * time.Second
	accountMutationRecoverWait = 100 * time.Millisecond
	accountMutationRecoverMax  = 30 * time.Second
)

type accountMutationRun struct {
	ready    chan struct{}
	done     chan struct{}
	terminal accountMutationTerminal
	result   AccountMutationResult
	err      error
	outcome  accountterminal.TerminalOutcome
}

func (s *Server) handleAccountMutationAck(ctx context.Context, request Request) Response {
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
			ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := running.terminal.Acknowledge(ackCtx, running.outcome.Digest)
			cancel()
			if err != nil && !errors.Is(err, accountterminal.ErrTerminalRetentionExpired) {
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
		active, receipt, err = s.exactAccountMutation(ctx, request)
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
		if err := s.prepareCommittedAccountMutation(ctx, *receipt); err != nil {
			return AccountMutationResult{}, err
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
		return AccountMutationResult{}, accountterminal.ErrTerminalOutputCursor
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
	lifetime, cancelLifetime := contextWithoutCancelUntil(ctx, s.accountMutationLifetime.Done())
	go func(ctx context.Context) {
		defer s.wg.Done()
		defer cancelLifetime(context.Canceled)
		s.watchAccountMutationRun(ctx, active, running)
	}(lifetime)
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
			if _, err := s.m.PrepareReservedAdd(
				ctx, accountMutationReservation(active), active.ConfigDir,
			); err != nil {
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
		attachment, err := terminal.Attach(context.WithoutCancel(ctx), accountterminal.TerminalAttachmentSpec{
			Role: accountterminal.TerminalObserver, DisconnectPolicy: accountterminal.DetachOnDisconnect,
		})
		if err == nil {
			startupCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_, err = attachment.ClaimControl(
				startupCtx, accountterminal.DetachOnDisconnect, accountterminal.DefaultTerminalControlLease,
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
	var replay *accountterminal.TerminalOutputCursor
	if cursor != nil {
		replay = &accountterminal.TerminalOutputCursor{NextSequence: *cursor}
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
	cursor *accountterminal.TerminalOutputCursor,
) (accountMutationTerminalAttachment, bool, error) {
	attachment, err := running.terminal.Attach(context.WithoutCancel(ctx), accountterminal.TerminalAttachmentSpec{
		Role: accountterminal.TerminalObserver, DisconnectPolicy: accountterminal.DetachOnDisconnect, Cursor: cursor,
	})
	if err != nil {
		return nil, false, err
	}
	_, err = attachment.ClaimControl(
		ctx, accountterminal.DetachOnDisconnect, accountterminal.DefaultTerminalControlLease,
	)
	switch {
	case err == nil:
		return attachment, true, nil
	case errors.Is(err, accountterminal.ErrTerminalControllerAttached),
		errors.Is(err, accountterminal.ErrTerminalSettled):
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
				if event.Kind == accountterminal.TerminalInputEOF {
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
					ioCtx, accountterminal.DetachOnDisconnect, accountterminal.DefaultTerminalControlLease,
				)
				switch {
				case err == nil:
					close(controlReady)
					controller = true
				case errors.Is(err, accountterminal.ErrTerminalControllerAttached):
				case errors.Is(err, accountterminal.ErrTerminalSettled):
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
		renew := time.NewTicker(accountterminal.DefaultTerminalControlLease / 3)
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
	ctx context.Context,
	mutation store.AccountMutation,
	running *accountMutationRun,
) {
	running.outcome, running.err = waitAccountMutationTerminal(
		ctx, s.accountMutationTerminal, running.terminal, mutation,
	)
	settleCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), accountMutationSettleWait,
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
	case <-ctx.Done():
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
		if errors.Is(err, sql.ErrNoRows) {
			receipt, receiptErr := s.m.Store.AccountMutationReceipt(mutation.OperationID)
			if receiptErr == nil && receipt.Terminal == store.AccountMutationCommitted {
				return AccountMutationResult{}, finishErr
			}
		}
		return AccountMutationResult{}, errors.Join(finishErr, err)
	}
	if current.Kind == store.AccountMutationAdd && current.State == store.AccountMutationCompensating {
		return AccountMutationResult{}, errors.Join(finishErr, store.ErrAccountMutationRecoveryRequired)
	}
	if current.Kind == store.AccountMutationPresentationRebind {
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
	ctx context.Context,
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
	if receipt, err := s.unacknowledgedAccountMutationReceipt(request); err == nil {
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
		if active.State == store.AccountMutationAwaitingPresentation {
			active, err = s.bindAccountMutationPresentation(ctx, active)
			if err != nil {
				return store.AccountMutation{}, nil, err
			}
		}
		return active, nil, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return store.AccountMutation{}, nil, err
	}
	if kind != store.AccountMutationAdd {
		if s.cl == nil {
			return store.AccountMutation{}, nil, errors.New("account mutation claims are required")
		}
		if !s.cl.ownExclusive(request.AccountID) {
			return store.AccountMutation{}, nil, errAccountExclusive
		}
		defer s.cl.releaseExclusive(request.AccountID)
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
	if kind == store.AccountMutationRelogin {
		if _, quarantineErr := s.m.Store.AccountPresentationQuarantine(account.ID); quarantineErr == nil {
			kind = store.AccountMutationPresentationRebind
		} else if !errors.Is(quarantineErr, sql.ErrNoRows) {
			return store.AccountMutation{}, nil, quarantineErr
		}
	}
	intent := accountMutationIntentDigest(kind, account.ID, request.Label)
	var operationID store.AccountMutationID
	var locator, expected store.CredentialDigest
	var previousLocator, previousCredential store.CredentialDigest
	var previousCredentialState store.CredentialSlotState
	switch kind {
	case store.AccountMutationAdd:
		operationID, err = store.NewPendingAddMutationID(
			account.ID, account.InstanceID, account.Generation, intent,
		)
	case store.AccountMutationPresentationRebind:
		previousLocator, previousCredentialState, previousCredential, err = s.m.AccountPresentationRebindSourceEvidence(ctx, account)
		if err == nil {
			operationID, err = store.NewPresentationRebindMutationID(
				account.ID, account.InstanceID, account.Generation+1,
				previousLocator, previousCredentialState, previousCredential, intent,
			)
		}
	default:
		expectedState, stateErr := s.m.CredentialExternalState(ctx, account)
		if stateErr != nil {
			return store.AccountMutation{}, nil, stateErr
		}
		expected, err = expectedState.Digest()
		if err != nil {
			return store.AccountMutation{}, nil, err
		}
		locator = store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		)
		operationID, err = store.NewAccountMutationID(
			account.ID, account.InstanceID, account.Generation, kind, locator, expected, intent,
		)
	}
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	owner, err := s.currentAccountMutationOwner()
	if err != nil {
		return store.AccountMutation{}, nil, err
	}
	accountGeneration := account.Generation
	if kind == store.AccountMutationPresentationRebind {
		accountGeneration++
	}
	beginRequest := store.BeginAccountMutationRequest{
		OperationID: operationID, AccountID: account.ID, Kind: kind,
		AccountInstanceID: account.InstanceID, AccountGeneration: accountGeneration,
		IntentDigest: intent, Label: request.Label, AccountUUID: account.AccountUUID, Owner: owner,
	}
	if kind == store.AccountMutationPresentationRebind {
		beginRequest.PreviousConfigDir = account.ConfigDir
		beginRequest.PreviousKeychainService = account.KeychainService
		beginRequest.PreviousKeychainAccount = account.KeychainAccount
		beginRequest.PreviousLocatorDigest = previousLocator
		beginRequest.PreviousCredentialState = previousCredentialState
		beginRequest.PreviousCredentialDigest = previousCredential
	} else if kind != store.AccountMutationAdd {
		beginRequest.LocatorDigest = locator
		beginRequest.ExpectedCredentialDigest = expected
		beginRequest.ConfigDir = account.ConfigDir
		beginRequest.KeychainService = account.KeychainService
		beginRequest.KeychainAccount = account.KeychainAccount
	}
	begin, err := s.m.Store.BeginAccountMutation(ctx, beginRequest)
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
	active = *begin.Active
	if active.State == store.AccountMutationAwaitingPresentation {
		active, err = s.bindAccountMutationPresentation(ctx, active)
		if err != nil {
			return store.AccountMutation{}, nil, err
		}
	}
	return active, nil, nil
}

func (s *Server) bindAccountMutationPresentation(
	ctx context.Context,
	mutation store.AccountMutation,
) (store.AccountMutation, error) {
	account := store.Account{
		ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
		Generation: mutation.AccountGeneration, ConfigDir: pool.FileProviderConfigDir(mutation.AccountID),
		Label: mutation.Label,
	}
	var identity store.FileProviderPresentationIdentity
	var err error
	if s.provisionPresentationIdentity != nil {
		identity, err = s.provisionPresentationIdentity(ctx, account)
	} else {
		if s.tenantCoordinator == nil {
			return store.AccountMutation{}, s.cancelUnboundAccountMutation(
				mutation, errors.New("FuseKit tenant coordinator is unavailable"),
			)
		}
		if err = s.tenantCoordinator.ensureTenant(ctx, account, pool.TenantAccount(account)); err == nil {
			identity, err = expectedPresentationIdentity(account)
		}
	}
	if err != nil {
		return store.AccountMutation{}, s.cancelUnboundAccountMutation(mutation, err)
	}
	publicPath := identity.PublicPath
	account.KeychainService = creds.ServiceName(publicPath)
	account.KeychainAccount = creds.AccountLabel()
	expectedState, err := s.m.CredentialExternalState(ctx, account)
	if err != nil {
		return store.AccountMutation{}, s.cancelUnboundAccountMutation(mutation, err)
	}
	expected, err := expectedState.Digest()
	if err != nil {
		return store.AccountMutation{}, s.cancelUnboundAccountMutation(mutation, err)
	}
	locator := store.CredentialKeychainLocatorDigest(
		account.KeychainService, account.KeychainAccount,
	)
	fence, err := s.m.Store.BindAccountMutationPresentation(
		mutation.Fence(), identity, publicPath, account.KeychainService, account.KeychainAccount,
		locator, expected,
	)
	if err != nil {
		return store.AccountMutation{}, s.cancelUnboundAccountMutation(mutation, err)
	}
	return s.m.Store.AccountMutation(fence.OperationID)
}

func (s *Server) unacknowledgedAccountMutationReceipt(
	request AccountMutationRequest,
) (store.AccountMutationReceipt, error) {
	receipt, err := s.m.Store.UnacknowledgedAccountMutationReceipt(
		store.AccountMutationKind(request.Kind), request.AccountID,
	)
	if err == nil || !errors.Is(err, sql.ErrNoRows) || request.Kind != AccountMutationRelogin {
		return receipt, err
	}
	return s.m.Store.UnacknowledgedAccountMutationReceipt(
		store.AccountMutationPresentationRebind, request.AccountID,
	)
}

func (s *Server) cancelUnboundAccountMutation(
	mutation store.AccountMutation,
	cause error,
) error {
	return errors.Join(cause, s.m.Store.CancelUnboundAccountMutation(mutation.Fence()))
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
	if !accountMutationKindMatchesRequest(mutation.Kind, request.Kind) || mutation.IntentDigest != want ||
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
	if !accountMutationKindMatchesRequest(receipt.Kind, request.Kind) || receipt.IntentDigest != want ||
		(request.Kind != AccountMutationAdd && receipt.AccountID != request.AccountID) {
		return errors.New("unacknowledged account mutation intent does not match request")
	}
	return nil
}

func accountMutationKindMatchesRequest(
	kind store.AccountMutationKind,
	request AccountMutationKind,
) bool {
	return kind == store.AccountMutationKind(request) ||
		(request == AccountMutationRelogin && kind == store.AccountMutationPresentationRebind)
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
		Label: request.Label,
	}
	return account, func() error { return s.m.Store.ReleaseAccountIndex(reservation) }, nil
}

func (s *Server) finishAccountMutation(
	ctx context.Context,
	mutation store.AccountMutation,
) (AccountMutationResult, error) {
	if mutation.State == store.AccountMutationApplying {
		account := accountMutationAccount(mutation)
		var written store.CredentialDigest
		var err error
		if mutation.Kind == store.AccountMutationPresentationRebind {
			written, err = s.m.VerifyAccountPresentationRebindCredential(ctx, mutation)
		} else {
			var state store.CredentialExternalState
			state, err = s.m.CredentialExternalState(ctx, account)
			if err == nil {
				written, err = state.Digest()
			}
			if err == nil && written == mutation.ExpectedCredentialDigest {
				err = errors.New("login exited without changing the credential")
			}
			var credential *creds.Credential
			if err == nil {
				credential, _, err = s.m.ReadCredential(ctx, account)
			}
			if err == nil && (!credential.HasRefreshToken() || credential.Expired()) {
				err = errors.New("login left no usable credential")
			}
			if err == nil && mutation.Kind == store.AccountMutationRelogin {
				err = s.m.AdoptRotatedToken(ctx, account)
			}
			if err == nil {
				state, err = s.m.CredentialExternalState(ctx, account)
			}
			if err == nil {
				written, err = state.Digest()
			}
		}
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
	if mutation.Kind == store.AccountMutationPresentationRebind &&
		mutation.State == store.AccountMutationPublishing {
		fence, _, err := s.m.Store.PublishAccountPresentationRebind(mutation.Fence())
		if err != nil {
			return AccountMutationResult{}, err
		}
		mutation, err = s.m.Store.AccountMutation(fence.OperationID)
		if err != nil {
			return AccountMutationResult{}, err
		}
	}
	if mutation.Kind == store.AccountMutationPresentationRebind &&
		mutation.State == store.AccountMutationRebindPublished {
		receipt, err := s.m.FinalizeAccountPresentationRebind(
			ctx, mutation, time.Now().Add(accountMutationReceiptTTL),
		)
		if err != nil {
			return AccountMutationResult{}, err
		}
		if err := s.prepareCommittedAccountMutation(ctx, receipt); err != nil {
			return AccountMutationResult{}, err
		}
		return accountMutationReceiptResult(receipt), nil
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
	if err := s.prepareCommittedAccountMutation(ctx, receipt); err != nil {
		return AccountMutationResult{}, err
	}
	return accountMutationReceiptResult(receipt), nil
}

func (s *Server) prepareCommittedAccountMutation(
	ctx context.Context,
	receipt store.AccountMutationReceipt,
) error {
	if receipt.Terminal != store.AccountMutationCommitted ||
		(receipt.Kind != store.AccountMutationAdd &&
			receipt.Kind != store.AccountMutationPresentationRebind) {
		return nil
	}
	account, err := s.m.Store.GetAccount(receipt.AccountID)
	if err != nil {
		return fmt.Errorf("read committed acct-%02d: %w", receipt.AccountID, err)
	}
	if account.InstanceID != receipt.AccountInstanceID || account.Generation != receipt.AccountGeneration {
		return fmt.Errorf("prepare committed acct-%02d: account generation changed", receipt.AccountID)
	}
	if account.ConfigDir != receipt.ConfigDir || account.KeychainService != receipt.KeychainService ||
		account.KeychainAccount != receipt.KeychainAccount {
		return fmt.Errorf("prepare committed acct-%02d: credential owner changed", receipt.AccountID)
	}
	presentation, err := s.m.Store.AccountPresentation(receipt.AccountID)
	if err != nil {
		return fmt.Errorf("read committed acct-%02d presentation: %w", receipt.AccountID, err)
	}
	if presentation.AccountInstanceID != account.InstanceID ||
		presentation.AccountGeneration != account.Generation ||
		presentation.Identity != receipt.PresentationIdentity {
		return fmt.Errorf("prepare committed acct-%02d: retained presentation identity changed", receipt.AccountID)
	}
	if receipt.PublicationPending {
		if err := s.m.AdoptRotatedToken(ctx, account); err != nil {
			return fmt.Errorf("publish committed acct-%02d credential: %w", receipt.AccountID, err)
		}
		if err := s.m.Store.MarkAccountMutationPublicationSettled(receipt.OperationID); err != nil {
			return fmt.Errorf("admit committed acct-%02d publication: %w", receipt.AccountID, err)
		}
	}
	return nil
}

func (s *Server) resolveSupersededAccountMutation(
	ctx context.Context,
	current store.AccountMutation,
) (AccountMutationResult, error) {
	if current.Kind == store.AccountMutationPresentationRebind {
		return AccountMutationResult{}, store.ErrAccountMutationRecoveryRequired
	}
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
				s.deferAccountMutationRecovery(mutation) //nolint:contextcheck // recovery follows the holder lifetime, not startup.
			}
		}
		if !more {
			return nil
		}
	}
}

func (s *Server) deferAccountMutationRecovery(
	mutation store.AccountMutation,
) {
	ctx := s.accountMutationLifetime
	if ctx == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		wait := accountMutationRecoverWait
		for {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if receipt, err := s.m.Store.AccountMutationReceipt(mutation.OperationID); err == nil {
				if err := s.prepareCommittedAccountMutation(ctx, receipt); err == nil {
					return
				}
			} else if current, readErr := s.m.Store.AccountMutation(mutation.OperationID); readErr == nil {
				mutation = current
				if err := s.recoverAccountMutation(ctx, mutation); err == nil {
					return
				}
			} else {
				return
			}
			if wait < accountMutationRecoverMax/2 {
				wait *= 2
			} else {
				wait = accountMutationRecoverMax
			}
		}
	}()
}

func (s *Server) recoverPendingAccountMutationPublications(ctx context.Context) error {
	receipts, err := s.m.Store.PendingAccountMutationPublications(store.CredentialOperationPageLimit)
	if err != nil {
		return err
	}
	for _, receipt := range receipts {
		if err := s.prepareCommittedAccountMutation(ctx, receipt); err != nil {
			s.log.Printf(
				"account mutation publication deferred: account=%d operation=%x: %v",
				receipt.AccountID, receipt.OperationID, err,
			)
		}
	}
	return nil
}

func (s *Server) monitorPendingAccountMutationPublications(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.recoverPendingAccountMutationPublications(ctx); err != nil {
				s.log.Printf("account mutation publication scan deferred: %v", err)
			}
		}
	}
}

func (s *Server) recoverAccountMutation(
	ctx context.Context,
	mutation store.AccountMutation,
) error {
	if (mutation.Kind == store.AccountMutationAdd ||
		mutation.Kind == store.AccountMutationPresentationRebind) &&
		mutation.State == store.AccountMutationAwaitingPresentation {
		_, err := s.bindAccountMutationPresentation(ctx, mutation)
		return err
	}
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
	case store.AccountMutationApplied, store.AccountMutationPublishing,
		store.AccountMutationRebindPublished:
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
) (accountterminal.TerminalInput, accountterminal.TerminalSize, bool, error) {
	var size accountterminal.TerminalSize
	timer := time.NewTimer(accountMutationInputWait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return accountterminal.TerminalInput{}, size, false, nil
		case <-timer.C:
			return accountterminal.TerminalInput{}, size, false, nil
		case chunk, ok := <-input:
			if !ok {
				return accountterminal.TerminalInput{}, size, false, nil
			}
			event, err := decodeAccountTerminalInput(chunk)
			if err != nil {
				return accountterminal.TerminalInput{}, size, false, err
			}
			switch event.Kind {
			case accountterminal.TerminalInputResize:
				size = event.Size
			case accountterminal.TerminalInputBytes:
				return event, size, true, nil
			case accountterminal.TerminalInputEOF:
				return accountterminal.TerminalInput{}, size, false, nil
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
		OperationID: [32]byte(mutation.OperationID), Kind: wireAccountMutationKind(mutation.Kind),
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
		OperationID: [32]byte(receipt.OperationID), Kind: wireAccountMutationKind(receipt.Kind),
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

func wireAccountMutationKind(kind store.AccountMutationKind) AccountMutationKind {
	if kind == store.AccountMutationPresentationRebind {
		return AccountMutationRelogin
	}
	return AccountMutationKind(kind)
}

func accountMutationRetainedResult(receipt store.AccountMutationReceipt) AccountMutationResult {
	result := accountMutationReceiptResult(receipt)
	result.State = AccountMutationApplying
	result.Completed = false
	return result
}
