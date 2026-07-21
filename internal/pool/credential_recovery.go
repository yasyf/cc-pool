package pool

import (
	"context"
	"errors"
	"log"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

const (
	credentialRecoveryReceiptPage = 16
	credentialRecoveryLanePage    = 64
	accountRecoveryReceiptPage    = 16
)

var credentialRecoveryClasses = [...]proc.RecoveryClass{
	proc.RecoveryTask,
	proc.RecoverySourceOwner,
}

// RecoverRetiredCredentialOwners performs one receipt-fenced bounded recovery
// pass and drains only while each subsequent page makes durable progress.
func (m *Manager) RecoverRetiredCredentialOwners(ctx context.Context) error {
	recoveryCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopCallerCancellation := context.AfterFunc(ctx, cancel)
	done := make(chan struct{})
	priorDone := m.publishCredentialOwnerRecovery(cancel, done)
	if priorDone != nil {
		<-priorDone
	}
	if err := recoveryCtx.Err(); err != nil {
		stopCallerCancellation()
		cancel()
		m.finishCredentialOwnerRecovery(done)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}
	if m.workers == nil {
		stopCallerCancellation()
		cancel()
		m.finishCredentialOwnerRecovery(done)
		return nil
	}
	remaining, progressed, err := m.recoverCredentialOwnerPass(recoveryCtx)
	if err != nil {
		stopCallerCancellation()
		cancel()
		m.finishCredentialOwnerRecovery(done)
		if recoveryCtx.Err() != nil && ctx.Err() == nil {
			return nil
		}
		return err
	}
	if !remaining || !progressed {
		stopCallerCancellation()
		cancel()
		m.finishCredentialOwnerRecovery(done)
		return nil
	}
	stopCallerCancellation()
	go m.drainCredentialOwnerRecovery(recoveryCtx, done)
	return nil
}

func (m *Manager) publishCredentialOwnerRecovery(
	cancel context.CancelFunc,
	done chan struct{},
) <-chan struct{} {
	m.recoveryMu.Lock()
	defer m.recoveryMu.Unlock()
	priorDone := m.recoveryDone
	if m.recoveryCancel != nil {
		m.recoveryCancel()
	}
	m.recoveryCancel = cancel
	m.recoveryDone = done
	return priorDone
}

func (m *Manager) finishCredentialOwnerRecovery(done chan struct{}) {
	close(done)
	m.recoveryMu.Lock()
	defer m.recoveryMu.Unlock()
	if m.recoveryDone != done {
		return
	}
	m.recoveryCancel = nil
	m.recoveryDone = nil
}

func (m *Manager) recoverCredentialOwnerPage(ctx context.Context) (bool, error) {
	remaining, _, err := m.recoverCredentialOwnerPass(ctx)
	return remaining, err
}

func (m *Manager) recoverCredentialOwnerPass(ctx context.Context) (bool, bool, error) {
	var owner proc.Record
	ownerReady := false
	remaining := false
	progressed := false
	for _, class := range credentialRecoveryClasses {
		page, err := m.workers.reaper.ReapReceipts(
			ctx,
			class,
			proc.ReapReceiptCursor{},
			credentialRecoveryReceiptPage,
		)
		if err != nil {
			return remaining, progressed, err
		}
		remaining = remaining || page.More
		if len(page.Receipts) == 0 {
			continue
		}
		if !ownerReady {
			owner, err = m.credentialOwnerRecord()
			if err != nil {
				return true, progressed, err
			}
			ownerReady = true
		}
		classRemaining, classProgressed, err := m.recoverCredentialOwnerClass(
			ctx, owner, page.Receipts,
		)
		remaining = remaining || classRemaining
		progressed = progressed || classProgressed
		if err != nil {
			return remaining, progressed, err
		}
	}
	return remaining, progressed, nil
}

func (m *Manager) recoverCredentialOwnerClass(
	ctx context.Context,
	owner proc.Record,
	receipts []proc.ReapReceipt,
) (bool, bool, error) {
	remaining := false
	progressed := false
	ackBlocked := false
	for _, receipt := range receipts {
		if err := m.workers.reaper.VerifyReapReceipt(ctx, receipt); err != nil {
			return true, progressed, err
		}
		operations, operationMore, err := m.Store.CredentialOperationsOwnedBy(
			receipt.Record, 0, credentialRecoveryLanePage,
		)
		if err != nil {
			return true, progressed, err
		}
		remaining = remaining || operationMore
		for _, operation := range operations {
			if err := m.recoverCredentialOperation(ctx, operation, receipt); err != nil {
				log.Printf("credential recovery deferred: account=%d token=%s: %v", operation.AccountID, operation.Token, err)
				continue
			}
			progressed = true
		}
		pending, pendingMore, err := m.Store.PendingAddReservationsOwnedBy(
			receipt.Record, 0, credentialRecoveryLanePage,
		)
		if err != nil {
			return true, progressed, err
		}
		remaining = remaining || pendingMore
		for _, reservation := range pending {
			if err := m.Store.ReleaseRetiredPendingAdd(
				ctx, reservation, owner, receipt, m.workers.reaper,
			); err != nil {
				log.Printf("pending add recovery deferred: account=%d: %v", reservation.ID, err)
				continue
			}
			progressed = true
		}
		remainingOperations, _, err := m.Store.CredentialOperationsOwnedBy(receipt.Record, 0, 1)
		if err != nil {
			return true, progressed, err
		}
		remainingMutations, _, err := m.Store.AccountMutationsOwnedBy(receipt.Record, 0, 1)
		if err != nil {
			return true, progressed, err
		}
		remainingPending, _, err := m.Store.PendingAddReservationsOwnedBy(receipt.Record, 0, 1)
		if err != nil {
			return true, progressed, err
		}
		unsettled := len(remainingOperations) != 0 || len(remainingMutations) != 0 || len(remainingPending) != 0
		if unsettled || ackBlocked {
			remaining = true
			ackBlocked = true
			continue
		}
		if _, err := m.workers.reaper.AcknowledgeReap(ctx, receipt); err != nil {
			return true, progressed, err
		}
		progressed = true
	}
	return remaining, progressed, nil
}

// TakeoverRetiredAccountMutationPage transfers one bounded page only after an
// exact daemonkit retirement receipt verifies the old owner. The caller owns
// state-specific account recovery after takeover.
func (m *Manager) TakeoverRetiredAccountMutationPage(
	ctx context.Context,
) ([]store.AccountMutation, bool, error) {
	if m.workers == nil || m.workers.reaper == nil {
		return nil, false, errors.New("account mutation recovery requires daemon worker ownership")
	}
	var owner proc.Record
	ownerReady := false
	taken := make([]store.AccountMutation, 0, credentialRecoveryLanePage)
	more := false
	for _, class := range credentialRecoveryClasses {
		page, err := m.workers.reaper.ReapReceipts(
			ctx,
			class,
			proc.ReapReceiptCursor{},
			accountRecoveryReceiptPage,
		)
		if err != nil {
			return taken, more, err
		}
		more = more || page.More
		if len(page.Receipts) != 0 && !ownerReady {
			owner, err = m.credentialOwnerRecord()
			if err != nil {
				return taken, true, err
			}
			ownerReady = true
		}
		for _, receipt := range page.Receipts {
			if err := m.workers.reaper.VerifyReapReceipt(ctx, receipt); err != nil {
				return taken, true, err
			}
			remaining := credentialRecoveryLanePage - len(taken)
			if remaining == 0 {
				return taken, true, nil
			}
			mutations, ownerMore, err := m.Store.AccountMutationsOwnedBy(
				receipt.Record,
				0,
				remaining,
			)
			if err != nil {
				return taken, true, err
			}
			more = more || ownerMore
			for _, mutation := range mutations {
				recovered, err := m.Store.TakeoverAccountMutation(
					ctx,
					mutation.Fence(),
					owner,
					receipt,
					m.workers.reaper,
				)
				if err != nil {
					return taken, true, err
				}
				taken = append(taken, recovered)
			}
		}
	}
	return taken, more, nil
}

func (m *Manager) drainCredentialOwnerRecovery(ctx context.Context, done chan struct{}) {
	defer m.finishCredentialOwnerRecovery(done)
	for {
		remaining, progressed, err := m.recoverCredentialOwnerPass(ctx)
		if err == nil && !remaining {
			return
		}
		if err != nil {
			log.Printf("credential recovery page deferred: %v", err)
			return
		}
		if !progressed {
			return
		}
		if err := ctx.Err(); err != nil {
			return
		}
	}
}

func (m *Manager) stopCredentialOwnerRecovery() {
	m.recoveryMu.Lock()
	cancel := m.recoveryCancel
	done := m.recoveryDone
	if cancel != nil {
		cancel()
	}
	m.recoveryMu.Unlock()
	if done != nil {
		<-done
	}
	m.recoveryMu.Lock()
	if m.recoveryDone == done {
		m.recoveryCancel = nil
		m.recoveryDone = nil
	}
	m.recoveryMu.Unlock()
}
