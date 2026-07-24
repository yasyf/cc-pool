package pool

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

const (
	credentialSettlementTimeout = 5 * time.Minute
	credentialTerminalTTL       = 10 * time.Minute
	credentialWriteReceiptPage  = 64
)

// ErrCredentialOperationFailed reports a terminal failure retained from another process.
var ErrCredentialOperationFailed = errors.New("credential operation failed in another process")

// ErrCredentialOperationQuarantined reports an operation requiring manual recovery.
var ErrCredentialOperationQuarantined = errors.New("credential operation requires manual recovery")

// ErrCredentialOperationReplayed reports that retained evidence determined the result.
var ErrCredentialOperationReplayed = errors.New("credential operation returned retained evidence")

// ErrCredentialOperationLiveProbe reports replay evidence from a live read-only probe.
var ErrCredentialOperationLiveProbe = errors.New("credential operation replay included a live read-only probe")

var errCredentialOperationRetry = errors.New("credential operation state changed before external I/O")

// CredentialWriteSettlement is one exact terminal credential write awaiting
// durable publication by a worker-backed consumer.
type CredentialWriteSettlement struct {
	OperationID        store.CredentialOperationID
	PublicationPayload []byte
}

// CredentialWritePublicationBuilder captures one exact non-secret host-sync
// publication before the terminal credential receipt commits.
type CredentialWritePublicationBuilder func(
	store.Account,
	*creds.Credential,
	store.CredentialOperationID,
	time.Time,
) ([]byte, error)

type credentialOperationCodec[T any] struct {
	target     store.CredentialTarget
	intent     *store.CredentialDigest
	resultCode func(T, error) store.CredentialResultCategory
	replay     func(context.Context, *Manager, store.Account, store.CredentialOperationReceipt) (T, error)
}

type credentialObservationFunc func(
	context.Context,
	store.Account,
) (store.CredentialExternalState, error)

type credentialOperationBoundary struct {
	manager            *Manager
	account            store.Account
	operation          store.CredentialOperation
	expected           store.CredentialExternalState
	observe            credentialObservationFunc
	crossed            bool
	publicationPayload []byte
}

func (boundary *credentialOperationBoundary) recordCredentialWrite(
	credential *creds.Credential,
) error {
	if credential == nil {
		return errors.New("credential publication requires exact written bytes")
	}
	if boundary.manager.BuildCredentialWritePublication == nil {
		return errors.New("credential publication builder is unavailable")
	}
	payload, err := boundary.manager.BuildCredentialWritePublication(
		boundary.account,
		credential,
		boundary.operation.OperationID,
		time.Now(),
	)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > store.CredentialPublicationPayloadMaxBytes {
		return errors.New("credential publication builder returned an invalid payload")
	}
	if boundary.publicationPayload != nil &&
		string(boundary.publicationPayload) != string(payload) {
		return errors.New("credential publication payload changed within one operation")
	}
	boundary.publicationPayload = append(boundary.publicationPayload[:0], payload...)
	if boundary.crossed {
		operation, err := boundary.manager.Store.StageCredentialOperationPublication(
			boundary.operation.Fence(), boundary.publicationPayload,
		)
		if err != nil {
			return err
		}
		boundary.operation = operation
	}
	return nil
}

func (boundary *credentialOperationBoundary) Cross(ctx context.Context) error {
	if boundary.crossed {
		return nil
	}
	owner, err := boundary.manager.credentialOwnerRecord()
	if err != nil {
		return err
	}
	if boundary.operation.Owner != owner {
		return store.ErrCredentialOperationOwner
	}
	actual, err := boundary.observe(ctx, boundary.account)
	if err != nil {
		return err
	}
	if !sameStoreObservation(boundary.expected, actual) {
		return ErrCredentialChangedUnderfoot
	}
	operation, err := boundary.manager.Store.MarkCredentialOperationApplying(
		boundary.operation.Fence(),
		boundary.publicationPayload,
	)
	if err != nil {
		return err
	}
	boundary.operation = operation
	boundary.crossed = true
	return nil
}

type credentialFlight struct {
	signature [32]byte
	done      chan struct{}
	result    any
	err       error
}

func (m *Manager) joinCredentialFlight(
	accountID int,
	signature [32]byte,
) (*credentialFlight, bool) {
	m.credentialMu.Lock()
	defer m.credentialMu.Unlock()
	if m.credentialFlights == nil {
		m.credentialFlights = make(map[int]*credentialFlight)
	}
	if current := m.credentialFlights[accountID]; current != nil {
		return current, false
	}
	flight := &credentialFlight{signature: signature, done: make(chan struct{})}
	m.credentialFlights[accountID] = flight
	return flight, true
}

func (m *Manager) finishCredentialFlight(
	accountID int,
	flight *credentialFlight,
	result any,
	err error,
) {
	m.credentialMu.Lock()
	flight.result = result
	flight.err = err
	delete(m.credentialFlights, accountID)
	close(flight.done)
	m.credentialMu.Unlock()
}

func runCredentialOperation[T any](
	ctx context.Context,
	manager *Manager,
	account store.Account,
	kind store.CredentialOperationKind,
	codec credentialOperationCodec[T],
	fn func(context.Context, *credentialOperationBoundary) (T, error),
	arguments ...string,
) (T, error) {
	return runCredentialOperationObserved(
		ctx,
		manager,
		account,
		kind,
		codec,
		manager.credentialMutationObservation,
		fn,
		arguments...,
	)
}

func runCredentialOperationObserved[T any](
	ctx context.Context,
	manager *Manager,
	account store.Account,
	kind store.CredentialOperationKind,
	codec credentialOperationCodec[T],
	observe credentialObservationFunc,
	fn func(context.Context, *credentialOperationBoundary) (T, error),
	arguments ...string,
) (T, error) {
	var zero T
	expected, err := observe(ctx, account)
	if err != nil {
		return zero, err
	}
	intent := credentialIntentDigest(kind, arguments...)
	if codec.intent != nil {
		intent = *codec.intent
	}
	locator := store.CredentialKeychainLocatorDigest(
		account.KeychainService, account.KeychainAccount,
	)
	operationID, err := store.NewCredentialOperationID(
		account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount,
		kind, codec.target, locator, expected, intent,
	)
	if err != nil {
		return zero, err
	}
	signature := [32]byte(operationID)
	for {
		flight, leader := manager.joinCredentialFlight(account.ID, signature)
		if !leader {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-flight.done:
			}
			if flight.signature != signature {
				continue
			}
			if errors.Is(flight.err, errCredentialOperationRetry) {
				expected, err = observe(ctx, account)
				if err != nil {
					return zero, err
				}
				operationID, err = store.NewCredentialOperationID(
					account.InstanceID, account.Generation, account.ConfigDir,
					account.KeychainService, account.KeychainAccount,
					kind, codec.target, locator, expected, intent,
				)
				if err != nil {
					return zero, err
				}
				signature = [32]byte(operationID)
				continue
			}
			if flight.result == nil {
				return zero, flight.err
			}
			result, ok := flight.result.(T)
			if !ok {
				return zero, errors.New("pool: credential operation result type mismatch")
			}
			return result, flight.err
		}
		claimCredentialMutation := credentialMutationClaim(ctx, manager.ClaimCredentialMutation)
		if claimCredentialMutation == nil {
			err = errors.New("pool: credential mutation claim is required")
			manager.finishCredentialFlight(account.ID, flight, zero, err)
			return zero, err
		}
		releaseCredentialClaim, err := claimCredentialMutation(account.ID)
		if err != nil {
			manager.finishCredentialFlight(account.ID, flight, zero, err)
			return zero, err
		}
		if releaseCredentialClaim == nil {
			err = errors.New("pool: credential mutation claim returned no release")
			manager.finishCredentialFlight(account.ID, flight, zero, err)
			return zero, err
		}
		result, err := executeCredentialOperation(
			ctx,
			manager,
			account,
			kind,
			operationID,
			locator,
			intent,
			expected,
			codec,
			observe,
			fn,
		)
		releaseCredentialClaim()
		manager.finishCredentialFlight(account.ID, flight, result, err)
		if errors.Is(err, errCredentialOperationRetry) {
			expected, err = observe(ctx, account)
			if err != nil {
				return zero, err
			}
			operationID, err = store.NewCredentialOperationID(
				account.InstanceID, account.Generation, account.ConfigDir,
				account.KeychainService, account.KeychainAccount,
				kind, codec.target, locator, expected, intent,
			)
			if err != nil {
				return zero, err
			}
			signature = [32]byte(operationID)
			continue
		}
		return result, err
	}
}

func executeCredentialOperation[T any](
	ctx context.Context,
	manager *Manager,
	account store.Account,
	kind store.CredentialOperationKind,
	operationID store.CredentialOperationID,
	locator store.CredentialDigest,
	intent store.CredentialDigest,
	expected store.CredentialExternalState,
	codec credentialOperationCodec[T],
	observe credentialObservationFunc,
	fn func(context.Context, *credentialOperationBoundary) (T, error),
) (T, error) {
	var zero T
	owner, err := manager.credentialOwnerRecord()
	if err != nil {
		return zero, err
	}
	for {
		begin, err := manager.Store.BeginCredentialOperation(
			store.BeginCredentialOperationRequest{
				OperationID: operationID,
				AccountID:   account.ID, AccountInstanceID: account.InstanceID,
				AccountGeneration: account.Generation,
				ConfigDir:         account.ConfigDir, KeychainService: account.KeychainService,
				KeychainAccount: account.KeychainAccount,
				LocatorDigest:   locator,
				Owner:           owner,
				Kind:            kind,
				Target:          codec.target,
				IntentDigest:    intent,
				Expected:        expected,
			},
		)
		switch {
		case err == nil && begin.Receipt != nil:
			return replayCredentialOperation(ctx, manager, account, codec, *begin.Receipt)
		case errors.Is(err, store.ErrCredentialOperationSettlementRequired):
			receipt := *begin.Receipt
			if credentialReceiptMatchesInvocation(
				receipt, account, kind, codec.target, locator, intent,
			) {
				return replayCredentialOperation(ctx, manager, account, codec, receipt)
			}
			if settleErr := manager.settleCredentialWrite(ctx, receipt); settleErr != nil {
				return zero, settleErr
			}
			if ackErr := manager.Store.AcknowledgeCredentialOperation(receipt.Token); ackErr != nil {
				return zero, ackErr
			}
			return zero, errCredentialOperationRetry
		case err == nil && begin.Created:
			return applyCredentialOperation(
				ctx, manager, account, *begin.Active, expected, codec, observe, fn,
			)
		case err == nil:
			receipt, waitErr := manager.waitCredentialOperation(ctx, begin.Active.Token)
			if errors.Is(waitErr, sql.ErrNoRows) {
				return zero, errCredentialOperationRetry
			}
			if waitErr != nil {
				return zero, waitErr
			}
			return replayCredentialOperation(ctx, manager, account, codec, receipt)
		case errors.Is(err, store.ErrCredentialOperationRecoveryRequired):
			operation := *begin.Active
			receipt, waitErr := manager.waitCredentialOperation(ctx, operation.Token)
			if errors.Is(waitErr, sql.ErrNoRows) {
				return zero, errCredentialOperationRetry
			}
			if waitErr != nil {
				return zero, waitErr
			}
			if operation.OperationID == operationID {
				return replayCredentialOperation(ctx, manager, account, codec, receipt)
			}
			return zero, errCredentialOperationRetry
		case errors.Is(err, store.ErrCredentialOperationBusy):
			if _, waitErr := manager.waitCredentialOperation(ctx, begin.Active.Token); waitErr != nil &&
				!errors.Is(waitErr, sql.ErrNoRows) {
				return zero, waitErr
			}
			return zero, errCredentialOperationRetry
		default:
			return zero, err
		}
	}
}

func (m *Manager) credentialMutationObservation(
	ctx context.Context,
	account store.Account,
) (store.CredentialExternalState, error) {
	return m.credentialMutationObservationAt(ctx, account, "")
}

func (m *Manager) credentialMutationObservationAt(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
) (store.CredentialExternalState, error) {
	quarantine, err := m.Store.CredentialQuarantine(account.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return m.credentialObservationAt(ctx, account, expectedPublicPath)
	}
	if err != nil {
		return store.CredentialExternalState{}, err
	}
	if err := validateCredentialQuarantineAccount(account, quarantine); err != nil {
		return store.CredentialExternalState{}, err
	}
	actual, err := m.credentialObservationAt(ctx, account, expectedPublicPath)
	if err != nil {
		return store.CredentialExternalState{}, errors.Join(
			credentialQuarantineError(quarantine), err,
		)
	}
	if !credentialStateReadable(actual) {
		return store.CredentialExternalState{}, credentialQuarantineError(quarantine)
	}
	tokenDigest, chainErr := m.credentialTokenChainStateDigestAt(
		ctx, account, expectedPublicPath,
	)
	if chainErr != nil {
		return store.CredentialExternalState{}, errors.Join(
			credentialQuarantineError(quarantine), chainErr,
		)
	}
	verified, err := m.credentialObservationAt(ctx, account, expectedPublicPath)
	if err != nil || !sameStoreObservation(actual, verified) {
		return store.CredentialExternalState{}, errors.Join(
			credentialQuarantineError(quarantine), err, ErrCredentialChangedUnderfoot,
		)
	}
	if quarantine.TokenChainDigest == nil {
		if !credentialObservationHasPresent(quarantine.Observation) && tokenDigest != nil {
			if err := m.Store.AcknowledgeCredentialQuarantine(quarantine); err != nil {
				return m.verifyResolvedCredentialQuarantineAt(
					ctx, account, expectedPublicPath, tokenDigest, err,
				)
			}
			if err := m.Store.ClearCredentialQuarantine(quarantine); err != nil {
				return m.verifyResolvedCredentialQuarantineAt(
					ctx, account, expectedPublicPath, tokenDigest, err,
				)
			}
			return verified, nil
		}
		if tokenDigest == nil {
			return store.CredentialExternalState{}, credentialQuarantineError(quarantine)
		}
		if _, err := m.Store.BindCredentialQuarantineTokenChain(quarantine, *tokenDigest); err != nil {
			return store.CredentialExternalState{}, err
		}
		return store.CredentialExternalState{}, credentialQuarantineError(quarantine)
	}
	if tokenDigest != nil && *quarantine.TokenChainDigest == *tokenDigest {
		return store.CredentialExternalState{}, credentialQuarantineError(quarantine)
	}
	if err := m.Store.AcknowledgeCredentialQuarantine(quarantine); err != nil {
		return m.verifyResolvedCredentialQuarantineAt(
			ctx, account, expectedPublicPath, tokenDigest, err,
		)
	}
	if err := m.Store.ClearCredentialQuarantine(quarantine); err != nil {
		return m.verifyResolvedCredentialQuarantineAt(
			ctx, account, expectedPublicPath, tokenDigest, err,
		)
	}
	return verified, nil
}

func (m *Manager) verifyResolvedCredentialQuarantine(
	ctx context.Context,
	account store.Account,
	replacementDigest *store.CredentialDigest,
	resolutionErr error,
) (store.CredentialExternalState, error) {
	return m.verifyResolvedCredentialQuarantineAt(
		ctx, account, "", replacementDigest, resolutionErr,
	)
}

func (m *Manager) verifyResolvedCredentialQuarantineAt(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
	replacementDigest *store.CredentialDigest,
	resolutionErr error,
) (store.CredentialExternalState, error) {
	if _, err := m.Store.CredentialQuarantine(account.ID); err == nil {
		return store.CredentialExternalState{}, resolutionErr
	} else if !errors.Is(err, sql.ErrNoRows) {
		return store.CredentialExternalState{}, errors.Join(resolutionErr, err)
	}
	actual, err := m.credentialObservationAt(ctx, account, expectedPublicPath)
	if err != nil || !credentialStateReadable(actual) {
		return store.CredentialExternalState{}, errors.Join(resolutionErr, err)
	}
	tokenDigest, err := m.credentialTokenChainStateDigestAt(
		ctx, account, expectedPublicPath,
	)
	if err != nil || !sameStoreOptionalCredentialDigest(tokenDigest, replacementDigest) {
		return store.CredentialExternalState{}, errors.Join(
			resolutionErr, err, ErrCredentialChangedUnderfoot,
		)
	}
	verified, err := m.credentialObservationAt(ctx, account, expectedPublicPath)
	if err != nil || !sameStoreObservation(actual, verified) {
		return store.CredentialExternalState{}, errors.Join(
			resolutionErr, err, ErrCredentialChangedUnderfoot,
		)
	}
	return verified, nil
}

func (m *Manager) requireCredentialMutationAllowed(account store.Account) error {
	quarantine, quarantineErr := m.Store.CredentialQuarantine(account.ID)
	if quarantineErr == nil {
		if err := validateCredentialQuarantineAccount(account, quarantine); err != nil {
			return err
		}
		return credentialQuarantineError(quarantine)
	}
	if !errors.Is(quarantineErr, sql.ErrNoRows) {
		return quarantineErr
	}
	return nil
}

func validateCredentialQuarantineAccount(
	account store.Account,
	quarantine store.CredentialQuarantine,
) error {
	if quarantine.AccountInstanceID != account.InstanceID ||
		quarantine.AccountGeneration != account.Generation ||
		quarantine.LocatorDigest != store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		) {
		return store.ErrAccountGenerationChanged
	}
	return nil
}

func credentialQuarantineError(quarantine store.CredentialQuarantine) error {
	terminalErr := fmt.Errorf(
		"%w: %s (a new credential or explicit reconciliation is required)",
		ErrCredentialOperationQuarantined,
		quarantine.Reason,
	)
	classification, err := credentialFailureError(quarantine.FailureClass)
	if err != nil {
		return err
	}
	if quarantine.Reason == store.CredentialResultCleanupFailed {
		terminalErr = errors.Join(
			terminalErr, ErrNeedsLogin, errCredentialCleanupPending,
		)
	}
	return errors.Join(terminalErr, classification, ErrCredentialOperationReplayed)
}

func applyCredentialOperation[T any](
	ctx context.Context,
	manager *Manager,
	account store.Account,
	operation store.CredentialOperation,
	expected store.CredentialExternalState,
	codec credentialOperationCodec[T],
	observe credentialObservationFunc,
	fn func(context.Context, *credentialOperationBoundary) (T, error),
) (T, error) {
	var zero T
	before, err := observe(ctx, account)
	if err != nil {
		_ = manager.Store.AbandonPreparedCredentialOperation(operation.Fence())
		return zero, err
	}
	if !sameStoreObservation(expected, before) {
		_ = manager.Store.AbandonPreparedCredentialOperation(operation.Fence())
		return zero, errCredentialOperationRetry
	}
	boundary := &credentialOperationBoundary{
		manager: manager, account: account, operation: operation, expected: expected,
		observe: observe,
	}
	result, operationErr := fn(ctx, boundary)
	settleCtx, settleCancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		credentialSettlementTimeout,
	)
	defer settleCancel()
	after, observeErr := manager.credentialObservation(settleCtx, account)
	if observeErr != nil {
		return result, errors.Join(operationErr, observeErr)
	}
	if !boundary.crossed && boundary.operation.Kind == store.CredentialOperationRemove &&
		(operationErr != nil || !sameStoreObservation(expected, after)) {
		applying, markErr := manager.Store.MarkCredentialOperationApplying(
			boundary.operation.Fence(), nil,
		)
		if markErr != nil {
			return result, errors.Join(operationErr, markErr)
		}
		boundary.operation = applying
		boundary.crossed = true
	}
	if !boundary.crossed {
		if !sameStoreObservation(expected, after) {
			_ = manager.Store.AbandonPreparedCredentialOperation(operation.Fence())
			return result, errCredentialOperationRetry
		}
		if operationErr != nil {
			_ = manager.Store.AbandonPreparedCredentialOperation(operation.Fence())
			return result, operationErr
		}
		receipt, settleErr := manager.Store.CommitPreparedCredentialOperation(
			operation.Fence(),
			after,
			codec.resultCode(result, operationErr),
			time.Now().Add(credentialTerminalTTL),
		)
		if settleErr != nil {
			return result, settleErr
		}
		return result, manager.Store.AcknowledgeCredentialOperation(receipt.Token)
	}
	actual, verifyErr := manager.credentialObservation(settleCtx, account)
	if verifyErr != nil {
		return result, errors.Join(operationErr, verifyErr)
	}
	status := store.CredentialTerminalSucceeded
	category := codec.resultCode(result, operationErr)
	failureClass := store.CredentialFailureNone
	outcome := actual
	if boundary.operation.Kind == store.CredentialOperationRemove && credentialStateEmpty(actual) {
		category = store.CredentialResultDone
	} else if (boundary.operation.Kind == store.CredentialOperationCompensate ||
		boundary.operation.Kind == store.CredentialOperationRemove) &&
		!credentialStateEmpty(actual) {
		status = store.CredentialTerminalQuarantined
		category = store.CredentialResultChangedUnderfoot
		failureClass = credentialFailureClassOrInternal(operationErr)
	} else if !sameStoreObservation(after, actual) {
		status = store.CredentialTerminalQuarantined
		category = store.CredentialResultChangedUnderfoot
		failureClass = credentialFailureClassOrInternal(operationErr)
	} else if category == store.CredentialResultCleanupFailed {
		status = store.CredentialTerminalQuarantined
		failureClass = store.CredentialFailureInternal
	} else if category == store.CredentialResultNeedsLogin ||
		category == store.CredentialResultNoTokens {
		status = store.CredentialTerminalSucceeded
	} else if operationErr != nil &&
		(boundary.operation.Kind == store.CredentialOperationEnsureFresh ||
			boundary.operation.Kind == store.CredentialOperationRefreshCurrent) {
		failureClass = credentialFailureClass(operationErr)
		switch failureClass {
		case store.CredentialFailureRefreshUnauthorized,
			store.CredentialFailureRefreshRejected:
			status = store.CredentialTerminalFailed
			category = store.CredentialResultFailed
		default:
			status = store.CredentialTerminalQuarantined
			category = store.CredentialResultAmbiguous
		}
	} else if operationErr != nil && !sameStoreObservation(expected, actual) {
		status = store.CredentialTerminalQuarantined
		category = store.CredentialResultAmbiguous
		failureClass = credentialFailureClass(operationErr)
	} else if operationErr != nil {
		status = store.CredentialTerminalFailed
		category = store.CredentialResultFailed
		failureClass = credentialFailureClass(operationErr)
	}
	var quarantineTokenChainDigest *store.CredentialDigest
	if status == store.CredentialTerminalQuarantined {
		stable, digest, stableErr := manager.credentialTokenChainStateAtObservation(
			settleCtx, account,
		)
		if stableErr == nil {
			if !sameStoreObservation(outcome, stable) {
				outcome = stable
				category = store.CredentialResultChangedUnderfoot
				failureClass = store.CredentialFailureInternal
			}
			quarantineTokenChainDigest = digest
		}
	}
	applied, err := manager.Store.MarkCredentialOperationApplied(
		boundary.operation.Fence(),
		outcome,
		status,
		category,
		failureClass,
		credentialPublicationPayload(status, category, boundary.publicationPayload),
	)
	if err != nil {
		return result, errors.Join(operationErr, err)
	}
	receipt, err := manager.Store.CommitCredentialOperation(
		applied.Fence(),
		outcome,
		quarantineTokenChainDigest,
		time.Now().Add(credentialTerminalTTL),
	)
	if err != nil {
		return result, errors.Join(operationErr, err)
	}
	if err := manager.settleCredentialWrite(ctx, receipt); err != nil {
		return result, errors.Join(operationErr, err)
	}
	if err := manager.Store.AcknowledgeCredentialOperation(receipt.Token); err != nil {
		return result, errors.Join(operationErr, err)
	}
	if status == store.CredentialTerminalQuarantined {
		bindErr := manager.bindCredentialQuarantineTokenChain(settleCtx, account)
		return result, errors.Join(operationErr, ErrCredentialOperationQuarantined, bindErr)
	}
	return result, operationErr
}

func (m *Manager) bindCredentialQuarantineTokenChain(
	ctx context.Context,
	account store.Account,
) error {
	quarantine, err := m.Store.CredentialQuarantine(account.ID)
	if err != nil {
		return err
	}
	before, err := m.credentialObservation(ctx, account)
	if err != nil {
		return err
	}
	if !sameStoreObservation(before, quarantine.Observation) {
		return ErrCredentialChangedUnderfoot
	}
	digest, err := m.credentialTokenChainStateDigest(ctx, account)
	if err != nil {
		return err
	}
	after, err := m.credentialObservation(ctx, account)
	if err != nil {
		return err
	}
	if digest == nil || !sameStoreObservation(before, after) {
		return ErrCredentialChangedUnderfoot
	}
	_, err = m.Store.BindCredentialQuarantineTokenChain(
		quarantine, *digest,
	)
	return err
}

func replayCredentialOperation[T any](
	ctx context.Context,
	manager *Manager,
	account store.Account,
	codec credentialOperationCodec[T],
	receipt store.CredentialOperationReceipt,
) (T, error) {
	result, replayErr := codec.replay(ctx, manager, account, receipt)
	if replayErr != nil && !credentialReplayDelivered(receipt, replayErr) {
		return result, replayErr
	}
	if replayErr == nil {
		if settleErr := manager.settleCredentialWrite(ctx, receipt); settleErr != nil {
			return result, settleErr
		}
	}
	var evidenceErr error
	if replayErr != nil {
		evidenceErr = ErrCredentialOperationReplayed
	}
	return result, errors.Join(
		replayErr, evidenceErr, manager.Store.AcknowledgeCredentialOperation(receipt.Token),
	)
}

func credentialReplayDelivered(
	receipt store.CredentialOperationReceipt,
	replayErr error,
) bool {
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded {
		return errors.Is(replayErr, ErrCredentialOperationFailed) ||
			errors.Is(replayErr, ErrCredentialOperationQuarantined)
	}
	return (receipt.Result == store.CredentialResultNoTokens ||
		receipt.Result == store.CredentialResultNeedsLogin) &&
		errors.Is(replayErr, ErrNeedsLogin)
}

func credentialReceiptMatchesInvocation(
	receipt store.CredentialOperationReceipt,
	account store.Account,
	kind store.CredentialOperationKind,
	target store.CredentialTarget,
	locator, intent store.CredentialDigest,
) bool {
	return receipt.AccountID == account.ID &&
		receipt.AccountInstanceID == account.InstanceID &&
		receipt.AccountGeneration == account.Generation &&
		receipt.ConfigDir == account.ConfigDir &&
		receipt.KeychainService == account.KeychainService &&
		receipt.KeychainAccount == account.KeychainAccount &&
		receipt.Kind == kind && receipt.Target == target &&
		receipt.LocatorDigest == locator &&
		receipt.IntentDigest == intent
}

// SettlePendingCredentialWrites publishes every retained terminal write in
// bounded account-id pages before the daemon admits new credential work.
func (m *Manager) SettlePendingCredentialWrites(ctx context.Context) error {
	if m.SettleCredentialWrite == nil {
		return errors.New("credential write settlement is unavailable")
	}
	afterAccountID := 0
	for {
		receipts, more, err := m.Store.UnacknowledgedCredentialWriteReceipts(
			afterAccountID, credentialWriteReceiptPage,
		)
		if err != nil {
			return err
		}
		for _, receipt := range receipts {
			if err := m.settleCredentialWrite(ctx, receipt); err != nil {
				return err
			}
			if err := m.Store.AcknowledgeCredentialOperation(receipt.Token); err != nil {
				return err
			}
			afterAccountID = receipt.AccountID
		}
		if !more {
			return nil
		}
		if len(receipts) == 0 {
			return store.ErrCredentialOperationState
		}
	}
}

func (m *Manager) settleCredentialWrite(
	ctx context.Context,
	receipt store.CredentialOperationReceipt,
) error {
	if !credentialReceiptPublishesWrite(receipt) {
		return nil
	}
	if m.SettleCredentialWrite == nil {
		return errors.New("credential write settlement is unavailable")
	}
	if len(receipt.PublicationPayload) == 0 {
		return store.ErrCredentialOperationState
	}
	return m.SettleCredentialWrite(ctx, CredentialWriteSettlement{
		OperationID:        receipt.OperationID,
		PublicationPayload: append([]byte(nil), receipt.PublicationPayload...),
	})
}

func credentialPublicationPayload(
	status store.CredentialTerminalStatus,
	result store.CredentialResultCategory,
	payload []byte,
) []byte {
	if status != store.CredentialTerminalSucceeded ||
		!credentialResultPublishesWrite(result) {
		return nil
	}
	return payload
}

func credentialResultPublishesWrite(result store.CredentialResultCategory) bool {
	switch result {
	case store.CredentialResultRefreshed,
		store.CredentialResultInstalled,
		store.CredentialResultAdopted:
		return true
	default:
		return false
	}
}

func credentialReceiptPublishesWrite(receipt store.CredentialOperationReceipt) bool {
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded {
		return false
	}
	switch receipt.Result {
	case store.CredentialResultRefreshed,
		store.CredentialResultInstalled,
		store.CredentialResultAdopted:
		return true
	default:
		return false
	}
}

func (m *Manager) waitCredentialOperation(
	ctx context.Context,
	token string,
) (store.CredentialOperationReceipt, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := m.Store.CredentialOperationByToken(token)
		if errors.Is(err, sql.ErrNoRows) {
			return m.Store.CredentialOperationReceipt(token)
		}
		if err != nil {
			return store.CredentialOperationReceipt{}, err
		}
		select {
		case <-ctx.Done():
			return store.CredentialOperationReceipt{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func replayCredentialReceiptFailure[T any](
	receipt store.CredentialOperationReceipt,
) (T, error) {
	var zero T
	var terminalErr error
	switch receipt.TerminalStatus {
	case store.CredentialTerminalSucceeded:
		return zero, nil
	case store.CredentialTerminalFailed:
		terminalErr = ErrCredentialOperationFailed
	case store.CredentialTerminalQuarantined:
		terminalErr = ErrCredentialOperationQuarantined
	default:
		return zero, errors.New("credential operation receipt has invalid terminal status")
	}
	classification, err := credentialFailureError(receipt.FailureClass)
	if err != nil {
		return zero, err
	}
	return zero, errors.Join(terminalErr, classification)
}

func credentialFailureClass(err error) store.CredentialFailureClass {
	if err == nil {
		return store.CredentialFailureNone
	}
	if errors.Is(err, oauth.ErrNetwork) {
		return store.CredentialFailureNetwork
	}
	var refreshErr *oauth.RefreshError
	if errors.As(err, &refreshErr) {
		switch {
		case refreshErr.Status == http.StatusUnauthorized:
			return store.CredentialFailureRefreshUnauthorized
		case refreshErr.Status >= http.StatusInternalServerError:
			return store.CredentialFailureRefreshServer
		default:
			return store.CredentialFailureRefreshRejected
		}
	}
	return store.CredentialFailureInternal
}

func credentialFailureClassOrInternal(err error) store.CredentialFailureClass {
	class := credentialFailureClass(err)
	if class == store.CredentialFailureNone {
		return store.CredentialFailureInternal
	}
	return class
}

func credentialFailureError(class store.CredentialFailureClass) (error, error) {
	switch class {
	case store.CredentialFailureNone, store.CredentialFailureInternal:
		return nil, nil
	case store.CredentialFailureNetwork:
		return oauth.ErrNetwork, nil
	case store.CredentialFailureRefreshUnauthorized:
		return &oauth.RefreshError{Status: http.StatusUnauthorized}, nil
	case store.CredentialFailureRefreshRejected:
		return &oauth.RefreshError{Status: http.StatusForbidden}, nil
	case store.CredentialFailureRefreshServer:
		return &oauth.RefreshError{Status: http.StatusInternalServerError}, nil
	default:
		return nil, store.ErrCredentialOperationState
	}
}

// CompensateCredentialState removes the exact credential state written by a
// superseded daemon mutation. It accepts only the aggregate digest, never
// credential bytes, and refuses to touch any state that drifted since publish.
func (m *Manager) CompensateCredentialState(
	ctx context.Context,
	account store.Account,
	exactWrittenDigest store.CredentialDigest,
) error {
	return m.compensateCredentialStateObserved(
		ctx,
		account,
		exactWrittenDigest,
		m.credentialMutationObservation,
	)
}

// CompensateQuarantinedCredentialState removes the exact credential state
// authorized by an unchanged quarantine. It does not clear the quarantine;
// the owning account mutation must resolve that durable evidence separately.
func (m *Manager) CompensateQuarantinedCredentialState(
	ctx context.Context,
	account store.Account,
	quarantine store.CredentialQuarantine,
	exactWrittenDigest store.CredentialDigest,
) error {
	return m.compensateCredentialStateObserved(
		ctx,
		account,
		exactWrittenDigest,
		func(ctx context.Context, observedAccount store.Account) (store.CredentialExternalState, error) {
			return m.credentialObservationWithExactQuarantine(
				ctx,
				observedAccount,
				quarantine,
			)
		},
	)
}

func (m *Manager) removeCredentialForAccountRemoval(
	ctx context.Context,
	account store.Account,
) error {
	return m.removeCredentialForAccountRemovalAt(ctx, account, "")
}

func (m *Manager) removeCredentialForAccountRemovalAt(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
) error {
	intent, err := store.CredentialRemovalIntentDigest(
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount,
	)
	if err != nil {
		return err
	}
	locator := store.CredentialKeychainLocatorDigest(
		account.KeychainService, account.KeychainAccount,
	)
	query := store.CredentialOperationEvidenceQuery{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		LocatorDigest: locator, Kind: store.CredentialOperationRemove,
		Target: store.CredentialTargetKeychain, IntentDigest: intent,
	}
	active, receipt, err := m.Store.CredentialOperationEvidence(query)
	if err != nil {
		return err
	}
	codec := unitCredentialOperationCodec(store.CredentialTargetKeychain)
	codec.intent = &intent
	if receipt != nil {
		_, err := replayCredentialOperation(ctx, m, account, codec, *receipt)
		return err
	}
	if active != nil {
		receipt, err := m.waitCredentialOperation(ctx, active.Token)
		if err != nil {
			return err
		}
		_, err = replayCredentialOperation(ctx, m, account, codec, receipt)
		return err
	}
	observe := func(ctx context.Context, account store.Account) (store.CredentialExternalState, error) {
		return m.credentialMutationObservationAt(ctx, account, expectedPublicPath)
	}
	_, err = runCredentialOperationObserved(
		ctx, m, account, store.CredentialOperationRemove, codec,
		observe,
		func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
			actual, err := m.credentialMutationObservationAt(
				ctx, account, expectedPublicPath,
			)
			if err != nil {
				return struct{}{}, err
			}
			if !credentialStateReadable(actual) {
				return struct{}{}, ErrCredentialUnverifiable
			}
			if credentialStateEmpty(actual) {
				return struct{}{}, nil
			}
			if err := boundary.Cross(ctx); err != nil {
				return struct{}{}, err
			}
			if m.credentialCAS == nil {
				return struct{}{}, errors.New("credential CAS worker is unavailable")
			}
			proof, err := m.credentialCAS(
				ctx, account, boundary.expected, credentialCASMutation{
					ExpectedPublicPath: expectedPublicPath,
					Delete:             true,
				},
			)
			if errors.Is(err, errCredentialCASConflict) {
				return struct{}{}, ErrCredentialChangedUnderfoot
			}
			if err != nil {
				return struct{}{}, err
			}
			if !credentialStateEmpty(proof.After) {
				return struct{}{}, ErrCredentialChangedUnderfoot
			}
			return struct{}{}, nil
		},
	)
	return err
}

func (m *Manager) requireCredentialAbsent(ctx context.Context, account store.Account) error {
	actual, err := m.credentialMutationObservation(ctx, account)
	if err != nil {
		return err
	}
	if !credentialStateReadable(actual) {
		return ErrCredentialUnverifiable
	}
	if !credentialStateEmpty(actual) {
		return ErrCredentialChangedUnderfoot
	}
	return nil
}

func (m *Manager) compensateCredentialStateObserved(
	ctx context.Context,
	account store.Account,
	exactWrittenDigest store.CredentialDigest,
	observe credentialObservationFunc,
) error {
	if exactWrittenDigest == (store.CredentialDigest{}) {
		return errors.New("credential compensation digest is required")
	}
	intent := credentialIntentDigest(
		store.CredentialOperationCompensate, string(exactWrittenDigest[:]),
	)
	active, receipt, err := m.Store.CredentialOperationEvidence(
		store.CredentialOperationEvidenceQuery{
			AccountID: account.ID, AccountInstanceID: account.InstanceID,
			AccountGeneration: account.Generation,
			ConfigDir:         account.ConfigDir,
			KeychainService:   account.KeychainService,
			KeychainAccount:   account.KeychainAccount,
			LocatorDigest: store.CredentialKeychainLocatorDigest(
				account.KeychainService, account.KeychainAccount,
			),
			Kind:         store.CredentialOperationCompensate,
			Target:       store.CredentialTargetKeychain,
			IntentDigest: intent,
		},
	)
	if err != nil {
		return err
	}
	codec := unitCredentialOperationCodec(
		store.CredentialTargetKeychain,
	)
	if receipt != nil {
		_, err := replayCredentialOperation(ctx, m, account, codec, *receipt)
		return err
	}
	if active != nil {
		receipt, err := m.waitCredentialOperation(ctx, active.Token)
		if err != nil {
			return err
		}
		_, err = replayCredentialOperation(ctx, m, account, codec, receipt)
		return err
	}
	actual, err := observe(ctx, account)
	if err != nil {
		return err
	}
	if !credentialStateReadable(actual) {
		return ErrCredentialUnverifiable
	}
	if credentialStateEmpty(actual) {
		return nil
	}
	actualDigest, err := actual.Digest()
	if err != nil {
		return err
	}
	if actualDigest != exactWrittenDigest {
		return ErrCredentialChangedUnderfoot
	}
	_, err = runCredentialOperationObserved(
		ctx,
		m,
		account,
		store.CredentialOperationCompensate,
		codec,
		observe,
		func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
			return struct{}{}, m.compensateCredentialState(
				ctx, account, exactWrittenDigest, boundary,
			)
		},
		string(exactWrittenDigest[:]),
	)
	return err
}

func (m *Manager) credentialObservationWithExactQuarantine(
	ctx context.Context,
	account store.Account,
	expected store.CredentialQuarantine,
) (store.CredentialExternalState, error) {
	actual, err := m.Store.CredentialQuarantine(account.ID)
	if err != nil {
		return store.CredentialExternalState{}, err
	}
	if !sameStoreCredentialQuarantine(expected, actual) {
		return store.CredentialExternalState{}, store.ErrCredentialOperationState
	}
	if actual.AccountInstanceID != account.InstanceID ||
		actual.AccountGeneration != account.Generation ||
		actual.LocatorDigest != store.CredentialKeychainLocatorDigest(
			account.KeychainService,
			account.KeychainAccount,
		) {
		return store.CredentialExternalState{}, store.ErrAccountGenerationChanged
	}
	return m.credentialObservation(ctx, account)
}

func (m *Manager) compensateCredentialState(
	ctx context.Context,
	account store.Account,
	exactWrittenDigest store.CredentialDigest,
	boundary *credentialOperationBoundary,
) error {
	actual, err := m.credentialObservation(ctx, account)
	if err != nil {
		return err
	}
	if !credentialStateReadable(actual) {
		return ErrCredentialUnverifiable
	}
	if credentialStateEmpty(actual) {
		return nil
	}
	actualDigest, err := actual.Digest()
	if err != nil {
		return err
	}
	if actualDigest != exactWrittenDigest {
		return ErrCredentialChangedUnderfoot
	}
	if err := boundary.Cross(ctx); err != nil {
		return err
	}
	if m.credentialCAS == nil {
		return errors.New("credential CAS worker is unavailable")
	}
	_, err = m.credentialCAS(ctx, account, boundary.expected, credentialCASMutation{Delete: true})
	if errors.Is(err, errCredentialCASConflict) {
		return ErrCredentialChangedUnderfoot
	}
	return err
}

func unitCredentialOperationCodec(
	target store.CredentialTarget,
) credentialOperationCodec[struct{}] {
	return credentialOperationCodec[struct{}]{
		target: target,
		resultCode: func(_ struct{}, err error) store.CredentialResultCategory {
			if err != nil {
				return store.CredentialResultFailed
			}
			return store.CredentialResultDone
		},
		replay: func(_ context.Context, _ *Manager, _ store.Account, receipt store.CredentialOperationReceipt) (struct{}, error) {
			if receipt.TerminalStatus != store.CredentialTerminalSucceeded {
				return replayCredentialReceiptFailure[struct{}](receipt)
			}
			if receipt.Result != store.CredentialResultDone {
				return struct{}{}, store.ErrCredentialOperationState
			}
			return struct{}{}, nil
		},
	}
}

func adoptRotatedCredentialOperationCodec(
	target store.CredentialTarget,
) credentialOperationCodec[struct{}] {
	return credentialOperationCodec[struct{}]{
		target: target,
		resultCode: func(_ struct{}, err error) store.CredentialResultCategory {
			if err != nil {
				return store.CredentialResultFailed
			}
			return store.CredentialResultAdopted
		},
		replay: func(
			_ context.Context,
			_ *Manager,
			_ store.Account,
			receipt store.CredentialOperationReceipt,
		) (struct{}, error) {
			if receipt.TerminalStatus != store.CredentialTerminalSucceeded {
				return replayCredentialReceiptFailure[struct{}](receipt)
			}
			if receipt.Result != store.CredentialResultAdopted {
				return struct{}{}, store.ErrCredentialOperationState
			}
			return struct{}{}, nil
		},
	}
}

func freshCredentialOperationCodec() credentialOperationCodec[freshTokenResult] {
	return credentialOperationCodec[freshTokenResult]{
		target: store.CredentialTargetKeychain,
		resultCode: func(result freshTokenResult, err error) store.CredentialResultCategory {
			if errors.Is(err, errCredentialCleanupPending) {
				return store.CredentialResultCleanupFailed
			}
			if errors.Is(err, errCredentialDeterministicNoTokens) {
				return store.CredentialResultNoTokens
			}
			if errors.Is(err, errCredentialDeterministicNeedsLogin) {
				return store.CredentialResultNeedsLogin
			}
			if err != nil {
				return store.CredentialResultFailed
			}
			if result.Refreshed {
				return store.CredentialResultRefreshed
			}
			return store.CredentialResultUnchanged
		},
		replay: func(ctx context.Context, manager *Manager, account store.Account, receipt store.CredentialOperationReceipt) (freshTokenResult, error) {
			actual, err := manager.credentialObservation(ctx, account)
			if err != nil {
				return freshTokenResult{}, err
			}
			if !sameStoreObservation(receipt.Outcome, actual) {
				quarantineObservation := actual
				var tokenChainDigest *store.CredentialDigest
				if stable, digest, stableErr := manager.credentialTokenChainStateAtObservation(
					ctx, account,
				); stableErr == nil {
					quarantineObservation = stable
					tokenChainDigest = digest
				}
				_, quarantineErr := manager.Store.QuarantineCredential(
					store.QuarantineCredentialRequest{
						AccountID: account.ID, AccountInstanceID: account.InstanceID,
						AccountGeneration: account.Generation,
						LocatorDigest: store.CredentialKeychainLocatorDigest(
							account.KeychainService,
							account.KeychainAccount,
						),
						Observation: quarantineObservation, TokenChainDigest: tokenChainDigest,
						Reason:       store.CredentialResultChangedUnderfoot,
						FailureClass: store.CredentialFailureInternal,
					},
				)
				return freshTokenResult{}, errors.Join(
					ErrCredentialOperationQuarantined, quarantineErr,
				)
			}
			if receipt.TerminalStatus != store.CredentialTerminalSucceeded {
				credential, source, readErr := manager.ReadCredential(ctx, account)
				failureResult := freshTokenResult{
					Credential: credential, Source: source, RefreshAttempted: true,
				}
				_, failureErr := replayCredentialReceiptFailure[freshTokenResult](receipt)
				if receipt.TerminalStatus == store.CredentialTerminalQuarantined &&
					receipt.Result == store.CredentialResultCleanupFailed {
					failureErr = errors.Join(
						failureErr, ErrNeedsLogin, errCredentialCleanupPending,
					)
				}
				return failureResult, errors.Join(failureErr, readErr)
			}
			switch receipt.Result {
			case store.CredentialResultNoTokens:
				return freshTokenResult{}, errors.Join(ErrNeedsLogin, creds.ErrNoTokens)
			case store.CredentialResultNeedsLogin:
				credential, source, err := manager.ReadCredential(ctx, account)
				if err != nil {
					return freshTokenResult{}, errors.Join(ErrNeedsLogin, err)
				}
				return freshTokenResult{Credential: credential, Source: source}, ErrNeedsLogin
			case store.CredentialResultRefreshed:
				credential, source, err := manager.ReadCredential(ctx, account)
				if err != nil {
					return freshTokenResult{}, err
				}
				return freshTokenResult{Credential: credential, Source: source, Refreshed: true}, nil
			case store.CredentialResultUnchanged:
				credential, source, err := manager.ReadCredential(ctx, account)
				if err != nil {
					return freshTokenResult{}, err
				}
				return freshTokenResult{Credential: credential, Source: source}, nil
			default:
				return freshTokenResult{}, store.ErrCredentialOperationState
			}
		},
	}
}

func installCredentialOperationCodec(
	target store.CredentialTarget,
	intent store.CredentialDigest,
) credentialOperationCodec[bool] {
	return credentialOperationCodec[bool]{
		target: target,
		intent: &intent,
		resultCode: func(installed bool, err error) store.CredentialResultCategory {
			if err != nil {
				return store.CredentialResultFailed
			}
			if installed {
				return store.CredentialResultInstalled
			}
			return store.CredentialResultSkipped
		},
		replay: func(_ context.Context, _ *Manager, _ store.Account, receipt store.CredentialOperationReceipt) (bool, error) {
			if receipt.TerminalStatus != store.CredentialTerminalSucceeded {
				return replayCredentialReceiptFailure[bool](receipt)
			}
			switch receipt.Result {
			case store.CredentialResultInstalled:
				return true, nil
			case store.CredentialResultSkipped:
				return false, nil
			default:
				return false, store.ErrCredentialOperationState
			}
		},
	}
}

func (m *Manager) recoverCredentialOperation(
	ctx context.Context,
	operation store.CredentialOperation,
	retirement proc.ReapReceipt,
) error {
	account, err := m.credentialOperationAccount(operation)
	if err != nil {
		return err
	}
	if account.InstanceID != operation.AccountInstanceID ||
		account.Generation != operation.AccountGeneration ||
		account.ConfigDir != operation.ConfigDir ||
		account.KeychainService != operation.KeychainService ||
		account.KeychainAccount != operation.KeychainAccount ||
		store.CredentialKeychainLocatorDigest(
			account.KeychainService,
			account.KeychainAccount,
		) != operation.LocatorDigest {
		return store.ErrAccountGenerationChanged
	}
	owner, err := m.credentialOwnerRecord()
	if err != nil {
		return err
	}
	operation, err = m.Store.TakeoverCredentialOperation(
		ctx,
		operation.Fence(),
		owner,
		retirement,
		m.workers.reaper,
	)
	if err != nil {
		return err
	}
	if operation.State == store.CredentialOperationPrepared &&
		operation.Kind != store.CredentialOperationRemove {
		return m.Store.AbandonPreparedCredentialOperation(operation.Fence())
	}
	return m.recoverRetiredCredentialOperation(ctx, account, operation)
}

func (m *Manager) credentialOperationAccount(
	operation store.CredentialOperation,
) (store.Account, error) {
	account, err := m.Store.GetAccount(operation.AccountID)
	if err == nil || !errors.Is(err, store.ErrAccountNotFound) {
		return account, err
	}
	if operation.Target != store.CredentialTargetKeychain {
		return store.Account{}, err
	}
	if operation.Kind == store.CredentialOperationRemove {
		return store.Account{
			ID: operation.AccountID, InstanceID: operation.AccountInstanceID,
			Generation: operation.AccountGeneration, ConfigDir: operation.ConfigDir,
			KeychainService: operation.KeychainService, KeychainAccount: operation.KeychainAccount,
		}, nil
	}
	if operation.Kind != store.CredentialOperationCompensate {
		return store.Account{}, err
	}
	mutation, mutationErr := m.Store.ActiveAccountMutation(operation.AccountID)
	if mutationErr != nil {
		return store.Account{}, errors.Join(err, mutationErr)
	}
	if mutation.Kind != store.AccountMutationAdd ||
		mutation.State != store.AccountMutationCompensating ||
		!mutation.CredentialWritten ||
		mutation.AccountInstanceID != operation.AccountInstanceID ||
		mutation.AccountGeneration != operation.AccountGeneration ||
		mutation.LocatorDigest != operation.LocatorDigest {
		return store.Account{}, store.ErrAccountGenerationChanged
	}
	expectedDigest, digestErr := operation.Expected.Digest()
	if digestErr != nil {
		return store.Account{}, digestErr
	}
	if expectedDigest != mutation.WrittenCredentialDigest ||
		credentialIntentDigest(
			store.CredentialOperationCompensate,
			string(mutation.WrittenCredentialDigest[:]),
		) != operation.IntentDigest {
		return store.Account{}, store.ErrAccountGenerationChanged
	}
	return store.Account{
		ID: operation.AccountID, InstanceID: mutation.AccountInstanceID,
		Generation: mutation.AccountGeneration, ConfigDir: mutation.ConfigDir,
		KeychainService: mutation.KeychainService, KeychainAccount: mutation.KeychainAccount,
		Label: mutation.Label, AccountUUID: mutation.AccountUUID,
	}, nil
}

func (m *Manager) recoverRetiredCredentialOperation(
	ctx context.Context,
	account store.Account,
	operation store.CredentialOperation,
) error {
	actual, err := m.credentialObservation(ctx, account)
	if err != nil {
		return err
	}
	if operation.State == store.CredentialOperationApplied &&
		operation.HasOutcome && sameStoreObservation(operation.Outcome, actual) {
		_, err := m.Store.CommitCredentialOperation(
			operation.Fence(), actual, nil, time.Now().Add(credentialTerminalTTL),
		)
		return err
	}
	switch operation.Kind {
	case store.CredentialOperationAdoptRotated:
		// Rewriting identical bytes is an ACL side effect not visible in the
		// credential fingerprint. A usable blob therefore does not prove adopt.
	case store.CredentialOperationEnsureFresh, store.CredentialOperationRefreshCurrent:
		// A process death after the OAuth boundary cannot prove whether a
		// single-use refresh token was consumed. An unrelated login can also
		// produce a perfectly usable changed credential. Without the exact
		// applied result recorded by the owner, neither state is attributable to
		// this operation, so recovery must quarantine and never retry OAuth.
	case store.CredentialOperationInstallSynced:
		if slot := credentialTargetSlot(actual, operation.Target); slot.State == store.CredentialSlotPresent &&
			slot.Digest != nil && *slot.Digest == operation.IntentDigest {
			return m.resolveRecoveredCredentialOperation(
				operation, actual, store.CredentialTerminalSucceeded, store.CredentialResultInstalled,
			)
		}
	case store.CredentialOperationCompensate, store.CredentialOperationRemove:
		return m.recoverCompensateCredentialOperation(ctx, account, operation, actual)
	}
	return m.resolveRecoveredCredentialOperation(
		operation, actual, store.CredentialTerminalQuarantined, store.CredentialResultAmbiguous,
	)
}

func (m *Manager) recoverCompensateCredentialOperation(
	ctx context.Context,
	account store.Account,
	operation store.CredentialOperation,
	actual store.CredentialExternalState,
) error {
	if !credentialStateReadable(operation.Expected) || !credentialStateReadable(actual) {
		return m.resolveRecoveredCredentialOperation(
			operation, actual, store.CredentialTerminalQuarantined, store.CredentialResultAmbiguous,
		)
	}
	expectedSlot := operation.Expected.Keychain
	actualSlot := actual.Keychain
	if actualSlot.State != store.CredentialSlotEmpty &&
		(!credentialSlotPresent(expectedSlot) || !credentialSlotPresent(actualSlot) ||
			*expectedSlot.Digest != *actualSlot.Digest) {
		return m.resolveRecoveredCredentialOperation(
			operation, actual, store.CredentialTerminalQuarantined, store.CredentialResultChangedUnderfoot,
		)
	}
	if m.credentialCAS == nil {
		return m.resolveRecoveredCredentialOperation(
			operation, actual, store.CredentialTerminalQuarantined, store.CredentialResultCleanupFailed,
		)
	}
	proof, err := m.credentialCAS(ctx, account, actual, credentialCASMutation{Delete: true})
	if err != nil {
		result := store.CredentialResultCleanupFailed
		if errors.Is(err, errCredentialCASConflict) {
			result = store.CredentialResultChangedUnderfoot
		}
		return m.resolveRecoveredCredentialOperation(
			operation, actual, store.CredentialTerminalQuarantined, result,
		)
	}
	return m.resolveRecoveredCredentialOperation(
		operation, proof.After, store.CredentialTerminalSucceeded, store.CredentialResultDone,
	)
}

func (m *Manager) resolveRecoveredCredentialOperation(
	operation store.CredentialOperation,
	actual store.CredentialExternalState,
	status store.CredentialTerminalStatus,
	result store.CredentialResultCategory,
) error {
	failureClass := store.CredentialFailureNone
	if status != store.CredentialTerminalSucceeded {
		failureClass = store.CredentialFailureInternal
	}
	_, err := m.Store.ResolveCredentialOperation(
		operation.Fence(),
		actual,
		status,
		result,
		failureClass,
		credentialPublicationPayload(status, result, operation.PublicationPayload),
		time.Now().Add(credentialTerminalTTL),
	)
	return err
}

func credentialTargetSlot(
	state store.CredentialExternalState,
	_ store.CredentialTarget,
) store.CredentialSlotObservation {
	return state.Keychain
}

func credentialSlotPresent(slot store.CredentialSlotObservation) bool {
	return slot.State == store.CredentialSlotPresent && slot.Digest != nil
}

func credentialStateReadable(state store.CredentialExternalState) bool {
	return state.Keychain.State == store.CredentialSlotEmpty ||
		state.Keychain.State == store.CredentialSlotPresent
}

func credentialObservationHasPresent(state store.CredentialExternalState) bool {
	return state.Keychain.State == store.CredentialSlotPresent
}

func credentialStateEmpty(state store.CredentialExternalState) bool {
	return state.Keychain.State == store.CredentialSlotEmpty
}

func (m *Manager) credentialObservation(
	ctx context.Context,
	account store.Account,
) (store.CredentialExternalState, error) {
	return m.credentialObservationAt(ctx, account, "")
}

func (m *Manager) credentialObservationAt(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
) (store.CredentialExternalState, error) {
	location, err := m.credentialStore(account, expectedPublicPath)
	if err != nil {
		return store.CredentialExternalState{}, err
	}
	slot := store.CredentialSlotObservation{}
	credential, err := location.Read(ctx)
	switch creds.ClassifyRead(err) {
	case creds.ReadEmpty:
		slot.State = store.CredentialSlotEmpty
	case creds.ReadPresent:
		raw, marshalErr := credential.Marshal()
		if marshalErr != nil {
			return store.CredentialExternalState{}, marshalErr
		}
		digest := store.CredentialDigest(sha256.Sum256(raw))
		slot.State = store.CredentialSlotPresent
		slot.Digest = &digest
	case creds.ReadUnsearchable:
		slot.State = store.CredentialSlotUnsearchable
	case creds.ReadFatal:
		slot.State = store.CredentialSlotUnreadable
	}
	result := store.CredentialExternalState{Keychain: slot}
	if _, err := result.Digest(); err != nil {
		return store.CredentialExternalState{}, err
	}
	return result, nil
}

// CredentialExternalState returns the replay-safe credential-store observation
// used to fence daemon-owned mutations. It contains state and digests, never secrets.
func (m *Manager) CredentialExternalState(
	ctx context.Context,
	account store.Account,
) (store.CredentialExternalState, error) {
	return m.credentialObservation(ctx, account)
}

// CredentialExternalStateAt observes after validating one explicit presentation path.
func (m *Manager) CredentialExternalStateAt(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
) (store.CredentialExternalState, error) {
	return m.credentialObservationAt(ctx, account, expectedPublicPath)
}

func credentialIntentDigest(
	kind store.CredentialOperationKind,
	arguments ...string,
) store.CredentialDigest {
	digest := sha256.New()
	for _, value := range append(
		[]string{
			"cc-pool:credential-intent:v1",
			string(kind),
		},
		arguments...,
	) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(value))
	}
	var result store.CredentialDigest
	copy(result[:], digest.Sum(nil))
	return result
}

func credentialTokenChainDigest(credential *creds.Credential) store.CredentialDigest {
	digest := sha256.New()
	for _, value := range []string{
		"cc-pool:credential-token-chain:v1",
		credential.ClaudeAiOauth.AccessToken,
		credential.ClaudeAiOauth.RefreshToken,
	} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(value))
	}
	var result store.CredentialDigest
	copy(result[:], digest.Sum(nil))
	return result
}

func (m *Manager) credentialTokenChainStateDigest(
	ctx context.Context,
	account store.Account,
) (*store.CredentialDigest, error) {
	return m.credentialTokenChainStateDigestAt(ctx, account, "")
}

func (m *Manager) credentialTokenChainStateDigestAt(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
) (*store.CredentialDigest, error) {
	location, err := m.credentialStore(account, expectedPublicPath)
	if err != nil {
		return nil, err
	}
	credential, err := location.Read(ctx)
	switch creds.ClassifyRead(err) {
	case creds.ReadEmpty:
		return nil, nil
	case creds.ReadPresent:
		digest := credentialTokenChainDigest(credential)
		return &digest, nil
	default:
		return nil, fmt.Errorf("read Keychain credential token chain: %w", err)
	}
}

func (m *Manager) credentialTokenChainStateAtObservation(
	ctx context.Context,
	account store.Account,
) (store.CredentialExternalState, *store.CredentialDigest, error) {
	for range 3 {
		before, err := m.credentialObservation(ctx, account)
		if err != nil {
			return store.CredentialExternalState{}, nil, err
		}
		digest, err := m.credentialTokenChainStateDigest(ctx, account)
		if err != nil {
			return store.CredentialExternalState{}, nil, err
		}
		after, err := m.credentialObservation(ctx, account)
		if err != nil {
			return store.CredentialExternalState{}, nil, err
		}
		if sameStoreObservation(before, after) {
			return after, digest, nil
		}
	}
	return store.CredentialExternalState{}, nil, ErrCredentialChangedUnderfoot
}

func sameStoreObservation(
	left, right store.CredentialExternalState,
) bool {
	leftDigest, leftErr := left.Digest()
	rightDigest, rightErr := right.Digest()
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}

func sameStoreCredentialQuarantine(
	left, right store.CredentialQuarantine,
) bool {
	return left.AccountID == right.AccountID &&
		left.AccountInstanceID == right.AccountInstanceID &&
		left.AccountGeneration == right.AccountGeneration &&
		left.LocatorDigest == right.LocatorDigest &&
		sameStoreObservation(left.Observation, right.Observation) &&
		sameStoreOptionalCredentialDigest(left.TokenChainDigest, right.TokenChainDigest) &&
		left.Reason == right.Reason &&
		left.FailureClass == right.FailureClass &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func sameStoreOptionalCredentialDigest(left, right *store.CredentialDigest) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return left == nil || *left == *right
}
