package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// ProcessRetirementVerifier proves a daemonkit reap receipt remains durably unacknowledged.
type ProcessRetirementVerifier interface {
	VerifyReapReceipt(context.Context, proc.ReapReceipt) error
}

// CredentialOperationState is one durable credential operation phase.
type CredentialOperationState string

const (
	// CredentialOperationPrepared is the durable pre-I/O phase.
	CredentialOperationPrepared CredentialOperationState = "prepared"
	// CredentialOperationApplying has crossed the external-I/O boundary.
	CredentialOperationApplying CredentialOperationState = "applying"
	// CredentialOperationApplied has completed external I/O.
	CredentialOperationApplied CredentialOperationState = "applied"
)

// CredentialPublicationPayloadMaxBytes bounds the daemon-owned non-secret envelope.
const CredentialPublicationPayloadMaxBytes = 4096

// CredentialTerminalStatus is the immutable outcome class of a committed operation.
type CredentialTerminalStatus string

const (
	// CredentialTerminalSucceeded records a successful immutable outcome.
	CredentialTerminalSucceeded CredentialTerminalStatus = "succeeded"
	// CredentialTerminalFailed records a classified terminal failure.
	CredentialTerminalFailed CredentialTerminalStatus = "failed"
	// CredentialTerminalQuarantined records unresolved external ambiguity.
	CredentialTerminalQuarantined CredentialTerminalStatus = "quarantined"
)

// CredentialOperationKind is one closed credential mutation policy.
type CredentialOperationKind string

const (
	// CredentialOperationEnsureFresh refreshes only when required.
	CredentialOperationEnsureFresh CredentialOperationKind = "ensure-fresh"
	// CredentialOperationRefreshCurrent refreshes the selected current account.
	CredentialOperationRefreshCurrent CredentialOperationKind = "refresh-current"
	// CredentialOperationInstallSynced installs synchronized credentials.
	CredentialOperationInstallSynced CredentialOperationKind = "install-synced"
	// CredentialOperationAdoptRotated adopts a proven rotated credential.
	CredentialOperationAdoptRotated CredentialOperationKind = "adopt-rotated"
	// CredentialOperationCompensate reverses an unpublished credential write.
	CredentialOperationCompensate CredentialOperationKind = "compensate"
)

// CredentialTarget identifies the exact external credential slot an operation targets.
type CredentialTarget string

const (
	// CredentialTargetKeychain selects only the Keychain slot.
	CredentialTargetKeychain CredentialTarget = "keychain"
)

// CredentialResultCategory is one closed, secret-free terminal result.
type CredentialResultCategory string

const (
	// CredentialResultDone records a completed operation without a narrower category.
	CredentialResultDone CredentialResultCategory = "done"
	// CredentialResultUnchanged records byte-identical external state.
	CredentialResultUnchanged CredentialResultCategory = "unchanged"
	// CredentialResultRefreshed records newly refreshed credentials.
	CredentialResultRefreshed CredentialResultCategory = "refreshed"
	// CredentialResultNeedsLogin records an interactive authentication requirement.
	CredentialResultNeedsLogin CredentialResultCategory = "needs-login"
	// CredentialResultNoTokens records absent source credentials.
	CredentialResultNoTokens CredentialResultCategory = "no-tokens"
	// CredentialResultInstalled records a completed synchronized install.
	CredentialResultInstalled CredentialResultCategory = "installed"
	// CredentialResultAdopted records a trusted write-back of rotated credentials.
	CredentialResultAdopted CredentialResultCategory = "adopted"
	// CredentialResultSkipped records an intentionally omitted mutation.
	CredentialResultSkipped CredentialResultCategory = "skipped"
	// CredentialResultFailed records a classified operation failure.
	CredentialResultFailed CredentialResultCategory = "failed"
	// CredentialResultAmbiguous records unprovable external state.
	CredentialResultAmbiguous CredentialResultCategory = "ambiguous"
	// CredentialResultDiverged records conflicting credential copies.
	CredentialResultDiverged CredentialResultCategory = "diverged"
	// CredentialResultCleanupFailed records a failed compensating cleanup.
	CredentialResultCleanupFailed CredentialResultCategory = "cleanup-failed"
	// CredentialResultChangedUnderfoot records concurrent external drift.
	CredentialResultChangedUnderfoot CredentialResultCategory = "changed-underfoot"
)

// CredentialFailureClass is the closed, secret-free cause taxonomy for a
// non-successful credential operation.
type CredentialFailureClass string

const (
	// CredentialFailureNone records the absence of a failure.
	CredentialFailureNone CredentialFailureClass = ""
	// CredentialFailureInternal records an invariant or local-system failure.
	CredentialFailureInternal CredentialFailureClass = "internal"
	// CredentialFailureNetwork records a transport failure.
	CredentialFailureNetwork CredentialFailureClass = "network"
	// CredentialFailureRefreshUnauthorized records rejected refresh authorization.
	CredentialFailureRefreshUnauthorized CredentialFailureClass = "refresh-unauthorized"
	// CredentialFailureRefreshRejected records a non-authorization refresh rejection.
	CredentialFailureRefreshRejected CredentialFailureClass = "refresh-rejected"
	// CredentialFailureRefreshServer records a remote refresh service failure.
	CredentialFailureRefreshServer CredentialFailureClass = "refresh-server"
)

// CredentialSlotState is one closed observation state for a credential slot.
type CredentialSlotState string

const (
	// CredentialSlotEmpty records a proven absent slot.
	CredentialSlotEmpty CredentialSlotState = "empty"
	// CredentialSlotPresent records a readable present slot.
	CredentialSlotPresent CredentialSlotState = "present"
	// CredentialSlotUnsearchable records a slot whose existence cannot be proven.
	CredentialSlotUnsearchable CredentialSlotState = "unsearchable"
	// CredentialSlotUnreadable records present but unreadable data.
	CredentialSlotUnreadable CredentialSlotState = "unreadable"
)

// CredentialDigest is a secret-free SHA-256 digest.
type CredentialDigest [32]byte

// CredentialOperationID is the stable semantic identity of one exact request.
type CredentialOperationID [32]byte

// CredentialKeychainLocatorDigest binds one exact Keychain owner without
// credential bytes.
func CredentialKeychainLocatorDigest(service, account string) CredentialDigest {
	hash := sha256.New()
	writeCredentialHashField(hash, []byte("cc-pool:keychain-credential-locator:v1"))
	writeCredentialHashField(hash, []byte(service))
	writeCredentialHashField(hash, []byte(account))
	var digest CredentialDigest
	copy(digest[:], hash.Sum(nil))
	return digest
}

// CredentialSlotObservation records one slot without storing credential bytes.
type CredentialSlotObservation struct {
	State  CredentialSlotState
	Digest *CredentialDigest
}

// CredentialExternalState records the Keychain slot without storing secrets.
type CredentialExternalState struct {
	Keychain CredentialSlotObservation
}

// Digest returns the canonical digest of the complete external state.
func (state CredentialExternalState) Digest() (CredentialDigest, error) {
	if err := state.validate(); err != nil {
		return CredentialDigest{}, err
	}
	hash := sha256.New()
	writeCredentialHashField(hash, []byte("cc-pool:keychain-credential-external-state:v1"))
	writeCredentialHashField(hash, []byte(state.Keychain.State))
	if state.Keychain.Digest == nil {
		writeCredentialHashField(hash, nil)
	} else {
		writeCredentialHashField(hash, state.Keychain.Digest[:])
	}
	var digest CredentialDigest
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

// NewCredentialOperationID derives the only operation ID accepted by Begin.
func NewCredentialOperationID(
	accountInstanceID string,
	accountGeneration uint64,
	kind CredentialOperationKind,
	target CredentialTarget,
	locator CredentialDigest,
	expected CredentialExternalState,
	intent CredentialDigest,
) (CredentialOperationID, error) {
	if err := validateAccountInstanceID(accountInstanceID); err != nil {
		return CredentialOperationID{}, err
	}
	if accountGeneration == 0 || !validCredentialKindTarget(kind, target) || locator.zero() || intent.zero() {
		return CredentialOperationID{}, errors.New("credential operation identity is invalid")
	}
	expectedDigest, err := expected.Digest()
	if err != nil {
		return CredentialOperationID{}, err
	}
	hash := sha256.New()
	writeCredentialHashField(hash, []byte("cc-pool:keychain-credential-operation:v1"))
	writeCredentialHashField(hash, []byte(accountInstanceID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], accountGeneration)
	writeCredentialHashField(hash, generation[:])
	writeCredentialHashField(hash, []byte(kind))
	writeCredentialHashField(hash, []byte(target))
	writeCredentialHashField(hash, locator[:])
	writeCredentialHashField(hash, expectedDigest[:])
	writeCredentialHashField(hash, intent[:])
	var operationID CredentialOperationID
	copy(operationID[:], hash.Sum(nil))
	return operationID, nil
}

var (
	// ErrCredentialOperationBusy means another exact account generation owns the lane.
	ErrCredentialOperationBusy = errors.New("credential operation lane busy")
	// ErrCredentialOperationRecoveryRequired means durable external state must be inspected.
	ErrCredentialOperationRecoveryRequired = errors.New("credential operation recovery required")
	// ErrCredentialOperationState means a phase transition lost its exact state fence.
	ErrCredentialOperationState = errors.New("credential operation state changed")
	// ErrCredentialOperationOwner means the exact process owner or recovery epoch changed.
	ErrCredentialOperationOwner = errors.New("credential operation owner changed")
	// ErrCredentialOperationEvidenceActive means deletion would destroy active or unacknowledged evidence.
	ErrCredentialOperationEvidenceActive = errors.New("credential operation evidence is active")
	// ErrCredentialOperationSettlementRequired means an exact terminal write must publish before new work.
	ErrCredentialOperationSettlementRequired = errors.New("credential operation settlement required")
)

// CredentialOperationPageLimit is the hard maximum for recovery and GC pages.
const CredentialOperationPageLimit = 256

// CredentialOperationFence authorizes one exact owner epoch transition.
type CredentialOperationFence struct {
	Token string
	Owner proc.Record
	Epoch uint64
}

// CredentialOperation is one durable, generation-fenced external credential lane.
type CredentialOperation struct {
	AccountID          int
	OperationID        CredentialOperationID
	Token              string
	Kind               CredentialOperationKind
	Target             CredentialTarget
	IntentDigest       CredentialDigest
	AccountInstanceID  string
	AccountGeneration  uint64
	LocatorDigest      CredentialDigest
	Owner              proc.Record
	OwnerEpoch         uint64
	State              CredentialOperationState
	Expected           CredentialExternalState
	Outcome            CredentialExternalState
	HasOutcome         bool
	TerminalStatus     CredentialTerminalStatus
	Result             CredentialResultCategory
	FailureClass       CredentialFailureClass
	PublicationPayload []byte
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Fence returns the exact authority for the operation's current owner epoch.
func (operation CredentialOperation) Fence() CredentialOperationFence {
	return CredentialOperationFence{
		Token: operation.Token, Owner: operation.Owner, Epoch: operation.OwnerEpoch,
	}
}

// CredentialOperationReceipt is an immutable replayable terminal result.
type CredentialOperationReceipt struct {
	AccountID          int
	OperationID        CredentialOperationID
	Token              string
	Kind               CredentialOperationKind
	Target             CredentialTarget
	IntentDigest       CredentialDigest
	AccountInstanceID  string
	AccountGeneration  uint64
	LocatorDigest      CredentialDigest
	Expected           CredentialExternalState
	Owner              proc.Record
	OwnerEpoch         uint64
	TerminalStatus     CredentialTerminalStatus
	Result             CredentialResultCategory
	FailureClass       CredentialFailureClass
	Outcome            CredentialExternalState
	PublicationPayload []byte
	CommittedAt        time.Time
	AcknowledgedAt     time.Time
	ExpiresAt          time.Time
}

// CredentialQuarantine blocks mutation until explicit verified and audited resolution.
type CredentialQuarantine struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	LocatorDigest     CredentialDigest
	Observation       CredentialExternalState
	TokenChainDigest  *CredentialDigest
	Reason            CredentialResultCategory
	FailureClass      CredentialFailureClass
	CreatedAt         time.Time
}

// BeginCredentialOperationRequest describes one exact idempotent operation.
type BeginCredentialOperationRequest struct {
	OperationID       CredentialOperationID
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	LocatorDigest     CredentialDigest
	Owner             proc.Record
	Kind              CredentialOperationKind
	Target            CredentialTarget
	IntentDigest      CredentialDigest
	Expected          CredentialExternalState
}

// BeginCredentialOperationResult returns exactly one active lane or immutable receipt.
type BeginCredentialOperationResult struct {
	Active  *CredentialOperation
	Receipt *CredentialOperationReceipt
	Created bool
}

// CredentialOperationEvidenceQuery identifies one exact retained operation scope.
type CredentialOperationEvidenceQuery struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	LocatorDigest     CredentialDigest
	Kind              CredentialOperationKind
	Target            CredentialTarget
	IntentDigest      CredentialDigest
}

// BeginCredentialOperation returns prior terminal evidence before admitting new work.
func (s *Store) BeginCredentialOperation(
	request BeginCredentialOperationRequest,
) (BeginCredentialOperationResult, error) {
	now := s.now()
	if err := validateCredentialOperationRequest(request); err != nil {
		return BeginCredentialOperationResult{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return BeginCredentialOperationResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := credentialOperationReceiptByID(tx, request.OperationID)
	if err == nil {
		if !credentialReceiptMatchesRequest(receipt, request) {
			return BeginCredentialOperationResult{}, ErrCredentialOperationRecoveryRequired
		}
		if err := tx.Commit(); err != nil {
			return BeginCredentialOperationResult{}, err
		}
		return BeginCredentialOperationResult{Receipt: &receipt}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BeginCredentialOperationResult{}, err
	}
	pending, err := unacknowledgedCredentialWriteReceiptByAccount(tx, request.AccountID)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return BeginCredentialOperationResult{}, err
		}
		return BeginCredentialOperationResult{Receipt: &pending}, ErrCredentialOperationSettlementRequired
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BeginCredentialOperationResult{}, err
	}
	if _, err := credentialQuarantine(tx, request.AccountID); err == nil {
		return BeginCredentialOperationResult{}, ErrCredentialOperationRecoveryRequired
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginCredentialOperationResult{}, err
	}
	if mutation, err := accountMutationByAccount(tx, request.AccountID); err == nil {
		if mutation.Kind == AccountMutationPresentationRebind {
			return BeginCredentialOperationResult{}, ErrAccountPresentationQuarantined
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginCredentialOperationResult{}, err
	}
	if removal, err := accountRemovalByID(tx, request.AccountID); err == nil {
		allowed, err := pendingAddRemovalAllowsCredentialCompensation(tx, removal, request)
		if err != nil {
			return BeginCredentialOperationResult{}, err
		}
		if !allowed {
			return BeginCredentialOperationResult{}, ErrAccountRemoving
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginCredentialOperationResult{}, err
	}
	if _, err := tx.Exec(`UPDATE accounts SET id=id WHERE id=? AND deleted_at IS NULL`, request.AccountID); err != nil {
		return BeginCredentialOperationResult{}, err
	}
	if err := credentialAccountMatchesRequest(tx, request); err != nil {
		return BeginCredentialOperationResult{}, err
	}
	token, err := newCredentialOperationToken()
	if err != nil {
		return BeginCredentialOperationResult{}, err
	}
	ownerRecord, err := encodeCredentialOwner(request.Owner)
	if err != nil {
		return BeginCredentialOperationResult{}, err
	}
	result, err := tx.Exec(
		`INSERT INTO credential_operations(
		 account_id,operation_id,token,kind,target,intent_digest,
		 account_instance_id,account_generation,locator_digest,
		 owner_record,owner_epoch,state,
		 expected_keychain_state,expected_keychain_digest,
		 created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,1,'prepared',?,?,?,?)
		 ON CONFLICT(account_id) DO NOTHING`,
		request.AccountID, request.OperationID[:], token, request.Kind, request.Target,
		request.IntentDigest[:], request.AccountInstanceID, request.AccountGeneration,
		request.LocatorDigest[:], ownerRecord,
		request.Expected.Keychain.State, credentialDigestValue(request.Expected.Keychain.Digest),
		now.UnixNano(), now.UnixNano(),
	)
	if err != nil {
		return BeginCredentialOperationResult{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return BeginCredentialOperationResult{}, err
	}
	operation, err := credentialOperationByAccount(tx, request.AccountID)
	if err != nil {
		return BeginCredentialOperationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return BeginCredentialOperationResult{}, err
	}
	begin := BeginCredentialOperationResult{Active: &operation, Created: rows == 1}
	if rows == 1 {
		return begin, nil
	}
	if operation.OperationID == request.OperationID {
		if !credentialOperationMatchesRequest(operation, request) {
			return BeginCredentialOperationResult{}, ErrCredentialOperationRecoveryRequired
		}
		return begin, nil
	}
	if !operationIdentityMatchesRequest(operation, request) {
		return begin, ErrCredentialOperationRecoveryRequired
	}
	return begin, ErrCredentialOperationBusy
}

func pendingAddRemovalAllowsCredentialCompensation(
	tx *sql.Tx,
	removal AccountRemoval,
	request BeginCredentialOperationRequest,
) (bool, error) {
	if !removal.DeleteCredential || request.Kind != CredentialOperationCompensate ||
		request.Target != CredentialTargetKeychain || removal.AccountID != request.AccountID ||
		removal.AccountInstanceID != request.AccountInstanceID ||
		removal.AccountGeneration != request.AccountGeneration {
		return false, nil
	}
	mutation, err := accountMutationByAccount(tx, request.AccountID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if mutation.State != AccountMutationCompensating || removal.RegistrySequence <= mutation.RegistrySequence {
		return false, nil
	}
	err = credentialPendingAddCompensationMatches(
		tx, request.AccountID, request.AccountInstanceID, request.AccountGeneration,
		request.LocatorDigest, request.Expected, request.IntentDigest,
	)
	if errors.Is(err, ErrAccountGenerationChanged) {
		return false, nil
	}
	return err == nil, err
}

// CredentialOperation returns the durable lane for accountID.
func (s *Store) CredentialOperation(accountID int) (CredentialOperation, error) {
	if accountID <= 0 {
		return CredentialOperation{}, errors.New("credential operation account id must be positive")
	}
	return credentialOperationByAccount(s.db, accountID)
}

// CredentialOperationByToken returns one exact durable operation.
func (s *Store) CredentialOperationByToken(token string) (CredentialOperation, error) {
	return credentialOperationByToken(s.db, token)
}

// CredentialOperationEvidence returns the sole exact active operation or retained receipt.
func (s *Store) CredentialOperationEvidence(
	query CredentialOperationEvidenceQuery,
) (active *CredentialOperation, receipt *CredentialOperationReceipt, err error) {
	if query.AccountID <= 0 || validateAccountInstanceID(query.AccountInstanceID) != nil ||
		query.AccountGeneration == 0 || query.LocatorDigest.zero() ||
		!validCredentialKindTarget(query.Kind, query.Target) ||
		query.IntentDigest.zero() {
		return nil, nil, ErrCredentialOperationState
	}
	operation, err := credentialOperationByAccount(s.db, query.AccountID)
	if err == nil {
		if credentialOperationMatchesEvidence(operation, query) {
			active = &operation
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, err
	}
	rows, err := s.db.Query(
		`SELECT `+receiptSelectColumns+` FROM credential_operation_receipts
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=? AND kind=? AND target=? AND intent_digest=?
		 ORDER BY committed_at DESC,operation_id DESC LIMIT 2`,
		query.AccountID, query.AccountInstanceID, query.AccountGeneration,
		query.LocatorDigest[:], query.Kind, query.Target,
		query.IntentDigest[:],
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		current, err := scanCredentialOperationReceipt(rows)
		if err != nil {
			return nil, nil, err
		}
		if receipt != nil || active != nil {
			return nil, nil, ErrCredentialOperationState
		}
		receipt = &current
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return active, receipt, nil
}

func credentialOperationMatchesEvidence(
	operation CredentialOperation,
	query CredentialOperationEvidenceQuery,
) bool {
	return operation.AccountID == query.AccountID &&
		operation.AccountInstanceID == query.AccountInstanceID &&
		operation.AccountGeneration == query.AccountGeneration &&
		operation.LocatorDigest == query.LocatorDigest &&
		operation.Kind == query.Kind && operation.Target == query.Target &&
		operation.IntentDigest == query.IntentDigest
}

// CredentialOperationsOwnedBy returns one bounded stable account-id page.
func (s *Store) CredentialOperationsOwnedBy(
	owner proc.Record,
	afterAccountID, limit int,
) (operations []CredentialOperation, more bool, err error) {
	if err := owner.Validate(); err != nil {
		return nil, false, err
	}
	if afterAccountID < 0 || limit <= 0 || limit > CredentialOperationPageLimit {
		return nil, false, errors.New("credential operation page is invalid")
	}
	ownerRecord, err := encodeCredentialOwner(owner)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.db.Query(
		`SELECT `+operationSelectColumns+`
		 FROM credential_operations
		 WHERE owner_record=? AND account_id>?
		 ORDER BY account_id LIMIT ?`,
		ownerRecord, afterAccountID, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	operations = make([]CredentialOperation, 0, limit)
	for rows.Next() {
		operation, err := scanCredentialOperation(rows)
		if err != nil {
			return nil, false, err
		}
		if len(operations) == limit {
			more = true
			break
		}
		operations = append(operations, operation)
	}
	return operations, more, rows.Err()
}

// MarkCredentialOperationApplying crosses the first external-I/O boundary.
func (s *Store) MarkCredentialOperationApplying(
	fence CredentialOperationFence,
	publicationPayload []byte,
) (CredentialOperation, error) {
	return s.advanceCredentialOperation(
		fence, CredentialOperationPrepared, CredentialOperationApplying,
		CredentialExternalState{}, false, "", "", CredentialFailureNone, publicationPayload,
	)
}

// StageCredentialOperationPublication durably records immutable publication
// bytes learned after an operation crossed its external-I/O boundary.
func (s *Store) StageCredentialOperationPublication(
	fence CredentialOperationFence,
	publicationPayload []byte,
) (CredentialOperation, error) {
	if err := validateCredentialFence(fence); err != nil {
		return CredentialOperation{}, err
	}
	if len(publicationPayload) == 0 || len(publicationPayload) > CredentialPublicationPayloadMaxBytes {
		return CredentialOperation{}, errors.New("credential publication payload is invalid")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return CredentialOperation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_operations SET updated_at=updated_at WHERE token=?`, fence.Token,
	); err != nil {
		return CredentialOperation{}, err
	}
	operation, err := credentialOperationByToken(tx, fence.Token)
	if err != nil {
		return CredentialOperation{}, err
	}
	if err := requireCredentialFence(operation, fence); err != nil {
		return CredentialOperation{}, err
	}
	if err := credentialAccountMatchesOperation(tx, operation); err != nil {
		return operation, err
	}
	if operation.State != CredentialOperationApplying || operation.HasOutcome {
		return operation, ErrCredentialOperationState
	}
	if len(operation.PublicationPayload) != 0 {
		if !bytes.Equal(operation.PublicationPayload, publicationPayload) {
			return operation, ErrCredentialOperationState
		}
		if err := tx.Commit(); err != nil {
			return CredentialOperation{}, err
		}
		return operation, nil
	}
	now := s.now()
	updated, err := tx.Exec(
		`UPDATE credential_operations SET publication_payload=?,updated_at=?
		 WHERE token=? AND owner_record=? AND owner_epoch=? AND state='applying' AND publication_payload IS NULL`,
		bytes.Clone(publicationPayload), now.UnixNano(), fence.Token,
		mustEncodeCredentialOwner(fence.Owner), fence.Epoch,
	)
	if err != nil {
		return CredentialOperation{}, err
	}
	rows, err := updated.RowsAffected()
	if err != nil {
		return CredentialOperation{}, err
	}
	if rows != 1 {
		return CredentialOperation{}, ErrCredentialOperationState
	}
	operation, err = credentialOperationByToken(tx, fence.Token)
	if err != nil {
		return CredentialOperation{}, err
	}
	if err := tx.Commit(); err != nil {
		return CredentialOperation{}, err
	}
	return operation, nil
}

// MarkCredentialOperationApplied records the exact expected terminal state.
func (s *Store) MarkCredentialOperationApplied(
	fence CredentialOperationFence,
	outcome CredentialExternalState,
	status CredentialTerminalStatus,
	result CredentialResultCategory,
	failure CredentialFailureClass,
	publicationPayload []byte,
) (CredentialOperation, error) {
	if err := outcome.validate(); err != nil {
		return CredentialOperation{}, err
	}
	return s.advanceCredentialOperation(
		fence, CredentialOperationApplying, CredentialOperationApplied,
		outcome, true, status, result, failure, publicationPayload,
	)
}

// CommitPreparedCredentialOperation seals a proven no-I/O result.
func (s *Store) CommitPreparedCredentialOperation(
	fence CredentialOperationFence,
	actual CredentialExternalState,
	result CredentialResultCategory,
	receiptExpiresAt time.Time,
) (CredentialOperationReceipt, error) {
	return s.settleCredentialOperation(settleCredentialRequest{
		Fence: fence, Actual: actual,
		Status: CredentialTerminalSucceeded, Result: result,
		ReceiptExpiresAt: receiptExpiresAt,
		RequiredState:    CredentialOperationPrepared,
		VerifyExpected:   true, RequireCurrentAccount: true,
	})
}

// CommitCredentialOperation seals the exact result recorded at Applied.
func (s *Store) CommitCredentialOperation(
	fence CredentialOperationFence,
	actual CredentialExternalState,
	quarantineTokenChainDigest *CredentialDigest,
	receiptExpiresAt time.Time,
) (CredentialOperationReceipt, error) {
	return s.settleCredentialOperation(settleCredentialRequest{
		Fence: fence, Actual: actual, ReceiptExpiresAt: receiptExpiresAt,
		QuarantineTokenChainDigest: quarantineTokenChainDigest,
		RequiredState:              CredentialOperationApplied,
		UseAppliedTerminal:         true, VerifyOutcome: true, RequireCurrentAccount: true,
	})
}

// ResolveCredentialOperation seals an inspected state after verified owner retirement.
func (s *Store) ResolveCredentialOperation(
	fence CredentialOperationFence,
	actual CredentialExternalState,
	status CredentialTerminalStatus,
	result CredentialResultCategory,
	failure CredentialFailureClass,
	publicationPayload []byte,
	receiptExpiresAt time.Time,
) (CredentialOperationReceipt, error) {
	if fence.Epoch <= 1 {
		return CredentialOperationReceipt{}, ErrCredentialOperationOwner
	}
	return s.settleCredentialOperation(settleCredentialRequest{
		Fence: fence, Actual: actual, Status: status, Result: result,
		FailureClass:       failure,
		PublicationPayload: publicationPayload,
		ReceiptExpiresAt:   receiptExpiresAt,
		Recovery:           true, RequireCurrentAccount: status != CredentialTerminalQuarantined,
	})
}

type settleCredentialRequest struct {
	Fence                      CredentialOperationFence
	Actual                     CredentialExternalState
	Status                     CredentialTerminalStatus
	Result                     CredentialResultCategory
	FailureClass               CredentialFailureClass
	ReceiptExpiresAt           time.Time
	RequiredState              CredentialOperationState
	UseAppliedTerminal         bool
	VerifyExpected             bool
	VerifyOutcome              bool
	Recovery                   bool
	RequireCurrentAccount      bool
	PublicationPayload         []byte
	QuarantineTokenChainDigest *CredentialDigest
}

func (s *Store) settleCredentialOperation(
	request settleCredentialRequest,
) (CredentialOperationReceipt, error) {
	if err := request.Actual.validate(); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if request.QuarantineTokenChainDigest != nil && request.QuarantineTokenChainDigest.zero() {
		return CredentialOperationReceipt{}, errors.New("credential quarantine token chain digest is invalid")
	}
	if err := validateCredentialFence(request.Fence); err != nil {
		return CredentialOperationReceipt{}, err
	}
	now := s.now()
	if !request.ReceiptExpiresAt.After(now) {
		return CredentialOperationReceipt{}, errors.New("credential operation receipt expiry must be in the future")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return CredentialOperationReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_operations SET updated_at=updated_at WHERE token=?`,
		request.Fence.Token,
	); err != nil {
		return CredentialOperationReceipt{}, err
	}
	operation, err := credentialOperationByToken(tx, request.Fence.Token)
	if errors.Is(err, sql.ErrNoRows) {
		receipt, receiptErr := credentialOperationReceiptByToken(tx, request.Fence.Token)
		if receiptErr != nil {
			return CredentialOperationReceipt{}, receiptErr
		}
		if !receiptFenceMatches(receipt, request.Fence) ||
			!sameCredentialExternalState(receipt.Outcome, request.Actual) ||
			(!request.UseAppliedTerminal && request.Status != "" &&
				(receipt.TerminalStatus != request.Status || receipt.Result != request.Result ||
					receipt.FailureClass != request.FailureClass ||
					!bytes.Equal(receipt.PublicationPayload, request.PublicationPayload))) {
			return receipt, ErrCredentialOperationState
		}
		if request.QuarantineTokenChainDigest != nil {
			quarantine, quarantineErr := credentialQuarantine(tx, receipt.AccountID)
			if quarantineErr != nil || quarantine.TokenChainDigest == nil ||
				*quarantine.TokenChainDigest != *request.QuarantineTokenChainDigest {
				return receipt, errors.Join(ErrCredentialOperationState, quarantineErr)
			}
		}
		return receipt, nil
	}
	if err != nil {
		return CredentialOperationReceipt{}, err
	}
	if err := requireCredentialFence(operation, request.Fence); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if request.Recovery {
		if operation.State == CredentialOperationPrepared {
			return CredentialOperationReceipt{}, ErrCredentialOperationState
		}
	} else if operation.State != request.RequiredState {
		return CredentialOperationReceipt{}, ErrCredentialOperationState
	}
	status, result, failure := request.Status, request.Result, request.FailureClass
	publicationPayload := request.PublicationPayload
	if request.UseAppliedTerminal {
		status, result = operation.TerminalStatus, operation.Result
		failure = operation.FailureClass
		publicationPayload = operation.PublicationPayload
	}
	if err := validateCredentialTerminal(operation.Kind, status, result, failure); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if request.QuarantineTokenChainDigest != nil && status != CredentialTerminalQuarantined {
		return CredentialOperationReceipt{}, errors.New("credential quarantine token chain digest requires quarantine")
	}
	if err := validateCredentialPublicationPayload(status, result, publicationPayload); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if request.VerifyExpected && !sameCredentialExternalState(operation.Expected, request.Actual) {
		return CredentialOperationReceipt{}, ErrCredentialOperationRecoveryRequired
	}
	if request.VerifyOutcome &&
		(!operation.HasOutcome || !sameCredentialExternalState(operation.Outcome, request.Actual)) {
		return CredentialOperationReceipt{}, ErrCredentialOperationRecoveryRequired
	}
	if request.RequireCurrentAccount {
		if err := credentialAccountMatchesOperation(tx, operation); err != nil {
			return CredentialOperationReceipt{}, err
		}
	}
	receipt := receiptFromOperation(
		operation, request.Actual, status, result, failure, publicationPayload, now, request.ReceiptExpiresAt,
	)
	if err := insertCredentialOperationReceipt(tx, receipt); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if status == CredentialTerminalQuarantined {
		if err := upsertCredentialQuarantine(tx, CredentialQuarantine{
			AccountID: operation.AccountID, AccountInstanceID: operation.AccountInstanceID,
			AccountGeneration: operation.AccountGeneration, LocatorDigest: operation.LocatorDigest,
			Observation: request.Actual, TokenChainDigest: request.QuarantineTokenChainDigest,
			Reason:       result,
			FailureClass: failure, CreatedAt: now,
		}); err != nil {
			return CredentialOperationReceipt{}, err
		}
	}
	deleted, err := tx.Exec(
		`DELETE FROM credential_operations
		 WHERE token=? AND owner_record=? AND owner_epoch=?`,
		request.Fence.Token, mustEncodeCredentialOwner(request.Fence.Owner), request.Fence.Epoch,
	)
	if err != nil {
		return CredentialOperationReceipt{}, err
	}
	rows, err := deleted.RowsAffected()
	if err != nil {
		return CredentialOperationReceipt{}, err
	}
	if rows != 1 {
		return CredentialOperationReceipt{}, ErrCredentialOperationState
	}
	if err := tx.Commit(); err != nil {
		return CredentialOperationReceipt{}, err
	}
	return receipt, nil
}

// TakeoverCredentialOperation transfers a provably retired lane into a new owner epoch.
func (s *Store) TakeoverCredentialOperation(
	ctx context.Context,
	expected CredentialOperationFence,
	newOwner proc.Record,
	receipt proc.ReapReceipt,
	verifier ProcessRetirementVerifier,
) (CredentialOperation, error) {
	now := s.now()
	if err := validateCredentialFence(expected); err != nil {
		return CredentialOperation{}, err
	}
	if err := newOwner.Validate(); err != nil {
		return CredentialOperation{}, err
	}
	if err := verifyProcessRetirement(ctx, expected.Owner, newOwner, receipt, verifier); err != nil {
		return CredentialOperation{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return CredentialOperation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_operations SET updated_at=updated_at WHERE token=?`, expected.Token,
	); err != nil {
		return CredentialOperation{}, err
	}
	operation, err := credentialOperationByToken(tx, expected.Token)
	if err != nil {
		return CredentialOperation{}, err
	}
	if err := requireCredentialFence(operation, expected); err != nil {
		return CredentialOperation{}, err
	}
	ownerRecord, err := encodeCredentialOwner(newOwner)
	if err != nil {
		return CredentialOperation{}, err
	}
	result, err := tx.Exec(
		`UPDATE credential_operations
		 SET owner_record=?,owner_epoch=owner_epoch+1,updated_at=?
		 WHERE token=? AND owner_record=? AND owner_epoch=?`,
		ownerRecord, now.UnixNano(), expected.Token,
		mustEncodeCredentialOwner(expected.Owner), expected.Epoch,
	)
	if err != nil {
		return CredentialOperation{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return CredentialOperation{}, err
	}
	if rows != 1 {
		return CredentialOperation{}, ErrCredentialOperationOwner
	}
	operation, err = credentialOperationByToken(tx, expected.Token)
	if err != nil {
		return CredentialOperation{}, err
	}
	if err := tx.Commit(); err != nil {
		return CredentialOperation{}, err
	}
	return operation, nil
}

func verifyProcessRetirement(
	ctx context.Context,
	expectedOwner proc.Record,
	newOwner proc.Record,
	receipt proc.ReapReceipt,
	verifier ProcessRetirementVerifier,
) error {
	if verifier == nil {
		return ErrCredentialOperationOwner
	}
	if err := receipt.Validate(); err != nil {
		return errors.Join(ErrCredentialOperationOwner, err)
	}
	if !sameCredentialOwner(receipt.Record, expectedOwner) ||
		receipt.ReaperGeneration != newOwner.Generation {
		return ErrCredentialOperationOwner
	}
	if err := verifier.VerifyReapReceipt(ctx, receipt); err != nil {
		return errors.Join(ErrCredentialOperationOwner, err)
	}
	return nil
}

// AbandonPreparedCredentialOperation removes a proven no-I/O lane.
func (s *Store) AbandonPreparedCredentialOperation(fence CredentialOperationFence) error {
	if err := validateCredentialFence(fence); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_operations SET updated_at=updated_at WHERE token=?`, fence.Token,
	); err != nil {
		return err
	}
	operation, err := credentialOperationByToken(tx, fence.Token)
	if errors.Is(err, sql.ErrNoRows) {
		if _, receiptErr := credentialOperationReceiptByToken(tx, fence.Token); receiptErr == nil {
			return ErrCredentialOperationState
		} else if !errors.Is(receiptErr, sql.ErrNoRows) {
			return receiptErr
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if err := requireCredentialFence(operation, fence); err != nil {
		return err
	}
	if operation.State != CredentialOperationPrepared {
		return ErrCredentialOperationState
	}
	if err := credentialAccountMatchesOperation(tx, operation); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM credential_operations WHERE token=? AND owner_record=? AND owner_epoch=?`,
		fence.Token, mustEncodeCredentialOwner(fence.Owner), fence.Epoch,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// CredentialOperationReceipt returns one immutable terminal result by token.
func (s *Store) CredentialOperationReceipt(token string) (CredentialOperationReceipt, error) {
	if err := validateSelectionToken(token); err != nil {
		return CredentialOperationReceipt{}, err
	}
	return credentialOperationReceiptByToken(s.db, token)
}

// CredentialOperationReceiptByID returns one immutable semantic result.
func (s *Store) CredentialOperationReceiptByID(
	operationID CredentialOperationID,
) (CredentialOperationReceipt, error) {
	if operationID.zero() {
		return CredentialOperationReceipt{}, errors.New("credential operation id is required")
	}
	return credentialOperationReceiptByID(s.db, operationID)
}

// UnacknowledgedCredentialWriteReceipts returns one stable account-id page.
func (s *Store) UnacknowledgedCredentialWriteReceipts(
	afterAccountID, limit int,
) (receipts []CredentialOperationReceipt, more bool, err error) {
	if afterAccountID < 0 || limit <= 0 || limit > CredentialOperationPageLimit {
		return nil, false, errors.New("credential write receipt page is invalid")
	}
	rows, err := s.db.Query(
		`SELECT `+receiptSelectColumns+` FROM credential_operation_receipts
		 WHERE account_id>? AND acknowledged_at IS NULL AND terminal_status='succeeded'
		 AND result_category IN ('refreshed','installed','moved')
		 ORDER BY account_id LIMIT ?`,
		afterAccountID, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	receipts = make([]CredentialOperationReceipt, 0, limit)
	for rows.Next() {
		receipt, err := scanCredentialOperationReceipt(rows)
		if err != nil {
			return nil, false, err
		}
		if len(receipts) == limit {
			more = true
			break
		}
		receipts = append(receipts, receipt)
	}
	return receipts, more, rows.Err()
}

// AcknowledgeCredentialOperation records delivery without invalidating replay for other waiters.
func (s *Store) AcknowledgeCredentialOperation(token string) error {
	if err := validateSelectionToken(token); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_operations SET updated_at=updated_at WHERE token=?`, token,
	); err != nil {
		return err
	}
	result, err := tx.Exec(
		`UPDATE credential_operation_receipts
		 SET acknowledged_at=COALESCE(acknowledged_at,?) WHERE token=?`,
		s.now().UnixNano(), token,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		if _, activeErr := credentialOperationByToken(tx, token); activeErr == nil {
			return ErrCredentialOperationState
		} else if !errors.Is(activeErr, sql.ErrNoRows) {
			return activeErr
		}
	}
	return tx.Commit()
}

// AcknowledgeCredentialQuarantine records delivery of the exact receipt that
// installed quarantine. Quarantine remains in place until an exact clear, so
// a crash between acknowledgement and clear stays fail-closed.
func (s *Store) AcknowledgeCredentialQuarantine(quarantine CredentialQuarantine) error {
	if quarantine.AccountID <= 0 || quarantine.AccountGeneration == 0 ||
		quarantine.LocatorDigest.zero() ||
		!quarantine.Reason.quarantine() || !quarantine.FailureClass.quarantine() ||
		quarantine.CreatedAt.IsZero() {
		return errors.New("credential quarantine is invalid")
	}
	if err := validateAccountInstanceID(quarantine.AccountInstanceID); err != nil {
		return err
	}
	if err := quarantine.Observation.validate(); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_quarantines SET account_id=account_id WHERE account_id=?`,
		quarantine.AccountID,
	); err != nil {
		return err
	}
	stored, err := credentialQuarantine(tx, quarantine.AccountID)
	if err != nil {
		return err
	}
	if !sameCredentialQuarantine(stored, quarantine) {
		return ErrCredentialOperationState
	}
	result, err := tx.Exec(
		`UPDATE credential_operation_receipts
		 SET acknowledged_at=COALESCE(acknowledged_at,?)
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=?
		 AND terminal_status='quarantined' AND result_category=? AND failure_class=?
		 AND outcome_keychain_state=? AND outcome_keychain_digest IS ?
		 AND committed_at=?`,
		s.now().UnixNano(), quarantine.AccountID, quarantine.AccountInstanceID,
		quarantine.AccountGeneration, quarantine.LocatorDigest[:],
		quarantine.Reason, quarantine.FailureClass, quarantine.Observation.Keychain.State,
		credentialDigestValue(quarantine.Observation.Keychain.Digest), quarantine.CreatedAt.UnixNano(),
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 1 {
		return ErrCredentialOperationState
	}
	return tx.Commit()
}

// QuarantineCredentialRequest describes a generation-fenced ambiguity discovered after replay.
type QuarantineCredentialRequest struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	LocatorDigest     CredentialDigest
	Observation       CredentialExternalState
	TokenChainDigest  *CredentialDigest
	Reason            CredentialResultCategory
	FailureClass      CredentialFailureClass
}

// QuarantineCredential installs one typed ambiguity fence without an active lane.
func (s *Store) QuarantineCredential(
	request QuarantineCredentialRequest,
) (CredentialQuarantine, error) {
	if request.AccountID <= 0 || request.AccountGeneration == 0 ||
		request.LocatorDigest.zero() {
		return CredentialQuarantine{}, errors.New("credential quarantine identity is invalid")
	}
	if err := validateAccountInstanceID(request.AccountInstanceID); err != nil {
		return CredentialQuarantine{}, err
	}
	if err := request.Observation.validate(); err != nil {
		return CredentialQuarantine{}, err
	}
	if request.TokenChainDigest != nil && request.TokenChainDigest.zero() {
		return CredentialQuarantine{}, errors.New("credential quarantine token chain digest is invalid")
	}
	if !request.Reason.quarantine() {
		return CredentialQuarantine{}, errors.New("credential quarantine reason is invalid")
	}
	if !request.FailureClass.quarantine() {
		return CredentialQuarantine{}, errors.New("credential quarantine failure class is invalid")
	}
	now := s.now()
	tx, err := s.db.Begin()
	if err != nil {
		return CredentialQuarantine{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`UPDATE accounts SET id=id WHERE id=?`, request.AccountID); err != nil {
		return CredentialQuarantine{}, err
	}
	if err := credentialAccountMatchesIdentity(
		tx, request.AccountID, request.AccountInstanceID,
		request.AccountGeneration, request.LocatorDigest,
	); err != nil {
		return CredentialQuarantine{}, err
	}
	quarantine := CredentialQuarantine{
		AccountID: request.AccountID, AccountInstanceID: request.AccountInstanceID,
		AccountGeneration: request.AccountGeneration, LocatorDigest: request.LocatorDigest,
		Observation: request.Observation, TokenChainDigest: request.TokenChainDigest,
		Reason:       request.Reason,
		FailureClass: request.FailureClass, CreatedAt: now,
	}
	if _, err := tx.Exec(
		`INSERT INTO credential_quarantines(
		 account_id,account_instance_id,account_generation,locator_digest,
		 observation_keychain_state,observation_keychain_digest,
		 token_chain_digest,reason,failure_class,created_at
		) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO NOTHING`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.LocatorDigest[:], quarantine.Observation.Keychain.State,
		credentialDigestValue(quarantine.Observation.Keychain.Digest), credentialDigestValue(quarantine.TokenChainDigest),
		quarantine.Reason, quarantine.FailureClass,
		quarantine.CreatedAt.UnixNano(),
	); err != nil {
		return CredentialQuarantine{}, err
	}
	stored, err := credentialQuarantine(tx, request.AccountID)
	if err != nil {
		return CredentialQuarantine{}, err
	}
	if !sameCredentialQuarantine(stored, quarantine) {
		return stored, ErrCredentialOperationState
	}
	if err := tx.Commit(); err != nil {
		return CredentialQuarantine{}, err
	}
	return stored, nil
}

// CredentialQuarantine returns one account's active unsafe-state fence.
func (s *Store) CredentialQuarantine(accountID int) (CredentialQuarantine, error) {
	if accountID <= 0 {
		return CredentialQuarantine{}, errors.New("credential quarantine account id must be positive")
	}
	return credentialQuarantine(s.db, accountID)
}

// BindCredentialQuarantineTokenChain binds an unbound quarantine to the exact
// normalized token-chain set whose ambiguity it fences.
func (s *Store) BindCredentialQuarantineTokenChain(
	quarantine CredentialQuarantine,
	digest CredentialDigest,
) (CredentialQuarantine, error) {
	if err := validateCredentialQuarantineValue(quarantine); err != nil {
		return CredentialQuarantine{}, err
	}
	if digest.zero() {
		return CredentialQuarantine{}, errors.New("credential quarantine token chain digest is required")
	}
	if quarantine.TokenChainDigest != nil && *quarantine.TokenChainDigest != digest {
		return CredentialQuarantine{}, ErrCredentialOperationState
	}
	tx, err := s.db.Begin()
	if err != nil {
		return CredentialQuarantine{}, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.Exec(
		`UPDATE credential_quarantines
		 SET token_chain_digest=COALESCE(token_chain_digest,?)
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=?
		 AND observation_keychain_state=? AND observation_keychain_digest IS ?
		 AND reason=? AND failure_class=? AND created_at=?
		 AND (token_chain_digest IS NULL OR token_chain_digest=?)`,
		digest[:], quarantine.AccountID, quarantine.AccountInstanceID,
		quarantine.AccountGeneration, quarantine.LocatorDigest[:],
		quarantine.Observation.Keychain.State,
		credentialDigestValue(quarantine.Observation.Keychain.Digest),
		quarantine.Reason, quarantine.FailureClass, quarantine.CreatedAt.UnixNano(), digest[:],
	)
	if err != nil {
		return CredentialQuarantine{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return CredentialQuarantine{}, err
	}
	if rows != 1 {
		return CredentialQuarantine{}, ErrCredentialOperationState
	}
	bound, err := credentialQuarantine(tx, quarantine.AccountID)
	if err != nil {
		return CredentialQuarantine{}, err
	}
	if !sameCredentialQuarantineIdentity(bound, quarantine) ||
		bound.TokenChainDigest == nil || *bound.TokenChainDigest != digest {
		return CredentialQuarantine{}, ErrCredentialOperationState
	}
	if err := tx.Commit(); err != nil {
		return CredentialQuarantine{}, err
	}
	return bound, nil
}

// ClearCredentialQuarantine removes one exact unchanged fence.
func (s *Store) ClearCredentialQuarantine(quarantine CredentialQuarantine) error {
	if quarantine.AccountID <= 0 || quarantine.AccountGeneration == 0 ||
		quarantine.LocatorDigest.zero() ||
		!quarantine.Reason.quarantine() || !quarantine.FailureClass.quarantine() ||
		quarantine.CreatedAt.IsZero() {
		return errors.New("credential quarantine is invalid")
	}
	if err := validateAccountInstanceID(quarantine.AccountInstanceID); err != nil {
		return err
	}
	if err := quarantine.Observation.validate(); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_quarantines SET account_id=account_id WHERE account_id=?`,
		quarantine.AccountID,
	); err != nil {
		return err
	}
	var pendingAddEvidence int
	if err := tx.QueryRow(
		`SELECT EXISTS(
		   SELECT 1 FROM pending_adds AS pending
		   WHERE pending.id=? AND pending.instance_id=? AND pending.generation=?
		   AND (
		     EXISTS(
		       SELECT 1 FROM account_mutations AS mutation
		       WHERE mutation.account_id=pending.id
		       AND mutation.account_instance_id=pending.instance_id
		       AND mutation.account_generation=pending.generation
		       AND mutation.kind='add'
		     )
		     OR EXISTS(
		       SELECT 1 FROM account_mutation_receipts AS receipt
		       WHERE receipt.account_id=pending.id
		       AND receipt.account_instance_id=pending.instance_id
		       AND receipt.account_generation=pending.generation
		       AND receipt.kind='add' AND receipt.terminal='quarantined'
		       AND receipt.resolution IS NULL
		     )
		   )
		 )`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
	).Scan(&pendingAddEvidence); err != nil {
		return err
	}
	if pendingAddEvidence != 0 {
		return ErrCredentialOperationEvidenceActive
	}
	var activeEvidence int
	if err := tx.QueryRow(
		`SELECT EXISTS(
		   SELECT 1 FROM credential_operations
		   WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 ) OR EXISTS(
		   SELECT 1 FROM credential_operation_receipts
		   WHERE account_id=? AND account_instance_id=? AND account_generation=?
		   AND acknowledged_at IS NULL
		 ) OR EXISTS(
		   SELECT 1 FROM account_mutations
		   WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 ) OR EXISTS(
		   SELECT 1 FROM account_mutation_receipts
		   WHERE account_id=? AND account_instance_id=? AND account_generation=?
		   AND acknowledged_at IS NULL
		 )`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
	).Scan(&activeEvidence); err != nil {
		return err
	}
	if activeEvidence != 0 {
		return ErrCredentialOperationEvidenceActive
	}
	if err := deleteCredentialQuarantine(tx, quarantine); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteCredentialQuarantine(execer rowExecer, quarantine CredentialQuarantine) error {
	result, err := execer.Exec(
		`DELETE FROM credential_quarantines
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		   AND locator_digest=?
		   AND observation_keychain_state=? AND observation_keychain_digest IS ?
		   AND token_chain_digest IS ?
		   AND reason=? AND failure_class=? AND created_at=?`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.LocatorDigest[:], quarantine.Observation.Keychain.State,
		credentialDigestValue(quarantine.Observation.Keychain.Digest),
		credentialDigestValue(quarantine.TokenChainDigest),
		quarantine.Reason, quarantine.FailureClass,
		quarantine.CreatedAt.UnixNano(),
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrCredentialOperationState
	}
	return nil
}

const credentialReceiptPostAckRetention = 10 * time.Minute

// DeleteExpiredCredentialOperationReceipts removes one bounded page after expiry and post-ack retention.
func (s *Store) DeleteExpiredCredentialOperationReceipts(limit int) (int, error) {
	if limit <= 0 || limit > CredentialOperationPageLimit {
		return 0, errors.New("credential operation receipt limit is invalid")
	}
	now := s.now()
	result, err := s.db.Exec(
		`DELETE FROM credential_operation_receipts
		 WHERE token IN (
		   SELECT receipt.token FROM credential_operation_receipts AS receipt
		   WHERE receipt.acknowledged_at IS NOT NULL AND receipt.acknowledged_at<=? AND receipt.expires_at<=?
		   AND NOT EXISTS (
		     SELECT 1 FROM credential_quarantines AS quarantine
		     WHERE quarantine.account_id=receipt.account_id
		     AND quarantine.account_instance_id=receipt.account_instance_id
		     AND quarantine.account_generation=receipt.account_generation
		   )
		   ORDER BY receipt.expires_at,receipt.token
		   LIMIT ?
		 )`,
		now.Add(-credentialReceiptPostAckRetention).UnixNano(), now.UnixNano(), limit,
	)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rows), nil
}

func (s *Store) advanceCredentialOperation(
	fence CredentialOperationFence,
	from, to CredentialOperationState,
	outcome CredentialExternalState,
	hasOutcome bool,
	status CredentialTerminalStatus,
	result CredentialResultCategory,
	failure CredentialFailureClass,
	publicationPayload []byte,
) (CredentialOperation, error) {
	if err := validateCredentialFence(fence); err != nil {
		return CredentialOperation{}, err
	}
	now := s.now()
	tx, err := s.db.Begin()
	if err != nil {
		return CredentialOperation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE credential_operations SET updated_at=updated_at WHERE token=?`, fence.Token,
	); err != nil {
		return CredentialOperation{}, err
	}
	operation, err := credentialOperationByToken(tx, fence.Token)
	if err != nil {
		return CredentialOperation{}, err
	}
	if err := requireCredentialFence(operation, fence); err != nil {
		return CredentialOperation{}, err
	}
	if err := credentialAccountMatchesOperation(tx, operation); err != nil {
		return operation, err
	}
	if operation.State == to {
		if operation.HasOutcome == hasOutcome && bytes.Equal(operation.PublicationPayload, publicationPayload) &&
			(!hasOutcome || (sameCredentialExternalState(operation.Outcome, outcome) &&
				operation.TerminalStatus == status && operation.Result == result && operation.FailureClass == failure &&
				bytes.Equal(operation.PublicationPayload, publicationPayload))) {
			if err := tx.Commit(); err != nil {
				return CredentialOperation{}, err
			}
			return operation, nil
		}
		return operation, ErrCredentialOperationState
	}
	if operation.State != from {
		return operation, ErrCredentialOperationState
	}
	if hasOutcome {
		if err := outcome.validate(); err != nil {
			return CredentialOperation{}, err
		}
		if err := validateCredentialTerminal(operation.Kind, status, result, failure); err != nil {
			return CredentialOperation{}, err
		}
		if err := validateCredentialPublicationPayload(status, result, publicationPayload); err != nil {
			return CredentialOperation{}, err
		}
		if credentialResultPublishesWrite(result) &&
			!bytes.Equal(operation.PublicationPayload, publicationPayload) {
			return operation, ErrCredentialOperationState
		}
	} else if len(publicationPayload) > CredentialPublicationPayloadMaxBytes {
		return CredentialOperation{}, errors.New("credential publication payload exceeds its limit")
	} else if len(publicationPayload) != 0 && to != CredentialOperationApplying {
		return CredentialOperation{}, errors.New("credential publication payload requires an applying operation")
	}
	var outcomeKeychainState, outcomeKeychainDigest any
	var terminalStatus, resultCategory, failureClass any
	var storedPublicationPayload any
	if !hasOutcome && len(publicationPayload) != 0 {
		storedPublicationPayload = bytes.Clone(publicationPayload)
	}
	if hasOutcome {
		outcomeKeychainState = outcome.Keychain.State
		outcomeKeychainDigest = credentialDigestValue(outcome.Keychain.Digest)
		terminalStatus, resultCategory = status, result
		failureClass = credentialFailureClassValue(failure)
		if len(publicationPayload) != 0 {
			storedPublicationPayload = bytes.Clone(publicationPayload)
		}
	}
	updated, err := tx.Exec(
		`UPDATE credential_operations SET
		 state=?,outcome_keychain_state=?,outcome_keychain_digest=?,
		 terminal_status=?,result_category=?,failure_class=?,
		 publication_payload=?,
		 updated_at=?
		 WHERE token=? AND owner_record=? AND owner_epoch=? AND state=?`,
		to, outcomeKeychainState, outcomeKeychainDigest,
		terminalStatus, resultCategory, failureClass, storedPublicationPayload, now.UnixNano(), fence.Token,
		mustEncodeCredentialOwner(fence.Owner), fence.Epoch, from,
	)
	if err != nil {
		return CredentialOperation{}, err
	}
	rows, err := updated.RowsAffected()
	if err != nil {
		return CredentialOperation{}, err
	}
	if rows != 1 {
		return CredentialOperation{}, ErrCredentialOperationState
	}
	operation, err = credentialOperationByToken(tx, fence.Token)
	if err != nil {
		return CredentialOperation{}, err
	}
	if err := tx.Commit(); err != nil {
		return CredentialOperation{}, err
	}
	return operation, nil
}

const operationSelectColumns = `
account_id,operation_id,token,kind,target,intent_digest,
account_instance_id,account_generation,locator_digest,owner_record,owner_epoch,state,
expected_keychain_state,expected_keychain_digest,
outcome_keychain_state,outcome_keychain_digest,
terminal_status,result_category,failure_class,publication_payload,created_at,updated_at`

const receiptSelectColumns = `
account_id,operation_id,token,kind,target,intent_digest,
account_instance_id,account_generation,locator_digest,
expected_keychain_state,expected_keychain_digest,
owner_record,owner_epoch,terminal_status,result_category,failure_class,
outcome_keychain_state,outcome_keychain_digest,
publication_payload,committed_at,acknowledged_at,expires_at`

type credentialOperationQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func credentialOperationByAccount(
	queryer credentialOperationQueryer,
	accountID int,
) (CredentialOperation, error) {
	return scanCredentialOperation(queryer.QueryRow(
		`SELECT `+operationSelectColumns+` FROM credential_operations WHERE account_id=?`,
		accountID,
	))
}

func credentialOperationByToken(
	queryer credentialOperationQueryer,
	token string,
) (CredentialOperation, error) {
	if err := validateSelectionToken(token); err != nil {
		return CredentialOperation{}, err
	}
	return scanCredentialOperation(queryer.QueryRow(
		`SELECT `+operationSelectColumns+` FROM credential_operations WHERE token=?`,
		token,
	))
}

type credentialOperationScanner interface {
	Scan(...any) error
}

func scanCredentialOperation(row credentialOperationScanner) (CredentialOperation, error) {
	var (
		operation                                          CredentialOperation
		operationID, intentDigest, locatorDigest, ownerRaw []byte
		expectedKeychainDigest, outcomeKeychainDigest      []byte
		outcomeKeychainState                               sql.NullString
		terminalStatus, resultCategory, failureClass       sql.NullString
		publicationPayload                                 []byte
		createdAt, updatedAt                               int64
	)
	if err := row.Scan(
		&operation.AccountID, &operationID, &operation.Token, &operation.Kind,
		&operation.Target, &intentDigest, &operation.AccountInstanceID,
		&operation.AccountGeneration, &locatorDigest, &ownerRaw, &operation.OwnerEpoch,
		&operation.State, &operation.Expected.Keychain.State, &expectedKeychainDigest,
		&outcomeKeychainState, &outcomeKeychainDigest,
		&terminalStatus, &resultCategory, &failureClass, &publicationPayload, &createdAt, &updatedAt,
	); err != nil {
		return CredentialOperation{}, err
	}
	if err := scanCredentialOperationID(operationID, &operation.OperationID); err != nil {
		return CredentialOperation{}, err
	}
	if err := scanCredentialDigest(intentDigest, &operation.IntentDigest); err != nil {
		return CredentialOperation{}, err
	}
	if err := scanCredentialDigest(locatorDigest, &operation.LocatorDigest); err != nil {
		return CredentialOperation{}, err
	}
	if err := decodeCredentialOwner(ownerRaw, &operation.Owner); err != nil {
		return CredentialOperation{}, err
	}
	var err error
	operation.Expected.Keychain.Digest, err = scanOptionalCredentialDigest(expectedKeychainDigest)
	if err != nil {
		return CredentialOperation{}, err
	}
	operation.PublicationPayload = bytes.Clone(publicationPayload)
	if outcomeKeychainState.Valid {
		if !terminalStatus.Valid || !resultCategory.Valid {
			return CredentialOperation{}, errors.New("credential operation outcome is corrupt")
		}
		operation.HasOutcome = true
		operation.Outcome.Keychain.State = CredentialSlotState(outcomeKeychainState.String)
		operation.Outcome.Keychain.Digest, err = scanOptionalCredentialDigest(outcomeKeychainDigest)
		if err != nil {
			return CredentialOperation{}, err
		}
		operation.TerminalStatus = CredentialTerminalStatus(terminalStatus.String)
		operation.Result = CredentialResultCategory(resultCategory.String)
		if failureClass.Valid {
			operation.FailureClass = CredentialFailureClass(failureClass.String)
		}
	} else if terminalStatus.Valid || resultCategory.Valid || failureClass.Valid ||
		outcomeKeychainDigest != nil {
		return CredentialOperation{}, errors.New("credential operation outcome is corrupt")
	}
	operation.CreatedAt = time.Unix(0, createdAt)
	operation.UpdatedAt = time.Unix(0, updatedAt)
	if err := validateCredentialOperation(operation); err != nil {
		return CredentialOperation{}, err
	}
	return operation, nil
}

func credentialOperationReceiptByToken(
	queryer credentialOperationQueryer,
	token string,
) (CredentialOperationReceipt, error) {
	return scanCredentialOperationReceipt(queryer.QueryRow(
		`SELECT `+receiptSelectColumns+` FROM credential_operation_receipts WHERE token=?`,
		token,
	))
}

func credentialOperationReceiptByID(
	queryer credentialOperationQueryer,
	operationID CredentialOperationID,
) (CredentialOperationReceipt, error) {
	return scanCredentialOperationReceipt(queryer.QueryRow(
		`SELECT `+receiptSelectColumns+` FROM credential_operation_receipts WHERE operation_id=?`,
		operationID[:],
	))
}

func unacknowledgedCredentialWriteReceiptByAccount(
	queryer credentialOperationQueryer,
	accountID int,
) (CredentialOperationReceipt, error) {
	return scanCredentialOperationReceipt(queryer.QueryRow(
		`SELECT `+receiptSelectColumns+` FROM credential_operation_receipts
		 WHERE account_id=? AND acknowledged_at IS NULL AND terminal_status='succeeded'
		 AND result_category IN ('refreshed','installed')
		 ORDER BY committed_at,operation_id LIMIT 1`,
		accountID,
	))
}

func scanCredentialOperationReceipt(
	row credentialOperationScanner,
) (CredentialOperationReceipt, error) {
	var (
		receipt                                            CredentialOperationReceipt
		operationID, intentDigest, locatorDigest, ownerRaw []byte
		expectedKeychainDigest, outcomeKeychainDigest      []byte
		publicationPayload                                 []byte
		failureClass                                       sql.NullString
		committedAt, expiresAt                             int64
		acknowledgedAt                                     sql.NullInt64
	)
	if err := row.Scan(
		&receipt.AccountID, &operationID, &receipt.Token, &receipt.Kind, &receipt.Target,
		&intentDigest, &receipt.AccountInstanceID, &receipt.AccountGeneration,
		&locatorDigest, &receipt.Expected.Keychain.State, &expectedKeychainDigest,
		&ownerRaw, &receipt.OwnerEpoch,
		&receipt.TerminalStatus, &receipt.Result, &failureClass, &receipt.Outcome.Keychain.State,
		&outcomeKeychainDigest,
		&publicationPayload, &committedAt, &acknowledgedAt, &expiresAt,
	); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if err := scanCredentialOperationID(operationID, &receipt.OperationID); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if err := scanCredentialDigest(intentDigest, &receipt.IntentDigest); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if err := scanCredentialDigest(locatorDigest, &receipt.LocatorDigest); err != nil {
		return CredentialOperationReceipt{}, err
	}
	if err := decodeCredentialOwner(ownerRaw, &receipt.Owner); err != nil {
		return CredentialOperationReceipt{}, err
	}
	var err error
	receipt.Expected.Keychain.Digest, err = scanOptionalCredentialDigest(expectedKeychainDigest)
	if err != nil {
		return CredentialOperationReceipt{}, err
	}
	receipt.Outcome.Keychain.Digest, err = scanOptionalCredentialDigest(outcomeKeychainDigest)
	if err != nil {
		return CredentialOperationReceipt{}, err
	}
	receipt.PublicationPayload = bytes.Clone(publicationPayload)
	if failureClass.Valid {
		receipt.FailureClass = CredentialFailureClass(failureClass.String)
	}
	receipt.CommittedAt = time.Unix(0, committedAt)
	if acknowledgedAt.Valid {
		receipt.AcknowledgedAt = time.Unix(0, acknowledgedAt.Int64)
	}
	receipt.ExpiresAt = time.Unix(0, expiresAt)
	if err := validateCredentialReceipt(receipt); err != nil {
		return CredentialOperationReceipt{}, err
	}
	return receipt, nil
}

func insertCredentialOperationReceipt(
	tx *sql.Tx,
	receipt CredentialOperationReceipt,
) error {
	if err := validateCredentialReceipt(receipt); err != nil {
		return err
	}
	_, err := tx.Exec(
		`INSERT INTO credential_operation_receipts(
		 operation_id,token,account_id,account_instance_id,account_generation,locator_digest,
		 kind,target,intent_digest,
		 expected_keychain_state,expected_keychain_digest,
		 owner_record,owner_epoch,terminal_status,result_category,failure_class,
		 outcome_keychain_state,outcome_keychain_digest,
		 publication_payload,committed_at,acknowledged_at,expires_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		receipt.OperationID[:], receipt.Token, receipt.AccountID, receipt.AccountInstanceID,
		receipt.AccountGeneration, receipt.LocatorDigest[:], receipt.Kind, receipt.Target,
		receipt.IntentDigest[:], receipt.Expected.Keychain.State,
		credentialDigestValue(receipt.Expected.Keychain.Digest), mustEncodeCredentialOwner(receipt.Owner),
		receipt.OwnerEpoch, receipt.TerminalStatus, receipt.Result,
		credentialFailureClassValue(receipt.FailureClass), receipt.Outcome.Keychain.State,
		credentialDigestValue(receipt.Outcome.Keychain.Digest), credentialPublicationPayloadValue(receipt.PublicationPayload),
		receipt.CommittedAt.UnixNano(),
		nil, receipt.ExpiresAt.UnixNano(),
	)
	return err
}

func receiptFromOperation(
	operation CredentialOperation,
	outcome CredentialExternalState,
	status CredentialTerminalStatus,
	result CredentialResultCategory,
	failure CredentialFailureClass,
	publicationPayload []byte,
	committedAt, expiresAt time.Time,
) CredentialOperationReceipt {
	return CredentialOperationReceipt{
		AccountID: operation.AccountID, OperationID: operation.OperationID,
		Token: operation.Token, Kind: operation.Kind, Target: operation.Target,
		IntentDigest: operation.IntentDigest, AccountInstanceID: operation.AccountInstanceID,
		AccountGeneration: operation.AccountGeneration, LocatorDigest: operation.LocatorDigest,
		Expected: operation.Expected, Owner: operation.Owner, OwnerEpoch: operation.OwnerEpoch,
		TerminalStatus: status, Result: result, FailureClass: failure, Outcome: outcome,
		PublicationPayload: bytes.Clone(publicationPayload),
		CommittedAt:        committedAt, ExpiresAt: expiresAt,
	}
}

func credentialQuarantine(
	queryer credentialOperationQueryer,
	accountID int,
) (CredentialQuarantine, error) {
	var (
		quarantine                    CredentialQuarantine
		locatorDigest, keychainDigest []byte
		tokenChainDigest              []byte
		createdAt                     int64
	)
	err := queryer.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,locator_digest,
		 observation_keychain_state,observation_keychain_digest,
		 token_chain_digest,reason,failure_class,created_at
		 FROM credential_quarantines WHERE account_id=?`, accountID,
	).Scan(
		&quarantine.AccountID, &quarantine.AccountInstanceID, &quarantine.AccountGeneration,
		&locatorDigest, &quarantine.Observation.Keychain.State, &keychainDigest,
		&tokenChainDigest, &quarantine.Reason,
		&quarantine.FailureClass, &createdAt,
	)
	if err != nil {
		return CredentialQuarantine{}, err
	}
	if err := scanCredentialDigest(locatorDigest, &quarantine.LocatorDigest); err != nil {
		return CredentialQuarantine{}, err
	}
	quarantine.Observation.Keychain.Digest, err = scanOptionalCredentialDigest(keychainDigest)
	if err != nil {
		return CredentialQuarantine{}, err
	}
	quarantine.TokenChainDigest, err = scanOptionalCredentialDigest(tokenChainDigest)
	if err != nil {
		return CredentialQuarantine{}, err
	}
	quarantine.CreatedAt = time.Unix(0, createdAt)
	if quarantine.AccountID <= 0 || quarantine.AccountGeneration == 0 ||
		quarantine.LocatorDigest.zero() ||
		!quarantine.Reason.quarantine() || !quarantine.FailureClass.quarantine() ||
		createdAt <= 0 {
		return CredentialQuarantine{}, errors.New("credential quarantine is corrupt")
	}
	if err := validateAccountInstanceID(quarantine.AccountInstanceID); err != nil {
		return CredentialQuarantine{}, err
	}
	if err := quarantine.Observation.validate(); err != nil {
		return CredentialQuarantine{}, err
	}
	return quarantine, nil
}

func upsertCredentialQuarantine(tx *sql.Tx, quarantine CredentialQuarantine) error {
	if !quarantine.Reason.quarantine() || !quarantine.FailureClass.quarantine() || quarantine.AccountID <= 0 ||
		validateAccountInstanceID(quarantine.AccountInstanceID) != nil ||
		quarantine.AccountGeneration == 0 || quarantine.LocatorDigest.zero() ||
		quarantine.CreatedAt.IsZero() {
		return errors.New("credential quarantine reason is invalid")
	}
	if err := quarantine.Observation.validate(); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO credential_quarantines(
		 account_id,account_instance_id,account_generation,locator_digest,
		 observation_keychain_state,observation_keychain_digest,
		 token_chain_digest,reason,failure_class,created_at
		) VALUES(?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id) DO NOTHING`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.LocatorDigest[:], quarantine.Observation.Keychain.State,
		credentialDigestValue(quarantine.Observation.Keychain.Digest),
		credentialDigestValue(quarantine.TokenChainDigest),
		quarantine.Reason, quarantine.FailureClass,
		quarantine.CreatedAt.UnixNano(),
	); err != nil {
		return err
	}
	stored, err := credentialQuarantine(tx, quarantine.AccountID)
	if err != nil {
		return err
	}
	if !sameCredentialQuarantine(stored, quarantine) {
		return ErrCredentialOperationState
	}
	return nil
}

func validateCredentialOperationRequest(request BeginCredentialOperationRequest) error {
	if request.AccountID <= 0 || request.AccountGeneration == 0 ||
		request.OperationID.zero() || request.LocatorDigest.zero() ||
		request.IntentDigest.zero() ||
		!validCredentialKindTarget(request.Kind, request.Target) {
		return errors.New("credential operation request identity is invalid")
	}
	if err := validateAccountInstanceID(request.AccountInstanceID); err != nil {
		return err
	}
	if err := request.Owner.Validate(); err != nil {
		return errors.New("credential operation owner process is invalid")
	}
	if err := request.Expected.validate(); err != nil {
		return err
	}
	expectedID, err := NewCredentialOperationID(
		request.AccountInstanceID, request.AccountGeneration, request.Kind, request.Target,
		request.LocatorDigest, request.Expected, request.IntentDigest,
	)
	if err != nil {
		return err
	}
	if expectedID != request.OperationID {
		return errors.New("credential operation id does not bind the exact request")
	}
	return nil
}

func validateCredentialOperation(operation CredentialOperation) error {
	if operation.AccountID <= 0 || operation.OperationID.zero() || operation.IntentDigest.zero() ||
		operation.AccountGeneration == 0 || operation.LocatorDigest.zero() ||
		operation.OwnerEpoch == 0 ||
		!validCredentialKindTarget(operation.Kind, operation.Target) || !operation.State.valid() {
		return errors.New("credential operation is corrupt")
	}
	if err := validateSelectionToken(operation.Token); err != nil {
		return err
	}
	if err := validateAccountInstanceID(operation.AccountInstanceID); err != nil {
		return err
	}
	if err := operation.Owner.Validate(); err != nil {
		return err
	}
	if err := operation.Expected.validate(); err != nil {
		return err
	}
	if operation.HasOutcome {
		if operation.State != CredentialOperationApplied {
			return errors.New("credential operation outcome state is corrupt")
		}
		if err := operation.Outcome.validate(); err != nil {
			return err
		}
		if err := validateCredentialTerminal(
			operation.Kind, operation.TerminalStatus, operation.Result, operation.FailureClass,
		); err != nil {
			return err
		}
		if err := validateCredentialPublicationPayload(
			operation.TerminalStatus, operation.Result, operation.PublicationPayload,
		); err != nil {
			return err
		}
	} else if operation.State == CredentialOperationApplied {
		return errors.New("credential operation applied result is missing")
	} else if operation.TerminalStatus != "" || operation.Result != "" ||
		operation.FailureClass != CredentialFailureNone {
		return errors.New("credential operation terminal result is corrupt")
	} else if len(operation.PublicationPayload) != 0 && operation.State != CredentialOperationApplying {
		return errors.New("credential operation publication payload is corrupt")
	} else if len(operation.PublicationPayload) > CredentialPublicationPayloadMaxBytes {
		return errors.New("credential operation publication payload exceeds its limit")
	}
	if operation.CreatedAt.IsZero() || operation.UpdatedAt.Before(operation.CreatedAt) {
		return errors.New("credential operation times are corrupt")
	}
	return nil
}

func validateCredentialReceipt(receipt CredentialOperationReceipt) error {
	if receipt.AccountID <= 0 || receipt.OperationID.zero() || receipt.IntentDigest.zero() ||
		receipt.AccountGeneration == 0 || receipt.LocatorDigest.zero() ||
		receipt.OwnerEpoch == 0 ||
		!validCredentialKindTarget(receipt.Kind, receipt.Target) {
		return errors.New("credential operation receipt is corrupt")
	}
	if err := validateSelectionToken(receipt.Token); err != nil {
		return err
	}
	if err := validateAccountInstanceID(receipt.AccountInstanceID); err != nil {
		return err
	}
	if err := receipt.Owner.Validate(); err != nil {
		return err
	}
	if err := receipt.Expected.validate(); err != nil {
		return err
	}
	if err := receipt.Outcome.validate(); err != nil {
		return err
	}
	if err := validateCredentialTerminal(
		receipt.Kind, receipt.TerminalStatus, receipt.Result, receipt.FailureClass,
	); err != nil {
		return err
	}
	if err := validateCredentialPublicationPayload(
		receipt.TerminalStatus, receipt.Result, receipt.PublicationPayload,
	); err != nil {
		return err
	}
	if receipt.CommittedAt.IsZero() || !receipt.ExpiresAt.After(receipt.CommittedAt) {
		return errors.New("credential operation receipt times are corrupt")
	}
	if !receipt.AcknowledgedAt.IsZero() && receipt.AcknowledgedAt.Before(receipt.CommittedAt) {
		return errors.New("credential operation receipt acknowledgement is corrupt")
	}
	return nil
}

func validateCredentialPublicationPayload(
	status CredentialTerminalStatus,
	result CredentialResultCategory,
	payload []byte,
) error {
	required := status == CredentialTerminalSucceeded && credentialResultPublishesWrite(result)
	if !required {
		if len(payload) != 0 {
			return errors.New("credential publication payload is forbidden for this terminal result")
		}
		return nil
	}
	if len(payload) == 0 {
		return errors.New("credential publication payload is required for this terminal result")
	}
	if len(payload) > CredentialPublicationPayloadMaxBytes {
		return errors.New("credential publication payload exceeds its limit")
	}
	return nil
}

func credentialResultPublishesWrite(result CredentialResultCategory) bool {
	switch result {
	case CredentialResultRefreshed, CredentialResultInstalled, CredentialResultAdopted:
		return true
	default:
		return false
	}
}

func credentialPublicationPayloadValue(payload []byte) any {
	if len(payload) == 0 {
		return nil
	}
	return bytes.Clone(payload)
}

func credentialFailureClassValue(failure CredentialFailureClass) any {
	if failure == CredentialFailureNone {
		return nil
	}
	return failure
}

func validateCredentialTerminal(
	kind CredentialOperationKind,
	status CredentialTerminalStatus,
	result CredentialResultCategory,
	failure CredentialFailureClass,
) error {
	switch status {
	case CredentialTerminalFailed:
		if result != CredentialResultFailed {
			return errors.New("credential operation failed result is invalid")
		}
		if !failure.allowed(kind, status) {
			return errors.New("credential operation failed class is invalid")
		}
		return nil
	case CredentialTerminalQuarantined:
		if !result.quarantine() {
			return errors.New("credential operation quarantine result is invalid")
		}
		if !failure.allowed(kind, status) {
			return errors.New("credential operation quarantine class is invalid")
		}
		return nil
	case CredentialTerminalSucceeded:
		if failure != CredentialFailureNone {
			return errors.New("successful credential operation must not carry a failure class")
		}
	default:
		return errors.New("credential operation terminal status is invalid")
	}
	valid := false
	switch kind {
	case CredentialOperationEnsureFresh, CredentialOperationRefreshCurrent:
		valid = result == CredentialResultUnchanged || result == CredentialResultRefreshed ||
			result == CredentialResultNeedsLogin || result == CredentialResultNoTokens
	case CredentialOperationInstallSynced:
		valid = result == CredentialResultInstalled || result == CredentialResultSkipped
	case CredentialOperationAdoptRotated:
		valid = result == CredentialResultAdopted
	case CredentialOperationCompensate:
		valid = result == CredentialResultDone
	}
	if !valid {
		return errors.New("credential operation result is invalid for its kind")
	}
	return nil
}

func (failure CredentialFailureClass) allowed(
	kind CredentialOperationKind,
	status CredentialTerminalStatus,
) bool {
	switch failure {
	case CredentialFailureInternal:
		return status == CredentialTerminalFailed || status == CredentialTerminalQuarantined
	case CredentialFailureNetwork, CredentialFailureRefreshServer:
		return (kind == CredentialOperationEnsureFresh || kind == CredentialOperationRefreshCurrent) &&
			status == CredentialTerminalQuarantined
	case CredentialFailureRefreshUnauthorized, CredentialFailureRefreshRejected:
		return (kind == CredentialOperationEnsureFresh || kind == CredentialOperationRefreshCurrent) &&
			status == CredentialTerminalFailed
	default:
		return false
	}
}

func (failure CredentialFailureClass) quarantine() bool {
	return failure == CredentialFailureInternal || failure == CredentialFailureNetwork ||
		failure == CredentialFailureRefreshServer
}

func (state CredentialExternalState) validate() error {
	if err := state.Keychain.validate(); err != nil {
		return fmt.Errorf("credential keychain observation: %w", err)
	}
	return nil
}

func (slot CredentialSlotObservation) validate() error {
	switch slot.State {
	case CredentialSlotPresent:
		if slot.Digest == nil || slot.Digest.zero() {
			return errors.New("present slot digest is required")
		}
	case CredentialSlotEmpty, CredentialSlotUnsearchable, CredentialSlotUnreadable:
		if slot.Digest != nil {
			return errors.New("non-present slot must not carry a digest")
		}
	default:
		return errors.New("credential slot state is invalid")
	}
	return nil
}

func (state CredentialOperationState) valid() bool {
	switch state {
	case CredentialOperationPrepared, CredentialOperationApplying, CredentialOperationApplied:
		return true
	default:
		return false
	}
}

func (kind CredentialOperationKind) valid() bool {
	switch kind {
	case CredentialOperationEnsureFresh,
		CredentialOperationRefreshCurrent, CredentialOperationInstallSynced,
		CredentialOperationAdoptRotated, CredentialOperationCompensate:
		return true
	default:
		return false
	}
}

func (target CredentialTarget) valid() bool { return target == CredentialTargetKeychain }

func validCredentialKindTarget(kind CredentialOperationKind, target CredentialTarget) bool {
	if !kind.valid() || !target.valid() {
		return false
	}
	return target == CredentialTargetKeychain
}

func (result CredentialResultCategory) quarantine() bool {
	switch result {
	case CredentialResultAmbiguous, CredentialResultDiverged,
		CredentialResultCleanupFailed, CredentialResultChangedUnderfoot:
		return true
	default:
		return false
	}
}

func validateCredentialFence(fence CredentialOperationFence) error {
	if err := validateSelectionToken(fence.Token); err != nil {
		return err
	}
	if fence.Epoch == 0 {
		return errors.New("credential operation owner epoch is required")
	}
	if err := fence.Owner.Validate(); err != nil {
		return errors.New("credential operation owner process is invalid")
	}
	return nil
}

func requireCredentialFence(
	operation CredentialOperation,
	fence CredentialOperationFence,
) error {
	if operation.Token != fence.Token || operation.OwnerEpoch != fence.Epoch ||
		!sameCredentialOwner(operation.Owner, fence.Owner) {
		return ErrCredentialOperationOwner
	}
	return nil
}

func receiptFenceMatches(
	receipt CredentialOperationReceipt,
	fence CredentialOperationFence,
) bool {
	return receipt.Token == fence.Token && receipt.OwnerEpoch == fence.Epoch &&
		sameCredentialOwner(receipt.Owner, fence.Owner)
}

func operationIdentityMatchesRequest(
	operation CredentialOperation,
	request BeginCredentialOperationRequest,
) bool {
	return operation.AccountInstanceID == request.AccountInstanceID &&
		operation.AccountGeneration == request.AccountGeneration &&
		operation.LocatorDigest == request.LocatorDigest
}

func credentialOperationMatchesRequest(
	operation CredentialOperation,
	request BeginCredentialOperationRequest,
) bool {
	return operation.OperationID == request.OperationID &&
		operationIdentityMatchesRequest(operation, request) &&
		operation.Kind == request.Kind && operation.Target == request.Target &&
		operation.IntentDigest == request.IntentDigest &&
		sameCredentialExternalState(operation.Expected, request.Expected)
}

func credentialReceiptMatchesRequest(
	receipt CredentialOperationReceipt,
	request BeginCredentialOperationRequest,
) bool {
	return receipt.OperationID == request.OperationID &&
		receipt.AccountID == request.AccountID &&
		receipt.AccountInstanceID == request.AccountInstanceID &&
		receipt.AccountGeneration == request.AccountGeneration &&
		receipt.LocatorDigest == request.LocatorDigest &&
		receipt.Kind == request.Kind && receipt.Target == request.Target &&
		receipt.IntentDigest == request.IntentDigest &&
		sameCredentialExternalState(receipt.Expected, request.Expected)
}

func credentialAccountMatchesRequest(
	queryer credentialOperationQueryer,
	request BeginCredentialOperationRequest,
) error {
	err := credentialAccountMatchesIdentity(
		queryer, request.AccountID, request.AccountInstanceID,
		request.AccountGeneration, request.LocatorDigest,
	)
	if err == nil || !errors.Is(err, sql.ErrNoRows) ||
		request.Kind != CredentialOperationCompensate || request.Target != CredentialTargetKeychain {
		return err
	}
	return credentialPendingAddCompensationMatches(
		queryer, request.AccountID, request.AccountInstanceID, request.AccountGeneration,
		request.LocatorDigest, request.Expected, request.IntentDigest,
	)
}

func credentialAccountMatchesOperation(
	queryer credentialOperationQueryer,
	operation CredentialOperation,
) error {
	err := credentialAccountMatchesIdentity(
		queryer, operation.AccountID, operation.AccountInstanceID,
		operation.AccountGeneration, operation.LocatorDigest,
	)
	if err == nil || !errors.Is(err, sql.ErrNoRows) ||
		operation.Kind != CredentialOperationCompensate || operation.Target != CredentialTargetKeychain {
		return err
	}
	return credentialPendingAddCompensationMatches(
		queryer, operation.AccountID, operation.AccountInstanceID, operation.AccountGeneration,
		operation.LocatorDigest, operation.Expected, operation.IntentDigest,
	)
}

func credentialPendingAddCompensationMatches(
	queryer credentialOperationQueryer,
	accountID int,
	instanceID string,
	generation uint64,
	locator CredentialDigest,
	expected CredentialExternalState,
	intent CredentialDigest,
) error {
	var pendingInstance string
	var pendingGeneration uint64
	var pendingOwner []byte
	if err := queryer.QueryRow(
		`SELECT instance_id,generation,owner_record FROM pending_adds WHERE id=?`, accountID,
	).Scan(&pendingInstance, &pendingGeneration, &pendingOwner); err != nil {
		return errors.Join(ErrAccountGenerationChanged, err)
	}
	mutation, err := accountMutationByAccount(queryer, accountID)
	if err != nil {
		return errors.Join(ErrAccountGenerationChanged, err)
	}
	expectedDigest, err := expected.Digest()
	if err != nil {
		return err
	}
	if pendingInstance != instanceID || pendingGeneration != generation ||
		!bytes.Equal(pendingOwner, mustEncodeCredentialOwner(mutation.Owner)) ||
		mutation.Kind != AccountMutationAdd || mutation.State != AccountMutationCompensating ||
		mutation.AccountInstanceID != instanceID || mutation.AccountGeneration != generation ||
		!mutation.CredentialWritten || mutation.WrittenCredentialDigest != expectedDigest ||
		mutation.LocatorDigest != locator ||
		CredentialKeychainLocatorDigest(mutation.KeychainService, mutation.KeychainAccount) != locator ||
		credentialCompensationIntentDigest(mutation.WrittenCredentialDigest) != intent {
		return ErrAccountGenerationChanged
	}
	return nil
}

func credentialCompensationIntentDigest(written CredentialDigest) CredentialDigest {
	hash := sha256.New()
	writeCredentialHashField(hash, []byte("cc-pool:credential-intent:v1"))
	writeCredentialHashField(hash, []byte(CredentialOperationCompensate))
	writeCredentialHashField(hash, written[:])
	var result CredentialDigest
	copy(result[:], hash.Sum(nil))
	return result
}

func credentialAccountMatchesIdentity(
	queryer credentialOperationQueryer,
	accountID int,
	instanceID string,
	generation uint64,
	locator CredentialDigest,
) error {
	var currentInstance, service, account string
	var currentGeneration uint64
	if err := queryer.QueryRow(
		`SELECT instance_id,generation,keychain_service,keychain_account
		 FROM accounts WHERE id=? AND deleted_at IS NULL`,
		accountID,
	).Scan(&currentInstance, &currentGeneration, &service, &account); err != nil {
		return errors.Join(ErrAccountGenerationChanged, err)
	}
	if currentInstance != instanceID || currentGeneration != generation ||
		CredentialKeychainLocatorDigest(service, account) != locator {
		return ErrAccountGenerationChanged
	}
	return nil
}

func validateAccountInstanceID(instanceID string) error {
	if len(instanceID) != 32 {
		return errors.New("credential operation account instance id is invalid")
	}
	decoded, err := hex.DecodeString(instanceID)
	if err != nil || len(decoded) != 16 {
		return errors.New("credential operation account instance id is invalid")
	}
	return nil
}

func encodeCredentialOwner(owner proc.Record) ([]byte, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(owner)
	if err != nil {
		return nil, fmt.Errorf("encode credential operation owner: %w", err)
	}
	return encoded, nil
}

func mustEncodeCredentialOwner(owner proc.Record) []byte {
	encoded, err := encodeCredentialOwner(owner)
	if err != nil {
		panic(err)
	}
	return encoded
}

func decodeCredentialOwner(encoded []byte, owner *proc.Record) error {
	if err := json.Unmarshal(encoded, owner); err != nil {
		return errors.Join(errors.New("credential operation owner is corrupt"), err)
	}
	if err := owner.Validate(); err != nil {
		return errors.Join(errors.New("credential operation owner is corrupt"), err)
	}
	return nil
}

func sameCredentialOwner(left, right proc.Record) bool {
	leftEncoded, leftErr := encodeCredentialOwner(left)
	rightEncoded, rightErr := encodeCredentialOwner(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftEncoded, rightEncoded)
}

func newCredentialOperationToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate credential operation token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func scanCredentialOperationID(raw []byte, destination *CredentialOperationID) error {
	if len(raw) != len(destination) {
		return errors.New("credential operation id is corrupt")
	}
	copy(destination[:], raw)
	if destination.zero() {
		return errors.New("credential operation id is corrupt")
	}
	return nil
}

func scanCredentialDigest(raw []byte, destination *CredentialDigest) error {
	if len(raw) != len(destination) {
		return errors.New("credential digest is corrupt")
	}
	copy(destination[:], raw)
	if destination.zero() {
		return errors.New("credential digest is corrupt")
	}
	return nil
}

func scanOptionalCredentialDigest(raw []byte) (*CredentialDigest, error) {
	if raw == nil {
		return nil, nil
	}
	var digest CredentialDigest
	if err := scanCredentialDigest(raw, &digest); err != nil {
		return nil, err
	}
	return &digest, nil
}

func credentialDigestValue(digest *CredentialDigest) any {
	if digest == nil {
		return nil
	}
	return digest[:]
}

func (digest CredentialDigest) zero() bool {
	return digest == CredentialDigest{}
}

func (operationID CredentialOperationID) zero() bool {
	return operationID == CredentialOperationID{}
}

func sameCredentialExternalState(left, right CredentialExternalState) bool {
	return sameCredentialSlot(left.Keychain, right.Keychain)
}

func sameCredentialSlot(left, right CredentialSlotObservation) bool {
	if left.State != right.State || (left.Digest == nil) != (right.Digest == nil) {
		return false
	}
	return left.Digest == nil || *left.Digest == *right.Digest
}

func sameCredentialQuarantine(left, right CredentialQuarantine) bool {
	return sameCredentialQuarantineIdentity(left, right) &&
		sameOptionalCredentialDigest(left.TokenChainDigest, right.TokenChainDigest)
}

func sameCredentialQuarantineIdentity(left, right CredentialQuarantine) bool {
	return left.AccountID == right.AccountID &&
		left.AccountInstanceID == right.AccountInstanceID &&
		left.AccountGeneration == right.AccountGeneration &&
		left.LocatorDigest == right.LocatorDigest &&
		sameCredentialExternalState(left.Observation, right.Observation) &&
		left.Reason == right.Reason && left.FailureClass == right.FailureClass &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func sameOptionalCredentialDigest(left, right *CredentialDigest) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return left == nil || *left == *right
}

func validateCredentialQuarantineValue(quarantine CredentialQuarantine) error {
	if quarantine.AccountID <= 0 || quarantine.AccountGeneration == 0 ||
		quarantine.LocatorDigest.zero() ||
		!quarantine.Reason.quarantine() || !quarantine.FailureClass.quarantine() ||
		quarantine.CreatedAt.IsZero() {
		return errors.New("credential quarantine is invalid")
	}
	if err := validateAccountInstanceID(quarantine.AccountInstanceID); err != nil {
		return err
	}
	if err := quarantine.Observation.validate(); err != nil {
		return err
	}
	if quarantine.TokenChainDigest != nil && quarantine.TokenChainDigest.zero() {
		return errors.New("credential quarantine token chain digest is invalid")
	}
	return nil
}

func writeCredentialHashField(hash interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(value)
}
