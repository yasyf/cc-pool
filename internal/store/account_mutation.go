package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// AccountMutationID is the stable semantic identity of one registry mutation.
type AccountMutationID [32]byte

// AccountMutationKind is one closed registry mutation policy.
type AccountMutationKind string

const (
	AccountMutationAdd         AccountMutationKind = "add"
	AccountMutationRelogin     AccountMutationKind = "relogin"
	AccountMutationSyncInstall AccountMutationKind = "sync-install"
)

// AccountMutationState is one daemon-owned durable mutation phase.
type AccountMutationState string

const (
	AccountMutationAwaitingInput AccountMutationState = "awaiting-input"
	AccountMutationReserved      AccountMutationState = "reserved"
	AccountMutationApplying      AccountMutationState = "applying"
	AccountMutationApplied       AccountMutationState = "applied"
	AccountMutationPublishing    AccountMutationState = "publishing"
	AccountMutationCompensating  AccountMutationState = "compensating"
)

// AccountMutationTerminal is one immutable registry mutation result.
type AccountMutationTerminal string

const (
	AccountMutationCommitted   AccountMutationTerminal = "committed"
	AccountMutationSuperseded  AccountMutationTerminal = "superseded"
	AccountMutationAborted     AccountMutationTerminal = "aborted"
	AccountMutationQuarantined AccountMutationTerminal = "quarantined"
)

var (
	ErrAccountMutationBusy             = errors.New("account mutation lane busy")
	ErrAccountMutationFence            = errors.New("account mutation fence changed")
	ErrAccountMutationState            = errors.New("account mutation state changed")
	ErrAccountMutationRecoveryRequired = errors.New("account mutation recovery required")
	ErrAccountMutationSuperseded       = errors.New("account mutation superseded by registry change")
	ErrAccountRemoving                 = errors.New("account removal already reserved")
)

// AccountMutationFence authorizes one exact daemon owner epoch.
type AccountMutationFence struct {
	OperationID AccountMutationID
	Owner       proc.Record
	OwnerEpoch  uint64
}

// AccountMutation is one daemon-owned registry and credential publication lane.
type AccountMutation struct {
	OperationID              AccountMutationID
	AccountID                int
	Kind                     AccountMutationKind
	State                    AccountMutationState
	RegistrySequence         uint64
	AccountInstanceID        string
	AccountGeneration        uint64
	LocatorDigest            CredentialDigest
	ExpectedCredentialDigest CredentialDigest
	IntentDigest             CredentialDigest
	InputDigest              CredentialDigest
	HasInput                 bool
	WrittenCredentialDigest  CredentialDigest
	CredentialWritten        bool
	ConfigDir                string
	KeychainService          string
	KeychainAccount          string
	Label                    string
	AccountUUID              string
	Owner                    proc.Record
	OwnerEpoch               uint64
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// Fence returns the mutation's exact current authority.
func (mutation AccountMutation) Fence() AccountMutationFence {
	return AccountMutationFence{
		OperationID: mutation.OperationID,
		Owner:       mutation.Owner,
		OwnerEpoch:  mutation.OwnerEpoch,
	}
}

// AccountMutationReceipt is an immutable replayable terminal result.
type AccountMutationReceipt struct {
	OperationID              AccountMutationID
	AccountID                int
	Kind                     AccountMutationKind
	RegistrySequence         uint64
	AccountInstanceID        string
	AccountGeneration        uint64
	LocatorDigest            CredentialDigest
	ExpectedCredentialDigest CredentialDigest
	IntentDigest             CredentialDigest
	InputDigest              CredentialDigest
	HasInput                 bool
	WrittenCredentialDigest  CredentialDigest
	CredentialWritten        bool
	OutcomeDigest            CredentialDigest
	Terminal                 AccountMutationTerminal
	ConfigDir                string
	KeychainService          string
	KeychainAccount          string
	Label                    string
	AccountUUID              string
	Owner                    proc.Record
	OwnerEpoch               uint64
	CommittedAt              time.Time
	AcknowledgedAt           time.Time
	ExpiresAt                time.Time
	QuarantineFileLocator    CredentialDigest
	QuarantineReason         CredentialResultCategory
	HasQuarantine            bool
	Resolution               AccountMutationResolution
	ResolutionObservedDigest CredentialDigest
	ResolvedAt               time.Time
}

// AccountMutationQuarantine records the exact unsafe external observation.
type AccountMutationQuarantine struct {
	FileLocatorDigest CredentialDigest
	Observation       CredentialExternalState
	Reason            CredentialResultCategory
}

// AccountMutationResolution is one explicit audited terminal recovery result.
type AccountMutationResolution string

const (
	AccountMutationCompensatedRelease AccountMutationResolution = "compensated-release"
)

// ResolveQuarantinedAddRequest fences one explicit pending-add recovery.
type ResolveQuarantinedAddRequest struct {
	OperationID AccountMutationID
	Quarantine  CredentialQuarantine
	Observed    CredentialExternalState
}

// BeginAccountMutationRequest describes one exact daemon-owned operation.
type BeginAccountMutationRequest struct {
	OperationID              AccountMutationID
	AccountID                int
	Kind                     AccountMutationKind
	AccountInstanceID        string
	AccountGeneration        uint64
	LocatorDigest            CredentialDigest
	ExpectedCredentialDigest CredentialDigest
	IntentDigest             CredentialDigest
	ConfigDir                string
	KeychainService          string
	KeychainAccount          string
	Label                    string
	AccountUUID              string
	Owner                    proc.Record
}

// BeginAccountMutationResult returns one active lane or immutable receipt.
type BeginAccountMutationResult struct {
	Active  *AccountMutation
	Receipt *AccountMutationReceipt
	Created bool
}

const accountMutationIDDomain = "cc-pool:account-mutation:v1"

// NewAccountMutationID derives the only operation ID accepted by Begin.
func NewAccountMutationID(
	accountID int,
	accountInstanceID string,
	accountGeneration uint64,
	kind AccountMutationKind,
	locator CredentialDigest,
	expected CredentialDigest,
	intent CredentialDigest,
) (AccountMutationID, error) {
	if accountID <= 0 || validateAccountInstanceID(accountInstanceID) != nil ||
		accountGeneration == 0 || !kind.valid() || locator.zero() || expected.zero() || intent.zero() {
		return AccountMutationID{}, ErrAccountMutationState
	}
	hash := sha256.New()
	writeCredentialHashField(hash, []byte(accountMutationIDDomain))
	var account [8]byte
	binary.BigEndian.PutUint64(account[:], uint64(accountID))
	writeCredentialHashField(hash, account[:])
	writeCredentialHashField(hash, []byte(accountInstanceID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], accountGeneration)
	writeCredentialHashField(hash, generation[:])
	writeCredentialHashField(hash, []byte(kind))
	writeCredentialHashField(hash, locator[:])
	writeCredentialHashField(hash, expected[:])
	writeCredentialHashField(hash, intent[:])
	var operationID AccountMutationID
	copy(operationID[:], hash.Sum(nil))
	return operationID, nil
}

// BeginAccountMutation reserves one registry-serialized lane before external I/O.
func (s *Store) BeginAccountMutation(
	ctx context.Context,
	request BeginAccountMutationRequest,
) (BeginAccountMutationResult, error) {
	now := s.now()
	if err := validateAccountMutationRequest(request); err != nil {
		return BeginAccountMutationResult{}, err
	}
	owner, err := json.Marshal(request.Owner)
	if err != nil {
		return BeginAccountMutationResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BeginAccountMutationResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO account_registry_sequences(account_id,sequence) VALUES(?,0)
		 ON CONFLICT(account_id) DO NOTHING`, request.AccountID,
	); err != nil {
		return BeginAccountMutationResult{}, err
	}
	if receipt, err := accountMutationReceiptByID(tx, request.OperationID); err == nil {
		if !sameAccountMutationReceiptIntent(receipt, request) {
			return BeginAccountMutationResult{}, ErrAccountMutationRecoveryRequired
		}
		if err := tx.Commit(); err != nil {
			return BeginAccountMutationResult{}, err
		}
		return BeginAccountMutationResult{Receipt: &receipt}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginAccountMutationResult{}, err
	}
	if _, err := credentialQuarantine(tx, request.AccountID); err == nil {
		return BeginAccountMutationResult{}, ErrAccountMutationRecoveryRequired
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginAccountMutationResult{}, err
	}
	if current, err := accountMutationByAccount(tx, request.AccountID); err == nil {
		if current.OperationID == request.OperationID && sameAccountMutationIntent(current, request) {
			if err := tx.Commit(); err != nil {
				return BeginAccountMutationResult{}, err
			}
			return BeginAccountMutationResult{Active: &current}, nil
		}
		return BeginAccountMutationResult{Active: &current}, ErrAccountMutationBusy
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginAccountMutationResult{}, err
	}
	if _, err := accountRemovalByID(tx, request.AccountID); err == nil {
		return BeginAccountMutationResult{}, ErrAccountRemoving
	} else if !errors.Is(err, sql.ErrNoRows) {
		return BeginAccountMutationResult{}, err
	}
	if err := validateAccountMutationSubject(tx, request); err != nil {
		return BeginAccountMutationResult{}, err
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx,
		`UPDATE account_registry_sequences SET sequence=sequence+1
		 WHERE account_id=? RETURNING sequence`, request.AccountID,
	).Scan(&sequence); err != nil {
		return BeginAccountMutationResult{}, err
	}
	state := AccountMutationReserved
	if request.Kind == AccountMutationAdd || request.Kind == AccountMutationRelogin {
		state = AccountMutationAwaitingInput
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO account_mutations(
		 operation_id,account_id,kind,state,registry_sequence,
		 account_instance_id,account_generation,locator_digest,
		 expected_credential_digest,intent_digest,config_dir,keychain_service,keychain_account,label,account_uuid,
		 owner_record,owner_epoch,created_at,updated_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)`,
		request.OperationID[:], request.AccountID, request.Kind, state, sequence,
		request.AccountInstanceID, request.AccountGeneration, request.LocatorDigest[:],
		request.ExpectedCredentialDigest[:], request.IntentDigest[:], request.ConfigDir,
		request.KeychainService, request.KeychainAccount, request.Label, request.AccountUUID,
		owner, now.UnixNano(), now.UnixNano(),
	); err != nil {
		if request.Kind == AccountMutationAdd {
			current, currentErr := accountMutationByKind(tx, AccountMutationAdd)
			if currentErr == nil {
				if _, releaseErr := tx.Exec(
					`DELETE FROM pending_adds WHERE id=? AND instance_id=? AND generation=?`,
					request.AccountID, request.AccountInstanceID, request.AccountGeneration,
				); releaseErr != nil {
					return BeginAccountMutationResult{}, errors.Join(err, releaseErr)
				}
				if commitErr := tx.Commit(); commitErr != nil {
					return BeginAccountMutationResult{}, errors.Join(err, commitErr)
				}
				return BeginAccountMutationResult{Active: &current}, ErrAccountMutationBusy
			}
		}
		return BeginAccountMutationResult{}, err
	}
	active, err := accountMutationByID(tx, request.OperationID)
	if err != nil {
		return BeginAccountMutationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return BeginAccountMutationResult{}, err
	}
	return BeginAccountMutationResult{Active: &active, Created: true}, nil
}

// AccountMutation returns one exact active mutation.
func (s *Store) AccountMutation(operationID AccountMutationID) (AccountMutation, error) {
	return accountMutationByID(s.db, operationID)
}

// ActiveAccountMutation returns the account's sole active mutation.
func (s *Store) ActiveAccountMutation(accountID int) (AccountMutation, error) {
	return accountMutationByAccount(s.db, accountID)
}

// ActiveAccountMutationByKind returns the sole globally serialized add lane.
func (s *Store) ActiveAccountMutationByKind(kind AccountMutationKind) (AccountMutation, error) {
	if kind != AccountMutationAdd {
		return AccountMutation{}, ErrAccountMutationState
	}
	return accountMutationByKind(s.db, kind)
}

// AccountMutationsOwnedBy returns one bounded stable account-id page for an exact owner.
func (s *Store) AccountMutationsOwnedBy(
	owner proc.Record,
	afterAccountID, limit int,
) ([]AccountMutation, bool, error) {
	if err := owner.Validate(); err != nil {
		return nil, false, err
	}
	if afterAccountID < 0 || limit <= 0 || limit > CredentialOperationPageLimit {
		return nil, false, ErrAccountMutationState
	}
	ownerRecord, err := encodeCredentialOwner(owner)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.db.Query(
		`SELECT `+accountMutationColumns+` FROM account_mutations
		 WHERE owner_record=? AND account_id>?
		 ORDER BY account_id LIMIT ?`,
		ownerRecord, afterAccountID, limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	mutations := make([]AccountMutation, 0, limit)
	more := false
	for rows.Next() {
		mutation, err := scanAccountMutation(rows)
		if err != nil {
			return nil, false, err
		}
		if len(mutations) == limit {
			more = true
			break
		}
		mutations = append(mutations, mutation)
	}
	return mutations, more, rows.Err()
}

// TakeoverAccountMutation transfers a provably retired lane into a new owner epoch.
func (s *Store) TakeoverAccountMutation(
	ctx context.Context,
	expected AccountMutationFence,
	newOwner proc.Record,
	receipt proc.ReapReceipt,
	verifier ProcessRetirementVerifier,
) (AccountMutation, error) {
	if err := expected.Owner.Validate(); err != nil {
		return AccountMutation{}, err
	}
	if err := newOwner.Validate(); err != nil {
		return AccountMutation{}, err
	}
	if err := verifyProcessRetirement(ctx, expected.Owner, newOwner, receipt, verifier); err != nil {
		return AccountMutation{}, errors.Join(ErrAccountMutationFence, err)
	}
	now := s.now()
	if expected.OperationID == (AccountMutationID{}) || expected.OwnerEpoch == 0 {
		return AccountMutation{}, ErrAccountMutationFence
	}
	oldOwner, err := json.Marshal(expected.Owner)
	if err != nil {
		return AccountMutation{}, err
	}
	encodedNewOwner, err := json.Marshal(newOwner)
	if err != nil {
		return AccountMutation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AccountMutation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	mutation, err := accountMutationByID(tx, expected.OperationID)
	if err != nil {
		return AccountMutation{}, err
	}
	if !sameAccountMutationFence(mutation, expected) {
		return AccountMutation{}, ErrAccountMutationFence
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE account_mutations
		 SET owner_record=?,owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?`,
		encodedNewOwner, now.UnixNano(), expected.OperationID[:],
		oldOwner, expected.OwnerEpoch,
	)
	if err != nil {
		return AccountMutation{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutation{}, ErrAccountMutationFence
	}
	if mutation.Kind == AccountMutationAdd {
		result, err := tx.ExecContext(ctx,
			`UPDATE pending_adds SET owner_record=?
			 WHERE id=? AND instance_id=? AND generation=? AND owner_record=?`,
			encodedNewOwner, mutation.AccountID, mutation.AccountInstanceID,
			mutation.AccountGeneration, oldOwner,
		)
		if err != nil {
			return AccountMutation{}, err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return AccountMutation{}, ErrAccountMutationFence
		}
	}
	recovered, err := accountMutationByID(tx, expected.OperationID)
	if err != nil {
		return AccountMutation{}, err
	}
	if err := tx.Commit(); err != nil {
		return AccountMutation{}, err
	}
	return recovered, nil
}

// MarkAccountMutationInputProvided records only the digest of ephemeral auth input.
func (s *Store) MarkAccountMutationInputProvided(
	fence AccountMutationFence,
	input CredentialDigest,
) (AccountMutationFence, error) {
	if input.zero() {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	return s.advanceAccountMutation(
		fence, AccountMutationAwaitingInput, AccountMutationReserved, input, true, CredentialDigest{}, false,
	)
}

// MarkAccountMutationApplying crosses the first external-I/O boundary.
func (s *Store) MarkAccountMutationApplying(fence AccountMutationFence) (AccountMutationFence, error) {
	return s.advanceAccountMutation(
		fence, AccountMutationReserved, AccountMutationApplying,
		CredentialDigest{}, false, CredentialDigest{}, false,
	)
}

// RearmAccountMutationInput returns an unchanged interactive mutation to input collection.
func (s *Store) RearmAccountMutationInput(
	fence AccountMutationFence,
	observed CredentialDigest,
) (AccountMutationFence, error) {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if observed.zero() {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	if err := fence.Owner.Validate(); err != nil {
		return AccountMutationFence{}, err
	}
	now := s.now()
	tx, err := s.db.Begin()
	if err != nil {
		return AccountMutationFence{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`UPDATE account_mutations SET updated_at=updated_at WHERE operation_id=?`,
		fence.OperationID[:],
	); err != nil {
		return AccountMutationFence{}, err
	}
	mutation, err := accountMutationByID(tx, fence.OperationID)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if sameCredentialOwner(mutation.Owner, fence.Owner) &&
		mutation.OwnerEpoch == fence.OwnerEpoch+1 &&
		mutation.State == AccountMutationAwaitingInput &&
		mutation.ExpectedCredentialDigest == observed {
		if err := tx.Commit(); err != nil {
			return AccountMutationFence{}, err
		}
		return mutation.Fence(), nil
	}
	if !sameAccountMutationFence(mutation, fence) {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if (mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationRelogin) ||
		mutation.State != AccountMutationApplying {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	if mutation.ExpectedCredentialDigest != observed {
		return AccountMutationFence{}, ErrAccountMutationRecoveryRequired
	}
	result, err := tx.Exec(
		`UPDATE account_mutations SET state='awaiting-input',owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND state='applying'`,
		now.UnixNano(), fence.OperationID[:], mustEncodeCredentialOwner(fence.Owner),
		fence.OwnerEpoch,
	)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if err := tx.Commit(); err != nil {
		return AccountMutationFence{}, err
	}
	fence.OwnerEpoch++
	return fence, nil
}

// MarkAccountMutationApplied records the exact external fingerprint written.
func (s *Store) MarkAccountMutationApplied(
	fence AccountMutationFence,
	written CredentialDigest,
) (AccountMutationFence, error) {
	if written.zero() {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	return s.advanceAccountMutation(
		fence, AccountMutationApplying, AccountMutationApplied,
		CredentialDigest{}, false, written, true,
	)
}

// SetAccountMutationMetadata records verified post-login account metadata before publication.
func (s *Store) SetAccountMutationMetadata(
	fence AccountMutationFence,
	label string,
	accountUUID string,
) (AccountMutationFence, error) {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	mutation, err := s.AccountMutation(fence.OperationID)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if !sameAccountMutationFence(mutation, fence) {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationRelogin {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	owner, err := json.Marshal(fence.Owner)
	if err != nil {
		return AccountMutationFence{}, err
	}
	now := s.now()
	result, err := s.db.Exec(
		`UPDATE account_mutations SET label=?,account_uuid=?,owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND state IN ('applying','applied')`,
		label, accountUUID, now.UnixNano(), fence.OperationID[:], owner, fence.OwnerEpoch,
	)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	fence.OwnerEpoch++
	return fence, nil
}

// MarkAccountMutationPublishing enters the registry CAS phase.
func (s *Store) MarkAccountMutationPublishing(fence AccountMutationFence) (AccountMutationFence, error) {
	return s.advanceAccountMutation(
		fence, AccountMutationApplied, AccountMutationPublishing,
		CredentialDigest{}, false, CredentialDigest{}, false,
	)
}

func (s *Store) advanceAccountMutation(
	fence AccountMutationFence,
	from, to AccountMutationState,
	input CredentialDigest,
	hasInput bool,
	written CredentialDigest,
	credentialWritten bool,
) (AccountMutationFence, error) {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	owner, err := json.Marshal(fence.Owner)
	if err != nil {
		return AccountMutationFence{}, err
	}
	now := s.now()
	result, err := s.db.Exec(
		`UPDATE account_mutations SET state=?,owner_epoch=owner_epoch+1,updated_at=?,
		 input_digest=CASE WHEN ? THEN ? ELSE input_digest END,
		 written_credential_digest=CASE WHEN ? THEN ? ELSE written_credential_digest END,
		 credential_written=CASE WHEN ? THEN 1 ELSE credential_written END
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=? AND state=?`,
		to, now.UnixNano(), hasInput, input[:], credentialWritten, written[:], credentialWritten,
		fence.OperationID[:], owner, fence.OwnerEpoch, from,
	)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	fence.OwnerEpoch++
	return fence, nil
}

// CommitAccountMutation atomically publishes the registry row under sequence/removal CAS.
func (s *Store) CommitAccountMutation(
	fence AccountMutationFence,
	receiptExpiresAt time.Time,
) (AccountMutationReceipt, error) {
	now := s.now()
	if !receiptExpiresAt.After(now) {
		return AccountMutationReceipt{}, ErrAccountMutationState
	}
	tx, err := s.db.Begin()
	if err != nil {
		return AccountMutationReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	mutation, err := accountMutationByID(tx, fence.OperationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			receipt, receiptErr := accountMutationReceiptByID(tx, fence.OperationID)
			if receiptErr != nil {
				return AccountMutationReceipt{}, receiptErr
			}
			if !accountMutationReceiptFenceMatches(receipt, fence) ||
				receipt.Terminal != AccountMutationCommitted {
				return receipt, ErrAccountMutationState
			}
			return receipt, nil
		}
		return AccountMutationReceipt{}, err
	}
	if !sameAccountMutationFence(mutation, fence) || mutation.State != AccountMutationPublishing {
		return AccountMutationReceipt{}, ErrAccountMutationFence
	}
	var sequence uint64
	if err := tx.QueryRow(
		`SELECT sequence FROM account_registry_sequences WHERE account_id=?`, mutation.AccountID,
	).Scan(&sequence); err != nil {
		return AccountMutationReceipt{}, err
	}
	_, removalErr := accountRemovalByID(tx, mutation.AccountID)
	removing := removalErr == nil
	if removalErr != nil && !errors.Is(removalErr, sql.ErrNoRows) {
		return AccountMutationReceipt{}, removalErr
	}
	if sequence != mutation.RegistrySequence || removing {
		if err := transitionAccountMutationCompensating(tx, mutation, now); err != nil {
			return AccountMutationReceipt{}, err
		}
		if err := tx.Commit(); err != nil {
			return AccountMutationReceipt{}, err
		}
		return AccountMutationReceipt{}, ErrAccountMutationSuperseded
	}
	if mutation.Kind == AccountMutationAdd {
		if err := consumeReservation(tx, PendingAccountReservation{
			ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
			Generation: mutation.AccountGeneration, Owner: mutation.Owner,
		}); err != nil {
			return AccountMutationReceipt{}, err
		}
		if _, err := tx.Exec(
			`INSERT INTO accounts(
			 id,instance_id,generation,config_dir,keychain_service,keychain_account,label,account_uuid,created_at
			 ) VALUES(?,?,?,?,?,?,?,?,?)`,
			mutation.AccountID, mutation.AccountInstanceID, mutation.AccountGeneration,
			mutation.ConfigDir, mutation.KeychainService, mutation.KeychainAccount,
			mutation.Label, mutation.AccountUUID, now.Unix(),
		); err != nil {
			return AccountMutationReceipt{}, err
		}
	} else {
		if err := accountMutationSubjectMatches(tx, mutation); err != nil {
			return AccountMutationReceipt{}, err
		}
		if mutation.Kind == AccountMutationRelogin {
			result, err := tx.Exec(
				`UPDATE accounts SET label=?,account_uuid=?
				 WHERE id=? AND instance_id=? AND generation=? AND deleted_at IS NULL`,
				mutation.Label, mutation.AccountUUID, mutation.AccountID,
				mutation.AccountInstanceID, mutation.AccountGeneration,
			)
			if err != nil {
				return AccountMutationReceipt{}, err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return AccountMutationReceipt{}, ErrAccountGenerationChanged
			}
		}
	}
	if err := insertAccountMutationReceipt(
		tx, mutation, AccountMutationCommitted, mutation.WrittenCredentialDigest, nil, now, receiptExpiresAt,
	); err != nil {
		return AccountMutationReceipt{}, err
	}
	if _, err := tx.Exec(
		`DELETE FROM account_mutations WHERE operation_id=?`, mutation.OperationID[:],
	); err != nil {
		return AccountMutationReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return AccountMutationReceipt{}, err
	}
	return s.AccountMutationReceipt(fence.OperationID)
}

func transitionAccountMutationCompensating(
	tx *sql.Tx,
	mutation AccountMutation,
	now time.Time,
) error {
	result, err := tx.Exec(
		`UPDATE account_mutations SET state='compensating',owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=? AND state='publishing'`,
		now.UnixNano(), mutation.OperationID[:], mustEncodeCredentialOwner(mutation.Owner), mutation.OwnerEpoch,
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrAccountMutationFence
	}
	return nil
}

// ResolveAccountMutation seals an abort, compensation, or ambiguity result.
func (s *Store) ResolveAccountMutation(
	fence AccountMutationFence,
	terminal AccountMutationTerminal,
	outcome CredentialDigest,
	quarantine *AccountMutationQuarantine,
	receiptExpiresAt time.Time,
) (AccountMutationReceipt, error) {
	now := s.now()
	if !terminal.valid() || terminal == AccountMutationCommitted || outcome.zero() ||
		!receiptExpiresAt.After(now) {
		return AccountMutationReceipt{}, ErrAccountMutationState
	}
	if (terminal == AccountMutationQuarantined) != (quarantine != nil) {
		return AccountMutationReceipt{}, ErrAccountMutationState
	}
	tx, err := s.db.Begin()
	if err != nil {
		return AccountMutationReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	mutation, err := accountMutationByID(tx, fence.OperationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			receipt, receiptErr := accountMutationReceiptByID(tx, fence.OperationID)
			if receiptErr != nil {
				return AccountMutationReceipt{}, receiptErr
			}
			if !accountMutationReceiptFenceMatches(receipt, fence) ||
				receipt.Terminal != terminal || receipt.OutcomeDigest != outcome {
				return receipt, ErrAccountMutationState
			}
			return receipt, nil
		}
		return AccountMutationReceipt{}, err
	}
	if !sameAccountMutationFence(mutation, fence) {
		return AccountMutationReceipt{}, ErrAccountMutationFence
	}
	switch terminal {
	case AccountMutationAborted:
		if mutation.State != AccountMutationAwaitingInput && mutation.State != AccountMutationReserved {
			return AccountMutationReceipt{}, ErrAccountMutationState
		}
		if outcome != mutation.ExpectedCredentialDigest {
			return AccountMutationReceipt{}, ErrAccountMutationState
		}
	case AccountMutationSuperseded:
		if mutation.State != AccountMutationCompensating {
			return AccountMutationReceipt{}, ErrAccountMutationState
		}
		if outcome != mutation.ExpectedCredentialDigest {
			return AccountMutationReceipt{}, ErrAccountMutationState
		}
	case AccountMutationQuarantined:
		if mutation.State == AccountMutationAwaitingInput || mutation.State == AccountMutationReserved {
			return AccountMutationReceipt{}, ErrAccountMutationState
		}
		if err := validateAccountMutationQuarantine(mutation, outcome, *quarantine); err != nil {
			return AccountMutationReceipt{}, err
		}
		if err := upsertCredentialQuarantine(tx, CredentialQuarantine{
			AccountID: mutation.AccountID, AccountInstanceID: mutation.AccountInstanceID,
			AccountGeneration: mutation.AccountGeneration, LocatorDigest: mutation.LocatorDigest,
			FileLocatorDigest: quarantine.FileLocatorDigest,
			Observation:       quarantine.Observation, Reason: quarantine.Reason,
			FailureClass: CredentialFailureInternal, CreatedAt: now,
		}); err != nil {
			return AccountMutationReceipt{}, err
		}
	}
	if mutation.Kind == AccountMutationAdd &&
		(terminal == AccountMutationAborted || terminal == AccountMutationSuperseded) {
		if err := consumeReservation(tx, PendingAccountReservation{
			ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
			Generation: mutation.AccountGeneration, Owner: mutation.Owner,
		}); err != nil {
			return AccountMutationReceipt{}, err
		}
		if terminal == AccountMutationSuperseded {
			result, err := tx.Exec(
				`DELETE FROM account_removals
				 WHERE account_id=? AND account_instance_id=? AND account_generation=?
				 AND registry_sequence>?`,
				mutation.AccountID, mutation.AccountInstanceID,
				mutation.AccountGeneration, mutation.RegistrySequence,
			)
			if err != nil {
				return AccountMutationReceipt{}, err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return AccountMutationReceipt{}, ErrAccountMutationState
			}
		}
	}
	if err := insertAccountMutationReceipt(tx, mutation, terminal, outcome, quarantine, now, receiptExpiresAt); err != nil {
		return AccountMutationReceipt{}, err
	}
	if _, err := tx.Exec(
		`DELETE FROM account_mutations WHERE operation_id=?`, mutation.OperationID[:],
	); err != nil {
		return AccountMutationReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return AccountMutationReceipt{}, err
	}
	return s.AccountMutationReceipt(fence.OperationID)
}

func validateAccountMutationQuarantine(
	mutation AccountMutation,
	outcome CredentialDigest,
	quarantine AccountMutationQuarantine,
) error {
	if quarantine.FileLocatorDigest.zero() || !quarantine.Reason.quarantine() {
		return ErrAccountMutationState
	}
	if err := quarantine.Observation.validate(); err != nil {
		return err
	}
	observedDigest, err := quarantine.Observation.Digest()
	if err != nil {
		return err
	}
	if observedDigest != outcome || credentialCompositeLocatorDigest(
		mutation.KeychainService, mutation.KeychainAccount, quarantine.FileLocatorDigest,
	) != mutation.LocatorDigest {
		return ErrAccountMutationState
	}
	return nil
}

// ResolveQuarantinedAdd atomically retires one exactly compensated pending add.
func (s *Store) ResolveQuarantinedAdd(
	request ResolveQuarantinedAddRequest,
) error {
	if request.OperationID == (AccountMutationID{}) {
		return ErrAccountMutationState
	}
	if err := request.Observed.validate(); err != nil {
		return err
	}
	if err := request.Quarantine.Observation.validate(); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := accountMutationReceiptByID(tx, request.OperationID)
	if err != nil {
		return err
	}
	if receipt.Kind != AccountMutationAdd || receipt.Terminal != AccountMutationQuarantined ||
		receipt.AcknowledgedAt.IsZero() || receipt.AccountID != request.Quarantine.AccountID ||
		receipt.AccountInstanceID != request.Quarantine.AccountInstanceID ||
		receipt.AccountGeneration != request.Quarantine.AccountGeneration ||
		receipt.LocatorDigest != request.Quarantine.LocatorDigest || !receipt.HasQuarantine ||
		receipt.QuarantineFileLocator != request.Quarantine.FileLocatorDigest ||
		receipt.QuarantineReason != request.Quarantine.Reason {
		return ErrAccountMutationState
	}
	quarantinedDigest, err := request.Quarantine.Observation.Digest()
	if err != nil {
		return err
	}
	if quarantinedDigest != receipt.OutcomeDigest {
		return ErrAccountMutationState
	}
	observedDigest, err := request.Observed.Digest()
	if err != nil {
		return err
	}
	if observedDigest != receipt.ExpectedCredentialDigest {
		return ErrAccountMutationRecoveryRequired
	}
	if !receipt.ResolvedAt.IsZero() {
		if receipt.Resolution != AccountMutationCompensatedRelease ||
			receipt.ResolutionObservedDigest != observedDigest {
			return ErrAccountMutationState
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	storedQuarantine, err := credentialQuarantine(tx, receipt.AccountID)
	if err != nil {
		return err
	}
	if !sameCredentialQuarantine(storedQuarantine, request.Quarantine) {
		return ErrAccountMutationState
	}
	var activeCredentialEvidence int
	if err := tx.QueryRow(
		`SELECT EXISTS(
		   SELECT 1 FROM credential_operations
		   WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 ) OR EXISTS(
		   SELECT 1 FROM credential_operation_receipts
		   WHERE account_id=? AND account_instance_id=? AND account_generation=?
		   AND acknowledged_at IS NULL
		 )`,
		receipt.AccountID, receipt.AccountInstanceID, receipt.AccountGeneration,
		receipt.AccountID, receipt.AccountInstanceID, receipt.AccountGeneration,
	).Scan(&activeCredentialEvidence); err != nil {
		return err
	}
	if activeCredentialEvidence != 0 {
		return ErrCredentialOperationEvidenceActive
	}
	if err := consumeReservation(tx, PendingAccountReservation{
		ID: receipt.AccountID, InstanceID: receipt.AccountInstanceID,
		Generation: receipt.AccountGeneration, Owner: receipt.Owner,
	}); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM account_removals
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		receipt.AccountID, receipt.AccountInstanceID, receipt.AccountGeneration,
	); err != nil {
		return err
	}
	if err := deleteCredentialQuarantine(tx, request.Quarantine); err != nil {
		return err
	}
	result, err := tx.Exec(
		`UPDATE account_mutation_receipts
		 SET resolution='compensated-release',resolution_observed_digest=?,resolved_at=?
		 WHERE operation_id=? AND acknowledged_at IS NOT NULL AND resolution IS NULL`,
		observedDigest[:], s.now().UnixNano(), request.OperationID[:],
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrAccountMutationState
	}
	return tx.Commit()
}

// AccountMutationReceipt returns one immutable result by operation ID.
func (s *Store) AccountMutationReceipt(operationID AccountMutationID) (AccountMutationReceipt, error) {
	return accountMutationReceiptByID(s.db, operationID)
}

// UnacknowledgedAccountMutationReceipt returns the latest terminal scope result.
func (s *Store) UnacknowledgedAccountMutationReceipt(
	kind AccountMutationKind,
	accountID int,
) (AccountMutationReceipt, error) {
	if !kind.valid() || accountID < 0 || (accountID == 0 && kind != AccountMutationAdd) {
		return AccountMutationReceipt{}, ErrAccountMutationState
	}
	if accountID == 0 {
		return scanAccountMutationReceipt(s.db.QueryRow(
			`SELECT ` + accountMutationReceiptColumns + ` FROM account_mutation_receipts
			 WHERE kind='add' AND acknowledged_at IS NULL
			 ORDER BY committed_at DESC,operation_id DESC LIMIT 1`,
		))
	}
	return scanAccountMutationReceipt(s.db.QueryRow(
		`SELECT `+accountMutationReceiptColumns+` FROM account_mutation_receipts
		 WHERE kind=? AND account_id=? AND acknowledged_at IS NULL
		 ORDER BY committed_at DESC,operation_id DESC LIMIT 1`, kind, accountID,
	))
}

// AcknowledgeAccountMutationReceipt records delivery without deleting replay evidence.
func (s *Store) AcknowledgeAccountMutationReceipt(operationID AccountMutationID) error {
	if operationID == (AccountMutationID{}) {
		return ErrAccountMutationState
	}
	result, err := s.db.Exec(
		`UPDATE account_mutation_receipts
		 SET acknowledged_at=COALESCE(acknowledged_at,?) WHERE operation_id=?`,
		s.now().UnixNano(), operationID[:],
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return sql.ErrNoRows
	}
	return nil
}

// DeleteExpiredAccountMutationReceipts removes one bounded acknowledged page.
func (s *Store) DeleteExpiredAccountMutationReceipts(limit int) (int, error) {
	if limit <= 0 || limit > CredentialOperationPageLimit {
		return 0, ErrAccountMutationState
	}
	now := s.now()
	result, err := s.db.Exec(
		`DELETE FROM account_mutation_receipts WHERE operation_id IN (
		 SELECT receipt.operation_id FROM account_mutation_receipts AS receipt
		 WHERE receipt.acknowledged_at IS NOT NULL
		 AND COALESCE(receipt.resolved_at,receipt.acknowledged_at)<=?
		 AND receipt.expires_at<=?
		 AND NOT EXISTS (
		   SELECT 1 FROM credential_quarantines AS quarantine
		   WHERE quarantine.account_id=receipt.account_id
		   AND quarantine.account_instance_id=receipt.account_instance_id
		   AND quarantine.account_generation=receipt.account_generation
		 )
		 ORDER BY expires_at,operation_id LIMIT ?
		)`,
		now.Add(-credentialReceiptPostAckRetention).UnixNano(), now.UnixNano(), limit,
	)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	return int(rows), err
}

func validateAccountMutationSubject(tx *sql.Tx, request BeginAccountMutationRequest) error {
	if request.Kind == AccountMutationAdd {
		var instanceID string
		var generation uint64
		var owner []byte
		err := tx.QueryRow(
			`SELECT instance_id,generation,owner_record FROM pending_adds WHERE id=?`, request.AccountID,
		).Scan(&instanceID, &generation, &owner)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAccountGenerationChanged
		}
		if err != nil {
			return err
		}
		if instanceID != request.AccountInstanceID || generation != request.AccountGeneration ||
			!bytes.Equal(owner, mustEncodeCredentialOwner(request.Owner)) {
			return ErrAccountGenerationChanged
		}
		return nil
	}
	var instanceID, configDir, service, account string
	var generation uint64
	err := tx.QueryRow(
		`SELECT instance_id,generation,config_dir,keychain_service,keychain_account
		 FROM accounts WHERE id=? AND deleted_at IS NULL`, request.AccountID,
	).Scan(&instanceID, &generation, &configDir, &service, &account)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAccountGenerationChanged
	}
	if err != nil {
		return err
	}
	if instanceID != request.AccountInstanceID || generation != request.AccountGeneration ||
		configDir != request.ConfigDir || service != request.KeychainService || account != request.KeychainAccount {
		return ErrAccountGenerationChanged
	}
	return nil
}

func accountMutationSubjectMatches(tx *sql.Tx, mutation AccountMutation) error {
	return validateAccountMutationSubject(tx, BeginAccountMutationRequest{
		AccountID: mutation.AccountID, Kind: mutation.Kind,
		AccountInstanceID: mutation.AccountInstanceID, AccountGeneration: mutation.AccountGeneration,
		ConfigDir: mutation.ConfigDir, KeychainService: mutation.KeychainService,
		KeychainAccount: mutation.KeychainAccount,
	})
}

type accountMutationQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func accountRemovalByID(
	queryer accountMutationQueryer,
	accountID int,
) (AccountRemoval, error) {
	var removal AccountRemoval
	var deleteCredential int
	var createdAt int64
	err := queryer.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,registry_sequence,
		 delete_credential,created_at FROM account_removals WHERE account_id=?`, accountID,
	).Scan(
		&removal.AccountID, &removal.AccountInstanceID, &removal.AccountGeneration,
		&removal.RegistrySequence, &deleteCredential, &createdAt,
	)
	if err != nil {
		return removal, err
	}
	removal.DeleteCredential = deleteCredential != 0
	removal.CreatedAt = time.Unix(0, createdAt)
	return removal, nil
}

const accountMutationColumns = `operation_id,account_id,kind,state,registry_sequence,
 account_instance_id,account_generation,locator_digest,expected_credential_digest,intent_digest,
 input_digest,written_credential_digest,credential_written,
 config_dir,keychain_service,keychain_account,label,account_uuid,
 owner_record,owner_epoch,created_at,updated_at`

func accountMutationByID(queryer accountMutationQueryer, operationID AccountMutationID) (AccountMutation, error) {
	return scanAccountMutation(queryer.QueryRow(
		`SELECT `+accountMutationColumns+` FROM account_mutations WHERE operation_id=?`, operationID[:],
	))
}

func accountMutationByAccount(queryer accountMutationQueryer, accountID int) (AccountMutation, error) {
	return scanAccountMutation(queryer.QueryRow(
		`SELECT `+accountMutationColumns+` FROM account_mutations WHERE account_id=?`, accountID,
	))
}

func accountMutationByKind(
	queryer accountMutationQueryer,
	kind AccountMutationKind,
) (AccountMutation, error) {
	return scanAccountMutation(queryer.QueryRow(
		`SELECT `+accountMutationColumns+` FROM account_mutations WHERE kind=?`, kind,
	))
}

func scanAccountMutation(row interface{ Scan(...any) error }) (AccountMutation, error) {
	var mutation AccountMutation
	var operationID, locator, expected, intent, input, written, owner []byte
	var credentialWritten int
	var createdAt, updatedAt int64
	if err := row.Scan(
		&operationID, &mutation.AccountID, &mutation.Kind, &mutation.State, &mutation.RegistrySequence,
		&mutation.AccountInstanceID, &mutation.AccountGeneration, &locator, &expected, &intent,
		&input, &written, &credentialWritten,
		&mutation.ConfigDir, &mutation.KeychainService, &mutation.KeychainAccount,
		&mutation.Label, &mutation.AccountUUID, &owner, &mutation.OwnerEpoch,
		&createdAt, &updatedAt,
	); err != nil {
		return mutation, err
	}
	if len(operationID) != 32 || len(locator) != 32 || len(expected) != 32 ||
		len(intent) != 32 || len(written) != 32 {
		return mutation, ErrAccountMutationState
	}
	copy(mutation.OperationID[:], operationID)
	copy(mutation.LocatorDigest[:], locator)
	copy(mutation.ExpectedCredentialDigest[:], expected)
	copy(mutation.IntentDigest[:], intent)
	if input != nil {
		if len(input) != 32 {
			return mutation, ErrAccountMutationState
		}
		copy(mutation.InputDigest[:], input)
		mutation.HasInput = true
	}
	copy(mutation.WrittenCredentialDigest[:], written)
	if err := json.Unmarshal(owner, &mutation.Owner); err != nil {
		return mutation, err
	}
	mutation.CredentialWritten = credentialWritten != 0
	mutation.CreatedAt = time.Unix(0, createdAt)
	mutation.UpdatedAt = time.Unix(0, updatedAt)
	if err := validateAccountMutation(mutation); err != nil {
		return AccountMutation{}, err
	}
	return mutation, nil
}

const accountMutationReceiptColumns = `operation_id,account_id,kind,registry_sequence,
 account_instance_id,account_generation,locator_digest,expected_credential_digest,intent_digest,
 input_digest,written_credential_digest,credential_written,outcome_digest,terminal,
 quarantine_file_locator_digest,quarantine_reason,resolution,resolution_observed_digest,resolved_at,
 config_dir,keychain_service,keychain_account,label,account_uuid,owner_record,owner_epoch,
 committed_at,acknowledged_at,expires_at`

func accountMutationReceiptByID(
	queryer accountMutationQueryer,
	operationID AccountMutationID,
) (AccountMutationReceipt, error) {
	return scanAccountMutationReceipt(queryer.QueryRow(
		`SELECT `+accountMutationReceiptColumns+` FROM account_mutation_receipts WHERE operation_id=?`,
		operationID[:],
	))
}

func scanAccountMutationReceipt(row interface{ Scan(...any) error }) (AccountMutationReceipt, error) {
	var receipt AccountMutationReceipt
	var operationID, locator, expected, intent, input, written, outcome, owner []byte
	var quarantineFileLocator, resolutionObserved []byte
	var credentialWritten int
	var committedAt, expiresAt int64
	var acknowledgedAt, resolvedAt sql.NullInt64
	var quarantineReason, resolution sql.NullString
	if err := row.Scan(
		&operationID, &receipt.AccountID, &receipt.Kind, &receipt.RegistrySequence,
		&receipt.AccountInstanceID, &receipt.AccountGeneration, &locator, &expected, &intent,
		&input, &written, &credentialWritten, &outcome, &receipt.Terminal,
		&quarantineFileLocator, &quarantineReason, &resolution, &resolutionObserved, &resolvedAt,
		&receipt.ConfigDir,
		&receipt.KeychainService, &receipt.KeychainAccount, &receipt.Label, &receipt.AccountUUID,
		&owner, &receipt.OwnerEpoch, &committedAt, &acknowledgedAt, &expiresAt,
	); err != nil {
		return receipt, err
	}
	if len(operationID) != 32 || len(locator) != 32 || len(expected) != 32 ||
		len(intent) != 32 || len(outcome) != 32 {
		return receipt, ErrAccountMutationState
	}
	copy(receipt.OperationID[:], operationID)
	copy(receipt.LocatorDigest[:], locator)
	copy(receipt.ExpectedCredentialDigest[:], expected)
	copy(receipt.IntentDigest[:], intent)
	if input != nil {
		if len(input) != 32 {
			return receipt, ErrAccountMutationState
		}
		copy(receipt.InputDigest[:], input)
		receipt.HasInput = true
	}
	if written != nil {
		if len(written) != 32 {
			return receipt, ErrAccountMutationState
		}
		copy(receipt.WrittenCredentialDigest[:], written)
	}
	copy(receipt.OutcomeDigest[:], outcome)
	if quarantineFileLocator != nil {
		if len(quarantineFileLocator) != 32 || !quarantineReason.Valid {
			return receipt, ErrAccountMutationState
		}
		copy(receipt.QuarantineFileLocator[:], quarantineFileLocator)
		receipt.QuarantineReason = CredentialResultCategory(quarantineReason.String)
		receipt.HasQuarantine = true
	} else if quarantineReason.Valid {
		return receipt, ErrAccountMutationState
	}
	if resolution.Valid || resolutionObserved != nil || resolvedAt.Valid {
		if !resolution.Valid || len(resolutionObserved) != 32 || !resolvedAt.Valid {
			return receipt, ErrAccountMutationState
		}
		receipt.Resolution = AccountMutationResolution(resolution.String)
		copy(receipt.ResolutionObservedDigest[:], resolutionObserved)
		receipt.ResolvedAt = time.Unix(0, resolvedAt.Int64)
	}
	if err := json.Unmarshal(owner, &receipt.Owner); err != nil {
		return receipt, err
	}
	receipt.CredentialWritten = credentialWritten != 0
	receipt.CommittedAt = time.Unix(0, committedAt)
	if acknowledgedAt.Valid {
		receipt.AcknowledgedAt = time.Unix(0, acknowledgedAt.Int64)
	}
	receipt.ExpiresAt = time.Unix(0, expiresAt)
	if err := validateAccountMutationReceipt(receipt); err != nil {
		return AccountMutationReceipt{}, err
	}
	return receipt, nil
}

func insertAccountMutationReceipt(
	tx *sql.Tx,
	mutation AccountMutation,
	terminal AccountMutationTerminal,
	outcome CredentialDigest,
	quarantine *AccountMutationQuarantine,
	committedAt, expiresAt time.Time,
) error {
	if outcome.zero() {
		return ErrAccountMutationState
	}
	var written any
	if mutation.CredentialWritten {
		written = mutation.WrittenCredentialDigest[:]
	}
	var input any
	if mutation.HasInput {
		input = mutation.InputDigest[:]
	}
	var quarantineFileLocator, quarantineReason any
	if quarantine != nil {
		quarantineFileLocator = quarantine.FileLocatorDigest[:]
		quarantineReason = quarantine.Reason
	}
	_, err := tx.Exec(
		`INSERT INTO account_mutation_receipts(
		 operation_id,account_id,kind,registry_sequence,
		 account_instance_id,account_generation,locator_digest,expected_credential_digest,intent_digest,
		 input_digest,written_credential_digest,credential_written,outcome_digest,terminal,
		 quarantine_file_locator_digest,quarantine_reason,resolution,resolution_observed_digest,resolved_at,
		 config_dir,keychain_service,keychain_account,label,account_uuid,owner_record,owner_epoch,
		 committed_at,acknowledged_at,expires_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,?,?,?,?,?,?,?,?,NULL,?)`,
		mutation.OperationID[:], mutation.AccountID, mutation.Kind, mutation.RegistrySequence,
		mutation.AccountInstanceID, mutation.AccountGeneration, mutation.LocatorDigest[:],
		mutation.ExpectedCredentialDigest[:], mutation.IntentDigest[:], input, written, mutation.CredentialWritten,
		outcome[:], terminal, quarantineFileLocator, quarantineReason,
		mutation.ConfigDir, mutation.KeychainService, mutation.KeychainAccount,
		mutation.Label, mutation.AccountUUID, mustEncodeCredentialOwner(mutation.Owner), mutation.OwnerEpoch,
		committedAt.UnixNano(), expiresAt.UnixNano(),
	)
	return err
}

func validateAccountMutationRequest(request BeginAccountMutationRequest) error {
	if request.AccountID <= 0 || !request.Kind.valid() || request.ConfigDir == "" ||
		request.KeychainService == "" || request.KeychainAccount == "" ||
		validateAccountInstanceID(request.AccountInstanceID) != nil ||
		request.AccountGeneration == 0 || request.LocatorDigest.zero() ||
		request.ExpectedCredentialDigest.zero() || request.IntentDigest.zero() {
		return ErrAccountMutationState
	}
	if err := request.Owner.Validate(); err != nil {
		return err
	}
	expectedID, err := NewAccountMutationID(
		request.AccountID, request.AccountInstanceID, request.AccountGeneration,
		request.Kind, request.LocatorDigest, request.ExpectedCredentialDigest, request.IntentDigest,
	)
	if err != nil || request.OperationID != expectedID {
		return ErrAccountMutationState
	}
	return nil
}

func validateAccountMutation(mutation AccountMutation) error {
	if mutation.OperationID == (AccountMutationID{}) || mutation.AccountID <= 0 ||
		!mutation.Kind.valid() || !mutation.State.valid() || mutation.RegistrySequence == 0 ||
		validateAccountInstanceID(mutation.AccountInstanceID) != nil || mutation.AccountGeneration == 0 ||
		mutation.LocatorDigest.zero() || mutation.ExpectedCredentialDigest.zero() || mutation.IntentDigest.zero() ||
		mutation.ConfigDir == "" || mutation.KeychainService == "" || mutation.KeychainAccount == "" ||
		mutation.OwnerEpoch == 0 || mutation.CreatedAt.IsZero() || mutation.UpdatedAt.Before(mutation.CreatedAt) {
		return ErrAccountMutationState
	}
	if err := mutation.Owner.Validate(); err != nil {
		return err
	}
	if mutation.CredentialWritten && mutation.WrittenCredentialDigest.zero() {
		return ErrAccountMutationState
	}
	return nil
}

func validateAccountMutationReceipt(receipt AccountMutationReceipt) error {
	if receipt.OperationID == (AccountMutationID{}) || receipt.AccountID <= 0 ||
		!receipt.Kind.valid() || !receipt.Terminal.valid() || receipt.RegistrySequence == 0 ||
		validateAccountInstanceID(receipt.AccountInstanceID) != nil || receipt.AccountGeneration == 0 ||
		receipt.LocatorDigest.zero() || receipt.ExpectedCredentialDigest.zero() || receipt.IntentDigest.zero() ||
		receipt.OutcomeDigest.zero() ||
		receipt.ConfigDir == "" || receipt.KeychainService == "" || receipt.KeychainAccount == "" ||
		receipt.OwnerEpoch == 0 || receipt.CommittedAt.IsZero() || !receipt.ExpiresAt.After(receipt.CommittedAt) ||
		(!receipt.AcknowledgedAt.IsZero() && receipt.AcknowledgedAt.Before(receipt.CommittedAt)) {
		return ErrAccountMutationState
	}
	if err := receipt.Owner.Validate(); err != nil {
		return err
	}
	if receipt.CredentialWritten && receipt.WrittenCredentialDigest.zero() {
		return ErrAccountMutationState
	}
	if (receipt.Terminal == AccountMutationQuarantined) != receipt.HasQuarantine ||
		(receipt.HasQuarantine &&
			(receipt.QuarantineFileLocator.zero() || !receipt.QuarantineReason.quarantine())) {
		return ErrAccountMutationState
	}
	if receipt.ResolvedAt.IsZero() {
		if receipt.Resolution != "" || !receipt.ResolutionObservedDigest.zero() {
			return ErrAccountMutationState
		}
	} else if receipt.Resolution != AccountMutationCompensatedRelease ||
		receipt.ResolutionObservedDigest.zero() || receipt.ResolvedAt.Before(receipt.CommittedAt) {
		return ErrAccountMutationState
	}
	return nil
}

func (kind AccountMutationKind) valid() bool {
	switch kind {
	case AccountMutationAdd, AccountMutationRelogin, AccountMutationSyncInstall:
		return true
	default:
		return false
	}
}

func (state AccountMutationState) valid() bool {
	switch state {
	case AccountMutationAwaitingInput, AccountMutationReserved, AccountMutationApplying,
		AccountMutationApplied, AccountMutationPublishing, AccountMutationCompensating:
		return true
	default:
		return false
	}
}

func (terminal AccountMutationTerminal) valid() bool {
	switch terminal {
	case AccountMutationCommitted, AccountMutationSuperseded,
		AccountMutationAborted, AccountMutationQuarantined:
		return true
	default:
		return false
	}
}

func sameAccountMutationIntent(mutation AccountMutation, request BeginAccountMutationRequest) bool {
	return mutation.Kind == request.Kind &&
		mutation.AccountInstanceID == request.AccountInstanceID &&
		mutation.AccountGeneration == request.AccountGeneration &&
		mutation.LocatorDigest == request.LocatorDigest &&
		mutation.ExpectedCredentialDigest == request.ExpectedCredentialDigest &&
		mutation.IntentDigest == request.IntentDigest &&
		mutation.ConfigDir == request.ConfigDir &&
		mutation.KeychainService == request.KeychainService &&
		mutation.KeychainAccount == request.KeychainAccount
}

func sameAccountMutationReceiptIntent(
	receipt AccountMutationReceipt,
	request BeginAccountMutationRequest,
) bool {
	return receipt.OperationID == request.OperationID &&
		receipt.AccountID == request.AccountID && receipt.Kind == request.Kind &&
		receipt.AccountInstanceID == request.AccountInstanceID &&
		receipt.AccountGeneration == request.AccountGeneration &&
		receipt.LocatorDigest == request.LocatorDigest &&
		receipt.ExpectedCredentialDigest == request.ExpectedCredentialDigest &&
		receipt.IntentDigest == request.IntentDigest &&
		receipt.ConfigDir == request.ConfigDir &&
		receipt.KeychainService == request.KeychainService &&
		receipt.KeychainAccount == request.KeychainAccount
}

func sameAccountMutationFence(mutation AccountMutation, fence AccountMutationFence) bool {
	return mutation.OperationID == fence.OperationID && mutation.OwnerEpoch == fence.OwnerEpoch &&
		sameCredentialOwner(mutation.Owner, fence.Owner)
}

func accountMutationReceiptFenceMatches(
	receipt AccountMutationReceipt,
	fence AccountMutationFence,
) bool {
	return receipt.OperationID == fence.OperationID && receipt.OwnerEpoch == fence.OwnerEpoch &&
		sameCredentialOwner(receipt.Owner, fence.Owner)
}
