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
	// AccountMutationAdd starts the globally serialized account-add lane.
	AccountMutationAdd AccountMutationKind = "add"
	// AccountMutationRelogin replaces one existing account's credentials.
	AccountMutationRelogin AccountMutationKind = "relogin"
	// AccountMutationPresentationRebind repairs one quarantined presentation generation.
	AccountMutationPresentationRebind AccountMutationKind = "presentation-rebind"
)

// AccountMutationState is one daemon-owned durable mutation phase.
type AccountMutationState string

const (
	// AccountMutationAwaitingPresentation owns an unbound add reservation.
	AccountMutationAwaitingPresentation AccountMutationState = "awaiting-presentation"
	// AccountMutationAwaitingInput is the pre-I/O interactive phase.
	AccountMutationAwaitingInput AccountMutationState = "awaiting-input"
	// AccountMutationReserved owns one durable account reservation.
	AccountMutationReserved AccountMutationState = "reserved"
	// AccountMutationApplying has crossed the external-I/O boundary.
	AccountMutationApplying AccountMutationState = "applying"
	// AccountMutationApplied has completed external I/O.
	AccountMutationApplied AccountMutationState = "applied"
	// AccountMutationPublishing is committing the registry result.
	AccountMutationPublishing AccountMutationState = "publishing"
	// AccountMutationCompensating is undoing an unpublished external write.
	AccountMutationCompensating AccountMutationState = "compensating"
	// AccountMutationRebindPublished holds a new binding quarantined until old owners are retired.
	AccountMutationRebindPublished AccountMutationState = "rebind-published"
)

// AccountMutationTerminal is one immutable registry mutation result.
type AccountMutationTerminal string

const (
	// AccountMutationCommitted records a successfully published mutation.
	AccountMutationCommitted AccountMutationTerminal = "committed"
	// AccountMutationSuperseded records registry drift before publication.
	AccountMutationSuperseded AccountMutationTerminal = "superseded"
	// AccountMutationAborted records a pre-I/O cancellation.
	AccountMutationAborted AccountMutationTerminal = "aborted"
	// AccountMutationQuarantined records unresolved external ambiguity.
	AccountMutationQuarantined AccountMutationTerminal = "quarantined"
)

var (
	// ErrAccountMutationBusy reports an occupied mutation lane.
	ErrAccountMutationBusy = errors.New("account mutation lane busy")
	// ErrAccountMutationFence reports stale owner authority.
	ErrAccountMutationFence = errors.New("account mutation fence changed")
	// ErrAccountMutationState reports an invalid phase transition.
	ErrAccountMutationState = errors.New("account mutation state changed")
	// ErrAccountMutationRecoveryRequired reports unresolved durable evidence.
	ErrAccountMutationRecoveryRequired = errors.New("account mutation recovery required")
	// ErrAccountMutationSuperseded reports registry generation drift.
	ErrAccountMutationSuperseded = errors.New("account mutation superseded by registry change")
	// ErrAccountRemoving reports a conflicting durable removal.
	ErrAccountRemoving = errors.New("account removal already reserved")
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
	PresentationProof        PresentationPreparationProof
	HasPresentationProof     bool
	PreviousConfigDir        string
	PreviousKeychainService  string
	PreviousKeychainAccount  string
	PreviousLocatorDigest    CredentialDigest
	PreviousCredentialState  CredentialSlotState
	PreviousCredentialDigest CredentialDigest
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
	PresentationProof        PresentationPreparationProof
	HasPresentationProof     bool
	PreviousConfigDir        string
	PreviousKeychainService  string
	PreviousKeychainAccount  string
	PreviousLocatorDigest    CredentialDigest
	PreviousCredentialState  CredentialSlotState
	PreviousCredentialDigest CredentialDigest
	Owner                    proc.Record
	OwnerEpoch               uint64
	PublicationPending       bool
	CommittedAt              time.Time
	AcknowledgedAt           time.Time
	ExpiresAt                time.Time
	QuarantineReason         CredentialResultCategory
	HasQuarantine            bool
	Resolution               AccountMutationResolution
	ResolutionObservedDigest CredentialDigest
	ResolvedAt               time.Time
}

// AccountMutationQuarantine records the exact unsafe external observation.
type AccountMutationQuarantine struct {
	Observation CredentialExternalState
	Reason      CredentialResultCategory
}

// AccountMutationResolution is one explicit audited terminal recovery result.
type AccountMutationResolution string

const (
	// AccountMutationCompensatedRelease records an explicitly released quarantined add.
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
	PreviousConfigDir        string
	PreviousKeychainService  string
	PreviousKeychainAccount  string
	PreviousLocatorDigest    CredentialDigest
	PreviousCredentialState  CredentialSlotState
	PreviousCredentialDigest CredentialDigest
	Owner                    proc.Record
}

// BeginAccountMutationResult returns one active lane or immutable receipt.
type BeginAccountMutationResult struct {
	Active  *AccountMutation
	Receipt *AccountMutationReceipt
	Created bool
}

const (
	accountMutationIDDomain            = "cc-pool:account-mutation:v1"
	pendingAddMutationIDDomain         = "cc-pool:pending-add-mutation:v1"
	presentationRebindMutationIDDomain = "cc-pool:presentation-rebind-mutation:v1"
)

// NewPendingAddMutationID derives an add operation from its reservation and intent.
func NewPendingAddMutationID(
	accountID int,
	accountInstanceID string,
	accountGeneration uint64,
	intent CredentialDigest,
) (AccountMutationID, error) {
	if accountID <= 0 || validateAccountInstanceID(accountInstanceID) != nil ||
		accountGeneration == 0 || intent.zero() {
		return AccountMutationID{}, ErrAccountMutationState
	}
	hash := sha256.New()
	writeCredentialHashField(hash, []byte(pendingAddMutationIDDomain))
	var account [8]byte
	binary.BigEndian.PutUint64(account[:], uint64(accountID))
	writeCredentialHashField(hash, account[:])
	writeCredentialHashField(hash, []byte(accountInstanceID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], accountGeneration)
	writeCredentialHashField(hash, generation[:])
	writeCredentialHashField(hash, intent[:])
	var operationID AccountMutationID
	copy(operationID[:], hash.Sum(nil))
	return operationID, nil
}

func validPreviousCredential(state CredentialSlotState, digest CredentialDigest) bool {
	switch state {
	case CredentialSlotEmpty:
		return digest.zero()
	case CredentialSlotPresent:
		return !digest.zero()
	default:
		return false
	}
}

func optionalPreviousCredentialDigest(state CredentialSlotState, digest CredentialDigest) any {
	if state == CredentialSlotEmpty || state == "" {
		return nil
	}
	return digest[:]
}

// NewPresentationRebindMutationID derives one quarantined presentation repair.
func NewPresentationRebindMutationID(
	accountID int,
	accountInstanceID string,
	accountGeneration uint64,
	previousLocator CredentialDigest,
	previousCredentialState CredentialSlotState,
	previousCredential CredentialDigest,
	intent CredentialDigest,
) (AccountMutationID, error) {
	if accountID <= 0 || validateAccountInstanceID(accountInstanceID) != nil ||
		accountGeneration < 2 || previousLocator.zero() ||
		!validPreviousCredential(previousCredentialState, previousCredential) || intent.zero() {
		return AccountMutationID{}, ErrAccountMutationState
	}
	hash := sha256.New()
	writeCredentialHashField(hash, []byte(presentationRebindMutationIDDomain))
	var account [8]byte
	binary.BigEndian.PutUint64(account[:], uint64(accountID))
	writeCredentialHashField(hash, account[:])
	writeCredentialHashField(hash, []byte(accountInstanceID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], accountGeneration)
	writeCredentialHashField(hash, generation[:])
	writeCredentialHashField(hash, previousLocator[:])
	writeCredentialHashField(hash, []byte(previousCredentialState))
	if previousCredentialState == CredentialSlotEmpty {
		writeCredentialHashField(hash, nil)
	} else {
		writeCredentialHashField(hash, previousCredential[:])
	}
	writeCredentialHashField(hash, intent[:])
	var operationID AccountMutationID
	copy(operationID[:], hash.Sum(nil))
	return operationID, nil
}

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
	if request.Kind == AccountMutationPresentationRebind {
		if _, err := credentialOperationByAccount(tx, request.AccountID); err == nil {
			return BeginAccountMutationResult{}, ErrAccountMutationRecoveryRequired
		} else if !errors.Is(err, sql.ErrNoRows) {
			return BeginAccountMutationResult{}, err
		}
		if _, err := unacknowledgedCredentialWriteReceiptByAccount(tx, request.AccountID); err == nil {
			return BeginAccountMutationResult{}, ErrAccountMutationRecoveryRequired
		} else if !errors.Is(err, sql.ErrNoRows) {
			return BeginAccountMutationResult{}, err
		}
	}
	if _, err := accountPresentationQuarantine(tx, request.AccountID); err == nil {
		if request.Kind != AccountMutationPresentationRebind {
			return BeginAccountMutationResult{}, ErrAccountPresentationQuarantined
		}
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
	if request.Kind != AccountMutationAdd {
		var sessionID int64
		err := tx.QueryRow(
			`SELECT id FROM sessions WHERE account_id=? AND ended_at IS NULL LIMIT 1`,
			request.AccountID,
		).Scan(&sessionID)
		if err == nil {
			return BeginAccountMutationResult{}, ErrAccountSessionActive
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return BeginAccountMutationResult{}, err
		}
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
	if request.Kind == AccountMutationAdd || request.Kind == AccountMutationPresentationRebind {
		state = AccountMutationAwaitingPresentation
	} else if request.Kind == AccountMutationRelogin {
		state = AccountMutationAwaitingInput
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO account_mutations(
		 operation_id,account_id,kind,state,registry_sequence,
		 account_instance_id,account_generation,locator_digest,
		 expected_credential_digest,intent_digest,config_dir,keychain_service,keychain_account,label,account_uuid,
		 proof_catalog_tenant_id,proof_catalog_generation,proof_requested,proof_desired,proof_observed,
		 proof_verified,proof_applied,proof_source_authority,proof_source_revision,proof_catalog_revision,
		 proof_change_id,proof_operation_id,proof_presentation_kind,proof_tenant_id,proof_domain_id,
		 proof_presentation_generation,proof_activation_generation,proof_public_path,
		 previous_config_dir,previous_keychain_service,previous_keychain_account,
		 previous_locator_digest,previous_credential_state,previous_credential_digest,
		 owner_record,owner_epoch,created_at,updated_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'',0,0,0,0,0,0,'',0,0,'','','','','',0,'','',?,?,?,?,?,?,?,1,?,?)`,
		request.OperationID[:], request.AccountID, request.Kind, state, sequence,
		request.AccountInstanceID, request.AccountGeneration, request.LocatorDigest[:],
		request.ExpectedCredentialDigest[:], request.IntentDigest[:], request.ConfigDir,
		request.KeychainService, request.KeychainAccount, request.Label, request.AccountUUID,
		request.PreviousConfigDir, request.PreviousKeychainService, request.PreviousKeychainAccount,
		request.PreviousLocatorDigest[:], request.PreviousCredentialState,
		optionalPreviousCredentialDigest(request.PreviousCredentialState, request.PreviousCredentialDigest),
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

// BindAccountMutationPresentation records proven public credential coordinates
// before an add or presentation rebind may accept interactive input.
func (s *Store) BindAccountMutationPresentation(
	fence AccountMutationFence,
	proof PresentationPreparationProof,
	configDir string,
	keychainService string,
	keychainAccount string,
	locator CredentialDigest,
	expected CredentialDigest,
) (AccountMutationFence, error) {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if err := validatePresentationPreparationProof(proof); err != nil {
		return AccountMutationFence{}, err
	}
	if configDir == "" || keychainService == "" || keychainAccount == "" ||
		locator.zero() || expected.zero() {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	if err := fence.Owner.Validate(); err != nil {
		return AccountMutationFence{}, err
	}
	mutation, err := s.AccountMutation(fence.OperationID)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if !sameAccountMutationFence(mutation, fence) || mutation.State != AccountMutationAwaitingPresentation ||
		(mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationPresentationRebind) {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if err := validatePresentationPreparationProofForAccount(
		proof, mutation.AccountInstanceID, mutation.AccountGeneration, configDir,
	); err != nil {
		return AccountMutationFence{}, err
	}
	if mutation.Kind == AccountMutationPresentationRebind {
		if configDir == mutation.PreviousConfigDir ||
			(keychainService == mutation.PreviousKeychainService &&
				keychainAccount == mutation.PreviousKeychainAccount) ||
			locator == mutation.PreviousLocatorDigest ||
			locator != CredentialKeychainLocatorDigest(keychainService, keychainAccount) {
			return AccountMutationFence{}, ErrAccountPresentationEvidence
		}
		quarantine, err := s.AccountPresentationQuarantine(mutation.AccountID)
		if err != nil {
			return AccountMutationFence{}, err
		}
		if !quarantineAllowsPresentationRebind(quarantine, proof) {
			return AccountMutationFence{}, ErrAccountPresentationEvidence
		}
	}
	owner, err := json.Marshal(fence.Owner)
	if err != nil {
		return AccountMutationFence{}, err
	}
	result, err := s.db.Exec(
		`UPDATE account_mutations SET state='awaiting-input',
		 config_dir=?,keychain_service=?,keychain_account=?,locator_digest=?,
		 expected_credential_digest=?,
		 proof_catalog_tenant_id=?,proof_catalog_generation=?,proof_requested=?,proof_desired=?,
		 proof_observed=?,proof_verified=?,proof_applied=?,proof_source_authority=?,
		 proof_source_revision=?,proof_catalog_revision=?,proof_change_id=?,proof_operation_id=?,
		 proof_presentation_kind=?,proof_tenant_id=?,proof_domain_id=?,
		 proof_presentation_generation=?,proof_activation_generation=?,proof_public_path=?,
		 owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND kind IN ('add','presentation-rebind') AND state='awaiting-presentation'`,
		configDir, keychainService, keychainAccount, locator[:], expected[:],
		proof.CatalogTenantID, proof.CatalogGeneration, proof.Requested, proof.Desired,
		proof.Observed, proof.Verified, proof.Applied, proof.SourceAuthority,
		proof.SourceRevision, proof.CatalogRevision, proof.ChangeID, proof.OperationID,
		proof.PresentationKind, proof.FileProvider.TenantID, proof.FileProvider.DomainID,
		proof.FileProvider.Generation, proof.FileProvider.ActivationGeneration,
		proof.FileProvider.PublicPath, s.now().UnixNano(),
		fence.OperationID[:], owner, fence.OwnerEpoch,
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

// RefreshAccountMutationPresentation atomically replaces advancing preparation
// provenance without permitting presentation identity drift.
func (s *Store) RefreshAccountMutationPresentation(
	fence AccountMutationFence,
	proof PresentationPreparationProof,
) (AccountMutationFence, error) {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if err := fence.Owner.Validate(); err != nil {
		return AccountMutationFence{}, err
	}
	if err := validatePresentationPreparationProof(proof); err != nil {
		return AccountMutationFence{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return AccountMutationFence{}, err
	}
	defer func() { _ = tx.Rollback() }()
	mutation, err := accountMutationByID(tx, fence.OperationID)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if !sameAccountMutationFence(mutation, fence) ||
		(mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationPresentationRebind) ||
		mutation.State == AccountMutationAwaitingPresentation ||
		mutation.State == AccountMutationCompensating {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if err := validatePresentationPreparationProofForAccount(
		proof, mutation.AccountInstanceID, mutation.AccountGeneration, mutation.ConfigDir,
	); err != nil {
		return AccountMutationFence{}, err
	}
	if err := ValidatePresentationPreparationProofAdvance(
		mutation.PresentationProof, proof,
	); err != nil {
		return AccountMutationFence{}, err
	}
	result, err := tx.Exec(
		`UPDATE account_mutations SET
		 proof_catalog_tenant_id=?,proof_catalog_generation=?,proof_requested=?,proof_desired=?,
		 proof_observed=?,proof_verified=?,proof_applied=?,proof_source_authority=?,
		 proof_source_revision=?,proof_catalog_revision=?,proof_change_id=?,proof_operation_id=?,
		 proof_presentation_kind=?,proof_tenant_id=?,proof_domain_id=?,
		 proof_presentation_generation=?,proof_activation_generation=?,proof_public_path=?,
		 owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND kind IN ('add','presentation-rebind')
		 AND state<>'awaiting-presentation' AND state<>'compensating'`,
		proof.CatalogTenantID, proof.CatalogGeneration, proof.Requested, proof.Desired,
		proof.Observed, proof.Verified, proof.Applied, proof.SourceAuthority,
		proof.SourceRevision, proof.CatalogRevision, proof.ChangeID, proof.OperationID,
		proof.PresentationKind, proof.FileProvider.TenantID, proof.FileProvider.DomainID,
		proof.FileProvider.Generation, proof.FileProvider.ActivationGeneration,
		proof.FileProvider.PublicPath, s.now().UnixNano(), fence.OperationID[:],
		mustEncodeCredentialOwner(fence.Owner), fence.OwnerEpoch,
	)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if mutation.State == AccountMutationRebindPublished {
		account, err := presentationAccount(tx, mutation.AccountID)
		if err != nil {
			return AccountMutationFence{}, err
		}
		if account.InstanceID != mutation.AccountInstanceID ||
			account.Generation != mutation.AccountGeneration || account.ConfigDir != mutation.ConfigDir ||
			account.KeychainService != mutation.KeychainService ||
			account.KeychainAccount != mutation.KeychainAccount {
			return AccountMutationFence{}, ErrAccountGenerationChanged
		}
		if err := upsertAccountPresentation(tx, AccountPresentation{
			AccountID: account.ID, AccountInstanceID: account.InstanceID,
			AccountGeneration: account.Generation, Proof: proof, ObservedAt: s.now(),
		}); err != nil {
			return AccountMutationFence{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return AccountMutationFence{}, err
	}
	fence.OwnerEpoch++
	return fence, nil
}

func quarantineAllowsPresentationRebind(
	quarantine AccountPresentationQuarantine,
	target PresentationPreparationProof,
) bool {
	return quarantine.Proof.CatalogTenantID == target.CatalogTenantID &&
		quarantine.Proof.SourceAuthority == target.SourceAuthority &&
		quarantine.Proof.PresentationKind == target.PresentationKind &&
		quarantine.Proof.FileProvider.TenantID == target.FileProvider.TenantID &&
		quarantine.Proof.FileProvider.DomainID == target.FileProvider.DomainID &&
		quarantine.Proof.FileProvider.PublicPath == target.FileProvider.PublicPath &&
		(quarantine.Proof.FileProvider.Generation == quarantine.AccountGeneration ||
			quarantine.Proof.FileProvider.Generation == target.FileProvider.Generation)
}

// CancelUnboundAccountMutation releases an add or rebind that never acquired a public presentation.
func (s *Store) CancelUnboundAccountMutation(fence AccountMutationFence) error {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return ErrAccountMutationFence
	}
	if err := fence.Owner.Validate(); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	mutation, err := accountMutationByID(tx, fence.OperationID)
	if err != nil {
		return err
	}
	if !sameAccountMutationFence(mutation, fence) {
		return ErrAccountMutationFence
	}
	if (mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationPresentationRebind) ||
		mutation.State != AccountMutationAwaitingPresentation {
		return ErrAccountMutationState
	}
	result, err := tx.Exec(
		`DELETE FROM account_mutations WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND kind IN ('add','presentation-rebind') AND state='awaiting-presentation'`,
		fence.OperationID[:], mustEncodeCredentialOwner(fence.Owner), fence.OwnerEpoch,
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrAccountMutationFence
	}
	if mutation.Kind == AccountMutationPresentationRebind {
		return tx.Commit()
	}
	result, err = tx.Exec(
		`DELETE FROM pending_adds WHERE id=? AND instance_id=? AND generation=? AND owner_record=?`,
		mutation.AccountID, mutation.AccountInstanceID, mutation.AccountGeneration,
		mustEncodeCredentialOwner(fence.Owner),
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrAccountMutationFence
	}
	return tx.Commit()
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
) (mutations []AccountMutation, more bool, err error) {
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
	defer func() { err = errors.Join(err, rows.Close()) }()
	mutations = make([]AccountMutation, 0, limit)
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
	if (mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationRelogin &&
		mutation.Kind != AccountMutationPresentationRebind) ||
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
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if written.zero() {
		return AccountMutationFence{}, ErrAccountMutationState
	}
	mutation, err := s.AccountMutation(fence.OperationID)
	if err != nil {
		return AccountMutationFence{}, err
	}
	if mutation.Kind == AccountMutationPresentationRebind {
		return s.markPresentationRebindApplied(fence, written)
	}
	return s.advanceAccountMutation(
		fence, AccountMutationApplying, AccountMutationApplied,
		CredentialDigest{}, false, written, true,
	)
}

func (s *Store) markPresentationRebindApplied(
	fence AccountMutationFence,
	written CredentialDigest,
) (AccountMutationFence, error) {
	if err := fence.Owner.Validate(); err != nil {
		return AccountMutationFence{}, err
	}
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
	if !sameAccountMutationFence(mutation, fence) ||
		mutation.Kind != AccountMutationPresentationRebind ||
		mutation.State != AccountMutationApplying {
		return AccountMutationFence{}, ErrAccountMutationFence
	}
	if _, err := credentialOperationByAccount(tx, mutation.AccountID); err == nil {
		return AccountMutationFence{}, ErrAccountMutationRecoveryRequired
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AccountMutationFence{}, err
	}
	if _, err := unacknowledgedCredentialWriteReceiptByAccount(tx, mutation.AccountID); err == nil {
		return AccountMutationFence{}, ErrAccountMutationRecoveryRequired
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AccountMutationFence{}, err
	}
	result, err := tx.Exec(
		`UPDATE account_mutations SET state='applied',owner_epoch=owner_epoch+1,updated_at=?,
		 written_credential_digest=?,credential_written=1
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND kind='presentation-rebind' AND state='applying'`,
		s.now().UnixNano(), written[:], fence.OperationID[:],
		mustEncodeCredentialOwner(fence.Owner), fence.OwnerEpoch,
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
	if mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationRelogin &&
		mutation.Kind != AccountMutationPresentationRebind {
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

// PublishAccountPresentationRebind atomically installs the target registry
// binding while retaining both the journal and presentation quarantine.
func (s *Store) PublishAccountPresentationRebind(
	fence AccountMutationFence,
) (AccountMutationFence, Account, error) {
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 {
		return AccountMutationFence{}, Account{}, ErrAccountMutationFence
	}
	if err := fence.Owner.Validate(); err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	defer func() { _ = tx.Rollback() }()
	mutation, err := accountMutationByID(tx, fence.OperationID)
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if !sameAccountMutationFence(mutation, fence) ||
		mutation.Kind != AccountMutationPresentationRebind || mutation.State != AccountMutationPublishing ||
		!mutation.CredentialWritten || mutation.WrittenCredentialDigest.zero() {
		return AccountMutationFence{}, Account{}, ErrAccountMutationFence
	}
	var sequence uint64
	if err := tx.QueryRow(
		`SELECT sequence FROM account_registry_sequences WHERE account_id=?`, mutation.AccountID,
	).Scan(&sequence); err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if sequence != mutation.RegistrySequence {
		return AccountMutationFence{}, Account{}, ErrAccountMutationSuperseded
	}
	if mutation.AccountUUID == "" {
		return AccountMutationFence{}, Account{}, ErrAccountMutationState
	}
	if err := accountMutationSubjectMatches(tx, mutation); err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	quarantine, err := accountPresentationQuarantine(tx, mutation.AccountID)
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if quarantine.AccountInstanceID != mutation.AccountInstanceID ||
		quarantine.AccountGeneration+1 != mutation.AccountGeneration ||
		quarantine.ExpectedConfigDir != mutation.PreviousConfigDir ||
		!quarantineAllowsPresentationRebind(quarantine, mutation.PresentationProof) {
		return AccountMutationFence{}, Account{}, ErrAccountPresentationEvidence
	}
	busy, err := accountPresentationBusyExceptMutation(tx, mutation.AccountID, mutation.OperationID)
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if busy {
		return AccountMutationFence{}, Account{}, ErrAccountPresentationBusy
	}
	result, err := tx.Exec(
		`UPDATE accounts SET generation=?,config_dir=?,keychain_service=?,keychain_account=?,label=?,account_uuid=?
		 WHERE id=? AND instance_id=? AND generation=? AND config_dir=?
		 AND keychain_service=? AND keychain_account=? AND deleted_at IS NULL`,
		mutation.AccountGeneration, mutation.ConfigDir, mutation.KeychainService,
		mutation.KeychainAccount, mutation.Label, mutation.AccountUUID,
		mutation.AccountID, mutation.AccountInstanceID, mutation.AccountGeneration-1,
		mutation.PreviousConfigDir, mutation.PreviousKeychainService, mutation.PreviousKeychainAccount,
	)
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationFence{}, Account{}, ErrAccountGenerationChanged
	}
	if _, err := tx.Exec(`DELETE FROM account_presentations WHERE account_id=?`, mutation.AccountID); err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: mutation.AccountID, AccountInstanceID: mutation.AccountInstanceID,
		AccountGeneration: mutation.AccountGeneration, Proof: mutation.PresentationProof,
		ObservedAt: s.now(),
	}); err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	result, err = tx.Exec(
		`UPDATE account_mutations SET state='rebind-published',owner_epoch=owner_epoch+1,updated_at=?
		 WHERE operation_id=? AND owner_record=? AND owner_epoch=?
		 AND kind='presentation-rebind' AND state='publishing'`,
		s.now().UnixNano(), mutation.OperationID[:], mustEncodeCredentialOwner(fence.Owner), fence.OwnerEpoch,
	)
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationFence{}, Account{}, ErrAccountMutationFence
	}
	updated, err := presentationAccount(tx, mutation.AccountID)
	if err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	if err := tx.Commit(); err != nil {
		return AccountMutationFence{}, Account{}, err
	}
	fence.OwnerEpoch++
	return fence, updated, nil
}

// CommitAccountPresentationRebind admits a published binding only after its
// caller has verified the old owners absent and the new owner unchanged.
func (s *Store) CommitAccountPresentationRebind(
	fence AccountMutationFence,
	receiptExpiresAt time.Time,
) (AccountMutationReceipt, error) {
	now := s.now()
	if fence.OperationID == (AccountMutationID{}) || fence.OwnerEpoch == 0 ||
		!receiptExpiresAt.After(now) {
		return AccountMutationReceipt{}, ErrAccountMutationState
	}
	if err := fence.Owner.Validate(); err != nil {
		return AccountMutationReceipt{}, err
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
				receipt.Kind != AccountMutationPresentationRebind ||
				receipt.Terminal != AccountMutationCommitted {
				return receipt, ErrAccountMutationState
			}
			return receipt, nil
		}
		return AccountMutationReceipt{}, err
	}
	if !sameAccountMutationFence(mutation, fence) ||
		mutation.Kind != AccountMutationPresentationRebind ||
		mutation.State != AccountMutationRebindPublished || !mutation.CredentialWritten {
		return AccountMutationReceipt{}, ErrAccountMutationFence
	}
	account, err := presentationAccount(tx, mutation.AccountID)
	if err != nil {
		return AccountMutationReceipt{}, err
	}
	if account.InstanceID != mutation.AccountInstanceID || account.Generation != mutation.AccountGeneration ||
		account.ConfigDir != mutation.ConfigDir || account.KeychainService != mutation.KeychainService ||
		account.KeychainAccount != mutation.KeychainAccount {
		return AccountMutationReceipt{}, ErrAccountGenerationChanged
	}
	presentation, err := accountPresentation(tx, mutation.AccountID)
	if err != nil {
		return AccountMutationReceipt{}, err
	}
	if presentation.AccountInstanceID != mutation.AccountInstanceID ||
		presentation.AccountGeneration != mutation.AccountGeneration ||
		presentation.Proof != mutation.PresentationProof {
		return AccountMutationReceipt{}, ErrAccountPresentationEvidence
	}
	quarantine, err := accountPresentationQuarantine(tx, mutation.AccountID)
	if err != nil {
		return AccountMutationReceipt{}, err
	}
	if quarantine.AccountInstanceID != mutation.AccountInstanceID ||
		quarantine.AccountGeneration+1 != mutation.AccountGeneration ||
		quarantine.ExpectedConfigDir != mutation.PreviousConfigDir ||
		!quarantineAllowsPresentationRebind(quarantine, mutation.PresentationProof) {
		return AccountMutationReceipt{}, ErrAccountPresentationEvidence
	}
	var sequence uint64
	if err := tx.QueryRow(
		`SELECT sequence FROM account_registry_sequences WHERE account_id=?`, mutation.AccountID,
	).Scan(&sequence); err != nil {
		return AccountMutationReceipt{}, err
	}
	if sequence != mutation.RegistrySequence {
		return AccountMutationReceipt{}, ErrAccountMutationSuperseded
	}
	if _, err := accountRemovalByID(tx, mutation.AccountID); err == nil {
		return AccountMutationReceipt{}, ErrAccountRemoving
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AccountMutationReceipt{}, err
	}
	result, err := tx.Exec(
		`DELETE FROM account_presentation_quarantines
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		mutation.AccountID, mutation.AccountInstanceID, mutation.AccountGeneration-1,
	)
	if err != nil {
		return AccountMutationReceipt{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountMutationReceipt{}, ErrAccountPresentationEvidence
	}
	if err := insertAccountMutationReceipt(
		tx, mutation, AccountMutationCommitted, mutation.WrittenCredentialDigest, nil,
		now, receiptExpiresAt,
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
	if mutation.Kind == AccountMutationPresentationRebind {
		return AccountMutationReceipt{}, ErrAccountMutationState
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
	if mutation.AccountUUID == "" {
		return AccountMutationReceipt{}, ErrAccountMutationState
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
		if err := upsertAccountPresentation(tx, AccountPresentation{
			AccountID: mutation.AccountID, AccountInstanceID: mutation.AccountInstanceID,
			AccountGeneration: mutation.AccountGeneration, Proof: mutation.PresentationProof,
			ObservedAt: now,
		}); err != nil {
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
			Observation: quarantine.Observation, Reason: quarantine.Reason,
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
	if !quarantine.Reason.quarantine() {
		return ErrAccountMutationState
	}
	if err := quarantine.Observation.validate(); err != nil {
		return err
	}
	observedDigest, err := quarantine.Observation.Digest()
	if err != nil {
		return err
	}
	if observedDigest != outcome || CredentialKeychainLocatorDigest(
		mutation.KeychainService, mutation.KeychainAccount,
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

// PendingAccountMutationPublications returns one bounded admission-fenced page.
func (s *Store) PendingAccountMutationPublications(limit int) ([]AccountMutationReceipt, error) {
	if limit <= 0 || limit > CredentialOperationPageLimit {
		return nil, ErrAccountMutationState
	}
	rows, err := s.db.Query(
		`SELECT `+accountMutationReceiptColumns+` FROM account_mutation_receipts
		 WHERE publication_pending=1 ORDER BY committed_at,operation_id LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	receipts := make([]AccountMutationReceipt, 0, limit)
	for rows.Next() {
		receipt, err := scanAccountMutationReceipt(rows)
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}
	return receipts, rows.Err()
}

// AcknowledgeAccountMutationReceipt records delivery without deleting replay evidence.
func (s *Store) AcknowledgeAccountMutationReceipt(operationID AccountMutationID) error {
	if operationID == (AccountMutationID{}) {
		return ErrAccountMutationState
	}
	result, err := s.db.Exec(
		`UPDATE account_mutation_receipts
		 SET acknowledged_at=COALESCE(acknowledged_at,?)
		 WHERE operation_id=? AND publication_pending=0`,
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

// MarkAccountMutationPublicationSettled clears the admission fence after the
// committed credential write-back and durable publication have settled.
func (s *Store) MarkAccountMutationPublicationSettled(operationID AccountMutationID) error {
	if operationID == (AccountMutationID{}) {
		return ErrAccountMutationState
	}
	result, err := s.db.Exec(
		`UPDATE account_mutation_receipts SET publication_pending=0
		 WHERE operation_id=? AND terminal='committed'
		 AND kind IN ('add','presentation-rebind')`,
		operationID[:],
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
	if request.Kind == AccountMutationPresentationRebind {
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
		if request.AccountGeneration != generation+1 || instanceID != request.AccountInstanceID ||
			configDir != request.PreviousConfigDir || service != request.PreviousKeychainService ||
			account != request.PreviousKeychainAccount {
			return ErrAccountGenerationChanged
		}
		quarantine, err := accountPresentationQuarantine(tx, request.AccountID)
		if err != nil {
			return err
		}
		if quarantine.AccountInstanceID != instanceID || quarantine.AccountGeneration != generation ||
			quarantine.ExpectedConfigDir != configDir ||
			quarantine.Proof.FileProvider.TenantID != "account-"+instanceID ||
			quarantine.Proof.FileProvider.PublicPath == configDir {
			return ErrAccountPresentationEvidence
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
	if mutation.Kind == AccountMutationPresentationRebind {
		return validateAccountMutationSubject(tx, BeginAccountMutationRequest{
			AccountID: mutation.AccountID, Kind: mutation.Kind,
			AccountInstanceID: mutation.AccountInstanceID, AccountGeneration: mutation.AccountGeneration,
			PreviousConfigDir:       mutation.PreviousConfigDir,
			PreviousKeychainService: mutation.PreviousKeychainService,
			PreviousKeychainAccount: mutation.PreviousKeychainAccount,
		})
	}
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
 proof_catalog_tenant_id,proof_catalog_generation,proof_requested,proof_desired,proof_observed,
 proof_verified,proof_applied,proof_source_authority,proof_source_revision,proof_catalog_revision,
 proof_change_id,proof_operation_id,proof_presentation_kind,
 proof_tenant_id,proof_domain_id,proof_presentation_generation,proof_activation_generation,proof_public_path,
 previous_config_dir,previous_keychain_service,previous_keychain_account,
 previous_locator_digest,previous_credential_state,previous_credential_digest,
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
	var operationID, locator, expected, intent, input, written, previousLocator, previousCredential, owner []byte
	var credentialWritten int
	var createdAt, updatedAt int64
	if err := row.Scan(
		&operationID, &mutation.AccountID, &mutation.Kind, &mutation.State, &mutation.RegistrySequence,
		&mutation.AccountInstanceID, &mutation.AccountGeneration, &locator, &expected, &intent,
		&input, &written, &credentialWritten,
		&mutation.ConfigDir, &mutation.KeychainService, &mutation.KeychainAccount,
		&mutation.Label, &mutation.AccountUUID,
		&mutation.PresentationProof.CatalogTenantID, &mutation.PresentationProof.CatalogGeneration,
		&mutation.PresentationProof.Requested, &mutation.PresentationProof.Desired,
		&mutation.PresentationProof.Observed, &mutation.PresentationProof.Verified,
		&mutation.PresentationProof.Applied, &mutation.PresentationProof.SourceAuthority,
		&mutation.PresentationProof.SourceRevision, &mutation.PresentationProof.CatalogRevision,
		&mutation.PresentationProof.ChangeID, &mutation.PresentationProof.OperationID,
		&mutation.PresentationProof.PresentationKind,
		&mutation.PresentationProof.FileProvider.TenantID,
		&mutation.PresentationProof.FileProvider.DomainID,
		&mutation.PresentationProof.FileProvider.Generation,
		&mutation.PresentationProof.FileProvider.ActivationGeneration,
		&mutation.PresentationProof.FileProvider.PublicPath,
		&mutation.PreviousConfigDir, &mutation.PreviousKeychainService,
		&mutation.PreviousKeychainAccount, &previousLocator, &mutation.PreviousCredentialState,
		&previousCredential,
		&owner, &mutation.OwnerEpoch,
		&createdAt, &updatedAt,
	); err != nil {
		return mutation, err
	}
	if len(operationID) != 32 || len(locator) != 32 || len(expected) != 32 ||
		len(intent) != 32 || len(written) != 32 || len(previousLocator) != 32 ||
		(previousCredential != nil && len(previousCredential) != 32) {
		return mutation, ErrAccountMutationState
	}
	copy(mutation.OperationID[:], operationID)
	copy(mutation.LocatorDigest[:], locator)
	copy(mutation.ExpectedCredentialDigest[:], expected)
	copy(mutation.IntentDigest[:], intent)
	copy(mutation.PreviousLocatorDigest[:], previousLocator)
	if previousCredential != nil {
		copy(mutation.PreviousCredentialDigest[:], previousCredential)
	}
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
	mutation.HasPresentationProof = mutation.PresentationProof.PresentationKind != ""
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
 quarantine_reason,resolution,resolution_observed_digest,resolved_at,
 config_dir,keychain_service,keychain_account,label,account_uuid,owner_record,owner_epoch,
 proof_catalog_tenant_id,proof_catalog_generation,proof_requested,proof_desired,proof_observed,
 proof_verified,proof_applied,proof_source_authority,proof_source_revision,proof_catalog_revision,
 proof_change_id,proof_operation_id,proof_presentation_kind,
 proof_tenant_id,proof_domain_id,proof_presentation_generation,proof_activation_generation,proof_public_path,
 previous_config_dir,previous_keychain_service,previous_keychain_account,
 previous_locator_digest,previous_credential_state,previous_credential_digest,
 publication_pending,committed_at,acknowledged_at,expires_at`

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
	var previousLocator, previousCredential []byte
	var resolutionObserved []byte
	var credentialWritten, publicationPending int
	var committedAt, expiresAt int64
	var acknowledgedAt, resolvedAt sql.NullInt64
	var quarantineReason, resolution sql.NullString
	if err := row.Scan(
		&operationID, &receipt.AccountID, &receipt.Kind, &receipt.RegistrySequence,
		&receipt.AccountInstanceID, &receipt.AccountGeneration, &locator, &expected, &intent,
		&input, &written, &credentialWritten, &outcome, &receipt.Terminal,
		&quarantineReason, &resolution, &resolutionObserved, &resolvedAt,
		&receipt.ConfigDir,
		&receipt.KeychainService, &receipt.KeychainAccount, &receipt.Label, &receipt.AccountUUID,
		&owner, &receipt.OwnerEpoch,
		&receipt.PresentationProof.CatalogTenantID, &receipt.PresentationProof.CatalogGeneration,
		&receipt.PresentationProof.Requested, &receipt.PresentationProof.Desired,
		&receipt.PresentationProof.Observed, &receipt.PresentationProof.Verified,
		&receipt.PresentationProof.Applied, &receipt.PresentationProof.SourceAuthority,
		&receipt.PresentationProof.SourceRevision, &receipt.PresentationProof.CatalogRevision,
		&receipt.PresentationProof.ChangeID, &receipt.PresentationProof.OperationID,
		&receipt.PresentationProof.PresentationKind,
		&receipt.PresentationProof.FileProvider.TenantID,
		&receipt.PresentationProof.FileProvider.DomainID,
		&receipt.PresentationProof.FileProvider.Generation,
		&receipt.PresentationProof.FileProvider.ActivationGeneration,
		&receipt.PresentationProof.FileProvider.PublicPath,
		&receipt.PreviousConfigDir, &receipt.PreviousKeychainService,
		&receipt.PreviousKeychainAccount, &previousLocator, &receipt.PreviousCredentialState,
		&previousCredential,
		&publicationPending, &committedAt, &acknowledgedAt, &expiresAt,
	); err != nil {
		return receipt, err
	}
	if len(operationID) != 32 || len(locator) != 32 || len(expected) != 32 ||
		len(intent) != 32 || len(outcome) != 32 || len(previousLocator) != 32 ||
		(previousCredential != nil && len(previousCredential) != 32) {
		return receipt, ErrAccountMutationState
	}
	copy(receipt.OperationID[:], operationID)
	copy(receipt.LocatorDigest[:], locator)
	copy(receipt.ExpectedCredentialDigest[:], expected)
	copy(receipt.IntentDigest[:], intent)
	copy(receipt.PreviousLocatorDigest[:], previousLocator)
	if previousCredential != nil {
		copy(receipt.PreviousCredentialDigest[:], previousCredential)
	}
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
	if quarantineReason.Valid {
		receipt.QuarantineReason = CredentialResultCategory(quarantineReason.String)
		receipt.HasQuarantine = true
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
	receipt.PublicationPending = publicationPending != 0
	receipt.HasPresentationProof = receipt.PresentationProof.PresentationKind != ""
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
	var quarantineReason any
	if quarantine != nil {
		quarantineReason = quarantine.Reason
	}
	_, err := tx.Exec(
		`INSERT INTO account_mutation_receipts(
		 operation_id,account_id,kind,registry_sequence,
		 account_instance_id,account_generation,locator_digest,expected_credential_digest,intent_digest,
		 input_digest,written_credential_digest,credential_written,outcome_digest,terminal,
		 quarantine_reason,resolution,resolution_observed_digest,resolved_at,
		 config_dir,keychain_service,keychain_account,label,account_uuid,owner_record,owner_epoch,
		 proof_catalog_tenant_id,proof_catalog_generation,proof_requested,proof_desired,proof_observed,
		 proof_verified,proof_applied,proof_source_authority,proof_source_revision,proof_catalog_revision,
		 proof_change_id,proof_operation_id,proof_presentation_kind,
		 proof_tenant_id,proof_domain_id,proof_presentation_generation,proof_activation_generation,proof_public_path,
		 previous_config_dir,previous_keychain_service,previous_keychain_account,
		 previous_locator_digest,previous_credential_state,previous_credential_digest,
		 publication_pending,committed_at,acknowledged_at,expires_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)`,
		mutation.OperationID[:], mutation.AccountID, mutation.Kind, mutation.RegistrySequence,
		mutation.AccountInstanceID, mutation.AccountGeneration, mutation.LocatorDigest[:],
		mutation.ExpectedCredentialDigest[:], mutation.IntentDigest[:], input, written, mutation.CredentialWritten,
		outcome[:], terminal, quarantineReason,
		mutation.ConfigDir, mutation.KeychainService, mutation.KeychainAccount,
		mutation.Label, mutation.AccountUUID, mustEncodeCredentialOwner(mutation.Owner), mutation.OwnerEpoch,
		mutation.PresentationProof.CatalogTenantID, mutation.PresentationProof.CatalogGeneration,
		mutation.PresentationProof.Requested, mutation.PresentationProof.Desired,
		mutation.PresentationProof.Observed, mutation.PresentationProof.Verified,
		mutation.PresentationProof.Applied, mutation.PresentationProof.SourceAuthority,
		mutation.PresentationProof.SourceRevision, mutation.PresentationProof.CatalogRevision,
		mutation.PresentationProof.ChangeID, mutation.PresentationProof.OperationID,
		mutation.PresentationProof.PresentationKind,
		mutation.PresentationProof.FileProvider.TenantID,
		mutation.PresentationProof.FileProvider.DomainID,
		mutation.PresentationProof.FileProvider.Generation,
		mutation.PresentationProof.FileProvider.ActivationGeneration,
		mutation.PresentationProof.FileProvider.PublicPath,
		mutation.PreviousConfigDir, mutation.PreviousKeychainService, mutation.PreviousKeychainAccount,
		mutation.PreviousLocatorDigest[:], mutation.PreviousCredentialState,
		optionalPreviousCredentialDigest(mutation.PreviousCredentialState, mutation.PreviousCredentialDigest),
		terminal == AccountMutationCommitted &&
			(mutation.Kind == AccountMutationAdd || mutation.Kind == AccountMutationPresentationRebind),
		committedAt.UnixNano(), expiresAt.UnixNano(),
	)
	return err
}

func validateAccountMutationRequest(request BeginAccountMutationRequest) error {
	if request.AccountID <= 0 || !request.Kind.valid() ||
		validateAccountInstanceID(request.AccountInstanceID) != nil ||
		request.AccountGeneration == 0 || request.IntentDigest.zero() {
		return ErrAccountMutationState
	}
	if err := request.Owner.Validate(); err != nil {
		return err
	}
	var expectedID AccountMutationID
	var err error
	previousEmpty := request.PreviousConfigDir == "" && request.PreviousKeychainService == "" &&
		request.PreviousKeychainAccount == "" && request.PreviousLocatorDigest.zero() &&
		request.PreviousCredentialState == "" && request.PreviousCredentialDigest.zero()
	if request.Kind == AccountMutationAdd {
		if request.ConfigDir != "" || request.KeychainService != "" || request.KeychainAccount != "" ||
			!request.LocatorDigest.zero() || !request.ExpectedCredentialDigest.zero() || !previousEmpty {
			return ErrAccountMutationState
		}
		expectedID, err = NewPendingAddMutationID(
			request.AccountID, request.AccountInstanceID, request.AccountGeneration, request.IntentDigest,
		)
	} else if request.Kind == AccountMutationPresentationRebind {
		if request.AccountGeneration < 2 || request.ConfigDir != "" || request.KeychainService != "" ||
			request.KeychainAccount != "" || !request.LocatorDigest.zero() ||
			!request.ExpectedCredentialDigest.zero() || request.PreviousConfigDir == "" ||
			request.PreviousKeychainService == "" || request.PreviousKeychainAccount == "" ||
			request.PreviousLocatorDigest.zero() ||
			!validPreviousCredential(request.PreviousCredentialState, request.PreviousCredentialDigest) {
			return ErrAccountMutationState
		}
		if request.PreviousLocatorDigest != CredentialKeychainLocatorDigest(
			request.PreviousKeychainService, request.PreviousKeychainAccount,
		) {
			return ErrAccountMutationState
		}
		expectedID, err = NewPresentationRebindMutationID(
			request.AccountID, request.AccountInstanceID, request.AccountGeneration,
			request.PreviousLocatorDigest, request.PreviousCredentialState,
			request.PreviousCredentialDigest, request.IntentDigest,
		)
	} else {
		if request.ConfigDir == "" || request.KeychainService == "" || request.KeychainAccount == "" ||
			request.LocatorDigest.zero() || request.ExpectedCredentialDigest.zero() || !previousEmpty {
			return ErrAccountMutationState
		}
		expectedID, err = NewAccountMutationID(
			request.AccountID, request.AccountInstanceID, request.AccountGeneration,
			request.Kind, request.LocatorDigest, request.ExpectedCredentialDigest, request.IntentDigest,
		)
	}
	if err != nil || request.OperationID != expectedID {
		return ErrAccountMutationState
	}
	return nil
}

func validateAccountMutation(mutation AccountMutation) error {
	if mutation.OperationID == (AccountMutationID{}) || mutation.AccountID <= 0 ||
		!mutation.Kind.valid() || !mutation.State.valid() || mutation.RegistrySequence == 0 ||
		validateAccountInstanceID(mutation.AccountInstanceID) != nil || mutation.AccountGeneration == 0 ||
		mutation.IntentDigest.zero() ||
		mutation.OwnerEpoch == 0 || mutation.CreatedAt.IsZero() || mutation.UpdatedAt.Before(mutation.CreatedAt) {
		return ErrAccountMutationState
	}
	if mutation.State == AccountMutationAwaitingPresentation {
		if (mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationPresentationRebind) ||
			!mutation.LocatorDigest.zero() ||
			!mutation.ExpectedCredentialDigest.zero() || mutation.ConfigDir != "" ||
			mutation.KeychainService != "" || mutation.KeychainAccount != "" ||
			mutation.HasPresentationProof {
			return ErrAccountMutationState
		}
	} else if mutation.LocatorDigest.zero() || mutation.ExpectedCredentialDigest.zero() ||
		mutation.ConfigDir == "" || mutation.KeychainService == "" || mutation.KeychainAccount == "" {
		return ErrAccountMutationState
	}
	if mutation.Kind == AccountMutationPresentationRebind {
		if mutation.PreviousConfigDir == "" || mutation.PreviousKeychainService == "" ||
			mutation.PreviousKeychainAccount == "" || mutation.PreviousLocatorDigest.zero() ||
			!validPreviousCredential(mutation.PreviousCredentialState, mutation.PreviousCredentialDigest) {
			return ErrAccountMutationState
		}
	} else if mutation.PreviousConfigDir != "" || mutation.PreviousKeychainService != "" ||
		mutation.PreviousKeychainAccount != "" || !mutation.PreviousLocatorDigest.zero() ||
		mutation.PreviousCredentialState != "" || !mutation.PreviousCredentialDigest.zero() {
		return ErrAccountMutationState
	}
	if (mutation.Kind == AccountMutationAdd || mutation.Kind == AccountMutationPresentationRebind) &&
		mutation.State != AccountMutationAwaitingPresentation {
		if !mutation.HasPresentationProof || validatePresentationPreparationProofForAccount(
			mutation.PresentationProof, mutation.AccountInstanceID, mutation.AccountGeneration,
			mutation.ConfigDir,
		) != nil {
			return ErrAccountMutationState
		}
	} else if mutation.Kind != AccountMutationAdd && mutation.Kind != AccountMutationPresentationRebind &&
		mutation.HasPresentationProof {
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
	if receipt.Kind == AccountMutationPresentationRebind {
		if receipt.PreviousConfigDir == "" || receipt.PreviousKeychainService == "" ||
			receipt.PreviousKeychainAccount == "" || receipt.PreviousLocatorDigest.zero() ||
			!validPreviousCredential(receipt.PreviousCredentialState, receipt.PreviousCredentialDigest) {
			return ErrAccountMutationState
		}
	} else if receipt.PreviousConfigDir != "" || receipt.PreviousKeychainService != "" ||
		receipt.PreviousKeychainAccount != "" || !receipt.PreviousLocatorDigest.zero() ||
		receipt.PreviousCredentialState != "" || !receipt.PreviousCredentialDigest.zero() {
		return ErrAccountMutationState
	}
	if receipt.Kind == AccountMutationAdd || receipt.Kind == AccountMutationPresentationRebind {
		if !receipt.HasPresentationProof || validatePresentationPreparationProofForAccount(
			receipt.PresentationProof, receipt.AccountInstanceID, receipt.AccountGeneration,
			receipt.ConfigDir,
		) != nil {
			return ErrAccountMutationState
		}
	} else if receipt.HasPresentationProof {
		return ErrAccountMutationState
	}
	if receipt.PublicationPending &&
		(receipt.Terminal != AccountMutationCommitted ||
			(receipt.Kind != AccountMutationAdd && receipt.Kind != AccountMutationPresentationRebind) ||
			!receipt.AcknowledgedAt.IsZero()) {
		return ErrAccountMutationState
	}
	if (receipt.Terminal == AccountMutationQuarantined) != receipt.HasQuarantine ||
		(receipt.HasQuarantine && !receipt.QuarantineReason.quarantine()) {
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
	case AccountMutationAdd, AccountMutationRelogin, AccountMutationPresentationRebind:
		return true
	default:
		return false
	}
}

func (state AccountMutationState) valid() bool {
	switch state {
	case AccountMutationAwaitingPresentation, AccountMutationAwaitingInput,
		AccountMutationReserved, AccountMutationApplying,
		AccountMutationApplied, AccountMutationPublishing, AccountMutationCompensating,
		AccountMutationRebindPublished:
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
	if mutation.Kind == AccountMutationAdd && request.Kind == AccountMutationAdd {
		return mutation.AccountInstanceID == request.AccountInstanceID &&
			mutation.AccountGeneration == request.AccountGeneration &&
			mutation.IntentDigest == request.IntentDigest
	}
	if mutation.Kind == AccountMutationPresentationRebind &&
		request.Kind == AccountMutationPresentationRebind {
		return mutation.AccountInstanceID == request.AccountInstanceID &&
			mutation.AccountGeneration == request.AccountGeneration &&
			mutation.IntentDigest == request.IntentDigest &&
			mutation.PreviousConfigDir == request.PreviousConfigDir &&
			mutation.PreviousKeychainService == request.PreviousKeychainService &&
			mutation.PreviousKeychainAccount == request.PreviousKeychainAccount &&
			mutation.PreviousLocatorDigest == request.PreviousLocatorDigest &&
			mutation.PreviousCredentialState == request.PreviousCredentialState &&
			mutation.PreviousCredentialDigest == request.PreviousCredentialDigest
	}
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
	if receipt.Kind == AccountMutationAdd && request.Kind == AccountMutationAdd {
		return receipt.OperationID == request.OperationID && receipt.AccountID == request.AccountID &&
			receipt.AccountInstanceID == request.AccountInstanceID &&
			receipt.AccountGeneration == request.AccountGeneration &&
			receipt.IntentDigest == request.IntentDigest
	}
	if receipt.Kind == AccountMutationPresentationRebind &&
		request.Kind == AccountMutationPresentationRebind {
		return receipt.OperationID == request.OperationID && receipt.AccountID == request.AccountID &&
			receipt.AccountInstanceID == request.AccountInstanceID &&
			receipt.AccountGeneration == request.AccountGeneration &&
			receipt.IntentDigest == request.IntentDigest &&
			receipt.PreviousConfigDir == request.PreviousConfigDir &&
			receipt.PreviousKeychainService == request.PreviousKeychainService &&
			receipt.PreviousKeychainAccount == request.PreviousKeychainAccount &&
			receipt.PreviousLocatorDigest == request.PreviousLocatorDigest &&
			receipt.PreviousCredentialState == request.PreviousCredentialState &&
			receipt.PreviousCredentialDigest == request.PreviousCredentialDigest
	}
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
