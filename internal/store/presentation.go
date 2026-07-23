package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"
)

var (
	// ErrAccountPresentationEvidence reports malformed or inconsistent proof evidence.
	ErrAccountPresentationEvidence = errors.New("account presentation evidence is invalid")
	// ErrAccountPresentationQuarantined reports durable presentation identity drift.
	ErrAccountPresentationQuarantined = errors.New("account presentation is quarantined")
	// ErrAccountPresentationBusy reports live state that prevents an explicit rebind.
	ErrAccountPresentationBusy = errors.New("account presentation is busy")
)

// FileProviderPreparationProof is the exact File Provider arm of one preparation proof.
type FileProviderPreparationProof struct {
	TenantID             string
	DomainID             string
	Generation           uint64
	ActivationGeneration string
	PublicPath           string
}

// PresentationKindFileProvider identifies the sole account presentation kind.
const PresentationKindFileProvider = "file_provider"

// PresentationPreparationProof is the product-owned projection of one exact
// FuseKit tenant preparation proof.
type PresentationPreparationProof struct {
	CatalogTenantID   string
	CatalogGeneration uint64
	Requested         uint64
	Desired           uint64
	Observed          uint64
	Verified          uint64
	Applied           uint64
	SourceAuthority   string
	SourceRevision    uint64
	CatalogRevision   uint64
	ChangeID          string
	OperationID       string
	PresentationKind  string
	FileProvider      FileProviderPreparationProof
}

// AccountPresentation is the last exact proof bound to an account generation.
type AccountPresentation struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	Proof             PresentationPreparationProof
	ObservedAt        time.Time
}

// AccountPresentationQuarantineReason classifies exact presentation identity drift.
type AccountPresentationQuarantineReason string

const (
	// AccountPresentationPublicPathDrift reports a changed public path.
	AccountPresentationPublicPathDrift AccountPresentationQuarantineReason = "public-path-drift"
	// AccountPresentationTenantIDDrift reports a changed tenant identity.
	AccountPresentationTenantIDDrift AccountPresentationQuarantineReason = "tenant-id-drift"
	// AccountPresentationDomainIDDrift reports a changed File Provider domain.
	AccountPresentationDomainIDDrift AccountPresentationQuarantineReason = "domain-id-drift"
	// AccountPresentationGenerationDrift reports a changed presentation generation.
	AccountPresentationGenerationDrift AccountPresentationQuarantineReason = "generation-drift"
)

// AccountPresentationQuarantine preserves one immutable unsafe observation.
type AccountPresentationQuarantine struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	ExpectedConfigDir string
	Proof             PresentationPreparationProof
	Reason            AccountPresentationQuarantineReason
	CreatedAt         time.Time
}

// ObserveAccountPresentation binds matching proof evidence or durably quarantines drift.
func (s *Store) ObserveAccountPresentation(account Account, proof PresentationPreparationProof) error {
	if err := validatePresentationPreparationProof(proof); err != nil {
		return err
	}
	fileProvider := proof.FileProvider
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return err
	}
	if !samePresentationAccount(current, account) {
		return ErrAccountGenerationChanged
	}
	if _, err := accountPresentationQuarantine(tx, account.ID); err == nil {
		return ErrAccountPresentationQuarantined
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	bound, err := accountPresentation(tx, account.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAccountPresentationEvidence
	}
	if err != nil {
		return err
	}
	reason := presentationMismatch(current, bound, fileProvider)
	if reason != "" {
		quarantine := AccountPresentationQuarantine{
			AccountID: current.ID, AccountInstanceID: current.InstanceID,
			AccountGeneration: current.Generation, ExpectedConfigDir: current.ConfigDir,
			Proof: proof, Reason: reason, CreatedAt: s.now(),
		}
		if err := insertAccountPresentationQuarantine(tx, quarantine); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return ErrAccountPresentationQuarantined
	}
	if err := ValidatePresentationPreparationProofAdvance(bound.Proof, proof); err != nil {
		return err
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: current.ID, AccountInstanceID: current.InstanceID,
		AccountGeneration: current.Generation, Proof: proof, ObservedAt: s.now(),
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// AccountPresentation returns the current proof binding for one account.
func (s *Store) AccountPresentation(accountID int) (AccountPresentation, error) {
	return accountPresentation(s.db, accountID)
}

// AccountPresentationQuarantine returns one durable presentation quarantine.
func (s *Store) AccountPresentationQuarantine(accountID int) (AccountPresentationQuarantine, error) {
	return accountPresentationQuarantine(s.db, accountID)
}

// RefreshAccountPresentation atomically advances one admitted binding while
// its account identity and previously retained proof still match.
func (s *Store) RefreshAccountPresentation(
	account Account,
	currentProof PresentationPreparationProof,
	freshProof PresentationPreparationProof,
) error {
	if err := validatePresentationPreparationProofForAccount(
		freshProof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return err
	}
	if err := ValidatePresentationPreparationProofAdvance(currentProof, freshProof); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return err
	}
	if !samePresentationAccount(current, account) {
		return ErrAccountGenerationChanged
	}
	if _, err := accountPresentationQuarantine(tx, account.ID); err == nil {
		return ErrAccountPresentationQuarantined
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil {
		return err
	}
	if bound.AccountInstanceID != account.InstanceID ||
		bound.AccountGeneration != account.Generation || bound.Proof != currentProof {
		return ErrAccountPresentationEvidence
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, Proof: freshProof, ObservedAt: s.now(),
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// StageSyncedAccountAdmission durably records exact external evidence and
// advances the presentation while retaining awaiting-origin state.
func (s *Store) StageSyncedAccountAdmission(
	account Account,
	currentProof PresentationPreparationProof,
	freshProof PresentationPreparationProof,
	credential SyncedCredentialAdmissionFence,
) (SyncedCredentialAdmissionStage, error) {
	if err := validatePresentationPreparationProofForAccount(
		freshProof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if err := ValidatePresentationPreparationProofAdvance(currentProof, freshProof); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if err := credential.validate(account); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if !samePresentationAccount(current, account) {
		return SyncedCredentialAdmissionStage{}, ErrAccountGenerationChanged
	}
	if err := validateSyncedAdmissionGuards(tx, account.ID); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	health, err := syncedAdmissionAuth(tx, account.ID)
	if err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if final, finalErr := syncedCredentialAdmissionTx(tx, account); finalErr == nil &&
		sameSyncedCredentialAdmissionFence(final.SyncedCredentialAdmissionFence, credential) &&
		bound.Proof == freshProof && health.healthyOwned() {
		return SyncedCredentialAdmissionStage{
			AccountID: account.ID, SyncedCredentialAdmissionFence: credential,
			StagedAt: final.AdmittedAt, Finalized: true,
		}, nil
	} else if finalErr != nil && !errors.Is(finalErr, sql.ErrNoRows) {
		return SyncedCredentialAdmissionStage{}, finalErr
	}
	if pending, pendingErr := pendingSyncedCredentialAdmissionTx(tx, account); pendingErr == nil &&
		sameSyncedCredentialAdmissionFence(pending.SyncedCredentialAdmissionFence, credential) &&
		bound.Proof == freshProof && health.awaitingOrigin() {
		return pending, nil
	} else if pendingErr != nil && !errors.Is(pendingErr, sql.ErrNoRows) {
		return SyncedCredentialAdmissionStage{}, pendingErr
	}
	if bound.AccountInstanceID != account.InstanceID ||
		bound.AccountGeneration != account.Generation || bound.Proof != currentProof ||
		!health.awaitingOrigin() {
		return SyncedCredentialAdmissionStage{}, ErrAccountPresentationEvidence
	}
	now := s.now()
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, Proof: freshProof, ObservedAt: now,
	}); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if _, err := tx.Exec(`DELETE FROM synced_credential_admissions WHERE account_id=?`, account.ID); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if _, err := tx.Exec(
		`INSERT INTO pending_synced_credential_admissions(
		 account_id,account_instance_id,account_generation,locator_digest,
		 external_state_digest,token_chain_digest,access_hash_digest,staged_at,candidate_at)
		 VALUES(?,?,?,?,?,?,?,?,0)
		 ON CONFLICT(account_id) DO UPDATE SET
		 account_instance_id=excluded.account_instance_id,
		 account_generation=excluded.account_generation,
		 locator_digest=excluded.locator_digest,
		 external_state_digest=excluded.external_state_digest,
		 token_chain_digest=excluded.token_chain_digest,
		 access_hash_digest=excluded.access_hash_digest,
		 staged_at=excluded.staged_at,
		 candidate_at=0`,
		account.ID, account.InstanceID, account.Generation, credential.LocatorDigest[:],
		credential.ExternalStateDigest[:], credential.TokenChainDigest[:],
		credential.AccessHashDigest[:], now.UnixNano(),
	); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if err := tx.Commit(); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	return SyncedCredentialAdmissionStage{
		AccountID: account.ID, SyncedCredentialAdmissionFence: credential,
		StagedAt: now,
	}, nil
}

// CommitSyncedAccountAdmissionCandidate persists exact candidate evidence while
// retaining the pending admission fence and awaiting-origin state.
func (s *Store) CommitSyncedAccountAdmissionCandidate(
	account Account,
	freshProof PresentationPreparationProof,
	credential SyncedCredentialAdmissionFence,
) (bool, error) {
	if err := validatePresentationPreparationProofForAccount(
		freshProof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return false, err
	}
	if err := credential.validate(account); err != nil {
		return false, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return false, err
	}
	if !samePresentationAccount(current, account) {
		return false, ErrAccountGenerationChanged
	}
	if err := validateSyncedAdmissionGuards(tx, account.ID); err != nil {
		return false, err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil {
		return false, err
	}
	health, err := syncedAdmissionAuth(tx, account.ID)
	if err != nil {
		return false, err
	}
	pending, err := pendingSyncedCredentialAdmissionTx(tx, account)
	if err != nil || !sameSyncedCredentialAdmissionFence(
		pending.SyncedCredentialAdmissionFence, credential,
	) || bound.Proof != freshProof || !health.awaitingOrigin() {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		return false, ErrAccountPresentationEvidence
	}
	if !pending.CandidateAt.IsZero() {
		if _, err := syncedCredentialAdmissionTx(tx, account); err == nil {
			return false, ErrAccountPresentationEvidence
		} else if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		return true, nil
	}
	if _, err := syncedCredentialAdmissionTx(tx, account); err == nil {
		return false, ErrAccountPresentationEvidence
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	now := s.now()
	if now.Before(pending.StagedAt) {
		now = pending.StagedAt
	}
	updated, err := tx.Exec(
		`UPDATE pending_synced_credential_admissions SET candidate_at=?
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=? AND external_state_digest=?
		 AND token_chain_digest=? AND access_hash_digest=? AND candidate_at=0`,
		now.UnixNano(), account.ID, account.InstanceID, account.Generation,
		credential.LocatorDigest[:], credential.ExternalStateDigest[:],
		credential.TokenChainDigest[:], credential.AccessHashDigest[:],
	)
	if err != nil {
		return false, err
	}
	if rows, _ := updated.RowsAffected(); rows != 1 {
		return false, ErrAccountPresentationEvidence
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// SettleSyncedAccountAdmission consumes an exact confirmed candidate and is the
// sole transition that clears awaiting-origin state.
func (s *Store) SettleSyncedAccountAdmission(
	account Account,
	freshProof PresentationPreparationProof,
	credential SyncedCredentialAdmissionFence,
) (bool, error) {
	if err := validatePresentationPreparationProofForAccount(
		freshProof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return false, err
	}
	if err := credential.validate(account); err != nil {
		return false, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return false, err
	}
	if !samePresentationAccount(current, account) {
		return false, ErrAccountGenerationChanged
	}
	if err := validateSyncedAdmissionGuards(tx, account.ID); err != nil {
		return false, err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil || bound.Proof != freshProof {
		if err != nil {
			return false, err
		}
		return false, ErrAccountPresentationEvidence
	}
	health, err := syncedAdmissionAuth(tx, account.ID)
	if err != nil {
		return false, err
	}
	final, finalErr := syncedCredentialAdmissionTx(tx, account)
	pending, pendingErr := pendingSyncedCredentialAdmissionTx(tx, account)
	if health.healthyOwned() && errors.Is(pendingErr, sql.ErrNoRows) && finalErr == nil &&
		sameSyncedCredentialAdmissionFence(final.SyncedCredentialAdmissionFence, credential) {
		return true, nil
	}
	if pendingErr != nil || !health.awaitingOrigin() || pending.CandidateAt.IsZero() ||
		!sameSyncedCredentialAdmissionFence(pending.SyncedCredentialAdmissionFence, credential) ||
		!errors.Is(finalErr, sql.ErrNoRows) {
		if pendingErr != nil && !errors.Is(pendingErr, sql.ErrNoRows) {
			return false, pendingErr
		}
		if finalErr != nil && !errors.Is(finalErr, sql.ErrNoRows) {
			return false, finalErr
		}
		return false, ErrAccountPresentationEvidence
	}
	now := s.now()
	if now.Before(pending.CandidateAt) {
		now = pending.CandidateAt
	}
	if _, err := tx.Exec(
		`INSERT INTO synced_credential_admissions(
		 account_id,account_instance_id,account_generation,locator_digest,
		 external_state_digest,token_chain_digest,access_hash_digest,admitted_at)
		 VALUES(?,?,?,?,?,?,?,?)`,
		account.ID, account.InstanceID, account.Generation, credential.LocatorDigest[:],
		credential.ExternalStateDigest[:], credential.TokenChainDigest[:],
		credential.AccessHashDigest[:], now.UnixNano(),
	); err != nil {
		return false, err
	}
	deleted, err := tx.Exec(
		`DELETE FROM pending_synced_credential_admissions
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=? AND external_state_digest=?
		 AND token_chain_digest=? AND access_hash_digest=? AND candidate_at=?`,
		account.ID, account.InstanceID, account.Generation, credential.LocatorDigest[:],
		credential.ExternalStateDigest[:], credential.TokenChainDigest[:],
		credential.AccessHashDigest[:], pending.CandidateAt.UnixNano(),
	)
	if err != nil {
		return false, err
	}
	if rows, _ := deleted.RowsAffected(); rows != 1 {
		return false, ErrAccountPresentationEvidence
	}
	result, err := tx.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, reason='none',
		 digest=zeroblob(32), kind='owned'
		 WHERE account_id=? AND needs_login=1 AND kind='awaiting_origin'
		 AND reason='awaiting_origin'`,
		account.ID,
	)
	if err != nil {
		return false, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return false, ErrAccountPresentationEvidence
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// RejectSyncedAccountAdmissionCandidate removes an unconfirmed candidate while
// retaining its pending liability and awaiting-origin state.
func (s *Store) RejectSyncedAccountAdmissionCandidate(
	account Account,
	freshProof PresentationPreparationProof,
	credential SyncedCredentialAdmissionFence,
) error {
	if err := validatePresentationPreparationProofForAccount(
		freshProof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return err
	}
	if err := credential.validate(account); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return err
	}
	if !samePresentationAccount(current, account) {
		return ErrAccountGenerationChanged
	}
	locked, err := tx.Exec(
		`UPDATE accounts SET id=id
		 WHERE id=? AND instance_id=? AND generation=? AND deleted_at IS NULL`,
		account.ID, account.InstanceID, account.Generation,
	)
	if err != nil {
		return err
	}
	if rows, _ := locked.RowsAffected(); rows != 1 {
		return ErrAccountGenerationChanged
	}
	if err := validateSyncedAdmissionInvalidationGuards(tx, account.ID); err != nil {
		return err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil || bound.Proof != freshProof {
		if err != nil {
			return err
		}
		return ErrAccountPresentationEvidence
	}
	health, err := syncedAdmissionAuth(tx, account.ID)
	if err != nil {
		return err
	}
	pending, err := pendingSyncedCredentialAdmissionTx(tx, account)
	if err != nil || !sameSyncedCredentialAdmissionFence(
		pending.SyncedCredentialAdmissionFence, credential,
	) || !health.awaitingOrigin() {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return ErrAccountPresentationEvidence
	}
	if pending.CandidateAt.IsZero() {
		if _, err := syncedCredentialAdmissionTx(tx, account); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		return ErrAccountPresentationEvidence
	}
	if _, err := syncedCredentialAdmissionTx(tx, account); err == nil {
		return ErrAccountPresentationEvidence
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	updated, err := tx.Exec(
		`UPDATE pending_synced_credential_admissions SET candidate_at=0
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=? AND external_state_digest=?
		 AND token_chain_digest=? AND access_hash_digest=? AND candidate_at=?`,
		account.ID, account.InstanceID, account.Generation, credential.LocatorDigest[:],
		credential.ExternalStateDigest[:], credential.TokenChainDigest[:],
		credential.AccessHashDigest[:], pending.CandidateAt.UnixNano(),
	)
	if err != nil {
		return err
	}
	if rows, _ := updated.RowsAffected(); rows != 1 {
		return ErrAccountPresentationEvidence
	}
	return tx.Commit()
}

// InvalidateSyncedAccountAdmission atomically restores settled evidence to
// pending liability after a confirmed external credential change.
func (s *Store) InvalidateSyncedAccountAdmission(
	account Account,
	proof PresentationPreparationProof,
	credential SyncedCredentialAdmissionFence,
) error {
	if err := validatePresentationPreparationProofForAccount(
		proof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return err
	}
	if err := credential.validate(account); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return err
	}
	if !samePresentationAccount(current, account) {
		return ErrAccountGenerationChanged
	}
	locked, err := tx.Exec(
		`UPDATE accounts SET id=id
		 WHERE id=? AND instance_id=? AND generation=? AND deleted_at IS NULL`,
		account.ID, account.InstanceID, account.Generation,
	)
	if err != nil {
		return err
	}
	if rows, _ := locked.RowsAffected(); rows != 1 {
		return ErrAccountGenerationChanged
	}
	if err := validateSyncedAdmissionInvalidationGuards(tx, account.ID); err != nil {
		return err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil || bound.Proof != proof {
		if err != nil {
			return err
		}
		return ErrAccountPresentationEvidence
	}
	health, err := syncedAdmissionAuth(tx, account.ID)
	if err != nil {
		return err
	}
	if pending, pendingErr := pendingSyncedCredentialAdmissionTx(tx, account); pendingErr == nil &&
		sameSyncedCredentialAdmissionFence(pending.SyncedCredentialAdmissionFence, credential) &&
		health.awaitingOrigin() {
		return nil
	} else if pendingErr != nil && !errors.Is(pendingErr, sql.ErrNoRows) {
		return pendingErr
	} else if pendingErr == nil {
		return ErrAccountPresentationEvidence
	}
	final, err := syncedCredentialAdmissionTx(tx, account)
	if err != nil || !sameSyncedCredentialAdmissionFence(
		final.SyncedCredentialAdmissionFence, credential,
	) || !health.healthyOwned() {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return ErrAccountPresentationEvidence
	}
	now := s.now()
	if _, err := tx.Exec(
		`INSERT INTO pending_synced_credential_admissions(
		 account_id,account_instance_id,account_generation,locator_digest,
		 external_state_digest,token_chain_digest,access_hash_digest,staged_at,candidate_at)
		 VALUES(?,?,?,?,?,?,?,?,0)`,
		account.ID, account.InstanceID, account.Generation, credential.LocatorDigest[:],
		credential.ExternalStateDigest[:], credential.TokenChainDigest[:],
		credential.AccessHashDigest[:], now.UnixNano(),
	); err != nil {
		return err
	}
	deleted, err := tx.Exec(
		`DELETE FROM synced_credential_admissions
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND locator_digest=? AND external_state_digest=?
		 AND token_chain_digest=? AND access_hash_digest=? AND admitted_at=?`,
		account.ID, account.InstanceID, account.Generation, credential.LocatorDigest[:],
		credential.ExternalStateDigest[:], credential.TokenChainDigest[:],
		credential.AccessHashDigest[:], final.AdmittedAt.UnixNano(),
	)
	if err != nil {
		return err
	}
	if rows, _ := deleted.RowsAffected(); rows != 1 {
		return ErrAccountPresentationEvidence
	}
	digest := DigestReason("host-sync: settled credential changed externally")
	updated, err := tx.Exec(
		`UPDATE auth_health SET needs_login=1,since=?,reason='awaiting_origin',
		 digest=?,kind='awaiting_origin',gen=gen+1
		 WHERE account_id=? AND needs_login=0 AND reason='none' AND kind='owned'`,
		now.Unix(), digest[:], account.ID,
	)
	if err != nil {
		return err
	}
	if rows, _ := updated.RowsAffected(); rows != 1 {
		return ErrAccountPresentationEvidence
	}
	return tx.Commit()
}

type syncedAdmissionAuthState struct {
	needsLogin bool
	reason     AuthReasonCategory
	kind       AuthKind
}

func (state syncedAdmissionAuthState) awaitingOrigin() bool {
	return state.needsLogin && state.reason == AuthReasonAwaitingOrigin && state.kind == AuthKindAwaitingOrigin
}

func (state syncedAdmissionAuthState) healthyOwned() bool {
	return !state.needsLogin && state.reason == AuthReasonNone && state.kind == AuthKindOwned
}

func syncedAdmissionAuth(tx *sql.Tx, accountID int) (syncedAdmissionAuthState, error) {
	var needsLogin int
	var reason string
	var kind string
	if err := tx.QueryRow(
		`SELECT needs_login,reason,kind FROM auth_health WHERE account_id=?`, accountID,
	).Scan(&needsLogin, &reason, &kind); err != nil {
		return syncedAdmissionAuthState{}, err
	}
	return syncedAdmissionAuthState{
		needsLogin: needsLogin != 0, reason: AuthReasonCategory(reason), kind: AuthKind(kind),
	}, nil
}

func validateSyncedAdmissionGuards(tx *sql.Tx, accountID int) error {
	if _, err := accountPresentationQuarantine(tx, accountID); err == nil {
		return ErrAccountPresentationQuarantined
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var liveSession int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM sessions WHERE account_id=? AND ended_at IS NULL)`, accountID,
	).Scan(&liveSession); err != nil {
		return err
	}
	if liveSession != 0 {
		return ErrAccountSessionActive
	}
	var busy int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM account_mutations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_quarantines WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_removals WHERE account_id=?)`,
		accountID, accountID, accountID, accountID,
	).Scan(&busy); err != nil {
		return err
	}
	if busy != 0 {
		return ErrAccountPresentationBusy
	}
	return nil
}

func validateSyncedAdmissionInvalidationGuards(tx *sql.Tx, accountID int) error {
	if _, err := accountPresentationQuarantine(tx, accountID); err == nil {
		return ErrAccountPresentationQuarantined
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var busy int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM account_mutations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_quarantines WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_removals WHERE account_id=?)`,
		accountID, accountID, accountID, accountID,
	).Scan(&busy); err != nil {
		return err
	}
	if busy != 0 {
		return ErrAccountPresentationBusy
	}
	return nil
}

func pendingSyncedCredentialAdmissionTx(
	tx *sql.Tx,
	account Account,
) (SyncedCredentialAdmissionStage, error) {
	row := tx.QueryRow(
		`SELECT locator_digest,external_state_digest,token_chain_digest,
		 access_hash_digest,staged_at,candidate_at
		 FROM pending_synced_credential_admissions
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		account.ID, account.InstanceID, account.Generation,
	)
	result := SyncedCredentialAdmissionStage{
		AccountID: account.ID,
		SyncedCredentialAdmissionFence: SyncedCredentialAdmissionFence{
			AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		},
	}
	var locator, external, tokenChain, access []byte
	var stagedAt, candidateAt int64
	if err := row.Scan(&locator, &external, &tokenChain, &access, &stagedAt, &candidateAt); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if !copyCredentialDigest(&result.LocatorDigest, locator) ||
		!copyCredentialDigest(&result.ExternalStateDigest, external) ||
		!copyCredentialDigest(&result.TokenChainDigest, tokenChain) ||
		!copyCredentialDigest(&result.AccessHashDigest, access) || stagedAt <= 0 ||
		candidateAt < 0 || (candidateAt > 0 && candidateAt < stagedAt) {
		return SyncedCredentialAdmissionStage{}, ErrAccountPresentationEvidence
	}
	result.StagedAt = time.Unix(0, stagedAt)
	if candidateAt > 0 {
		result.CandidateAt = time.Unix(0, candidateAt)
	}
	return result, nil
}

func syncedCredentialAdmissionTx(
	tx *sql.Tx,
	account Account,
) (SyncedCredentialAdmission, error) {
	row := tx.QueryRow(
		`SELECT locator_digest,external_state_digest,token_chain_digest,
		 access_hash_digest,admitted_at
		 FROM synced_credential_admissions
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		account.ID, account.InstanceID, account.Generation,
	)
	result := SyncedCredentialAdmission{
		AccountID: account.ID,
		SyncedCredentialAdmissionFence: SyncedCredentialAdmissionFence{
			AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		},
	}
	var locator, external, tokenChain, access []byte
	var admittedAt int64
	if err := row.Scan(&locator, &external, &tokenChain, &access, &admittedAt); err != nil {
		return SyncedCredentialAdmission{}, err
	}
	if !copyCredentialDigest(&result.LocatorDigest, locator) ||
		!copyCredentialDigest(&result.ExternalStateDigest, external) ||
		!copyCredentialDigest(&result.TokenChainDigest, tokenChain) ||
		!copyCredentialDigest(&result.AccessHashDigest, access) || admittedAt <= 0 {
		return SyncedCredentialAdmission{}, ErrAccountPresentationEvidence
	}
	result.AdmittedAt = time.Unix(0, admittedAt)
	return result, nil
}

func validateFileProviderPreparationProof(fileProvider FileProviderPreparationProof) error {
	if fileProvider.TenantID == "" || fileProvider.DomainID == "" || fileProvider.Generation == 0 ||
		fileProvider.ActivationGeneration == "" || !exactPresentationPath(fileProvider.PublicPath) ||
		strings.ContainsRune(fileProvider.TenantID, 0) || strings.ContainsRune(fileProvider.DomainID, 0) ||
		strings.ContainsRune(fileProvider.ActivationGeneration, 0) {
		return ErrAccountPresentationEvidence
	}
	return nil
}

func validatePresentationPreparationProof(proof PresentationPreparationProof) error {
	if proof.PresentationKind != PresentationKindFileProvider {
		return ErrAccountPresentationEvidence
	}
	if err := validateFileProviderPreparationProof(proof.FileProvider); err != nil {
		return err
	}
	if proof.CatalogTenantID == "" || proof.CatalogGeneration == 0 || proof.Requested == 0 ||
		proof.Desired != proof.Requested || proof.Observed != proof.Requested ||
		proof.Verified != proof.Requested || proof.Applied != proof.Requested ||
		proof.SourceAuthority == "" || proof.SourceRevision == 0 ||
		proof.CatalogRevision != proof.Requested || proof.ChangeID == "" || proof.OperationID == "" ||
		proof.CatalogTenantID != proof.FileProvider.TenantID ||
		proof.CatalogGeneration != proof.FileProvider.Generation {
		return ErrAccountPresentationEvidence
	}
	for _, value := range []string{
		proof.CatalogTenantID, proof.SourceAuthority, proof.ChangeID, proof.OperationID,
	} {
		if strings.ContainsRune(value, 0) {
			return ErrAccountPresentationEvidence
		}
	}
	return nil
}

func validatePresentationPreparationProofForAccount(
	proof PresentationPreparationProof,
	instanceID string,
	generation uint64,
	publicPath string,
) error {
	if err := validatePresentationPreparationProof(proof); err != nil {
		return err
	}
	if validateAccountInstanceID(instanceID) != nil ||
		proof.CatalogTenantID != "account-"+instanceID ||
		proof.CatalogGeneration != generation || proof.FileProvider.Generation != generation ||
		proof.FileProvider.PublicPath != publicPath {
		return ErrAccountPresentationEvidence
	}
	return nil
}

// ValidateReservedPresentationPreparationProof requires proof for exactly one
// prospective account identity before any product-owned state is materialized.
func ValidateReservedPresentationPreparationProof(
	reservation PendingAccountReservation,
	proof PresentationPreparationProof,
) error {
	return validatePresentationPreparationProofForAccount(
		proof,
		reservation.InstanceID,
		reservation.Generation,
		proof.FileProvider.PublicPath,
	)
}

// ValidatePresentationPreparationProofAdvance permits only a monotonic proof
// refresh for the same presentation identity and source authority.
func ValidatePresentationPreparationProofAdvance(
	current PresentationPreparationProof,
	next PresentationPreparationProof,
) error {
	if err := validatePresentationPreparationProof(current); err != nil {
		return err
	}
	if err := validatePresentationPreparationProof(next); err != nil {
		return err
	}
	if current.CatalogTenantID != next.CatalogTenantID ||
		current.CatalogGeneration != next.CatalogGeneration ||
		current.SourceAuthority != next.SourceAuthority ||
		current.PresentationKind != next.PresentationKind ||
		current.FileProvider.TenantID != next.FileProvider.TenantID ||
		current.FileProvider.DomainID != next.FileProvider.DomainID ||
		current.FileProvider.Generation != next.FileProvider.Generation ||
		current.FileProvider.PublicPath != next.FileProvider.PublicPath ||
		next.Requested < current.Requested ||
		next.SourceRevision < current.SourceRevision ||
		next.CatalogRevision < current.CatalogRevision {
		return ErrAccountPresentationEvidence
	}
	revisionAdvanced := next.Requested > current.Requested ||
		next.SourceRevision > current.SourceRevision ||
		next.CatalogRevision > current.CatalogRevision
	if !revisionAdvanced && (next.ChangeID != current.ChangeID || next.OperationID != current.OperationID) {
		return ErrAccountPresentationEvidence
	}
	return nil
}

func exactPresentationPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsRune(path, 0)
}

func samePresentationAccount(current, expected Account) bool {
	return current.ID == expected.ID && current.InstanceID == expected.InstanceID &&
		current.Generation == expected.Generation && current.ConfigDir == expected.ConfigDir &&
		current.KeychainService == expected.KeychainService &&
		current.KeychainAccount == expected.KeychainAccount
}

func presentationMismatch(
	account Account,
	bound AccountPresentation,
	fileProvider FileProviderPreparationProof,
) AccountPresentationQuarantineReason {
	if fileProvider.Generation != account.Generation {
		return AccountPresentationGenerationDrift
	}
	if fileProvider.PublicPath != account.ConfigDir {
		return AccountPresentationPublicPathDrift
	}
	if fileProvider.TenantID != bound.Proof.FileProvider.TenantID {
		return AccountPresentationTenantIDDrift
	}
	if fileProvider.DomainID != bound.Proof.FileProvider.DomainID {
		return AccountPresentationDomainIDDrift
	}
	if fileProvider.Generation != bound.Proof.FileProvider.Generation {
		return AccountPresentationGenerationDrift
	}
	if fileProvider.PublicPath != bound.Proof.FileProvider.PublicPath {
		return AccountPresentationPublicPathDrift
	}
	return ""
}

type presentationQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func presentationAccount(queryer presentationQueryer, accountID int) (Account, error) {
	return scanAccount(queryer.QueryRow(
		`SELECT `+accountCols+` FROM accounts WHERE id=? AND deleted_at IS NULL`, accountID,
	))
}

func accountPresentation(queryer presentationQueryer, accountID int) (AccountPresentation, error) {
	var presentation AccountPresentation
	var observedAt int64
	err := queryer.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,tenant_id,domain_id,
		 presentation_generation,activation_generation,public_path,
		 presentation_kind,catalog_tenant_id,catalog_generation,
		 catalog_requested,catalog_desired,catalog_observed,catalog_verified,catalog_applied,
		 source_authority,source_revision,catalog_revision,change_id,operation_id,observed_at
		 FROM account_presentations WHERE account_id=?`, accountID,
	).Scan(
		&presentation.AccountID, &presentation.AccountInstanceID, &presentation.AccountGeneration,
		&presentation.Proof.FileProvider.TenantID, &presentation.Proof.FileProvider.DomainID,
		&presentation.Proof.FileProvider.Generation, &presentation.Proof.FileProvider.ActivationGeneration,
		&presentation.Proof.FileProvider.PublicPath, &presentation.Proof.PresentationKind,
		&presentation.Proof.CatalogTenantID, &presentation.Proof.CatalogGeneration,
		&presentation.Proof.Requested, &presentation.Proof.Desired, &presentation.Proof.Observed,
		&presentation.Proof.Verified, &presentation.Proof.Applied,
		&presentation.Proof.SourceAuthority, &presentation.Proof.SourceRevision,
		&presentation.Proof.CatalogRevision, &presentation.Proof.ChangeID,
		&presentation.Proof.OperationID, &observedAt,
	)
	if err == nil {
		err = validatePresentationPreparationProof(presentation.Proof)
	}
	presentation.ObservedAt = time.Unix(0, observedAt)
	return presentation, err
}

func upsertAccountPresentation(tx *sql.Tx, presentation AccountPresentation) error {
	_, err := tx.Exec(
		`INSERT INTO account_presentations(
		 account_id,account_instance_id,account_generation,tenant_id,domain_id,
		 presentation_generation,activation_generation,public_path,
		 presentation_kind,catalog_tenant_id,catalog_generation,
		 catalog_requested,catalog_desired,catalog_observed,catalog_verified,catalog_applied,
		 source_authority,source_revision,catalog_revision,change_id,operation_id,observed_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id) DO UPDATE SET
		 activation_generation=excluded.activation_generation,
		 catalog_requested=excluded.catalog_requested,catalog_desired=excluded.catalog_desired,
		 catalog_observed=excluded.catalog_observed,catalog_verified=excluded.catalog_verified,
		 catalog_applied=excluded.catalog_applied,source_authority=excluded.source_authority,
		 source_revision=excluded.source_revision,catalog_revision=excluded.catalog_revision,
		 change_id=excluded.change_id,operation_id=excluded.operation_id,observed_at=excluded.observed_at`,
		presentation.AccountID, presentation.AccountInstanceID, presentation.AccountGeneration,
		presentation.Proof.FileProvider.TenantID, presentation.Proof.FileProvider.DomainID,
		presentation.Proof.FileProvider.Generation, presentation.Proof.FileProvider.ActivationGeneration,
		presentation.Proof.FileProvider.PublicPath, presentation.Proof.PresentationKind,
		presentation.Proof.CatalogTenantID, presentation.Proof.CatalogGeneration,
		presentation.Proof.Requested, presentation.Proof.Desired, presentation.Proof.Observed,
		presentation.Proof.Verified, presentation.Proof.Applied,
		presentation.Proof.SourceAuthority, presentation.Proof.SourceRevision,
		presentation.Proof.CatalogRevision, presentation.Proof.ChangeID,
		presentation.Proof.OperationID, presentation.ObservedAt.UnixNano(),
	)
	return err
}

func accountPresentationQuarantine(
	queryer presentationQueryer,
	accountID int,
) (AccountPresentationQuarantine, error) {
	var quarantine AccountPresentationQuarantine
	var createdAt int64
	err := queryer.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,expected_config_dir,
		 observed_tenant_id,observed_domain_id,observed_generation,
		 observed_activation_generation,observed_public_path,
		 observed_presentation_kind,
		 observed_catalog_tenant_id,observed_catalog_generation,
		 observed_catalog_requested,observed_catalog_desired,observed_catalog_observed,
		 observed_catalog_verified,observed_catalog_applied,observed_source_authority,
		 observed_source_revision,observed_catalog_revision,observed_change_id,
		 observed_operation_id,reason,created_at
		 FROM account_presentation_quarantines WHERE account_id=?`, accountID,
	).Scan(
		&quarantine.AccountID, &quarantine.AccountInstanceID, &quarantine.AccountGeneration,
		&quarantine.ExpectedConfigDir, &quarantine.Proof.FileProvider.TenantID,
		&quarantine.Proof.FileProvider.DomainID, &quarantine.Proof.FileProvider.Generation,
		&quarantine.Proof.FileProvider.ActivationGeneration, &quarantine.Proof.FileProvider.PublicPath,
		&quarantine.Proof.PresentationKind,
		&quarantine.Proof.CatalogTenantID, &quarantine.Proof.CatalogGeneration,
		&quarantine.Proof.Requested, &quarantine.Proof.Desired, &quarantine.Proof.Observed,
		&quarantine.Proof.Verified, &quarantine.Proof.Applied,
		&quarantine.Proof.SourceAuthority, &quarantine.Proof.SourceRevision,
		&quarantine.Proof.CatalogRevision, &quarantine.Proof.ChangeID,
		&quarantine.Proof.OperationID,
		&quarantine.Reason, &createdAt,
	)
	if err == nil {
		err = validatePresentationPreparationProof(quarantine.Proof)
	}
	quarantine.CreatedAt = time.Unix(0, createdAt)
	return quarantine, err
}

func insertAccountPresentationQuarantine(tx *sql.Tx, quarantine AccountPresentationQuarantine) error {
	_, err := tx.Exec(
		`INSERT INTO account_presentation_quarantines(
		 account_id,account_instance_id,account_generation,expected_config_dir,
		 observed_tenant_id,observed_domain_id,observed_generation,
		 observed_activation_generation,observed_public_path,
		 observed_presentation_kind,
		 observed_catalog_tenant_id,observed_catalog_generation,
		 observed_catalog_requested,observed_catalog_desired,observed_catalog_observed,
		 observed_catalog_verified,observed_catalog_applied,observed_source_authority,
		 observed_source_revision,observed_catalog_revision,observed_change_id,
		 observed_operation_id,reason,created_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO NOTHING`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.ExpectedConfigDir, quarantine.Proof.FileProvider.TenantID,
		quarantine.Proof.FileProvider.DomainID, quarantine.Proof.FileProvider.Generation,
		quarantine.Proof.FileProvider.ActivationGeneration, quarantine.Proof.FileProvider.PublicPath,
		quarantine.Proof.PresentationKind,
		quarantine.Proof.CatalogTenantID, quarantine.Proof.CatalogGeneration,
		quarantine.Proof.Requested, quarantine.Proof.Desired, quarantine.Proof.Observed,
		quarantine.Proof.Verified, quarantine.Proof.Applied,
		quarantine.Proof.SourceAuthority, quarantine.Proof.SourceRevision,
		quarantine.Proof.CatalogRevision, quarantine.Proof.ChangeID,
		quarantine.Proof.OperationID,
		quarantine.Reason, quarantine.CreatedAt.UnixNano(),
	)
	return err
}

func accountPresentationBusyExceptMutation(
	tx *sql.Tx,
	accountID int,
	operationID AccountMutationID,
) (bool, error) {
	var busy int
	err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM sessions WHERE account_id=? AND ended_at IS NULL)
		 OR EXISTS(SELECT 1 FROM account_mutations WHERE account_id=? AND operation_id<>?)
		 OR EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_removals WHERE account_id=?)`,
		accountID, accountID, operationID[:], accountID, accountID,
	).Scan(&busy)
	return busy != 0, err
}
