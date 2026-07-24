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

// AccountPresentation is the immutable presentation identity bound to an account generation.
type AccountPresentation struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	Identity          FileProviderPresentationIdentity
	ObservedAt        time.Time
}

// FileProviderPresentationIdentity is the expected immutable presentation
// identity derived by the product layer for one account generation.
type FileProviderPresentationIdentity struct {
	TenantID   string
	DomainID   string
	Generation uint64
	PublicPath string
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
	Observed          FileProviderPresentationIdentity
	Reason            AccountPresentationQuarantineReason
	CreatedAt         time.Time
}

// ObserveAccountPresentation validates an observed presentation identity or durably quarantines drift.
func (s *Store) ObserveAccountPresentation(account Account, observed FileProviderPresentationIdentity) error {
	if err := validateFileProviderPresentationIdentity(observed); err != nil {
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
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAccountPresentationEvidence
	}
	if err != nil {
		return err
	}
	reason := presentationMismatch(current, bound, observed)
	if reason != "" {
		quarantine := AccountPresentationQuarantine{
			AccountID: current.ID, AccountInstanceID: current.InstanceID,
			AccountGeneration: current.Generation, ExpectedConfigDir: current.ConfigDir,
			Observed: observed, Reason: reason, CreatedAt: s.now(),
		}
		if err := insertAccountPresentationQuarantine(tx, quarantine); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return ErrAccountPresentationQuarantined
	}
	return tx.Commit()
}

// BindDesiredAccountPresentation atomically installs one immutable presentation identity.
func (s *Store) BindDesiredAccountPresentation(
	account Account,
	expected FileProviderPresentationIdentity,
) error {
	if err := validateExpectedPresentationIdentity(account, expected); err != nil {
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
	if _, err := desiredPresentationAccount(tx, account.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAccountPresentationBusy
		}
		return err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err == nil {
		if bound.Identity != expected {
			return ErrAccountPresentationEvidence
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: current.ID, AccountInstanceID: current.InstanceID,
		AccountGeneration: current.Generation, Identity: expected, ObservedAt: s.now(),
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// AccountPresentation returns the current identity binding for one account.
func (s *Store) AccountPresentation(accountID int) (AccountPresentation, error) {
	return accountPresentation(s.db, accountID)
}

// AccountPresentationQuarantine returns one durable presentation quarantine.
func (s *Store) AccountPresentationQuarantine(accountID int) (AccountPresentationQuarantine, error) {
	return accountPresentationQuarantine(s.db, accountID)
}

// StageSyncedAccountAdmission durably records exact external evidence and
// advances the presentation while retaining awaiting-origin state.
func (s *Store) StageSyncedAccountAdmission(
	account Account,
	currentIdentity FileProviderPresentationIdentity,
	freshIdentity FileProviderPresentationIdentity,
	credential SyncedCredentialAdmissionFence,
) (SyncedCredentialAdmissionStage, error) {
	if err := validateExpectedPresentationIdentity(account, freshIdentity); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	if currentIdentity != freshIdentity {
		return SyncedCredentialAdmissionStage{}, ErrAccountPresentationEvidence
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
		bound.Identity == freshIdentity && health.healthyOwned() {
		return SyncedCredentialAdmissionStage{
			AccountID: account.ID, SyncedCredentialAdmissionFence: credential,
			StagedAt: final.AdmittedAt, Finalized: true,
		}, nil
	} else if finalErr != nil && !errors.Is(finalErr, sql.ErrNoRows) {
		return SyncedCredentialAdmissionStage{}, finalErr
	}
	if pending, pendingErr := pendingSyncedCredentialAdmissionTx(tx, account); pendingErr == nil &&
		sameSyncedCredentialAdmissionFence(pending.SyncedCredentialAdmissionFence, credential) &&
		bound.Identity == freshIdentity && health.awaitingOrigin() {
		return pending, nil
	} else if pendingErr != nil && !errors.Is(pendingErr, sql.ErrNoRows) {
		return SyncedCredentialAdmissionStage{}, pendingErr
	}
	if bound.AccountInstanceID != account.InstanceID ||
		bound.AccountGeneration != account.Generation || bound.Identity != currentIdentity ||
		!health.awaitingOrigin() {
		return SyncedCredentialAdmissionStage{}, ErrAccountPresentationEvidence
	}
	now := s.now()
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, Identity: freshIdentity, ObservedAt: now,
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
	identity FileProviderPresentationIdentity,
	credential SyncedCredentialAdmissionFence,
) (bool, error) {
	if err := validateExpectedPresentationIdentity(account, identity); err != nil {
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
	) || bound.Identity != identity || !health.awaitingOrigin() {
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
	identity FileProviderPresentationIdentity,
	credential SyncedCredentialAdmissionFence,
) (bool, error) {
	if err := validateExpectedPresentationIdentity(account, identity); err != nil {
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
	if err != nil || bound.Identity != identity {
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
	identity FileProviderPresentationIdentity,
	credential SyncedCredentialAdmissionFence,
) error {
	if err := validateExpectedPresentationIdentity(account, identity); err != nil {
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
	if err != nil || bound.Identity != identity {
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
	identity FileProviderPresentationIdentity,
	credential SyncedCredentialAdmissionFence,
) error {
	if err := validateExpectedPresentationIdentity(account, identity); err != nil {
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
	if err != nil || bound.Identity != identity {
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

func validateFileProviderPresentationIdentity(identity FileProviderPresentationIdentity) error {
	if identity.TenantID == "" || identity.DomainID == "" || identity.Generation == 0 ||
		!exactPresentationPath(identity.PublicPath) || strings.ContainsRune(identity.TenantID, 0) ||
		strings.ContainsRune(identity.DomainID, 0) {
		return ErrAccountPresentationEvidence
	}
	return nil
}

// ValidateReservedPresentationIdentity requires one exact prospective account identity.
func ValidateReservedPresentationIdentity(
	reservation PendingAccountReservation,
	identity FileProviderPresentationIdentity,
) error {
	if err := validateFileProviderPresentationIdentity(identity); err != nil {
		return err
	}
	if validateAccountInstanceID(reservation.InstanceID) != nil ||
		identity.TenantID != "account-"+reservation.InstanceID ||
		identity.Generation != reservation.Generation {
		return ErrAccountPresentationEvidence
	}
	return nil
}

func exactPresentationPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsRune(path, 0)
}

func validateExpectedPresentationIdentity(account Account, expected FileProviderPresentationIdentity) error {
	if validateFileProviderPresentationIdentity(expected) != nil ||
		validateAccountInstanceID(account.InstanceID) != nil ||
		expected.TenantID != "account-"+account.InstanceID ||
		expected.DomainID == "" || strings.ContainsRune(expected.DomainID, 0) ||
		expected.Generation != account.Generation || expected.PublicPath != account.ConfigDir ||
		!exactPresentationPath(expected.PublicPath) {
		return ErrAccountPresentationEvidence
	}
	return nil
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
	observed FileProviderPresentationIdentity,
) AccountPresentationQuarantineReason {
	if observed.Generation != account.Generation {
		return AccountPresentationGenerationDrift
	}
	if observed.PublicPath != account.ConfigDir {
		return AccountPresentationPublicPathDrift
	}
	if observed.TenantID != bound.Identity.TenantID {
		return AccountPresentationTenantIDDrift
	}
	if observed.DomainID != bound.Identity.DomainID {
		return AccountPresentationDomainIDDrift
	}
	if observed.Generation != bound.Identity.Generation {
		return AccountPresentationGenerationDrift
	}
	if observed.PublicPath != bound.Identity.PublicPath {
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

func desiredPresentationAccount(queryer presentationQueryer, accountID int) (Account, error) {
	return scanAccount(queryer.QueryRow(
		`SELECT `+accountCols+` FROM accounts WHERE accounts.id=? AND `+desiredAccountPredicate,
		accountID,
	))
}

func accountPresentation(queryer presentationQueryer, accountID int) (AccountPresentation, error) {
	var presentation AccountPresentation
	var observedAt int64
	err := queryer.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,tenant_id,domain_id,
		 presentation_generation,public_path,observed_at
		 FROM account_presentations WHERE account_id=?`, accountID,
	).Scan(
		&presentation.AccountID, &presentation.AccountInstanceID, &presentation.AccountGeneration,
		&presentation.Identity.TenantID, &presentation.Identity.DomainID,
		&presentation.Identity.Generation, &presentation.Identity.PublicPath, &observedAt,
	)
	if err == nil {
		err = validateFileProviderPresentationIdentity(presentation.Identity)
	}
	presentation.ObservedAt = time.Unix(0, observedAt)
	return presentation, err
}

func upsertAccountPresentation(tx *sql.Tx, presentation AccountPresentation) error {
	_, err := tx.Exec(
		`INSERT INTO account_presentations(
		 account_id,account_instance_id,account_generation,tenant_id,domain_id,
		 presentation_generation,public_path,observed_at
		 ) VALUES(?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id) DO UPDATE SET observed_at=excluded.observed_at`,
		presentation.AccountID, presentation.AccountInstanceID, presentation.AccountGeneration,
		presentation.Identity.TenantID, presentation.Identity.DomainID,
		presentation.Identity.Generation, presentation.Identity.PublicPath, presentation.ObservedAt.UnixNano(),
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
		 observed_tenant_id,observed_domain_id,observed_generation,observed_public_path,reason,created_at
		 FROM account_presentation_quarantines WHERE account_id=?`, accountID,
	).Scan(
		&quarantine.AccountID, &quarantine.AccountInstanceID, &quarantine.AccountGeneration,
		&quarantine.ExpectedConfigDir, &quarantine.Observed.TenantID,
		&quarantine.Observed.DomainID, &quarantine.Observed.Generation,
		&quarantine.Observed.PublicPath,
		&quarantine.Reason, &createdAt,
	)
	if err == nil {
		err = validateFileProviderPresentationIdentity(quarantine.Observed)
	}
	quarantine.CreatedAt = time.Unix(0, createdAt)
	return quarantine, err
}

func insertAccountPresentationQuarantine(tx *sql.Tx, quarantine AccountPresentationQuarantine) error {
	_, err := tx.Exec(
		`INSERT INTO account_presentation_quarantines(
		 account_id,account_instance_id,account_generation,expected_config_dir,
		 observed_tenant_id,observed_domain_id,observed_generation,observed_public_path,reason,created_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO NOTHING`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.ExpectedConfigDir, quarantine.Observed.TenantID,
		quarantine.Observed.DomainID, quarantine.Observed.Generation, quarantine.Observed.PublicPath,
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
