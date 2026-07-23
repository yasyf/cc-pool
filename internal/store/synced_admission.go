package store

import (
	"errors"
	"time"
)

// SyncedCredentialAdmissionFence binds admission to one exact external
// credential observation.
type SyncedCredentialAdmissionFence struct {
	AccountInstanceID   string
	AccountGeneration   uint64
	LocatorDigest       CredentialDigest
	ExternalStateDigest CredentialDigest
	TokenChainDigest    CredentialDigest
	AccessHashDigest    CredentialDigest
}

// SyncedCredentialAdmission is the durable, secret-free admission evidence.
type SyncedCredentialAdmission struct {
	AccountID int
	SyncedCredentialAdmissionFence
	AdmittedAt time.Time
}

// SyncedCredentialAdmissionStage is durable admission liability that has not
// yet cleared awaiting-origin state. Finalized reports an exact replay.
type SyncedCredentialAdmissionStage struct {
	AccountID int
	SyncedCredentialAdmissionFence
	StagedAt    time.Time
	CandidateAt time.Time
	Finalized   bool
}

func (fence SyncedCredentialAdmissionFence) validate(account Account) error {
	if fence.AccountInstanceID != account.InstanceID ||
		fence.AccountGeneration != account.Generation ||
		fence.LocatorDigest != CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		) || fence.LocatorDigest.zero() || fence.ExternalStateDigest.zero() ||
		fence.TokenChainDigest.zero() || fence.AccessHashDigest.zero() {
		return ErrAccountPresentationEvidence
	}
	return nil
}

// SyncedCredentialAdmission returns the exact credential evidence for account's generation.
func (s *Store) SyncedCredentialAdmission(account Account) (SyncedCredentialAdmission, error) {
	row := s.db.QueryRow(
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
		return SyncedCredentialAdmission{}, errors.New("synced credential admission evidence is corrupt")
	}
	result.AdmittedAt = time.Unix(0, admittedAt)
	if err := result.validate(account); err != nil {
		return SyncedCredentialAdmission{}, err
	}
	return result, nil
}

// PendingSyncedCredentialAdmission returns the exact unfinalized evidence for account.
func (s *Store) PendingSyncedCredentialAdmission(account Account) (SyncedCredentialAdmissionStage, error) {
	row := s.db.QueryRow(
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
		return SyncedCredentialAdmissionStage{}, errors.New("pending synced credential admission evidence is corrupt")
	}
	result.StagedAt = time.Unix(0, stagedAt)
	if candidateAt > 0 {
		result.CandidateAt = time.Unix(0, candidateAt)
	}
	if err := result.validate(account); err != nil {
		return SyncedCredentialAdmissionStage{}, err
	}
	return result, nil
}

func sameSyncedCredentialAdmissionFence(left, right SyncedCredentialAdmissionFence) bool {
	return left == right
}

func copyCredentialDigest(target *CredentialDigest, source []byte) bool {
	if len(source) != len(target) {
		return false
	}
	copy(target[:], source)
	return true
}
