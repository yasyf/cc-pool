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
	if err := result.SyncedCredentialAdmissionFence.validate(account); err != nil {
		return SyncedCredentialAdmission{}, err
	}
	return result, nil
}

func copyCredentialDigest(target *CredentialDigest, source []byte) bool {
	if len(source) != len(target) {
		return false
	}
	copy(target[:], source)
	return true
}
