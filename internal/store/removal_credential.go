package store

import (
	"database/sql"
	"errors"
)

// CredentialRemovalSubject returns the exact slot coordinates retained by one removal operation.
func (s *Store) CredentialRemovalSubject(removal AccountRemoval) (Account, error) {
	stored, err := accountRemovalByID(s.db, removal.AccountID)
	if err != nil {
		return Account{}, err
	}
	if !sameAccountRemoval(stored, removal) || !removal.DeleteCredential {
		return Account{}, ErrAccountGenerationChanged
	}
	operation, err := credentialOperationByAccount(s.db, removal.AccountID)
	if err == nil {
		if operation.Kind != CredentialOperationRemove ||
			operation.AccountInstanceID != removal.AccountInstanceID ||
			operation.AccountGeneration != removal.AccountGeneration {
			return Account{}, ErrCredentialOperationState
		}
		return accountFromCredentialRemoval(
			operation.AccountID, operation.AccountInstanceID, operation.AccountGeneration,
			operation.ConfigDir, operation.KeychainService, operation.KeychainAccount,
			operation.LocatorDigest, operation.IntentDigest,
		)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Account{}, err
	}
	receipt, err := credentialRemovalReceipt(s.db, removal)
	if err != nil {
		return Account{}, err
	}
	return accountFromCredentialRemoval(
		receipt.AccountID, receipt.AccountInstanceID, receipt.AccountGeneration,
		receipt.ConfigDir, receipt.KeychainService, receipt.KeychainAccount,
		receipt.LocatorDigest, receipt.IntentDigest,
	)
}

// FinalizePendingAccountRemoval atomically clears a retired pending reservation after exact credential settlement.
func (s *Store) FinalizePendingAccountRemoval(removal AccountRemoval) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stored, err := accountRemovalByID(tx, removal.AccountID)
	if err != nil {
		return err
	}
	if !sameAccountRemoval(stored, removal) || !removal.DeleteCredential {
		return ErrAccountGenerationChanged
	}
	pending, err := pendingAccountReservationByID(tx, removal.AccountID)
	if err != nil {
		return err
	}
	if pending.InstanceID != removal.AccountInstanceID || pending.Generation != removal.AccountGeneration {
		return ErrAccountGenerationChanged
	}
	if _, err := credentialOperationByAccount(tx, removal.AccountID); err == nil {
		return ErrCredentialOperationEvidenceActive
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	receipt, err := credentialRemovalReceipt(tx, removal)
	if err != nil {
		return err
	}
	if receipt.TerminalStatus != CredentialTerminalSucceeded ||
		receipt.Result != CredentialResultDone || receipt.AcknowledgedAt.IsZero() {
		return ErrCredentialOperationEvidenceActive
	}
	if _, err := credentialQuarantine(tx, removal.AccountID); err == nil {
		return ErrCredentialOperationEvidenceActive
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var structuralEvidence int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM account_mutations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_mutation_receipts WHERE account_id=? AND acknowledged_at IS NULL)`,
		removal.AccountID, removal.AccountID,
	).Scan(&structuralEvidence); err != nil {
		return err
	}
	if structuralEvidence != 0 {
		return ErrCredentialOperationEvidenceActive
	}
	result, err := tx.Exec(
		`DELETE FROM account_removals
		 WHERE account_id=? AND account_instance_id=? AND account_generation=? AND registry_sequence=?`,
		removal.AccountID, removal.AccountInstanceID, removal.AccountGeneration, removal.RegistrySequence,
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrAccountGenerationChanged
	}
	result, err = tx.Exec(
		`DELETE FROM pending_adds WHERE id=? AND instance_id=? AND generation=?`,
		removal.AccountID, removal.AccountInstanceID, removal.AccountGeneration,
	)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrAccountGenerationChanged
	}
	return tx.Commit()
}

func credentialRemovalReceipt(
	queryer credentialOperationQueryer,
	removal AccountRemoval,
) (CredentialOperationReceipt, error) {
	return scanCredentialOperationReceipt(queryer.QueryRow(
		`SELECT `+receiptSelectColumns+` FROM credential_operation_receipts
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND kind='remove' AND target='keychain'
		 ORDER BY committed_at DESC,operation_id DESC LIMIT 1`,
		removal.AccountID, removal.AccountInstanceID, removal.AccountGeneration,
	))
}

func accountFromCredentialRemoval(
	accountID int,
	instanceID string,
	generation uint64,
	configDir string,
	keychainService string,
	keychainAccount string,
	locator CredentialDigest,
	intent CredentialDigest,
) (Account, error) {
	expectedIntent, err := CredentialRemovalIntentDigest(
		accountID, instanceID, generation, configDir, keychainService, keychainAccount,
	)
	if err != nil || intent != expectedIntent ||
		locator != CredentialKeychainLocatorDigest(keychainService, keychainAccount) {
		return Account{}, ErrCredentialOperationState
	}
	return Account{
		ID: accountID, InstanceID: instanceID, Generation: generation,
		ConfigDir: configDir, KeychainService: keychainService, KeychainAccount: keychainAccount,
	}, nil
}

func sameAccountRemoval(left, right AccountRemoval) bool {
	return left.AccountID == right.AccountID &&
		left.AccountInstanceID == right.AccountInstanceID &&
		left.AccountGeneration == right.AccountGeneration &&
		left.RegistrySequence == right.RegistrySequence &&
		left.DeleteCredential == right.DeleteCredential &&
		left.CreatedAt.Equal(right.CreatedAt)
}
