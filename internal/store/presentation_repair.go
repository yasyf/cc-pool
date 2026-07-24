package store

import (
	"database/sql"
	"errors"
	"time"
)

// AccountPresentationRepair is one durable path-only presentation transition.
type AccountPresentationRepair struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	Previous          FileProviderPresentationIdentity
	Target            FileProviderPresentationIdentity
	CreatedAt         time.Time
}

// StageAccountPresentationRepair records one exact quarantined transition before link I/O.
func (s *Store) StageAccountPresentationRepair(
	account Account,
	target FileProviderPresentationIdentity,
) (AccountPresentationRepair, error) {
	if validateFileProviderPresentationIdentity(target) != nil ||
		validateAccountInstanceID(account.InstanceID) != nil ||
		target.TenantID != "account-"+account.InstanceID || target.Generation < account.Generation {
		return AccountPresentationRepair{}, ErrAccountPresentationEvidence
	}
	tx, err := s.db.Begin()
	if err != nil {
		return AccountPresentationRepair{}, err
	}
	defer func() { _ = tx.Rollback() }()
	current, previous, quarantine, err := presentationRepairEvidence(tx, account.ID)
	if err != nil {
		return AccountPresentationRepair{}, err
	}
	if !samePresentationAccount(current, account) ||
		previous.AccountInstanceID != account.InstanceID ||
		previous.AccountGeneration != account.Generation ||
		quarantine.AccountInstanceID != account.InstanceID ||
		quarantine.AccountGeneration != account.Generation ||
		quarantine.ExpectedConfigDir != account.ConfigDir ||
		quarantine.Observed != target || !validPresentationRepair(previous.Identity, target) {
		return AccountPresentationRepair{}, ErrAccountPresentationEvidence
	}
	busy, err := accountPresentationRepairBusy(tx, account.ID)
	if err != nil {
		return AccountPresentationRepair{}, err
	}
	if busy {
		return AccountPresentationRepair{}, ErrAccountPresentationBusy
	}
	repair := AccountPresentationRepair{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, Previous: previous.Identity,
		Target: target, CreatedAt: s.now(),
	}
	stored, err := accountPresentationRepair(tx, account.ID)
	if err == nil {
		if !sameAccountPresentationRepair(stored, repair) {
			return AccountPresentationRepair{}, ErrAccountPresentationBusy
		}
		if err := tx.Commit(); err != nil {
			return AccountPresentationRepair{}, err
		}
		return stored, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return AccountPresentationRepair{}, err
	}
	_, err = tx.Exec(
		`INSERT INTO account_presentation_repairs(
		 account_id,account_instance_id,account_generation,
		 previous_tenant_id,previous_domain_id,previous_presentation_generation,previous_public_path,
		 target_tenant_id,target_domain_id,target_presentation_generation,target_public_path,created_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		repair.AccountID, repair.AccountInstanceID, repair.AccountGeneration,
		repair.Previous.TenantID, repair.Previous.DomainID, repair.Previous.Generation, repair.Previous.PublicPath,
		repair.Target.TenantID, repair.Target.DomainID, repair.Target.Generation, repair.Target.PublicPath,
		repair.CreatedAt.UnixNano(),
	)
	if err != nil {
		return AccountPresentationRepair{}, err
	}
	if err := tx.Commit(); err != nil {
		return AccountPresentationRepair{}, err
	}
	return repair, nil
}

// AccountPresentationRepair returns one pending durable path repair.
func (s *Store) AccountPresentationRepair(accountID int) (AccountPresentationRepair, error) {
	return accountPresentationRepair(s.db, accountID)
}

// PendingAccountPresentationRepairs returns every unfinished path repair.
func (s *Store) PendingAccountPresentationRepairs() ([]AccountPresentationRepair, error) {
	rows, err := s.db.Query(
		`SELECT account_id,account_instance_id,account_generation,
		 previous_tenant_id,previous_domain_id,previous_presentation_generation,previous_public_path,
		 target_tenant_id,target_domain_id,target_presentation_generation,target_public_path,created_at
		 FROM account_presentation_repairs ORDER BY account_id`,
	)
	if err != nil {
		return nil, err
	}
	var repairs []AccountPresentationRepair
	for rows.Next() {
		repair, err := scanAccountPresentationRepair(rows)
		if err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		repairs = append(repairs, repair)
	}
	return repairs, errors.Join(rows.Err(), rows.Close())
}

// CommitAccountPresentationRepair installs one path-only binding after exact link repair.
func (s *Store) CommitAccountPresentationRepair(
	repair AccountPresentationRepair,
) (AccountPresentation, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return AccountPresentation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	stored, err := accountPresentationRepair(tx, repair.AccountID)
	if errors.Is(err, sql.ErrNoRows) {
		current, currentErr := accountPresentation(tx, repair.AccountID)
		if currentErr == nil && current.AccountInstanceID == repair.AccountInstanceID &&
			current.AccountGeneration == repair.AccountGeneration && current.Identity == repair.Target {
			if _, quarantineErr := accountPresentationQuarantine(tx, repair.AccountID); errors.Is(quarantineErr, sql.ErrNoRows) {
				return current, tx.Commit()
			}
		}
		return AccountPresentation{}, ErrAccountPresentationEvidence
	}
	if err != nil {
		return AccountPresentation{}, err
	}
	if !sameAccountPresentationRepair(stored, repair) {
		return AccountPresentation{}, ErrAccountPresentationEvidence
	}
	current, previous, quarantine, err := presentationRepairEvidence(tx, repair.AccountID)
	if err != nil {
		return AccountPresentation{}, err
	}
	if current.InstanceID != repair.AccountInstanceID || current.Generation != repair.AccountGeneration ||
		previous.AccountInstanceID != repair.AccountInstanceID ||
		previous.AccountGeneration != repair.AccountGeneration || previous.Identity != repair.Previous ||
		quarantine.AccountInstanceID != repair.AccountInstanceID ||
		quarantine.AccountGeneration != repair.AccountGeneration || quarantine.Observed != repair.Target ||
		quarantine.ExpectedConfigDir != current.ConfigDir {
		return AccountPresentation{}, ErrAccountPresentationEvidence
	}
	busy, err := accountPresentationRepairBusy(tx, repair.AccountID)
	if err != nil {
		return AccountPresentation{}, err
	}
	if busy {
		return AccountPresentation{}, ErrAccountPresentationBusy
	}
	result, err := tx.Exec(
		`UPDATE account_presentations
		 SET tenant_id=?,domain_id=?,presentation_generation=?,public_path=?,observed_at=?
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?
		 AND tenant_id=? AND domain_id=? AND presentation_generation=? AND public_path=?`,
		repair.Target.TenantID, repair.Target.DomainID, repair.Target.Generation,
		repair.Target.PublicPath, s.now().UnixNano(), repair.AccountID, repair.AccountInstanceID,
		repair.AccountGeneration, repair.Previous.TenantID, repair.Previous.DomainID,
		repair.Previous.Generation, repair.Previous.PublicPath,
	)
	if err != nil {
		return AccountPresentation{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return AccountPresentation{}, ErrAccountPresentationEvidence
	}
	if _, err := tx.Exec(
		`DELETE FROM account_presentation_quarantines
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		repair.AccountID, repair.AccountInstanceID, repair.AccountGeneration,
	); err != nil {
		return AccountPresentation{}, err
	}
	if _, err := tx.Exec(
		`DELETE FROM account_presentation_repairs
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		repair.AccountID, repair.AccountInstanceID, repair.AccountGeneration,
	); err != nil {
		return AccountPresentation{}, err
	}
	committed, err := accountPresentation(tx, repair.AccountID)
	if err != nil {
		return AccountPresentation{}, err
	}
	if err := tx.Commit(); err != nil {
		return AccountPresentation{}, err
	}
	return committed, nil
}

func validPresentationRepair(previous, target FileProviderPresentationIdentity) bool {
	return previous.TenantID == target.TenantID && previous.DomainID == target.DomainID &&
		previous.Generation == target.Generation && previous.PublicPath != target.PublicPath
}

func presentationRepairEvidence(
	tx *sql.Tx,
	accountID int,
) (Account, AccountPresentation, AccountPresentationQuarantine, error) {
	account, err := presentationAccount(tx, accountID)
	if err != nil {
		return Account{}, AccountPresentation{}, AccountPresentationQuarantine{}, err
	}
	presentation, err := accountPresentation(tx, accountID)
	if err != nil {
		return Account{}, AccountPresentation{}, AccountPresentationQuarantine{}, err
	}
	quarantine, err := accountPresentationQuarantine(tx, accountID)
	return account, presentation, quarantine, err
}

func accountPresentationRepairBusy(tx *sql.Tx, accountID int) (bool, error) {
	var busy int
	err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM sessions WHERE account_id=? AND ended_at IS NULL)
		 OR EXISTS(SELECT 1 FROM account_mutations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?)
		 OR EXISTS(SELECT 1 FROM account_removals WHERE account_id=?)`,
		accountID, accountID, accountID, accountID,
	).Scan(&busy)
	return busy != 0, err
}

func accountPresentationRepair(
	queryer presentationQueryer,
	accountID int,
) (AccountPresentationRepair, error) {
	return scanAccountPresentationRepair(queryer.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,
		 previous_tenant_id,previous_domain_id,previous_presentation_generation,previous_public_path,
		 target_tenant_id,target_domain_id,target_presentation_generation,target_public_path,created_at
		 FROM account_presentation_repairs WHERE account_id=?`, accountID,
	))
}

func scanAccountPresentationRepair(row interface{ Scan(...any) error }) (AccountPresentationRepair, error) {
	var repair AccountPresentationRepair
	var createdAt int64
	err := row.Scan(
		&repair.AccountID, &repair.AccountInstanceID, &repair.AccountGeneration,
		&repair.Previous.TenantID, &repair.Previous.DomainID, &repair.Previous.Generation,
		&repair.Previous.PublicPath, &repair.Target.TenantID, &repair.Target.DomainID,
		&repair.Target.Generation, &repair.Target.PublicPath, &createdAt,
	)
	repair.CreatedAt = time.Unix(0, createdAt)
	if err == nil && (repair.AccountID < 1 || validateAccountInstanceID(repair.AccountInstanceID) != nil ||
		repair.AccountGeneration == 0 || validateFileProviderPresentationIdentity(repair.Previous) != nil ||
		validateFileProviderPresentationIdentity(repair.Target) != nil ||
		!validPresentationRepair(repair.Previous, repair.Target) || repair.CreatedAt.IsZero()) {
		err = ErrAccountPresentationEvidence
	}
	return repair, err
}

func sameAccountPresentationRepair(left, right AccountPresentationRepair) bool {
	return left.AccountID == right.AccountID && left.AccountInstanceID == right.AccountInstanceID &&
		left.AccountGeneration == right.AccountGeneration && left.Previous == right.Previous &&
		left.Target == right.Target
}
