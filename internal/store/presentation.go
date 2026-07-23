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

// PresentationEvidence is the product-owned projection of one exact FuseKit proof.
type PresentationEvidence struct {
	TenantID             string
	DomainID             string
	Generation           uint64
	ActivationGeneration string
	PublicPath           string
}

// AccountPresentation is the last exact proof bound to an account generation.
type AccountPresentation struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	Evidence          PresentationEvidence
	ObservedAt        time.Time
}

// AccountPresentationQuarantineReason classifies exact presentation identity drift.
type AccountPresentationQuarantineReason string

const (
	AccountPresentationPublicPathDrift AccountPresentationQuarantineReason = "public-path-drift"
	AccountPresentationTenantIDDrift   AccountPresentationQuarantineReason = "tenant-id-drift"
	AccountPresentationDomainIDDrift   AccountPresentationQuarantineReason = "domain-id-drift"
	AccountPresentationGenerationDrift AccountPresentationQuarantineReason = "generation-drift"
)

// AccountPresentationQuarantine preserves one immutable unsafe observation.
type AccountPresentationQuarantine struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	ExpectedConfigDir string
	Observed          PresentationEvidence
	Reason            AccountPresentationQuarantineReason
	CreatedAt         time.Time
}

// ObserveAccountPresentation binds matching proof evidence or durably quarantines drift.
func (s *Store) ObserveAccountPresentation(account Account, evidence PresentationEvidence) error {
	if err := validatePresentationEvidence(evidence); err != nil {
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	reason := presentationMismatch(current, bound, err == nil, evidence)
	if reason != "" {
		quarantine := AccountPresentationQuarantine{
			AccountID: current.ID, AccountInstanceID: current.InstanceID,
			AccountGeneration: current.Generation, ExpectedConfigDir: current.ConfigDir,
			Observed: evidence, Reason: reason, CreatedAt: s.now(),
		}
		if err := insertAccountPresentationQuarantine(tx, quarantine); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return ErrAccountPresentationQuarantined
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: current.ID, AccountInstanceID: current.InstanceID,
		AccountGeneration: current.Generation, Evidence: evidence, ObservedAt: s.now(),
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

// RebindAccountPresentation atomically installs an operator-approved new path and generation.
func (s *Store) RebindAccountPresentation(
	account Account,
	evidence PresentationEvidence,
	keychainService string,
	keychainAccount string,
) (Account, error) {
	if err := validatePresentationEvidence(evidence); err != nil {
		return Account{}, err
	}
	if keychainService == "" || keychainAccount == "" || evidence.Generation != account.Generation+1 ||
		evidence.PublicPath == account.ConfigDir {
		return Account{}, ErrAccountPresentationEvidence
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Account{}, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := presentationAccount(tx, account.ID)
	if err != nil {
		return Account{}, err
	}
	if !samePresentationAccount(current, account) {
		return Account{}, ErrAccountGenerationChanged
	}
	quarantine, err := accountPresentationQuarantine(tx, account.ID)
	if err != nil {
		return Account{}, err
	}
	if quarantine.AccountInstanceID != account.InstanceID ||
		quarantine.AccountGeneration != account.Generation ||
		quarantine.ExpectedConfigDir != account.ConfigDir ||
		quarantine.Observed.TenantID != evidence.TenantID ||
		quarantine.Observed.DomainID != evidence.DomainID ||
		quarantine.Observed.PublicPath != evidence.PublicPath {
		return Account{}, ErrAccountPresentationEvidence
	}
	busy, err := accountPresentationBusy(tx, account.ID)
	if err != nil {
		return Account{}, err
	}
	if busy {
		return Account{}, ErrAccountPresentationBusy
	}
	result, err := tx.Exec(
		`UPDATE accounts SET generation=?,config_dir=?,keychain_service=?,keychain_account=?
		 WHERE id=? AND instance_id=? AND generation=? AND config_dir=? AND deleted_at IS NULL`,
		evidence.Generation, evidence.PublicPath, keychainService, keychainAccount,
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
	)
	if err != nil {
		return Account{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Account{}, ErrAccountGenerationChanged
	}
	if _, err := tx.Exec(`DELETE FROM account_presentations WHERE account_id=?`, account.ID); err != nil {
		return Account{}, err
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: evidence.Generation, Evidence: evidence, ObservedAt: s.now(),
	}); err != nil {
		return Account{}, err
	}
	result, err = tx.Exec(
		`DELETE FROM account_presentation_quarantines
		 WHERE account_id=? AND account_instance_id=? AND account_generation=?`,
		account.ID, account.InstanceID, account.Generation,
	)
	if err != nil {
		return Account{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return Account{}, ErrAccountPresentationEvidence
	}
	updated, err := presentationAccount(tx, account.ID)
	if err != nil {
		return Account{}, err
	}
	if err := tx.Commit(); err != nil {
		return Account{}, err
	}
	return updated, nil
}

func validatePresentationEvidence(evidence PresentationEvidence) error {
	if evidence.TenantID == "" || evidence.DomainID == "" || evidence.Generation == 0 ||
		evidence.ActivationGeneration == "" || !exactPresentationPath(evidence.PublicPath) ||
		strings.ContainsRune(evidence.TenantID, 0) || strings.ContainsRune(evidence.DomainID, 0) ||
		strings.ContainsRune(evidence.ActivationGeneration, 0) {
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
	hasBinding bool,
	evidence PresentationEvidence,
) AccountPresentationQuarantineReason {
	if evidence.Generation != account.Generation {
		return AccountPresentationGenerationDrift
	}
	if evidence.PublicPath != account.ConfigDir {
		return AccountPresentationPublicPathDrift
	}
	if !hasBinding {
		return ""
	}
	if evidence.TenantID != bound.Evidence.TenantID {
		return AccountPresentationTenantIDDrift
	}
	if evidence.DomainID != bound.Evidence.DomainID {
		return AccountPresentationDomainIDDrift
	}
	if evidence.Generation != bound.Evidence.Generation {
		return AccountPresentationGenerationDrift
	}
	if evidence.PublicPath != bound.Evidence.PublicPath {
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
		 presentation_generation,activation_generation,public_path,observed_at
		 FROM account_presentations WHERE account_id=?`, accountID,
	).Scan(
		&presentation.AccountID, &presentation.AccountInstanceID, &presentation.AccountGeneration,
		&presentation.Evidence.TenantID, &presentation.Evidence.DomainID,
		&presentation.Evidence.Generation, &presentation.Evidence.ActivationGeneration,
		&presentation.Evidence.PublicPath, &observedAt,
	)
	presentation.ObservedAt = time.Unix(0, observedAt)
	return presentation, err
}

func upsertAccountPresentation(tx *sql.Tx, presentation AccountPresentation) error {
	_, err := tx.Exec(
		`INSERT INTO account_presentations(
		 account_id,account_instance_id,account_generation,tenant_id,domain_id,
		 presentation_generation,activation_generation,public_path,observed_at
		 ) VALUES(?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id) DO UPDATE SET
		 activation_generation=excluded.activation_generation,observed_at=excluded.observed_at`,
		presentation.AccountID, presentation.AccountInstanceID, presentation.AccountGeneration,
		presentation.Evidence.TenantID, presentation.Evidence.DomainID,
		presentation.Evidence.Generation, presentation.Evidence.ActivationGeneration,
		presentation.Evidence.PublicPath, presentation.ObservedAt.UnixNano(),
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
		 observed_activation_generation,observed_public_path,reason,created_at
		 FROM account_presentation_quarantines WHERE account_id=?`, accountID,
	).Scan(
		&quarantine.AccountID, &quarantine.AccountInstanceID, &quarantine.AccountGeneration,
		&quarantine.ExpectedConfigDir, &quarantine.Observed.TenantID,
		&quarantine.Observed.DomainID, &quarantine.Observed.Generation,
		&quarantine.Observed.ActivationGeneration, &quarantine.Observed.PublicPath,
		&quarantine.Reason, &createdAt,
	)
	quarantine.CreatedAt = time.Unix(0, createdAt)
	return quarantine, err
}

func insertAccountPresentationQuarantine(tx *sql.Tx, quarantine AccountPresentationQuarantine) error {
	_, err := tx.Exec(
		`INSERT INTO account_presentation_quarantines(
		 account_id,account_instance_id,account_generation,expected_config_dir,
		 observed_tenant_id,observed_domain_id,observed_generation,
		 observed_activation_generation,observed_public_path,reason,created_at
		 ) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO NOTHING`,
		quarantine.AccountID, quarantine.AccountInstanceID, quarantine.AccountGeneration,
		quarantine.ExpectedConfigDir, quarantine.Observed.TenantID,
		quarantine.Observed.DomainID, quarantine.Observed.Generation,
		quarantine.Observed.ActivationGeneration, quarantine.Observed.PublicPath,
		quarantine.Reason, quarantine.CreatedAt.UnixNano(),
	)
	return err
}

func accountPresentationBusy(tx *sql.Tx, accountID int) (bool, error) {
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
