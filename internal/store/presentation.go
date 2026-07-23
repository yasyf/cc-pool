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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	reason := presentationMismatch(current, bound, err == nil, fileProvider)
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

// AdmitSyncedAccount atomically advances the complete presentation proof and
// clears awaiting-origin state only while the account and old proof match.
func (s *Store) AdmitSyncedAccount(
	account Account,
	currentProof PresentationPreparationProof,
	freshProof PresentationPreparationProof,
) (bool, error) {
	if err := validatePresentationPreparationProofForAccount(
		freshProof, account.InstanceID, account.Generation, account.ConfigDir,
	); err != nil {
		return false, err
	}
	if err := ValidatePresentationPreparationProofAdvance(currentProof, freshProof); err != nil {
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
	if _, err := accountPresentationQuarantine(tx, account.ID); err == nil {
		return false, ErrAccountPresentationQuarantined
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	bound, err := accountPresentation(tx, account.ID)
	if err != nil {
		return false, err
	}
	if bound.AccountInstanceID != account.InstanceID ||
		bound.AccountGeneration != account.Generation || bound.Proof != currentProof {
		return false, ErrAccountPresentationEvidence
	}
	if err := upsertAccountPresentation(tx, AccountPresentation{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, Proof: freshProof, ObservedAt: s.now(),
	}); err != nil {
		return false, err
	}
	result, err := tx.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, reason='none',
		 digest=zeroblob(32), kind='owned'
		 WHERE account_id=? AND needs_login=1 AND kind='awaiting_origin'`,
		account.ID,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, ErrAccountPresentationEvidence
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
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
	hasBinding bool,
	fileProvider FileProviderPreparationProof,
) AccountPresentationQuarantineReason {
	if fileProvider.Generation != account.Generation {
		return AccountPresentationGenerationDrift
	}
	if fileProvider.PublicPath != account.ConfigDir {
		return AccountPresentationPublicPathDrift
	}
	if !hasBinding {
		return ""
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
