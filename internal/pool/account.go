package pool

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

// ErrNotInitialized means the pool has not been set up yet.
var ErrNotInitialized = errors.New("pool not initialized")

// InitResult summarizes one exact pool initialization.
type InitResult struct {
	Already bool
}

// Init prepares only cc-pool-owned private state. FuseKit owns every presentation.
func (m *Manager) Init() (*InitResult, error) {
	if err := EnsureStateDir(); err != nil {
		return nil, err
	}
	already, err := m.Initialized()
	if err != nil {
		return nil, err
	}
	if err := m.Store.SetMeta(metaInitialized, "1"); err != nil {
		return nil, err
	}
	return &InitResult{Already: already}, nil
}

// PendingAdd is one private account backing awaiting interactive login.
type PendingAdd struct {
	Reservation          store.PendingAccountReservation
	ConfigDir            string
	PublicPath           string
	KeychainService      string
	ClaudeJSONSeed       SeedOutcome
	PresentationIdentity store.FileProviderPresentationIdentity
}

// PendingAddRetirementProof binds one verified FuseKit retirement to the
// exact pending reservation and public presentation path it retired.
type PendingAddRetirementProof struct {
	AccountID         int
	AccountInstanceID string
	AccountGeneration uint64
	PublicPath        string
}

var abandonAddFailpoint func(string) error

var finishAccountRemovalFailpoint func(string) error

// DuplicateIdentity returns an existing account sharing want's subscription.
func (m *Manager) DuplicateIdentity(ctx context.Context, want Identity) (*store.Account, error) {
	accounts, err := m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	for index := range accounts {
		identity, err := m.AccountIdentity(
			ctx, accounts[index].ID, accounts[index].ConfigDir,
		)
		if errors.Is(err, ErrNoIdentity) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read acct-%02d identity: %w", accounts[index].ID, err)
		}
		if identity.AccountUUID == want.AccountUUID {
			return &accounts[index], nil
		}
	}
	return nil, nil
}

// ReserveAdd durably allocates the prospective account identity without
// touching its backing or credential stores.
func (m *Manager) ReserveAdd() (store.PendingAccountReservation, error) {
	initialized, err := m.Initialized()
	if err != nil {
		return store.PendingAccountReservation{}, err
	}
	if !initialized {
		return store.PendingAccountReservation{}, ErrNotInitialized
	}
	owner, err := m.MutationOwner()
	if err != nil {
		return store.PendingAccountReservation{}, err
	}
	return m.Store.ReserveAccountIndex(owner)
}

// PrepareReservedAdd seeds one already-journaled prospective account.
func (m *Manager) PrepareReservedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	publicPath string,
) (pending *PendingAdd, err error) {
	return m.prepareReservedAdd(ctx, reservation, publicPath, store.FileProviderPresentationIdentity{})
}

// PrepareReservedSyncedAdd seeds a peer-added account from its immutable presentation identity.
func (m *Manager) PrepareReservedSyncedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	identity store.FileProviderPresentationIdentity,
) (pending *PendingAdd, err error) {
	if err := store.ValidateReservedPresentationIdentity(reservation, identity); err != nil {
		return nil, fmt.Errorf("prepare reserved synced add: %w", err)
	}
	return m.prepareReservedAdd(ctx, reservation, identity.PublicPath, identity)
}

func (m *Manager) prepareReservedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	publicPath string,
	identity store.FileProviderPresentationIdentity,
) (pending *PendingAdd, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(publicPath) || filepath.Clean(publicPath) != publicPath || strings.ContainsRune(publicPath, 0) {
		return nil, errors.New("prepare reserved add: proven public path must be one exact absolute path")
	}
	configDir, err := AccountConfigDir(reservation.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("prepare reserved add: stable config dir: %w", err)
	}
	service, err := AccountKeychainService(reservation.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("prepare reserved add: stable Keychain service: %w", err)
	}
	seed, err := m.prepareAccountBacking(ctx, reservation.ID, ClaudeJSONPath())
	if err != nil {
		return nil, fmt.Errorf("seed .claude.json for %s: %w", publicPath, err)
	}
	if err := EnsureAccountConfigDir(reservation.InstanceID, publicPath); err != nil {
		return nil, fmt.Errorf("prepare reserved add: stable config link: %w", err)
	}
	return &PendingAdd{
		Reservation: reservation, ConfigDir: configDir, PublicPath: publicPath, KeychainService: service,
		ClaudeJSONSeed: seed, PresentationIdentity: identity,
	}, nil
}

var promoteSyncedAddFailpoint func(string) error

// PromoteSyncedAdd publishes one proven-path non-origin account before its
// access-only credential is installed.
func (m *Manager) PromoteSyncedAdd(
	ctx context.Context,
	pending *PendingAdd,
	label string,
	accountUUID string,
) (*store.Account, error) {
	if pending == nil {
		return nil, errors.New("promote synced add: pending account is nil")
	}
	identity, err := m.AccountIdentity(ctx, pending.Reservation.ID, pending.ConfigDir)
	if err != nil {
		return nil, fmt.Errorf("read synced identity for %s: %w", pending.ConfigDir, err)
	}
	if accountUUID == "" || identity.AccountUUID != accountUUID {
		return nil, fmt.Errorf(
			"promote synced add: external UUID mismatch: registry %q, identity %q",
			accountUUID, identity.AccountUUID,
		)
	}
	account := syncedPromotionAccount(pending, label, accountUUID)
	if err := m.Store.PromoteReservedSyncedAccount(
		pending.Reservation, account, pending.PresentationIdentity,
	); err != nil {
		return nil, fmt.Errorf("promote synced account %s: %w", pending.ConfigDir, err)
	}
	if promoteSyncedAddFailpoint != nil {
		if err := promoteSyncedAddFailpoint("after-commit"); err != nil {
			return nil, err
		}
	}
	committed, exact, err := m.ResolvePromotedSyncedAdd(pending, label, accountUUID)
	if err != nil {
		return nil, err
	}
	if !exact {
		return nil, store.ErrSyncedPromotionAmbiguous
	}
	return committed, nil
}

// ResolvePromotedSyncedAdd proves whether an interrupted promotion committed
// exactly or retains its untouched reservation and is safe to abandon.
func (m *Manager) ResolvePromotedSyncedAdd(
	pending *PendingAdd,
	label string,
	accountUUID string,
) (*store.Account, bool, error) {
	if pending == nil || accountUUID == "" {
		return nil, false, errors.New("resolve synced promotion: incomplete expected account")
	}
	expected := syncedPromotionAccount(pending, label, accountUUID)
	account, exact, err := m.Store.ResolveReservedSyncedPromotion(
		pending.Reservation, expected, pending.PresentationIdentity,
	)
	if err != nil || !exact {
		return nil, exact, err
	}
	return &account, true, nil
}

func syncedPromotionAccount(pending *PendingAdd, label, accountUUID string) store.Account {
	return store.Account{
		ID: pending.Reservation.ID, InstanceID: pending.Reservation.InstanceID,
		Generation: pending.Reservation.Generation, ConfigDir: pending.ConfigDir,
		KeychainService: pending.KeychainService, KeychainAccount: creds.AccountLabel(),
		Label: label, AccountUUID: accountUUID, CreatedAt: time.Now(),
	}
}

// AbandonAdd removes an exactly retired uncommitted account's execution link,
// credentials, private backing, and reservation.
func (m *Manager) AbandonAdd(
	ctx context.Context,
	pending *PendingAdd,
	retirement PendingAddRetirementProof,
) error {
	if pending == nil {
		return errors.New("abandon add: pending account is nil")
	}
	if err := validatePendingAddRetirement(
		pending.Reservation, pending.PublicPath, retirement,
	); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	account := store.Account{
		ID: pending.Reservation.ID, InstanceID: pending.Reservation.InstanceID,
		Generation: pending.Reservation.Generation,
		ConfigDir:  pending.ConfigDir, KeychainService: pending.KeychainService,
		KeychainAccount: creds.AccountLabel(),
	}
	settled, err := m.credentialRemovalSettled(account)
	if err != nil {
		return fmt.Errorf("read pending credential retirement evidence: %w", err)
	}
	if !settled {
		if err := ValidateAccountCredentialBoundary(account, pending.PublicPath); err != nil {
			return fmt.Errorf("validate retired pending credential boundary: %w", err)
		}
		if err := m.removeCredentialForAccountRemovalAt(ctx, account, pending.PublicPath); err != nil {
			return fmt.Errorf("retire pending credential for %s: %w", pending.ConfigDir, err)
		}
	}
	if abandonAddFailpoint != nil {
		if err := abandonAddFailpoint("after-credential"); err != nil {
			return err
		}
	}
	if err := RemoveAccountConfigDir(
		pending.Reservation.InstanceID, pending.PublicPath,
	); err != nil {
		return fmt.Errorf("unlink retired pending execution path: %w", err)
	}
	if abandonAddFailpoint != nil {
		if err := abandonAddFailpoint("after-unlink"); err != nil {
			return err
		}
	}
	if err := m.removeAccountBacking(ctx, pending.Reservation.ID); err != nil {
		return err
	}
	if abandonAddFailpoint != nil {
		if err := abandonAddFailpoint("after-backing"); err != nil {
			return err
		}
	}
	return m.Store.ReleaseAccountIndex(pending.Reservation)
}

func (m *Manager) credentialRemovalSettled(account store.Account) (bool, error) {
	intent, err := store.CredentialRemovalIntentDigest(
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount,
	)
	if err != nil {
		return false, err
	}
	active, receipt, err := m.Store.CredentialOperationEvidence(store.CredentialOperationEvidenceQuery{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		LocatorDigest: store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		),
		Kind: store.CredentialOperationRemove, Target: store.CredentialTargetKeychain,
		IntentDigest: intent,
	})
	if err != nil || active != nil || receipt == nil {
		return false, err
	}
	return receipt.TerminalStatus == store.CredentialTerminalSucceeded &&
		receipt.Result == store.CredentialResultDone &&
		!receipt.AcknowledgedAt.IsZero(), nil
}

// AbandonReservedAdd removes an exactly retired reservation whose preparation
// did not return a complete PendingAdd.
func (m *Manager) AbandonReservedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	retirement PendingAddRetirementProof,
) error {
	pending, err := pendingAddForReservation(reservation, retirement.PublicPath)
	if err != nil {
		return err
	}
	return m.AbandonAdd(ctx, pending, retirement)
}

// AbandonPreparedAdd removes pool backing and an exact stable link after
// preparation failed before any credential operation could begin.
func (m *Manager) AbandonPreparedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	retirement PendingAddRetirementProof,
) error {
	if err := validatePendingAddRetirement(reservation, retirement.PublicPath, retirement); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := RemoveAccountConfigDir(reservation.InstanceID, retirement.PublicPath); err != nil {
		return fmt.Errorf("unlink prepared pending execution path: %w", err)
	}
	if err := m.removeAccountBacking(ctx, reservation.ID); err != nil {
		return err
	}
	return m.Store.ReleaseAccountIndex(reservation)
}

// FinalizeUnpreparedAdd releases a reservation after presentation retirement
// proved that preparation failed before pool-owned external I/O began.
func (m *Manager) FinalizeUnpreparedAdd(
	reservation store.PendingAccountReservation,
	retirement PendingAddRetirementProof,
) error {
	if err := validatePendingAddRetirement(reservation, retirement.PublicPath, retirement); err != nil {
		return err
	}
	return m.Store.ReleaseAccountIndex(reservation)
}

func pendingAddForReservation(
	reservation store.PendingAccountReservation,
	publicPath string,
) (*PendingAdd, error) {
	configDir, err := AccountConfigDir(reservation.InstanceID)
	if err != nil {
		return nil, err
	}
	service, err := AccountKeychainService(reservation.InstanceID)
	if err != nil {
		return nil, err
	}
	return &PendingAdd{
		Reservation: reservation, ConfigDir: configDir,
		PublicPath: publicPath, KeychainService: service,
	}, nil
}

func validatePendingAddRetirement(
	reservation store.PendingAccountReservation,
	publicPath string,
	retirement PendingAddRetirementProof,
) error {
	if retirement.AccountID != reservation.ID ||
		retirement.AccountInstanceID != reservation.InstanceID ||
		retirement.AccountGeneration != reservation.Generation ||
		retirement.PublicPath != publicPath {
		return errors.New("pending add retirement proof does not match reservation")
	}
	if !filepath.IsAbs(publicPath) || filepath.Clean(publicPath) != publicPath || strings.ContainsRune(publicPath, 0) {
		return errors.New("pending add retirement proof has invalid public path")
	}
	return nil
}

// FinishAccountRemoval settles one exact deprovisioned account removal.
func (m *Manager) FinishAccountRemoval(
	ctx context.Context,
	removal store.AccountRemoval,
	expectedPublicPath string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	account, err := m.Store.GetAccount(removal.AccountID)
	pending := false
	if errors.Is(err, store.ErrAccountNotFound) {
		account, err = m.Store.CredentialRemovalSubject(removal)
		pending = true
	}
	if err != nil {
		return err
	}
	if account.ID != removal.AccountID || account.InstanceID != removal.AccountInstanceID ||
		account.Generation != removal.AccountGeneration {
		return store.ErrAccountGenerationChanged
	}
	if removal.DeleteCredential {
		settled, err := m.credentialRemovalSettled(account)
		if err != nil {
			return fmt.Errorf("read acct-%02d credential retirement evidence: %w", account.ID, err)
		}
		if !settled {
			if err := ValidateAccountCredentialBoundary(account, expectedPublicPath); err != nil {
				return fmt.Errorf("validate acct-%02d credential boundary: %w", account.ID, err)
			}
			if err := m.removeCredentialForAccountRemovalAt(ctx, account, expectedPublicPath); err != nil {
				return fmt.Errorf("retire acct-%02d credential: %w", account.ID, err)
			}
		}
	}
	if finishAccountRemovalFailpoint != nil {
		if err := finishAccountRemovalFailpoint("after-credential"); err != nil {
			return err
		}
	}
	if err := RemoveAccountConfigDir(account.InstanceID, expectedPublicPath); err != nil {
		return fmt.Errorf("retire acct-%02d execution link: %w", account.ID, err)
	}
	if finishAccountRemovalFailpoint != nil {
		if err := finishAccountRemovalFailpoint("after-unlink"); err != nil {
			return err
		}
	}
	if err := m.removeAccountBacking(ctx, account.ID); err != nil {
		return err
	}
	if finishAccountRemovalFailpoint != nil {
		if err := finishAccountRemovalFailpoint("after-backing"); err != nil {
			return err
		}
	}
	if pending {
		return m.Store.FinalizePendingAccountRemoval(removal)
	}
	return m.Store.DeleteAccount(account.ID)
}
