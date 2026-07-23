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
	Reservation       store.PendingAccountReservation
	ConfigDir         string
	KeychainService   string
	ClaudeJSONSeed    SeedOutcome
	PresentationProof store.PresentationPreparationProof
}

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
	configDir string,
) (pending *PendingAdd, err error) {
	return m.prepareReservedAdd(ctx, reservation, configDir, store.PresentationPreparationProof{})
}

// PrepareReservedSyncedAdd seeds a peer-added account only from its complete
// generation-fenced FuseKit presentation proof.
func (m *Manager) PrepareReservedSyncedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	proof store.PresentationPreparationProof,
) (pending *PendingAdd, err error) {
	if err := store.ValidateReservedPresentationPreparationProof(reservation, proof); err != nil {
		return nil, fmt.Errorf("prepare reserved synced add: %w", err)
	}
	return m.prepareReservedAdd(ctx, reservation, proof.FileProvider.PublicPath, proof)
}

func (m *Manager) prepareReservedAdd(
	ctx context.Context,
	reservation store.PendingAccountReservation,
	configDir string,
	proof store.PresentationPreparationProof,
) (pending *PendingAdd, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(configDir) || filepath.Clean(configDir) != configDir || strings.ContainsRune(configDir, 0) {
		return nil, errors.New("prepare reserved add: proven config dir must be one exact absolute path")
	}
	seed, err := m.prepareAccountBacking(ctx, reservation.ID, ClaudeJSONPath())
	if err != nil {
		return nil, fmt.Errorf("seed .claude.json for %s: %w", configDir, err)
	}
	service := creds.ServiceName(configDir)
	return &PendingAdd{
		Reservation: reservation, ConfigDir: configDir, KeychainService: service,
		ClaudeJSONSeed: seed, PresentationProof: proof,
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
		pending.Reservation, account, pending.PresentationProof,
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
		pending.Reservation, expected, pending.PresentationProof,
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

// ReleaseAdd releases the index while retaining its private login state.
func (m *Manager) ReleaseAdd(pending *PendingAdd) error {
	if pending == nil {
		return errors.New("release add: pending account is nil")
	}
	return m.Store.ReleaseAccountIndex(pending.Reservation)
}

// AbandonAdd removes an uncommitted account's credentials and private backing.
func (m *Manager) AbandonAdd(ctx context.Context, pending *PendingAdd) error {
	if pending == nil {
		return errors.New("abandon add: pending account is nil")
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
	keychainAccount, err := m.Creds.Discover(ctx, pending.KeychainService)
	switch {
	case errors.Is(err, creds.ErrNotFound):
	case err != nil:
		return fmt.Errorf("probe credential for %s: %w", pending.ConfigDir, err)
	default:
		account.KeychainAccount = keychainAccount
	}
	if err := m.removeCredentialForAccountRemoval(ctx, account); err != nil {
		return fmt.Errorf("retire pending credential for %s: %w", pending.ConfigDir, err)
	}
	removal, err := m.Store.BeginAccountRemoval(pending.Reservation.ID, true)
	if err != nil {
		return fmt.Errorf("read pending removal for %s: %w", pending.ConfigDir, err)
	}
	if err := m.removeAccountBacking(ctx, pending.Reservation.ID); err != nil {
		return err
	}
	return m.Store.FinalizePendingAccountRemoval(removal)
}

// FinishAccountRemoval settles one exact deprovisioned account removal.
func (m *Manager) FinishAccountRemoval(ctx context.Context, removal store.AccountRemoval) error {
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
		if err := m.removeCredentialForAccountRemoval(ctx, account); err != nil {
			return fmt.Errorf("retire acct-%02d credential: %w", account.ID, err)
		}
	}
	if err := m.removeAccountBacking(ctx, account.ID); err != nil {
		return err
	}
	if pending {
		return m.Store.FinalizePendingAccountRemoval(removal)
	}
	return m.Store.DeleteAccount(account.ID)
}
