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
	if err := EnsureAccountsDir(); err != nil {
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
	Reservation     store.PendingAccountReservation
	ConfigDir       string
	KeychainService string
	ClaudeJSONSeed  SeedOutcome
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
		ClaudeJSONSeed: seed,
	}, nil
}

// FinalizeAdd verifies the logged-in identity and atomically promotes its reservation.
func (m *Manager) FinalizeAdd(ctx context.Context, pending *PendingAdd, label string) (*store.Account, error) {
	if pending == nil {
		return nil, errors.New("finalize add: pending account is nil")
	}
	if _, err := m.AccountIdentity(
		ctx, pending.Reservation.ID, pending.ConfigDir,
	); err != nil {
		if errors.Is(err, ErrNoIdentity) {
			return nil, fmt.Errorf("login didn't complete for %s: %w", pending.ConfigDir, ErrNoIdentity)
		}
		return nil, fmt.Errorf("read account identity for %s: %w", pending.ConfigDir, err)
	}
	account := store.Account{
		ID: pending.Reservation.ID, InstanceID: pending.Reservation.InstanceID,
		Generation: pending.Reservation.Generation,
		ConfigDir:  pending.ConfigDir, KeychainService: pending.KeychainService,
		Label: label, CreatedAt: time.Now(),
	}
	source := creds.SourceKeychain
	keychainAccount, err := m.Creds.Discover(ctx, pending.KeychainService)
	switch {
	case err == nil:
		account.KeychainAccount = keychainAccount
	case errors.Is(err, creds.ErrNotFound):
		if _, readErr := m.Creds.Store(account, creds.SourceFile).Read(ctx); readErr != nil {
			if errors.Is(readErr, creds.ErrNotFound) {
				return nil, fmt.Errorf("no credential found for %s", pending.ConfigDir)
			}
			return nil, readErr
		}
		account.KeychainAccount = creds.AccountLabel()
		source = creds.SourceFile
	default:
		return nil, err
	}
	if source == creds.SourceKeychain {
		item := m.Creds.Store(account, source)
		credential, err := item.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("re-assert keychain item: %w", err)
		}
		if err := item.Write(ctx, credential); err != nil {
			return nil, fmt.Errorf("re-assert keychain item: %w", err)
		}
	}
	if err := m.Store.PromoteReservedAccount(pending.Reservation, account); err != nil {
		return nil, fmt.Errorf("finalize %s: %w", pending.ConfigDir, err)
	}
	committed, err := m.Store.GetAccount(account.ID)
	if err != nil {
		return nil, err
	}
	if _, _, _, err := m.SampleUsage(ctx, committed, SampleOpts{AllowRefresh: true}); err != nil {
		return &committed, fmt.Errorf("account added but usage validation failed: %w", err)
	}
	return &committed, nil
}

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
	account := store.Account{
		ID: pending.Reservation.ID, InstanceID: pending.Reservation.InstanceID,
		Generation: pending.Reservation.Generation, ConfigDir: pending.ConfigDir,
		KeychainService: pending.KeychainService, KeychainAccount: creds.AccountLabel(),
		Label: label, AccountUUID: accountUUID, CreatedAt: time.Now(),
	}
	if err := m.Store.PromoteReservedSyncedAccount(pending.Reservation, account); err != nil {
		return nil, fmt.Errorf("promote synced account %s: %w", pending.ConfigDir, err)
	}
	committed, err := m.Store.GetAccount(account.ID)
	if err != nil {
		return nil, err
	}
	return &committed, nil
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
	}
	var result error
	keychainAccount, err := m.Creds.Discover(ctx, pending.KeychainService)
	switch {
	case errors.Is(err, creds.ErrNotFound):
	case err != nil:
		result = fmt.Errorf("probe credential for %s: %w", pending.ConfigDir, err)
	default:
		account.KeychainAccount = keychainAccount
		result = m.Creds.Store(account, creds.SourceKeychain).Delete(ctx)
	}
	result = errors.Join(result, m.Creds.Store(account, creds.SourceFile).Delete(ctx))
	result = errors.Join(result, m.removeAccountBacking(ctx, pending.Reservation.ID))
	return errors.Join(result, m.Store.ReleaseAccountIndex(pending.Reservation))
}

// Remove deletes one deprovisioned account's private data, credential, and source rows.
func (m *Manager) Remove(ctx context.Context, id int, deleteCredential bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	account, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	if m.Creds != nil {
		if _, err := m.credentialMutationObservation(ctx, account); err != nil {
			return fmt.Errorf("remove acct-%02d credential guard: %w", id, err)
		}
	}
	if !deleteCredential {
		_, source, err := m.ReadCredential(ctx, account)
		switch {
		case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrUnavailable):
		case err != nil:
			return fmt.Errorf("resolve acct-%02d credential backend: %w", id, err)
		case source == creds.SourceFile:
			return fmt.Errorf("cannot keep acct-%02d credential: it is stored in the private backing", id)
		}
	}
	if err := m.removeAccountBacking(ctx, account.ID); err != nil {
		return err
	}
	if deleteCredential {
		if err := m.Creds.Store(account, creds.SourceKeychain).Delete(ctx); err != nil {
			return fmt.Errorf("delete keychain item %q: %w", account.KeychainService, err)
		}
	}
	return m.Store.DeleteAccount(id)
}
