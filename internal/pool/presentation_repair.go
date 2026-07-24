package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/store"
)

// ValidateAccountExecutionIdentity requires one immutable instance-derived Claude identity.
func ValidateAccountExecutionIdentity(account store.Account) error {
	configDir, err := AccountConfigDir(account.InstanceID)
	if err != nil {
		return err
	}
	service, err := AccountKeychainService(account.InstanceID)
	if err != nil {
		return err
	}
	if account.ConfigDir != configDir || account.KeychainService != service || account.KeychainAccount == "" {
		return errors.New("account execution identity is not instance-derived")
	}
	return nil
}

// ReconcileAccountPresentation durably repairs one verified public-path change.
func (m *Manager) ReconcileAccountPresentation(
	ctx context.Context,
	account store.Account,
	verified store.FileProviderPresentationIdentity,
) (store.AccountPresentation, error) {
	if err := ctx.Err(); err != nil {
		return store.AccountPresentation{}, err
	}
	if err := ValidateAccountExecutionIdentity(account); err != nil {
		return store.AccountPresentation{}, err
	}
	err := m.Store.ObserveAccountPresentation(account, verified)
	if err == nil {
		if err := EnsureAccountConfigDir(account.InstanceID, verified.PublicPath); err != nil {
			return store.AccountPresentation{}, err
		}
		return m.Store.AccountPresentation(account.ID)
	}
	if !errors.Is(err, store.ErrAccountPresentationQuarantined) {
		return store.AccountPresentation{}, err
	}
	repair, err := m.Store.StageAccountPresentationRepair(account, verified)
	if err != nil {
		return store.AccountPresentation{}, err
	}
	return m.finishAccountPresentationRepair(ctx, repair)
}

// RecoverAccountPresentationRepairs finishes every path repair staged before a restart.
func (m *Manager) RecoverAccountPresentationRepairs(ctx context.Context) error {
	repairs, err := m.Store.PendingAccountPresentationRepairs()
	if err != nil {
		return err
	}
	for _, repair := range repairs {
		if _, err := m.finishAccountPresentationRepair(ctx, repair); err != nil {
			return fmt.Errorf("recover acct-%02d presentation repair: %w", repair.AccountID, err)
		}
	}
	return nil
}

// RecoverAccountConfigDir recreates one missing stable link from its committed binding.
func (m *Manager) RecoverAccountConfigDir(account store.Account) error {
	if err := ValidateAccountExecutionIdentity(account); err != nil {
		return err
	}
	if _, err := m.Store.AccountPresentationRepair(account.ID); err == nil {
		return store.ErrAccountPresentationBusy
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	presentation, err := m.Store.AccountPresentation(account.ID)
	if err != nil {
		return err
	}
	if presentation.AccountInstanceID != account.InstanceID ||
		presentation.AccountGeneration != account.Generation {
		return store.ErrAccountPresentationEvidence
	}
	return EnsureAccountConfigDir(account.InstanceID, presentation.Identity.PublicPath)
}

func (m *Manager) finishAccountPresentationRepair(
	ctx context.Context,
	repair store.AccountPresentationRepair,
) (store.AccountPresentation, error) {
	if err := ctx.Err(); err != nil {
		return store.AccountPresentation{}, err
	}
	account, err := m.Store.GetAccount(repair.AccountID)
	if err != nil {
		return store.AccountPresentation{}, err
	}
	if err := ValidateAccountExecutionIdentity(account); err != nil {
		return store.AccountPresentation{}, err
	}
	if account.InstanceID != repair.AccountInstanceID || account.Generation != repair.AccountGeneration {
		return store.AccountPresentation{}, store.ErrAccountGenerationChanged
	}
	if err := RepairAccountConfigDir(
		repair.AccountInstanceID, repair.Previous.PublicPath, repair.Target.PublicPath,
	); err != nil {
		return store.AccountPresentation{}, err
	}
	return m.Store.CommitAccountPresentationRepair(repair)
}
