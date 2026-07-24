package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"path/filepath"
)

// ValidateAccountCredentialBoundary verifies exact execution identity before credential I/O.
func ValidateAccountCredentialBoundary(account store.Account, expectedPublicPath string) error {
	if account.ID < 1 || account.Generation == 0 || account.KeychainAccount == "" ||
		filepath.Base(account.KeychainAccount) != account.KeychainAccount {
		return errors.New("account credential identity is incomplete")
	}
	if err := ValidateAccountExecutionIdentity(account); err != nil {
		return err
	}
	if account.KeychainService != creds.ServiceName(account.ConfigDir) {
		return errors.New("account credential identity is not instance-stable")
	}
	if err := ValidateAccountConfigDir(account.InstanceID, expectedPublicPath); err != nil {
		return fmt.Errorf("validate account credential execution link: %w", err)
	}
	return nil
}

func (m *Manager) credentialStore(
	account store.Account,
	expectedPublicPath string,
) (creds.Store, error) {
	if expectedPublicPath == "" {
		var err error
		expectedPublicPath, err = m.validateStoredCredentialBoundary(account)
		if err != nil {
			return nil, err
		}
	} else if err := ValidateAccountCredentialBoundary(account, expectedPublicPath); err != nil {
		return nil, err
	}
	return m.Creds.Store(account, creds.SourceKeychain), nil
}

// DiscoverCredentialAccount validates exact execution identity before Keychain discovery.
func (m *Manager) DiscoverCredentialAccount(
	ctx context.Context,
	account store.Account,
	expectedPublicPath string,
) (string, error) {
	if err := ValidateAccountCredentialBoundary(account, expectedPublicPath); err != nil {
		return "", err
	}
	return m.Creds.Discover(ctx, account.KeychainService)
}

func (m *Manager) credentialPublicPath(account store.Account) (string, error) {
	if m == nil || m.Store == nil {
		return "", errors.New("account credential presentation store is unavailable")
	}
	presentation, err := m.Store.AccountPresentation(account.ID)
	if err == nil {
		if presentation.AccountInstanceID != account.InstanceID ||
			presentation.AccountGeneration != account.Generation {
			return "", store.ErrAccountGenerationChanged
		}
		return presentation.Identity.PublicPath, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	mutation, err := m.Store.ActiveAccountMutation(account.ID)
	if err != nil {
		return "", err
	}
	if mutation.AccountInstanceID != account.InstanceID ||
		mutation.AccountGeneration != account.Generation ||
		mutation.PresentationIdentity.PublicPath == "" {
		return "", store.ErrAccountGenerationChanged
	}
	return mutation.PresentationIdentity.PublicPath, nil
}

func (m *Manager) validateStoredCredentialBoundary(account store.Account) (string, error) {
	publicPath, err := m.credentialPublicPath(account)
	if err != nil {
		return "", err
	}
	if err := ValidateAccountCredentialBoundary(account, publicPath); err != nil {
		return "", err
	}
	return publicPath, nil
}
