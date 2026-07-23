package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

// AccountPresentationRebindSourceEvidence proves one exact Keychain slot and
// returns its locator, state, and optional token-chain digest.
func (m *Manager) AccountPresentationRebindSourceEvidence(
	ctx context.Context,
	account store.Account,
) (store.CredentialDigest, store.CredentialSlotState, store.CredentialDigest, error) {
	if account.ID < 1 || account.ConfigDir == "" || account.KeychainService == "" ||
		account.KeychainAccount == "" {
		return store.CredentialDigest{}, "", store.CredentialDigest{},
			store.ErrAccountPresentationEvidence
	}
	state, digest, err := m.credentialTokenChainStateAtObservation(ctx, account)
	if err != nil {
		return store.CredentialDigest{}, "", store.CredentialDigest{},
			fmt.Errorf("observe presentation rebind source: %w", err)
	}
	locator := store.CredentialKeychainLocatorDigest(account.KeychainService, account.KeychainAccount)
	switch {
	case state.Keychain.State == store.CredentialSlotEmpty &&
		state.Keychain.Digest == nil && digest == nil:
		return locator, store.CredentialSlotEmpty, store.CredentialDigest{}, nil
	case state.Keychain.State == store.CredentialSlotPresent &&
		state.Keychain.Digest != nil && digest != nil:
		return locator, store.CredentialSlotPresent, *digest, nil
	default:
		return store.CredentialDigest{}, "", store.CredentialDigest{}, ErrCredentialChangedUnderfoot
	}
}

// VerifyAccountPresentationRebindCredential proves the target has one usable
// Keychain owner and no plaintext owner.
func (m *Manager) VerifyAccountPresentationRebindCredential(
	ctx context.Context,
	mutation store.AccountMutation,
) (store.CredentialDigest, error) {
	if err := validatePresentationRebindTarget(mutation); err != nil {
		return store.CredentialDigest{}, err
	}
	state, digest, err := m.credentialTokenChainStateAtObservation(
		ctx, presentationRebindTargetAccount(mutation),
	)
	if err != nil {
		return store.CredentialDigest{}, fmt.Errorf("observe rebound credential: %w", err)
	}
	if state.Keychain.State != store.CredentialSlotPresent || state.Keychain.Digest == nil || digest == nil {
		return store.CredentialDigest{}, ErrCredentialChangedUnderfoot
	}
	credential, err := m.Creds.Store(
		presentationRebindTargetAccount(mutation), creds.SourceKeychain,
	).Read(ctx)
	if err != nil {
		return store.CredentialDigest{}, fmt.Errorf("read rebound Keychain credential: %w", err)
	}
	if !credential.HasRefreshToken() || credential.Expired() {
		return store.CredentialDigest{}, ErrNeedsLogin
	}
	if credentialTokenChainDigest(credential) != *digest {
		return store.CredentialDigest{}, ErrCredentialChangedUnderfoot
	}
	switch mutation.State {
	case store.AccountMutationApplying:
		if *digest == mutation.ExpectedCredentialDigest {
			return store.CredentialDigest{}, ErrCredentialChangedUnderfoot
		}
	case store.AccountMutationApplied, store.AccountMutationPublishing,
		store.AccountMutationRebindPublished:
		if !mutation.CredentialWritten || *digest != mutation.WrittenCredentialDigest {
			return store.CredentialDigest{}, ErrCredentialChangedUnderfoot
		}
	default:
		return store.CredentialDigest{}, store.ErrAccountMutationState
	}
	return *digest, nil
}

// FinalizeAccountPresentationRebind CAS-deletes the exact previous Keychain
// owner and admits an already-published target. An absent previous owner is a
// completed retry; any other previous or target state remains quarantined.
func (m *Manager) FinalizeAccountPresentationRebind(
	ctx context.Context,
	mutation store.AccountMutation,
	receiptExpiresAt time.Time,
) (store.AccountMutationReceipt, error) {
	if mutation.Kind != store.AccountMutationPresentationRebind ||
		mutation.State != store.AccountMutationRebindPublished || !mutation.CredentialWritten ||
		mutation.AccountGeneration < 2 ||
		mutation.PreviousLocatorDigest != store.CredentialKeychainLocatorDigest(
			mutation.PreviousKeychainService, mutation.PreviousKeychainAccount,
		) ||
		(mutation.PreviousCredentialState == store.CredentialSlotEmpty &&
			mutation.PreviousCredentialDigest != (store.CredentialDigest{})) ||
		(mutation.PreviousCredentialState == store.CredentialSlotPresent &&
			mutation.PreviousCredentialDigest == (store.CredentialDigest{})) ||
		(mutation.PreviousCredentialState != store.CredentialSlotEmpty &&
			mutation.PreviousCredentialState != store.CredentialSlotPresent) {
		return store.AccountMutationReceipt{}, store.ErrAccountMutationState
	}
	if m.credentialCAS == nil {
		return store.AccountMutationReceipt{}, errors.New("credential CAS worker is unavailable")
	}
	if _, err := m.VerifyAccountPresentationRebindCredential(ctx, mutation); err != nil {
		return store.AccountMutationReceipt{}, err
	}
	oldAccount := presentationRebindPreviousAccount(mutation)
	oldState, oldDigest, err := m.credentialTokenChainStateAtObservation(ctx, oldAccount)
	if err != nil {
		return store.AccountMutationReceipt{}, fmt.Errorf("observe previous credential: %w", err)
	}
	switch mutation.PreviousCredentialState {
	case store.CredentialSlotEmpty:
		if oldState.Keychain.State != store.CredentialSlotEmpty || oldDigest != nil {
			return store.AccountMutationReceipt{}, ErrCredentialChangedUnderfoot
		}
	case store.CredentialSlotPresent:
		switch {
		case oldState.Keychain.State == store.CredentialSlotEmpty && oldDigest == nil:
		case oldState.Keychain.State == store.CredentialSlotPresent &&
			oldState.Keychain.Digest != nil && oldDigest != nil &&
			*oldDigest == mutation.PreviousCredentialDigest:
			proof, casErr := m.credentialCAS(ctx, oldAccount, oldState, credentialCASMutation{
				Delete: true,
			})
			if casErr != nil {
				if errors.Is(casErr, errCredentialCASConflict) {
					return store.AccountMutationReceipt{}, ErrCredentialChangedUnderfoot
				}
				return store.AccountMutationReceipt{}, fmt.Errorf("delete previous Keychain credential: %w", casErr)
			}
			if proof.After.Keychain.State != store.CredentialSlotEmpty {
				return store.AccountMutationReceipt{}, ErrCredentialChangedUnderfoot
			}
		default:
			return store.AccountMutationReceipt{}, ErrCredentialChangedUnderfoot
		}
	default:
		return store.AccountMutationReceipt{}, store.ErrAccountMutationState
	}
	oldState, oldDigest, err = m.credentialTokenChainStateAtObservation(ctx, oldAccount)
	if err != nil {
		return store.AccountMutationReceipt{}, fmt.Errorf("verify previous credential absent: %w", err)
	}
	if oldState.Keychain.State != store.CredentialSlotEmpty || oldDigest != nil {
		return store.AccountMutationReceipt{}, ErrCredentialChangedUnderfoot
	}
	if _, err := m.VerifyAccountPresentationRebindCredential(ctx, mutation); err != nil {
		return store.AccountMutationReceipt{}, err
	}
	receipt, err := m.Store.CommitAccountPresentationRebind(mutation.Fence(), receiptExpiresAt)
	if err != nil {
		return store.AccountMutationReceipt{}, fmt.Errorf("admit rebound presentation: %w", err)
	}
	return receipt, nil
}

func validatePresentationRebindTarget(mutation store.AccountMutation) error {
	if mutation.Kind != store.AccountMutationPresentationRebind ||
		mutation.ConfigDir == "" || mutation.KeychainService == "" || mutation.KeychainAccount == "" ||
		mutation.LocatorDigest != store.CredentialKeychainLocatorDigest(
			mutation.KeychainService, mutation.KeychainAccount,
		) {
		return store.ErrAccountPresentationEvidence
	}
	return nil
}

func presentationRebindTargetAccount(mutation store.AccountMutation) store.Account {
	return store.Account{
		ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
		Generation: mutation.AccountGeneration, ConfigDir: mutation.ConfigDir,
		KeychainService: mutation.KeychainService, KeychainAccount: mutation.KeychainAccount,
		Label: mutation.Label, AccountUUID: mutation.AccountUUID,
	}
}

func presentationRebindPreviousAccount(mutation store.AccountMutation) store.Account {
	return store.Account{
		ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID,
		Generation: mutation.AccountGeneration - 1, ConfigDir: mutation.PreviousConfigDir,
		KeychainService: mutation.PreviousKeychainService,
		KeychainAccount: mutation.PreviousKeychainAccount,
	}
}
