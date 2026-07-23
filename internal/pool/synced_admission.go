package pool

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

var syncedAdmissionFailpoint func(string)
var syncedAdmissionResultFailpoint func(string) error

// AdmitSyncedCredential serializes credential validation with every credential
// mutation and advances presentation and admission through durable exact evidence.
func (m *Manager) AdmitSyncedCredential(
	ctx context.Context,
	account store.Account,
	currentProof store.PresentationPreparationProof,
	freshProof store.PresentationPreparationProof,
	expectedAccessHash string,
) (bool, error) {
	accessHashBytes, err := hex.DecodeString(expectedAccessHash)
	if err != nil || len(accessHashBytes) != sha256.Size {
		return false, errors.New("synced credential admission requires an exact access hash")
	}
	var accessHashDigest store.CredentialDigest
	copy(accessHashDigest[:], accessHashBytes)
	signature := syncedAdmissionSignature(account, accessHashDigest)
	for {
		flight, leader := m.joinCredentialFlight(account.ID, signature)
		if !leader {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-flight.done:
			}
			if flight.signature != signature {
				continue
			}
			result, ok := flight.result.(bool)
			if !ok {
				return false, errors.Join(errors.New("synced credential admission result type mismatch"), flight.err)
			}
			return result, flight.err
		}
		admitted, admissionErr := m.admitSyncedCredentialClaimed(
			ctx, account, currentProof, freshProof, accessHashDigest, expectedAccessHash,
		)
		m.finishCredentialFlight(account.ID, flight, admitted, admissionErr)
		return admitted, admissionErr
	}
}

func (m *Manager) admitSyncedCredentialClaimed(
	ctx context.Context,
	account store.Account,
	currentProof store.PresentationPreparationProof,
	freshProof store.PresentationPreparationProof,
	accessHashDigest store.CredentialDigest,
	expectedAccessHash string,
) (bool, error) {
	if m.ClaimCredentialMutation == nil {
		return false, errors.New("synced credential admission requires credential mutation authority")
	}
	release, err := m.ClaimCredentialMutation(account.ID)
	if err != nil {
		return false, err
	}
	if release == nil {
		return false, errors.New("credential mutation authority returned a nil release")
	}
	defer release()
	return m.admitSyncedCredential(
		ctx, account, currentProof, freshProof, accessHashDigest, expectedAccessHash,
	)
}

func (m *Manager) admitSyncedCredential(
	ctx context.Context,
	account store.Account,
	currentProof store.PresentationPreparationProof,
	freshProof store.PresentationPreparationProof,
	accessHashDigest store.CredentialDigest,
	expectedAccessHash string,
) (admitted bool, resultErr error) {
	lease, err := acquireCredentialRefreshLocks(ctx, account.ID, account.ConfigDir)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Release(ctx)) }()
	if err := m.requireCredentialMutationAllowed(account); err != nil {
		return false, err
	}
	current, err := m.Store.GetAccount(account.ID)
	if err != nil {
		return false, err
	}
	if !sameAccountCredentialFence(current, account) {
		return false, store.ErrAccountGenerationChanged
	}

	observed, tokenChain, err := m.credentialTokenChainStateAtObservation(ctx, account)
	if err != nil {
		return false, err
	}
	if !credentialObservationHasPresent(observed) || tokenChain == nil {
		return false, ErrCredentialChangedUnderfoot
	}
	credential, _, err := m.ReadCredential(ctx, account)
	if err != nil {
		return false, err
	}
	if credential.HasRefreshToken() || !credential.Synced() ||
		creds.AccessHash(credential) != expectedAccessHash ||
		credentialTokenChainDigest(credential) != *tokenChain {
		return false, ErrCredentialChangedUnderfoot
	}
	if syncedAdmissionFailpoint != nil {
		syncedAdmissionFailpoint("before-final-observation")
	}
	verified, verifiedTokenChain, err := m.credentialTokenChainStateAtObservation(ctx, account)
	if err != nil {
		return false, err
	}
	if !sameStoreObservation(observed, verified) || verifiedTokenChain == nil ||
		*verifiedTokenChain != *tokenChain {
		return false, ErrCredentialChangedUnderfoot
	}
	externalStateDigest, err := verified.Digest()
	if err != nil {
		return false, err
	}
	fence := store.SyncedCredentialAdmissionFence{
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		),
		ExternalStateDigest: externalStateDigest,
		TokenChainDigest:    *verifiedTokenChain,
		AccessHashDigest:    accessHashDigest,
	}
	stage, err := m.Store.StageSyncedAccountAdmission(account, currentProof, freshProof, fence)
	if err != nil {
		return false, err
	}
	if syncedAdmissionFailpoint != nil {
		syncedAdmissionFailpoint("after-stage")
	}
	if syncedAdmissionResultFailpoint != nil {
		if err := syncedAdmissionResultFailpoint("after-stage"); err != nil {
			return false, err
		}
	}
	if !stage.Finalized {
		postStage, postStageTokenChain, err := m.credentialTokenChainStateAtObservation(ctx, account)
		if err != nil {
			return false, err
		}
		if !sameStoreObservation(verified, postStage) || postStageTokenChain == nil ||
			*postStageTokenChain != *verifiedTokenChain {
			return false, ErrCredentialChangedUnderfoot
		}
		if syncedAdmissionFailpoint != nil {
			syncedAdmissionFailpoint("after-post-stage-observation")
		}
		admitted, err := m.Store.FinalizeSyncedAccountAdmission(account, freshProof, fence)
		if err != nil {
			return false, err
		}
		if !admitted {
			return false, store.ErrAccountPresentationEvidence
		}
		if syncedAdmissionResultFailpoint != nil {
			if err := syncedAdmissionResultFailpoint("after-finalize"); err != nil {
				return false, err
			}
		}
	}
	if syncedAdmissionFailpoint != nil {
		syncedAdmissionFailpoint("before-post-finalize-observation")
	}
	postFinalize, postFinalizeTokenChain, err := m.credentialTokenChainStateAtObservation(ctx, account)
	if err == nil && sameStoreObservation(verified, postFinalize) &&
		postFinalizeTokenChain != nil && *postFinalizeTokenChain == *verifiedTokenChain {
		return true, nil
	}
	reopenErr := m.Store.ReopenSyncedAccountAdmission(account, freshProof, fence)
	if err != nil {
		return false, errors.Join(err, reopenErr)
	}
	return false, errors.Join(ErrCredentialChangedUnderfoot, reopenErr)
}

func syncedAdmissionSignature(
	account store.Account,
	accessHashDigest store.CredentialDigest,
) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("cc-pool:synced-credential-admission:v1"))
	_, _ = hash.Write([]byte(account.InstanceID))
	var generation [8]byte
	binary.BigEndian.PutUint64(generation[:], account.Generation)
	_, _ = hash.Write(generation[:])
	_, _ = hash.Write(accessHashDigest[:])
	var signature [32]byte
	copy(signature[:], hash.Sum(nil))
	return signature
}

func sameAccountCredentialFence(left, right store.Account) bool {
	return left.ID == right.ID && left.InstanceID == right.InstanceID &&
		left.Generation == right.Generation && left.ConfigDir == right.ConfigDir &&
		left.KeychainService == right.KeychainService &&
		left.KeychainAccount == right.KeychainAccount
}
