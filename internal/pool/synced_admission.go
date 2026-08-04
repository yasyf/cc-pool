package pool

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

var (
	syncedAdmissionFailpoint       func(string)
	syncedAdmissionResultFailpoint func(string) error
)

// AdmitSyncedCredential serializes credential validation with every credential
// mutation and advances presentation and admission through durable exact evidence.
func (m *Manager) AdmitSyncedCredential(
	ctx context.Context,
	account store.Account,
	currentIdentity store.FileProviderPresentationIdentity,
	freshIdentity store.FileProviderPresentationIdentity,
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
			ctx, account, currentIdentity, freshIdentity, accessHashDigest, expectedAccessHash,
		)
		m.finishCredentialFlight(account.ID, flight, admitted, admissionErr)
		return admitted, admissionErr
	}
}

func (m *Manager) admitSyncedCredentialClaimed(
	ctx context.Context,
	account store.Account,
	currentIdentity store.FileProviderPresentationIdentity,
	freshIdentity store.FileProviderPresentationIdentity,
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
		ctx, account, currentIdentity, freshIdentity, accessHashDigest, expectedAccessHash,
	)
}

func (m *Manager) admitSyncedCredential(
	ctx context.Context,
	account store.Account,
	currentIdentity store.FileProviderPresentationIdentity,
	freshIdentity store.FileProviderPresentationIdentity,
	accessHashDigest store.CredentialDigest,
	expectedAccessHash string,
) (admitted bool, resultErr error) {
	owner, err := m.MutationOwner()
	if err != nil {
		return false, err
	}
	lease, err := acquireCredentialRefreshLocks(ctx, owner, account.ID, account.ConfigDir)
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
		return false, errors.Join(
			ErrCredentialChangedUnderfoot,
			m.reconcileChangedSyncedAdmission(account),
		)
	}
	credential, _, err := m.ReadCredential(ctx, account)
	if err != nil {
		return false, err
	}
	if credential.HasRefreshToken() || !credential.Synced() ||
		creds.AccessHash(credential) != expectedAccessHash ||
		credentialTokenChainDigest(credential) != *tokenChain {
		return false, errors.Join(
			ErrCredentialChangedUnderfoot,
			m.reconcileChangedSyncedAdmission(account),
		)
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
		return false, errors.Join(
			ErrCredentialChangedUnderfoot,
			m.reconcileChangedSyncedAdmission(account),
		)
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
	stage, err := m.Store.StageSyncedAccountAdmission(account, currentIdentity, freshIdentity, fence)
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
		candidate, err := m.Store.CommitSyncedAccountAdmissionCandidate(account, freshIdentity, fence)
		if err != nil {
			return false, err
		}
		if !candidate {
			return false, store.ErrAccountPresentationEvidence
		}
		if syncedAdmissionFailpoint != nil {
			syncedAdmissionFailpoint("after-candidate")
		}
		if syncedAdmissionResultFailpoint != nil {
			if err := syncedAdmissionResultFailpoint("after-candidate"); err != nil {
				return false, err
			}
		}
		postCandidate, postCandidateTokenChain, err := m.credentialTokenChainStateAtObservation(ctx, account)
		if err != nil || !sameStoreObservation(verified, postCandidate) ||
			postCandidateTokenChain == nil || *postCandidateTokenChain != *verifiedTokenChain {
			rejectErr := m.Store.RejectSyncedAccountAdmissionCandidate(account, freshIdentity, fence)
			if err != nil {
				return false, errors.Join(err, rejectErr)
			}
			return false, errors.Join(ErrCredentialChangedUnderfoot, rejectErr)
		}
		if syncedAdmissionFailpoint != nil {
			syncedAdmissionFailpoint("before-settle-observation")
		}
		settleObserved, settleTokenChain, err := m.credentialTokenChainStateAtObservation(ctx, account)
		if err != nil || !sameStoreObservation(verified, settleObserved) ||
			settleTokenChain == nil || *settleTokenChain != *verifiedTokenChain {
			rejectErr := m.Store.RejectSyncedAccountAdmissionCandidate(account, freshIdentity, fence)
			if err != nil {
				return false, errors.Join(err, rejectErr)
			}
			return false, errors.Join(ErrCredentialChangedUnderfoot, rejectErr)
		}
		settled, err := m.Store.SettleSyncedAccountAdmission(account, freshIdentity, fence)
		if err != nil {
			return false, err
		}
		if !settled {
			return false, store.ErrAccountPresentationEvidence
		}
		if syncedAdmissionFailpoint != nil {
			syncedAdmissionFailpoint("after-settle")
		}
		if syncedAdmissionResultFailpoint != nil {
			if err := syncedAdmissionResultFailpoint("after-settle"); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (m *Manager) reconcileChangedSyncedAdmission(account store.Account) error {
	pending, err := m.Store.PendingSyncedCredentialAdmission(account)
	if err == nil && pending.CandidateAt.IsZero() {
		return nil
	}
	if err == nil {
		presentation, presentationErr := m.Store.AccountPresentation(account.ID)
		if presentationErr != nil {
			return presentationErr
		}
		return m.Store.RejectSyncedAccountAdmissionCandidate(
			account,
			presentation.Identity,
			pending.SyncedCredentialAdmissionFence,
		)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	final, err := m.Store.SyncedCredentialAdmission(account)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	presentation, err := m.Store.AccountPresentation(account.ID)
	if err != nil {
		return err
	}
	return m.Store.InvalidateSyncedAccountAdmission(
		account,
		presentation.Identity,
		final.SyncedCredentialAdmissionFence,
	)
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
