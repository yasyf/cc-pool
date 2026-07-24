package pool

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

// ErrEnvelopeCarriesSecret rejects a synced credential envelope that still
// carries a refresh token: the secret never leaves its origin host, so an
// RT-bearing envelope (e.g. from a downrev peer) is never installable.
var ErrEnvelopeCarriesSecret = errors.New("synced credential envelope carries a refresh token")

// ErrEnvelopeNoAccessToken rejects a credential envelope with no access
// token: there is nothing to serve, and installing it would bury a usable
// local copy under a tombstone.
var ErrEnvelopeNoAccessToken = errors.New("credential envelope carries no access token")

// InstallSyncedCredential installs one stripped credential from a validated delivery
// under owned precedence, reporting whether it installed (a precedence or
// freshness skip is a normal outcome). Owned-precedence rationale: ccn note e30f860.
func (m *Manager) InstallSyncedCredential(ctx context.Context, a store.Account, cred *creds.Credential) (bool, error) {
	switch {
	case cred.HasRefreshToken():
		return false, fmt.Errorf("refusing install for acct-%d: %w", a.ID, ErrEnvelopeCarriesSecret)
	case !cred.Synced():
		return false, fmt.Errorf("refusing install for acct-%d: %w", a.ID, ErrEnvelopeNoAccessToken)
	}
	raw, err := cred.Marshal()
	if err != nil {
		return false, err
	}
	fingerprint := sha256.Sum256(raw)
	return runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationInstallSynced,
		installCredentialOperationCodec(store.CredentialTargetKeychain, store.CredentialDigest(fingerprint)),
		func(ctx context.Context, boundary *credentialOperationBoundary) (bool, error) {
			return m.installSyncedCredential(ctx, a, cred, boundary)
		},
	)
}

func (m *Manager) installSyncedCredential(
	ctx context.Context,
	a store.Account,
	cred *creds.Credential,
	boundary *credentialOperationBoundary,
) (bool, error) {
	current, _, err := m.ReadCredential(ctx, a)
	var prev *creds.Credential
	switch {
	case err == nil:
		if current.HasRefreshToken() {
			return false, nil
		}
		if cred.ClaudeAiOauth.ExpiresAt <= current.ClaudeAiOauth.ExpiresAt {
			return false, nil
		}
		prev = current
	case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrNoTokens):
	default:
		return false, fmt.Errorf("%w: %w", ErrCredentialUnverifiable, err)
	}
	if err := m.writeObservedCredential(ctx, a, creds.SourceKeychain, prev, cred, boundary); err != nil {
		if errors.Is(err, ErrCredentialChangedUnderfoot) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
