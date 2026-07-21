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

// InstallSyncedCredential installs cred — a stripped copy pulled from a peer —
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
	target := store.CredentialTargetKeychain
	if _, source, readErr := m.ReadCredential(ctx, a); readErr == nil {
		target = credentialTarget(source)
	} else if errors.Is(readErr, creds.ErrUnavailable) {
		target = store.CredentialTargetFile
	}
	return runCredentialOperation(
		ctx,
		m,
		a,
		store.CredentialOperationInstallSynced,
		installCredentialOperationCodec(target, store.CredentialDigest(fingerprint)),
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
	current, src, err := m.ReadCredential(ctx, a)
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
		// No credential, or a claude tombstone: install to the resolved backend
		// (writeObservedCredential re-verifies the slot is still empty before writing).
	case errors.Is(err, creds.ErrUnavailable):
		// No readable credential and the keychain is unsearchable (headless
		// host); the re-check below retargets the write at the file store.
	default:
		return false, fmt.Errorf("%w: %w", ErrCredentialUnverifiable, err)
	}
	// The source re-read guards only src, so re-check every backend: owned
	// anywhere wins outright; a backend not PROVEN not-owned fails closed.
	// ErrUnavailable is installEnvelope's headless file fallback, not an
	// abort — owned-before-synced resolution (credOutranks) means an owned
	// chain surfacing there later still outranks the synced copy.
	for _, s := range m.Creds.Stores(a) {
		cur, rerr := s.Read(ctx)
		switch creds.ClassifyRead(rerr) {
		case creds.ReadEmpty:
		case creds.ReadUnsearchable:
			if prev == nil && s.Source() == src {
				src = creds.SourceFile
			}
		case creds.ReadFatal:
			return false, fmt.Errorf("%w: %s: %w", ErrCredentialUnverifiable, s, rerr)
		case creds.ReadPresent:
			if cur.HasRefreshToken() {
				return false, nil
			}
		}
	}
	if err := m.writeObservedCredential(ctx, a, src, prev, cred, boundary); err != nil {
		if errors.Is(err, ErrCredentialChangedUnderfoot) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
