package pool

import (
	"context"
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
// under owned precedence: an owned local blob (refresh token present) on ANY
// backend is never overwritten or shadowed, even when expired; an absent or
// tombstoned local always installs; a synced local yields only to a strictly
// fresher expiry. A backend whose owned-state cannot be proven (a read error
// other than not-found/tombstone/unavailable) aborts the install with
// ErrCredentialUnverifiable; an unsearchable backend (creds.ErrUnavailable —
// the headless login keychain) falls back to the file store, mirroring
// hostsync's installEnvelope. The write goes through writeCredCAS, so an
// underfoot `claude /login` aborts as a clean skip. Reports whether it
// installed; a precedence or freshness skip is a normal outcome.
func (m *Manager) InstallSyncedCredential(ctx context.Context, a store.Account, cred *creds.Credential) (bool, error) {
	switch {
	case cred.HasRefreshToken():
		return false, fmt.Errorf("refusing install for acct-%d: %w", a.ID, ErrEnvelopeCarriesSecret)
	case !cred.Synced():
		return false, fmt.Errorf("refusing install for acct-%d: %w", a.ID, ErrEnvelopeNoAccessToken)
	}
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return false, err
	}
	defer release()

	current, src, err := m.ReadCredential(a)
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
		// (writeCredCAS re-verifies the slot is still empty before writing).
	case errors.Is(err, creds.ErrUnavailable):
		// No readable credential and the keychain is unsearchable (headless
		// host); the re-check below retargets the write at the file store.
	default:
		return false, err
	}
	// The CAS re-read guards only src, so re-check every backend: owned
	// anywhere wins outright; a backend not PROVEN not-owned fails closed.
	// ErrUnavailable is installEnvelope's headless file fallback, not an
	// abort — owned-before-synced resolution (credOutranks) means an owned
	// chain surfacing there later still outranks the synced copy.
	for _, s := range m.Creds.Stores(a) {
		cur, rerr := s.Read()
		switch {
		case errors.Is(rerr, creds.ErrNotFound), errors.Is(rerr, creds.ErrNoTokens):
		case errors.Is(rerr, creds.ErrUnavailable):
			if prev == nil && s.Source() == src {
				src = creds.SourceFile
			}
		case rerr != nil:
			return false, fmt.Errorf("%w: %s: %w", ErrCredentialUnverifiable, s, rerr)
		case cur.HasRefreshToken():
			return false, nil
		}
	}
	if err := m.writeCredCAS(a, src, prev, cred); err != nil {
		if errors.Is(err, ErrCredentialChangedUnderfoot) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
