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

// InstallSyncedCredential installs cred — a stripped copy pulled from a peer —
// under owned precedence: an owned local blob (refresh token present) is never
// overwritten, even when expired; an absent or tombstoned local always
// installs; a synced local yields only to a strictly fresher expiry. The write
// goes through writeCredCAS, so an underfoot `claude /login` aborts as a clean
// skip. Reports whether it installed; a precedence or freshness skip is a
// normal outcome.
func (m *Manager) InstallSyncedCredential(ctx context.Context, a store.Account, cred *creds.Credential) (bool, error) {
	if cred.HasRefreshToken() {
		return false, fmt.Errorf("refusing install for acct-%d: %w", a.ID, ErrEnvelopeCarriesSecret)
	}
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return false, err
	}
	defer release()

	current, src, err := m.ReadCredential(a)
	prevAccess := ""
	switch {
	case err == nil:
		if current.HasRefreshToken() {
			return false, nil
		}
		if cred.ClaudeAiOauth.ExpiresAt <= current.ClaudeAiOauth.ExpiresAt {
			return false, nil
		}
		prevAccess = current.ClaudeAiOauth.AccessToken
	case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrNoTokens):
		// No credential, or a claude tombstone: install to the resolved backend
		// (writeCredCAS writes through an unreadable prior blob).
	default:
		return false, err
	}
	if err := m.writeCredCAS(a, src, prevAccess, cred, ""); err != nil {
		if errors.Is(err, ErrCredentialChangedUnderfoot) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
