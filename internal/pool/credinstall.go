package pool

import (
	"context"
	"errors"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

// InstallSyncedCredential installs cred — pulled from a peer — when it wins
// the lineage-aware freshness re-check under the account lock; chainParentHash
// is the registry chain's advertised parent ("" when unknown). The write goes
// through writeCredCAS, so an underfoot `claude /login` aborts as a clean
// skip. Reports whether it installed; a freshness skip is a normal outcome.
func (m *Manager) InstallSyncedCredential(ctx context.Context, a store.Account, cred *creds.Credential, chainParentHash string) (bool, error) {
	release, err := m.lockAccount(ctx, a.ID)
	if err != nil {
		return false, err
	}
	defer release()

	current, src, err := m.ReadCredential(a)
	prevAccess := ""
	switch {
	case err == nil:
		incomingHash := creds.CredentialHash(cred)
		currentHash := creds.CredentialHash(current)
		if incomingHash == currentHash {
			return false, nil
		}
		row, rerr := m.Store.GetAccount(a.ID)
		if rerr != nil {
			return false, rerr
		}
		if incomingHash == row.CredParentHash {
			// The pull is our own chain's parent: we are ahead regardless of expiry — see ccn 10bf17d.
			return false, nil
		}
		child := chainParentHash != "" && chainParentHash == currentHash
		if !child && cred.ClaudeAiOauth.ExpiresAt <= current.ClaudeAiOauth.ExpiresAt {
			return false, nil
		}
		prevAccess = current.ClaudeAiOauth.AccessToken
	case errors.Is(err, creds.ErrNotFound):
		// No credential on any backend: install to the resolved default (Keychain).
	default:
		return false, err
	}
	if err := m.writeCredCAS(a, src, prevAccess, cred, chainParentHash); err != nil {
		if errors.Is(err, ErrCredentialChangedUnderfoot) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
