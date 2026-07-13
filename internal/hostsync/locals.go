package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// localRow is one logged-in local account: its store row plus the identity
// read from its private .claude.json.
type localRow struct {
	acct  store.Account
	uuid  string
	email string
	oauth json.RawMessage
}

// ManagerLocals builds the Service.Locals seam over m: every logged-in local
// account with its identity, label, and secretless chain stamp. Credential
// reads are read-only — building the scan must never spend a refresh token.
func ManagerLocals(m *pool.Manager, self string, now func() time.Time) func(context.Context) ([]LocalAccount, error) {
	return func(context.Context) ([]LocalAccount, error) {
		rows, err := scanLocalAccounts(m)
		if err != nil {
			return nil, err
		}
		out := make([]LocalAccount, 0, len(rows))
		for _, r := range rows {
			chain, err := localChainStamp(m, r.acct, self, now)
			if err != nil {
				return nil, err
			}
			out = append(out, LocalAccount{
				UUID:         r.uuid,
				Email:        r.email,
				Label:        r.acct.Label,
				OAuthAccount: r.oauth,
				Chain:        chain,
			})
		}
		return out, nil
	}
}

// ManagerLocalIndex builds the LoadRegistry uuid-backfill index (uuid -> row
// id) over the same identity scan as ManagerLocals.
func ManagerLocalIndex(m *pool.Manager) LocalIndex {
	return func(context.Context) (map[string]int, error) {
		rows, err := scanLocalAccounts(m)
		if err != nil {
			return nil, err
		}
		idx := make(map[string]int, len(rows))
		for _, r := range rows {
			idx[r.uuid] = r.acct.ID
		}
		return idx, nil
	}
}

// scanLocalAccounts reads every pool row's private identity; rows with no
// identity yet (pre-login) are skipped — they have no uuid to sync under.
func scanLocalAccounts(m *pool.Manager) ([]localRow, error) {
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	out := make([]localRow, 0, len(accts))
	for _, a := range accts {
		backend, err := fkoverlay.Parse(a.OverlayKind)
		if err != nil {
			return nil, fmt.Errorf("acct-%d: unparseable overlay kind %q: %w", a.ID, a.OverlayKind, err)
		}
		raw, id, err := pool.AccountOAuth(backend, a.ConfigDir)
		if errors.Is(err, pool.ErrNoIdentity) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("acct-%d: %w", a.ID, err)
		}
		out = append(out, localRow{acct: a, uuid: id.AccountUUID, email: id.EmailAddress, oauth: raw})
	}
	return out, nil
}

// localChainStamp stamps a's chain from a refresh-free credential read;
// holder = self; no readable credential is a zero chain, which the fold's
// strictly-ahead gates never adopt — see ccn 10bf17d.
func localChainStamp(m *pool.Manager, a store.Account, self string, now func() time.Time) (ChainStamp, error) {
	cred, _, err := m.ReadCredential(a)
	switch {
	case errors.Is(err, creds.ErrNotFound), errors.Is(err, creds.ErrUnavailable):
		return ChainStamp{}, nil
	case err != nil:
		return ChainStamp{}, fmt.Errorf("read acct-%d credential: %w", a.ID, err)
	}
	return ChainStamp{
		ExpiresAt: cred.ClaudeAiOauth.ExpiresAt,
		Hash:      CredentialHash(cred),
		Holder:    self,
		RotatedAt: now().UnixMilli(),
	}, nil
}
