package hostsync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/daemonkit"
)

// localsFixture is a temp-dir pool Manager for the Locals builder:
// real store, in-memory keychain, no network.
type localsFixture struct {
	m  *pool.Manager
	fk *credstest.Fake
}

func hostSyncTestScope(t *testing.T) daemonkit.Ctx {
	t.Helper()
	scopeCtx, cancelScope := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancelScope)
	owned, err := daemonkit.OwnProcesses(scopeCtx, filepath.Join(t.TempDir(), "workers.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		if err := owned.Close(closeCtx); err != nil {
			t.Errorf("close test process scope: %v", err)
		}
	})
	return owned.Ctx(scopeCtx)
}

func newLocalsFixture(t *testing.T) *localsFixture {
	t.Helper()
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	m, err := pool.OpenHostSyncWorker(hostSyncTestScope(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	fk := credstest.NewFake()
	m.Creds = backingCredentials{fk}
	m.OAuth = stubRefresher{}
	return &localsFixture{
		m:  m,
		fk: fk,
	}
}

// addAccount inserts a pool row; identityJSON (when non-empty) is written to
// the row's private .claude.json, honoring the backend's private-root math.
func (fx *localsFixture) addAccount(t *testing.T, id int, _ string, label, identityJSON string) store.Account {
	t.Helper()
	dir := testAccountConfigDir(id)
	var identity struct {
		OAuthAccount *struct {
			AccountUUID string `json:"accountUuid"`
		} `json:"oauthAccount"`
	}
	if err := json.Unmarshal([]byte(identityJSON), &identity); err != nil ||
		identity.OAuthAccount == nil || identity.OAuthAccount.AccountUUID == "" {
		t.Fatalf("acct-%d test identity is not exact: %v", id, err)
	}
	a := store.Account{
		ID: id, ConfigDir: dir, Label: label,
		KeychainService: fmt.Sprintf("svc%d", id), KeychainAccount: "me",
		AccountUUID: identity.OAuthAccount.AccountUUID,
	}
	admitHostsyncTestAccount(t, fx.m, a)
	path := filepath.Join(pool.AccountBackingDir(id), ".claude.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(identityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := fx.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func credWith(access, refresh string, expiresAt int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = access
	c.ClaudeAiOauth.RefreshToken = refresh
	c.ClaudeAiOauth.ExpiresAt = expiresAt
	return c
}

// TestManagerLocalsAdvertisesOnlyOwnedChains pins ManagerLocals: identity from
// the private .claude.json; an OWNED credential stamps {Origin: self,
// AccessHash}, while a SYNCED (stripped) copy is a zero stamp — a peer never
// advertises a chain it doesn't own.
func TestManagerLocalsAdvertisesOnlyOwnedChains(t *testing.T) {
	fixed := time.Now()
	now := func() time.Time { return fixed }
	const oauthRaw = `{"accountUuid":"u1","emailAddress":"a@x.com","organizationRole":"admin"}`

	t.Run("owned account carries identity, label, and an origin chain stamp", func(t *testing.T) {
		fx := newLocalsFixture(t)
		a := fx.addAccount(t, 1, "symlink", "work", `{"oauthAccount":`+oauthRaw+`,"other":true}`)
		cred := credWith("at-1", "rt-1", 4_200_000)
		fx.fk.Put(a.KeychainService, a.KeychainAccount, cred)

		locals, err := ManagerLocals(fx.m, "host-self", now)(context.Background())
		if err != nil {
			t.Fatalf("ManagerLocals: %v", err)
		}
		if len(locals) != 1 {
			t.Fatalf("locals = %d entries, want 1", len(locals))
		}
		l := locals[0]
		if l.UUID != "u1" || l.Email != "a@x.com" || l.Label != "work" {
			t.Errorf("identity = %q/%q/%q, want u1/a@x.com/work", l.UUID, l.Email, l.Label)
		}
		if string(l.OAuthAccount) != oauthRaw {
			t.Errorf("OAuthAccount not verbatim:\n got %s\nwant %s", l.OAuthAccount, oauthRaw)
		}
		want := ChainStamp{
			Origin:    "host-self",
			ExpiresAt: 4_200_000,
			Hash:      creds.AccessHash(cred),
			RotatedAt: fixed.UnixMilli(),
		}
		if l.Chain != want {
			t.Errorf("chain = %+v, want %+v", l.Chain, want)
		}
	})

	t.Run("synced (stripped) credential is a zero stamp, never advertised", func(t *testing.T) {
		fx := newLocalsFixture(t)
		a := fx.addAccount(t, 1, "symlink", "l", `{"oauthAccount":`+oauthRaw+`}`)
		fx.fk.Put(a.KeychainService, a.KeychainAccount, credWith("at-synced", "", 9_000_000))

		locals, err := ManagerLocals(fx.m, "host-self", now)(context.Background())
		if err != nil {
			t.Fatalf("ManagerLocals: %v", err)
		}
		if len(locals) != 1 {
			t.Fatalf("locals = %d entries, want 1", len(locals))
		}
		if locals[0].Chain != (ChainStamp{}) {
			t.Fatalf("chain = %+v, want zero — a synced copy must never be advertised", locals[0].Chain)
		}
	})

	t.Run("tombstoned credential is a zero stamp", func(t *testing.T) {
		fx := newLocalsFixture(t)
		a := fx.addAccount(t, 1, "symlink", "l", `{"oauthAccount":`+oauthRaw+`}`)
		fx.fk.Put(a.KeychainService, a.KeychainAccount, credWith("", "", 0))

		locals, err := ManagerLocals(fx.m, "host-self", now)(context.Background())
		if err != nil {
			t.Fatalf("ManagerLocals: %v", err)
		}
		if locals[0].Chain != (ChainStamp{}) {
			t.Fatalf("chain = %+v, want zero for a tombstone", locals[0].Chain)
		}
	})

	t.Run("credential-less account lists with a zero chain", func(t *testing.T) {
		fx := newLocalsFixture(t)
		fx.addAccount(t, 1, "symlink", "l", `{"oauthAccount":`+oauthRaw+`}`)

		locals, err := ManagerLocals(fx.m, "host-self", now)(context.Background())
		if err != nil {
			t.Fatalf("ManagerLocals: %v", err)
		}
		if len(locals) != 1 {
			t.Fatalf("locals = %d entries, want 1", len(locals))
		}
		if locals[0].Chain != (ChainStamp{}) {
			t.Fatalf("chain = %+v, want zero (no credential, no holder claim)", locals[0].Chain)
		}
	})

	t.Run("unsearchable keychain reads as a zero chain, not a failure", func(t *testing.T) {
		fx := newLocalsFixture(t)
		fx.addAccount(t, 1, "symlink", "l", `{"oauthAccount":`+oauthRaw+`}`)
		fx.fk.KeychainFaults.Read = creds.ErrUnavailable

		locals, err := ManagerLocals(fx.m, "host-self", now)(context.Background())
		if err != nil {
			t.Fatalf("a locked keychain must not abort the scan: %v", err)
		}
		if locals[0].Chain != (ChainStamp{}) {
			t.Fatalf("chain = %+v, want zero", locals[0].Chain)
		}
	})
}
