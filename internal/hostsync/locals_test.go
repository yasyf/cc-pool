package hostsync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// localsFixture is a temp-dir pool Manager for the Locals/LocalIndex builders:
// real store, in-memory keychain, no network.
type localsFixture struct {
	m  *pool.Manager
	fk *credstest.Fake
}

func newLocalsFixture(t *testing.T) *localsFixture {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	fk := credstest.NewFake()
	return &localsFixture{
		m:  &pool.Manager{Store: st, OAuth: stubRefresher{}, Creds: fk, LockDir: t.TempDir()},
		fk: fk,
	}
}

// addAccount inserts a pool row; identityJSON (when non-empty) is written to
// the row's private .claude.json, honoring the backend's private-root math.
func (fx *localsFixture) addAccount(t *testing.T, id int, kind, label, identityJSON string) store.Account {
	t.Helper()
	dir := filepath.Join(t.TempDir(), fmt.Sprintf("acct-%02d", id))
	a := store.Account{
		ID: id, ConfigDir: dir, OverlayKind: kind, Label: label,
		KeychainService: fmt.Sprintf("svc%d", id), KeychainAccount: "me",
	}
	if err := fx.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	if identityJSON != "" {
		backend, err := fkoverlay.Parse(kind)
		if err != nil {
			t.Fatal(err)
		}
		path := privateClaudeJSON(backend, dir)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(identityJSON), 0o600); err != nil {
			t.Fatal(err)
		}
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

// TestManagerLocalsCarriesLineage pins ManagerLocals: identity from the private
// .claude.json, Chain.Hash/ExpiresAt from a read-only credential read,
// holder = self.
func TestManagerLocalsCarriesLineage(t *testing.T) {
	fixed := time.Now()
	now := func() time.Time { return fixed }
	const oauthRaw = `{"accountUuid":"u1","emailAddress":"a@x.com","organizationRole":"admin"}`

	t.Run("logged-in account carries identity, label, and chain stamp", func(t *testing.T) {
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
			ExpiresAt: 4_200_000,
			Hash:      CredentialHash(cred),
			Holder:    "host-self",
			RotatedAt: fixed.UnixMilli(),
		}
		if l.Chain != want {
			t.Errorf("chain = %+v, want %+v", l.Chain, want)
		}
	})

	t.Run("pre-login account is skipped", func(t *testing.T) {
		fx := newLocalsFixture(t)
		fx.addAccount(t, 1, "symlink", "l", "") // no .claude.json at all
		fx.addAccount(t, 2, "symlink", "l", `{"no":"identity"}`)
		fx.addAccount(t, 3, "symlink", "l", `{"oauthAccount":null}`)

		locals, err := ManagerLocals(fx.m, "h", now)(context.Background())
		if err != nil {
			t.Fatalf("ManagerLocals: %v", err)
		}
		if len(locals) != 0 {
			t.Fatalf("pre-login accounts leaked into the scan: %+v", locals)
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

	t.Run("fuse row reads identity from the private backing root", func(t *testing.T) {
		fx := newLocalsFixture(t)
		a := fx.addAccount(t, 1, "nfs", "l", `{"oauthAccount":`+oauthRaw+`}`)
		// A decoy at the bridged path must never be read: the account dir of a
		// fuse row is a bridge symlink whose traversal is unbounded.
		if err := os.MkdirAll(a.ConfigDir, 0o700); err != nil {
			t.Fatal(err)
		}
		decoy := `{"oauthAccount":{"accountUuid":"DECOY","emailAddress":"d@x.com"}}`
		if err := os.WriteFile(filepath.Join(a.ConfigDir, ".claude.json"), []byte(decoy), 0o600); err != nil {
			t.Fatal(err)
		}

		locals, err := ManagerLocals(fx.m, "h", now)(context.Background())
		if err != nil {
			t.Fatalf("ManagerLocals: %v", err)
		}
		if len(locals) != 1 || locals[0].UUID != "u1" {
			t.Fatalf("locals = %+v, want the private-root identity u1", locals)
		}
	})

	t.Run("unparseable overlay kind fails loud", func(t *testing.T) {
		fx := newLocalsFixture(t)
		fx.addAccount(t, 1, "symlink", "l", `{"oauthAccount":`+oauthRaw+`}`)
		bad := store.Account{ID: 2, ConfigDir: t.TempDir(), OverlayKind: "bogus", KeychainService: "svc2", KeychainAccount: "me"}
		if err := fx.m.Store.UpsertAccount(bad); err != nil {
			t.Fatal(err)
		}

		if _, err := ManagerLocals(fx.m, "h", now)(context.Background()); err == nil {
			t.Fatal("corrupt overlay_kind must fail the scan loudly")
		}
	})
}

// TestManagerLocalIndex pins the uuid backfill index: logged-in rows map uuid
// to row id, pre-login rows are absent.
func TestManagerLocalIndex(t *testing.T) {
	fx := newLocalsFixture(t)
	fx.addAccount(t, 1, "symlink", "l", `{"oauthAccount":{"accountUuid":"u1","emailAddress":"a@x.com"}}`)
	fx.addAccount(t, 2, "symlink", "l", `{"oauthAccount":{"accountUuid":"u2","emailAddress":"b@x.com"}}`)
	fx.addAccount(t, 3, "symlink", "l", "") // pre-login

	idx, err := ManagerLocalIndex(fx.m)(context.Background())
	if err != nil {
		t.Fatalf("ManagerLocalIndex: %v", err)
	}
	if len(idx) != 2 || idx["u1"] != 1 || idx["u2"] != 2 {
		t.Fatalf("index = %v, want {u1:1 u2:2}", idx)
	}
}
