package daemon

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// newGateServer builds a one-account daemon Server whose account carries
// AccountUUID "u1" and the given credential; sessions drives busy/idle. It wires
// the fake OAuth and credential seams but no sync engine (s.syncSvc is nil).
func newGateServer(t *testing.T, cred *creds.Credential, sessions []procscan.Session) (*Server, *fakeOAuth, store.Account) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user", AccountUUID: "u1",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: cred.ClaudeAiOauth.RefreshToken}
	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return sessions, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}
	return s, fo, a
}
