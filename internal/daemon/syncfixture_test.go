package daemon

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// newGateServer builds a one-account daemon Server whose account carries
// AccountUUID "u1" and the given credential; sessions drives busy/idle. It wires
// the fake OAuth and credential seams but no host-sync helper client.
func newGateServer(t *testing.T, cred *creds.Credential, sessions []procscan.Session) (*Server, *fakeOAuth, store.Account) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user", AccountUUID: "u1",
	}
	a = admitDaemonTestAccount(t, st, a)
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: cred.ClaudeAiOauth.RefreshToken}
	manager := newDaemonTestManager(t, st, fo, fk)
	manager.ScanSessions = func(context.Context) ([]procscan.Session, error) { return sessions, nil }
	s := &Server{
		m:            manager,
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return sessions, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}
	return s, fo, a
}
