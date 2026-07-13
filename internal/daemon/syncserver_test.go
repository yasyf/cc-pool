package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func hasMethod(ms []string, want string) bool {
	for _, m := range ms {
		if m == want {
			return true
		}
	}
	return false
}

// TestSyncSocketServesConsumer stands up the real second socket and round-trips
// both a contract method (svc.capabilities) and the custom credential fetch
// (ccp.fetch_credential) over a unix client, and pins the 0600 socket mode.
func TestSyncSocketServesConsumer(t *testing.T) {
	// macOS caps sun_path at 104 bytes; t.TempDir paths overflow it.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-sync")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "sync.sock")

	regDir := t.TempDir()
	rf := hostsync.RegistryFile{Path: filepath.Join(regDir, "registry.json"), LockPath: filepath.Join(regDir, "registry.lock")}
	svc := &hostsync.Service{Registry: &rf, StampDir: filepath.Join(regDir, "stamps")}
	consumer := hostsync.NewConsumer(svc, func() (bool, error) { return true, nil })

	served := &creds.Credential{}
	served.ClaudeAiOauth.AccessToken = "at-sock"
	served.ClaudeAiOauth.RefreshToken = "rt-sock"
	served.ClaudeAiOauth.ExpiresAt = 5_000_000_000_000
	acct := store.Account{ID: 9, ConfigDir: "/cfg/acct-09", KeychainService: "svc9", KeychainAccount: "me"}
	lookup := func(uuid string) (store.Account, bool, error) {
		if uuid == "u-sock" {
			return acct, true, nil
		}
		return store.Account{}, false, nil
	}
	read := func(context.Context, store.Account) (*creds.Credential, error) { return served, nil }
	fetch := hostsync.NewFetchCredentialHandler(lookup, read)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	if err := serveSyncSocket(ctx, &wg, sock, consumer, fetch, log.New(io.Discard, "", 0)); err != nil {
		t.Fatalf("serveSyncSocket: %v", err)
	}

	if fi, err := os.Stat(sock); err != nil {
		t.Fatalf("stat socket: %v", err)
	} else if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket mode = %o, want 0600", perm)
	}

	client := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = client.Close() }()
	caps, err := client.Capabilities(ctx)
	if err != nil {
		t.Fatalf("Capabilities RPC: %v", err)
	}
	if caps.Name != "cc-pool" {
		t.Errorf("Capabilities Name = %q, want cc-pool", caps.Name)
	}
	if !hasMethod(caps.Methods, hostsync.MethodFetchCredential) {
		t.Errorf("Capabilities Methods %v missing %s", caps.Methods, hostsync.MethodFetchCredential)
	}

	tx := syncservice.Socket(sock)
	defer func() { _ = tx.Close() }()
	resp, err := tx.Do(ctx, &rpc.Request{Method: hostsync.MethodFetchCredential, Params: map[string]any{"uuid": "u-sock"}})
	if err != nil {
		t.Fatalf("fetch_credential Do: %v", err)
	}
	if !resp.OK {
		t.Fatalf("fetch_credential not OK: %s", resp.Error)
	}
	var env hostsync.CredentialEnvelope
	if err := json.Unmarshal(resp.Result, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Hash != creds.AccessHash(served) {
		t.Errorf("envelope hash = %q, want %q", env.Hash, creds.AccessHash(served))
	}
	if env.ExpiresAt != served.ClaudeAiOauth.ExpiresAt {
		t.Errorf("envelope ExpiresAt = %d, want %d", env.ExpiresAt, served.ClaudeAiOauth.ExpiresAt)
	}

	// An unknown uuid fails loud rather than serving a blank credential.
	bad, err := tx.Do(ctx, &rpc.Request{Method: hostsync.MethodFetchCredential, Params: map[string]any{"uuid": "nope"}})
	if err != nil {
		t.Fatalf("fetch unknown Do: %v", err)
	}
	if bad.OK {
		t.Error("fetch_credential for an unknown uuid returned OK")
	}
}

// TestServerClaimsAdapter pins the uuid->id convert-claim adapter: an unknown or
// erroring lookup refuses, a known uuid claims via beginConvert (blocking a second
// claim) and releases via endConvert.
func TestServerClaimsAdapter(t *testing.T) {
	s := &Server{cl: newClaims()}
	sc := serverClaims{s: s, byUUID: func(uuid string) (store.Account, bool, error) {
		switch uuid {
		case "known":
			return store.Account{ID: 3}, true, nil
		case "boom":
			return store.Account{}, false, errors.New("lookup failed")
		default:
			return store.Account{}, false, nil
		}
	}}

	if _, ok := sc.TryClaim("missing"); ok {
		t.Error("claimed an unknown uuid")
	}
	if _, ok := sc.TryClaim("boom"); ok {
		t.Error("claimed despite a lookup error")
	}

	release, ok := sc.TryClaim("known")
	if !ok {
		t.Fatal("known uuid not claimed")
	}
	if !s.cl.held(3) {
		t.Error("account 3 not marked converting after claim")
	}
	if _, ok2 := sc.TryClaim("known"); ok2 {
		t.Error("a second claim on a held account succeeded")
	}
	release()
	if s.cl.held(3) {
		t.Error("account 3 still converting after release")
	}
}

// TestSyncEnabledMeta pins that syncEnabled reflects the store's sync_enabled meta
// and needs no restart: unset or "0" is off, "1" is on.
func TestSyncEnabledMeta(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &Server{cl: newClaims(), m: &pool.Manager{Store: st}}

	if on, err := s.syncEnabled(); err != nil || on {
		t.Fatalf("syncEnabled with no meta = %v (err %v), want false", on, err)
	}
	if err := st.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if on, err := s.syncEnabled(); err != nil || !on {
		t.Fatalf("syncEnabled after set 1 = %v (err %v), want true", on, err)
	}
	if err := st.SetMeta(metaSyncEnabled, "0"); err != nil {
		t.Fatal(err)
	}
	if on, err := s.syncEnabled(); err != nil || on {
		t.Fatalf("syncEnabled after set 0 = %v (err %v), want false", on, err)
	}
}

// TestReadCredentialForFetchNeverRefreshes pins that serving a peer never
// spends the refresh token: even an expired credential is returned verbatim,
// proving allowRefresh=false.
func TestReadCredentialForFetchNeverRefreshes(t *testing.T) {
	s, _ := newTestServer(t)
	fk := s.m.Creds.(*credstest.Fake)

	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.KeychainService = "svc-fetch"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	a, err = s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}

	expired := &creds.Credential{}
	expired.ClaudeAiOauth.AccessToken = "at-stale"
	expired.ClaudeAiOauth.RefreshToken = "rt-stale"
	expired.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, expired)

	got, err := s.readCredentialForFetch(context.Background(), a)
	if err != nil {
		t.Fatalf("readCredentialForFetch: %v", err)
	}
	if got.ClaudeAiOauth.AccessToken != "at-stale" || got.ClaudeAiOauth.RefreshToken != "rt-stale" {
		t.Fatalf("fetch reader rotated the chain to %+v; serving a peer must never refresh the single-use token", got.ClaudeAiOauth)
	}
}
