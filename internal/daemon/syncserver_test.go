package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/drain"
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
// over a unix client, pins the 0600 socket mode, and pins that v1's
// ccp.fetch_credential (which served the full refresh-token-bearing blob) is
// NOT registered — a downrev peer gets unknown-method, never a credential.
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
	var intake drain.Intake
	t.Cleanup(func() { cancel(); wg.Wait() })
	if _, err := serveSyncSocket(ctx, &wg, &intake, sock, consumer, fetch, log.New(io.Discard, "", 0)); err != nil {
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

	// The removed secret-bearing method must never be answered by the fresh v1
	// server; stale callers receive unknown-method and no payload.
	removed, err := tx.Do(ctx, &rpc.Request{Method: "ccp.fetch_credential", Params: map[string]any{"uuid": "u-sock"}})
	if err != nil {
		t.Fatalf("removed fetch Do: %v", err)
	}
	if removed.OK {
		t.Fatal("the fresh-v1 server answered removed ccp.fetch_credential")
	}
	if !strings.Contains(removed.Error, "unknown method") {
		t.Errorf("removed fetch error = %q, want an unknown-method rejection", removed.Error)
	}
	if payload := string(removed.Result); payload != "" && payload != "null" {
		t.Errorf("removed fetch carried a result payload: %s", payload)
	}
}

// TestSyncSocketDrainsInFlightHandler pins the second socket into the daemon's
// settle-before-cancel order: Deactivate refuses new dials, an admitted handler
// keeps Drain blocked with a live context, and its store read finishes before
// teardown closes the store.
func TestSyncSocketDrainsInFlightHandler(t *testing.T) {
	sockDir, err := os.MkdirTemp("/tmp", "ccp-sync-drain")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "sync.sock")
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}

	regDir := t.TempDir()
	rf := hostsync.RegistryFile{Path: filepath.Join(regDir, "registry.json"), LockPath: filepath.Join(regDir, "registry.lock")}
	svc := &hostsync.Service{Registry: &rf, StampDir: filepath.Join(regDir, "stamps")}
	consumer := hostsync.NewConsumer(svc, func() (bool, error) { return true, nil })
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	ctxErr := make(chan error, 1)
	storeErr := make(chan error, 1)
	fetch := func(ctx context.Context, _ map[string]any) (any, error) {
		close(entered)
		<-release
		ctxErr <- ctx.Err()
		_, _, err := st.GetMeta("sync-drain-probe")
		storeErr <- err
		return map[string]any{"settled": true}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var intake drain.Intake
	ln, err := serveSyncSocket(ctx, &wg, &intake, sock, consumer, fetch, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		wg.Wait()
	})

	tx := syncservice.Socket(sock)
	defer func() { _ = tx.Close() }()
	type callResult struct {
		resp *syncservice.Response
		err  error
	}
	called := make(chan callResult, 1)
	go func() {
		resp, err := tx.Do(context.Background(), &rpc.Request{Method: hostsync.MethodFetchCredential})
		called <- callResult{resp: resp, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sync handler never entered")
	}

	deactivated := make(chan struct{})
	drained := make(chan error, 1)
	go func() {
		intake.Close()
		err := ln.Close()
		close(deactivated)
		if settleErr := intake.Settle(ctx); settleErr != nil {
			err = errors.Join(err, settleErr)
		}
		cancel()
		drained <- err
	}()
	<-deactivated
	if conn, err := net.DialTimeout("unix", sock, 100*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("post-drain sync dial succeeded")
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned before the in-flight sync handler settled: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case <-ctx.Done():
		t.Fatalf("sync handler context canceled before settle: %v", ctx.Err())
	default:
	}

	close(release)
	released = true
	if err := <-ctxErr; err != nil {
		t.Fatalf("sync handler context canceled before store access: %v", err)
	}
	if err := <-storeErr; err != nil {
		t.Fatalf("sync handler store access raced teardown: %v", err)
	}
	result := <-called
	if result.err != nil || result.resp == nil || !result.resp.OK {
		t.Fatalf("in-flight sync response: resp=%+v err=%v", result.resp, result.err)
	}
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after sync settle: %v", err)
	}
}

// TestSyncEnabledMeta pins that syncEnabled reflects the store's sync_enabled meta
// and needs no restart: unset or "0" is off, "1" is on.
func TestSyncEnabledMeta(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
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
