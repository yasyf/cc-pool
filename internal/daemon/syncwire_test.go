package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/syncservice"
)

// newWireServer builds a Server over a short real HOME (macOS caps sun_path at
// 104 bytes) so pool paths, the sync socket, and the synckit config dir all
// stay inside the test sandbox. The cleanup cancels the returned ctx before
// waiting out the wg-tracked sync goroutines.
func newWireServer(t *testing.T) (*Server, context.Context) {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-wire")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	st, err := store.Open(filepath.Join(home, "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: &fakeOAuth{}, Creds: credstest.NewFake(), LockDir: filepath.Join(home, "locks")},
		syncSocket:   filepath.Join(home, "sync.sock"),
		snapshot:     filepath.Join(home, "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); s.wg.Wait() })
	return s, ctx
}

// writeWireMeshState writes the shared synckit state.json under the fixture's
// XDG_CONFIG_HOME so SynckitMesh resolves the given self and peers.
func writeWireMeshState(t *testing.T, self string, hosts []string) {
	t.Helper()
	dir := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "synckit")
	if err := os.MkdirAll(dir, 0o700); err != nil { //nolint:gosec // G703: dir is under the test's own XDG_CONFIG_HOME (t.TempDir), not external input
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"self": self, "hosts": hosts})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), body, 0o600); err != nil { //nolint:gosec // G703: dir is under the test's own XDG_CONFIG_HOME (t.TempDir), not external input
		t.Fatal(err)
	}
}

// wiredService digs the *hostsync.Service back out of the gate.
func wiredService(t *testing.T, s *Server) *hostsync.Service {
	t.Helper()
	svc, ok := s.sync.svc.(*hostsync.Service)
	if !ok {
		t.Fatalf("gate svc is %T, want *hostsync.Service", s.sync.svc)
	}
	return svc
}

// TestSetupSyncWiresEverything pins that one setupSync call constructs the
// full engine — gate (mesh-resolved self), heal pull, cred mirror hook,
// Sessions AND Claims, driver, fetcher — and binds the consumer socket.
func TestSetupSyncWiresEverything(t *testing.T) {
	s, ctx := newWireServer(t)
	writeWireMeshState(t, "host-mesh", []string{"peer-b"})
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}

	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}

	if s.sync == nil || !s.sync.active() {
		t.Fatal("gate not wired or not active with sync_enabled=1")
	}
	if s.sync.self != "host-mesh" {
		t.Errorf("gate self = %q, want the mesh-resolved host-mesh", s.sync.self)
	}
	if s.syncPull == nil {
		t.Error("syncPull not wired")
	}
	if s.m.OnCredWrite == nil {
		t.Error("Manager.OnCredWrite not wired to the cred mirror")
	}

	svc := wiredService(t, s)
	if svc.Sessions == nil {
		t.Error("svc.Sessions not wired — teardown would defer everything forever")
	}
	if svc.Claims == nil {
		t.Error("svc.Claims not wired")
	}
	if svc.Driver == nil || svc.Fetcher == nil || svc.Locals == nil || svc.Mesh == nil || svc.Status == nil {
		t.Errorf("service incompletely wired: %+v", svc)
	}
	if want := filepath.Join(pool.SyncDir(), "registry.json"); svc.Registry.Path != want {
		t.Errorf("registry path = %q, want %q", svc.Registry.Path, want)
	}
	if svc.StampDir != pool.SyncStampsDir() {
		t.Errorf("stamp dir = %q, want %q", svc.StampDir, pool.SyncStampsDir())
	}

	client := syncservice.NewClient(syncservice.Socket(s.syncSocket))
	defer func() { _ = client.Close() }()
	caps, err := client.Capabilities(ctx)
	if err != nil {
		t.Fatalf("Capabilities over the wired socket: %v", err)
	}
	if caps.Name != "cc-pool" || !hasMethod(caps.Methods, hostsync.MethodFetchCredential) {
		t.Fatalf("capabilities = %+v, want cc-pool with %s", caps, hostsync.MethodFetchCredential)
	}
}

// TestSetupSyncStaysInertWhenDisabled pins the per-call enablement contract:
// with the meta unset the engine is constructed but every acting path no-ops
// with zero on-disk residue, and flipping the meta enables it with NO restart.
func TestSetupSyncStaysInertWhenDisabled(t *testing.T) {
	s, ctx := newWireServer(t)
	// No mesh state: self falls back to the hostname without failing setup.
	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}
	if s.sync == nil || s.sync.self == "" {
		t.Fatal("gate not wired, or self empty on the hostname fallback")
	}

	if s.sync.active() {
		t.Fatal("gate active with sync disabled")
	}
	if !s.sync.mayRefresh(ctx, store.Account{ID: 1, AccountUUID: "u1"}) {
		t.Error("disabled gate must leave refresh behavior byte-identical to syncless")
	}

	if err := s.syncPull(ctx); err != nil {
		t.Errorf("disabled syncPull = %v, want a silent no-op", err)
	}
	svc := wiredService(t, s)
	note := s.gatedNote(svc)
	if err := note(ctx, "u1", hostsync.ChainStamp{ExpiresAt: 42, Hash: "h"}); err != nil {
		t.Errorf("disabled note = %v, want a silent no-op", err)
	}
	if _, err := os.Stat(pool.SyncDir()); !os.IsNotExist(err) {
		t.Errorf("disabled sync left residue under %s (stat err %v)", pool.SyncDir(), err)
	}

	client := syncservice.NewClient(syncservice.Socket(s.syncSocket))
	defer func() { _ = client.Close() }()
	if _, err := client.Capabilities(ctx); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled consumer Capabilities = %v, want a loud sync-disabled error", err)
	}

	// `ccp sync enable` writes the meta; the running daemon must honor it
	// with no restart.
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if !s.sync.active() {
		t.Fatal("gate did not pick up sync_enabled=1 without a restart")
	}
	if _, err := client.Capabilities(ctx); err != nil {
		t.Fatalf("Capabilities after enable = %v, want OK without a restart", err)
	}
}

// TestServerSessionsBusy pins the Sessions seam: live procscan sessions,
// live reservations, and in-flight conversions all read busy; an idle or
// unknown uuid reads free; a scan failure propagates so teardown fails closed.
func TestServerSessionsBusy(t *testing.T) {
	newSessionsFixture := func(t *testing.T) (*Server, serverSessions) {
		t.Helper()
		st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		a := store.Account{
			ID: 1, ConfigDir: "/cfg/acct-01", OverlayKind: "symlink",
			KeychainService: "svc", KeychainAccount: "me", AccountUUID: "u1",
		}
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		s := &Server{
			m:            &pool.Manager{Store: st},
			log:          log.New(io.Discard, "", 0),
			scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
			cl:           newClaims(),
		}
		return s, serverSessions{s: s}
	}

	cases := map[string]struct {
		arrange    func(t *testing.T, s *Server)
		uuid       string
		wantBusy   bool
		wantReason string
		wantErr    bool
	}{
		"idle account is free": {
			arrange: func(*testing.T, *Server) {}, uuid: "u1",
		},
		"live session reads busy": {
			arrange: func(_ *testing.T, s *Server) {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: "/cfg/acct-01", StartedAt: time.Now()}}, nil
				}
			},
			uuid: "u1", wantBusy: true, wantReason: "live session",
		},
		"reservation reads busy": {
			arrange: func(t *testing.T, s *Server) {
				if !s.cl.reserve(1) {
					t.Fatal("tryReserve failed")
				}
			},
			uuid: "u1", wantBusy: true, wantReason: "reserved",
		},
		"conversion reads busy": {
			arrange: func(t *testing.T, s *Server) {
				if !s.cl.own(1) {
					t.Fatal("beginConvert failed")
				}
			},
			uuid: "u1", wantBusy: true, wantReason: "conversion",
		},
		"unknown uuid is free": {
			arrange: func(*testing.T, *Server) {}, uuid: "u-elsewhere",
		},
		"scan failure fails closed": {
			arrange: func(_ *testing.T, s *Server) {
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return nil, errors.New("ps wedged")
				}
			},
			uuid: "u1", wantErr: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, ss := newSessionsFixture(t)
			tc.arrange(t, s)
			busy, reason, err := ss.Busy(context.Background(), tc.uuid)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want an error so teardown defers, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Busy: %v", err)
			}
			if busy != tc.wantBusy {
				t.Fatalf("busy = %v (reason %q), want %v", busy, reason, tc.wantBusy)
			}
			if tc.wantReason != "" && !strings.Contains(reason, tc.wantReason) {
				t.Errorf("reason = %q, want it to mention %q", reason, tc.wantReason)
			}
		})
	}
}

// TestExecPeerRoundTripThroughWiredFetcher proves the exec: peer convention
// end to end through the PRODUCTION dial path: a second wired daemon serves
// its registry and credential, reached via `exec:nc -U <sock>`.
func TestExecPeerRoundTripThroughWiredFetcher(t *testing.T) {
	// Peer B: a fully wired server with one synced account and its credential.
	b, ctxB := newWireServer(t)
	if err := b.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	credB := &creds.Credential{}
	credB.ClaudeAiOauth.AccessToken = "at-peer"
	credB.ClaudeAiOauth.RefreshToken = "rt-peer"
	credB.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	acctB := store.Account{
		ID: 9, ConfigDir: "/cfg/acct-09", OverlayKind: "symlink",
		KeychainService: "svc9", KeychainAccount: "me", AccountUUID: "u9",
	}
	if err := b.m.Store.UpsertAccount(acctB); err != nil {
		t.Fatal(err)
	}
	b.m.Creds.(*credstest.Fake).Put(acctB.KeychainService, acctB.KeychainAccount, credB)
	if err := b.setupSync(ctxB); err != nil {
		t.Fatalf("setupSync(B): %v", err)
	}
	// The holder IS the exec: peer, so the holder-first dial exercises the
	// production transport without ever touching real ssh.
	peer := "exec:nc -U " + b.syncSocket
	chain := hostsync.ChainStamp{
		ExpiresAt: credB.ClaudeAiOauth.ExpiresAt,
		Hash:      hostsync.CredentialHash(credB),
		Holder:    peer,
		RotatedAt: time.Now().UnixMilli(),
	}
	svcB := wiredService(t, b)
	if err := svcB.PublishAccount(ctxB, hostsync.AccountValue{UUID: "u9", Email: "b@x.com", Chain: chain}); err != nil {
		t.Fatalf("publish on B: %v", err)
	}

	// Host A: its own wired engine; B is reachable only as an exec: peer.
	a, ctxA := newWireServer(t)
	if err := a.setupSync(ctxA); err != nil {
		t.Fatalf("setupSync(A): %v", err)
	}

	reg, err := wiredService(t, a).Fetcher.Fetch(ctxA, peer)
	if err != nil {
		t.Fatalf("Fetch via exec peer: %v", err)
	}
	entry, ok := reg["u9"]
	if !ok || !entry.Present() {
		t.Fatalf("fetched registry missing u9: %+v", reg)
	}
	if entry.Value.Chain != chain {
		t.Fatalf("fetched chain = %+v, want %+v", entry.Value.Chain, chain)
	}

	got, err := hostsync.FetchCredential(ctxA, hostsync.PeerTransport, "u9", chain, 0, "", []string{peer})
	if err != nil {
		t.Fatalf("FetchCredential via exec peer: %v", err)
	}
	if got.ClaudeAiOauth.AccessToken != "at-peer" || got.ClaudeAiOauth.RefreshToken != "rt-peer" {
		t.Fatalf("pulled credential = %+v, want the peer's chain", got.ClaudeAiOauth)
	}
}
