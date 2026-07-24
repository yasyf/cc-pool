package daemon

import (
	"context"
	"fmt"
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
	"github.com/yasyf/synckit/hostregistry"
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
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &Server{
		m:            newDaemonTestManager(t, st, &fakeOAuth{}, credstest.NewFake()),
		syncSocket:   filepath.Join(home, "sync.sock"),
		snapshot:     filepath.Join(home, "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}
	s.disposableWorkers = activatedDaemonTestWorkers(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); s.wg.Wait() })
	return s, ctx
}

// writeWireMeshState writes the shared synckit state.json under the fixture's
// XDG_CONFIG_HOME so SynckitMesh resolves the given self and peers.
func writeWireMeshState(t *testing.T, self string, hosts []string) {
	t.Helper()
	ctx := context.Background()
	if err := hostregistry.Mesh.InitializeState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := hostregistry.Mesh.Update(ctx, func(reg *hostregistry.Registry) error {
		reg.Self = self
		reg.Hosts = append([]string{}, hosts...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// wiredService returns the wired *hostsync.Service.
func wiredService(t *testing.T, s *Server) *hostsync.Service {
	t.Helper()
	if s.syncSvc == nil {
		t.Fatal("sync service not wired")
	}
	return s.syncSvc
}

// TestSetupSyncWiresEverything pins that one setupSync call constructs the
// mutation publisher, mesh identity, heal worker, credential settlement, and
// worker-backed consumer socket without retaining an in-process converge path.
func TestSetupSyncWiresEverything(t *testing.T) {
	s, ctx := newWireServer(t)
	writeWireMeshState(t, "host-mesh", []string{"peer-b"})
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}

	if s.syncSvc == nil || !s.syncEnabledBool() {
		t.Fatal("sync not wired or not active with sync_enabled=1")
	}
	if s.syncSelf != "host-mesh" {
		t.Errorf("sync self = %q, want the mesh-resolved host-mesh", s.syncSelf)
	}
	if s.syncPull == nil {
		t.Error("syncPull not wired")
	}
	if s.m.SettleCredentialWrite == nil {
		t.Error("Manager.SettleCredentialWrite not wired")
	}

	svc := wiredService(t, s)
	if svc.M != nil || svc.Locals != nil || svc.Mesh != nil || svc.Sessions != nil ||
		svc.Remover != nil || svc.Status != nil || svc.Driver != nil || svc.Fetcher != nil || svc.Run != nil {
		t.Errorf("parent retained an in-process host-sync operation path: %+v", svc)
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

func TestServerReadinessRefusesPublicationWhenSyncSetupFails(t *testing.T) {
	s, ctx := newWireServer(t)
	s.m.BuildCredentialWritePublication = nil
	s.m.SettleCredentialWrite = nil
	s.syncSocket = filepath.Join(pool.StateDir(), "missing", "sync.sock")
	err := (serverReadiness{owner: s}).BeforeReady(ctx)
	if err == nil || !strings.Contains(err.Error(), "setup host sync publication") {
		t.Fatalf("BeforeReady error = %v, want sync-publication setup failure", err)
	}
	if s.runtimePublished.Load() {
		t.Fatal("failed sync setup published runtime readiness")
	}
	if s.m.BuildCredentialWritePublication != nil || s.m.SettleCredentialWrite != nil {
		t.Fatal("failed setup retained partial credential publication wiring")
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
	if s.syncSvc == nil || s.syncSelf == "" {
		t.Fatal("sync not wired, or self empty on the hostname fallback")
	}

	if s.syncEnabledBool() {
		t.Fatal("sync active with sync disabled")
	}
	// The refresh gate is now structural (a refresh-token-free synced credential
	// cannot refresh), so a disabled pool needs no gate to stay syncless.
	if kind, err := s.authKind(t.Context(), store.Account{ID: 1, AccountUUID: "u1"}); err != nil || kind != store.AuthKindOwned {
		t.Errorf("disabled authKind = (%q, %v), want owned (no registry classification)", kind, err)
	}

	if err := s.syncPull(ctx); err != nil {
		t.Errorf("disabled syncPull = %v, want a silent no-op", err)
	}
	credential := creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "disabled-access"
	credential.ClaudeAiOauth.RefreshToken = "disabled-refresh"
	payload, err := s.m.BuildCredentialWritePublication(
		store.Account{ID: 1, AccountUUID: "u1"},
		&credential,
		store.CredentialOperationID{1},
		time.UnixMilli(1_700_000_000_000),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.m.SettleCredentialWrite(ctx, pool.CredentialWriteSettlement{
		OperationID: store.CredentialOperationID{1}, PublicationPayload: payload,
	}); err != nil {
		t.Errorf("disabled credential settlement = %v, want a silent no-op", err)
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
	if !s.syncEnabledBool() {
		t.Fatal("sync did not pick up sync_enabled=1 without a restart")
	}
	if _, err := client.Capabilities(ctx); err != nil {
		t.Fatalf("Capabilities after enable = %v, want OK without a restart", err)
	}
}

func TestHostSyncWorkerDeadlineKillsReapsAndReusesLane(t *testing.T) {
	s, ctx := newWireServer(t)
	writeWireMeshState(t, "host-mesh", nil)
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.setupSync(ctx); err != nil {
		t.Fatal(err)
	}
	lock, err := (daemonproc.FileLockSpec{
		Path: s.syncSvc.Registry.LockPath, Mode: daemonproc.FileLockExclusive, Deadline: time.Second,
	}).Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client := syncservice.NewClient(syncservice.Socket(s.syncSocket))
	defer func() { _ = client.Close() }()
	deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, reconcileErr := client.Reconcile(deadline, "")
	cancel()
	if reconcileErr == nil {
		t.Fatal("blocked reconcile survived its deadline")
	}

	records, err := (&daemonproc.FileStore{
		Path: filepath.Join(os.Getenv("HOME"), "workers.json"),
	}).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("timed-out host-sync worker retained records: %+v", records)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Reconcile(ctx, ""); err != nil {
		t.Fatalf("reconcile after reaping timed-out worker: %v", err)
	}
}

// TestExecPeerRoundTripThroughWiredFetcher proves the exec: peer convention
// end to end through the PRODUCTION dial path: a second wired daemon serves
// its registry and credential, reached via `exec:nc -U <sock>`.
func TestAuthKindClassification(t *testing.T) {
	s, ctx := newWireServer(t)
	writeWireMeshState(t, "host-self", []string{"peer-b"})
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	accounts := make(map[string]store.Account)
	for index, uuid := range []string{"u-self", "u-peer", "u-absent", "u-noorigin", "u-foreign"} {
		id := index + 1
		accounts[uuid] = admitDaemonTestAccount(t, s.m.Store, store.Account{
			ID: id, ConfigDir: testFileProviderConfigDir(id),
			KeychainService: fmt.Sprintf("svc-auth-kind-%d", id), KeychainAccount: "cc-pool",
			AccountUUID: uuid,
		})
	}
	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}
	if s.syncSelf != "host-self" {
		t.Fatalf("syncSelf = %q, want host-self", s.syncSelf)
	}
	pub := func(uuid, origin string) {
		if err := s.syncSvc.PublishAccount(ctx, hostsync.AccountValue{
			UUID: uuid, Chain: hostsync.ChainStamp{Origin: origin, ExpiresAt: 1},
		}); err != nil {
			t.Fatal(err)
		}
	}
	pub("u-self", "host-self")
	pub("u-peer", "peer-b")
	pub("u-foreign", "intruder")
	// An origin-less entry can only predate the PublishAccount guard (or come
	// from a foreign writer); seed one as an identity-only value.
	if err := s.syncSvc.PublishAccount(ctx, hostsync.AccountValue{UUID: "u-noorigin"}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		uuid    string
		disable bool
		want    store.AuthKind
		wantErr bool
	}{
		"origin is self → owned":      {uuid: "u-self", want: store.AuthKindOwned},
		"origin is a peer → awaiting": {uuid: "u-peer", want: store.AuthKindAwaitingOrigin},
		"no registry entry → owned":   {uuid: "u-absent", want: store.AuthKindOwned},
		"no account uuid → owned":     {uuid: "", want: store.AuthKindOwned},
		"sync disabled → owned":       {uuid: "u-peer", disable: true, want: store.AuthKindOwned},
		"empty origin is unproven":    {uuid: "u-noorigin", wantErr: true},
		"foreign origin is unproven":  {uuid: "u-foreign", wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.disable {
				if err := s.m.Store.SetMeta(metaSyncEnabled, "0"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.m.Store.SetMeta(metaSyncEnabled, "1") })
			}
			a := accounts[tc.uuid]
			got, err := s.authKind(t.Context(), a)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("authKind(uuid=%q) = %q, want error", tc.uuid, got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("authKind(uuid=%q) = (%q, %v), want %q", tc.uuid, got, err, tc.want)
			}
		})
	}
}
