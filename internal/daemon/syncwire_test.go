package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	daemonproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/rpc"
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
	testhome.Sandbox(t, home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	bin := filepath.Join(home, "bin")
	if err := os.MkdirAll(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	synckitd := filepath.Join(bin, "synckitd")
	if err := os.WriteFile(synckitd, []byte("#!/bin/sh\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(synckitd, 0o700); err != nil { // #nosec G302 -- the owner-only test stub must be executable.
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
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
	s.launchSyncHelper = func(ctx context.Context, executable, synckitdExecutable string) error {
		workers, children := testSyncHelperOwners(t)
		consumer, err := hostsync.NewWorkerClient(workers, executable, synckitdExecutable)
		if err != nil {
			return err
		}
		runtime, err := newSyncHelperRuntime(
			s.syncSocket, consumer, workers, children,
			&daemonproc.FileStore{Path: filepath.Join(t.TempDir(), "helper-stop-v1.db")},
		)
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			done <- runtime.Run(ctx)
		}()
		client := rpc.NewClient(rpc.ClientConfig{
			Dial: wire.UnixDialer(s.syncSocket), WireBuild: rpc.WireBuild,
		})
		defer func() { _ = client.Close() }()
		deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			health, probeErr := client.RuntimeHealth(deadline)
			if probeErr == nil {
				probeErr = validateSyncHelperHealth(health)
			}
			if probeErr == nil {
				return nil
			}
			select {
			case runErr := <-done:
				return fmt.Errorf("test host-sync helper exited before readiness: %w", runErr)
			case <-deadline.Done():
				return fmt.Errorf("await test host-sync helper: %w", deadline.Err())
			case <-ticker.C:
			}
		}
	}
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
	for _, identity := range hosts {
		fact, err := hostregistry.NewSSHHostFact(identity, "/opt/homebrew/bin/synckitd", []string{identity})
		if err != nil {
			t.Fatal(err)
		}
		if err := hostregistry.Mesh.RegisterHost(ctx, fact); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hostregistry.Mesh.Update(ctx, func(reg *hostregistry.Registry) error {
		reg.Self = self
		reg.Hosts = append([]string{}, hosts...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSetupSyncWiresEverything pins that one setupSync call constructs the
// mutation publisher, mesh identity, heal worker, credential settlement, and
// helper-backed consumer socket without retaining an in-process converge path.
func TestSetupSyncWiresEverything(t *testing.T) {
	s, ctx := newWireServer(t)
	writeWireMeshState(t, "test@host-mesh", []string{"test@peer-b"})
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}

	if s.syncClient == nil || !s.syncEnabledBool() {
		t.Fatal("sync not wired or not active with sync_enabled=1")
	}
	if s.syncSelf != "test@host-mesh" {
		t.Errorf("sync self = %q, want the mesh-resolved test@host-mesh", s.syncSelf)
	}
	if s.syncPull == nil {
		t.Error("syncPull not wired")
	}
	if s.m.SettleCredentialWrite == nil {
		t.Error("Manager.SettleCredentialWrite not wired")
	}

	client := syncservice.NewClient(syncservice.Socket(s.syncSocket))
	defer func() { _ = client.Close() }()
	caps, err := client.Capabilities(ctx)
	if err != nil {
		t.Fatalf("Capabilities over the wired socket: %v", err)
	}
	want := syncservice.DefaultCapabilities(hostsync.SyncServiceID)
	if caps.Name != want.Name || !slices.Equal(caps.Methods, want.Methods) {
		t.Fatalf("capabilities = %+v, want exact default %+v", caps, want)
	}
}

func TestServerReadinessRefusesPublicationWhenSyncSetupFails(t *testing.T) {
	s, ctx := newWireServer(t)
	s.m.BuildCredentialWritePublication = nil
	s.m.SettleCredentialWrite = nil
	s.launchSyncHelper = func(context.Context, string, string) error {
		return errors.New("helper launch unavailable")
	}
	err := s.setupSync(ctx)
	if err == nil {
		t.Fatal("setupSync accepted an unavailable helper socket")
	}
	if s.syncClient != nil {
		t.Fatal("failed sync setup retained helper client")
	}
	if s.m.BuildCredentialWritePublication != nil || s.m.SettleCredentialWrite != nil {
		t.Fatal("failed setup retained partial credential publication wiring")
	}
}

// TestSetupSyncStaysInertWhenDisabled pins the per-call enablement contract:
// with the meta unset the helper is constructed but every acting path no-ops
// with zero on-disk residue, and flipping the meta enables it with NO restart.
func TestSetupSyncStaysInertWhenDisabled(t *testing.T) {
	s, ctx := newWireServer(t)
	// No mesh state: self falls back to the hostname without failing setup.
	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}
	if s.syncClient == nil || s.syncSelf == "" {
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
	writeWireMeshState(t, "test@host-mesh", nil)
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.setupSync(ctx); err != nil {
		t.Fatal(err)
	}
	registry := hostsync.NewRegistryFile(pool.SyncDir())
	lock, err := (daemonproc.FileLockSpec{
		Path: registry.LockPath, Mode: daemonproc.FileLockExclusive, Deadline: time.Second,
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

	workerStore := &daemonproc.FileStore{Path: pool.HostSyncHelperWorkerStorePath()}
	untracked := time.Now().Add(3 * time.Second)
	for {
		records, loadErr := workerStore.Load(t.Context())
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if len(records) == 0 {
			break
		}
		if time.Now().After(untracked) {
			t.Fatalf("timed-out host-sync worker retained records: %+v", records)
		}
		time.Sleep(10 * time.Millisecond)
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
	writeWireMeshState(t, "test@host-self", []string{"test@peer-b"})
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	accounts := make(map[string]store.Account)
	for index, uuid := range []string{"u-self", "u-peer", "u-absent", "u-noorigin", "u-foreign"} {
		id := index + 1
		accounts[uuid] = admitDaemonTestAccount(t, s.m.Store, store.Account{
			ID:              id,
			KeychainService: fmt.Sprintf("svc-auth-kind-%d", id), KeychainAccount: "cc-pool",
			AccountUUID: uuid,
		})
	}
	if err := s.setupSync(ctx); err != nil {
		t.Fatalf("setupSync: %v", err)
	}
	if s.syncSelf != "test@host-self" {
		t.Fatalf("syncSelf = %q, want test@host-self", s.syncSelf)
	}
	registryService := &hostsync.Service{
		Registry: hostsync.NewRegistryFile(pool.SyncDir()),
		StampDir: pool.SyncStampsDir(),
	}
	pub := func(uuid, origin string) {
		if err := registryService.PublishAccount(ctx, hostsync.AccountValue{
			UUID: uuid, Chain: hostsync.ChainStamp{Origin: origin, ExpiresAt: 1},
		}); err != nil {
			t.Fatal(err)
		}
	}
	pub("u-self", "test@host-self")
	pub("u-peer", "test@peer-b")
	pub("u-foreign", "intruder")
	// An origin-less entry can only predate the PublishAccount guard (or come
	// from a foreign writer); seed one as an identity-only value.
	if err := registryService.PublishAccount(ctx, hostsync.AccountValue{UUID: "u-noorigin"}); err != nil {
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
