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
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/synckit/hostregistry"
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
		snapshot:     filepath.Join(home, "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}
	s.disposableWorkers = daemonTestRunner(t)
	s.launchSyncHelper = func(context.Context, string, string) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); s.wg.Wait() })
	return s, ctx
}

// wireSync runs both halves of daemon sync setup back to back, the shape a
// test that only wants the wired result needs; startProductRuntime is what
// pins the foreign-lane claim that belongs between them.
func wireSync(ctx context.Context, t *testing.T, s *Server) error {
	t.Helper()
	plan, err := s.setupSyncPublication(ctx)
	if err != nil {
		return err
	}
	return s.startSyncConsumer(ctx, plan)
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

// TestSetupSyncWiresEverything pins that the two setup halves construct the
// mutation publisher, mesh identity, heal worker, credential settlement, and
// helper-backed consumer socket without retaining an in-process converge path.
func TestSetupSyncWiresEverything(t *testing.T) {
	s, ctx := newWireServer(t)
	writeWireMeshState(t, "test@host-mesh", []string{"test@peer-b"})
	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	plan, err := s.setupSyncPublication(ctx)
	if err != nil {
		t.Fatalf("setupSyncPublication: %v", err)
	}
	if s.syncSelf != "test@host-mesh" {
		t.Errorf("sync self = %q, want the mesh-resolved test@host-mesh", s.syncSelf)
	}
	if s.m.SettleCredentialWrite == nil {
		t.Error("Manager.SettleCredentialWrite not wired")
	}
	if s.syncClient != nil || s.syncPull != nil {
		t.Fatal("the publication half published a converge path before the helper is live")
	}
	if err := s.startSyncConsumer(ctx, plan); err != nil {
		t.Fatalf("startSyncConsumer: %v", err)
	}
	if s.syncClient == nil || !s.syncEnabledBool() {
		t.Fatal("sync not wired or not active with sync_enabled=1")
	}
	if s.syncPull == nil {
		t.Error("syncPull not wired")
	}
}

func TestServerReadinessRefusesPublicationWhenSyncSetupFails(t *testing.T) {
	s, ctx := newWireServer(t)
	s.launchSyncHelper = func(context.Context, string, string) error {
		return errors.New("helper launch unavailable")
	}
	plan, err := s.setupSyncPublication(ctx)
	if err != nil {
		t.Fatalf("setupSyncPublication: %v", err)
	}
	if err := s.startSyncConsumer(ctx, plan); err == nil {
		t.Fatal("startSyncConsumer accepted an unavailable helper socket")
	}
	if s.syncClient != nil || s.syncPull != nil {
		t.Fatal("failed consumer start retained a converge path")
	}
}

// TestSetupSyncPublicationUnwiresPublicationOnFailure pins that a publication
// half that fails after installing the hooks restores what it found: the
// manager it half-wired is the same one the caller aborts start on.
func TestSetupSyncPublicationUnwiresPublicationOnFailure(t *testing.T) {
	s, ctx := newWireServer(t)
	s.m.BuildCredentialWritePublication = nil
	s.m.SettleCredentialWrite = nil
	s.disposableWorkers = nil
	if _, err := s.setupSyncPublication(ctx); err == nil {
		t.Fatal("setupSyncPublication accepted a daemon without disposable workers")
	}
	if s.m.BuildCredentialWritePublication != nil || s.m.SettleCredentialWrite != nil {
		t.Fatal("failed setup retained partial credential publication wiring")
	}
}

// TestStartProductRuntimeClaimsForeignLanesBeforeTheHelperGoesLive pins the
// startup order the pending-add ownership argument rests on. Every host-sync
// worker mints an owner of its own, so a reservation one holds reads as
// foreign to a claim scan that classifies on owner bytes alone — and the scan
// retires what it classifies. It may therefore only run while no resident
// helper is reachable for synckitd's watcher to drive a converge through.
func TestStartProductRuntimeClaimsForeignLanesBeforeTheHelperGoesLive(t *testing.T) {
	s, ctx := newWireServer(t)
	s.holderSessionDone = make(chan struct{})

	deadOwner, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := s.m.Store.ReserveAccountIndex(deadOwner)
	if err != nil {
		t.Fatal(err)
	}

	var live store.PendingAccountReservation
	s.launchSyncHelper = func(context.Context, string, string) error {
		workerOwner, mintErr := store.MintOwnerRecord(time.Now())
		if mintErr != nil {
			return mintErr
		}
		var reserveErr error
		live, reserveErr = s.m.Store.ReserveAccountIndex(workerOwner)
		return reserveErr
	}

	var retired []int
	s.m.RetirePendingAdd = func(
		_ context.Context,
		reservation store.PendingAccountReservation,
	) (pool.PendingAddRetirementProof, error) {
		retired = append(retired, reservation.ID)
		return pool.PendingAddRetirementProof{}, errors.New("fixture retires no presentation")
	}

	if err := s.startProductRuntime(ctx); err != nil {
		t.Fatalf("startProductRuntime: %v", err)
	}
	if live.ID == 0 {
		t.Fatal("the helper stub never reserved a pending add")
	}
	if !slices.Contains(retired, orphan.ID) {
		t.Errorf("retired = %v, want the dead predecessor's acct-%02d reservation claimed", retired, orphan.ID)
	}
	if slices.Contains(retired, live.ID) {
		t.Fatalf(
			"retired = %v: the claim scan retired live host-sync worker reservation acct-%02d",
			retired, live.ID,
		)
	}
}

// TestSetupSyncStaysInertWhenDisabled pins the per-call enablement contract:
// with the meta unset the helper is constructed but every acting path no-ops
// with zero on-disk residue, and flipping the meta enables it with NO restart.
func TestSetupSyncStaysInertWhenDisabled(t *testing.T) {
	s, ctx := newWireServer(t)
	// No mesh state: self falls back to the hostname without failing setup.
	if err := wireSync(ctx, t, s); err != nil {
		t.Fatalf("wire sync: %v", err)
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
	credential.ClaudeAiOauth.ExpiresAt = 2
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

	registryFile := hostsync.NewRegistryFile(pool.SyncDir())
	publisher := &hostsync.Service{Registry: registryFile, StampDir: pool.SyncStampsDir()}
	if err := publisher.PublishAccount(ctx, hostsync.AccountValue{
		UUID: "u1", Chain: hostsync.ChainStamp{Origin: s.syncSelf, ExpiresAt: 1},
	}); err != nil {
		t.Fatal(err)
	}
	settle := func() {
		t.Helper()
		if err := s.m.SettleCredentialWrite(ctx, pool.CredentialWriteSettlement{
			OperationID: store.CredentialOperationID{1}, PublicationPayload: payload,
		}); err != nil {
			t.Fatalf("credential settlement = %v", err)
		}
	}
	publishedChain := func() hostsync.ChainStamp {
		t.Helper()
		registry, err := registryFile.Load()
		if err != nil {
			t.Fatal(err)
		}
		entry, present := registry["u1"]
		if !present || !entry.Present() {
			t.Fatalf("registry = %+v, want a present u1 entry", registry)
		}
		return entry.Value.Chain
	}
	settle()
	if chain := publishedChain(); chain.ExpiresAt != 1 {
		t.Fatalf("disabled settlement published %+v over a registered account", chain)
	}

	if err := s.m.Store.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if !s.syncEnabledBool() {
		t.Fatal("sync inert after the meta flipped, with no daemon restart between")
	}
	settle()
	chain := publishedChain()
	if chain.ExpiresAt != 2 || chain.Origin != s.syncSelf || chain.RotatedAt != 1_700_000_000_000 {
		t.Fatalf("enabled settlement published %+v, want this host's rotated chain", chain)
	}
}

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
	if err := wireSync(ctx, t, s); err != nil {
		t.Fatalf("wire sync: %v", err)
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
