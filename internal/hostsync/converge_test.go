package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
)

// fakeMesh resolves a fixed host + peer set for Converge.
type fakeMesh struct {
	self  string
	peers []string
	err   error
}

func (m fakeMesh) Resolve(context.Context) (string, []string, error) {
	return m.self, m.peers, m.err
}

// fakeDriver is an injectable converge.Driver: LoadRegistry returns a fixed
// registry and every Reconcile records the id + origin it was handed, so a test
// asserts what converge threaded through.
type fakeDriver struct {
	load     Registry
	saveErr  error
	reconOut converge.Outcome
	ids      []string
	origins  []string
}

func (d *fakeDriver) LoadRegistry(context.Context) (cregistry.Registry[AccountValue], error) {
	if d.load == nil {
		return cregistry.New[AccountValue](), nil
	}
	return d.load, nil
}

func (d *fakeDriver) SaveRegistry(context.Context, cregistry.Registry[AccountValue]) error {
	return d.saveErr
}

func (d *fakeDriver) Reconcile(_ context.Context, id string, _ cregistry.Entry[AccountValue], _ []string, origin string) (converge.Outcome, error) {
	d.ids = append(d.ids, id)
	d.origins = append(d.origins, origin)
	out := d.reconOut
	if out == "" {
		out = OutcomeUnchanged
	}
	return out, nil
}

// fakeFetcher records every peer it is asked to fetch and fails the peers in fail.
type fakeFetcher struct {
	fetched []string
	fail    map[string]bool
}

func (f *fakeFetcher) Fetch(_ context.Context, peer string) (cregistry.Registry[AccountValue], error) {
	f.fetched = append(f.fetched, peer)
	if f.fail[peer] {
		return nil, fmt.Errorf("peer %s down", peer)
	}
	return cregistry.New[AccountValue](), nil
}

// fakeClaims records claim/release and can refuse.
type fakeClaims struct {
	claimed  []string
	released int
	refuse   bool
}

func (c *fakeClaims) TryClaim(uuid string) (func(), bool) {
	c.claimed = append(c.claimed, uuid)
	if c.refuse {
		return func() {}, false
	}
	return func() { c.released++ }, true
}

var _ Claims = (*fakeClaims)(nil)

// regWith returns a one-entry registry with a present account, for driving the
// fake driver's LoadRegistry.
func regWith(uuid string) Registry {
	reg := cregistry.New[AccountValue]()
	reg.Add(uuid, acctVal(uuid, "e@x", "l", "hostA", 1000), 1)
	return reg
}

func TestConvergeOriginPassedThrough(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	drv := &fakeDriver{load: regWith("u1")}
	ftc := &fakeFetcher{fail: map[string]bool{}}
	s.Mesh = fakeMesh{self: "self", peers: []string{"peerX", "origin-peer"}}
	s.Driver = drv
	s.Fetcher = ftc
	s.Status = converge.NewPeerStatus()

	if _, err := s.Converge(ctx, "origin-peer"); err != nil {
		t.Fatalf("Converge: %v", err)
	}

	// The Driver.Reconcile for the present item carries the pass origin verbatim.
	if len(drv.origins) != 1 || drv.origins[0] != "origin-peer" {
		t.Fatalf("driver origins = %v, want [origin-peer]", drv.origins)
	}
	// The origin peer is skipped in the pull (anti-echo); the other peer is fetched.
	if containsStr(ftc.fetched, "origin-peer") {
		t.Errorf("fetched the origin peer %v; converge must skip it", ftc.fetched)
	}
	if !containsStr(ftc.fetched, "peerX") {
		t.Errorf("did not fetch the non-origin peer; fetched = %v", ftc.fetched)
	}
}

func TestConvergeUnreachablePeerNotFatal(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	drv := &fakeDriver{load: regWith("u1")}
	ftc := &fakeFetcher{fail: map[string]bool{"deadpeer": true}}
	s.Mesh = fakeMesh{peers: []string{"deadpeer", "livepeer"}}
	s.Driver = drv
	s.Fetcher = ftc
	s.Status = converge.NewPeerStatus()

	if _, err := s.Converge(ctx, ""); err != nil {
		t.Fatalf("Converge with one dead peer must not be fatal: %v", err)
	}
	if !containsStr(ftc.fetched, "deadpeer") || !containsStr(ftc.fetched, "livepeer") {
		t.Errorf("fetched = %v, want both peers attempted", ftc.fetched)
	}
	// Reconcile still ran for the local present item despite the dead peer.
	if len(drv.ids) != 1 || drv.ids[0] != "u1" {
		t.Errorf("driver reconciled ids = %v, want [u1]", drv.ids)
	}
}

func TestConvergeMeshResolveFailsLoud(t *testing.T) {
	s, _ := newTestService(t)
	s.Mesh = fakeMesh{err: errors.New("no mesh")}
	s.Driver = &fakeDriver{}
	s.Fetcher = &fakeFetcher{}
	s.Status = converge.NewPeerStatus()
	if _, err := s.Converge(context.Background(), ""); err == nil {
		t.Fatal("Converge with a failing Mesh returned nil error")
	}
}

// materializeForTeardown creates a real local account, tombstones its uuid,
// and wires the converge fakes inert so only the teardown pass acts.
func materializeForTeardown(t *testing.T, s *Service, m *pool.Manager, uuid string) (int, string, string) {
	t.Helper()
	ctx := context.Background()
	oauthAccount := json.RawMessage(`{"accountUuid":"` + uuid + `"}`)
	res, err := s.Materialize(ctx, materializeVal(uuid, "e@x", oauthAccount), []string{"hostB"}, pullConst(freshEnvelope("at-"+uuid)), materializeManifest)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	row, err := m.Store.GetAccount(res.AccountID)
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if err := s.RecordRemoval(ctx, uuid); err != nil {
		t.Fatalf("RecordRemoval: %v", err)
	}
	s.Mesh = fakeMesh{}
	s.Driver = &fakeDriver{}
	s.Fetcher = &fakeFetcher{}
	s.Status = converge.NewPeerStatus()
	return res.AccountID, row.ConfigDir, row.KeychainService
}

func TestTombstoneTeardownDefersBusy(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	const uuid = "u-busy"
	id, configDir, _ := materializeForTeardown(t, s, m, uuid)

	s.Sessions = fakeSessions{busy: map[string]bool{uuid: true}, reason: "live session"}
	claims := &fakeClaims{}
	s.Claims = claims

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if res.SkippedBusy != 1 || res.Converged != 0 {
		t.Fatalf("result = %+v, want SkippedBusy 1 / Converged 0", res)
	}
	// A busy account is never claimed and never removed.
	if len(claims.claimed) != 0 {
		t.Errorf("claimed %v; a busy teardown must defer before claiming", claims.claimed)
	}
	if _, ok, _ := m.Store.GetAccountByUUID(uuid); !ok {
		t.Errorf("acct-%d row removed while a session was live", id)
	}
	if _, err := os.Stat(configDir); err != nil {
		t.Errorf("account dir removed while busy: %v", err)
	}
}

func TestTombstoneTeardownDefersOnClaimRefused(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	const uuid = "u-claimed-elsewhere"
	id, configDir, _ := materializeForTeardown(t, s, m, uuid)

	s.Sessions = fakeSessions{busy: map[string]bool{}}
	claims := &fakeClaims{refuse: true}
	s.Claims = claims

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if res.SkippedBusy != 1 || res.Converged != 0 {
		t.Fatalf("result = %+v, want SkippedBusy 1 / Converged 0", res)
	}
	if len(claims.claimed) != 1 {
		t.Errorf("claimed = %v, want exactly one attempt", claims.claimed)
	}
	if _, ok, _ := m.Store.GetAccountByUUID(uuid); !ok {
		t.Errorf("acct-%d row removed after the claim was refused", id)
	}
	if _, err := os.Stat(configDir); err != nil {
		t.Errorf("account dir removed after the claim was refused: %v", err)
	}
}

func TestTeardownRemovesEverything(t *testing.T) {
	ctx := context.Background()
	s, m, fk, _ := newMaterializeService(t)
	const uuid = "u-gone"
	id, configDir, keySvc := materializeForTeardown(t, s, m, uuid)

	// Prove the account really exists before teardown.
	if _, ok := fk.Get(keySvc, creds.AccountLabel()); !ok {
		t.Fatalf("precondition: acct-%d credential not seeded in the keychain", id)
	}

	s.Sessions = fakeSessions{busy: map[string]bool{}}
	claims := &fakeClaims{}
	s.Claims = claims

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if res.Converged != 1 || res.SkippedBusy != 0 {
		t.Fatalf("result = %+v, want Converged 1 / SkippedBusy 0", res)
	}
	if len(claims.claimed) != 1 || claims.released != 1 {
		t.Errorf("claim lifecycle = claimed %v / released %d, want one claim + one release", claims.claimed, claims.released)
	}
	// Row, dir, and credential are all gone (Remove(id, true) contract).
	if _, ok, _ := m.Store.GetAccountByUUID(uuid); ok {
		t.Errorf("acct-%d row survived teardown", id)
	}
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		t.Errorf("account dir survived teardown: stat err = %v", err)
	}
	if _, ok := fk.Get(keySvc, creds.AccountLabel()); ok {
		t.Errorf("keychain credential %q survived a delete-credential teardown", keySvc)
	}
}

// TestTeardownIsolatesFailedRemove pins the per-item contract: one failing
// removal is logged and deferred, never aborting the pass or starving the
// other tombstoned accounts.
func TestTeardownIsolatesFailedRemove(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	idX, dirX, _ := materializeForTeardown(t, s, m, "u-wedged")
	idY, dirY, _ := materializeForTeardown(t, s, m, "u-healthy")

	// Wedge X's removal: an unwritable subdir fails os.RemoveAll inside Remove
	// (dir is removed before keychain and row, so both survive for a retry).
	sub := filepath.Join(dirX, "wedge")
	if err := os.MkdirAll(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sub, 0o500); err != nil { //nolint:gosec // G302: deliberately makes the dir read-only to exercise the write-failure path
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sub, 0o700) }) //nolint:gosec // G302: restoring a test dir to traversable perms in cleanup

	s.Sessions = fakeSessions{busy: map[string]bool{}}
	claims := &fakeClaims{}
	s.Claims = claims

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge must not fail the pass on one broken removal: %v", err)
	}
	if res.Converged != 1 {
		t.Fatalf("result = %+v, want exactly the healthy teardown converged", res)
	}
	// The healthy sibling is fully gone regardless of iteration order.
	if _, ok, _ := m.Store.GetAccountByUUID("u-healthy"); ok {
		t.Errorf("healthy acct-%d row survived; the wedged sibling starved it", idY)
	}
	if _, err := os.Stat(dirY); !os.IsNotExist(err) {
		t.Errorf("healthy account dir survived: stat err = %v", err)
	}
	// The wedged account is intact for a later pass: row and dir both remain.
	if _, ok, _ := m.Store.GetAccountByUUID("u-wedged"); !ok {
		t.Errorf("wedged acct-%d row deleted despite the failed remove", idX)
	}
	if _, err := os.Stat(dirX); err != nil {
		t.Errorf("wedged account dir must survive the failed remove: %v", err)
	}
	if claims.released != len(claims.claimed) {
		t.Errorf("claims unbalanced: claimed %v, released %d — a failed remove must still release", claims.claimed, claims.released)
	}
}

// readdClaims grants the claim but lands an explicit re-add first, modeling a
// user re-adding the account between the teardown pass's registry snapshot and
// its claim.
type readdClaims struct {
	s        *Service
	v        AccountValue
	claimErr error
	claimed  int
	released int
}

func (c *readdClaims) TryClaim(string) (func(), bool) {
	c.claimed++
	c.claimErr = c.s.PublishAccount(context.Background(), c.v)
	return func() { c.released++ }, true
}

// TestTeardownRecheckSparesReadd pins the post-claim re-check: a re-add
// landing after the pass snapshot is spared — destroying it would eat a fresh
// login whose chain exists nowhere else.
func TestTeardownRecheckSparesReadd(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	const uuid = "u-readd"
	id, configDir, _ := materializeForTeardown(t, s, m, uuid)

	s.Sessions = fakeSessions{busy: map[string]bool{}}
	claims := &readdClaims{s: s, v: materializeVal(uuid, "e@x", json.RawMessage(`{"accountUuid":"`+uuid+`"}`))}
	s.Claims = claims

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if claims.claimErr != nil {
		t.Fatalf("re-add publish inside the claim: %v", claims.claimErr)
	}
	if claims.claimed != 1 || claims.released != 1 {
		t.Errorf("claim lifecycle = %d/%d, want one claim and one release", claims.claimed, claims.released)
	}
	if res.Converged != 0 {
		t.Errorf("result = %+v; a spared re-add must not count as torn down", res)
	}
	if _, ok, _ := m.Store.GetAccountByUUID(uuid); !ok {
		t.Errorf("acct-%d row destroyed despite the re-add landing first", id)
	}
	if _, err := os.Stat(configDir); err != nil {
		t.Errorf("account dir destroyed despite the re-add landing first: %v", err)
	}
}

// TestTeardownRefusesAmbiguousUUID pins the duplicate-uuid guard: a tombstone
// whose uuid resolves to more than one local row is deferred loudly instead of
// serially destroying every row that shares the uuid.
func TestTeardownRefusesAmbiguousUUID(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	const uuid = "u-dup"
	_, configDir, _ := materializeForTeardown(t, s, m, uuid)

	if err := m.Store.UpsertAccount(store.Account{
		ID: 99, ConfigDir: filepath.Join(t.TempDir(), "dup"), OverlayKind: "symlink",
		KeychainService: "ccp-test-dup", KeychainAccount: "ccp-test",
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Store.SetAccountUUID(99, uuid); err != nil {
		t.Fatal(err)
	}

	s.Sessions = fakeSessions{busy: map[string]bool{}}
	claims := &fakeClaims{}
	s.Claims = claims

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if res.SkippedBusy != 1 || res.Converged != 0 {
		t.Fatalf("result = %+v, want SkippedBusy 1 / Converged 0", res)
	}
	if len(claims.claimed) != 0 {
		t.Errorf("claimed %v; an ambiguous uuid must be refused before claiming", claims.claimed)
	}
	rows, err := m.Store.AccountsByUUID(uuid)
	if err != nil || len(rows) != 2 {
		t.Errorf("rows sharing the uuid = %d (err %v), want both to survive", len(rows), err)
	}
	if _, err := os.Stat(configDir); err != nil {
		t.Errorf("account dir destroyed despite the ambiguity: %v", err)
	}
}
