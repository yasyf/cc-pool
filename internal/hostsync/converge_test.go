package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/daemonkit/proc"
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
	s.Mesh = fakeMesh{self: "self", peers: []string{"peerX", "origin-peer"}}
	s.Driver = drv

	if _, err := s.Converge(ctx, "origin-peer"); err != nil {
		t.Fatalf("Converge: %v", err)
	}

	// The Driver.Reconcile for the present item carries the pass origin verbatim.
	if len(drv.origins) != 1 || drv.origins[0] != "origin-peer" {
		t.Fatalf("driver origins = %v, want [origin-peer]", drv.origins)
	}
}

func TestConvergeUsesMeshOnlyAsProductPeerContext(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	drv := &fakeDriver{load: regWith("u1")}
	s.Mesh = fakeMesh{peers: []string{"deadpeer", "livepeer"}}
	s.Driver = drv

	if _, err := s.Converge(ctx, "origin"); err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if len(drv.ids) != 1 || drv.ids[0] != "u1" {
		t.Errorf("driver reconciled ids = %v, want [u1]", drv.ids)
	}
}

func TestConvergeMeshResolveFailsLoud(t *testing.T) {
	s, _ := newTestService(t)
	s.Mesh = fakeMesh{err: errors.New("no mesh")}
	s.Driver = &fakeDriver{}
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
	res, err := s.Materialize(ctx, materializeVal(uuid, "e@x", oauthAccount), freshEnvelope("at-"+uuid), materializeManifest)
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
	return res.AccountID, pool.AccountBackingDir(res.AccountID), row.KeychainService
}

func TestTombstoneTeardownDefersBusy(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	const uuid = "u-busy"
	id, configDir, _ := materializeForTeardown(t, s, m, uuid)

	s.Sessions = fakeSessions{busy: map[string]bool{uuid: true}, reason: "live session"}

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if res.SkippedBusy != 1 || res.Converged != 0 {
		t.Fatalf("result = %+v, want SkippedBusy 1 / Converged 0", res)
	}
	// A busy account never receives a durable removal intent.
	if calls := s.Remover.(*fixtureAccountRemover).callsSnapshot(); len(calls) != 0 {
		t.Errorf("removal calls = %v; a busy teardown must defer", calls)
	}
	if _, ok, _ := m.Store.GetAccountByUUID(uuid); !ok {
		t.Errorf("acct-%d row removed while a session was live", id)
	}
	if _, err := os.Stat(configDir); err != nil {
		t.Errorf("account dir removed while busy: %v", err)
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
	remover := s.Remover.(*fixtureAccountRemover)

	res, err := s.Converge(ctx, "")
	if err != nil {
		t.Fatalf("Converge: %v", err)
	}
	if res.Converged != 1 || res.SkippedBusy != 0 {
		t.Fatalf("result = %+v, want Converged 1 / SkippedBusy 0", res)
	}
	calls := remover.callsSnapshot()
	if len(calls) != 1 || calls[0] != id {
		t.Errorf("lifecycle removals = %v, want [%d]", calls, id)
	}
	// The lifecycle proof precedes removal of the row, presentation, and credential.
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

	// Fail X at the lifecycle seam. Presentation/backing removal belongs behind
	// this seam, so a presentation-path chmod no longer exercises the contract.
	remover := s.Remover.(*fixtureAccountRemover)
	remover.setFailure(idX, errors.New("holder refused tenant removal"))

	s.Sessions = fakeSessions{busy: map[string]bool{}}

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
	calls := remover.callsSnapshot()
	if len(calls) != 2 || !containsInt(calls, idX) || !containsInt(calls, idY) {
		t.Errorf("lifecycle removals = %v, want exactly acct-%d and acct-%d", calls, idX, idY)
	}
}

func containsInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// TestTeardownRegistryFencePrecedesRemovalIntent pins the ordering boundary:
// blocked registry I/O cannot install the durable account-removal intent.
func TestTeardownRegistryFencePrecedesRemovalIntent(t *testing.T) {
	ctx := context.Background()
	s, m, _, _ := newMaterializeService(t)
	const uuid = "u-registry-fence"
	materializeForTeardown(t, s, m, uuid)

	s.Sessions = fakeSessions{busy: map[string]bool{}}
	remover := s.Remover.(*fixtureAccountRemover)

	lock, err := (proc.FileLockSpec{
		Path: s.Registry.LockPath, Mode: proc.FileLockExclusive, Deadline: time.Second,
	}).Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Converge(ctx, "")
		done <- err
	}()
	<-time.After(100 * time.Millisecond)
	if len(remover.callsSnapshot()) != 0 {
		t.Fatal("removal intent installed while registry lock was unavailable")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Converge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Converge did not resume after registry lock release")
	}
	calls := remover.callsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("removal calls = %v, want one after registry fence completed", calls)
	}
}
