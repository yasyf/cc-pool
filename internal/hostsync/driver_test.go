package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
)

// --- fakes -------------------------------------------------------------------

type uuidSet struct {
	id   int
	uuid string
}
type labelSet struct {
	id    int
	label string
}

// fakeDriverStore is an in-memory DriverStore: it resolves accounts by uuid
// (lowest id wins, like the real ORDER BY id), tags a row's uuid, and relabels a
// row, recording each write for assertions.
type fakeDriverStore struct {
	byID      map[int]*store.Account
	uuidSets  []uuidSet
	labelSets []labelSet
}

func newFakeStore() *fakeDriverStore { return &fakeDriverStore{byID: map[int]*store.Account{}} }

func (s *fakeDriverStore) add(a store.Account) { s.byID[a.ID] = &a }

func (s *fakeDriverStore) GetAccountByUUID(uuid string) (store.Account, bool, error) {
	if uuid == "" {
		return store.Account{}, false, nil
	}
	best := -1
	for id, a := range s.byID {
		if a.AccountUUID == uuid && (best == -1 || id < best) {
			best = id
		}
	}
	if best == -1 {
		return store.Account{}, false, nil
	}
	return *s.byID[best], true, nil
}

func (s *fakeDriverStore) SetAccountUUID(id int, uuid string) error {
	a, ok := s.byID[id]
	if !ok {
		return errors.New("account not found")
	}
	a.AccountUUID = uuid
	s.uuidSets = append(s.uuidSets, uuidSet{id, uuid})
	return nil
}

func (s *fakeDriverStore) SetAccountLabel(id int, label string) error {
	a, ok := s.byID[id]
	if !ok {
		return errors.New("account not found")
	}
	a.Label = label
	s.labelSets = append(s.labelSets, labelSet{id, label})
	return nil
}

type credInstall struct {
	id         int
	expiresAt  int64
	parentHash string
}

// fakeCred is an in-memory CredentialManager: it reports a per-account current
// credential (from cred when seeded, else synthesized from expiry; absent ⇒
// creds.ErrNotFound) and records every install.
type fakeCred struct {
	expiry     map[int]int64
	cred       map[int]*creds.Credential
	readErr    error
	installs   []credInstall
	installOK  bool
	installErr error
}

func newFakeCred() *fakeCred {
	return &fakeCred{expiry: map[int]int64{}, cred: map[int]*creds.Credential{}, installOK: true}
}

func (c *fakeCred) ReadCredential(a store.Account) (*creds.Credential, creds.Source, error) {
	if c.readErr != nil {
		return nil, creds.SourceKeychain, c.readErr
	}
	if cr, ok := c.cred[a.ID]; ok {
		return cr, creds.SourceKeychain, nil
	}
	exp, ok := c.expiry[a.ID]
	if !ok {
		return nil, creds.SourceKeychain, creds.ErrNotFound
	}
	cr := &creds.Credential{}
	cr.ClaudeAiOauth.ExpiresAt = exp
	return cr, creds.SourceKeychain, nil
}

func (c *fakeCred) InstallSyncedCredential(_ context.Context, a store.Account, cred *creds.Credential, chainParentHash string) (bool, error) {
	c.installs = append(c.installs, credInstall{a.ID, cred.ClaudeAiOauth.ExpiresAt, chainParentHash})
	if c.installErr != nil {
		return false, c.installErr
	}
	if c.installOK {
		c.expiry[a.ID] = cred.ClaudeAiOauth.ExpiresAt
		delete(c.cred, a.ID)
	}
	return c.installOK, nil
}

type materializeCall struct {
	uuid  string
	peers []string
}

// fakeMaterializer records each call and, on success, inserts the new row into the
// shared fake store (and seeds its credential expiry) so a later pass resolves it —
// the property TestThreeWayMergeConverges' idempotence turns on.
type fakeMaterializer struct {
	store    *fakeDriverStore
	cred     *fakeCred
	nextID   int
	calls    []materializeCall
	err      error
	deferAll bool
}

func (m *fakeMaterializer) materialize(_ context.Context, v AccountValue, peers []string) (MaterializeResult, error) {
	m.calls = append(m.calls, materializeCall{v.UUID, peers})
	if m.err != nil {
		return MaterializeResult{}, m.err
	}
	if m.deferAll {
		return MaterializeResult{UUID: v.UUID, Deferred: true}, nil
	}
	m.nextID++
	id := m.nextID
	m.store.add(store.Account{ID: id, AccountUUID: v.UUID, Label: v.Label})
	if m.cred != nil {
		m.cred.expiry[id] = v.Chain.ExpiresAt
	}
	return MaterializeResult{UUID: v.UUID, AccountID: id}, nil
}

type pullCall struct {
	uuid      string
	chain     ChainStamp
	localExp  int64
	localHash string
	peers     []string
}

// fakePuller records each pull and returns a canned credential/error.
type fakePuller struct {
	calls []pullCall
	cred  *creds.Credential
	err   error
}

func (p *fakePuller) pull(_ context.Context, uuid string, chain ChainStamp, localExp int64, localHash string, peers []string) (*creds.Credential, error) {
	p.calls = append(p.calls, pullCall{uuid, chain, localExp, localHash, peers})
	return p.cred, p.err
}

// fakeConvergeFetcher serves a fixed per-peer registry and can fail a chosen peer.
// It has NO write method — the structural loop guard.
type fakeConvergeFetcher struct {
	regs    map[string]cregistry.Registry[AccountValue]
	fail    map[string]error
	mu      sync.Mutex
	fetched []string
}

func (f *fakeConvergeFetcher) Fetch(_ context.Context, peer string) (cregistry.Registry[AccountValue], error) {
	f.mu.Lock()
	f.fetched = append(f.fetched, peer)
	f.mu.Unlock()
	if err := f.fail[peer]; err != nil {
		return nil, err
	}
	return f.regs[peer], nil
}

// --- harness -----------------------------------------------------------------

type driverHarness struct {
	svc   *Service
	store *fakeDriverStore
	cred  *fakeCred
	mat   *fakeMaterializer
	pull  *fakePuller
	idx   map[string]int
	d     *Driver
}

func newDriverHarness(t *testing.T) *driverHarness {
	t.Helper()
	svc, _ := newTestService(t)
	svc.Locals = func(context.Context) ([]LocalAccount, error) { return nil, nil }
	st := newFakeStore()
	cr := newFakeCred()
	h := &driverHarness{
		svc:   svc,
		store: st,
		cred:  cr,
		mat:   &fakeMaterializer{store: st, cred: cr},
		pull:  &fakePuller{},
		idx:   map[string]int{},
	}
	h.d = NewDriver(svc, DriverDeps{
		Store:       st,
		Cred:        cr,
		LocalIndex:  func(context.Context) (map[string]int, error) { return h.idx, nil },
		Materialize: h.mat.materialize,
		Pull:        h.pull.pull,
	})
	return h
}

func acctValue(uuid, label, holder string, expiresAt int64, oauth string) AccountValue {
	return AccountValue{
		UUID:         uuid,
		Email:        uuid + "@x.com",
		Label:        label,
		OAuthAccount: json.RawMessage(oauth),
		Chain:        ChainStamp{ExpiresAt: expiresAt, Hash: "h-" + uuid, Holder: holder, RotatedAt: expiresAt - 1},
	}
}

func presentEntry(v AccountValue) cregistry.Entry[AccountValue] {
	return cregistry.Entry[AccountValue]{Added: 100, Value: v}
}

func freshOAuth(uuid string) string { return `{"accountUuid":"` + uuid + `"}` }

// --- table -------------------------------------------------------------------

func TestDriverReconcile(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name        string
		setup       func(h *driverHarness)
		id          string
		val         AccountValue
		peers       []string
		wantOutcome converge.Outcome
		wantErr     bool
		check       func(t *testing.T, h *driverHarness)
	}{
		{
			name:        "materialize-missing",
			id:          "u1",
			val:         acctValue("u1", "peer-u1", "hostA", 5000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeMaterialized,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.mat.calls) != 1 || h.mat.calls[0].uuid != "u1" {
					t.Fatalf("materialize calls = %+v, want one for u1", h.mat.calls)
				}
				if len(h.cred.installs) != 0 {
					t.Fatalf("install ran during a materialize: %+v", h.cred.installs)
				}
			},
		},
		{
			name:        "empty-oauth-deferred",
			id:          "u1",
			val:         acctValue("u1", "peer-u1", "hostA", 5000, "null"),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeDeferred,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.mat.calls) != 0 {
					t.Fatalf("materialize ran for a not-yet-published identity: %+v", h.mat.calls)
				}
			},
		},
		{
			name: "materialize-no-envelope-deferred",
			setup: func(h *driverHarness) {
				h.mat.err = ErrMaterializeNoEnvelope
			},
			id:          "u1",
			val:         acctValue("u1", "peer-u1", "hostA", 5000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeDeferred,
		},
		{
			name: "label-LWW-apply",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "old"})
				h.cred.expiry[1] = 5000
			},
			id:          "u1",
			val:         acctValue("u1", "new", "hostA", 5000, freshOAuth("u1")), // chain equal ⇒ no pull
			peers:       []string{"hostB"},
			wantOutcome: OutcomeLabeled,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.store.labelSets) != 1 || h.store.labelSets[0] != (labelSet{1, "new"}) {
					t.Fatalf("label sets = %+v, want one (1,new)", h.store.labelSets)
				}
				if len(h.pull.calls) != 0 || len(h.cred.installs) != 0 {
					t.Fatalf("a label-only reconcile pulled/installed: pulls=%+v installs=%+v", h.pull.calls, h.cred.installs)
				}
			},
		},
		{
			name: "fresher-pulls-envelope-and-installs",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.expiry[1] = 1000
				h.pull.cred = credWithExpiry(2000)
			},
			id:          "u1",
			val:         acctValue("u1", "same", "hostA", 2000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeCredInstalled,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.pull.calls) != 1 || h.pull.calls[0].localExp != 1000 {
					t.Fatalf("pull calls = %+v, want one with localExp 1000", h.pull.calls)
				}
				if len(h.cred.installs) != 1 || h.cred.installs[0] != (credInstall{1, 2000, ""}) {
					t.Fatalf("installs = %+v, want one (1,2000,\"\")", h.cred.installs)
				}
			},
		},
		{
			name: "child-lineage-pulls-despite-skewed-expiry",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.cred[1] = credWithExpiry(2000) // local parent, expiry skewed AHEAD
				h.pull.cred = childCred(1500)
			},
			id: "u1",
			val: AccountValue{
				UUID:         "u1",
				Email:        "u1@x.com",
				Label:        "same",
				OAuthAccount: json.RawMessage(freshOAuth("u1")),
				Chain:        ChainStamp{ExpiresAt: 1500, Hash: childHash(), Holder: "hostA", ParentHash: localHash()},
			},
			peers:       []string{"hostB"},
			wantOutcome: OutcomeCredInstalled,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.pull.calls) != 1 || h.pull.calls[0].localHash != localHash() {
					t.Fatalf("pull calls = %+v, want one with localHash %q", h.pull.calls, localHash())
				}
				if len(h.cred.installs) != 1 || h.cred.installs[0] != (credInstall{1, 1500, localHash()}) {
					t.Fatalf("installs = %+v, want one (1,1500,parent)", h.cred.installs)
				}
			},
		},
		{
			name: "same-chain-hash-never-pulls",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.cred[1] = credWithExpiry(1000) // same chain, expiry lagging the registry's
			},
			id: "u1",
			val: AccountValue{
				UUID:         "u1",
				Email:        "u1@x.com",
				Label:        "same",
				OAuthAccount: json.RawMessage(freshOAuth("u1")),
				Chain:        ChainStamp{ExpiresAt: 2000, Hash: localHash(), Holder: "hostA"},
			},
			peers:       []string{"hostB"},
			wantOutcome: OutcomeUnchanged,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.pull.calls) != 0 || len(h.cred.installs) != 0 {
					t.Fatalf("same-chain entry pulled/installed: pulls=%+v installs=%+v", h.pull.calls, h.cred.installs)
				}
			},
		},
		{
			name: "staler-registry-chain-never-installed", // the never-write-staler pin
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.expiry[1] = 2000 // local strictly fresher than the registry entry
				h.pull.cred = credWithExpiry(1000)
			},
			id:          "u1",
			val:         acctValue("u1", "same", "hostA", 1000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeUnchanged,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.pull.calls) != 0 {
					t.Fatalf("a staler registry chain triggered a pull: %+v", h.pull.calls)
				}
				if len(h.cred.installs) != 0 {
					t.Fatalf("a staler registry chain triggered an install: %+v", h.cred.installs)
				}
			},
		},
		{
			name: "hash-mismatch-rejected-surfaces-as-deferred",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.expiry[1] = 1000
				h.pull.err = ErrNoPeerCredential // the credpull sentinel: no acceptable envelope
			},
			id:          "u1",
			val:         acctValue("u1", "same", "hostA", 2000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeDeferred,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.pull.calls) != 1 {
					t.Fatalf("pull calls = %+v, want one (the fresher entry was pulled)", h.pull.calls)
				}
				if len(h.cred.installs) != 0 {
					t.Fatalf("a rejected pull still installed: %+v", h.cred.installs)
				}
			},
		},
		{
			name: "holder-unreachable-falls-back",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.expiry[1] = 1000
				h.pull.cred = credWithExpiry(2000) // fallback to a non-holder peer succeeded
			},
			id:          "u1",
			val:         acctValue("u1", "same", "hostA", 2000, freshOAuth("u1")),
			peers:       []string{"hostB", "hostA"},
			wantOutcome: OutcomeCredInstalled,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.pull.calls) != 1 {
					t.Fatalf("pull calls = %+v, want one", h.pull.calls)
				}
				got := h.pull.calls[0]
				if got.chain.Holder != "hostA" {
					t.Fatalf("pull chain holder = %q, want hostA (the holder is tried first)", got.chain.Holder)
				}
				if len(got.peers) != 2 || got.peers[0] != "hostB" || got.peers[1] != "hostA" {
					t.Fatalf("pull peers = %v, want the full mesh for fallback", got.peers)
				}
			},
		},
		{
			name: "keychain-unavailable-deferred",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.expiry[1] = 1000
				h.pull.cred = credWithExpiry(2000)
				h.cred.installErr = creds.ErrUnavailable
			},
			id:          "u1",
			val:         acctValue("u1", "same", "hostA", 2000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeDeferred,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.cred.installs) != 1 {
					t.Fatalf("installs = %+v, want one attempt that the keychain refused", h.cred.installs)
				}
			},
		},
		{
			name: "unchanged",
			setup: func(h *driverHarness) {
				h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "same"})
				h.cred.expiry[1] = 5000
			},
			id:          "u1",
			val:         acctValue("u1", "same", "hostA", 5000, freshOAuth("u1")),
			peers:       []string{"hostB"},
			wantOutcome: OutcomeUnchanged,
			check: func(t *testing.T, h *driverHarness) {
				if len(h.store.labelSets) != 0 || len(h.pull.calls) != 0 || len(h.cred.installs) != 0 {
					t.Fatalf("a no-op reconcile wrote: labels=%+v pulls=%+v installs=%+v",
						h.store.labelSets, h.pull.calls, h.cred.installs)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newDriverHarness(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			outcome, err := h.d.Reconcile(ctx, tc.id, presentEntry(tc.val), tc.peers, "")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Reconcile err = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if outcome != tc.wantOutcome {
				t.Fatalf("outcome = %q, want %q", outcome, tc.wantOutcome)
			}
			if tc.check != nil {
				tc.check(t, h)
			}
		})
	}
}

func credWithExpiry(exp int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.ExpiresAt = exp
	c.ClaudeAiOauth.AccessToken = "at"
	c.ClaudeAiOauth.RefreshToken = "rt"
	return c
}

// childCred is a distinct chain minted off credWithExpiry's token pair.
func childCred(exp int64) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.ExpiresAt = exp
	c.ClaudeAiOauth.AccessToken = "at-child"
	c.ClaudeAiOauth.RefreshToken = "rt-child"
	return c
}

// CredentialHash ignores expiry, so these are expiry-independent.
func localHash() string { return CredentialHash(credWithExpiry(0)) }
func childHash() string { return CredentialHash(childCred(0)) }

// TestDriverUnifyBackfillsUUID pins the LoadRegistry backfill: a local account row
// the store has not yet tagged with its accountUuid is unified with the matching
// registry entry — so Reconcile resolves it and NEVER materializes a duplicate.
func TestDriverUnifyBackfillsUUID(t *testing.T) {
	ctx := context.Background()
	h := newDriverHarness(t)

	// An existing local row with no uuid yet, and the identity index that knows it.
	h.store.add(store.Account{ID: 5, AccountUUID: "", Label: "mine"})
	h.idx["u1"] = 5
	h.svc.Locals = func(context.Context) ([]LocalAccount, error) {
		return []LocalAccount{{UUID: "u1", Email: "u1@x", Label: "mine", Chain: ChainStamp{ExpiresAt: 5000, Hash: "h-u1"}}}, nil
	}

	// A peer already advertises this account in the seeded registry.
	seed := cregistry.New[AccountValue]()
	seed.Add("u1", acctValue("u1", "mine", "hostA", 5000, freshOAuth("u1")), cregistry.Micros(10))
	if err := h.svc.Registry.Save(seed); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	reg, err := h.d.LoadRegistry(ctx)
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	if len(h.store.uuidSets) != 1 || h.store.uuidSets[0] != (uuidSet{5, "u1"}) {
		t.Fatalf("uuid backfills = %+v, want one (5,u1)", h.store.uuidSets)
	}

	h.cred.expiry[5] = 5000 // chain equal ⇒ no pull
	outcome, err := h.d.Reconcile(ctx, "u1", reg["u1"], []string{"hostB"}, "")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if outcome != OutcomeUnchanged {
		t.Fatalf("outcome = %q, want unchanged (unified, not materialized)", outcome)
	}
	if len(h.mat.calls) != 0 {
		t.Fatalf("a tagged local account was materialized as a duplicate: %+v", h.mat.calls)
	}

	// A second LoadRegistry finds the row already tagged and writes nothing.
	if _, err := h.d.LoadRegistry(ctx); err != nil {
		t.Fatalf("second LoadRegistry: %v", err)
	}
	if len(h.store.uuidSets) != 1 {
		t.Fatalf("backfill wrote again on a converged host: %+v", h.store.uuidSets)
	}
}

// TestSaveRegistryTouchesChangedStamps proves SaveRegistry notifies peers of only
// the entries a pass changed: a new entry and a mutated one get their stamp
// touched, an untouched one does not.
func TestSaveRegistryTouchesChangedStamps(t *testing.T) {
	ctx := context.Background()
	h := newDriverHarness(t)

	// On-disk baseline: u1@stamp100, u3@stamp30.
	base := cregistry.New[AccountValue]()
	base.Add("u1", acctValue("u1", "one", "hostA", 1000, freshOAuth("u1")), cregistry.Micros(100))
	base.Add("u3", acctValue("u3", "three", "hostA", 3000, freshOAuth("u3")), cregistry.Micros(30))
	if err := h.svc.Registry.Save(base); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := h.d.LoadRegistry(ctx); err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}

	// The merged registry: u1 changed (new value + higher stamp), u3 identical, u2 new.
	merged := cregistry.New[AccountValue]()
	merged.Add("u1", acctValue("u1", "one-renamed", "hostA", 1000, freshOAuth("u1")), cregistry.Micros(200))
	merged.Add("u3", acctValue("u3", "three", "hostA", 3000, freshOAuth("u3")), cregistry.Micros(30))
	merged.Add("u2", acctValue("u2", "two", "hostB", 2000, freshOAuth("u2")), cregistry.Micros(20))

	if err := h.d.SaveRegistry(ctx, merged); err != nil {
		t.Fatalf("SaveRegistry: %v", err)
	}
	if _, ok := stampSig(t, h.svc, "u1"); !ok {
		t.Error("changed entry u1 did not get its stamp touched")
	}
	if _, ok := stampSig(t, h.svc, "u2"); !ok {
		t.Error("new entry u2 did not get its stamp touched")
	}
	if _, ok := stampSig(t, h.svc, "u3"); ok {
		t.Error("unchanged entry u3 had its stamp touched")
	}
}

// TestThreeWayMergeConverges pins converge end to end over three registries:
// the merge is byte-identical regardless of peer order, and a second pass is
// idempotent.
func TestThreeWayMergeConverges(t *testing.T) {
	ctx := context.Background()

	// u1 is local to host A; u2 lives on B, u3 on C.
	vA := acctValue("u1", "one", "hostA", 1000, freshOAuth("u1"))
	vB := acctValue("u2", "two", "hostB", 2000, freshOAuth("u2"))
	vC := acctValue("u3", "three", "hostC", 3000, freshOAuth("u3"))

	regB := cregistry.New[AccountValue]()
	regB.Add("u1", vA, cregistry.Micros(10))
	regB.Add("u2", vB, cregistry.Micros(20))
	regC := cregistry.New[AccountValue]()
	regC.Add("u1", vA, cregistry.Micros(10))
	regC.Add("u3", vC, cregistry.Micros(30))

	// runOrder builds a fresh host A (with u1 already local + converged) and runs one
	// converge pass pulling B and C in the given order; it returns the persisted
	// registry bytes and the pass outcomes.
	runOrder := func(t *testing.T, order []string) ([]byte, map[string]converge.Outcome) {
		t.Helper()
		h := newDriverHarness(t)
		h.mat.nextID = 1 // materialized ids start at 2 (u1 is the pre-seeded acct-1)
		h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "one"})
		h.cred.expiry[1] = 1000

		regA := cregistry.New[AccountValue]()
		regA.Add("u1", vA, cregistry.Micros(10))
		if err := h.svc.Registry.Save(regA); err != nil {
			t.Fatalf("seed regA: %v", err)
		}

		fetcher := &fakeConvergeFetcher{regs: map[string]cregistry.Registry[AccountValue]{"hostB": regB, "hostC": regC}}
		results, err := converge.Reconcile(ctx, h.svc.Registry.WithLock, h.d, fetcher, converge.NewPeerStatus(), order, "")
		if err != nil {
			t.Fatalf("converge (order %v): %v", order, err)
		}
		outcomes := map[string]converge.Outcome{}
		for _, r := range results {
			if r.Err != nil {
				t.Fatalf("item %s errored: %v", r.ID, r.Err)
			}
			outcomes[r.ID] = r.Outcome
		}
		if h.pull.calls != nil {
			t.Fatalf("no pull should happen (all chains equal): %+v", h.pull.calls)
		}

		// Second pass: every account now resolves locally and matches ⇒ no mutation.
		second, err := converge.Reconcile(ctx, h.svc.Registry.WithLock, h.d, fetcher, converge.NewPeerStatus(), order, "")
		if err != nil {
			t.Fatalf("second converge: %v", err)
		}
		for _, r := range second {
			if r.Outcome != OutcomeUnchanged {
				t.Fatalf("second pass item %s outcome = %q, want unchanged (idempotence)", r.ID, r.Outcome)
			}
		}

		raw, err := os.ReadFile(h.svc.Registry.Path)
		if err != nil {
			t.Fatalf("read persisted registry: %v", err)
		}
		return raw, outcomes
	}

	bc, outcomes := runOrder(t, []string{"hostB", "hostC"})
	cb, _ := runOrder(t, []string{"hostC", "hostB"})

	if string(bc) != string(cb) {
		t.Fatalf("merged registry differs by peer order:\n[B,C]=%s\n[C,B]=%s", bc, cb)
	}
	// First pass: u1 already local (unchanged), u2 and u3 materialized.
	if outcomes["u1"] != OutcomeUnchanged {
		t.Errorf("u1 outcome = %q, want unchanged", outcomes["u1"])
	}
	if outcomes["u2"] != OutcomeMaterialized || outcomes["u3"] != OutcomeMaterialized {
		t.Errorf("peer accounts not materialized: u2=%q u3=%q", outcomes["u2"], outcomes["u3"])
	}
}

// TestUnreachablePeerSkippedNotFatal proves one peer failing its fetch never aborts
// the pass: the pass still converges against every peer that answered, and the
// account only the reachable peer advertised is materialized.
func TestUnreachablePeerSkippedNotFatal(t *testing.T) {
	ctx := context.Background()
	h := newDriverHarness(t)
	h.mat.nextID = 1
	h.store.add(store.Account{ID: 1, AccountUUID: "u1", Label: "one"})
	h.cred.expiry[1] = 1000

	regA := cregistry.New[AccountValue]()
	regA.Add("u1", acctValue("u1", "one", "hostA", 1000, freshOAuth("u1")), cregistry.Micros(10))
	if err := h.svc.Registry.Save(regA); err != nil {
		t.Fatalf("seed: %v", err)
	}

	regC := cregistry.New[AccountValue]()
	regC.Add("u1", acctValue("u1", "one", "hostA", 1000, freshOAuth("u1")), cregistry.Micros(10))
	regC.Add("u3", acctValue("u3", "three", "hostC", 3000, freshOAuth("u3")), cregistry.Micros(30))

	fetcher := &fakeConvergeFetcher{
		regs: map[string]cregistry.Registry[AccountValue]{"hostC": regC},
		fail: map[string]error{"hostB": errors.New("connection refused")},
	}

	results, err := converge.Reconcile(ctx, h.svc.Registry.WithLock, h.d, fetcher, converge.NewPeerStatus(), []string{"hostB", "hostC"}, "")
	if err != nil {
		t.Fatalf("converge must not fail when one peer is down: %v", err)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("item %s errored: %v", r.ID, r.Err)
		}
	}
	if len(h.mat.calls) != 1 || h.mat.calls[0].uuid != "u3" {
		t.Fatalf("materialize calls = %+v, want u3 (learned from the reachable peer)", h.mat.calls)
	}
}
