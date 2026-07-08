package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/synckit/cregistry"
)

// fakeClock is a deterministic, mutable wall clock for the stamp-ordering tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func (c *fakeClock) set(t time.Time)         { c.t = t }

// newTestService builds a Service over a temp registry + temp stamp dir driven by
// a fake clock, with no external command runner.
func newTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	rf := tempRegistry(t)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := &Service{
		Registry: &rf,
		StampDir: filepath.Join(t.TempDir(), "stamps"),
		Now:      clk.now,
	}
	return s, clk
}

func acctVal(uuid, email, label, holder string, expiresAt int64) AccountValue {
	return AccountValue{
		UUID:         uuid,
		Email:        email,
		Label:        label,
		OAuthAccount: json.RawMessage(`{"accountUuid":"` + uuid + `"}`),
		Chain:        ChainStamp{ExpiresAt: expiresAt, Hash: "h-" + uuid, Holder: holder, RotatedAt: expiresAt - 100},
	}
}

// loadEntry reads uuid's current entry off the service's registry.
func loadEntry(t *testing.T, s *Service, uuid string) (cregistry.Entry[AccountValue], bool) {
	t.Helper()
	reg, err := s.Registry.Load()
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	e, ok := reg[uuid]
	return e, ok
}

// stampSig returns the current stamp file's content and whether it exists, the
// signature the no-op-vs-touch assertions compare.
func stampSig(t *testing.T, s *Service, uuid string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(s.StampDir, uuid, "stamp"))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatalf("read stamp %s: %v", uuid, err)
	}
	return string(b), true
}

func TestPublishAccountOverridesTombstone(t *testing.T) {
	ctx := context.Background()

	t.Run("fresh publish is present", func(t *testing.T) {
		s, _ := newTestService(t)
		v := acctVal("u1", "e@x.com", "work", "hostA", 1000)
		if err := s.PublishAccount(ctx, v); err != nil {
			t.Fatalf("PublishAccount: %v", err)
		}
		e, ok := loadEntry(t, s, "u1")
		if !ok || !e.Present() {
			t.Fatalf("account not present after publish: ok=%v entry=%+v", ok, e)
		}
		if e.Value.Chain.Holder != "hostA" || e.Value.Label != "work" {
			t.Errorf("value not stored: %+v", e.Value)
		}
		if _, ok := stampSig(t, s, "u1"); !ok {
			t.Error("publish did not touch the stamp")
		}
	})

	t.Run("re-publish overrides a normal tombstone", func(t *testing.T) {
		s, clk := newTestService(t)
		if err := s.PublishAccount(ctx, acctVal("u1", "e", "l1", "hostA", 1000)); err != nil {
			t.Fatalf("publish: %v", err)
		}
		clk.advance(time.Second)
		if err := s.RecordRemoval(ctx, "u1"); err != nil {
			t.Fatalf("removal: %v", err)
		}
		if e, _ := loadEntry(t, s, "u1"); e.Present() {
			t.Fatal("account still present after removal")
		}
		clk.advance(time.Second)
		if err := s.PublishAccount(ctx, acctVal("u1", "e", "l2", "hostB", 2000)); err != nil {
			t.Fatalf("re-publish: %v", err)
		}
		e, _ := loadEntry(t, s, "u1")
		if !e.Present() {
			t.Fatal("re-publish did not override the tombstone")
		}
		if e.Value.Label != "l2" || e.Value.Chain.Holder != "hostB" {
			t.Errorf("re-published value not applied: %+v", e.Value)
		}
	})

	t.Run("re-publish overrides a future-skewed tombstone", func(t *testing.T) {
		s, _ := newTestService(t)
		// A removal recorded under a badly-skewed clock lands far in the future,
		// past the current wall clock. max(now, Removed+1) must still flip Present.
		const future = cregistry.Micros(9_000_000_000_000_000)
		reg := cregistry.New[AccountValue]()
		reg.Add("u1", acctVal("u1", "e", "old", "hostA", 1000), 1)
		reg.Remove("u1", future)
		if err := s.Registry.Save(reg); err != nil {
			t.Fatalf("seed skewed tombstone: %v", err)
		}
		if err := s.PublishAccount(ctx, acctVal("u1", "e", "new", "hostB", 2000)); err != nil {
			t.Fatalf("re-publish: %v", err)
		}
		e, _ := loadEntry(t, s, "u1")
		if !e.Present() {
			t.Fatalf("future-skewed tombstone not overridden: added=%d removed=%d", e.Added, e.Removed)
		}
		if e.Added != future+1 {
			t.Errorf("add stamp = %d, want future+1 = %d", e.Added, future+1)
		}
		if e.Value.Label != "new" {
			t.Errorf("value not updated across skewed override: %+v", e.Value)
		}
	})

	t.Run("empty uuid fails loud", func(t *testing.T) {
		s, _ := newTestService(t)
		if err := s.PublishAccount(ctx, AccountValue{}); err == nil {
			t.Fatal("PublishAccount with empty UUID returned nil error")
		}
	})
}

func TestRecordLabelLWW(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestService(t)
	v := acctVal("u1", "e@x.com", "before", "hostA", 1000)
	if err := s.PublishAccount(ctx, v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	published, _ := loadEntry(t, s, "u1")

	clk.advance(time.Second)
	if err := s.RecordLabel(ctx, "u1", "after"); err != nil {
		t.Fatalf("RecordLabel: %v", err)
	}
	e, _ := loadEntry(t, s, "u1")
	if e.Value.Label != "after" {
		t.Errorf("label = %q, want after", e.Value.Label)
	}
	// Every non-label field is preserved. Compared against the post-publish loaded
	// entry (not the compact literal v) because the registry's MarshalIndent
	// re-indents the embedded RawMessage, so both loads share identical bytes.
	if e.Value.Email != published.Value.Email || e.Value.Chain != published.Value.Chain ||
		string(e.Value.OAuthAccount) != string(published.Value.OAuthAccount) {
		t.Errorf("RecordLabel disturbed other fields: %+v", e.Value)
	}
	if e.Added <= published.Added {
		t.Errorf("add stamp did not advance: %d <= %d", e.Added, published.Added)
	}

	// A later local rename always lands: even with the wall clock rewound behind
	// the winning add, forceStamp advances the add stamp past entry.Added, so the
	// rename applies and touches the stamp. (Cross-host order still resolves by the
	// LWW max-join — see TestRecordLabelCrossHostLWW.)
	sig, _ := stampSig(t, s, "u1")
	rewBefore, _ := loadEntry(t, s, "u1")
	clk.set(time.Unix(1_600_000_000, 0)) // wall clock rewound behind the winning add
	if err := s.RecordLabel(ctx, "u1", "rewound"); err != nil {
		t.Fatalf("rewound RecordLabel: %v", err)
	}
	rew, _ := loadEntry(t, s, "u1")
	if rew.Value.Label != "rewound" {
		t.Errorf("later local rename did not land: got %q, want rewound", rew.Value.Label)
	}
	if rew.Added <= rewBefore.Added {
		t.Errorf("forced add stamp did not advance past entry.Added: %d <= %d", rew.Added, rewBefore.Added)
	}
	if got, _ := stampSig(t, s, "u1"); got == sig {
		t.Error("forced RecordLabel did not touch the stamp")
	}

	// Renaming an unknown account fails loud rather than resurrecting it.
	if err := s.RecordLabel(ctx, "ghost", "x"); err == nil {
		t.Fatal("RecordLabel for unknown account returned nil error")
	}
	// Renaming a removed account must not resurrect it.
	clk.set(time.Unix(1_800_000_000, 0))
	if err := s.RecordRemoval(ctx, "u1"); err != nil {
		t.Fatalf("removal: %v", err)
	}
	clk.advance(time.Second)
	if err := s.RecordLabel(ctx, "u1", "zombie"); err == nil {
		t.Fatal("RecordLabel resurrected a tombstoned account")
	}
	if e, _ := loadEntry(t, s, "u1"); e.Present() {
		t.Fatal("tombstoned account became present via RecordLabel")
	}
}

func TestNoteCredWriteNoopOnEqualChain(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestService(t)
	if err := s.PublishAccount(ctx, acctVal("u1", "e", "l", "hostA", 1000)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	base, _ := loadEntry(t, s, "u1")
	clk.advance(time.Second)
	sig, ok := stampSig(t, s, "u1")
	if !ok {
		t.Fatal("expected a stamp after publish")
	}

	noops := []struct {
		name  string
		chain ChainStamp
	}{
		{"equal expiry", ChainStamp{ExpiresAt: 1000, Hash: "other", Holder: "hostB"}},
		{"staler expiry", ChainStamp{ExpiresAt: 999, Hash: "other", Holder: "hostB"}},
	}
	for _, tc := range noops {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.NoteCredWrite(ctx, "u1", tc.chain); err != nil {
				t.Fatalf("NoteCredWrite: %v", err)
			}
			e, _ := loadEntry(t, s, "u1")
			if e.Value.Chain != base.Value.Chain {
				t.Errorf("chain mutated on no-op: %+v", e.Value.Chain)
			}
			if e.Added != base.Added {
				t.Errorf("add stamp advanced on no-op: %d -> %d", base.Added, e.Added)
			}
			if got, _ := stampSig(t, s, "u1"); got != sig {
				t.Error("no-op NoteCredWrite touched the stamp")
			}
		})
	}

	t.Run("strictly fresher updates and touches", func(t *testing.T) {
		clk.advance(time.Second)
		fresh := ChainStamp{ExpiresAt: 2000, Hash: "fresh", Holder: "hostB", RotatedAt: 1900}
		if err := s.NoteCredWrite(ctx, "u1", fresh); err != nil {
			t.Fatalf("NoteCredWrite fresher: %v", err)
		}
		e, _ := loadEntry(t, s, "u1")
		if e.Value.Chain != fresh {
			t.Errorf("fresher chain not installed: %+v", e.Value.Chain)
		}
		if got, _ := stampSig(t, s, "u1"); got == sig {
			t.Error("fresher NoteCredWrite did not touch the stamp")
		}
	})

	t.Run("absent account is a no-op, never created", func(t *testing.T) {
		if err := s.NoteCredWrite(ctx, "ghost", ChainStamp{ExpiresAt: 5000}); err != nil {
			t.Fatalf("NoteCredWrite absent: %v", err)
		}
		if _, ok := loadEntry(t, s, "ghost"); ok {
			t.Error("NoteCredWrite created an entry for an absent account")
		}
	})

	t.Run("tombstoned account is never resurrected", func(t *testing.T) {
		clk.advance(time.Second)
		if err := s.RecordRemoval(ctx, "u1"); err != nil {
			t.Fatalf("removal: %v", err)
		}
		clk.advance(time.Second)
		if err := s.NoteCredWrite(ctx, "u1", ChainStamp{ExpiresAt: 9999, Hash: "x"}); err != nil {
			t.Fatalf("NoteCredWrite on tombstone: %v", err)
		}
		if e, _ := loadEntry(t, s, "u1"); e.Present() {
			t.Fatal("NoteCredWrite resurrected a tombstoned account")
		}
	})
}

func TestClaimHolderAndLeaseRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestService(t)
	if err := s.PublishAccount(ctx, acctVal("u1", "e", "l", "", 1000)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	clk.advance(time.Second)
	if err := s.ClaimHolder(ctx, "u1", "hostA"); err != nil {
		t.Fatalf("ClaimHolder: %v", err)
	}
	if e, _ := loadEntry(t, s, "u1"); e.Value.Chain.Holder != "hostA" {
		t.Fatalf("holder = %q, want hostA", e.Value.Chain.Holder)
	}

	clk.advance(time.Second)
	if err := s.RenewLease(ctx, "u1", "hostA", 5000); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	e, _ := loadEntry(t, s, "u1")
	if e.Value.Lease == nil || e.Value.Lease.Host != "hostA" || e.Value.Lease.Until != 5000 {
		t.Fatalf("lease not set: %+v", e.Value.Lease)
	}
	if e.Value.Chain.Holder != "hostA" {
		t.Errorf("holder lost across RenewLease: %+v", e.Value.Chain)
	}

	// Renew extends the same lease.
	clk.advance(time.Second)
	if err := s.RenewLease(ctx, "u1", "hostA", 6000); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if e, _ := loadEntry(t, s, "u1"); e.Value.Lease.Until != 6000 {
		t.Errorf("lease not renewed: until = %d", e.Value.Lease.Until)
	}

	// A non-owner cannot release the lease: no-op, no stamp touch.
	clk.advance(time.Second)
	sig, _ := stampSig(t, s, "u1")
	if err := s.ReleaseLease(ctx, "u1", "hostB"); err != nil {
		t.Fatalf("ReleaseLease non-owner: %v", err)
	}
	e, _ = loadEntry(t, s, "u1")
	if e.Value.Lease == nil || e.Value.Lease.Host != "hostA" {
		t.Errorf("non-owner released the lease: %+v", e.Value.Lease)
	}
	if got, _ := stampSig(t, s, "u1"); got != sig {
		t.Error("non-owner ReleaseLease touched the stamp")
	}

	// The owner releases the lease.
	clk.advance(time.Second)
	if err := s.ReleaseLease(ctx, "u1", "hostA"); err != nil {
		t.Fatalf("ReleaseLease owner: %v", err)
	}
	e, _ = loadEntry(t, s, "u1")
	if e.Value.Lease != nil {
		t.Errorf("owner did not release the lease: %+v", e.Value.Lease)
	}
	if e.Value.Chain.Holder != "hostA" {
		t.Errorf("holder lost across ReleaseLease: %+v", e.Value.Chain)
	}
	if got, _ := stampSig(t, s, "u1"); got == sig {
		t.Error("owner ReleaseLease did not touch the stamp")
	}

	// Lease ops on an unknown account fail loud.
	if err := s.ClaimHolder(ctx, "ghost", "hostA"); err == nil {
		t.Error("ClaimHolder for unknown account returned nil error")
	}
	if err := s.RenewLease(ctx, "ghost", "hostA", 1); err == nil {
		t.Error("RenewLease for unknown account returned nil error")
	}
	// Releasing a lease on an unknown account is a tolerant no-op.
	if err := s.ReleaseLease(ctx, "ghost", "hostA"); err != nil {
		t.Errorf("ReleaseLease for unknown account errored: %v", err)
	}
}

func TestScanPublishNewLocalAccount(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	s.Locals = func(context.Context) ([]LocalAccount, error) {
		return []LocalAccount{
			{UUID: "u1", Email: "e1@x", Label: "one", Chain: ChainStamp{ExpiresAt: 1000, Hash: "h1", Holder: "hostA"}},
			{UUID: "u2", Email: "e2@x", Label: "two", Chain: ChainStamp{ExpiresAt: 2000, Hash: "h2", Holder: "hostA"}},
		}, nil
	}
	reg := cregistry.New[AccountValue]()
	changed, err := s.ScanPublish(ctx, reg)
	if err != nil {
		t.Fatalf("ScanPublish: %v", err)
	}
	if !changed {
		t.Fatal("ScanPublish reported no change for two brand-new locals")
	}
	for _, want := range []struct {
		uuid, email, label string
		expiresAt          int64
	}{{"u1", "e1@x", "one", 1000}, {"u2", "e2@x", "two", 2000}} {
		e, ok := reg[want.uuid]
		if !ok || !e.Present() {
			t.Fatalf("%s not present after scan: ok=%v", want.uuid, ok)
		}
		if e.Value.Email != want.email || e.Value.Label != want.label || e.Value.Chain.ExpiresAt != want.expiresAt {
			t.Errorf("%s value = %+v", want.uuid, e.Value)
		}
	}

	// Idempotent: a second scan with identical locals changes nothing.
	changed, err = s.ScanPublish(ctx, reg)
	if err != nil {
		t.Fatalf("second ScanPublish: %v", err)
	}
	if changed {
		t.Error("second identical ScanPublish reported a change")
	}
}

func TestScanPublishFresherChainOnly(t *testing.T) {
	ctx := context.Background()

	// The registry entry carries peer-set metadata (label, email, oauthAccount)
	// that a local scan must never clobber — only the chain may move, and only when
	// strictly fresher.
	regVal := AccountValue{
		UUID:         "u1",
		Email:        "registry@x",
		Label:        "registry-label",
		OAuthAccount: json.RawMessage(`{"registry":true}`),
		Chain:        ChainStamp{ExpiresAt: 1000, Hash: "reg", Holder: "hostA"},
	}

	cases := []struct {
		name        string
		localExpiry int64
		wantChanged bool
		wantExpiry  int64
	}{
		{"equal chain untouched", 1000, false, 1000},
		{"staler chain untouched", 999, false, 1000},
		{"fresher chain updates", 2000, true, 2000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestService(t)
			s.Locals = func(context.Context) ([]LocalAccount, error) {
				return []LocalAccount{{
					UUID:  "u1",
					Email: "local@x", // differs from the registry — must be ignored
					Label: "local-label",
					Chain: ChainStamp{ExpiresAt: tc.localExpiry, Hash: "local", Holder: "hostB"},
				}}, nil
			}
			reg := cregistry.New[AccountValue]()
			reg.Add("u1", regVal, cregistry.Micros(1000))
			before := reg["u1"].Added

			changed, err := s.ScanPublish(ctx, reg)
			if err != nil {
				t.Fatalf("ScanPublish: %v", err)
			}
			if changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tc.wantChanged)
			}
			e := reg["u1"]
			if e.Value.Chain.ExpiresAt != tc.wantExpiry {
				t.Errorf("chain expiry = %d, want %d", e.Value.Chain.ExpiresAt, tc.wantExpiry)
			}
			// Non-chain fields always come from the registry, never the local scan.
			if e.Value.Email != "registry@x" || e.Value.Label != "registry-label" || string(e.Value.OAuthAccount) != `{"registry":true}` {
				t.Errorf("scan clobbered non-chain fields: %+v", e.Value)
			}
			if !tc.wantChanged && e.Added != before {
				t.Errorf("no-op scan advanced the add stamp: %d -> %d", before, e.Added)
			}
			if tc.wantChanged && e.Value.Chain.Hash != "local" {
				t.Errorf("fresher scan did not adopt the local chain hash: %q", e.Value.Chain.Hash)
			}
		})
	}
}

func TestScanPublishNeverResurrectsTombstone(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	// A super-fresh local chain must not resurrect a removed account.
	s.Locals = func(context.Context) ([]LocalAccount, error) {
		return []LocalAccount{{
			UUID:  "u1",
			Email: "e",
			Label: "l",
			Chain: ChainStamp{ExpiresAt: 9_000_000_000_000, Hash: "superfresh", Holder: "hostB"},
		}}, nil
	}
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", acctVal("u1", "e", "l", "hostA", 1000), cregistry.Micros(10))
	reg.Remove("u1", cregistry.Micros(20)) // tombstoned: Removed > Added

	changed, err := s.ScanPublish(ctx, reg)
	if err != nil {
		t.Fatalf("ScanPublish: %v", err)
	}
	if changed {
		t.Fatal("ScanPublish resurrected a tombstone (reported a change)")
	}
	if reg["u1"].Present() {
		t.Fatal("tombstoned account became present after scan — removal pin broken")
	}
	if reg["u1"].Value.Chain.ExpiresAt != 1000 {
		t.Errorf("tombstoned entry's chain was mutated: %+v", reg["u1"].Value.Chain)
	}
}

func TestNudgeSynckitdBestEffort(t *testing.T) {
	ctx := context.Background()

	t.Run("runs synckitd register with the manifest path", func(t *testing.T) {
		s, _ := newTestService(t)
		var gotName string
		var gotArgs []string
		s.Run = func(_ context.Context, name string, args ...string) error {
			gotName, gotArgs = name, args
			return nil
		}
		s.NudgeSynckitd(ctx, "/cfg/cc-pool.json")
		if gotName != "synckitd" {
			t.Errorf("ran %q, want synckitd", gotName)
		}
		want := []string{"register", "/cfg/cc-pool.json"}
		if len(gotArgs) != len(want) || gotArgs[0] != want[0] || gotArgs[1] != want[1] {
			t.Errorf("args = %v, want %v", gotArgs, want)
		}
	})

	t.Run("swallows a runner error", func(t *testing.T) {
		s, _ := newTestService(t)
		s.Run = func(context.Context, string, ...string) error {
			return errors.New("synckitd not on PATH")
		}
		// Must not panic or block; the failure is advisory.
		s.NudgeSynckitd(ctx, "/cfg/cc-pool.json")
	})
}

// TestTouchStampCreatesTree proves TouchStamp creates the per-account dir on
// demand and writes a stamp a watcher can observe.
func TestTouchStampCreatesTree(t *testing.T) {
	s, _ := newTestService(t)
	if err := s.TouchStamp("u-new"); err != nil {
		t.Fatalf("TouchStamp: %v", err)
	}
	if _, ok := stampSig(t, s, "u-new"); !ok {
		t.Fatal("TouchStamp did not create the stamp file")
	}
	fi, err := os.Stat(filepath.Join(s.StampDir, "u-new"))
	if err != nil {
		t.Fatalf("stat stamp dir: %v", err)
	}
	if !fi.IsDir() {
		t.Fatal("stamp path is not a directory")
	}
}

// skewAhead is a stamp far past newTestService's fake wall clock (Unix
// 1_700_000_000): the peer-stamped-under-skew shape where a bare s.stamp() add
// would silently no-op.
const skewAhead = cregistry.Micros(9_000_000_000_000_000)

// TestLocalMutationForcesPastSkewedAdd seeds every mutate-based local write with
// an entry whose add/remove stamp is AHEAD of the fake clock and proves the
// mutation still lands (present, value applied, add stamp forced past the skew,
// stamp touched). Removing forceStamp from any one method's Add regresses its
// case: the bare wall-clock stamp is behind the seeded stamp, so cregistry.Add
// no-ops. This is the exact shape findings 1-3 turn on.
func TestLocalMutationForcesPastSkewedAdd(t *testing.T) {
	ctx := context.Background()

	seedPresent := func(t *testing.T, s *Service, v AccountValue) {
		t.Helper()
		reg := cregistry.New[AccountValue]()
		reg.Add(v.UUID, v, skewAhead)
		if err := s.Registry.Save(reg); err != nil {
			t.Fatalf("seed present: %v", err)
		}
	}
	leased := func(host string) AccountValue {
		v := acctVal("u1", "e", "l", "hostA", 1000)
		v.Lease = &Lease{Host: host, Until: 5000}
		return v
	}

	cases := []struct {
		name   string
		seed   func(t *testing.T, s *Service)
		mutate func(s *Service) error
		check  func(t *testing.T, e cregistry.Entry[AccountValue])
	}{
		{
			name: "NoteCredWrite fresher chain",
			seed: func(t *testing.T, s *Service) { seedPresent(t, s, acctVal("u1", "e", "l", "hostA", 1000)) },
			mutate: func(s *Service) error {
				return s.NoteCredWrite(ctx, "u1", ChainStamp{ExpiresAt: 2000, Hash: "fresh", Holder: "hostA", RotatedAt: 1900})
			},
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Chain.ExpiresAt != 2000 || e.Value.Chain.Hash != "fresh" {
					t.Errorf("fresher chain not installed: %+v", e.Value.Chain)
				}
			},
		},
		{
			name:   "PublishAccount over present entry",
			seed:   func(t *testing.T, s *Service) { seedPresent(t, s, acctVal("u1", "e", "old", "hostA", 1000)) },
			mutate: func(s *Service) error { return s.PublishAccount(ctx, acctVal("u1", "e", "new", "hostB", 2000)) },
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Label != "new" || e.Value.Chain.Holder != "hostB" {
					t.Errorf("relogin publish did not land: %+v", e.Value)
				}
			},
		},
		{
			name: "PublishAccount over tombstone",
			seed: func(t *testing.T, s *Service) {
				reg := cregistry.New[AccountValue]()
				reg.Add("u1", acctVal("u1", "e", "old", "hostA", 1000), cregistry.Micros(10))
				reg.Remove("u1", skewAhead)
				if err := s.Registry.Save(reg); err != nil {
					t.Fatalf("seed tombstone: %v", err)
				}
			},
			mutate: func(s *Service) error { return s.PublishAccount(ctx, acctVal("u1", "e", "new", "hostB", 2000)) },
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Label != "new" {
					t.Errorf("re-publish over skewed tombstone did not land: %+v", e.Value)
				}
			},
		},
		{
			name:   "RecordLabel",
			seed:   func(t *testing.T, s *Service) { seedPresent(t, s, acctVal("u1", "e", "old", "hostA", 1000)) },
			mutate: func(s *Service) error { return s.RecordLabel(ctx, "u1", "renamed") },
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Label != "renamed" {
					t.Errorf("rename did not land: %q", e.Value.Label)
				}
			},
		},
		{
			name:   "ClaimHolder",
			seed:   func(t *testing.T, s *Service) { seedPresent(t, s, acctVal("u1", "e", "l", "", 1000)) },
			mutate: func(s *Service) error { return s.ClaimHolder(ctx, "u1", "hostZ") },
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Chain.Holder != "hostZ" {
					t.Errorf("holder claim did not land: %q", e.Value.Chain.Holder)
				}
			},
		},
		{
			name:   "RenewLease",
			seed:   func(t *testing.T, s *Service) { seedPresent(t, s, acctVal("u1", "e", "l", "hostA", 1000)) },
			mutate: func(s *Service) error { return s.RenewLease(ctx, "u1", "hostA", 6000) },
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Lease == nil || e.Value.Lease.Until != 6000 {
					t.Errorf("lease renew did not land: %+v", e.Value.Lease)
				}
			},
		},
		{
			name:   "ReleaseLease",
			seed:   func(t *testing.T, s *Service) { seedPresent(t, s, leased("hostA")) },
			mutate: func(s *Service) error { return s.ReleaseLease(ctx, "u1", "hostA") },
			check: func(t *testing.T, e cregistry.Entry[AccountValue]) {
				if e.Value.Lease != nil {
					t.Errorf("lease release did not land: %+v", e.Value.Lease)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestService(t)
			tc.seed(t, s)
			if err := tc.mutate(s); err != nil {
				t.Fatalf("mutate: %v", err)
			}
			e, ok := loadEntry(t, s, "u1")
			if !ok || !e.Present() {
				t.Fatalf("entry not present after mutation: ok=%v added=%d removed=%d", ok, e.Added, e.Removed)
			}
			if e.Added <= skewAhead {
				t.Errorf("add stamp not forced past skewed stamp: %d <= %d", e.Added, skewAhead)
			}
			if _, ok := stampSig(t, s, "u1"); !ok {
				t.Error("forced mutation did not touch the stamp")
			}
			tc.check(t, e)
		})
	}
}

// TestScanPublishForcesPastSkewedAdd is the ScanPublish arm of the skew coverage:
// it mutates a registry in place (no stamp touch, by contract), so it lives apart
// from the mutate-based table. Removing forceStamp from the present branch fails
// the fresher-chain case.
func TestScanPublishForcesPastSkewedAdd(t *testing.T) {
	ctx := context.Background()

	t.Run("fresher chain forces past a skewed add", func(t *testing.T) {
		s, _ := newTestService(t)
		s.Locals = func(context.Context) ([]LocalAccount, error) {
			return []LocalAccount{{
				UUID:  "u1",
				Email: "local@x",
				Label: "local",
				Chain: ChainStamp{ExpiresAt: 2000, Hash: "local", Holder: "hostB"},
			}}, nil
		}
		reg := cregistry.New[AccountValue]()
		reg.Add("u1", acctVal("u1", "reg@x", "reg", "hostA", 1000), skewAhead)

		changed, err := s.ScanPublish(ctx, reg)
		if err != nil {
			t.Fatalf("ScanPublish: %v", err)
		}
		if !changed {
			t.Fatal("fresher-chain scan under skew reported no change")
		}
		e := reg["u1"]
		if e.Added <= skewAhead {
			t.Errorf("add stamp not forced past skewed add: %d <= %d", e.Added, skewAhead)
		}
		if e.Value.Chain.ExpiresAt != 2000 || e.Value.Chain.Hash != "local" {
			t.Errorf("fresher chain not adopted: %+v", e.Value.Chain)
		}
	})

	t.Run("new local add is present with its oauthAccount", func(t *testing.T) {
		s, _ := newTestService(t)
		oauth := json.RawMessage(`{"accountUuid":"u9"}`)
		s.Locals = func(context.Context) ([]LocalAccount, error) {
			return []LocalAccount{{
				UUID:         "u9",
				Email:        "e9@x",
				Label:        "nine",
				OAuthAccount: oauth,
				Chain:        ChainStamp{ExpiresAt: 3000, Hash: "h9", Holder: "hostA"},
			}}, nil
		}
		reg := cregistry.New[AccountValue]()
		changed, err := s.ScanPublish(ctx, reg)
		if err != nil {
			t.Fatalf("ScanPublish: %v", err)
		}
		if !changed {
			t.Fatal("new-local scan reported no change")
		}
		e := reg["u9"]
		if !e.Present() {
			t.Fatal("new local not present after scan")
		}
		if string(e.Value.OAuthAccount) != string(oauth) {
			t.Errorf("new local oauthAccount = %s, want %s", e.Value.OAuthAccount, oauth)
		}
	})
}

// TestScanPublishBackfillsOAuthAccount pins the fill-if-empty oauthAccount
// backfill (finding 4): a scan fills a present entry that has none, treats the
// round-tripped null as empty, and never clobbers a value a peer already set.
func TestScanPublishBackfillsOAuthAccount(t *testing.T) {
	ctx := context.Background()
	local := LocalAccount{
		UUID:         "u1",
		Email:        "local@x",
		Label:        "local",
		OAuthAccount: json.RawMessage(`{"local":true}`),
		Chain:        ChainStamp{ExpiresAt: 1000, Hash: "local", Holder: "hostB"}, // equal chain: only oauth may move
	}
	withLocal := func(s *Service) {
		s.Locals = func(context.Context) ([]LocalAccount, error) { return []LocalAccount{local}, nil }
	}

	t.Run("empty oauthAccount is backfilled without moving the chain", func(t *testing.T) {
		s, _ := newTestService(t)
		withLocal(s)
		reg := cregistry.New[AccountValue]()
		reg.Add("u1", AccountValue{
			UUID:  "u1",
			Email: "reg@x",
			Label: "reg",
			Chain: ChainStamp{ExpiresAt: 1000, Hash: "reg", Holder: "hostA"},
		}, cregistry.Micros(1000))

		changed, err := s.ScanPublish(ctx, reg)
		if err != nil {
			t.Fatalf("ScanPublish: %v", err)
		}
		if !changed {
			t.Fatal("backfill of an empty oauthAccount reported no change")
		}
		e := reg["u1"]
		if string(e.Value.OAuthAccount) != `{"local":true}` {
			t.Errorf("oauthAccount not backfilled: %s", e.Value.OAuthAccount)
		}
		if e.Value.Chain.Hash != "reg" || e.Value.Label != "reg" {
			t.Errorf("backfill disturbed registry fields: %+v", e.Value)
		}
	})

	t.Run("null oauthAccount is treated as empty and backfilled", func(t *testing.T) {
		s, _ := newTestService(t)
		withLocal(s)
		reg := cregistry.New[AccountValue]()
		reg.Add("u1", AccountValue{
			UUID:         "u1",
			Email:        "reg@x",
			Label:        "reg",
			OAuthAccount: json.RawMessage(`null`),
			Chain:        ChainStamp{ExpiresAt: 1000, Hash: "reg", Holder: "hostA"},
		}, cregistry.Micros(1000))

		changed, err := s.ScanPublish(ctx, reg)
		if err != nil {
			t.Fatalf("ScanPublish: %v", err)
		}
		if !changed {
			t.Fatal("null oauthAccount not treated as empty")
		}
		if string(reg["u1"].Value.OAuthAccount) != `{"local":true}` {
			t.Errorf("null oauthAccount not backfilled: %s", reg["u1"].Value.OAuthAccount)
		}
	})

	t.Run("non-empty peer-set oauthAccount is never overwritten", func(t *testing.T) {
		s, _ := newTestService(t)
		withLocal(s)
		reg := cregistry.New[AccountValue]()
		reg.Add("u1", AccountValue{
			UUID:         "u1",
			Email:        "reg@x",
			Label:        "reg",
			OAuthAccount: json.RawMessage(`{"peer":true}`),
			Chain:        ChainStamp{ExpiresAt: 1000, Hash: "reg", Holder: "hostA"},
		}, cregistry.Micros(1000))

		changed, err := s.ScanPublish(ctx, reg)
		if err != nil {
			t.Fatalf("ScanPublish: %v", err)
		}
		if changed {
			t.Error("scan overwrote a peer-set oauthAccount (reported a change)")
		}
		if string(reg["u1"].Value.OAuthAccount) != `{"peer":true}` {
			t.Errorf("peer oauthAccount was clobbered: %s", reg["u1"].Value.OAuthAccount)
		}
	})
}

// TestRecordLabelCrossHostLWW proves the forced local stamp does not disturb
// cross-host convergence: two hosts relabel a shared account from a common
// ancestor, and the strictly-later write wins the merge regardless of merge
// order (the LWW max-join is commutative).
func TestRecordLabelCrossHostLWW(t *testing.T) {
	ctx := context.Background()

	relabel := func(t *testing.T, label string, clockUnix int64) Registry {
		t.Helper()
		s, clk := newTestService(t)
		// Identical common ancestor on both hosts.
		base := cregistry.New[AccountValue]()
		base.Add("u1", acctVal("u1", "e", "base", "hostX", 1000), cregistry.Micros(1_000_000))
		if err := s.Registry.Save(base); err != nil {
			t.Fatalf("seed base: %v", err)
		}
		clk.set(time.Unix(clockUnix, 0))
		if err := s.RecordLabel(ctx, "u1", label); err != nil {
			t.Fatalf("RecordLabel: %v", err)
		}
		reg, err := s.Registry.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		return reg
	}

	early := relabel(t, "early", 1_700_000_100)
	late := relabel(t, "late", 1_700_000_200) // strictly later wall clock → higher add stamp

	if got := cregistry.Merge(early, late)["u1"].Value.Label; got != "late" {
		t.Errorf("Merge(early, late) label = %q, want late", got)
	}
	if got := cregistry.Merge(late, early)["u1"].Value.Label; got != "late" {
		t.Errorf("Merge(late, early) label = %q, want late", got)
	}
}
