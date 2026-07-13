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

func acctVal(uuid, email, label, origin string, expiresAt int64) AccountValue {
	return AccountValue{
		UUID:         uuid,
		Email:        email,
		Label:        label,
		OAuthAccount: json.RawMessage(`{"accountUuid":"` + uuid + `"}`),
		Chain:        ChainStamp{Origin: origin, ExpiresAt: expiresAt, Hash: "h-" + uuid, RotatedAt: expiresAt - 100},
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
	b, err := os.ReadFile(filepath.Join(s.StampDir, uuid, "stamp")) //nolint:gosec // G304: s.StampDir is a test-owned temp dir, not external input
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
		if e.Value.Chain.Origin != "hostA" || e.Value.Label != "work" {
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
		if e.Value.Label != "l2" || e.Value.Chain.Origin != "hostB" {
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

	t.Run("origin-less non-zero chain fails loud", func(t *testing.T) {
		s, _ := newTestService(t)
		v := acctVal("u1", "e@x.com", "work", "", 1000)
		if err := s.PublishAccount(ctx, v); err == nil {
			t.Fatal("PublishAccount with a non-zero chain naming no origin returned nil error")
		}
		if _, ok := loadEntry(t, s, "u1"); ok {
			t.Fatal("rejected publish must leave no registry entry")
		}
	})

	t.Run("zero chain (identity only) publishes", func(t *testing.T) {
		s, _ := newTestService(t)
		v := AccountValue{UUID: "u1", Email: "e@x.com", Label: "work"}
		if err := s.PublishAccount(ctx, v); err != nil {
			t.Fatalf("PublishAccount with a zero chain: %v", err)
		}
		e, ok := loadEntry(t, s, "u1")
		if !ok || !e.Present() {
			t.Fatalf("identity-only publish not present: ok=%v entry=%+v", ok, e)
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
		{"equal expiry", ChainStamp{Origin: "hostB", ExpiresAt: 1000, Hash: "other"}},
		{"staler expiry", ChainStamp{Origin: "hostB", ExpiresAt: 999, Hash: "other"}},
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
		fresh := ChainStamp{Origin: "hostB", ExpiresAt: 2000, Hash: "fresh", RotatedAt: 1900}
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

// TestNoteCredWriteSkewedChildNeverLands pins the strictly-fresher-only rule:
// an origin clock rollback can stamp a rotation child EARLIER than its parent,
// and that child never lands — benign, the registry keeps the still-valid
// parent until it expires and the next rotation overtakes it.
func TestNoteCredWriteSkewedChildNeverLands(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestService(t)
	v := acctVal("u1", "e", "l", "hostA", 2000)
	v.Chain.Hash = "H2"
	if err := s.PublishAccount(ctx, v); err != nil {
		t.Fatalf("publish: %v", err)
	}
	clk.advance(time.Second)

	child := ChainStamp{Origin: "hostA", ExpiresAt: 1500, Hash: "H3", RotatedAt: 1400}
	if err := s.NoteCredWrite(ctx, "u1", child); err != nil {
		t.Fatalf("NoteCredWrite: %v", err)
	}
	e, _ := loadEntry(t, s, "u1")
	if e.Value.Chain.Hash != "H2" {
		t.Fatalf("an earlier-expiring chain landed: %+v", e.Value.Chain)
	}
}

func TestScanPublishNewLocalAccount(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	s.Locals = func(context.Context) ([]LocalAccount, error) {
		return []LocalAccount{
			{UUID: "u1", Email: "e1@x", Label: "one", Chain: ChainStamp{Origin: "hostA", ExpiresAt: 1000, Hash: "h1"}},
			{UUID: "u2", Email: "e2@x", Label: "two", Chain: ChainStamp{Origin: "hostA", ExpiresAt: 2000, Hash: "h2"}},
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
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 1000, Hash: "reg"},
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
					Chain: ChainStamp{Origin: "hostB", ExpiresAt: tc.localExpiry, Hash: "local"},
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

// TestScanPublishSyncedOnlyNeverSeedsZeroChain pins the cold-start guard: a
// host holding only a synced copy (zero chain) creates no registry entry, so
// its fresh add stamp can never erase a peer origin's live chain in the merge.
func TestScanPublishSyncedOnlyNeverSeedsZeroChain(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	s.Locals = func(context.Context) ([]LocalAccount, error) {
		return []LocalAccount{{UUID: "u1", Email: "e", Label: "l", Chain: ChainStamp{}}}, nil
	}
	reg := cregistry.New[AccountValue]()

	changed, err := s.ScanPublish(ctx, reg)
	if err != nil {
		t.Fatalf("ScanPublish: %v", err)
	}
	if changed {
		t.Fatal("ScanPublish reported a change for a synced-only account")
	}
	if _, ok := reg["u1"]; ok {
		t.Fatalf("ScanPublish seeded an entry for an unowned account: %+v", reg["u1"])
	}

	// The hazard this closes: the peer origin's older-stamped live chain must
	// survive the merge, which a fresh zero-chain add would have erased.
	peer := cregistry.New[AccountValue]()
	peer.Add("u1", acctVal("u1", "e", "l", "hostA", 5000), cregistry.Micros(100))
	merged := cregistry.Merge(reg, peer)
	if got := merged["u1"].Value.Chain; got.Origin != "hostA" || got.ExpiresAt != 5000 {
		t.Fatalf("merged chain = %+v, want the peer origin's live chain intact", got)
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
			Chain: ChainStamp{Origin: "hostB", ExpiresAt: 9_000_000_000_000, Hash: "superfresh"},
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

// TestLocalMutationForcesPastSkewedAdd pins forceStamp: every mutate-based
// local write lands even when the entry's stamp is ahead of the clock —
// removing forceStamp from any one method regresses its case.
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
				return s.NoteCredWrite(ctx, "u1", ChainStamp{Origin: "hostA", ExpiresAt: 2000, Hash: "fresh", RotatedAt: 1900})
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
				if e.Value.Label != "new" || e.Value.Chain.Origin != "hostB" {
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

// TestRecordRemovalForcesPastSkewedAdd pins the removal arm of the skew
// coverage: an explicit remove lands even under a skew-ahead Added, where a
// bare wall-clock stamp would leave the entry Present.
func TestRecordRemovalForcesPastSkewedAdd(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestService(t)
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", acctVal("u1", "e", "l", "hostA", 1000), skewAhead)
	if err := s.Registry.Save(reg); err != nil {
		t.Fatalf("seed skewed entry: %v", err)
	}

	if err := s.RecordRemoval(ctx, "u1"); err != nil {
		t.Fatalf("RecordRemoval: %v", err)
	}

	e, ok := loadEntry(t, s, "u1")
	if !ok {
		t.Fatal("entry vanished from the registry")
	}
	if e.Present() {
		t.Fatalf("tombstone no-oped under skew: added=%d removed=%d", e.Added, e.Removed)
	}
	if e.Removed <= skewAhead {
		t.Errorf("removed stamp not forced past the skewed add: %d <= %d", e.Removed, skewAhead)
	}
	if _, ok := stampSig(t, s, "u1"); !ok {
		t.Error("landed removal did not touch the stamp")
	}
}

// TestAutomaticMutationNeverCancelsUnmergedTombstone pins bumpStamp: routine
// mutations on an entry whose unmerged removal was recorded earlier in real
// time never cancel that removal, in either merge order.
func TestAutomaticMutationNeverCancelsUnmergedTombstone(t *testing.T) {
	ctx := context.Background()
	const base = cregistry.Micros(1000)

	cases := map[string]func(t *testing.T, b *Service){
		"NoteCredWrite fresher chain": func(t *testing.T, b *Service) {
			if err := b.NoteCredWrite(ctx, "u1", ChainStamp{Origin: "hostB", ExpiresAt: 2000, Hash: "fresh", RotatedAt: 1900}); err != nil {
				t.Fatal(err)
			}
		},
		"ScanPublish fresher fold": func(t *testing.T, b *Service) {
			b.Locals = func(context.Context) ([]LocalAccount, error) {
				return []LocalAccount{{UUID: "u1", Chain: ChainStamp{Origin: "hostB", ExpiresAt: 2000, Hash: "fresh"}}}, nil
			}
			reg, err := b.Registry.Load()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.ScanPublish(ctx, reg); err != nil {
				t.Fatal(err)
			}
			if err := b.Registry.Save(reg); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			seed := acctVal("u1", "e", "l", "hostA", 1000)
			a, _ := newTestService(t) // the remover
			b, _ := newTestService(t) // the busy peer, tombstone not yet merged
			for _, s := range []*Service{a, b} {
				reg := cregistry.New[AccountValue]()
				reg.Add("u1", seed, base)
				if err := s.Registry.Save(reg); err != nil {
					t.Fatal(err)
				}
			}

			if err := a.RecordRemoval(ctx, "u1"); err != nil {
				t.Fatalf("RecordRemoval: %v", err)
			}
			mutate(t, b)

			// The mutation must have landed locally on B (bump beats the seed
			// stamp) — otherwise the merge assertion below passes vacuously.
			if e, ok := loadEntry(t, b, "u1"); !ok || !e.Present() || e.Added <= base {
				t.Fatalf("mutation did not land on B: ok=%v added=%d removed=%d", ok, e.Added, e.Removed)
			}

			load := func(s *Service) Registry {
				t.Helper()
				reg, err := s.Registry.Load()
				if err != nil {
					t.Fatal(err)
				}
				return reg
			}
			if e := cregistry.Merge(load(a), load(b))["u1"]; e.Present() {
				t.Errorf("B into A: removal cancelled: added=%d removed=%d", e.Added, e.Removed)
			}
			if e := cregistry.Merge(load(b), load(a))["u1"]; e.Present() {
				t.Errorf("A into B: removal cancelled: added=%d removed=%d", e.Added, e.Removed)
			}
		})
	}
}

// TestScanPublishForcesPastSkewedAdd pins forceStamp in ScanPublish, which
// mutates a registry in place (no stamp touch) and so lives apart from the
// mutate-based table.
func TestScanPublishForcesPastSkewedAdd(t *testing.T) {
	ctx := context.Background()

	t.Run("fresher chain forces past a skewed add", func(t *testing.T) {
		s, _ := newTestService(t)
		s.Locals = func(context.Context) ([]LocalAccount, error) {
			return []LocalAccount{{
				UUID:  "u1",
				Email: "local@x",
				Label: "local",
				Chain: ChainStamp{Origin: "hostB", ExpiresAt: 2000, Hash: "local"},
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
				Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 3000, Hash: "h9"},
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
		Chain:        ChainStamp{Origin: "hostB", ExpiresAt: 1000, Hash: "local"}, // equal chain: only oauth may move
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
			Chain: ChainStamp{Origin: "hostA", ExpiresAt: 1000, Hash: "reg"},
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
			Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 1000, Hash: "reg"},
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
			Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 1000, Hash: "reg"},
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

// TestRecordLabelCrossHostLWW pins that the forced local stamp preserves
// cross-host convergence: the strictly-later relabel wins the merge in either order.
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
