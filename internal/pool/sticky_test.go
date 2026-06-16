package pool

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

func openTestManager(t *testing.T) *Manager {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &Manager{Store: st, LockDir: t.TempDir()}
}

// seedSessionFor writes one tracked session for cwd on the given account.
// Live fixtures use pid 0 so the select path's dead-session sweep (which only
// knows claude pids) can never reap them.
func seedSessionFor(t *testing.T, m *Manager, accountID int, cwd string, started time.Time, ended *time.Time) {
	t.Helper()
	id, err := m.Store.OpenSession(accountID, 0, "dir", cwd, started)
	if err != nil {
		t.Fatal(err)
	}
	if ended != nil {
		if err := m.Store.CloseSession(id, *ended); err != nil {
			t.Fatal(err)
		}
	}
}

// seedSession seeds on account 2, the pinned account in most fixtures.
func seedSession(t *testing.T, m *Manager, cwd string, started time.Time, ended *time.Time) {
	t.Helper()
	seedSessionFor(t, m, 2, cwd, started, ended)
}

func TestStickyPick(t *testing.T) {
	// Truncate to seconds: SelectedAt round-trips through the store as Unix
	// seconds, and the TTL-boundary case needs an exact comparison.
	now := time.Now().Truncate(time.Second)
	ts := func(d time.Duration) *time.Time { t := now.Add(d); return &t }
	// acct-1 ranks first; acct-2 is the sticky target the record points at.
	healthy := []score.Result{
		{AccountID: 1, Score: 80, Available: true, Components: score.Components{RawRemaining5h: 90}},
		{AccountID: 2, Score: 50, Available: true, Components: score.Components{RawRemaining5h: 50}},
	}
	pinUnusable := []score.Result{
		{AccountID: 1, Score: 80, Available: true, Components: score.Components{RawRemaining5h: 90}},
		{AccountID: 2, Score: 5, Available: true, Components: score.Components{RawRemaining5h: score.StickyMinRemaining5h - 1}},
	}
	type session struct {
		account int // 0 = the pinned account (2)
		started time.Time
		ended   *time.Time // nil = still live
	}
	cases := []struct {
		name        string
		cwd         string // cwd passed to StickyPick ("" disables)
		record      bool   // whether to write a sticky row for /proj -> recordID
		manual      bool
		recordID    int
		recordedAt  time.Time
		sessions    []session
		ranked      []score.Result
		wantOutcome StickyOutcome
		wantID      int
		wantRowGone bool // row 4 hygiene: expired records are deleted on read
	}{
		// No-data fallback (row 10): selected_at freshness alone binds.
		{name: "fresh select binds with no sessions", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-30 * time.Minute), ranked: healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "exactly at TTL still binds", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-StickyTTL), ranked: healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "expired with no sessions misses and is dropped", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-StickyTTL - time.Minute), ranked: healthy, wantOutcome: StickyMiss, wantRowGone: true},

		// Activity rules: live sessions hold, warm ends bind, cold ends expire.
		{name: "live-only session holds", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-10 * time.Minute),
			sessions:   []session{{started: now.Add(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyHold, wantID: 2},
		{name: "long session keeps stale pin alive", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour), // selected long ago, session still running
			sessions:   []session{{started: now.Add(-3 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyHold, wantID: 2},
		{name: "warm ended session binds despite stale select", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour), // the headline fix: >1h session, ended 10m ago
			sessions:   []session{{started: now.Add(-3 * time.Hour), ended: ts(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "warm end binds even with another session live", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions: []session{
				{started: now.Add(-2 * time.Hour), ended: ts(-10 * time.Minute)},
				{started: now.Add(-30 * time.Minute)}, // still live
			},
			ranked: healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "warm window keys off the later end", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-5 * time.Hour),
			sessions: []session{
				{started: now.Add(-5 * time.Hour), ended: ts(-4 * time.Hour)},
				{started: now.Add(-3 * time.Hour), ended: ts(-30 * time.Minute)},
			},
			ranked: healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "cold ended sessions expire the pin", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{started: now.Add(-3 * time.Hour), ended: ts(-2 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyMiss, wantRowGone: true},
		{name: "cold history but fresh select binds", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-5 * time.Minute), // e.g. a pid-0 select after old sessions
			sessions:   []session{{started: now.Add(-3 * time.Hour), ended: ts(-2 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyBind, wantID: 2},

		// Activity is account-scoped: the cache a pin protects belongs to the
		// pinned account, so another account's sessions neither warm nor hold.
		{name: "other account's warm end cannot warm the pin", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{account: 1, started: now.Add(-3 * time.Hour), ended: ts(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyMiss, wantRowGone: true},
		{name: "other account's live session cannot hold the pin", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{account: 1, started: now.Add(-3 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyMiss, wantRowGone: true},

		// Manual pins: no warm-cache requirement, no live-session skip.
		{name: "manual binds with no sessions", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now.Add(-30 * time.Minute), ranked: healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "manual binds through a live session", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now.Add(-10 * time.Minute),
			sessions:   []session{{started: now.Add(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyBind, wantID: 2},
		{name: "manual expires like auto and is dropped", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now.Add(-2 * time.Hour), ranked: healthy, wantOutcome: StickyMiss, wantRowGone: true},
		{name: "manual to unusable account is held", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now, ranked: pinUnusable, wantOutcome: StickyHoldManual, wantID: 2},

		// Unusable auto pins are abandoned (2026-06-10 incident behavior).
		{name: "near-full auto pin abandoned", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now, ranked: pinUnusable, wantOutcome: StickyMiss},
		{name: "rate-limited auto pin abandoned", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now, ranked: []score.Result{
				{AccountID: 1, Score: 80, Available: true, Components: score.Components{RawRemaining5h: 90}},
				{AccountID: 2, Score: -50, Available: false, Components: score.Components{RawRemaining5h: 50}},
			}, wantOutcome: StickyMiss},
		// The 2026-06-10 incident: the pinned account is exhausted but its
		// imminent reset keeps eff5 high — the pin must still be abandoned.
		{name: "exhausted auto pin abandoned", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now, ranked: []score.Result{
				{AccountID: 1, Score: 80, Available: true, Components: score.Components{RawRemaining5h: 60}},
				{AccountID: 2, Score: 65, Available: false, Exhausted: true, Components: score.Components{Eff5: 93, RawRemaining5h: 0}},
			}, wantOutcome: StickyMiss},

		// Structural misses.
		{name: "account deleted", cwd: "/proj", record: true, recordID: 9,
			recordedAt: now, ranked: healthy, wantOutcome: StickyMiss},
		{name: "empty cwd disabled", cwd: "", record: true, recordID: 2,
			recordedAt: now, ranked: healthy, wantOutcome: StickyMiss},
		{name: "no record", cwd: "/other", record: true, recordID: 2,
			recordedAt: now, ranked: healthy, wantOutcome: StickyMiss},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := openTestManager(t)
			if tc.record {
				var err error
				if tc.manual {
					m.Store.UpsertAccount(store.Account{ID: tc.recordID, ConfigDir: "dir", KeychainService: "s", KeychainAccount: "u"})
					err = m.Store.PinManual("/proj", tc.recordID, tc.recordedAt)
				} else {
					err = m.Store.UpsertSticky("/proj", tc.recordID, tc.recordedAt)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, se := range tc.sessions {
				acct := se.account
				if acct == 0 {
					acct = 2
				}
				seedSessionFor(t, m, acct, "/proj", se.started, se.ended)
			}
			r, outcome := m.StickyPick(tc.cwd, tc.ranked, now)
			if outcome != tc.wantOutcome {
				t.Fatalf("outcome = %v, want %v", outcome, tc.wantOutcome)
			}
			if outcome != StickyMiss && r.AccountID != tc.wantID {
				t.Fatalf("picked acct %d, want %d", r.AccountID, tc.wantID)
			}
			if _, ok, _ := m.Store.GetSticky("/proj"); tc.record && tc.cwd == "/proj" && ok == tc.wantRowGone {
				t.Fatalf("row present=%v, wantGone=%v", ok, tc.wantRowGone)
			}
		})
	}
}

func TestRecordStickySlidingTTL(t *testing.T) {
	m := openTestManager(t)
	t0 := time.Now()
	ranked := []score.Result{{AccountID: 2, Score: 50, Available: true, Components: score.Components{RawRemaining5h: 50}}}

	if err := m.RecordSticky("/proj", 2, t0); err != nil {
		t.Fatal(err)
	}
	if _, o := m.StickyPick("/proj", ranked, t0.Add(50*time.Minute)); o != StickyBind {
		t.Fatalf("expected bind at t0+50m, got %v", o)
	}
	// A select at t0+50m refreshes the clock, keeping t0+100m sticky.
	if err := m.RecordSticky("/proj", 2, t0.Add(50*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, o := m.StickyPick("/proj", ranked, t0.Add(100*time.Minute)); o != StickyBind {
		t.Fatalf("expected bind at t0+100m after sliding refresh, got %v", o)
	}
	// Control: without the refresh, t0+100m is past the 1h TTL.
	if err := m.RecordSticky("/control", 2, t0); err != nil {
		t.Fatal(err)
	}
	if _, o := m.StickyPick("/control", ranked, t0.Add(100*time.Minute)); o != StickyMiss {
		t.Fatalf("expected miss at t0+100m without refresh, got %v", o)
	}

	// Empty cwd is a no-op, never an error.
	if err := m.RecordSticky("", 2, t0); err != nil {
		t.Fatalf("empty cwd: %v", err)
	}
}

func TestPinAPI(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	t.Run("pin validates account and cwd", func(t *testing.T) {
		m := openTestManager(t)
		if err := m.PinManual("", 1, now); err == nil {
			t.Fatal("empty cwd must fail")
		}
		if err := m.PinManual("/proj", 9, now); err == nil {
			t.Fatal("unknown account must fail")
		}
		m.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
		if err := m.PinManual("/proj", 1, now); err != nil {
			t.Fatal(err)
		}
		st, ok, _ := m.Store.GetSticky("/proj")
		if !ok || !st.Manual || st.AccountID != 1 {
			t.Fatalf("pin not recorded: %+v", st)
		}
	})

	t.Run("toggle pins, repins, unpins", func(t *testing.T) {
		m := openTestManager(t)
		m.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
		m.Store.UpsertAccount(store.Account{ID: 2, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

		pinned, err := m.TogglePin("/proj", 1, now)
		if err != nil || !pinned {
			t.Fatalf("first toggle: pinned=%v err=%v", pinned, err)
		}
		// Toggling a different account repins rather than unpinning.
		pinned, err = m.TogglePin("/proj", 2, now)
		if err != nil || !pinned {
			t.Fatalf("repin toggle: pinned=%v err=%v", pinned, err)
		}
		if st, _, _ := m.Store.GetSticky("/proj"); st.AccountID != 2 || !st.Manual {
			t.Fatalf("repin: %+v", st)
		}
		pinned, err = m.TogglePin("/proj", 2, now)
		if err != nil || pinned {
			t.Fatalf("unpin toggle: pinned=%v err=%v", pinned, err)
		}
		if _, ok, _ := m.Store.GetSticky("/proj"); ok {
			t.Fatal("pin should be gone")
		}
		// An AUTO pin to the toggled account also unpins (release the dir).
		m.Store.UpsertSticky("/proj", 2, now)
		pinned, err = m.TogglePin("/proj", 2, now)
		if err != nil || pinned {
			t.Fatalf("auto unpin toggle: pinned=%v err=%v", pinned, err)
		}
		// An EXPIRED unpruned pin counts as absent: the press must pin, not
		// silently release a pin the selector already misses.
		m.Store.PinManual("/proj", 2, now.Add(-2*time.Hour))
		pinned, err = m.TogglePin("/proj", 2, now)
		if err != nil || !pinned {
			t.Fatalf("toggle on an expired pin must pin: pinned=%v err=%v", pinned, err)
		}
		if st, ok, _ := m.Store.GetSticky("/proj"); !ok || !st.Manual || !st.SelectedAt.Equal(now) {
			t.Fatalf("expired pin not replaced by a fresh one: %+v ok=%v", st, ok)
		}
	})

	t.Run("view reflects state and hides expired pins", func(t *testing.T) {
		m := openTestManager(t)
		m.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})

		if _, ok, err := m.PinView("/proj", now); ok || err != nil {
			t.Fatalf("no pin: ok=%v err=%v", ok, err)
		}
		if err := m.PinManual("/proj", 1, now.Add(-30*time.Minute)); err != nil {
			t.Fatal(err)
		}
		pv, ok, err := m.PinView("/proj", now)
		if err != nil || !ok {
			t.Fatalf("view: ok=%v err=%v", ok, err)
		}
		if pv.AccountID != 1 || !pv.Manual || !pv.Binding || pv.Live {
			t.Fatalf("view = %+v", pv)
		}
		if want := now.Add(30 * time.Minute); !pv.ExpiresAt.Equal(want) {
			t.Fatalf("expires = %v, want %v", pv.ExpiresAt, want)
		}

		// A live session on the pinned account suppresses the deadline; an
		// auto pin under a live session reads as non-binding.
		seedSessionFor(t, m, 1, "/proj", now.Add(-10*time.Minute), nil)
		pv, _, _ = m.PinView("/proj", now)
		if !pv.Live || !pv.ExpiresAt.IsZero() {
			t.Fatalf("live view = %+v", pv)
		}
		m.Store.UpsertSticky("/auto", 1, now)
		seedSessionFor(t, m, 1, "/auto", now.Add(-10*time.Minute), nil)
		pv, _, _ = m.PinView("/auto", now)
		if pv.Manual || pv.Binding {
			t.Fatalf("auto live view should not promise binding: %+v", pv)
		}

		// Expired pins are invisible.
		m.Store.PinManual("/stale", 1, now.Add(-2*time.Hour))
		if _, ok, _ := m.PinView("/stale", now); ok {
			t.Fatal("expired pin must not render")
		}
	})
}

// TestClassifyDegradesOnStoreError: an activity-read failure must degrade to
// selected_at-only freshness (the pre-activity behavior), never escape — the
// select path treats stickiness as best-effort.
func TestClassifyDegradesOnStoreError(t *testing.T) {
	m := openTestManager(t)
	m.Store.Close() // force GetCwdActivity to fail
	now := time.Now()
	ps := m.classify(store.Sticky{Cwd: "/proj", AccountID: 1, SelectedAt: now.Add(-10 * time.Minute)}, now)
	if ps.live || ps.warm {
		t.Fatalf("degraded read must carry no session signals: %+v", ps)
	}
	if !ps.alive(now) {
		t.Fatal("a fresh selected_at must keep the pin alive on a degraded read")
	}
	stale := m.classify(store.Sticky{Cwd: "/proj", AccountID: 1, SelectedAt: now.Add(-2 * time.Hour)}, now)
	if stale.alive(now) {
		t.Fatal("a stale selected_at must read expired on a degraded read")
	}
}

// TestSelectSweepReconcilesDeadSessions covers the select-path self-heal: with
// no daemon running, a select must reap session rows whose pids are gone —
// including when the scan finds ZERO claude processes (a nil slice from a
// successful scan), the exact state after the last session exits.
func TestSelectSweepReconcilesDeadSessions(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	setup := func(t *testing.T, scan func(context.Context) ([]procscan.Session, error)) *Manager {
		t.Helper()
		old := scanSessions
		scanSessions = scan
		t.Cleanup(func() { scanSessions = old })
		m := openTestManager(t)
		dir := t.TempDir()
		for id, util := range map[int]float64{1: 10, 2: 50} {
			if err := m.Store.UpsertAccount(store.Account{
				ID: id, ConfigDir: filepath.Join(dir, "acct", string(rune('a'+id))),
				KeychainService: "svc", KeychainAccount: "u",
			}); err != nil {
				t.Fatal(err)
			}
			if err := m.Store.InsertUsageSample(store.UsageSample{AccountID: id, TS: now, Util5h: util, Util7d: util}); err != nil {
				t.Fatal(err)
			}
		}
		// Pin /proj -> acct-2 long ago; its session (a dead pid no claude can
		// own) was last seen alive 10 minutes ago.
		if err := m.Store.UpsertSticky("/proj", 2, now.Add(-3*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Store.OpenSession(2, 4000000, "dir", "/proj", now.Add(-3*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Store.CloseDeadSessions(map[int]bool{4000000: true}, now.Add(-10*time.Minute)); err != nil {
			t.Fatal(err)
		}
		return m
	}

	t.Run("zero-claude scan reaps and the pin binds warm", func(t *testing.T) {
		m := setup(t, func(context.Context) ([]procscan.Session, error) { return nil, nil })
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj"})
		if err != nil {
			t.Fatal(err)
		}
		if sr.Best.ID != 2 || !sr.Sticky {
			t.Fatalf("reaped warm end must bind the pin: got acct %d sticky=%v", sr.Best.ID, sr.Sticky)
		}
		if live, _ := m.Store.ListActiveSessions(); len(live) != 0 {
			t.Fatalf("dead row must be reaped: %+v", live)
		}
	})

	t.Run("scan failure skips the sweep and the pin holds", func(t *testing.T) {
		m := setup(t, func(context.Context) ([]procscan.Session, error) { return nil, errors.New("ps exploded") })
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj"})
		if err != nil {
			t.Fatal(err)
		}
		// Without a trustworthy scan the row must stay open (no fabricated
		// end): the still-live pin holds and the free ranking wins.
		if sr.Best.ID != 1 || sr.Sticky {
			t.Fatalf("failed scan must not reap: got acct %d sticky=%v", sr.Best.ID, sr.Sticky)
		}
		if live, _ := m.Store.ListActiveSessions(); len(live) != 1 {
			t.Fatalf("row must survive a failed scan: %+v", live)
		}
	})

	t.Run("fresh dead row survives the reap grace", func(t *testing.T) {
		m := setup(t, func(context.Context) ([]procscan.Session, error) { return nil, nil })
		// A just-marked checkout (ccp run pre-exec) looks dead to a claude-only
		// scan; the grace keeps it alive.
		if _, err := m.Store.OpenSession(1, 4000001, "dir", "/other", now); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Select(ctx, SelectOptions{Cwd: "/proj"}); err != nil {
			t.Fatal(err)
		}
		live, _ := m.Store.ListActiveSessions()
		if len(live) != 1 || live[0].PID != 4000001 {
			t.Fatalf("graced row must survive: %+v", live)
		}
	})
}

// TestSelectHonorsSticky pins the slot-in location (after Rank, before Pick):
// a fresh sticky record overrides the emptier-account ranking, an expired one
// does not, and the winner is always (re)recorded.
func TestSelectHonorsSticky(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	setup := func(t *testing.T) *Manager {
		m := openTestManager(t)
		dir := t.TempDir()
		for id, util := range map[int]float64{1: 10, 2: 50} {
			if err := m.Store.UpsertAccount(store.Account{
				ID: id, ConfigDir: filepath.Join(dir, "acct", string(rune('a'+id))),
				KeychainService: "svc", KeychainAccount: "u",
			}); err != nil {
				t.Fatal(err)
			}
			if err := m.Store.InsertUsageSample(store.UsageSample{AccountID: id, TS: now, Util5h: util, Util7d: util}); err != nil {
				t.Fatal(err)
			}
		}
		return m
	}

	t.Run("fresh sticky overrides ranking", func(t *testing.T) {
		m := setup(t)
		if err := m.Store.UpsertSticky("/proj", 2, now); err != nil {
			t.Fatal(err)
		}
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj"})
		if err != nil {
			t.Fatal(err)
		}
		if sr.Best.ID != 2 || !sr.Sticky {
			t.Fatalf("got acct %d sticky=%v, want sticky acct 2", sr.Best.ID, sr.Sticky)
		}
	})

	t.Run("expired sticky falls through and is overwritten", func(t *testing.T) {
		m := setup(t)
		if err := m.Store.UpsertSticky("/proj", 2, now.Add(-2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj"})
		if err != nil {
			t.Fatal(err)
		}
		if sr.Best.ID != 1 || sr.Sticky {
			t.Fatalf("got acct %d sticky=%v, want non-sticky acct 1 (emptier)", sr.Best.ID, sr.Sticky)
		}
		st, ok, err := m.Store.GetSticky("/proj")
		if err != nil || !ok || st.AccountID != 1 {
			t.Fatalf("winner not recorded: %+v ok=%v err=%v", st, ok, err)
		}
	})

	t.Run("held pin is not repointed", func(t *testing.T) {
		m := setup(t)
		// Live-only session in /proj: the auto pin to acct-2 must hold — the
		// free ranking picks acct-1 without overwriting the pin.
		if err := m.Store.UpsertSticky("/proj", 2, now); err != nil {
			t.Fatal(err)
		}
		seedSession(t, m, "/proj", now.Add(-10*time.Minute), nil)
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj"})
		if err != nil {
			t.Fatal(err)
		}
		if sr.Best.ID != 1 || sr.Sticky {
			t.Fatalf("got acct %d sticky=%v, want free non-sticky acct 1", sr.Best.ID, sr.Sticky)
		}
		if sr.PinHeldAccount != nil {
			t.Fatalf("auto hold must not flag a held manual pin: %+v", sr.PinHeldAccount)
		}
		st, ok, _ := m.Store.GetSticky("/proj")
		if !ok || st.AccountID != 2 {
			t.Fatalf("held pin was repointed: %+v ok=%v", st, ok)
		}
	})

	t.Run("held manual pin surfaces and survives", func(t *testing.T) {
		m := setup(t)
		// Pin /proj to acct-2, then exhaust it: manual hold, free pick acct-1.
		if err := m.Store.PinManual("/proj", 2, now); err != nil {
			t.Fatal(err)
		}
		if err := m.Store.InsertUsageSample(store.UsageSample{
			AccountID: 2, TS: now.Add(time.Second), Util5h: 100, Util7d: 50,
			Resets5h: now.Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj"})
		if err != nil {
			t.Fatal(err)
		}
		if sr.Best.ID != 1 || sr.Sticky {
			t.Fatalf("got acct %d sticky=%v, want free acct 1", sr.Best.ID, sr.Sticky)
		}
		if sr.PinHeldAccount == nil || *sr.PinHeldAccount != 2 {
			t.Fatalf("held manual pin not surfaced: %+v", sr.PinHeldAccount)
		}
		st, ok, _ := m.Store.GetSticky("/proj")
		if !ok || st.AccountID != 2 || !st.Manual {
			t.Fatalf("manual pin lost on hold: %+v ok=%v", st, ok)
		}
	})

	t.Run("marking opens a session row with cwd", func(t *testing.T) {
		m := setup(t)
		sr, err := m.Select(ctx, SelectOptions{Cwd: "/proj", PID: 4242})
		if err != nil {
			t.Fatal(err)
		}
		live, err := m.Store.ListActiveSessions()
		if err != nil || len(live) != 1 {
			t.Fatalf("sessions = %+v err=%v", live, err)
		}
		if live[0].PID != 4242 || live[0].Cwd != "/proj" || live[0].AccountID != sr.Best.ID {
			t.Fatalf("session row = %+v", live[0])
		}
	})

	t.Run("no cwd records nothing", func(t *testing.T) {
		m := setup(t)
		if _, err := m.Select(ctx, SelectOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := m.Store.GetSticky(""); ok {
			t.Fatal("empty cwd must not be recorded")
		}
	})
}

// TestSelectAllExhaustedFallback: when every account's 5h window is pegged with
// a pending reset, Select must still return the least-bad one — flagged so the
// caller warns — rather than erroring; only all-rate-limited errors.
func TestSelectAllExhaustedFallback(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	m := openTestManager(t)
	dir := t.TempDir()
	for id, util7 := range map[int]float64{1: 90, 2: 10} {
		if err := m.Store.UpsertAccount(store.Account{
			ID: id, ConfigDir: filepath.Join(dir, "acct", string(rune('a'+id))),
			KeychainService: "svc", KeychainAccount: "u",
		}); err != nil {
			t.Fatal(err)
		}
		if err := m.Store.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Util7d: util7,
			Resets5h: now.Add(20 * time.Minute), ExtraEnabled: id == 2,
		}); err != nil {
			t.Fatal(err)
		}
	}

	sr, err := m.Select(ctx, SelectOptions{})
	if err != nil {
		t.Fatalf("all-exhausted select must fall back, got %v", err)
	}
	if !sr.ExhaustedFallback || !sr.Result.Exhausted {
		t.Fatalf("fallback pick must be flagged: %+v", sr)
	}
	if sr.Best.ID != 2 {
		t.Fatalf("expected least-bad acct 2 (emptier 7d), got %d", sr.Best.ID)
	}
	if !sr.ExtraEnabled {
		t.Fatal("pick's extra-usage flag must surface for the warning")
	}

	// Negative control: rate-limited accounts cannot serve at all.
	m2 := openTestManager(t)
	if err := m2.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: filepath.Join(dir, "rl"), KeychainService: "svc", KeychainAccount: "u"}); err != nil {
		t.Fatal(err)
	}
	if err := m2.Store.InsertUsageSample(store.UsageSample{AccountID: 1, TS: now, Util5h: 100, Resets5h: now.Add(20 * time.Minute), RateLimited: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.Select(ctx, SelectOptions{}); !errors.Is(err, ErrNoneAvailable) {
		t.Fatalf("all-rate-limited must error ErrNoneAvailable, got %v", err)
	}
}
