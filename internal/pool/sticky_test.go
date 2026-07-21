package pool

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

var poolTestToken atomic.Uint64

func nextPoolTestToken() string {
	return fmt.Sprintf("%032x", poolTestToken.Add(1))
}

func openTestManager(t *testing.T) *Manager {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &Manager{Store: st}
}

// seedSessionFor's live fixtures use pid 0 so the select-path dead-session sweep
// (claude pids only) can never reap them.
func seedSessionFor(t *testing.T, m *Manager, accountID int, cwd string, started time.Time, ended *time.Time) {
	t.Helper()
	if _, err := m.Store.GetAccount(accountID); errors.Is(err, store.ErrAccountNotFound) {
		if err := m.Store.UpsertAccount(store.Account{
			ID: accountID, ConfigDir: AccountDir(accountID),
			KeychainService: fmt.Sprintf("svc-%d", accountID), KeychainAccount: "user",
		}); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	pid := 900000 + accountID*1000 + int(started.UnixNano()%997)
	id := activatePoolTestSession(t, m, accountID, pid, cwd, started)
	if ended != nil {
		if err := m.Store.CloseSession(id, *ended); err != nil {
			t.Fatal(err)
		}
	}
}

func activatePoolTestSession(t *testing.T, m *Manager, accountID, pid int, cwd string, started time.Time) int64 {
	t.Helper()
	started = started.Truncate(time.Microsecond)
	a, err := m.Store.GetAccount(accountID)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Store.ActivateSelection(store.SelectionActivation{
		Token:     nextPoolTestToken(),
		AccountID: accountID, ExpectedInstanceID: a.InstanceID, ExpectedGeneration: a.Generation,
		Process:   store.ProcessIdentity{PID: pid, StartedAt: started},
		ConfigDir: AccountPresentationDir(accountID),
		Cwd:       cwd, At: started,
	}); err != nil {
		t.Fatal(err)
	}
	for _, session := range mustPoolSessions(t, m) {
		if session.PID == pid && session.ProcessStartedAt.Equal(started) && session.Cwd == cwd {
			return session.ID
		}
	}
	t.Fatal("activated session was not stored")
	return 0
}

func mustPoolSessions(t *testing.T, m *Manager) []store.Session {
	t.Helper()
	sessions, err := m.Store.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	return sessions
}

// seedSession seeds on account 2, the pinned account in most fixtures.
func seedSession(t *testing.T, m *Manager, cwd string, started time.Time, ended *time.Time) {
	t.Helper()
	seedSessionFor(t, m, 2, cwd, started, ended)
}

func TestStickyPick(t *testing.T) {
	// SelectedAt round-trips through the store as Unix seconds; the TTL-boundary
	// case needs the exact comparison.
	now := time.Now().Truncate(time.Second)
	ts := func(d time.Duration) *time.Time { t := now.Add(d); return &t }
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
		cwd         string // "" disables
		record      bool
		manual      bool
		recordID    int
		recordedAt  time.Time
		sessions    []session
		ranked      []score.Result
		wantOutcome StickyOutcome
		wantID      int
		wantRowGone bool // expired records are deleted on read
	}{
		{
			name: "fresh select binds with no sessions", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-30 * time.Minute), ranked: healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "exactly at TTL still binds", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-StickyTTL), ranked: healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "expired with no sessions misses and is dropped", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-StickyTTL - time.Minute), ranked: healthy, wantOutcome: StickyMiss, wantRowGone: true,
		},

		{
			name: "live-only session holds", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-10 * time.Minute),
			sessions:   []session{{started: now.Add(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyHold, wantID: 2,
		},
		{
			name: "long session keeps stale pin alive", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{started: now.Add(-3 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyHold, wantID: 2,
		},
		{
			name: "warm ended session binds despite stale select", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{started: now.Add(-3 * time.Hour), ended: ts(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "warm end binds even with another session live", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions: []session{
				{started: now.Add(-2 * time.Hour), ended: ts(-10 * time.Minute)},
				{started: now.Add(-30 * time.Minute)},
			},
			ranked: healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "warm window keys off the later end", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-5 * time.Hour),
			sessions: []session{
				{started: now.Add(-5 * time.Hour), ended: ts(-4 * time.Hour)},
				{started: now.Add(-3 * time.Hour), ended: ts(-30 * time.Minute)},
			},
			ranked: healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "cold ended sessions expire the pin", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{started: now.Add(-3 * time.Hour), ended: ts(-2 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyMiss, wantRowGone: true,
		},
		{
			name: "cold history but fresh select binds", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-5 * time.Minute),
			sessions:   []session{{started: now.Add(-3 * time.Hour), ended: ts(-2 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyBind, wantID: 2,
		},

		// Activity is account-scoped: a pin protects the pinned account's cache,
		// so another account's sessions neither warm nor hold.
		{
			name: "other account's warm end cannot warm the pin", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{account: 1, started: now.Add(-3 * time.Hour), ended: ts(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyMiss, wantRowGone: true,
		},
		{
			name: "other account's live session cannot hold the pin", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now.Add(-3 * time.Hour),
			sessions:   []session{{account: 1, started: now.Add(-3 * time.Hour)}},
			ranked:     healthy, wantOutcome: StickyMiss, wantRowGone: true,
		},

		{
			name: "manual binds with no sessions", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now.Add(-30 * time.Minute), ranked: healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "manual binds through a live session", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now.Add(-10 * time.Minute),
			sessions:   []session{{started: now.Add(-10 * time.Minute)}},
			ranked:     healthy, wantOutcome: StickyBind, wantID: 2,
		},
		{
			name: "manual expires like auto and is dropped", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now.Add(-2 * time.Hour), ranked: healthy, wantOutcome: StickyMiss, wantRowGone: true,
		},
		{
			name: "manual to unusable account is held", cwd: "/proj", record: true, manual: true, recordID: 2,
			recordedAt: now, ranked: pinUnusable, wantOutcome: StickyHoldManual, wantID: 2,
		},

		// Unusable auto pins are abandoned (2026-06-10 incident).
		{
			name: "near-full auto pin abandoned", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now, ranked: pinUnusable, wantOutcome: StickyMiss,
		},
		{
			name: "rate-limited auto pin abandoned", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now, ranked: []score.Result{
				{AccountID: 1, Score: 80, Available: true, Components: score.Components{RawRemaining5h: 90}},
				{AccountID: 2, Score: -50, Available: false, Components: score.Components{RawRemaining5h: 50}},
			}, wantOutcome: StickyMiss,
		},
		// 2026-06-10 incident: the pinned account is exhausted but its imminent
		// reset keeps eff5 high — the pin must still be abandoned.
		{
			name: "exhausted auto pin abandoned", cwd: "/proj", record: true, recordID: 2,
			recordedAt: now, ranked: []score.Result{
				{AccountID: 1, Score: 80, Available: true, Components: score.Components{RawRemaining5h: 60}},
				{AccountID: 2, Score: 65, Available: false, Exhausted: true, Components: score.Components{Eff5: 93, RawRemaining5h: 0}},
			}, wantOutcome: StickyMiss,
		},

		{
			name: "account deleted", cwd: "/proj", record: true, recordID: 9,
			recordedAt: now, ranked: healthy, wantOutcome: StickyMiss,
		},
		{
			name: "empty cwd disabled", cwd: "", record: true, recordID: 2,
			recordedAt: now, ranked: healthy, wantOutcome: StickyMiss,
		},
		{
			name: "no record", cwd: "/other", record: true, recordID: 2,
			recordedAt: now, ranked: healthy, wantOutcome: StickyMiss,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := openTestManager(t)
			if tc.record {
				var err error
				if tc.manual {
					_ = m.Store.UpsertAccount(store.Account{ID: tc.recordID, ConfigDir: "dir", KeychainService: "s", KeychainAccount: "u"})
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
		_ = m.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
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
		_ = m.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
		_ = m.Store.UpsertAccount(store.Account{ID: 2, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

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
		_ = m.Store.UpsertSticky("/proj", 2, now)
		pinned, err = m.TogglePin("/proj", 2, now)
		if err != nil || pinned {
			t.Fatalf("auto unpin toggle: pinned=%v err=%v", pinned, err)
		}
		// An expired unpruned pin counts as absent: the press must pin, not
		// release a pin the selector already misses.
		_ = m.Store.PinManual("/proj", 2, now.Add(-2*time.Hour))
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
		_ = m.Store.UpsertAccount(store.Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})

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
		_ = m.Store.UpsertSticky("/auto", 1, now)
		seedSessionFor(t, m, 1, "/auto", now.Add(-10*time.Minute), nil)
		pv, _, _ = m.PinView("/auto", now)
		if pv.Manual || pv.Binding {
			t.Fatalf("auto live view should not promise binding: %+v", pv)
		}

		// Expired pins are invisible.
		_ = m.Store.PinManual("/stale", 1, now.Add(-2*time.Hour))
		if _, ok, _ := m.PinView("/stale", now); ok {
			t.Fatal("expired pin must not render")
		}
	})
}

// TestClassifyDegradesOnStoreError: an activity-read failure degrades to
// selected_at-only freshness (best-effort), never escaping.
func TestClassifyDegradesOnStoreError(t *testing.T) {
	m := openTestManager(t)
	_ = m.Store.Close() // force GetCwdActivity to fail
	now := time.Now().Truncate(time.Microsecond)
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

// TestSelectSweepReconcilesDeadSessions covers the select-path self-heal: with no
// daemon, a select reaps session rows whose pids are gone — even when the scan
// finds ZERO claude processes (a nil slice), the state after the last exit.
