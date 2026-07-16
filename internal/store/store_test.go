package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenMigratesAuthHealthGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE auth_health (
		account_id INTEGER PRIMARY KEY,
		needs_login INTEGER NOT NULL DEFAULT 0,
		since INTEGER,
		last_err TEXT NOT NULL DEFAULT '',
		kind TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.SetNeedsLogin(1, time.Unix(1_000_000, 0), "revoked", AuthKindOwned); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetAuthHealth(1)
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen != 1 {
		t.Fatalf("generation after migrating and flagging = %d, want 1", h.Gen)
	}
}

func TestAccountCRUD(t *testing.T) {
	s := openTest(t)
	a := Account{ID: 1, ConfigDir: "/home/.cc-pool/accounts/acct-01", KeychainService: "svc1", KeychainAccount: "me", Label: "work"}
	if err := s.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfigDir != a.ConfigDir || got.Label != "work" {
		t.Fatalf("got %+v", got)
	}
	a.Label = "renamed"
	if err := s.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetAccount(1)
	if got.Label != "renamed" {
		t.Fatalf("label not updated: %q", got.Label)
	}
	all, _ := s.ListAccounts()
	if len(all) != 1 {
		t.Fatalf("len = %d", len(all))
	}
}

func TestJournalRisks(t *testing.T) {
	s := openTest(t)
	if risks, err := s.ListJournalRisks(); err != nil || len(risks) != 0 {
		t.Fatalf("empty ledger = (%v, %v), want ([], nil)", risks, err)
	}

	now := time.Unix(1_700_000_000, 0)
	if err := s.RecordJournalRisk("/cfg/acct-01", "journal save failed", now); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordJournalRisk("/cfg/acct-02", "still warning", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	risks, err := s.ListJournalRisks()
	if err != nil {
		t.Fatal(err)
	}
	if len(risks) != 2 || risks[0].Dir != "/cfg/acct-01" || risks[1].Dir != "/cfg/acct-02" {
		t.Fatalf("risks = %+v, want two entries oldest-first", risks)
	}
	if risks[0].Warning != "journal save failed" || !risks[0].RecordedAt.Equal(now) {
		t.Fatalf("risk[0] = %+v, want the recorded warning and time", risks[0])
	}

	// A re-record overwrites the same dir's warning, never duplicates it.
	if err := s.RecordJournalRisk("/cfg/acct-01", "newer warning", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	risks, _ = s.ListJournalRisks()
	if len(risks) != 2 {
		t.Fatalf("re-record duplicated the entry: %+v", risks)
	}

	if err := s.ClearJournalRisk("/cfg/acct-01"); err != nil {
		t.Fatal(err)
	}
	risks, _ = s.ListJournalRisks()
	if len(risks) != 1 || risks[0].Dir != "/cfg/acct-02" {
		t.Fatalf("after clear = %+v, want only acct-02", risks)
	}
	// Clearing an absent dir is a no-op, not an error.
	if err := s.ClearJournalRisk("/cfg/acct-absent"); err != nil {
		t.Fatalf("clear of an absent risk = %v, want nil", err)
	}
}

func TestSetAccountLabel(t *testing.T) {
	s := openTest(t)
	a := Account{ID: 1, ConfigDir: "/home/.cc-pool/accounts/acct-01", KeychainService: "svc1", KeychainAccount: "me", Label: "me@example.com", OverlayKind: "symlink"}
	if err := s.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	if err := s.SetAccountLabel(1, "Example"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Label != "Example" {
		t.Fatalf("label = %q, want %q", got.Label, "Example")
	}
	if got.ConfigDir != a.ConfigDir || got.KeychainService != a.KeychainService ||
		got.KeychainAccount != a.KeychainAccount || got.OverlayKind != a.OverlayKind {
		t.Fatalf("non-label fields changed: %+v", got)
	}

	if err := s.SetAccountLabel(1, "Example"); err != nil {
		t.Fatalf("idempotent set: %v", err)
	}

	if err := s.SetAccountLabel(99, "Ghost"); err == nil {
		t.Fatal("want error for unknown account, got nil")
	}
	if _, err := s.GetAccount(99); err == nil {
		t.Fatal("unknown id materialized a row")
	}
}

func TestMetaRoundTrip(t *testing.T) {
	s := openTest(t)
	if _, ok, err := s.GetMeta("initialized"); ok || err != nil {
		t.Fatalf("absent key: ok=%v err=%v", ok, err)
	}
	if err := s.SetMeta("initialized", "1"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.GetMeta("initialized")
	if err != nil || !ok || v != "1" {
		t.Fatalf("get after set: v=%q ok=%v err=%v", v, ok, err)
	}
	if err := s.SetMeta("initialized", "2"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := s.GetMeta("initialized"); v != "2" {
		t.Fatalf("overwrite failed: %q", v)
	}
}

func TestUsageSampleLatest(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	old := UsageSample{AccountID: 1, TS: time.Now().Add(-time.Minute), Util5h: 10}
	cur := UsageSample{AccountID: 1, TS: time.Now(), Util5h: 50, Resets5h: time.Now().Add(time.Hour), RateLimited: true}
	if err := s.InsertUsageSample(old); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertUsageSample(cur); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LatestUsageSample(1)
	if err != nil || !ok {
		t.Fatalf("latest: ok=%v err=%v", ok, err)
	}
	if got.Util5h != 50 || !got.RateLimited || got.Resets5h.IsZero() {
		t.Fatalf("latest sample wrong: %+v", got)
	}
}

func TestLatestGoodUsageSample(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	type spec struct {
		age         time.Duration
		util7d      float64
		rateLimited bool
	}
	cases := map[string]struct {
		samples   []spec
		wantOK    bool
		wantUtil  float64
		wantAgeTS time.Duration
	}{
		"reads through a newer rate_limited marker to the last good sample": {
			samples: []spec{
				{age: 2 * time.Minute, util7d: 73, rateLimited: false},
				{age: 0, util7d: 0, rateLimited: true},
			},
			wantOK: true, wantUtil: 73, wantAgeTS: 2 * time.Minute,
		},
		"returns the newest good sample, skipping interleaved markers": {
			samples: []spec{
				{age: 4 * time.Minute, util7d: 40, rateLimited: false},
				{age: 3 * time.Minute, util7d: 0, rateLimited: true},
				{age: 2 * time.Minute, util7d: 55, rateLimited: false},
				{age: 1 * time.Minute, util7d: 0, rateLimited: true},
			},
			wantOK: true, wantUtil: 55, wantAgeTS: 2 * time.Minute,
		},
		"ok=false when only rate_limited markers exist": {
			samples: []spec{
				{age: 2 * time.Minute, util7d: 0, rateLimited: true},
				{age: 0, util7d: 0, rateLimited: true},
			},
			wantOK: false,
		},
		"ok=false when the account was never sampled": {
			samples: nil,
			wantOK:  false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
			for _, sp := range tc.samples {
				if err := s.InsertUsageSample(UsageSample{
					AccountID:   1,
					TS:          now.Add(-sp.age),
					Util7d:      sp.util7d,
					RateLimited: sp.rateLimited,
				}); err != nil {
					t.Fatal(err)
				}
			}
			got, ok, err := s.LatestGoodUsageSample(1)
			if err != nil {
				t.Fatalf("LatestGoodUsageSample: %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (got %+v)", ok, tc.wantOK, got)
			}
			if !tc.wantOK {
				return
			}
			if got.RateLimited {
				t.Fatalf("returned a rate_limited row: %+v", got)
			}
			if got.Util7d != tc.wantUtil {
				t.Errorf("Util7d = %v, want %v", got.Util7d, tc.wantUtil)
			}
			if wantTS := now.Add(-tc.wantAgeTS); !got.TS.Equal(wantTS) {
				t.Errorf("TS = %v, want %v", got.TS, wantTS)
			}
		})
	}
}

func TestUsageSampleExtraUsageRoundTrip(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	in := UsageSample{AccountID: 1, TS: time.Now(), Util5h: 100, ExtraEnabled: true, ExtraUsed: 177, ExtraLimit: 5000}
	if err := s.InsertUsageSample(in); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LatestUsageSample(1)
	if err != nil || !ok {
		t.Fatalf("latest: ok=%v err=%v", ok, err)
	}
	if !got.ExtraEnabled || got.ExtraUsed != 177 || got.ExtraLimit != 5000 {
		t.Fatalf("extra usage did not round-trip: %+v", got)
	}
	recent, err := s.UsageSamplesSince(1, time.Time{})
	if err != nil || len(recent) != 1 || !recent[0].ExtraEnabled {
		t.Fatalf("recent samples missing extra usage: %+v err=%v", recent, err)
	}
}

func TestUsageSampleScopedRoundTrip(t *testing.T) {
	reset := time.Now().Add(3 * 24 * time.Hour).Truncate(time.Second)
	cases := map[string]struct {
		in         UsageSample
		wantModel  string
		wantUtil   float64
		wantReset  time.Time
		wantAbsent bool
	}{
		"full scoped bucket round-trips": {
			in: UsageSample{
				AccountID: 1, TS: time.Now(),
				Scoped7dModel: "Fable", Scoped7dUtil: 100, Scoped7dResets: reset,
			},
			wantModel: "Fable", wantUtil: 100, wantReset: reset,
		},
		"absent bucket (empty model) round-trips as zero values": {
			in:         UsageSample{AccountID: 1, TS: time.Now(), Util7d: 60},
			wantAbsent: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
			if err := s.InsertUsageSample(tc.in); err != nil {
				t.Fatal(err)
			}
			got, ok, err := s.LatestUsageSample(1)
			if err != nil || !ok {
				t.Fatalf("latest: ok=%v err=%v", ok, err)
			}
			if tc.wantAbsent {
				if got.Scoped7dModel != "" || got.Scoped7dUtil != 0 || !got.Scoped7dResets.IsZero() {
					t.Fatalf("absent bucket did not round-trip as zero: %+v", got)
				}
				return
			}
			if got.Scoped7dModel != tc.wantModel || got.Scoped7dUtil != tc.wantUtil {
				t.Fatalf("scoped model/util = %q/%v, want %q/%v", got.Scoped7dModel, got.Scoped7dUtil, tc.wantModel, tc.wantUtil)
			}
			if !got.Scoped7dResets.Equal(tc.wantReset) {
				t.Fatalf("scoped resets = %v, want %v", got.Scoped7dResets, tc.wantReset)
			}
		})
	}
}

func TestUsageSamplesSince(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	_ = s.UpsertAccount(Account{ID: 2, ConfigDir: "b", KeychainService: "s2", KeychainAccount: "u2"})
	now := time.Now().Truncate(time.Second)
	for _, sp := range []struct {
		age  time.Duration
		util float64
	}{{0, 30}, {30 * time.Minute, 20}, {60 * time.Minute, 10}, {120 * time.Minute, 5}} {
		if err := s.InsertUsageSample(UsageSample{AccountID: 1, TS: now.Add(-sp.age), Util5h: sp.util}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.InsertUsageSample(UsageSample{AccountID: 2, TS: now, Util5h: 99}); err != nil {
		t.Fatal(err)
	}

	since := now.Add(-90 * time.Minute)
	got, err := s.UsageSamplesSince(1, since)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d samples, want 3: %+v", len(got), got)
	}
	// Newest first.
	if got[0].Util5h != 30 || got[1].Util5h != 20 || got[2].Util5h != 10 {
		t.Fatalf("ordering wrong: %+v", got)
	}
	for _, g := range got {
		if g.AccountID != 1 {
			t.Fatalf("other-account row leaked: %+v", g)
		}
	}

	exact := now.Add(-60 * time.Minute)
	got2, err := s.UsageSamplesSince(1, exact)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 3 || got2[2].Util5h != 10 {
		t.Fatalf("inclusive boundary dropped the on-cutoff sample: %+v", got2)
	}
	got3, err := s.UsageSamplesSince(1, exact.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(got3) != 2 {
		t.Fatalf("strict-after cutoff want 2, got %d: %+v", len(got3), got3)
	}
}

func TestSessionsReconcile(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	started := now.Add(-2 * SessionReapGrace)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	id1, _ := s.OpenSession(1, 111, "b", "/proj", started)
	_, _ = s.OpenSession(1, 222, "b", "/proj", started)
	_, _ = s.OpenSession(1, 333, "b", "/proj", now) // fresh: inside the reap grace
	if n, _ := s.ActiveSessionCount(1); n != 3 {
		t.Fatalf("active = %d, want 3", n)
	}
	live, err := s.ListActiveSessions()
	if err != nil || len(live) != 3 || live[0].Cwd != "/proj" {
		t.Fatalf("active sessions = %+v err=%v", live, err)
	}
	closed, err := s.CloseDeadSessions(map[int]bool{222: true}, now)
	if err != nil || closed != 1 {
		t.Fatalf("closed = %d err=%v", closed, err)
	}
	if n, _ := s.ActiveSessionCount(1); n != 2 {
		t.Fatalf("active after reconcile = %d, want 2", n)
	}
	for _, se := range mustActive(t, s) {
		if se.PID == 222 && (se.LastSeenAt == nil || !se.LastSeenAt.Equal(now)) {
			t.Fatalf("alive row not stamped last-seen: %+v", se)
		}
	}
	if act, _ := s.GetCwdActivity("/proj", 1); !act.LastEnded.Equal(started) {
		t.Fatalf("reaped row must end at its start, got %v want %v", act.LastEnded, started)
	}
	if err := s.CloseSession(id1, now); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSelectionAtomic(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	if _, err := s.db.Exec(`
		CREATE TRIGGER fail_session BEFORE INSERT ON sessions
		BEGIN
			SELECT RAISE(ABORT, 'forced session failure');
		END`); err != nil {
		t.Fatal(err)
	}

	err := s.CommitSelection(1, 4242, "/acct-01", "/proj", now, true)
	if err == nil {
		t.Fatal("CommitSelection succeeded with failing session insert")
	}
	if _, ok, getErr := s.GetSticky("/proj"); getErr != nil {
		t.Fatal(getErr)
	} else if ok {
		t.Fatal("sticky write survived failed session insert")
	}
	if sessions, listErr := s.ListActiveSessions(); listErr != nil {
		t.Fatal(listErr)
	} else if len(sessions) != 0 {
		t.Fatalf("sessions after failed commit = %+v", sessions)
	}
}

func TestCommitSelectionConditionalEffects(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	if err := s.CommitSelection(1, 4242, "/acct-01", "/proj", now, false); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetSticky("/proj"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("recordSticky=false recorded sticky state")
	}
	sessions, err := s.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].PID != 4242 {
		t.Fatalf("sessions = %+v, want pid 4242", sessions)
	}

	if err := s.CommitSelection(1, 0, "", "/sticky-only", now, true); err != nil {
		t.Fatal(err)
	}
	if sticky, ok, err := s.GetSticky("/sticky-only"); err != nil {
		t.Fatal(err)
	} else if !ok || sticky.AccountID != 1 {
		t.Fatalf("sticky = %+v ok=%v, want account 1", sticky, ok)
	}
	sessions, err = s.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 {
		t.Fatalf("sticky-only commit opened a session: %+v", sessions)
	}
}

func mustActive(t *testing.T, s *Store) []Session {
	t.Helper()
	live, err := s.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	return live
}

// TestCloseDeadSessionsEndsAtLastSeen: a pid seen alive by an earlier reconcile
// is closed at that observation, not reap time.
func TestCloseDeadSessionsEndsAtLastSeen(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	_, _ = s.OpenSession(1, 555, "b", "/proj", now.Add(-5*time.Hour))

	// A reconcile 4h ago saw the pid alive; the process then died unobserved.
	if _, err := s.CloseDeadSessions(map[int]bool{555: true}, now.Add(-4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	closed, err := s.CloseDeadSessions(map[int]bool{}, now)
	if err != nil || closed != 1 {
		t.Fatalf("closed = %d err=%v", closed, err)
	}
	act, err := s.GetCwdActivity("/proj", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !act.LastEnded.Equal(now.Add(-4 * time.Hour)) {
		t.Fatalf("end = %v, want the last-seen time %v (not reap time %v)",
			act.LastEnded, now.Add(-4*time.Hour), now)
	}
}

func TestGetCwdActivity(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

	act, err := s.GetCwdActivity("/proj", 1)
	if err != nil || act.Live != 0 || !act.LastEnded.IsZero() {
		t.Fatalf("empty table: %+v err=%v", act, err)
	}

	_, _ = s.OpenSession(1, 100, "b", "/proj", now.Add(-3*time.Hour))
	early, _ := s.OpenSession(1, 200, "b", "/proj", now.Add(-2*time.Hour))
	late, _ := s.OpenSession(1, 300, "b", "/proj", now.Add(-90*time.Minute))
	_ = s.CloseSession(early, now.Add(-time.Hour))
	_ = s.CloseSession(late, now.Add(-10*time.Minute))
	_, _ = s.OpenSession(1, 400, "b", "", now)
	other, _ := s.OpenSession(2, 500, "c", "/proj", now.Add(-time.Hour))
	_ = s.CloseSession(other, now.Add(-time.Minute))

	act, err = s.GetCwdActivity("/proj", 1)
	if err != nil {
		t.Fatal(err)
	}
	if act.Live != 1 {
		t.Fatalf("live = %d, want 1", act.Live)
	}
	// Account 2's fresher end (1m ago) must not leak into account 1's view.
	if !act.LastEnded.Equal(now.Add(-10 * time.Minute)) {
		t.Fatalf("lastEnded = %v, want %v", act.LastEnded, now.Add(-10*time.Minute))
	}

	if act, _ := s.GetCwdActivity("/other", 1); act.Live != 0 || !act.LastEnded.IsZero() {
		t.Fatalf("unrelated cwd sees activity: %+v", act)
	}
}

// TestDeleteStickyVersion: version-guarded delete removes only the exact row
// version; a refreshed or repinned row survives.
func TestDeleteStickyVersion(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)

	_ = s.UpsertSticky("/proj", 1, now.Add(-2*time.Hour))
	_ = s.PinManual("/proj", 2, now)
	if err := s.DeleteStickyVersion("/proj", now.Add(-2*time.Hour), false); err != nil {
		t.Fatal(err)
	}
	if st, ok, _ := s.GetSticky("/proj"); !ok || st.AccountID != 2 || !st.Manual {
		t.Fatalf("newer manual pin must survive a stale-versioned delete: %+v ok=%v", st, ok)
	}

	if err := s.DeleteStickyVersion("/proj", now, true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetSticky("/proj"); ok {
		t.Fatal("matching version must be deleted")
	}
}

func TestSticky(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	_ = s.UpsertAccount(Account{ID: 2, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

	if _, ok, err := s.GetSticky("/proj"); ok || err != nil {
		t.Fatalf("empty table: ok=%v err=%v", ok, err)
	}

	t0 := time.Now().Truncate(time.Second)
	if err := s.UpsertSticky("/proj", 1, t0); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetSticky("/proj")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.Cwd != "/proj" || got.AccountID != 1 || !got.SelectedAt.Equal(t0) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	t1 := t0.Add(time.Minute)
	if err := s.UpsertSticky("/proj", 2, t1); err != nil {
		t.Fatal(err)
	}
	got, _, _ = s.GetSticky("/proj")
	if got.AccountID != 2 || !got.SelectedAt.Equal(t1) || got.Manual {
		t.Fatalf("upsert did not overwrite: %+v", got)
	}
}

func TestUpsertStickyNeverRepointsManualPin(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	if err := s.PinManual("/proj", 1, now); err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertSticky("/proj", 2, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := s.GetSticky("/proj")
	if !ok || got.AccountID != 1 || !got.Manual || !got.SelectedAt.Equal(now) {
		t.Fatalf("manual pin repointed: %+v", got)
	}

	t2 := now.Add(2 * time.Minute)
	if err := s.UpsertSticky("/proj", 1, t2); err != nil {
		t.Fatal(err)
	}
	got, _, _ = s.GetSticky("/proj")
	if got.AccountID != 1 || !got.Manual || !got.SelectedAt.Equal(t2) {
		t.Fatalf("manual pin not refreshed: %+v", got)
	}
}

func TestPinManualAndDeleteSticky(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)

	_ = s.UpsertSticky("/proj", 1, now.Add(-time.Minute))
	if err := s.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := s.GetSticky("/proj")
	if !ok || got.AccountID != 2 || !got.Manual || !got.SelectedAt.Equal(now) {
		t.Fatalf("manual pin: %+v", got)
	}

	if err := s.PinManual("/proj", 1, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := s.GetSticky("/proj"); got.AccountID != 1 || !got.Manual {
		t.Fatalf("re-pin: %+v", got)
	}

	if err := s.DeleteSticky("/proj"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetSticky("/proj"); ok {
		t.Fatal("pin should be deleted")
	}
	if err := s.DeleteSticky("/proj"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}

func TestPruneSticky(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	cutoff := now.Add(-time.Hour)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

	// /old: selected long ago, no sessions -> pruned.
	_ = s.UpsertSticky("/old", 1, now.Add(-2*time.Hour))
	// /fresh: recent select -> survives.
	_ = s.UpsertSticky("/fresh", 1, now)
	// /live: stale select but a live tracked session holds it.
	_ = s.UpsertSticky("/live", 1, now.Add(-3*time.Hour))
	_, _ = s.OpenSession(1, 100, "b", "/live", now.Add(-3*time.Hour))
	// /warm: stale select, last session ended within the TTL.
	_ = s.UpsertSticky("/warm", 1, now.Add(-3*time.Hour))
	warm, _ := s.OpenSession(1, 200, "b", "/warm", now.Add(-3*time.Hour))
	_ = s.CloseSession(warm, now.Add(-30*time.Minute))
	// /cold: stale select, last session ended before the cutoff.
	_ = s.UpsertSticky("/cold", 1, now.Add(-3*time.Hour))
	cold, _ := s.OpenSession(1, 300, "b", "/cold", now.Add(-3*time.Hour))
	_ = s.CloseSession(cold, now.Add(-2*time.Hour))
	// /manual-new: never-used manual pin inside its 1h minimum.
	_ = s.PinManual("/manual-new", 1, now.Add(-30*time.Minute))
	// /manual-old: never-used manual pin past its 1h minimum.
	_ = s.PinManual("/manual-old", 1, now.Add(-2*time.Hour))

	n, err := s.PruneSticky(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("pruned %d rows, want 3 (/old, /cold, /manual-old)", n)
	}
	for _, cwd := range []string{"/fresh", "/live", "/warm", "/manual-new"} {
		if _, ok, _ := s.GetSticky(cwd); !ok {
			t.Errorf("%s should survive", cwd)
		}
	}
	for _, cwd := range []string{"/old", "/cold", "/manual-old"} {
		if _, ok, _ := s.GetSticky(cwd); ok {
			t.Errorf("%s should be pruned", cwd)
		}
	}
}

func TestDeleteAccountRemovesSticky(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	_ = s.UpsertSticky("/proj", 1, time.Now())
	if err := s.DeleteAccount(1); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetSticky("/proj"); ok {
		t.Fatal("sticky row should be deleted with its account")
	}
}

func TestRefreshLog(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	if _, ok, _ := s.LastRefresh(1); ok {
		t.Fatal("expected no refresh yet")
	}
	_ = s.LogRefresh(1, false, "boom")
	e, ok, err := s.LastRefresh(1)
	if err != nil || !ok || e.OK || e.Err != "boom" {
		t.Fatalf("last refresh = %+v ok=%v err=%v", e, ok, err)
	}
}

func TestAuthHealthTransitions(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})

	if h, err := s.GetAuthHealth(1); err != nil || h.NeedsLogin {
		t.Fatalf("fresh account = %+v err=%v, want healthy", h, err)
	}

	t0 := time.Unix(1_000_000, 0)
	changed, err := s.SetNeedsLogin(1, t0, "401 revoked", AuthKindOwned)
	if err != nil || !changed {
		t.Fatalf("first SetNeedsLogin changed=%v err=%v, want true", changed, err)
	}
	h, _ := s.GetAuthHealth(1)
	if !h.NeedsLogin || !h.Since.Equal(t0) || h.LastErr != "401 revoked" || h.Kind != AuthKindOwned {
		t.Fatalf("after flag = %+v", h)
	}

	// A repeat preserves Since but refreshes LastErr AND Kind (owned→awaiting).
	changed, _ = s.SetNeedsLogin(1, t0.Add(time.Hour), "401 again", AuthKindAwaitingOrigin)
	if changed {
		t.Fatal("repeat SetNeedsLogin must report changed=false")
	}
	h, _ = s.GetAuthHealth(1)
	if !h.Since.Equal(t0) {
		t.Fatalf("Since must measure the whole outage; got %v want %v", h.Since, t0)
	}
	if h.LastErr != "401 again" {
		t.Fatalf("LastErr should update; got %q", h.LastErr)
	}
	if h.Kind != AuthKindAwaitingOrigin {
		t.Fatalf("Kind should refresh to awaiting-origin; got %q", h.Kind)
	}

	if m, err := s.ListAuthHealth(); err != nil || len(m) != 1 || !m[1].NeedsLogin || m[1].Kind != AuthKindAwaitingOrigin {
		t.Fatalf("ListAuthHealth = %+v err=%v", m, err)
	}

	if _, err := s.SetNeedsLogin(1, t0, "x", AuthKind("bogus")); err == nil {
		t.Fatal("SetNeedsLogin with an invalid kind must error")
	}

	changed, err = s.ClearNeedsLogin(1)
	if err != nil || !changed {
		t.Fatalf("ClearNeedsLogin changed=%v err=%v, want true", changed, err)
	}
	if changed, _ := s.ClearNeedsLogin(1); changed {
		t.Fatal("clearing an already-healthy account must report changed=false")
	}
	if h, _ := s.GetAuthHealth(1); h.NeedsLogin || !h.Since.IsZero() || h.LastErr != "" || h.Kind != AuthKindOwned {
		t.Fatalf("after clear = %+v, want healthy/zeroed", h)
	}
	if m, _ := s.ListAuthHealth(); len(m) != 0 {
		t.Fatalf("ListAuthHealth after clear = %+v, want empty", m)
	}
}

func TestAuthHealthGenerationCAS(t *testing.T) {
	s := openTest(t)
	if err := s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"}); err != nil {
		t.Fatal(err)
	}

	h, err := s.GetAuthHealth(1)
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen != 0 {
		t.Fatalf("never-flagged generation = %d, want 0", h.Gen)
	}

	t0 := time.Unix(1_000_000, 0)
	if _, err := s.SetNeedsLogin(1, t0, "first", AuthKindOwned); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetNeedsLogin(1, t0.Add(time.Minute), "second", AuthKindAwaitingOrigin); err != nil {
		t.Fatal(err)
	}
	h, err = s.GetAuthHealth(1)
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen != 2 {
		t.Fatalf("generation after two flags = %d, want 2", h.Gen)
	}
	health, err := s.ListAuthHealth()
	if err != nil {
		t.Fatal(err)
	}
	if health[1].Gen != 2 {
		t.Fatalf("listed generation = %d, want 2", health[1].Gen)
	}

	cleared, err := s.ClearNeedsLoginIfGen(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cleared {
		t.Fatal("stale generation cleared a fresher needs-login verdict")
	}
	h, err = s.GetAuthHealth(1)
	if err != nil {
		t.Fatal(err)
	}
	if !h.NeedsLogin || h.Gen != 2 {
		t.Fatalf("after stale clear = %+v, want flagged at generation 2", h)
	}

	cleared, err = s.ClearNeedsLoginIfGen(1, 2)
	if err != nil || !cleared {
		t.Fatalf("current-generation clear changed=%v err=%v, want true", cleared, err)
	}
	h, err = s.GetAuthHealth(1)
	if err != nil {
		t.Fatal(err)
	}
	if h.NeedsLogin || h.Gen != 2 || !h.Since.IsZero() || h.LastErr != "" || h.Kind != AuthKindOwned {
		t.Fatalf("after current-generation clear = %+v, want healthy at generation 2", h)
	}

	if _, err := s.SetNeedsLogin(1, t0.Add(2*time.Minute), "third", AuthKindOwned); err != nil {
		t.Fatal(err)
	}
	h, err = s.GetAuthHealth(1)
	if err != nil {
		t.Fatal(err)
	}
	if !h.NeedsLogin || h.Gen != 3 {
		t.Fatalf("after reflag = %+v, want flagged at generation 3", h)
	}
}

func TestDeleteAccountRemovesAuthHealth(t *testing.T) {
	s := openTest(t)
	_ = s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	_, _ = s.SetNeedsLogin(1, time.Now(), "x", AuthKindOwned)
	if err := s.DeleteAccount(1); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("auth_health row should be deleted with its account")
	}
}
