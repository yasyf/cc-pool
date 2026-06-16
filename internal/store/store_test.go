package store

import (
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
	t.Cleanup(func() { s.Close() })
	return s
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
	// Update label.
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
	// Only the label changed.
	if got.ConfigDir != a.ConfigDir || got.KeychainService != a.KeychainService ||
		got.KeychainAccount != a.KeychainAccount || got.OverlayKind != a.OverlayKind {
		t.Fatalf("non-label fields changed: %+v", got)
	}

	// Idempotent re-set is fine.
	if err := s.SetAccountLabel(1, "Example"); err != nil {
		t.Fatalf("idempotent set: %v", err)
	}

	// Unknown id fails loudly and materializes nothing.
	if err := s.SetAccountLabel(99, "Ghost"); err == nil {
		t.Fatal("want error for unknown account, got nil")
	}
	if _, err := s.GetAccount(99); err == nil {
		t.Fatal("unknown id materialized a row")
	}
}

func TestNextAccountIndex(t *testing.T) {
	s := openTest(t)
	if n, _ := s.NextAccountIndex(); n != 1 {
		t.Fatalf("first index = %d, want 1", n)
	}
	s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	s.UpsertAccount(Account{ID: 2, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	if n, _ := s.NextAccountIndex(); n != 3 {
		t.Fatalf("next index = %d, want 3", n)
	}
	// Remove 1 -> reused.
	s.DeleteAccount(1)
	if n, _ := s.NextAccountIndex(); n != 1 {
		t.Fatalf("reused index = %d, want 1", n)
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
	// Upsert overwrites.
	if err := s.SetMeta("initialized", "2"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := s.GetMeta("initialized"); v != "2" {
		t.Fatalf("overwrite failed: %q", v)
	}
}

func TestUsageSampleLatest(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
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

func TestUsageSampleExtraUsageRoundTrip(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
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

func TestUsageSamplesSince(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	s.UpsertAccount(Account{ID: 2, ConfigDir: "b", KeychainService: "s2", KeychainAccount: "u2"})
	now := time.Now().Truncate(time.Second)
	// acct 1: three samples spanning 90 minutes plus one older than the cutoff.
	for _, sp := range []struct {
		age  time.Duration
		util float64
	}{{0, 30}, {30 * time.Minute, 20}, {60 * time.Minute, 10}, {120 * time.Minute, 5}} {
		if err := s.InsertUsageSample(UsageSample{AccountID: 1, TS: now.Add(-sp.age), Util5h: sp.util}); err != nil {
			t.Fatal(err)
		}
	}
	// acct 2 inside the window must never leak into acct 1's result.
	if err := s.InsertUsageSample(UsageSample{AccountID: 2, TS: now, Util5h: 99}); err != nil {
		t.Fatal(err)
	}

	since := now.Add(-90 * time.Minute) // cutoff sits exactly on the 60-min sample? no — on nothing; boundary tested below
	got, err := s.UsageSamplesSince(1, since)
	if err != nil {
		t.Fatal(err)
	}
	// The 120-min-old sample is older than the cutoff; the acct-2 row is excluded.
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

	// Inclusive boundary: a cutoff landing exactly on a sample's ts includes it.
	exact := now.Add(-60 * time.Minute)
	got2, err := s.UsageSamplesSince(1, exact)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 3 || got2[2].Util5h != 10 {
		t.Fatalf("inclusive boundary dropped the on-cutoff sample: %+v", got2)
	}
	// One second later excludes it.
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
	started := now.Add(-2 * SessionReapGrace) // old enough to reap
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	id1, _ := s.OpenSession(1, 111, "b", "/proj", started)
	s.OpenSession(1, 222, "b", "/proj", started)
	s.OpenSession(1, 333, "b", "/proj", now) // fresh: inside the reap grace
	if n, _ := s.ActiveSessionCount(1); n != 3 {
		t.Fatalf("active = %d, want 3", n)
	}
	live, err := s.ListActiveSessions()
	if err != nil || len(live) != 3 || live[0].Cwd != "/proj" {
		t.Fatalf("active sessions = %+v err=%v", live, err)
	}
	// Only 222 is alive -> 111 closed; 333 is dead but too young to reap.
	closed, err := s.CloseDeadSessions(map[int]bool{222: true}, now)
	if err != nil || closed != 1 {
		t.Fatalf("closed = %d err=%v", closed, err)
	}
	if n, _ := s.ActiveSessionCount(1); n != 2 {
		t.Fatalf("active after reconcile = %d, want 2", n)
	}
	// The alive row was stamped last-seen; the never-observed dead row was
	// closed at its start (no liveness was ever witnessed), not at reap time.
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

func mustActive(t *testing.T, s *Store) []Session {
	t.Helper()
	live, err := s.ListActiveSessions()
	if err != nil {
		t.Fatal(err)
	}
	return live
}

// TestCloseDeadSessionsEndsAtLastSeen pins the honest-end rule: a row whose
// pid was observed alive by an earlier reconcile is closed at that
// observation, never at reap time — a long observer gap must not fabricate a
// warm cache.
func TestCloseDeadSessionsEndsAtLastSeen(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	s.OpenSession(1, 555, "b", "/proj", now.Add(-5*time.Hour))

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
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

	act, err := s.GetCwdActivity("/proj", 1)
	if err != nil || act.Live != 0 || !act.LastEnded.IsZero() {
		t.Fatalf("empty table: %+v err=%v", act, err)
	}

	// One live, two ended (the later end must win), one unattributed
	// (cwd-less) row, and one row on a DIFFERENT account in the same directory.
	s.OpenSession(1, 100, "b", "/proj", now.Add(-3*time.Hour))
	early, _ := s.OpenSession(1, 200, "b", "/proj", now.Add(-2*time.Hour))
	late, _ := s.OpenSession(1, 300, "b", "/proj", now.Add(-90*time.Minute))
	s.CloseSession(early, now.Add(-time.Hour))
	s.CloseSession(late, now.Add(-10*time.Minute))
	s.OpenSession(1, 400, "b", "", now)
	other, _ := s.OpenSession(2, 500, "c", "/proj", now.Add(-time.Hour))
	s.CloseSession(other, now.Add(-time.Minute))

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

	// The empty-cwd row is invisible to any real directory, and a different
	// directory sees nothing.
	if act, _ := s.GetCwdActivity("/other", 1); act.Live != 0 || !act.LastEnded.IsZero() {
		t.Fatalf("unrelated cwd sees activity: %+v", act)
	}
}

// TestDeleteStickyVersion: the version-guarded delete removes only the exact
// row version it was given — a refreshed or repinned row survives.
func TestDeleteStickyVersion(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)

	s.UpsertSticky("/proj", 1, now.Add(-2*time.Hour))
	// Concurrent writer repins before the stale delete lands.
	s.PinManual("/proj", 2, now)
	if err := s.DeleteStickyVersion("/proj", now.Add(-2*time.Hour), false); err != nil {
		t.Fatal(err)
	}
	if st, ok, _ := s.GetSticky("/proj"); !ok || st.AccountID != 2 || !st.Manual {
		t.Fatalf("newer manual pin must survive a stale-versioned delete: %+v ok=%v", st, ok)
	}

	// The matching version is deleted.
	if err := s.DeleteStickyVersion("/proj", now, true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetSticky("/proj"); ok {
		t.Fatal("matching version must be deleted")
	}
}

func TestSticky(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	s.UpsertAccount(Account{ID: 2, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

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

	// Re-upsert (the sliding-TTL write path) overwrites both fields.
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

	// A select that landed elsewhere must not touch the manual pin.
	if err := s.UpsertSticky("/proj", 2, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := s.GetSticky("/proj")
	if !ok || got.AccountID != 1 || !got.Manual || !got.SelectedAt.Equal(now) {
		t.Fatalf("manual pin repointed: %+v", got)
	}

	// A select that landed on the pinned account refreshes it, keeping manual.
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

	// PinManual overrides an existing auto pin.
	s.UpsertSticky("/proj", 1, now.Add(-time.Minute))
	if err := s.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := s.GetSticky("/proj")
	if !ok || got.AccountID != 2 || !got.Manual || !got.SelectedAt.Equal(now) {
		t.Fatalf("manual pin: %+v", got)
	}

	// PinManual also overrides another manual pin (re-pin to a new account).
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
	// Idempotent on a missing row.
	if err := s.DeleteSticky("/proj"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}

func TestPruneSticky(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Second)
	cutoff := now.Add(-time.Hour)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})

	// /old: selected long ago, no sessions -> pruned (today's rule preserved).
	s.UpsertSticky("/old", 1, now.Add(-2*time.Hour))
	// /fresh: recent select -> survives.
	s.UpsertSticky("/fresh", 1, now)
	// /live: stale select but a live tracked session holds it.
	s.UpsertSticky("/live", 1, now.Add(-3*time.Hour))
	s.OpenSession(1, 100, "b", "/live", now.Add(-3*time.Hour))
	// /warm: stale select, last session ended within the TTL.
	s.UpsertSticky("/warm", 1, now.Add(-3*time.Hour))
	warm, _ := s.OpenSession(1, 200, "b", "/warm", now.Add(-3*time.Hour))
	s.CloseSession(warm, now.Add(-30*time.Minute))
	// /cold: stale select, last session ended before the cutoff.
	s.UpsertSticky("/cold", 1, now.Add(-3*time.Hour))
	cold, _ := s.OpenSession(1, 300, "b", "/cold", now.Add(-3*time.Hour))
	s.CloseSession(cold, now.Add(-2*time.Hour))
	// /manual-new: never-used manual pin inside its 1h minimum.
	s.PinManual("/manual-new", 1, now.Add(-30*time.Minute))
	// /manual-old: never-used manual pin past its 1h minimum.
	s.PinManual("/manual-old", 1, now.Add(-2*time.Hour))

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
	s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	s.UpsertSticky("/proj", 1, time.Now())
	if err := s.DeleteAccount(1); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.GetSticky("/proj"); ok {
		t.Fatal("sticky row should be deleted with its account")
	}
}

func TestRefreshLog(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "b", KeychainService: "s", KeychainAccount: "u"})
	if _, ok, _ := s.LastRefresh(1); ok {
		t.Fatal("expected no refresh yet")
	}
	s.LogRefresh(1, false, "boom")
	e, ok, err := s.LastRefresh(1)
	if err != nil || !ok || e.OK || e.Err != "boom" {
		t.Fatalf("last refresh = %+v ok=%v err=%v", e, ok, err)
	}
}

func TestAuthHealthTransitions(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})

	// No row → healthy.
	if h, err := s.GetAuthHealth(1); err != nil || h.NeedsLogin {
		t.Fatalf("fresh account = %+v err=%v, want healthy", h, err)
	}

	// false→true: changed, Since stamped, err recorded.
	t0 := time.Unix(1_000_000, 0)
	changed, err := s.SetNeedsLogin(1, t0, "401 revoked")
	if err != nil || !changed {
		t.Fatalf("first SetNeedsLogin changed=%v err=%v, want true", changed, err)
	}
	h, _ := s.GetAuthHealth(1)
	if !h.NeedsLogin || !h.Since.Equal(t0) || h.LastErr != "401 revoked" {
		t.Fatalf("after flag = %+v", h)
	}

	// true→true: not changed; Since preserved even with a later timestamp; err updates.
	changed, _ = s.SetNeedsLogin(1, t0.Add(time.Hour), "401 again")
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

	// ListAuthHealth surfaces only flagged accounts.
	if m, err := s.ListAuthHealth(); err != nil || len(m) != 1 || !m[1].NeedsLogin {
		t.Fatalf("ListAuthHealth = %+v err=%v", m, err)
	}

	// true→false: changed once, then a no-op clear reports unchanged.
	changed, err = s.ClearNeedsLogin(1)
	if err != nil || !changed {
		t.Fatalf("ClearNeedsLogin changed=%v err=%v, want true", changed, err)
	}
	if changed, _ := s.ClearNeedsLogin(1); changed {
		t.Fatal("clearing an already-healthy account must report changed=false")
	}
	if h, _ := s.GetAuthHealth(1); h.NeedsLogin || !h.Since.IsZero() || h.LastErr != "" {
		t.Fatalf("after clear = %+v, want healthy/zeroed", h)
	}
	if m, _ := s.ListAuthHealth(); len(m) != 0 {
		t.Fatalf("ListAuthHealth after clear = %+v, want empty", m)
	}
}

func TestDeleteAccountRemovesAuthHealth(t *testing.T) {
	s := openTest(t)
	s.UpsertAccount(Account{ID: 1, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"})
	s.SetNeedsLogin(1, time.Now(), "x")
	if err := s.DeleteAccount(1); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("auth_health row should be deleted with its account")
	}
}
