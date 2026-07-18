package daemon

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

var t0 = time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

// pol fetches a policy row from the table; the tests exercise the substrate on
// the real self-heal tunings, not synthetic ones.
func pol(t *testing.T, name string) policy {
	t.Helper()
	p, ok := policies[name]
	if !ok {
		t.Fatalf("unknown policy %q", name)
	}
	return p
}

// TestLedgerDebounce covers the Phase-1 fault verdict: strikes accumulate toward
// the policy's debounce, latch faulted on reaching it, and clear resets. Negative
// cases: below debounce is not faulted; a policy with no debounce never faults
// via strike.
func TestLedgerDebounce(t *testing.T) {
	cases := []struct {
		name       string
		policy     string
		strikes    int
		wantFault  bool
		wantStrike int // strikes after the strikes above
	}{
		{"deepwedge/one_strike_below_threshold", "fuse.deepwedge", 1, false, 1},
		{"deepwedge/reaches_threshold_faults", "fuse.deepwedge", 2, true, 0},
		{"deepwedge/extra_strikes_stay_faulted_no_lane_advance", "fuse.deepwedge", 5, true, 0},
		{"auth/two_below_threshold", "auth.streak", 2, false, 2},
		{"auth/three_faults", "auth.streak", 3, true, 0},
		{"fp/reaches_two_faults", "fp.domain", 2, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pol(t, tc.policy)
			l := &ledger{}
			for i := 0; i < tc.strikes; i++ {
				l.strike(p, t0, errors.New("probe failed"))
			}
			if l.faulted != tc.wantFault {
				t.Fatalf("faulted = %v, want %v", l.faulted, tc.wantFault)
			}
			if l.strikes != tc.wantStrike {
				t.Fatalf("strikes = %d, want %d", l.strikes, tc.wantStrike)
			}
			if l.lastErr == nil || l.lastAt != t0 {
				t.Fatalf("lastErr/lastAt not recorded: %v %v", l.lastErr, l.lastAt)
			}
		})
	}
}

// TestLedgerStrikeAfterFaultDoesNotAdvanceLane pins the incident-relevant guard:
// once faulted, further strikes must not creep the primary breaker lane (they
// only refresh lastErr/lastAt), so continued probe failures can never trip the
// recovery breaker.
func TestLedgerStrikeAfterFaultDoesNotAdvanceLane(t *testing.T) {
	p := pol(t, "fp.domain") // debounce 2, breaker 5
	l := &ledger{}
	l.strike(p, t0, errors.New("x"))
	l.strike(p, t0, errors.New("x")) // faults, strikes -> 0
	for i := 0; i < 10; i++ {
		l.strike(p, t0, errors.New("still failing"))
	}
	if !l.faulted || l.strikes != 0 {
		t.Fatalf("faulted=%v strikes=%d, want faulted with strikes 0", l.faulted, l.strikes)
	}
	if l.parked(p) {
		t.Fatal("post-fault strikes tripped the recovery breaker — must not")
	}
}

// TestLedgerForceFault latches immediately, bypassing the debounce, and resets
// the primary lane so recovery starts clean — the select-time forceWedge shape.
func TestLedgerForceFault(t *testing.T) {
	p := pol(t, "fp.domain")
	l := &ledger{strikes: 1} // a partial debounce in flight
	l.forceFault(t0, errors.New("select probe"))
	if !l.faulted {
		t.Fatal("forceFault did not latch faulted")
	}
	if l.strikes != 0 {
		t.Fatalf("forceFault left strikes = %d, want 0", l.strikes)
	}
	if !l.due(p, t0) {
		t.Fatal("a freshly forced fault should be immediately due for recovery")
	}
}

// TestLedgerStampAndSetNextDue pins the two additive streak-port ops: stamp
// records the clock without touching the debounce or the recovery ladder (the
// definitive-needs-login throttle stamp, which flags the store not the streak),
// and setNextDue overrides the backoff window (the 429 Retry-After hint).
func TestLedgerStampAndSetNextDue(t *testing.T) {
	l := &ledger{}
	l.stamp(t0, errors.New("needs login"))
	if l.strikes != 0 || l.faulted || l.attempts != 0 {
		t.Fatalf("stamp advanced the ledger: %+v", *l)
	}
	if l.lastAt != t0 || l.lastErr == nil {
		t.Fatalf("stamp did not record the clock/err: %+v", *l)
	}

	rl := pol(t, "ratelimit.pool")
	l2 := &ledger{}
	l2.attempt(rl, attemptPrimary, t0) // nextDue = t0 + rlBackoff(1)
	override := t0.Add(2 * time.Second)
	l2.setNextDue(override)
	if l2.nextDue != override {
		t.Fatalf("setNextDue = %v, want %v", l2.nextDue, override)
	}
	if l2.attempts != 1 {
		t.Fatalf("setNextDue disturbed the streak count: attempts = %d, want 1", l2.attempts)
	}
}

// TestLedgerBackoffDue walks the recovery ladder: each attempt advances the
// shared clock and spaces the next attempt by proc.Backoff.After(attempts); due
// is false inside the window and true once it elapses.
func TestLedgerBackoffDue(t *testing.T) {
	p := pol(t, "fp.domain") // backoff 30s..10m
	bo := proc.Backoff{Base: 30 * time.Second, Cap: 10 * time.Minute}
	l := &ledger{}
	l.forceFault(t0, errors.New("wedged"))
	now := t0
	for n := 1; n <= 4; n++ {
		l.attempt(p, attemptPrimary, now)
		wantDue := now.Add(bo.After(n))
		if l.nextDue != wantDue {
			t.Fatalf("attempt %d nextDue = %v, want %v", n, l.nextDue, wantDue)
		}
		if l.due(p, now) {
			t.Fatalf("attempt %d: due immediately after attempting (inside backoff)", n)
		}
		if l.due(p, wantDue.Add(-time.Nanosecond)) {
			t.Fatalf("attempt %d: due one tick before nextDue", n)
		}
		if !l.due(p, wantDue) {
			t.Fatalf("attempt %d: not due at nextDue", n)
		}
		now = wantDue
	}
}

// TestLedgerBreakerTrip covers the breaker: consecutive primary attempts park
// the ledger exactly at the policy's breaker — via the attempts clock for the
// single-lane fp.domain, via the primary lane for the two-lane fuse.remount.
func TestLedgerBreakerTrip(t *testing.T) {
	cases := []struct {
		name    string
		policy  string
		breaker int
		onTrip  tripAction
	}{
		{"fp_domain_parks_at_5", "fp.domain", 5, tripPark},
		{"fuse_remount_retreats_at_5", "fuse.remount", 5, tripRetreat},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pol(t, tc.policy)
			if p.onTrip != tc.onTrip {
				t.Fatalf("policy onTrip = %v, want %v", p.onTrip, tc.onTrip)
			}
			l := &ledger{}
			for n := 1; n <= tc.breaker; n++ {
				parked := l.attempt(p, attemptPrimary, t0)
				wantParked := n >= tc.breaker
				if parked != wantParked {
					t.Fatalf("attempt %d parked = %v, want %v", n, parked, wantParked)
				}
			}
			if !l.parked(p) {
				t.Fatal("breaker did not trip at threshold")
			}
			if l.due(p, t0.Add(time.Hour)) {
				t.Fatal("a parked ledger must never be due")
			}
		})
	}
}

// TestLedgerSingleLanePreFaultAttemptsNeverErodeDebounce pins the single-lane
// contract: recovery attempts on an un-faulted row (the FP Missing control-plane
// heal riding the backoff schedule) never touch the strikes debounce — the wedge
// verdict still takes the full pinned debounce (2) to latch. The old fpState kept
// these counters in separate maps; the fold must not merge them.
func TestLedgerSingleLanePreFaultAttemptsNeverErodeDebounce(t *testing.T) {
	p := pol(t, "fp.domain") // single-lane: alt 0, debounce 2, breaker 5
	l := &ledger{}
	for i := 0; i < 3; i++ {
		l.attempt(p, attemptPrimary, t0) // pre-fault control-plane repair attempts
	}
	if l.faulted || l.strikes != 0 {
		t.Fatalf("pre-fault attempts touched the debounce: faulted=%v strikes=%d, want false/0", l.faulted, l.strikes)
	}
	l.strike(p, t0, errors.New("wedged"))
	if l.faulted {
		t.Fatal("one strike after pre-fault attempts latched the fault — the debounce eroded to 1")
	}
	l.strike(p, t0, errors.New("wedged"))
	if !l.faulted {
		t.Fatal("second strike did not latch the fault (debounce 2)")
	}
}

// TestLedgerSingleLaneParksOnAttemptsAtBreaker pins that a single-lane policy's
// breaker measures the shared attempts clock — the old fpState recovery ladder's
// park-at-5-attempts contract — with strikes never charged along the way.
func TestLedgerSingleLaneParksOnAttemptsAtBreaker(t *testing.T) {
	p := pol(t, "fp.domain") // breaker 5 on attempts
	l := &ledger{}
	l.forceFault(t0, errors.New("wedged"))
	for n := 1; n <= fpRecoveryBreaker; n++ {
		parked := l.attempt(p, attemptPrimary, t0)
		if want := n >= fpRecoveryBreaker; parked != want {
			t.Fatalf("attempt %d parked = %v, want %v", n, parked, want)
		}
		if l.strikes != 0 {
			t.Fatalf("attempt %d charged the strikes lane on a single-lane policy: strikes=%d", n, l.strikes)
		}
	}
	if !l.parked(p) {
		t.Fatal("breaker did not trip at fpRecoveryBreaker attempts")
	}
	if l.due(p, t0.Add(24*time.Hour)) {
		t.Fatal("a parked ledger must never be due")
	}
}

// TestLedgerTwoLaneMutualReset is fuse.remount's incident contract: primary
// (hazard) and alt (TCC) attempts each reset the other's lane, so an alternating
// row trips NEITHER breaker while the shared attempts clock still advances the
// backoff. This is the case a naive single-counter breaker would trip falsely.
func TestLedgerTwoLaneMutualReset(t *testing.T) {
	p := pol(t, "fuse.remount") // breaker 5 (strikes), alt 6 (altHits)
	bo := proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap}
	l := &ledger{}
	kinds := []attemptKind{attemptPrimary, attemptAlt}
	for n := 1; n <= 20; n++ {
		parked := l.attempt(p, kinds[(n-1)%2], t0)
		if parked {
			t.Fatalf("attempt %d parked under alternating kinds — must never trip", n)
		}
		// Neither lane exceeds 1 under strict alternation.
		if l.strikes > 1 || l.altHits > 1 {
			t.Fatalf("attempt %d: lanes not mutually resetting: strikes=%d altHits=%d", n, l.strikes, l.altHits)
		}
	}
	if l.attempts != 20 {
		t.Fatalf("shared clock attempts = %d, want 20", l.attempts)
	}
	if want := t0.Add(bo.After(20)); l.nextDue != want {
		t.Fatalf("shared backoff clock not advanced: nextDue = %v, want %v", l.nextDue, want)
	}
}

// TestLedgerAltBreaker: the alt lane trips at alt when charged consecutively, and
// a primary attempt mid-run resets it so it must climb from zero again.
func TestLedgerAltBreaker(t *testing.T) {
	p := pol(t, "fuse.remount") // alt 6
	l := &ledger{}
	for n := 1; n <= 5; n++ {
		if l.attempt(p, attemptAlt, t0) {
			t.Fatalf("alt attempt %d parked early (alt breaker is 6)", n)
		}
	}
	if l.altHits != 5 {
		t.Fatalf("altHits = %d, want 5", l.altHits)
	}
	// A primary attempt resets the alt lane.
	l.attempt(p, attemptPrimary, t0)
	if l.altHits != 0 {
		t.Fatalf("primary attempt did not reset alt lane: altHits = %d", l.altHits)
	}
	// Now alt must climb from zero: 6 consecutive to trip.
	var parked bool
	for n := 1; n <= 6; n++ {
		parked = l.attempt(p, attemptAlt, t0)
	}
	if !parked || !l.parked(p) {
		t.Fatal("alt breaker did not trip after 6 consecutive alt attempts")
	}
}

// TestLedgerNeutralResetsBothLanes: a benign deferral advances the shared clock
// but resets both breaker lanes, so it can never reach a breaker.
func TestLedgerNeutralResetsBothLanes(t *testing.T) {
	p := pol(t, "fuse.remount")
	l := &ledger{}
	l.attempt(p, attemptPrimary, t0)
	l.attempt(p, attemptAlt, t0)
	l.strikes = 4 // pretend nearly tripped
	l.attempt(p, attemptNeutral, t0)
	if l.strikes != 0 || l.altHits != 0 {
		t.Fatalf("neutral did not reset lanes: strikes=%d altHits=%d", l.strikes, l.altHits)
	}
	if l.attempts != 3 {
		t.Fatalf("neutral did not advance shared clock: attempts = %d, want 3", l.attempts)
	}
}

// TestLedgersStoreDefaults: absent (p, resource) rows answer the healthy default
// — not faulted, not parked, due — with no allocation.
func TestLedgersStoreDefaults(t *testing.T) {
	p := pol(t, "fp.domain")
	ls := newLedgers()
	if ls.faulted(p, "acct-01") {
		t.Fatal("absent ledger reported faulted")
	}
	if ls.parked(p, "acct-01") {
		t.Fatal("absent ledger reported parked")
	}
	if !ls.due(p, "acct-01", t0) {
		t.Fatal("absent ledger not due (never attempted should be due)")
	}
	if ls.peek(p, "acct-01") != nil {
		t.Fatal("read path allocated a ledger for an absent resource")
	}
}

// TestLedgersStoreKeying: the store keys by (policy, resource) — same resource
// under different policies is independent, and clear drops exactly one row.
func TestLedgersStoreKeying(t *testing.T) {
	fp := pol(t, "fp.domain")
	fuse := pol(t, "fuse.remount")
	ls := newLedgers()
	ls.forceFault(fp, "acct-01", t0, errors.New("wedged"))
	ls.attempt(fuse, "acct-01", attemptPrimary, t0)
	if !ls.faulted(fp, "acct-01") {
		t.Fatal("fp.domain row not faulted")
	}
	if ls.faulted(fuse, "acct-01") {
		t.Fatal("fuse.remount row should not share the fp.domain fault")
	}
	ls.clear(fp, "acct-01")
	if ls.faulted(fp, "acct-01") {
		t.Fatal("clear did not drop the fp.domain row")
	}
	if ls.peek(fuse, "acct-01") == nil {
		t.Fatal("clear dropped the wrong policy's row")
	}
}

// TestLedgersSnapshot reflects live state, including derived parked, and omits a
// nil lastErr as an empty string.
func TestLedgersSnapshot(t *testing.T) {
	p := pol(t, "fuse.remount") // breaker 5
	ls := newLedgers()
	for i := 0; i < 5; i++ {
		ls.attempt(p, "acct-07", attemptPrimary, t0)
	}
	ls.attempt(p, "acct-08", attemptNeutral, t0)
	snaps := ls.snapshot()
	if len(snaps) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snaps))
	}
	by := map[string]ledgerSnapshot{}
	for _, s := range snaps {
		by[s.Resource] = s
	}
	if s := by["acct-07"]; !s.Parked || s.Strikes != 5 || s.Attempts != 5 || s.Policy != "fuse.remount" {
		t.Fatalf("acct-07 snapshot wrong: %+v", s)
	}
	if s := by["acct-08"]; s.Parked || s.Attempts != 1 || s.LastErr != "" {
		t.Fatalf("acct-08 snapshot wrong: %+v", s)
	}
}

// TestLedgersPrune drops a policy's rows whose resource keep rejects, and leaves
// other policies' rows untouched.
func TestLedgersPrune(t *testing.T) {
	fuse := pol(t, "fuse.remount")
	fp := pol(t, "fp.domain")
	ls := newLedgers()
	ls.attempt(fuse, "acct-01", attemptPrimary, t0)
	ls.attempt(fuse, "acct-02", attemptPrimary, t0)
	ls.forceFault(fp, "acct-01", t0, errors.New("x"))
	live := map[string]bool{"acct-01": true}
	ls.prune(fuse, func(resource string) bool { return live[resource] })
	if ls.peek(fuse, "acct-02") != nil {
		t.Fatal("prune kept a fuse row not in the live set")
	}
	if ls.peek(fuse, "acct-01") == nil {
		t.Fatal("prune dropped a live fuse row")
	}
	if ls.peek(fp, "acct-01") == nil {
		t.Fatal("prune touched another policy's row")
	}
}
