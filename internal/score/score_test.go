package score

import (
	"reflect"
	"testing"
	"time"
)

func TestScorePrefersMoreRemaining(t *testing.T) {
	now := time.Now()
	full := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 10, Util7d: 5}, now)
	drained := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 90, Util7d: 80}, now)
	if full.Score <= drained.Score {
		t.Fatalf("expected emptier account to score higher: full=%.2f drained=%.2f", full.Score, drained.Score)
	}
}

func TestRateLimitMakesUnavailable(t *testing.T) {
	now := time.Now()
	r := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 0, RateLimited: true}, now)
	if r.Available {
		t.Fatal("rate-limited account must be unavailable")
	}
	if r.Components.RateLimitPenalty != PenRateLimit {
		t.Fatalf("expected rate-limit penalty %v, got %v", PenRateLimit, r.Components.RateLimitPenalty)
	}
}

func TestStaleWhenOld(t *testing.T) {
	now := time.Now()
	r := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now.Add(-10 * time.Minute), Util5h: 0}, now)
	if !r.Stale {
		t.Fatal("old sample must be stale")
	}
}

// Between StaleAfter (90s) and DisplayStaleAfter (5m) a sample is penalized but
// not shown stale, so a ~180s daemon poll doesn't flash "stale".
func TestDisplayStaleDecoupledFromPenalty(t *testing.T) {
	now := time.Now()

	mid := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now.Add(-100 * time.Second), Util5h: 0}, now)
	if mid.Stale {
		t.Fatal("a 100s-old sample must not be display-stale (< DisplayStaleAfter)")
	}
	if mid.Components.StalePenalty != PenStale {
		t.Fatalf("a 100s-old sample must still take the scoring penalty, got %.1f", mid.Components.StalePenalty)
	}

	fresh := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now.Add(-30 * time.Second), Util5h: 0}, now)
	if fresh.Stale || fresh.Components.StalePenalty != 0 {
		t.Fatalf("a 30s-old sample must be neither penalized nor display-stale, got stale=%v pen=%.1f",
			fresh.Stale, fresh.Components.StalePenalty)
	}

	old := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now.Add(-6 * time.Minute), Util5h: 0}, now)
	if !old.Stale {
		t.Fatal("a 6m-old sample must be display-stale")
	}
}

func TestSessionPenalty(t *testing.T) {
	now := time.Now()
	idle := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 0}, now)
	busy := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 0, ActiveSessions: 3}, now)
	if diff := idle.Score - busy.Score; diff != WSession*3 {
		t.Fatalf("expected session penalty %.1f, got %.1f", WSession*3, diff)
	}
}

func TestRankTieBreakBySoonestReset(t *testing.T) {
	now := time.Now()
	inputs := []Input{
		{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 50, Util7d: 50, Resets5h: now.Add(2 * time.Hour)},
		{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 50, Util7d: 50, Resets5h: now.Add(1 * time.Hour)},
	}
	ranked := Rank(inputs, now)
	if ranked[0].AccountID != 2 {
		t.Fatalf("tie should break to soonest reset (acct 2), got acct %d", ranked[0].AccountID)
	}
}

func TestPickSkipsRateLimited(t *testing.T) {
	now := time.Now()
	inputs := []Input{
		{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 0, RateLimited: true},
		{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 30},
	}
	best, ok := Pick(Rank(inputs, now))
	if !ok || best.AccountID != 2 {
		t.Fatalf("expected to pick available acct 2, got ok=%v id=%d", ok, best.AccountID)
	}
}

func TestPickNoneWhenAllRateLimited(t *testing.T) {
	now := time.Now()
	inputs := []Input{
		{AccountID: 1, HasUsage: true, SampleTS: now, RateLimited: true},
		{AccountID: 2, HasUsage: true, SampleTS: now, RateLimited: true},
	}
	if _, ok := Pick(Rank(inputs, now)); ok {
		t.Fatal("expected no available account")
	}
}

func TestNeverSampledIsSelectableButPenalized(t *testing.T) {
	now := time.Now()
	known := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 20}, now)
	unknown := Score(Input{AccountID: 2, HasUsage: false}, now)
	if !unknown.Available {
		t.Fatal("never-sampled account should still be available")
	}
	if unknown.Score >= known.Score {
		t.Fatal("never-sampled account should score below a known-good one due to stale penalty")
	}
}

// Healthy inputs (far from reset, above the barrier knee, no burn) trip no
// guards, so the score is the exact baseline formula.
func TestHealthyEqualsBaseline(t *testing.T) {
	now := time.Now()
	in := Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 40, Util7d: 30}
	got := Score(in, now).Score
	want := W5h*(100-40) + W7d*(100-30)
	if got != want {
		t.Fatalf("healthy score = %.4f, want baseline %.4f", got, want)
	}
}

func TestImminentResetRanksUp(t *testing.T) {
	now := time.Now()
	imminent := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 80, Resets5h: now.Add(12 * time.Minute)}, now)
	far := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 80, Resets5h: now.Add(4 * time.Hour)}, now)
	if imminent.Score <= far.Score {
		t.Fatalf("about-to-reset account should rank up: imminent=%.2f far=%.2f", imminent.Score, far.Score)
	}
	if imminent.Components.Eff5 < 90 {
		t.Fatalf("imminent reset should lift eff5 near full, got %.1f", imminent.Components.Eff5)
	}
}

// Reset credit applies only within MaxResetCreditHorizon — a 7d reset days away
// earns none.
func TestSevenDayCreditCapped(t *testing.T) {
	now := time.Now()
	farReset := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util7d: 73, Resets7d: now.Add(59 * time.Hour)}, now)
	if got := farReset.Components.Eff7; got != 100-73 {
		t.Fatalf("7d reset days away should earn no credit: eff7 = %.1f, want plain remaining 27", got)
	}
	nearReset := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util7d: 73, Resets7d: now.Add(time.Hour)}, now)
	if nearReset.Components.Eff7 <= farReset.Components.Eff7 {
		t.Fatalf("a 7d reset within the horizon should lift eff7: near=%.1f far=%.1f", nearReset.Components.Eff7, farReset.Components.Eff7)
	}
}

// Without the barrier, the weighted sum would mask a nearly-exhausted 7d window
// behind 5h headroom.
func TestBarrierGuardsLowSevenDay(t *testing.T) {
	now := time.Now()
	lowWeekly := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 10, Util7d: 92}, now)
	balanced := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 40, Util7d: 40}, now)
	if lowWeekly.Components.Barrier7d == 0 {
		t.Fatal("expected a 7d barrier penalty for the nearly-exhausted weekly window")
	}
	if lowWeekly.Score >= balanced.Score {
		t.Fatalf("barrier should downrank the low-weekly account: low=%.2f balanced=%.2f", lowWeekly.Score, balanced.Score)
	}
}

func TestBurnRateRunwayDownranks(t *testing.T) {
	now := time.Now()
	draining := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 50, Burn5hPerHour: 20}, now)
	stable := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 50, Burn5hPerHour: 0}, now)
	if draining.Components.RunwayPenalty == 0 {
		t.Fatal("expected a runway penalty for the actively-draining account")
	}
	if draining.Score >= stable.Score {
		t.Fatalf("burn-rate should downrank the draining account: draining=%.2f stable=%.2f", draining.Score, stable.Score)
	}
}

func TestZeroKnobsReproduceBaseline(t *testing.T) {
	defer restoreKnobs(BarrierKnee, RunwayWeight)
	BarrierKnee, RunwayWeight = 0, 0
	now := time.Now()
	in := Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 95, Util7d: 96, Burn5hPerHour: 50}
	got := Score(in, now).Score
	want := W5h*(100-95) + W7d*(100-96)
	if got != want {
		t.Fatalf("with guards disabled, score = %.4f, want baseline %.4f", got, want)
	}
}

func restoreKnobs(knee, runway float64) { BarrierKnee, RunwayWeight = knee, runway }

func TestUsableForSticky(t *testing.T) {
	cases := []struct {
		name string
		r    Result
		want bool
	}{
		{"healthy", Result{Available: true, Components: Components{RawRemaining5h: 90}}, true},
		{"rate-limited despite headroom", Result{Available: false, Components: Components{RawRemaining5h: 90}}, false},
		{"just below floor", Result{Available: true, Components: Components{RawRemaining5h: StickyMinRemaining5h - 0.1}}, false},
		{"exactly at floor", Result{Available: true, Components: Components{RawRemaining5h: StickyMinRemaining5h}}, true},
		// The 2026-06-10 incident shape (see TestIncidentRegression20260610).
		{"exhausted despite high eff5", Result{
			Available: false, Exhausted: true,
			Components: Components{Eff5: 93, RawRemaining5h: 0},
		}, false},
		{"high eff cannot mask low raw", Result{
			Available:  true,
			Components: Components{Eff5: 95, RawRemaining5h: 5},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := UsableForSticky(tc.r); got != tc.want {
				t.Fatalf("UsableForSticky(%+v) = %v, want %v", tc.r, got, tc.want)
			}
		})
	}
}

func TestExhaustedGate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name          string
		in            Input
		wantExhausted bool
		wantAvailable bool
	}{
		{
			"pegged 5h, future reset",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100, Resets5h: now.Add(20 * time.Minute)},
			true, false,
		},
		{
			"pegged 5h, past reset (stale pre-poll sample)",
			Input{AccountID: 1, HasUsage: true, SampleTS: now.Add(-2 * time.Minute), Util5h: 100, Resets5h: now.Add(-time.Minute)},
			false, true,
		},
		{
			"pegged 5h, unknown reset",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100},
			false, true,
		},
		{
			"util 99 below threshold",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 99, Resets5h: now.Add(20 * time.Minute)},
			false, true,
		},
		{
			"pegged 7d, future reset",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util7d: 100, Resets7d: now.Add(24 * time.Hour)},
			true, false,
		},
		{
			"never sampled",
			Input{AccountID: 1, HasUsage: false},
			false, true,
		},
		{
			"rate-limited and exhausted",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100, Resets5h: now.Add(20 * time.Minute), RateLimited: true},
			true, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := Score(tc.in, now)
			if r.Exhausted != tc.wantExhausted || r.Available != tc.wantAvailable {
				t.Fatalf("exhausted=%v available=%v, want exhausted=%v available=%v",
					r.Exhausted, r.Available, tc.wantExhausted, tc.wantAvailable)
			}
			if r.Exhausted == r.ExhaustedUntil.IsZero() {
				t.Fatalf("ExhaustedUntil must be set exactly when exhausted: exhausted=%v until=%v",
					r.Exhausted, r.ExhaustedUntil)
			}
		})
	}
}

// Recovery is the latest reset among the windows that tripped the gate.
func TestExhaustedUntilBindingReset(t *testing.T) {
	now := time.Now()
	reset5, reset7 := now.Add(20*time.Minute), now.Add(3*24*time.Hour)

	sevenOnly := Score(Input{
		AccountID: 1, HasUsage: true, SampleTS: now,
		Util5h: 20, Util7d: 100, Resets5h: reset5, Resets7d: reset7,
	}, now)
	if !sevenOnly.Exhausted || !sevenOnly.ExhaustedUntil.Equal(reset7) {
		t.Fatalf("7d-only exhaustion must recover at the 7d reset: %+v", sevenOnly)
	}

	both := Score(Input{
		AccountID: 2, HasUsage: true, SampleTS: now,
		Util5h: 100, Util7d: 100, Resets5h: reset5, Resets7d: reset7,
	}, now)
	if !both.ExhaustedUntil.Equal(reset7) {
		t.Fatalf("both-windows exhaustion must recover at the LATEST reset, got %v", both.ExhaustedUntil)
	}

	fiveOnly := Score(Input{
		AccountID: 3, HasUsage: true, SampleTS: now,
		Util5h: 100, Util7d: 10, Resets5h: reset5, Resets7d: reset7,
	}, now)
	if !fiveOnly.ExhaustedUntil.Equal(reset5) {
		t.Fatalf("5h-only exhaustion must recover at the 5h reset, got %v", fiveOnly.ExhaustedUntil)
	}
}

// A stale pegged sample past its reset has already refilled: raw remaining
// self-lifts like the gate and windowFrac.
func TestRawRemainingSelfLiftsAtReset(t *testing.T) {
	now := time.Now()
	r := Score(Input{
		AccountID: 1, HasUsage: true, SampleTS: now.Add(-2 * time.Minute),
		Util5h: 100, Resets5h: now.Add(-time.Minute), Burn5hPerHour: 50,
	}, now)
	if r.Components.RawRemaining5h != 100 {
		t.Fatalf("raw remaining must self-lift at the reset, got %.1f", r.Components.RawRemaining5h)
	}
	// The burn-derived runway penalty legitimately remains — hence the same-burn control.
	full := Score(Input{
		AccountID: 1, HasUsage: true, SampleTS: now.Add(-2 * time.Minute),
		Util5h: 0, Burn5hPerHour: 50,
	}, now)
	if r.Components.Barrier5h != 0 || r.Components.RunwayPenalty != full.Components.RunwayPenalty {
		t.Fatalf("post-reset sample must score as a full window: barrier=%.1f runway=%.1f want runway=%.1f",
			r.Components.Barrier5h, r.Components.RunwayPenalty, full.Components.RunwayPenalty)
	}
	if !UsableForSticky(r) {
		t.Fatal("a sticky pin must survive the post-reset poll gap")
	}
	pre := Score(Input{
		AccountID: 2, HasUsage: true, SampleTS: now,
		Util5h: 100, Resets5h: now.Add(time.Minute), Burn5hPerHour: 50,
	}, now)
	if pre.Components.RawRemaining5h != 0 || pre.Components.Barrier5h != BarrierKnee {
		t.Fatalf("pre-reset pegged sample must keep raw=0/full barrier: %+v", pre.Components)
	}
}

// Reset credit (eff5≈93) must not mask zero current headroom: a pegged window
// takes the full barrier despite an imminent reset.
func TestBarrierOnRawRemaining(t *testing.T) {
	now := time.Now()
	pegged := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100, Resets5h: now.Add(21 * time.Minute)}, now)
	if pegged.Components.Barrier5h != BarrierKnee {
		t.Fatalf("pegged window must take the full barrier %v, got %v", BarrierKnee, pegged.Components.Barrier5h)
	}
	if pegged.Components.Eff5 <= 90 {
		t.Fatalf("precondition: imminent reset should keep eff5 high (got %.1f) — otherwise this test proves nothing", pegged.Components.Eff5)
	}
	healthy := Score(Input{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 40, Resets5h: now.Add(21 * time.Minute)}, now)
	if healthy.Components.Barrier5h != 0 {
		t.Fatalf("healthy window must take no barrier, got %v", healthy.Components.Barrier5h)
	}
}

// Time-to-wall is raw remaining over burn, not the reset-credited eff5.
func TestRunwayUsesRawRemaining(t *testing.T) {
	now := time.Now()
	r := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 95, Resets5h: now.Add(10 * time.Minute), Burn5hPerHour: 30}, now)
	want := RunwayWeight * (1 - (5.0/30.0)/RunwayHorizon.Hours())
	if got := r.Components.RunwayPenalty; got != want {
		t.Fatalf("runway penalty = %.4f, want %.4f (raw-remaining based)", got, want)
	}
}

func TestPickFallback(t *testing.T) {
	now := time.Now()
	reset := now.Add(20 * time.Minute)
	t.Run("prefers best exhausted over rate-limited", func(t *testing.T) {
		inputs := []Input{
			{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100, Util7d: 90, Resets5h: reset},
			{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 100, Util7d: 10, Resets5h: reset},
			{AccountID: 3, HasUsage: true, SampleTS: now, Util5h: 0, RateLimited: true},
		}
		ranked := Rank(inputs, now)
		if _, ok := Pick(ranked); ok {
			t.Fatal("precondition: no account should be available")
		}
		fb, ok := PickFallback(ranked)
		if !ok || fb.AccountID != 2 {
			t.Fatalf("expected fallback to best exhausted acct 2, got ok=%v id=%d", ok, fb.AccountID)
		}
	})
	t.Run("none when all rate-limited", func(t *testing.T) {
		inputs := []Input{
			{AccountID: 1, HasUsage: true, SampleTS: now, RateLimited: true},
			{AccountID: 2, HasUsage: true, SampleTS: now, RateLimited: true},
		}
		if _, ok := PickFallback(Rank(inputs, now)); ok {
			t.Fatal("expected no fallback when every account is rate-limited")
		}
	})
}

// TestIncidentRegression20260610 replays the real 2026-06-10 05:18 selection
// (from ~/.cc-pool/pool-v1.db) where acct-1 (100% 5h-used, reset 21m out) outranked
// acct-2 (31% used) via reset credit and was launched, silently billing
// extra-usage credits. It must never be picked again.
func TestIncidentRegression20260610(t *testing.T) {
	now := time.Now()
	in21m, in2h42m, in4h41m := now.Add(21*time.Minute), now.Add(2*time.Hour+42*time.Minute), now.Add(4*time.Hour+41*time.Minute)
	nextDay := now.Add(24*time.Hour + 41*time.Minute)
	incident := func(sessions1, sessions2, sessions3 int) []Input {
		return []Input{
			{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100, Util7d: 21, Resets5h: in21m, Resets7d: nextDay, ActiveSessions: sessions1},
			{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 31, Util7d: 6, Resets5h: in2h42m, Resets7d: now.Add(7*time.Hour + 41*time.Minute), ActiveSessions: sessions2},
			{AccountID: 3, HasUsage: true, SampleTS: now, Util5h: 40, Util7d: 8, Resets5h: in21m, Resets7d: in4h41m, ActiveSessions: sessions3},
		}
	}

	ranked := Rank(incident(0, 0, 0), now)
	best, ok := Pick(ranked)
	if !ok || best.AccountID == 1 {
		t.Fatalf("exhausted acct-1 must never win: ok=%v picked=%d", ok, best.AccountID)
	}
	if best.AccountID != 3 {
		t.Fatalf("expected acct-3 (highest headroom) to win, got acct-%d", best.AccountID)
	}
	for _, r := range ranked {
		if r.AccountID == 1 {
			if !r.Exhausted || r.Available {
				t.Fatalf("acct-1 must be exhausted+unavailable, got %+v", r)
			}
			// Defense in depth: even pre-gate, barrier-on-raw must rank it last.
			for _, other := range ranked {
				if other.AccountID != 1 && other.Score <= r.Score {
					t.Fatalf("acct-1 (%.1f) must score below acct-%d (%.1f) via the raw barrier alone",
						r.Score, other.AccountID, other.Score)
				}
			}
		}
	}

	// Heavy sessions on the healthy accounts (the real tiebreaker that night)
	// still must not route to the exhausted one.
	best, ok = Pick(Rank(incident(0, 4, 6), now))
	if !ok || best.AccountID == 1 {
		t.Fatalf("exhausted acct-1 must never win even under session pressure: ok=%v picked=%d", ok, best.AccountID)
	}

	// Sticky pin on acct-1 (cc-skills had one) must be abandoned.
	for _, r := range Rank(incident(0, 0, 0), now) {
		if r.AccountID == 1 && UsableForSticky(r) {
			t.Fatal("sticky pin on the exhausted account must be abandoned")
		}
	}
}

// TestScoreNeedsLoginExcluded pins that a needs-login account has no valid
// token, so it is unavailable and skipped by both Pick and PickFallback even at
// full raw headroom.
func TestScoreNeedsLoginExcluded(t *testing.T) {
	now := time.Now()
	r := Score(Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 0, NeedsLogin: true}, now)
	if r.Available {
		t.Fatal("a needs-login account must be unavailable")
	}
	if !r.NeedsLogin || r.Components.NeedsLoginPenalty != PenNeedsLogin {
		t.Fatalf("needs-login penalty not recorded: %+v", r)
	}

	inputs := []Input{
		{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 0, NeedsLogin: true},
		{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 50},
	}
	ranked := Rank(inputs, now)
	best, ok := Pick(ranked)
	if !ok || best.AccountID != 2 {
		t.Fatalf("Pick = id %d ok=%v, want the healthy acct 2", best.AccountID, ok)
	}

	onlyFlagged := Rank([]Input{{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 0, NeedsLogin: true}}, now)
	if _, ok := Pick(onlyFlagged); ok {
		t.Fatal("Pick must find nothing when the only account needs login")
	}
	if _, ok := PickFallback(onlyFlagged); ok {
		t.Fatal("PickFallback must skip a needs-login account")
	}
}

func TestScoreCredentialQuarantineExcluded(t *testing.T) {
	now := time.Now()
	quarantined := Input{
		AccountID: 1, HasUsage: true, SampleTS: now,
		CredentialQuarantined: true,
	}
	r := Score(quarantined, now)
	if r.Available || !r.CredentialQuarantined ||
		r.Components.CredentialQuarantinePenalty != PenCredentialQuarantine {
		t.Fatalf("quarantined score = %+v", r)
	}
	ranked := Rank([]Input{
		quarantined,
		{AccountID: 2, HasUsage: true, SampleTS: now, Util5h: 50},
	}, now)
	if best, ok := Pick(ranked); !ok || best.AccountID != 2 {
		t.Fatalf("Pick = %+v ok=%t, want healthy account 2", best, ok)
	}
	if _, ok := PickFallback(Rank([]Input{quarantined}, now)); ok {
		t.Fatal("PickFallback selected a credential-quarantined account")
	}
}

// The model-scoped weekly bucket min-folds into the 7d term: a scoped bucket
// emptier than the aggregate binds Eff7/RawRemaining7d/Barrier7d; a fuller one
// is inert.
func TestScopedWeeklyBindsSevenDayTerm(t *testing.T) {
	now := time.Now()
	base := Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 10, Util7d: 50}
	baseline := Score(base, now)

	cases := []struct {
		name       string
		scopedUtil float64
		wantBinds  bool
	}{
		{"scoped above aggregate binds", 90, true},
		{"scoped below aggregate is inert", 20, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			in.HasScoped7d = true
			in.Util7dScoped = tc.scopedUtil
			got := Score(in, now)
			if !tc.wantBinds {
				if !reflect.DeepEqual(got, baseline) {
					t.Fatalf("a non-binding scoped bucket must not change the result:\ngot  %+v\nwant %+v", got, baseline)
				}
				return
			}
			// Zero resets ⇒ no credit or self-lift: eff == raw == 100 − util.
			want := 100 - tc.scopedUtil
			if got.Components.Eff7 != want {
				t.Fatalf("Eff7 = %.1f, want scoped-bound %.1f", got.Components.Eff7, want)
			}
			if got.Components.RawRemaining7d != want {
				t.Fatalf("RawRemaining7d = %.1f, want scoped-bound %.1f", got.Components.RawRemaining7d, want)
			}
			if got.Components.Barrier7d <= baseline.Components.Barrier7d {
				t.Fatalf("scoped bucket below the knee must raise Barrier7d: got %.1f, baseline %.1f",
					got.Components.Barrier7d, baseline.Components.Barrier7d)
			}
			if got.Score >= baseline.Score {
				t.Fatalf("binding scoped bucket must downrank: got %.2f, baseline %.2f", got.Score, baseline.Score)
			}
		})
	}
}

// HasScoped7d alone guards the fold: stale garbage in the scoped fields is
// ignored when the flag is false.
func TestScopedAbsentIsIdentity(t *testing.T) {
	now := time.Now()
	base := Input{
		AccountID: 1, HasUsage: true, SampleTS: now,
		Util5h: 30, Util7d: 40, Resets5h: now.Add(2 * time.Hour), Resets7d: now.Add(3 * 24 * time.Hour),
	}
	garbage := base
	garbage.Util7dScoped = 100
	garbage.Resets7dScoped = now.Add(24 * time.Hour)
	got, want := Score(garbage, now), Score(base, now)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HasScoped7d=false must be identity:\ngot  %+v\nwant %+v", got, want)
	}
	if got.WeeklyExhausted {
		t.Fatal("garbage scoped fields must not trip WeeklyExhausted when HasScoped7d=false")
	}
}

func TestScopedWeeklyExhaustionGate(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name          string
		util          float64
		reset         time.Time
		wantExhausted bool
	}{
		{"pegged scoped, future reset", 100, now.Add(2 * 24 * time.Hour), true},
		{"pegged scoped, past reset self-lifts", 100, now.Add(-time.Minute), false},
		{"scoped util 99 below threshold", 99, now.Add(2 * 24 * time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := Score(Input{
				AccountID: 1, HasUsage: true, SampleTS: now,
				HasScoped7d: true, Util7dScoped: tc.util, Resets7dScoped: tc.reset,
			}, now)
			if r.Exhausted != tc.wantExhausted || r.Available == tc.wantExhausted {
				t.Fatalf("exhausted=%v available=%v, want exhausted=%v available=%v",
					r.Exhausted, r.Available, tc.wantExhausted, !tc.wantExhausted)
			}
			if r.WeeklyExhausted != tc.wantExhausted {
				t.Fatalf("WeeklyExhausted = %v, want %v", r.WeeklyExhausted, tc.wantExhausted)
			}
			if tc.wantExhausted && !r.ExhaustedUntil.Equal(tc.reset) {
				t.Fatalf("ExhaustedUntil = %v, want the scoped reset %v", r.ExhaustedUntil, tc.reset)
			}
			if !tc.wantExhausted && !r.ExhaustedUntil.IsZero() {
				t.Fatalf("ExhaustedUntil must stay zero when not exhausted, got %v", r.ExhaustedUntil)
			}
			if !tc.reset.After(now) {
				// The raw term must self-lift once the scoped reset passes: a
				// stale pegged sample may not impose a phantom barrier on a
				// bucket that has actually refilled.
				if r.Components.RawRemaining7d != 100 {
					t.Fatalf("RawRemaining7d = %v, want 100 after the scoped reset passed", r.Components.RawRemaining7d)
				}
				if r.Components.Barrier7d != 0 {
					t.Fatalf("Barrier7d = %v, want 0 after the scoped reset passed", r.Components.Barrier7d)
				}
			}
		})
	}
}

// Recovery is the latest reset among the tripping windows, now including the
// scoped bucket's own reset.
func TestScopedExhaustedUntilLatest(t *testing.T) {
	now := time.Now()
	reset5, reset7, resetScoped := now.Add(20*time.Minute), now.Add(2*24*time.Hour), now.Add(5*24*time.Hour)
	r := Score(Input{
		AccountID: 1, HasUsage: true, SampleTS: now,
		Util5h: 100, Util7d: 100, Resets5h: reset5, Resets7d: reset7,
		HasScoped7d: true, Util7dScoped: 100, Resets7dScoped: resetScoped,
	}, now)
	if !r.Exhausted {
		t.Fatal("precondition: all three windows must trip the gate")
	}
	if !r.ExhaustedUntil.Equal(resetScoped) {
		t.Fatalf("ExhaustedUntil = %v, want the latest (scoped) reset %v", r.ExhaustedUntil, resetScoped)
	}
}

// A scoped reset within MaxResetCreditHorizon earns windowFrac credit against
// the scoped drag; one beyond the horizon earns none (plain remaining).
func TestScopedImminentResetLifts(t *testing.T) {
	now := time.Now()
	scoped := func(reset time.Time) Input {
		return Input{
			AccountID: 1, HasUsage: true, SampleTS: now,
			HasScoped7d: true, Util7dScoped: 80, Resets7dScoped: reset,
		}
	}
	far := Score(scoped(now.Add(59*time.Hour)), now)
	if got := far.Components.Eff7; got != 100-80 {
		t.Fatalf("scoped reset beyond the horizon must earn no credit: Eff7 = %.1f, want 20", got)
	}
	near := Score(scoped(now.Add(time.Hour)), now)
	if near.Components.Eff7 <= far.Components.Eff7 {
		t.Fatalf("imminent scoped reset must lift Eff7: near=%.1f far=%.1f",
			near.Components.Eff7, far.Components.Eff7)
	}
	if near.Score <= far.Score {
		t.Fatalf("imminent scoped reset must rank up: near=%.2f far=%.2f", near.Score, far.Score)
	}
}

// WeeklyExhausted fires only for weekly windows — aggregate or model-scoped —
// never for the 5h window alone.
func TestWeeklyExhausted(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name          string
		in            Input
		wantWeekly    bool
		wantExhausted bool
	}{
		{
			"aggregate weekly trip",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util7d: 100, Resets7d: now.Add(2 * 24 * time.Hour)},
			true, true,
		},
		{
			"scoped-only trip",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, HasScoped7d: true, Util7dScoped: 100, Resets7dScoped: now.Add(2 * 24 * time.Hour)},
			true, true,
		},
		{
			"5h-only trip",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 100, Resets5h: now.Add(20 * time.Minute)},
			false, true,
		},
		{
			"healthy",
			Input{AccountID: 1, HasUsage: true, SampleTS: now, Util5h: 40, Util7d: 30},
			false, false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := Score(tc.in, now)
			if r.WeeklyExhausted != tc.wantWeekly || r.Exhausted != tc.wantExhausted {
				t.Fatalf("WeeklyExhausted=%v Exhausted=%v, want WeeklyExhausted=%v Exhausted=%v",
					r.WeeklyExhausted, r.Exhausted, tc.wantWeekly, tc.wantExhausted)
			}
		})
	}
}
