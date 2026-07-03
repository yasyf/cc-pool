// Package score implements cc-pool's account-selection scoring. Higher is
// better; select picks argmax across the whole pool — there are no roles.
package score

import (
	"sort"
	"time"
)

// Scoring knobs; vars so tests can tune or disable them.
var (
	W5h          = 0.70
	W7d          = 0.25
	WSession     = 2.00
	PenRateLimit = 100.0
	// PenNeedsLogin only orders needs-login accounts among themselves:
	// Available=false and PickFallback already exclude them.
	PenNeedsLogin = 100.0
	PenStale      = 20.0

	// StaleAfter is the sample age past which the selection penalty engages;
	// uniform across the pool between polls, so it does not skew ranking.
	StaleAfter = 90 * time.Second

	// DisplayStaleAfter is the sample age past which status renders an account
	// "stale"; must exceed the daemon's worst-case poll gap (basePollInterval
	// 180s + pollJitter 30s) so a healthy account is never shown stale.
	DisplayStaleAfter = 5 * time.Minute

	// FiveHourWindow / SevenDayWindow are the window lengths used to credit
	// depletion against an imminent reset, capped at MaxResetCreditHorizon.
	FiveHourWindow = 5 * time.Hour
	SevenDayWindow = 7 * 24 * time.Hour

	// MaxResetCreditHorizon caps how far ahead a reset earns credit, independent
	// of window length: a weekly reset only lifts headroom when it lands within
	// one 5h session; the 5h window (already ≤ the cap) is unchanged.
	MaxResetCreditHorizon = 5 * time.Hour

	// BarrierKnee is the remaining-% below which a convex low-headroom penalty
	// kicks in (so a nearly-exhausted window can't be masked by the other).
	BarrierKnee = 20.0

	// ExhaustedAtUtil is the utilization percent at or above which a window is
	// treated as fully exhausted while its reset is still future. The API reports
	// plan-window utilization in integer percent, so only a reported 100 trips it.
	ExhaustedAtUtil = 99.5

	// RunwayWeight / RunwayHorizon shape the burn-rate term: an account draining
	// its 5h headroom within RunwayHorizon is downranked up to RunwayWeight points.
	RunwayWeight  = 15.0
	RunwayHorizon = 5 * time.Hour

	// StickyMinRemaining5h is the raw-5h-remaining floor (percent) below which a
	// sticky selection is abandoned as worthless. Raw, not reset-aware: an
	// imminent reset must not pin an account that cannot serve until it resets.
	StickyMinRemaining5h = 10.0
)

// Input is everything the scorer needs about one account.
type Input struct {
	AccountID int

	HasUsage bool      // false if we have never sampled this account
	SampleTS time.Time // when the latest usage sample was taken
	Util5h   float64   // percent used 0..100 of the 5-hour window
	Util7d   float64   // percent used 0..100 of the 7-day window
	Resets5h time.Time // when the 5-hour window resets (also the tie-break)
	Resets7d time.Time // when the 7-day window resets

	// HasScoped7d reports whether a model-scoped weekly bucket was sampled for
	// this account; false leaves the 7d term untouched (the scoped fields below
	// are ignored).
	HasScoped7d bool
	// Util7dScoped is the percent used 0..100 of the model-scoped weekly bucket.
	Util7dScoped float64
	// Resets7dScoped is when the model-scoped weekly bucket resets.
	Resets7dScoped time.Time

	// Burn5hPerHour is the recent rate of change of util_5h in percent/hour,
	// from usage history. Zero means unknown or idle (no runway penalty).
	Burn5hPerHour float64

	ActiveSessions int
	RateLimited    bool // a live 429 / rate-limit observed
	RefreshFailed  bool // the most recent refresh attempt failed
	NeedsLogin     bool // the daemon flagged the account: refresh token gone/revoked
}

// staleAfter reports stale if the sample is older than d, refresh failed, or the
// account was never sampled; the latter two hold under any threshold.
func (in Input) staleAfter(now time.Time, d time.Duration) bool {
	if in.RefreshFailed {
		return true
	}
	if !in.HasUsage {
		return true
	}
	return now.Sub(in.SampleTS) > d
}

// Components is the per-term breakdown, for `status` and debugging.
type Components struct {
	Eff5              float64 // reset-aware effective 5h remaining
	Eff7              float64 // reset-aware effective 7d remaining
	RawRemaining5h    float64 // 100 − util5, no reset credit (drives barriers/runway/sticky)
	RawRemaining7d    float64 // 100 − util7
	Remaining5h       float64 // W5h · Eff5 (weighted contribution)
	Remaining7d       float64 // W7d · Eff7
	SessionPenalty    float64
	RateLimitPenalty  float64
	NeedsLoginPenalty float64
	StalePenalty      float64
	Barrier5h         float64
	Barrier7d         float64
	RunwayPenalty     float64
}

// Result is a scored account.
type Result struct {
	AccountID   int
	Score       float64
	Components  Components
	Resets5h    time.Time
	Stale       bool
	RateLimited bool
	NeedsLogin  bool // refresh token gone/revoked; only `ccp login` recovers it
	Exhausted   bool // a window is fully used and its reset is still pending
	// ExhaustedUntil is when the account actually recovers: the latest reset
	// among the windows that tripped the gate. Zero unless Exhausted.
	ExhaustedUntil time.Time
	// WeeklyExhausted reports that a weekly window — aggregate or model-scoped —
	// is pegged with its reset pending; the single source for the pool-mood
	// signal.
	WeeklyExhausted bool
	Available       bool // false if rate-limited or exhausted (cannot serve right now)
}

// Score computes the score for one account: weighted 5h+7d remaining minus
// penalties. The reset-aware, barrier, and runway terms only engage near limits.
func Score(in Input, now time.Time) Result {
	util5, util7 := in.Util5h, in.Util7d
	if !in.HasUsage {
		// Unknown usage: assume empty so it stays selectable; the stale penalty
		// keeps it behind known-good accounts.
		util5, util7 = 0, 0
	}

	eff5 := 100 - windowFrac(in.Resets5h, now, FiveHourWindow)*util5
	eff7 := 100 - windowFrac(in.Resets7d, now, SevenDayWindow)*util7

	// Raw remaining self-lifts once the reset passes: a stale pegged sample from
	// before the reset must not barrier or sticky-floor a window that refilled.
	raw5, raw7 := 100-util5, 100-util7
	if resetPassed(in.Resets5h, now) {
		raw5 = 100
	}
	if resetPassed(in.Resets7d, now) {
		raw7 = 100
	}

	// A model-scoped weekly bucket is a second cap on the same 7d horizon: the
	// binding (lower-headroom) one drives the term, so eff7/raw7 min-fold before
	// they feed Components and the barrier. Each window uses its own reset for
	// credit, self-lift, and exhaustion.
	exhausted7Scoped := false
	if in.HasScoped7d {
		utilS := in.Util7dScoped
		if !in.HasUsage {
			utilS = 0
		}
		effS := 100 - windowFrac(in.Resets7dScoped, now, SevenDayWindow)*utilS
		rawS := 100 - utilS
		if resetPassed(in.Resets7dScoped, now) {
			rawS = 100
		}
		if effS < eff7 {
			eff7 = effS
		}
		if rawS < raw7 {
			raw7 = rawS
		}
		exhausted7Scoped = in.HasUsage && utilS >= ExhaustedAtUtil && now.Before(in.Resets7dScoped)
	}

	// A pegged window with a pending reset can't serve now, and forward-looking
	// reset credit must not mask it. now.Before(zero) is false, so an unknown
	// reset never gates; a stale post-reset sample self-lifts at the reset; a
	// stale pre-reset sample gating is sound (util can't drop within a window).
	exhausted5 := in.HasUsage && util5 >= ExhaustedAtUtil && now.Before(in.Resets5h)
	exhausted7 := in.HasUsage && util7 >= ExhaustedAtUtil && now.Before(in.Resets7d)
	exhausted := exhausted5 || exhausted7 || exhausted7Scoped
	var exhaustedUntil time.Time
	if exhausted5 {
		exhaustedUntil = in.Resets5h
	}
	if exhausted7 && in.Resets7d.After(exhaustedUntil) {
		exhaustedUntil = in.Resets7d
	}
	if exhausted7Scoped && in.Resets7dScoped.After(exhaustedUntil) {
		exhaustedUntil = in.Resets7dScoped
	}

	c := Components{
		Eff5:           eff5,
		Eff7:           eff7,
		RawRemaining5h: raw5,
		RawRemaining7d: raw7,
		Remaining5h:    W5h * eff5,
		Remaining7d:    W7d * eff7,
		SessionPenalty: WSession * float64(in.ActiveSessions),
		Barrier5h:      barrier(raw5),
		Barrier7d:      barrier(raw7),
		RunwayPenalty:  runwayPenalty(raw5, in.Burn5hPerHour),
	}
	if in.RateLimited {
		c.RateLimitPenalty = PenRateLimit
	}
	if in.NeedsLogin {
		c.NeedsLoginPenalty = PenNeedsLogin
	}
	if in.staleAfter(now, StaleAfter) {
		c.StalePenalty = PenStale
	}
	stale := in.staleAfter(now, DisplayStaleAfter)

	score := c.Remaining5h + c.Remaining7d -
		c.SessionPenalty - c.RateLimitPenalty - c.NeedsLoginPenalty - c.StalePenalty -
		c.Barrier5h - c.Barrier7d - c.RunwayPenalty
	return Result{
		AccountID:       in.AccountID,
		Score:           score,
		Components:      c,
		Resets5h:        in.Resets5h,
		Stale:           stale,
		RateLimited:     in.RateLimited,
		NeedsLogin:      in.NeedsLogin,
		Exhausted:       exhausted,
		ExhaustedUntil:  exhaustedUntil,
		WeeklyExhausted: exhausted7 || exhausted7Scoped,
		Available:       !in.RateLimited && !exhausted && !in.NeedsLogin,
	}
}

// resetPassed reports whether a known reset time has already elapsed — the
// window has refilled even if the latest sample predates it.
func resetPassed(resetsAt, now time.Time) bool {
	return !resetsAt.IsZero() && !now.Before(resetsAt)
}

// windowFrac is the fraction of the credit horizon still ahead before the reset,
// in [0,1]: a zero/far reset returns 1 (depletion counts fully), an imminent one
// ~0 (about to refill). The horizon is min(window, MaxResetCreditHorizon), so a
// reset beyond the cap earns no credit.
func windowFrac(resetsAt, now time.Time, window time.Duration) float64 {
	if resetsAt.IsZero() || window <= 0 {
		return 1
	}
	horizon := window
	if horizon > MaxResetCreditHorizon {
		horizon = MaxResetCreditHorizon
	}
	frac := resetsAt.Sub(now).Seconds() / horizon.Seconds()
	switch {
	case frac < 0:
		return 0
	case frac > 1:
		return 1
	default:
		return frac
	}
}

// barrier is a low-headroom penalty: zero above BarrierKnee, rising linearly to
// BarrierKnee as raw remaining approaches zero. Raw, not reset-credited eff — an
// imminent reset must not mask a window nearly empty right now.
func barrier(remaining float64) float64 {
	if remaining >= BarrierKnee {
		return 0
	}
	if remaining < 0 {
		remaining = 0
	}
	return BarrierKnee - remaining
}

// runwayPenalty downranks an account being drained: if its raw 5h headroom would
// be exhausted within RunwayHorizon at the current burn rate, penalize up to
// RunwayWeight points. Raw, not eff (time-to-wall is real remaining over burn).
// Zero/negative burn (idle or unknown) → 0.
func runwayPenalty(remaining5h, burnPerHour float64) float64 {
	if burnPerHour <= 0 || RunwayWeight <= 0 || RunwayHorizon <= 0 {
		return 0
	}
	runwayHours := remaining5h / burnPerHour
	horizon := RunwayHorizon.Hours()
	frac := 1 - runwayHours/horizon
	if frac <= 0 {
		return 0
	}
	if frac > 1 {
		frac = 1
	}
	return RunwayWeight * frac
}

// UsableForSticky reports whether a previously-selected account can keep serving
// a sticky session: available and at least StickyMinRemaining5h raw 5h headroom.
func UsableForSticky(r Result) bool {
	return r.Available && r.Components.RawRemaining5h >= StickyMinRemaining5h
}

// Rank scores all inputs and returns results sorted best-first, breaking ties
// toward the soonest 5h reset (freshest to drain first); zero Resets5h sorts last.
func Rank(inputs []Input, now time.Time) []Result {
	results := make([]Result, len(inputs))
	for i, in := range inputs {
		results[i] = Score(in, now)
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return resetsBefore(results[i].Resets5h, results[j].Resets5h)
	})
	return results
}

// resetsBefore orders earlier non-zero reset times first; zero (unknown) last.
func resetsBefore(a, b time.Time) bool {
	switch {
	case a.IsZero() && b.IsZero():
		return false
	case a.IsZero():
		return false
	case b.IsZero():
		return true
	default:
		return a.Before(b)
	}
}

// Pick returns the best available account from ranked results; ok=false if none
// is available or the pool is empty.
func Pick(ranked []Result) (Result, bool) {
	for _, r := range ranked {
		if r.Available {
			return r, true
		}
	}
	return Result{}, false
}

// PickFallback returns the best account that is not rate-limited or needs-login —
// the least-bad pick when Pick found nothing. An exhausted account can still
// serve (billing extra-usage credits or waiting out its reset); rate-limited and
// needs-login ones cannot. ok=false if all are rate-limited/needs-login or empty.
func PickFallback(ranked []Result) (Result, bool) {
	for _, r := range ranked {
		if !r.RateLimited && !r.NeedsLogin {
			return r, true
		}
	}
	return Result{}, false
}

// SoonestReset returns the earliest non-zero 5h reset across results, used by
// `select --wait`. ok=false if no reset time is known.
func SoonestReset(results []Result) (time.Time, bool) {
	var best time.Time
	for _, r := range results {
		if r.Resets5h.IsZero() {
			continue
		}
		if best.IsZero() || r.Resets5h.Before(best) {
			best = r.Resets5h
		}
	}
	return best, !best.IsZero()
}
