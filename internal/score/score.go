// Package score implements cc-pool's account-selection scoring. Higher is
// better; select picks argmax. There are no roles — the best account across the
// whole pool wins.
package score

import (
	"sort"
	"time"
)

// Scoring coefficients and knobs. All are package vars so they can be tuned or
// disabled in tests. Setting BarrierKnee=0 and RunwayWeight=0 reduces the score
// to the exact baseline 0.70·rem5 + 0.25·rem7 − 2·sessions − 100·rl − 20·stale.
var (
	W5h          = 0.70
	W7d          = 0.25
	WSession     = 2.00
	PenRateLimit = 100.0
	// PenNeedsLogin matches PenRateLimit: a signed-out account also cannot serve.
	// It is paired with Available=false (and PickFallback skipping it), so the
	// penalty only orders needs-login accounts among themselves.
	PenNeedsLogin = 100.0
	PenStale      = 20.0

	// StaleAfter is the sample age past which the selection penalty engages. It
	// is deliberately short so select prefers the freshest-sampled account; it is
	// uniform across the pool between polls, so it does not skew ranking.
	StaleAfter = 90 * time.Second

	// DisplayStaleAfter is the sample age past which status renders an account as
	// "stale". It must exceed the daemon's worst-case normal poll gap
	// (daemon.basePollInterval 180s + pollJitter 30s = 210s) so a healthy,
	// regularly-polled account is never shown stale, while an account genuinely
	// stuck in rate-limit backoff (whose data really is old) still surfaces.
	DisplayStaleAfter = 5 * time.Minute

	// FiveHourWindow / SevenDayWindow are the window lengths used to credit an
	// imminent reset: depletion is discounted by how much of the window is still
	// ahead before it refills, but never further ahead than MaxResetCreditHorizon.
	FiveHourWindow = 5 * time.Hour
	SevenDayWindow = 7 * 24 * time.Hour

	// MaxResetCreditHorizon caps how far ahead a reset earns credit, independent
	// of window length. Without it the 7-day window credits depletion linearly
	// across its 168h, so a reset 2.5 days out still forgives ~65% of weekly
	// usage. Capping at one 5-hour session means a weekly reset only lifts
	// headroom when it lands within the span of work about to start; the 5h
	// window (already ≤ the cap) is unchanged.
	MaxResetCreditHorizon = 5 * time.Hour

	// BarrierKnee is the remaining-% below which a convex low-headroom penalty
	// kicks in (so a nearly-exhausted window can't be masked by the other).
	BarrierKnee = 20.0

	// ExhaustedAtUtil is the utilization (percent) at or above which a window is
	// treated as fully exhausted while its reset is still in the future. An
	// exhausted account is unavailable: launching on it either silently bills
	// pay-as-you-go extra-usage credits or rate-limits immediately. The API
	// reports plan-window utilization in integer percent, so only a reported 100
	// trips this.
	ExhaustedAtUtil = 99.5

	// RunwayWeight / RunwayHorizon shape the burn-rate term: an account whose
	// effective 5h headroom would be drained within RunwayHorizon is downranked,
	// up to RunwayWeight points.
	RunwayWeight  = 15.0
	RunwayHorizon = 5 * time.Hour

	// StickyMinRemaining5h is the raw-5h-remaining floor (percent) below which a
	// sticky selection is abandoned: with this little headroom right now the
	// resumed session would hit the limit anyway, so cache continuity is
	// worthless. Raw, not reset-aware: an imminent reset must not keep a pin on
	// an account that cannot serve until it actually resets.
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

	// Burn5hPerHour is the recent rate of change of util_5h in percent/hour,
	// from usage history. Zero means unknown or idle (no runway penalty).
	Burn5hPerHour float64

	ActiveSessions int
	RateLimited    bool // a live 429 / rate-limit observed
	RefreshFailed  bool // the most recent refresh attempt failed
	NeedsLogin     bool // the daemon flagged the account: refresh token gone/revoked
}

// staleAfter reports whether this account's data is older than d, its refresh
// failed, or it was never sampled. A failed refresh or never-sampled account is
// stale under any threshold. Two thresholds use this: StaleAfter for the
// selection penalty and DisplayStaleAfter for the status "stale" flag.
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
	Available      bool // false if rate-limited or exhausted (cannot serve right now)
}

// Score computes the score for one account. For a healthy account (windows far
// from a reset and well above the barrier knee, no measured burn) it equals the
// baseline 0.70·rem5 + 0.25·rem7 − penalties; the reset-aware, barrier, and
// runway terms only engage near limits.
func Score(in Input, now time.Time) Result {
	util5, util7 := in.Util5h, in.Util7d
	if !in.HasUsage {
		// Unknown usage: assume empty so a never-sampled account is still
		// selectable, but the stale penalty keeps it behind known-good ones.
		util5, util7 = 0, 0
	}

	// Reset-aware effective remaining: discount depletion by how much of the
	// credit horizon is still ahead before the window refills. frac=1 (reset
	// beyond the horizon or unknown) gives the plain remaining; frac→0 (imminent
	// reset) gives ~100. The horizon is capped at MaxResetCreditHorizon, so a
	// weekly reset days away earns no credit.
	eff5 := 100 - windowFrac(in.Resets5h, now, FiveHourWindow)*util5
	eff7 := 100 - windowFrac(in.Resets7d, now, SevenDayWindow)*util7

	// Raw remaining self-lifts once the reset passes, like windowFrac and the
	// exhausted gate below: a stale pegged sample from before the reset must not
	// barrier or sticky-floor an account whose window already refilled.
	raw5, raw7 := 100-util5, 100-util7
	if resetPassed(in.Resets5h, now) {
		raw5 = 100
	}
	if resetPassed(in.Resets7d, now) {
		raw7 = 100
	}

	// A pegged window with its reset still pending means the account cannot
	// serve right now — reset credit is forward-looking and must not mask it.
	// now.Before(zero) is false, so an unknown reset never gates; neither does a
	// stale post-reset sample (the gate self-lifts at the reset even before the
	// next poll). A stale pre-reset sample gating is sound: utilization cannot
	// decrease within a window.
	exhausted5 := in.HasUsage && util5 >= ExhaustedAtUtil && now.Before(in.Resets5h)
	exhausted7 := in.HasUsage && util7 >= ExhaustedAtUtil && now.Before(in.Resets7d)
	exhausted := exhausted5 || exhausted7
	// ExhaustedUntil is the binding recovery time: the latest reset among the
	// windows that tripped the gate (a 7d-exhausted account does not recover at
	// its 5h reset).
	var exhaustedUntil time.Time
	if exhausted5 {
		exhaustedUntil = in.Resets5h
	}
	if exhausted7 && in.Resets7d.After(exhaustedUntil) {
		exhaustedUntil = in.Resets7d
	}

	// Barriers and runway use raw remaining, not eff: they answer "can a session
	// start on this account right now", which an imminent reset doesn't change.
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
	// The penalty engages at the short StaleAfter; the displayed Stale flag uses
	// the longer DisplayStaleAfter so a normally-polled account isn't shown stale.
	if in.staleAfter(now, StaleAfter) {
		c.StalePenalty = PenStale
	}
	stale := in.staleAfter(now, DisplayStaleAfter)

	score := c.Remaining5h + c.Remaining7d -
		c.SessionPenalty - c.RateLimitPenalty - c.NeedsLoginPenalty - c.StalePenalty -
		c.Barrier5h - c.Barrier7d - c.RunwayPenalty
	return Result{
		AccountID:      in.AccountID,
		Score:          score,
		Components:     c,
		Resets5h:       in.Resets5h,
		Stale:          stale,
		RateLimited:    in.RateLimited,
		NeedsLogin:     in.NeedsLogin,
		Exhausted:      exhausted,
		ExhaustedUntil: exhaustedUntil,
		Available:      !in.RateLimited && !exhausted && !in.NeedsLogin,
	}
}

// resetPassed reports whether a known reset time has already elapsed — the
// window has refilled even if the latest sample predates it.
func resetPassed(resetsAt, now time.Time) bool {
	return !resetsAt.IsZero() && !now.Before(resetsAt)
}

// windowFrac is the fraction of the credit horizon still ahead before the
// window resets, in [0,1]. A zero/far reset returns 1 (the depletion counts
// fully); an imminent reset returns ~0 (the depletion is about to be refilled).
// The horizon is min(window, MaxResetCreditHorizon), so a reset beyond the cap —
// e.g. a weekly window resetting days from now — earns no credit.
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

// barrier is a convex low-headroom penalty: zero above BarrierKnee, rising
// linearly to BarrierKnee as raw current remaining approaches zero. It takes
// raw remaining, not the reset-credited eff — an imminent reset must not mask
// a window that is nearly empty right now.
func barrier(remaining float64) float64 {
	if remaining >= BarrierKnee {
		return 0
	}
	if remaining < 0 {
		remaining = 0
	}
	return BarrierKnee - remaining
}

// runwayPenalty downranks an account being actively drained: if its raw 5h
// headroom would be exhausted within RunwayHorizon at the current burn rate,
// penalize up to RunwayWeight points. Raw remaining, not eff: time-to-wall is
// how much is actually left over how fast it burns. Zero/negative burn (idle
// or unknown) → 0.
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

// UsableForSticky reports whether a previously-selected account can keep
// serving a sticky session: it must be available (not rate-limited or
// exhausted) and have at least StickyMinRemaining5h raw 5h headroom right now.
func UsableForSticky(r Result) bool {
	return r.Available && r.Components.RawRemaining5h >= StickyMinRemaining5h
}

// Rank scores all inputs and returns results sorted best-first. Ties on score
// break toward the soonest 5-hour reset (an account about to reset is freshest
// to drain first); a zero Resets5h sorts last among ties.
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

// Pick returns the best AVAILABLE account from ranked results. ok=false if no
// account is currently available (all exhausted/rate-limited or the pool is
// empty).
func Pick(ranked []Result) (Result, bool) {
	for _, r := range ranked {
		if r.Available {
			return r, true
		}
	}
	return Result{}, false
}

// PickFallback returns the best exhausted-but-not-rate-limited account, the
// least-bad pick when Pick found nothing: an exhausted account can still serve
// (billing extra-usage credits or waiting out its reset), a rate-limited one
// cannot, and neither can a needs-login one (no valid token at all). ok=false if
// every account is rate-limited / needs-login, or the pool is empty.
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
