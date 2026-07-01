package forecast

import (
	"time"

	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// Estimate is one account's 5h-window display forecast; the zero value means
// no projection (idle, stale, rate-limited, exhausted, or too little history).
type Estimate struct {
	// BurnPerHour is the smoothed drain of the 5h window, percent/hour.
	BurnPerHour float64
	// AtReset is the projected REMAINING percent at Resets5h, clamped to
	// 0..100; 0 when the reset time is unknown (or depletion lands first,
	// which DepletedAt signals).
	AtReset float64
	// DepletedAt is when remaining hits 0 at the current burn, whole seconds;
	// zero when a reset refills the window first.
	DepletedAt time.Time
}

// Estimate5h computes the display forecast from recent samples (newest
// first). exhausted is score's exhausted gate: a pegged window whose reset is
// pending. Projections anchor at the latest sample's timestamp, not now.
func Estimate5h(samples []store.UsageSample, exhausted bool, now time.Time) Estimate {
	latest, ok := displayable(samples, exhausted, now)
	if !ok {
		return Estimate{}
	}
	if !latest.Resets5h.IsZero() && !latest.Resets5h.After(now) {
		return Estimate{} // the sample predates the refill it projects across
	}
	burn := Burn5h(samples, now)
	if burn <= 0 {
		return Estimate{}
	}
	est := Estimate{BurnPerHour: burn}
	if !latest.Resets5h.IsZero() {
		projected := latest.Util5h + burn*latest.Resets5h.Sub(latest.TS).Hours()
		est.AtReset = max(0, min(100, 100-projected))
	}
	depleted := latest.TS.Add(hours((100 - latest.Util5h) / burn)).Truncate(time.Second)
	if depleted.After(now) && (latest.Resets5h.IsZero() || depleted.Before(latest.Resets5h)) {
		est.DepletedAt = depleted
	}
	return est
}

// displayable reports whether the latest sample can drive a display forecast;
// window-specific reset gates stay with their estimator.
func displayable(samples []store.UsageSample, exhausted bool, now time.Time) (store.UsageSample, bool) {
	if len(samples) == 0 || exhausted {
		return store.UsageSample{}, false
	}
	latest := samples[0]
	if latest.RateLimited || now.Sub(latest.TS) > score.DisplayStaleAfter {
		return store.UsageSample{}, false
	}
	return latest, true
}

// Burn7dGated is the display-safe 7d drain in percent/hour: 0 unless the
// latest sample is displayable, and 0 on a passed 7d reset. A passed 5h reset
// does NOT gate it; the windows are independent.
func Burn7dGated(samples []store.UsageSample, exhausted bool, now time.Time) float64 {
	latest, ok := displayable(samples, exhausted, now)
	if !ok {
		return 0
	}
	if !latest.Resets7d.IsZero() && !latest.Resets7d.After(now) {
		return 0
	}
	return Burn7d(samples, now)
}

func hours(h float64) time.Duration {
	return time.Duration(h * float64(time.Hour))
}
