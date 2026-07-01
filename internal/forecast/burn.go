// Package forecast computes display predictions from recent usage history:
// per-account burn rates and depletion estimates, plus the pool-wide rollup
// (mean remaining capacity, dry-out ETA, alarm mood) that the status snapshot
// ships to out-of-process readers like the macOS widget.
package forecast

import (
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

const (
	// BurnWindow is how far back the 5h burn estimate looks.
	BurnWindow = 45 * time.Minute
	// BurnMinSpan is the minimum newest-to-oldest span; below it the API's
	// integer-percent quantization dominates the slope.
	BurnMinSpan = 10 * time.Minute
	// BurnMinSamples is the minimum usable sample count, shared by both burns.
	BurnMinSamples = 3
	// Burn7dWindow is how far back the 7d burn estimate looks — wider than the 5h
	// because the 7d window drains ~16× slower, so a wider lookback is needed to
	// clear the API's integer-percent quantization noise.
	Burn7dWindow = 6 * time.Hour
	// Burn7dMinSpan is the minimum span for the 7d burn. Below half the window
	// one quantization step exceeds half the sustainable rate.
	Burn7dMinSpan = 3 * time.Hour
)

// Util5h selects the 5h-window utilization from a sample.
func Util5h(s store.UsageSample) float64 { return s.Util5h }

// Util7d selects the 7d-window utilization from a sample.
func Util7d(s store.UsageSample) float64 { return s.Util7d }

// secantBurn estimates a window's recent drain in percent/hour from samples
// (newest first), reading the window via util and looking back at most window.
// 0 means idle or too-thin history. Rate-limited samples are dropped first: a
// 429 records a zeroed placeholder whose drop would read as a window reset. A
// genuine reset (higher utilization in an older sample) truncates to the
// post-reset segment. The estimate is the endpoint secant — utilization is
// monotone within a window, so it is unbiased and smooth against the API's
// integer-percent quantization.
func secantBurn(samples []store.UsageSample, util func(store.UsageSample) float64,
	window, minSpan time.Duration, now time.Time,
) float64 {
	usable := make([]store.UsageSample, 0, len(samples))
	for _, s := range samples {
		if s.RateLimited || now.Sub(s.TS) > window {
			continue
		}
		usable = append(usable, s)
	}
	for i := 0; i+1 < len(usable); i++ {
		if util(usable[i+1]) > util(usable[i]) {
			usable = usable[:i+1]
			break
		}
	}
	if len(usable) < BurnMinSamples {
		return 0
	}
	span := usable[0].TS.Sub(usable[len(usable)-1].TS)
	if span < minSpan {
		return 0
	}
	return (util(usable[0]) - util(usable[len(usable)-1])) / span.Hours()
}

// Burn5h estimates the 5h window's recent drain in percent/hour from samples
// (newest first); 0 means idle or unknown.
func Burn5h(samples []store.UsageSample, now time.Time) float64 {
	return secantBurn(samples, Util5h, BurnWindow, BurnMinSpan, now)
}

// Burn7d estimates the 7d window's recent drain in percent/hour from samples
// (newest first), over the wider Burn7dWindow; 0 means idle or too-thin history.
func Burn7d(samples []store.UsageSample, now time.Time) float64 {
	return secantBurn(samples, Util7d, Burn7dWindow, Burn7dMinSpan, now)
}
