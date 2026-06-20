package forecast

import (
	"math"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

// sample builds a usage sample age before now at the given 5h utilization.
func sample(now time.Time, age time.Duration, util5h float64) store.UsageSample {
	return store.UsageSample{AccountID: 1, TS: now.Add(-age), Util5h: util5h}
}

// rlSample is sample with the rate-limited 429 placeholder shape: zeroed
// utilization, RateLimited set — exactly what recordSample stores on a 429.
func rlSample(now time.Time, age time.Duration) store.UsageSample {
	s := sample(now, age, 0)
	s.RateLimited = true
	return s
}

func TestBurn5h(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		samples []store.UsageSample // newest first, as RecentUsageSamples returns
		want    float64
	}{
		"no samples": {nil, 0},
		"single sample": {
			[]store.UsageSample{sample(now, 0, 50)}, 0,
		},
		"two samples below min count": {
			[]store.UsageSample{sample(now, 0, 55), sample(now, 15*time.Minute, 50)}, 0,
		},
		"steady burn yields exact secant": {
			// +1%/3min over 12 minutes = 20%/hr.
			[]store.UsageSample{
				sample(now, 0, 54), sample(now, 3*time.Minute, 53),
				sample(now, 6*time.Minute, 52), sample(now, 9*time.Minute, 51),
				sample(now, 12*time.Minute, 50),
			},
			20,
		},
		"integer staircase smooths through the endpoints": {
			// Quantized API: pairs repeat, secant sees 2% over 15min = 8%/hr.
			[]store.UsageSample{
				sample(now, 0, 52), sample(now, 3*time.Minute, 52),
				sample(now, 6*time.Minute, 51), sample(now, 9*time.Minute, 51),
				sample(now, 12*time.Minute, 50), sample(now, 15*time.Minute, 50),
			},
			8,
		},
		"flat idle yields zero": {
			[]store.UsageSample{
				sample(now, 0, 50), sample(now, 6*time.Minute, 50),
				sample(now, 12*time.Minute, 50),
			},
			0,
		},
		"reset mid-window truncates to the post-reset segment": {
			// Pre-reset sample at util 90 must not poison the slope:
			// post-reset segment is (12−1)% over 15min = 44%/hr.
			[]store.UsageSample{
				sample(now, 0, 12), sample(now, 5*time.Minute, 8),
				sample(now, 10*time.Minute, 4), sample(now, 15*time.Minute, 1),
				sample(now, 20*time.Minute, 90),
			},
			44,
		},
		"post-reset segment too short yields zero": {
			[]store.UsageSample{
				sample(now, 0, 5), sample(now, 3*time.Minute, 3),
				sample(now, 6*time.Minute, 1), sample(now, 9*time.Minute, 95),
			},
			0,
		},
		"rate-limited placeholder does not fake a reset": {
			// The zeroed 429 sample sits mid-stream; dropping it keeps the
			// window intact: 4% over 12min = 20%/hr.
			[]store.UsageSample{
				sample(now, 0, 54), sample(now, 3*time.Minute, 53),
				rlSample(now, 5*time.Minute),
				sample(now, 6*time.Minute, 52), sample(now, 9*time.Minute, 51),
				sample(now, 12*time.Minute, 50),
			},
			20,
		},
		"samples beyond the window are excluded": {
			// The 50-minute-old wild sample would inflate the slope massively.
			[]store.UsageSample{
				sample(now, 0, 54), sample(now, 6*time.Minute, 52),
				sample(now, 12*time.Minute, 50), sample(now, 50*time.Minute, 1),
			},
			20,
		},
		"span below minimum yields zero": {
			[]store.UsageSample{
				sample(now, 0, 54), sample(now, 3*time.Minute, 52),
				sample(now, 6*time.Minute, 50),
			},
			0,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Burn5h(tc.samples, now)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("Burn5h = %v, want %v", got, tc.want)
			}
		})
	}
}

// s7 builds a sample age before now at the given 7d utilization.
func s7(now time.Time, age time.Duration, util7d float64) store.UsageSample {
	return store.UsageSample{AccountID: 1, TS: now.Add(-age), Util7d: util7d}
}

func TestBurn7d(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	hr := time.Hour
	cases := map[string]struct {
		samples []store.UsageSample // newest first
		want    float64
	}{
		"exact secant over six hours": {
			// 12% drained over 6h = 2%/hr.
			[]store.UsageSample{s7(now, 0, 12), s7(now, 2*hr, 8), s7(now, 4*hr, 4), s7(now, 6*hr, 0)},
			2,
		},
		"forty-five-minute staircase is rejected": {
			// A noisy 45-min climb that Burn5h would read as a steep slope: the
			// 7d span is far below Burn7dMinSpan, so it yields nothing.
			[]store.UsageSample{
				s7(now, 0, 4), s7(now, 15*time.Minute, 3),
				s7(now, 30*time.Minute, 2), s7(now, 45*time.Minute, 1),
			},
			0,
		},
		"sub-three-hour span yields zero": {
			[]store.UsageSample{s7(now, 0, 6), s7(now, 1*hr, 4), s7(now, 2*hr, 2)},
			0,
		},
		"weekly reset truncates to the post-reset segment": {
			// The 5h-old sample at util 90 predates a weekly reset and must not
			// poison the slope: post-reset segment (12−4)/4h = 2%/hr.
			[]store.UsageSample{s7(now, 0, 12), s7(now, 2*hr, 8), s7(now, 4*hr, 4), s7(now, 5*hr, 90)},
			2,
		},
		"post-reset segment too short yields zero": {
			// Only 2h of post-reset history before the reset jump: below
			// Burn7dMinSpan, so zero.
			[]store.UsageSample{s7(now, 0, 5), s7(now, 1*hr, 3), s7(now, 2*hr, 1), s7(now, 3*hr, 90)},
			0,
		},
		"rate-limited placeholder does not fake a reset": {
			// The zeroed 429 sample mid-stream is dropped; the window stays
			// intact at 12% over 6h = 2%/hr.
			[]store.UsageSample{
				s7(now, 0, 12), s7(now, 2*hr, 8), rlSample(now, 3*hr),
				s7(now, 4*hr, 4), s7(now, 6*hr, 0),
			},
			2,
		},
		"samples beyond the window are excluded": {
			// The 7h-old wild sample is outside Burn7dWindow: 12% over 6h = 2%/hr.
			[]store.UsageSample{s7(now, 0, 12), s7(now, 3*hr, 6), s7(now, 6*hr, 0), s7(now, 7*hr, 200)},
			2,
		},
		"burn5h unaffected by deep history": {
			// Six hours of wild Util5h history, but a clean +20%/hr climb in the
			// last 12 minutes: Burn5h self-filters to its 45-min window and reads 20.
			append(
				[]store.UsageSample{
					sample(now, 0, 54), sample(now, 3*time.Minute, 53),
					sample(now, 6*time.Minute, 52), sample(now, 9*time.Minute, 51),
					sample(now, 12*time.Minute, 50),
				},
				sample(now, 1*hr, 5), sample(now, 3*hr, 90), sample(now, 6*hr, 1),
			),
			20, // asserted against Burn5h, not Burn7d (see below)
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if name == "burn5h unaffected by deep history" {
				if got := Burn5h(tc.samples, now); math.Abs(got-tc.want) > 1e-9 {
					t.Errorf("Burn5h = %v, want %v", got, tc.want)
				}
				return
			}
			got := Burn7d(tc.samples, now)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("Burn7d = %v, want %v", got, tc.want)
			}
		})
	}
}
