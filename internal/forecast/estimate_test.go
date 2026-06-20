package forecast

import (
	"math"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

// climb builds n newest-first samples ending at util at age before now,
// dropping stepPct per step going back in time, each step minutes apart.
func climb(now time.Time, age time.Duration, util, stepPct float64, step time.Duration, n int) []store.UsageSample {
	out := make([]store.UsageSample, n)
	for i := range out {
		out[i] = sample(now, age+time.Duration(i)*step, util-float64(i)*stepPct)
	}
	return out
}

// withReset stamps Resets5h on every sample.
func withReset(samples []store.UsageSample, reset time.Time) []store.UsageSample {
	for i := range samples {
		samples[i].Resets5h = reset
	}
	return samples
}

// withReset7d stamps Resets7d on every sample.
func withReset7d(samples []store.UsageSample, reset time.Time) []store.UsageSample {
	for i := range samples {
		samples[i].Resets7d = reset
	}
	return samples
}

func TestEstimate5h(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	// burning: util 50 now, +1%/3min over 12 minutes = 20%/hr.
	burning := func() []store.UsageSample { return climb(now, 0, 50, 1, 3*time.Minute, 5) }

	cases := map[string]struct {
		samples   []store.UsageSample
		exhausted bool
		want      Estimate
	}{
		"no samples":         {nil, false, Estimate{}},
		"exhausted is gated": {burning(), true, Estimate{}},
		"rate-limited latest is gated": {
			append([]store.UsageSample{rlSample(now, 0)}, burning()...), false, Estimate{},
		},
		"stale latest is gated": {
			climb(now, 6*time.Minute, 50, 1, 3*time.Minute, 5), false, Estimate{},
		},
		"already-passed reset is gated": {
			withReset(burning(), now.Add(-time.Minute)), false, Estimate{},
		},
		"idle burn yields zero estimate": {
			[]store.UsageSample{
				sample(now, 0, 50), sample(now, 6*time.Minute, 50),
				sample(now, 12*time.Minute, 50),
			},
			false,
			Estimate{},
		},
		"projection at a future reset": {
			// 50% used, 20%/hr, reset in 1h: 70% used at reset = 30% left.
			// Depletion (2.5h out) lands after the reset, so it is omitted.
			withReset(burning(), now.Add(time.Hour)), false,
			Estimate{BurnPerHour: 20, AtReset: 30},
		},
		"depletion before reset wins": {
			// 90% used, 20%/hr, reset in 1h: projected 110% used clamps the
			// remaining to 0, and depletion lands at +30min, before the reset.
			withReset(climb(now, 0, 90, 1, 3*time.Minute, 5), now.Add(time.Hour)), false,
			Estimate{BurnPerHour: 20, AtReset: 0, DepletedAt: now.Add(30 * time.Minute)},
		},
		"unknown reset still projects depletion": {
			burning(), false,
			Estimate{BurnPerHour: 20, DepletedAt: now.Add(150 * time.Minute)},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Estimate5h(tc.samples, tc.exhausted, now)
			if math.Abs(got.BurnPerHour-tc.want.BurnPerHour) > 1e-9 {
				t.Errorf("BurnPerHour = %v, want %v", got.BurnPerHour, tc.want.BurnPerHour)
			}
			if math.Abs(got.AtReset-tc.want.AtReset) > 1e-9 {
				t.Errorf("AtReset = %v, want %v", got.AtReset, tc.want.AtReset)
			}
			if !got.DepletedAt.Equal(tc.want.DepletedAt) {
				t.Errorf("DepletedAt = %v, want %v", got.DepletedAt, tc.want.DepletedAt)
			}
			if !got.DepletedAt.Equal(got.DepletedAt.Truncate(time.Second)) {
				t.Errorf("DepletedAt %v carries sub-second precision", got.DepletedAt)
			}
		})
	}
}

func TestBurn7dGated(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	hr := time.Hour
	// burning7d: 12% drained over 6h = 2%/hr, newest first, age-shifted by off.
	burning7d := func(off time.Duration) []store.UsageSample {
		return []store.UsageSample{
			s7(now, off, 12), s7(now, off+2*hr, 8),
			s7(now, off+4*hr, 4), s7(now, off+6*hr, 0),
		}
	}

	cases := map[string]struct {
		samples   []store.UsageSample
		exhausted bool
		want      float64
	}{
		"burning yields the 7d burn":   {burning7d(0), false, 2},
		"exhausted is gated":           {burning7d(0), true, 0},
		"rate-limited latest is gated": {append([]store.UsageSample{rlSample(now, 0)}, burning7d(0)...), false, 0},
		"stale latest is gated":        {burning7d(6 * time.Minute), false, 0},
		"passed 7d reset is gated":     {withReset7d(burning7d(0), now.Add(-time.Minute)), false, 0},
		"passed 5h reset does not gate": {
			// A passed 5h reset is irrelevant to the 7d window: burn survives.
			withReset(burning7d(0), now.Add(-time.Minute)), false, 2,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := Burn7dGated(tc.samples, tc.exhausted, now)
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("Burn7dGated = %v, want %v", got, tc.want)
			}
		})
	}
}
