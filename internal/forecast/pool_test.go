package forecast

import (
	"math"
	"testing"
	"time"
)

func TestPoolOf(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		accts  []PoolAccount
		want   Pool
		wantOK bool
	}{
		"zero accounts": {nil, Pool{}, false},
		"never sampled pool": {
			[]PoolAccount{{}, {}}, Pool{}, false,
		},
		"all rate-limited is panic with zero everything": {
			[]PoolAccount{
				{HasUsage: true, RateLimited: true, Remaining5h: 100},
				{HasUsage: true, RateLimited: true, Remaining5h: 100},
			},
			Pool{Mood: MoodPanic},
			true,
		},
		"pace arithmetic over two usable accounts": {
			// burn sums to 20 over usable regen 20×2=40 → Pace5h 0.5; burn7
			// sums to 0.8 over 7d regen → Pace7d. No resets: dry = 140/20 = 7h.
			[]PoolAccount{
				{HasUsage: true, Remaining5h: 80, Remaining7d: 60, Burn5hPerHour: 10, Burn7dPerHour: 0.3},
				{HasUsage: true, Remaining5h: 60, Remaining7d: 40, Burn5hPerHour: 10, Burn7dPerHour: 0.5},
			},
			Pool{
				Remaining5h: 70, Remaining7d: 50, BurnPerHour: 20,
				Pace5h: 0.5, Pace7d: 0.8 / (regen7dPerHour * 2), NetBurnPerHour: 10,
				DryAt: now.Add(7 * time.Hour), Mood: MoodEasy,
			},
			true,
		},
		"exhausted account stays in the pace denominator": {
			// The zero-burn (exhausted) account counts as usable, so Pace5h is
			// 20/(20×2)=0.5 — not 1.0 as it would be if only the burner counted.
			[]PoolAccount{
				{HasUsage: true, Remaining5h: 60, Remaining7d: 50, Burn5hPerHour: 20, Burn7dPerHour: 1.0},
				{HasUsage: true, Remaining5h: 0, Remaining7d: 10, Burn5hPerHour: 0, Burn7dPerHour: 0},
			},
			Pool{
				Remaining5h: 30, Remaining7d: 30, BurnPerHour: 20,
				Pace5h: 0.5, Pace7d: 1.0 / (regen7dPerHour * 2), NetBurnPerHour: 10,
				DryAt: now.Add(3 * time.Hour), Mood: MoodWorried,
			},
			true,
		},
		"rate-limited account excluded from mean and both paces": {
			// The RL account's fabricated remaining 100 and wild burns must not
			// leak into the rollup or the denominator: usable is 1.
			[]PoolAccount{
				{HasUsage: true, Remaining5h: 80, Remaining7d: 60, Burn5hPerHour: 10, Burn7dPerHour: 0.5},
				{HasUsage: true, RateLimited: true, Remaining5h: 100, Burn5hPerHour: 50, Burn7dPerHour: 5},
			},
			Pool{
				Remaining5h: 80, Remaining7d: 60, BurnPerHour: 10,
				Pace5h: 0.5, Pace7d: 0.5 / regen7dPerHour, NetBurnPerHour: 10,
				DryAt: now.Add(8 * time.Hour), Mood: MoodEasy,
			},
			true,
		},
		"relief no longer suppresses a later wall": {
			// remaining 30, burn 40, reset at +30m. The window refills to 100 at
			// +30m crediting only the 20 it used, then drains 40%/h to dry at +3h.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 30, Remaining7d: 30, Burn5hPerHour: 40,
				Resets5h: now.Add(30 * time.Minute),
			}},
			Pool{
				Remaining5h: 30, Remaining7d: 30, BurnPerHour: 40,
				Pace5h: 2.0, NetBurnPerHour: -50,
				DryAt: now.Add(3 * time.Hour), Mood: MoodWorried,
			},
			true,
		},
		"sustainable pace with chained resets never dries": {
			// Pace exactly 1.0 (burn 20 = regen 20): the known reset plus the
			// 5h chain refill exactly what the window drains, so the simulation
			// never reaches 0 inside the 24h horizon.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 50, Remaining7d: 50, Burn5hPerHour: 20,
				Resets5h: now.Add(time.Hour),
			}},
			Pool{
				Remaining5h: 50, Remaining7d: 50, BurnPerHour: 20,
				Pace5h: 1.0, NetBurnPerHour: -50, Mood: MoodEasy,
			},
			true,
		},
		"refill credits only what the window used": {
			// remaining 90, burn 60, reset at +10m. Used only 10 by the reset,
			// so the refill credits 100−80=20 (window was near full) → total
			// back to 100, dry at +1h50m. A flat +100 model would over-credit
			// and push the wall out to +3h10m.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 90, Remaining7d: 90, Burn5hPerHour: 60,
				Resets5h: now.Add(10 * time.Minute),
			}},
			Pool{
				Remaining5h: 90, Remaining7d: 90, BurnPerHour: 60,
				Pace5h: 3.0, NetBurnPerHour: 40,
				DryAt: now.Add(110 * time.Minute), Mood: MoodEasy,
			},
			true,
		},
		"dry exactly at reset lets the refill win": {
			// remaining 20, burn 20 → naive dry would land exactly at the +1h
			// reset; the strict-before tie-break hands it to the refill, so the
			// window survives and never dries.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 20, Remaining7d: 20, Burn5hPerHour: 20,
				Resets5h: now.Add(time.Hour),
			}},
			Pool{
				Remaining5h: 20, Remaining7d: 20, BurnPerHour: 20,
				Pace5h: 1.0, NetBurnPerHour: -80, Mood: MoodWorried,
			},
			true,
		},
		"past reset neither suppresses nor credits": {
			// remaining 50, burn 25, reset 1h ago: no relief scheduled, so the
			// pool dries in 2h — the past reset adds nothing either way.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 50, Remaining7d: 50, Burn5hPerHour: 25,
				Resets5h: now.Add(-time.Hour),
			}},
			Pool{
				Remaining5h: 50, Remaining7d: 50, BurnPerHour: 25,
				Pace5h: 1.25, NetBurnPerHour: 25,
				DryAt: now.Add(2 * time.Hour), Mood: MoodUneasy,
			},
			true,
		},
		"wall beyond the horizon has no clock": {
			// remaining 100, burn 4, no reset → dry at +25h, past the 24h
			// horizon: DryAt is zero and pace 0.2 carries the (sustainable) story.
			[]PoolAccount{{HasUsage: true, Remaining5h: 100, Remaining7d: 100, Burn5hPerHour: 4}},
			Pool{
				Remaining5h: 100, Remaining7d: 100, BurnPerHour: 4,
				Pace5h: 0.2, NetBurnPerHour: 4, Mood: MoodChill,
			},
			true,
		},
		"wall beyond the horizon with a far reset still has no clock": {
			// remaining 100, burn 4 → dry at +25h, past the 24h horizon. A reset
			// 30h out is itself beyond the horizon, so it schedules no relief and
			// must not leak a clock for a wall the horizon already disowns.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 100, Remaining7d: 100, Burn5hPerHour: 4,
				Resets5h: now.Add(30 * time.Hour),
			}},
			Pool{
				Remaining5h: 100, Remaining7d: 100, BurnPerHour: 4,
				Pace5h: 0.2, NetBurnPerHour: 4, Mood: MoodChill,
			},
			true,
		},
		"drained but unexhausted dries now": {
			// remaining 0 with a live burn and no reset: the wall is already
			// here, DryAt = now.
			[]PoolAccount{{HasUsage: true, Remaining5h: 0, Remaining7d: 20, Burn5hPerHour: 10}},
			Pool{
				Remaining5h: 0, Remaining7d: 20, BurnPerHour: 10,
				Pace5h: 0.5, NetBurnPerHour: 0, DryAt: now, Mood: MoodPanic,
			},
			true,
		},
		"remaining clamped before aggregation": {
			[]PoolAccount{
				{HasUsage: true, Remaining5h: -5, Remaining7d: 120},
				{HasUsage: true, Remaining5h: 100, Remaining7d: 100},
			},
			Pool{Remaining5h: 50, Remaining7d: 100, Mood: MoodEasy},
			true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := PoolOf(tc.accts, now)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if math.Abs(got.Remaining5h-tc.want.Remaining5h) > 1e-9 {
				t.Errorf("Remaining5h = %v, want %v", got.Remaining5h, tc.want.Remaining5h)
			}
			if math.Abs(got.Remaining7d-tc.want.Remaining7d) > 1e-9 {
				t.Errorf("Remaining7d = %v, want %v", got.Remaining7d, tc.want.Remaining7d)
			}
			if math.Abs(got.BurnPerHour-tc.want.BurnPerHour) > 1e-9 {
				t.Errorf("BurnPerHour = %v, want %v", got.BurnPerHour, tc.want.BurnPerHour)
			}
			if math.Abs(got.Pace5h-tc.want.Pace5h) > 1e-9 {
				t.Errorf("Pace5h = %v, want %v", got.Pace5h, tc.want.Pace5h)
			}
			if math.Abs(got.Pace7d-tc.want.Pace7d) > 1e-9 {
				t.Errorf("Pace7d = %v, want %v", got.Pace7d, tc.want.Pace7d)
			}
			if math.Abs(got.NetBurnPerHour-tc.want.NetBurnPerHour) > 1e-9 {
				t.Errorf("NetBurnPerHour = %v, want %v", got.NetBurnPerHour, tc.want.NetBurnPerHour)
			}
			if !got.DryAt.Equal(tc.want.DryAt) {
				t.Errorf("DryAt = %v, want %v", got.DryAt, tc.want.DryAt)
			}
			if got.Mood != tc.want.Mood {
				t.Errorf("Mood = %q, want %q", got.Mood, tc.want.Mood)
			}
		})
	}
}

func TestMoodOf(t *testing.T) {
	cases := map[string]struct {
		usable    int
		remaining float64
		dry       bool
		weekly    bool
		want      Mood
	}{
		"no usable accounts is panic":  {0, 0, false, false, MoodPanic},
		"60 is chill":                  {1, 60, false, false, MoodChill},
		"just below 60 is easy":        {1, 59.9, false, false, MoodEasy},
		"40 is easy":                   {1, 40, false, false, MoodEasy},
		"just below 40 is uneasy":      {1, 39.9, false, false, MoodUneasy},
		"25 is uneasy":                 {1, 25, false, false, MoodUneasy},
		"just below 25 is worried":     {1, 24.9, false, false, MoodWorried},
		"10 is worried":                {1, 10, false, false, MoodWorried},
		"just below 10 is alarmed":     {1, 9.9, false, false, MoodAlarmed},
		"dry bumps chill to easy":      {1, 80, true, false, MoodEasy},
		"dry bumps easy to uneasy":     {1, 50, true, false, MoodUneasy},
		"dry bumps uneasy to worried":  {1, 30, true, false, MoodWorried},
		"dry bumps worried to alarmed": {1, 15, true, false, MoodAlarmed},
		"dry bumps alarmed to panic":   {1, 5, true, false, MoodPanic},
		"panic stays panic under bump": {0, 0, true, false, MoodPanic},
		// Weekly exhaustion floors the mood at alarmed however fresh the 5h mean.
		"weekly floors chill at alarmed":    {1, 100, false, true, MoodAlarmed},
		"weekly floors easy at alarmed":     {1, 50, false, true, MoodAlarmed},
		"weekly no-op once already alarmed": {1, 5, false, true, MoodAlarmed},
		// The dry bump applies on top of the weekly floor, reaching panic.
		"weekly floor then dry is panic": {1, 100, true, true, MoodPanic},
		// Floor off leaves the base mapping untouched (partial exhaustion path).
		"weekly off leaves chill": {1, 100, false, false, MoodChill},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := moodOf(tc.usable, tc.remaining, tc.dry, tc.weekly); got != tc.want {
				t.Errorf("moodOf(%d, %v, %v, %v) = %q, want %q",
					tc.usable, tc.remaining, tc.dry, tc.weekly, got, tc.want)
			}
		})
	}
}

// TestPoolMoodWeeklyExhaustion pins the mood floor a fully weekly-exhausted pool
// raises through PoolOf: the mascot must alarm however fresh the 5h windows,
// while partial exhaustion changes nothing.
func TestPoolMoodWeeklyExhaustion(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		accts []PoolAccount
		want  Mood
	}{
		"all usable weekly-exhausted floors at alarmed": {
			// Fresh 5h windows (Remaining 100) but every usable account is weekly
			// pegged: the pool can't serve default-model work, so the mood floors.
			[]PoolAccount{
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100},
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100},
			},
			MoodAlarmed,
		},
		"all exhausted plus a projected dry-out reaches panic": {
			// The weekly floor lands alarmed, then the dry-out bump takes it to panic.
			[]PoolAccount{
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100, Burn5hPerHour: 100},
			},
			MoodPanic,
		},
		"one of two exhausted changes nothing": {
			// Partial exhaustion: selection routes around the pegged account, so the
			// mood stays the fresh-5h baseline (chill), never floored.
			[]PoolAccount{
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100},
				{HasUsage: true, Remaining5h: 100, Remaining7d: 100},
			},
			MoodChill,
		},
		"a rate-limited straggler cannot veto the floor": {
			// The fold runs over usable accounts only: a non-usable
			// (rate-limited) account with a clear flag must not lift the floor
			// off a pool whose every usable account is weekly-pegged.
			[]PoolAccount{
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100},
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100},
				{HasUsage: true, RateLimited: true, Remaining5h: 100},
			},
			MoodAlarmed,
		},
		"zero usable is panic despite the exhausted flag": {
			// Rate-limited accounts are excluded from usable, so usable is 0 and the
			// panic verdict wins regardless of the weekly flag.
			[]PoolAccount{
				{HasUsage: true, RateLimited: true, WeeklyExhausted: true, Remaining5h: 100},
				{HasUsage: true, RateLimited: true, WeeklyExhausted: true, Remaining5h: 100},
			},
			MoodPanic,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := PoolOf(tc.accts, now)
			if !ok {
				t.Fatalf("PoolOf ok = false, want true")
			}
			if got.Mood != tc.want {
				t.Errorf("Mood = %q, want %q", got.Mood, tc.want)
			}
		})
	}

	// The floor fires only when ALL usable accounts are pegged: a partially
	// exhausted pool must land on the exact mood the flag-cleared pool does.
	t.Run("partial exhaustion matches the flag-cleared baseline", func(t *testing.T) {
		partial := []PoolAccount{
			{HasUsage: true, WeeklyExhausted: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
		}
		baseline := []PoolAccount{
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
		}
		gotPartial, _ := PoolOf(partial, now)
		gotBaseline, _ := PoolOf(baseline, now)
		if gotPartial.Mood != gotBaseline.Mood {
			t.Errorf("partial mood %q != baseline mood %q", gotPartial.Mood, gotBaseline.Mood)
		}
		if gotPartial.Mood == MoodAlarmed {
			t.Error("partial exhaustion floored the mood at alarmed; the floor must not fire")
		}
	})
}
