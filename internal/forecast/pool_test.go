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
		"scoped lower than aggregate folds the mean down": {
			// Aggregate weekly headroom 60, but the scoped bucket is 80% used
			// (headroom 20) with its reset pending: the mean folds to the binding 20.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 100, Remaining7d: 60,
				HasScoped7d: true, Scoped7dUtil: 80, Scoped7dResets: now.Add(48 * time.Hour),
			}},
			Pool{Remaining5h: 100, Remaining7d: 20, Mood: MoodWorried},
			true,
		},
		"scoped higher than aggregate is ignored": {
			// Scoped headroom 90 exceeds aggregate 40: the min-fold keeps aggregate.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 100, Remaining7d: 40,
				HasScoped7d: true, Scoped7dUtil: 10, Scoped7dResets: now.Add(48 * time.Hour),
			}},
			Pool{Remaining5h: 100, Remaining7d: 40, Mood: MoodEasy},
			true,
		},
		"scoped with a passed reset is ignored": {
			// The scoped bucket is pegged (5% left) but its reset already elapsed —
			// the window refilled, so the stale sample must not fold pressure in.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 100, Remaining7d: 70,
				HasScoped7d: true, Scoped7dUtil: 95, Scoped7dResets: now.Add(-time.Hour),
			}},
			Pool{Remaining5h: 100, Remaining7d: 70, Mood: MoodChill},
			true,
		},
		"zero scoped reset still folds": {
			// An unknown (zero) scoped reset never counts as passed, so the fold
			// applies: aggregate 90 folds to the scoped headroom 30.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 100, Remaining7d: 90,
				HasScoped7d: true, Scoped7dUtil: 70,
			}},
			Pool{Remaining5h: 100, Remaining7d: 30, Mood: MoodUneasy},
			true,
		},
		"HasScoped7d false ignores a stray scoped util": {
			// Without HasScoped7d the scoped util is inert, however pegged.
			[]PoolAccount{{
				HasUsage: true, Remaining5h: 100, Remaining7d: 55, Scoped7dUtil: 99,
			}},
			Pool{Remaining5h: 100, Remaining7d: 55, Mood: MoodChill},
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
		in   moodInput
		want Mood
	}{
		"no usable accounts is panic":  {moodInput{usable: 0}, MoodPanic},
		"panic stays panic under bump": {moodInput{usable: 0, dryProjected: true}, MoodPanic},

		// 5h rungs bind when the weekly window has ample slack (remaining7d 100).
		"5h 60 is chill":           {moodInput{usable: 1, remaining5h: 60, remaining7d: 100}, MoodChill},
		"5h just below 60 is easy": {moodInput{usable: 1, remaining5h: 59.9, remaining7d: 100}, MoodEasy},
		"5h 40 is easy":            {moodInput{usable: 1, remaining5h: 40, remaining7d: 100}, MoodEasy},
		"5h below 40 is uneasy":    {moodInput{usable: 1, remaining5h: 39.9, remaining7d: 100}, MoodUneasy},
		"5h 25 is uneasy":          {moodInput{usable: 1, remaining5h: 25, remaining7d: 100}, MoodUneasy},
		"5h below 25 is worried":   {moodInput{usable: 1, remaining5h: 24.9, remaining7d: 100}, MoodWorried},
		"5h 10 is worried":         {moodInput{usable: 1, remaining5h: 10, remaining7d: 100}, MoodWorried},
		"5h below 10 is alarmed":   {moodInput{usable: 1, remaining5h: 9.9, remaining7d: 100}, MoodAlarmed},

		// Weekly rungs bind when the 5h window has ample slack (remaining5h 100).
		"weekly 50 is chill":         {moodInput{usable: 1, remaining5h: 100, remaining7d: 50}, MoodChill},
		"weekly below 50 is easy":    {moodInput{usable: 1, remaining5h: 100, remaining7d: 49.9}, MoodEasy},
		"weekly 35 is easy":          {moodInput{usable: 1, remaining5h: 100, remaining7d: 35}, MoodEasy},
		"weekly below 35 is uneasy":  {moodInput{usable: 1, remaining5h: 100, remaining7d: 34.9}, MoodUneasy},
		"weekly 25 is uneasy":        {moodInput{usable: 1, remaining5h: 100, remaining7d: 25}, MoodUneasy},
		"weekly below 25 is worried": {moodInput{usable: 1, remaining5h: 100, remaining7d: 24.9}, MoodWorried},
		"weekly 15 is worried":       {moodInput{usable: 1, remaining5h: 100, remaining7d: 15}, MoodWorried},
		"weekly below 15 is alarmed": {moodInput{usable: 1, remaining5h: 100, remaining7d: 14.9}, MoodAlarmed},

		// The 7d pace bumps the weekly bucket one level at paceHotWeekly (1.25),
		// hysteresis-guarded so 1.24 does nothing.
		"pace 1.24 leaves chill":        {moodInput{usable: 1, remaining5h: 100, remaining7d: 100, pace7d: 1.24}, MoodChill},
		"pace 1.25 bumps chill to easy": {moodInput{usable: 1, remaining5h: 100, remaining7d: 100, pace7d: 1.25}, MoodEasy},
		// Worried weekly + pace bump (→ alarmed) + dry bump (→ panic) stack.
		"worried weekly pace and dry stack to panic": {moodInput{usable: 1, remaining5h: 100, remaining7d: 20, pace7d: 1.25, dryProjected: true}, MoodPanic},

		// The dry bump raises the merged mood one level, saturating at panic.
		"dry bumps chill to easy":      {moodInput{usable: 1, remaining5h: 80, remaining7d: 100, dryProjected: true}, MoodEasy},
		"dry bumps easy to uneasy":     {moodInput{usable: 1, remaining5h: 50, remaining7d: 100, dryProjected: true}, MoodUneasy},
		"dry bumps uneasy to worried":  {moodInput{usable: 1, remaining5h: 30, remaining7d: 100, dryProjected: true}, MoodWorried},
		"dry bumps worried to alarmed": {moodInput{usable: 1, remaining5h: 15, remaining7d: 100, dryProjected: true}, MoodAlarmed},
		"dry bumps alarmed to panic":   {moodInput{usable: 1, remaining5h: 5, remaining7d: 100, dryProjected: true}, MoodPanic},

		// The weekly-exhausted fraction floors the mood proportionally.
		"floor 1 of 4 does not fire": {moodInput{usable: 4, remaining5h: 100, remaining7d: 100, weeklyExhausted: 1}, MoodChill},
		"floor 1 of 3 is uneasy":     {moodInput{usable: 3, remaining5h: 100, remaining7d: 100, weeklyExhausted: 1}, MoodUneasy},
		"floor 1 of 2 is worried":    {moodInput{usable: 2, remaining5h: 100, remaining7d: 100, weeklyExhausted: 1}, MoodWorried},
		"floor 2 of 3 is alarmed":    {moodInput{usable: 3, remaining5h: 100, remaining7d: 100, weeklyExhausted: 2}, MoodAlarmed},
		"floor 3 of 3 is alarmed":    {moodInput{usable: 3, remaining5h: 100, remaining7d: 100, weeklyExhausted: 3}, MoodAlarmed},

		// The 2026-07 screenshot state: fresh 5h (92) masks a nearly-spent, hot,
		// partly-exhausted weekly window that must alarm the mascot.
		"screenshot state alarms": {moodInput{usable: 16, remaining5h: 92, remaining7d: 16.4, pace7d: 1.79, weeklyExhausted: 6}, MoodAlarmed},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := moodOf(tc.in); got != tc.want {
				t.Errorf("moodOf(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestPoolMoodWeeklyExhaustion pins the mood floor the weekly-exhausted fraction
// raises through PoolOf: the mascot alarms when the pool is fully pegged and
// floors proportionally short of that, however fresh the 5h windows.
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
		"one of two exhausted floors at worried": {
			// Half the usable pool is weekly-pegged (frac 1/2), so the proportional
			// floor raises the fresh-5h baseline (chill) to worried.
			[]PoolAccount{
				{HasUsage: true, WeeklyExhausted: true, Remaining5h: 100, Remaining7d: 100},
				{HasUsage: true, Remaining5h: 100, Remaining7d: 100},
			},
			MoodWorried,
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

	// Below the first floor rung (frac < 1/3) exhaustion adds nothing: a 1-of-4
	// pegged pool must land on the exact mood the flag-cleared pool does.
	t.Run("sub-rung exhaustion matches the flag-cleared baseline", func(t *testing.T) {
		partial := []PoolAccount{
			{HasUsage: true, WeeklyExhausted: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
		}
		baseline := []PoolAccount{
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
			{HasUsage: true, Remaining5h: 30, Remaining7d: 40},
		}
		gotPartial, _ := PoolOf(partial, now)
		gotBaseline, _ := PoolOf(baseline, now)
		if gotPartial.Mood != gotBaseline.Mood {
			t.Errorf("partial mood %q != baseline mood %q", gotPartial.Mood, gotBaseline.Mood)
		}
		if gotPartial.Mood == MoodAlarmed {
			t.Error("sub-rung exhaustion floored the mood at alarmed; the floor must not fire")
		}
	})
}
