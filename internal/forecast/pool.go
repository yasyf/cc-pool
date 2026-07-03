package forecast

import (
	"sort"
	"time"
)

// Mood is the pool-health alarm level, ordered
// chill < easy < uneasy < worried < alarmed < panic.
type Mood string

// Mood levels, calmest first.
const (
	MoodChill   Mood = "chill"
	MoodEasy    Mood = "easy"
	MoodUneasy  Mood = "uneasy"
	MoodWorried Mood = "worried"
	MoodAlarmed Mood = "alarmed"
	MoodPanic   Mood = "panic"
)

// worse returns the next more-alarmed level; panic is terminal.
func (m Mood) worse() Mood {
	switch m {
	case MoodChill:
		return MoodEasy
	case MoodEasy:
		return MoodUneasy
	case MoodUneasy:
		return MoodWorried
	case MoodWorried:
		return MoodAlarmed
	default:
		return MoodPanic
	}
}

// PoolAccount is one account's contribution to the pool rollup.
type PoolAccount struct {
	HasUsage    bool
	RateLimited bool
	// WeeklyExhausted marks a weekly window — aggregate or model-scoped — pegged
	// with its reset pending; such an account can serve only by billing overage.
	WeeklyExhausted bool
	Remaining5h     float64 // percent 0..100
	Remaining7d     float64 // percent 0..100
	Burn5hPerHour   float64 // gated display 5h burn (Estimate.BurnPerHour)
	Burn7dPerHour   float64 // gated display 7d burn (Burn7dGated)
	Resets5h        time.Time
}

// Pool is the pool-wide rollup behind the widget's headline and mascot.
type Pool struct {
	// Remaining5h and Remaining7d are unweighted means over usable accounts: the
	// API exposes only percentages, so equal weights are the only honest aggregate.
	Remaining5h float64
	Remaining7d float64
	// BurnPerHour is the summed 5h drain across usable accounts (percent of one
	// account's window per hour); the Pace5h numerator, wire-live for --json.
	BurnPerHour float64
	// Pace5h is gross 5h burn over the pool's 5h regen rate (regen5hPerHour ×
	// usable accounts); 1.0 is the sustainable fixed point, above it a wall looms.
	Pace5h float64
	// Pace7d is gross 7d burn over the pool's 7d regen rate (regen7dPerHour ×
	// usable accounts); same 1.0-is-sustainable semantics over the weekly window.
	Pace7d float64
	// NetBurnPerHour is the projected change of the pool's mean 5h remaining over
	// the next hour (percentage points per hour), crediting refills that land
	// inside it. Positive drains, negative recovers; mean-based, unlike summed
	// BurnPerHour.
	NetBurnPerHour float64
	// DryAt is when the pool's combined 5h remaining hits 0 under the dryAt
	// forward simulation; zero means no wall lands within dryHorizon.
	DryAt time.Time
	Mood  Mood
}

// PoolOf rolls up account states; ok=false means no account has a known-good
// sample — never sampled, or only 429 placeholders — (the snapshot omits the
// pool block). Usable = has a good sample and not rate-limited: a 429 sample
// is a zeroed placeholder, so its remaining is fabricated. Stale accounts
// still count toward remaining; their burn is gated to 0.
func PoolOf(accts []PoolAccount, now time.Time) (Pool, bool) {
	sampled := false
	for _, a := range accts {
		if a.HasUsage {
			sampled = true
			break
		}
	}
	if !sampled {
		return Pool{}, false
	}

	var usableAccts []PoolAccount
	var sum5, sum7, burn, burn7, drop float64
	allWeeklyExhausted := true
	for _, a := range accts {
		if !a.HasUsage || a.RateLimited {
			continue
		}
		usableAccts = append(usableAccts, a)
		sum5 += clamp(a.Remaining5h)
		sum7 += clamp(a.Remaining7d)
		burn += a.Burn5hPerHour
		burn7 += a.Burn7dPerHour
		drop += netDrop(a, now)
		if !a.WeeklyExhausted {
			allWeeklyExhausted = false
		}
	}
	usable := len(usableAccts)
	var p Pool
	if usable > 0 {
		p.Remaining5h = sum5 / float64(usable)
		p.Remaining7d = sum7 / float64(usable)
		p.BurnPerHour = burn
		p.Pace5h = burn / (regen5hPerHour * float64(usable))
		p.Pace7d = burn7 / (regen7dPerHour * float64(usable))
		p.NetBurnPerHour = drop / float64(usable) / netBurnHorizon.Hours()
		p.DryAt = dryAt(usableAccts, sum5, burn, now)
	}
	p.Mood = moodOf(usable, p.Remaining5h, !p.DryAt.IsZero(), usable > 0 && allWeeklyExhausted)
	return p, true
}

const (
	// regen5hPerHour is one account's 5h-window refill in points per hour.
	regen5hPerHour = 100.0 / 5
	// regen7dPerHour is one account's 7d-window refill in points per hour.
	regen7dPerHour = 100.0 / (7 * 24)
	// resetPeriod5h spaces chained 5h resets: under burn a window reopens as soon
	// as it refills, so resets recur every 5h.
	resetPeriod5h = 5 * time.Hour
	// dryHorizon bounds the dry-out simulation: past ~5 reset cycles extrapolation
	// is noise, and the bound keeps the projected event list finite.
	dryHorizon = 24 * time.Hour
)

// dryAt forward-simulates the pool's combined 5h remaining to the first instant
// it hits 0, or zero time if no wall lands within dryHorizon. Each usable
// account drains its even share of the gross burn; each window's reset (known
// Resets5h, then chained every resetPeriod5h) credits back only what it used
// since last full. A homogeneous pool hits the fixed point pace = 1 exactly; a
// heterogeneous mix makes the even-share drain an approximation, so a rare
// conservative clock can surface on a pool pace alone calls sustainable. Ties
// go to the refill, matching Estimate5h's strict-before convention.
func dryAt(usableAccts []PoolAccount, sum5, burn float64, now time.Time) time.Time {
	if burn <= 0 {
		return time.Time{}
	}
	share := burn / float64(len(usableAccts))

	type event struct {
		at      time.Time
		lastRem float64 // remaining the window held when it was last full
		lastAt  time.Time
	}
	horizon := now.Add(dryHorizon)
	var events []event
	for _, a := range usableAccts {
		if !a.Resets5h.After(now) || !a.Resets5h.Before(horizon) {
			continue // reset past, zero, or beyond the horizon: no relief scheduled
		}
		events = append(events, event{at: a.Resets5h, lastRem: clamp(a.Remaining5h), lastAt: now})
		for t := a.Resets5h.Add(resetPeriod5h); t.Before(horizon); t = t.Add(resetPeriod5h) {
			events = append(events, event{at: t, lastRem: 100, lastAt: t.Add(-resetPeriod5h)})
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].at.Before(events[j].at) })

	total := sum5
	at := now
	for _, e := range events {
		dry := at.Add(hours(total / burn))
		if dry.Before(e.at) {
			return dry.Truncate(time.Second)
		}
		total -= burn * e.at.Sub(at).Hours()
		levelAtReset := max(0, e.lastRem-share*e.at.Sub(e.lastAt).Hours())
		total += 100 - levelAtReset
		at = e.at
	}
	dry := at.Add(hours(total / burn))
	if dry.Before(horizon) {
		return dry.Truncate(time.Second)
	}
	return time.Time{}
}

// moodRank orders the mood levels for floor comparisons; higher is more
// alarmed. worse() steps exactly one level, atLeast compares arbitrary pairs.
var moodRank = map[Mood]int{
	MoodChill:   0,
	MoodEasy:    1,
	MoodUneasy:  2,
	MoodWorried: 3,
	MoodAlarmed: 4,
	MoodPanic:   5,
}

// atLeast returns whichever of m or floor is the more-alarmed level, raising m
// up to floor but never lowering it.
func atLeast(m, floor Mood) Mood {
	if moodRank[floor] > moodRank[m] {
		return floor
	}
	return m
}

// moodOf maps mean remaining to an alarm level. When every usable account is
// weekly-exhausted the pool cannot serve its default-model work within plan
// limits however fresh the 5h windows, so the mood floors at MoodAlarmed; the
// dry-out bump then applies on top, so a floored-alarmed pool can still reach
// panic.
func moodOf(usable int, remaining5h float64, dryProjected, allWeeklyExhausted bool) Mood {
	if usable == 0 {
		return MoodPanic
	}
	var m Mood
	switch {
	case remaining5h >= 60:
		m = MoodChill
	case remaining5h >= 40:
		m = MoodEasy
	case remaining5h >= 25:
		m = MoodUneasy
	case remaining5h >= 10:
		m = MoodWorried
	default:
		m = MoodAlarmed
	}
	if allWeeklyExhausted {
		m = atLeast(m, MoodAlarmed)
	}
	if dryProjected {
		m = m.worse()
	}
	return m
}

// netBurnHorizon is the NetBurnPerHour lookahead: refills landing inside it
// are credited, later ones ignored.
const netBurnHorizon = time.Hour

// netDrop projects how many points of a's clamped 5h remaining vanish over the
// next netBurnHorizon; a reset inside the horizon refills to 100 then drains the
// rest, remaining floors at 0, and negative means refill outweighs burn.
func netDrop(a PoolAccount, now time.Time) float64 {
	start := clamp(a.Remaining5h)
	if a.Resets5h.After(now) && !a.Resets5h.After(now.Add(netBurnHorizon)) {
		rest := netBurnHorizon - a.Resets5h.Sub(now)
		return start - max(0, 100-a.Burn5hPerHour*rest.Hours())
	}
	return start - max(0, start-a.Burn5hPerHour*netBurnHorizon.Hours())
}

func clamp(v float64) float64 { return max(0, min(100, v)) }
