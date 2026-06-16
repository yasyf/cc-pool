package forecast

import (
	"sort"
	"time"
)

// Mood is the pool-health alarm level, computed daemon-side so every consumer
// (widget mascot, CLI) agrees on one source of truth. Levels order
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
	HasUsage      bool
	RateLimited   bool
	Remaining5h   float64 // percent 0..100
	Remaining7d   float64 // percent 0..100
	Burn5hPerHour float64 // gated display 5h burn (Estimate.BurnPerHour)
	Burn7dPerHour float64 // gated display 7d burn (Burn7dGated)
	Resets5h      time.Time
}

// Pool is the pool-wide rollup behind the widget's headline and mascot.
type Pool struct {
	// Remaining5h and Remaining7d are unweighted means over usable accounts —
	// the API exposes only percentages, never absolute plan capacity, so
	// equal weights are the only honest aggregate.
	Remaining5h float64
	Remaining7d float64
	// BurnPerHour is the summed 5h drain across usable accounts, in
	// percent-of-one-account's-window per hour. It is the Pace5h numerator and
	// stays wire-live for --json consumers.
	BurnPerHour float64
	// Pace5h is the gross 5h burn divided by the pool's 5h regeneration rate
	// (regen5hPerHour × usable accounts). 1.0 is the sustainable fixed point:
	// below it the pool refills indefinitely, above it a wall is coming.
	Pace5h float64
	// Pace7d is the gross 7d burn divided by the pool's 7d regeneration rate
	// (regen7dPerHour × usable accounts), with the same 1.0-is-sustainable
	// semantics over the weekly window.
	Pace7d float64
	// NetBurnPerHour is the projected change of the pool's mean 5h remaining
	// over the next hour, in percentage points per hour, crediting 5h-window
	// refills that land inside that hour. Positive means draining; negative
	// means refills outpace burn (the pool is recovering). Unlike the summed
	// BurnPerHour it is mean-based, so it describes exactly how fast
	// Remaining5h moves.
	NetBurnPerHour float64
	// DryAt is when the pool's combined 5h remaining hits 0 under a forward
	// simulation that drains every usable account by its even share and
	// credits each window's reset (known, then chained every resetPeriod5h).
	// Zero means no wall lands within dryHorizon — a farther wall is pace's
	// story, not a clock's.
	DryAt time.Time
	Mood  Mood
}

// PoolOf rolls up account states. ok=false means no account has ever been
// sampled — the snapshot omits the pool block entirely.
//
// Usable means sampled and not rate-limited: a rate-limited account cannot
// serve, and its latest sample is the zeroed 429 placeholder, so its
// "remaining" is fabricated. Stale accounts still count toward remaining
// (last known data is the best estimate); their burn is already gated to 0.
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
	p.Mood = moodOf(usable, p.Remaining5h, !p.DryAt.IsZero())
	return p, true
}

const (
	// regen5hPerHour is how fast one account's 5h window refills, in points per
	// hour (a full 100% window every 5h).
	regen5hPerHour = 100.0 / 5
	// regen7dPerHour is how fast one account's 7d window refills, in points per
	// hour (a full 100% window every 7×24h).
	regen7dPerHour = 100.0 / (7 * 24)
	// resetPeriod5h is the assumed spacing of chained 5h resets: under burn,
	// selection reopens a window the moment it refills, so future resets recur
	// every 5h.
	resetPeriod5h = 5 * time.Hour
	// dryHorizon bounds the dry-out simulation. Burns are short secants;
	// extrapolating past ~5 reset cycles is noise, a farther wall is pace's
	// story, and the bound keeps the projected event list finite.
	dryHorizon = 24 * time.Hour
)

// dryAt forward-simulates the pool's combined 5h remaining to find the first
// instant it hits 0, or zero time if no wall lands within dryHorizon. Each
// usable account drains its even share of the gross burn; each window's reset
// (the known Resets5h, then a chain every resetPeriod5h out to dryHorizon)
// credits back only what that window actually used since it was last full —
// 100 − max(0, lastRem − share·Δt). For a homogeneous pool the fixed point is
// exactly pace = 1; a heterogeneous mix (idle or exhausted low-remaining
// members) makes the even-share drain an approximation, so a rare conservative
// clock can surface on a pool that pace alone calls sustainable. Ties (dry
// landing exactly on a reset) go to the refill, matching the strict-before
// convention in Estimate5h. Past, zero, or beyond-horizon resets contribute
// their remaining but schedule no relief: they neither suppress a later wall
// (the old naive bug) nor fabricate one.
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
		total -= burn * e.at.Sub(at).Hours() // drain up to the reset
		levelAtReset := max(0, e.lastRem-share*e.at.Sub(e.lastAt).Hours())
		total += 100 - levelAtReset // credit the window back to full
		at = e.at
	}
	dry := at.Add(hours(total / burn))
	if dry.Before(horizon) {
		return dry.Truncate(time.Second)
	}
	return time.Time{}
}

// moodOf maps pool state to an alarm level: thresholds on mean remaining,
// bumped one level worse when a dry-out is projected — the forward simulation
// hits 0 inside dryHorizon (the overshoot signal).
func moodOf(usable int, remaining5h float64, dryProjected bool) Mood {
	if usable == 0 {
		return MoodPanic // nothing can serve right now
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
	if dryProjected {
		m = m.worse()
	}
	return m
}

// netBurnHorizon is the NetBurnPerHour lookahead: refills landing inside it
// are credited, later ones ignored.
const netBurnHorizon = time.Hour

// netDrop projects how many points of a's clamped 5h remaining vanish over
// the next netBurnHorizon: a window resetting inside the horizon refills to
// 100 and keeps draining for the rest of it, and remaining never drains below
// 0. Negative means the refill outweighs the burn.
func netDrop(a PoolAccount, now time.Time) float64 {
	start := clamp(a.Remaining5h)
	if a.Resets5h.After(now) && !a.Resets5h.After(now.Add(netBurnHorizon)) {
		rest := netBurnHorizon - a.Resets5h.Sub(now)
		return start - max(0, 100-a.Burn5hPerHour*rest.Hours())
	}
	return start - max(0, start-a.Burn5hPerHour*netBurnHorizon.Hours())
}

func clamp(v float64) float64 { return max(0, min(100, v)) }
