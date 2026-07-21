package pool

import (
	"context"
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// Snapshot is a fully-resolved per-account view for status/list rendering. It
// is provider- and transport-neutral so both the live CLI path and the daemon
// produce the same shape.
type Snapshot struct {
	Account               store.Account
	Score                 float64
	HasUsage              bool    // a known-good sample exists (false = never sampled, or only 429 placeholders)
	Util5h                float64 // percent used 0..100
	Util7d                float64
	Remaining5h           float64
	Remaining7d           float64
	ActiveSessions        int
	RateLimited           bool
	Exhausted             bool // a window is fully used and its reset is still pending
	NeedsLogin            bool // refresh token gone/revoked; only `ccp login` recovers it
	CredentialQuarantined bool // exact credential state requires reconciliation
	// AwaitingOrigin narrows NeedsLogin: this is a synced peer copy whose token
	// expired, so it recovers when the origin's rotation syncs over (or a local
	// `ccp login`). Display-only — scoring treats it identically to NeedsLogin.
	AwaitingOrigin bool
	Stale          bool
	Resets5h       time.Time
	Resets7d       time.Time
	// Burn5hPerHour is the ungated scoring burn: feeds score.Input even when the
	// sample is stale. Display consumers use Forecast instead.
	Burn5hPerHour float64
	// Forecast is the gated display forecast — zero when idle, stale,
	// rate-limited, or exhausted; only it reaches the status wire's prediction
	// fields.
	Forecast forecast.Estimate
	// Burn7dPerHour is the gated 7d display drain (percent/hour), separate from
	// the 5h Forecast; feeds only the pool pace rollup. Zero when idle, stale,
	// rate-limited, exhausted, or past its 7d reset.
	Burn7dPerHour float64
	SampleAge     time.Duration
	// Extra-usage (pay-as-you-go overage) from the latest sample: an exhausted
	// account with ExtraEnabled bills credits instead of rate-limiting.
	ExtraEnabled bool
	ExtraUsed    float64 // credits consumed this month (currency cents)
	ExtraLimit   float64 // credit cap (currency cents)
	// Scoped7dUtil/Scoped7dResets/Scoped7dModel carry the account's binding
	// model-scoped weekly bucket (e.g. the Fable 5 weekly cap) from the last
	// known-good sample, like ExtraEnabled. Scoped7dModel is "" when no scoped
	// bucket was sampled — the presence signal.
	Scoped7dUtil   float64 // percent used 0..100 of the scoped weekly bucket
	Scoped7dResets time.Time
	Scoped7dModel  string
	// Components is the per-term score breakdown, so status can explain a score
	// without recomputing.
	Components score.Components
	// WeeklyExhausted reports that a weekly window — aggregate or model-scoped —
	// is pegged with its reset pending (from the ranked score.Result); feeds the
	// pool mood.
	WeeklyExhausted bool
}

// Snapshots returns a scored view of every account. When live is true, stale
// usage is sampled synchronously first for a live status view.
func (m *Manager) Snapshots(ctx context.Context, live bool, fresh time.Duration) ([]Snapshot, error) {
	accts, err := m.Store.ListActiveAccounts()
	if err != nil {
		return nil, err
	}
	sessions, scanErr := m.scanSessions(ctx)
	if live {
		m.sampleStale(ctx, accts, sessions, scanErr == nil, fresh)
	}
	now := time.Now()

	inputs := make([]score.Input, len(accts))
	samples := make([][]store.UsageSample, len(accts))
	goods := make([]*store.UsageSample, len(accts))
	awaiting := make([]bool, len(accts))
	for i, a := range accts {
		in, recent, good, awaitingOrigin, err := m.scoreInput(ctx, a, sessions, now)
		if err != nil {
			return nil, err
		}
		inputs[i] = in
		samples[i] = recent
		goods[i] = good
		awaiting[i] = awaitingOrigin
	}
	results := make(map[int]score.Result)
	for _, r := range score.Rank(inputs, now) {
		results[r.AccountID] = r
	}

	out := make([]Snapshot, 0, len(accts))
	for i, a := range accts {
		in := inputs[i]
		r := results[a.ID]
		// Extra-usage reads through to the last known-good sample: a rate-limit
		// placeholder zeroes it.
		var good store.UsageSample
		if goods[i] != nil {
			good = *goods[i]
		}
		s := Snapshot{
			Account:               a,
			Score:                 r.Score,
			HasUsage:              in.HasUsage,
			Util5h:                in.Util5h,
			Util7d:                in.Util7d,
			Remaining5h:           100 - in.Util5h,
			Remaining7d:           100 - in.Util7d,
			ActiveSessions:        in.ActiveSessions,
			RateLimited:           in.RateLimited,
			Exhausted:             r.Exhausted,
			NeedsLogin:            r.NeedsLogin,
			CredentialQuarantined: r.CredentialQuarantined,
			AwaitingOrigin:        awaiting[i],
			Stale:                 r.Stale,
			Resets5h:              in.Resets5h,
			Resets7d:              in.Resets7d,
			Burn5hPerHour:         in.Burn5hPerHour,
			Forecast:              forecast.Estimate5h(samples[i], r.Exhausted, now),
			Burn7dPerHour:         forecast.Burn7dGated(samples[i], r.Exhausted, now),
			ExtraEnabled:          good.ExtraEnabled,
			ExtraUsed:             good.ExtraUsed,
			ExtraLimit:            good.ExtraLimit,
			Scoped7dUtil:          good.Scoped7dUtil,
			Scoped7dResets:        good.Scoped7dResets,
			Scoped7dModel:         good.Scoped7dModel,
			Components:            r.Components,
			WeeklyExhausted:       r.WeeklyExhausted,
		}
		if in.HasUsage {
			s.SampleAge = now.Sub(in.SampleTS)
		}
		out = append(out, s)
	}
	return out, nil
}
