package pool

import (
	"context"
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// Snapshot is a fully-resolved per-account view for status/list rendering. It
// is provider- and transport-neutral so both the live CLI path and the daemon
// produce the same shape.
type Snapshot struct {
	Account        store.Account
	Score          float64
	HasUsage       bool
	Util5h         float64 // percent used 0..100
	Util7d         float64
	Remaining5h    float64
	Remaining7d    float64
	ActiveSessions int
	RateLimited    bool
	Exhausted      bool // a window is fully used and its reset is still pending
	NeedsLogin     bool // refresh token gone/revoked; only `ccp login` recovers it
	Stale          bool
	Resets5h       time.Time
	Resets7d       time.Time
	// Burn5hPerHour is the ungated scoring burn: it feeds score.Input
	// rebuilds (the daemon's reservation re-rank) even when the sample is
	// stale. Display consumers use Forecast instead.
	Burn5hPerHour float64
	// Forecast is the gated display forecast — zero when the account is
	// idle, stale, rate-limited, or exhausted. Only it reaches the status
	// wire's prediction fields.
	Forecast  forecast.Estimate
	SampleAge time.Duration
	// Extra-usage (pay-as-you-go overage) state from the latest sample, for
	// status display: an exhausted account with ExtraEnabled bills credits
	// instead of rate-limiting.
	ExtraEnabled bool
	ExtraUsed    float64 // credits consumed this month (currency cents)
	ExtraLimit   float64 // credit cap (currency cents)
	// Components is the per-term score breakdown, so status can explain why an
	// account scored what it did without recomputing.
	Components score.Components
}

// Snapshots returns a scored view of every account. When live is true, stale
// usage is sampled synchronously first (the no-daemon path).
func (m *Manager) Snapshots(ctx context.Context, live bool, fresh time.Duration) ([]Snapshot, error) {
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	sessions, _ := procscan.Scan()
	if live {
		m.sampleStale(ctx, accts, sessions, fresh)
	}
	now := time.Now()

	inputs := make([]score.Input, len(accts))
	samples := make([][]store.UsageSample, len(accts))
	for i, a := range accts {
		in, recent, err := m.scoreInput(a, sessions, now)
		if err != nil {
			return nil, err
		}
		inputs[i] = in
		samples[i] = recent
	}
	results := make(map[int]score.Result)
	for _, r := range score.Rank(inputs, now) {
		results[r.AccountID] = r
	}

	out := make([]Snapshot, 0, len(accts))
	for i, a := range accts {
		in := inputs[i]
		r := results[a.ID]
		var latest store.UsageSample
		if len(samples[i]) > 0 {
			latest = samples[i][0]
		}
		s := Snapshot{
			Account:        a,
			Score:          r.Score,
			HasUsage:       in.HasUsage,
			Util5h:         in.Util5h,
			Util7d:         in.Util7d,
			Remaining5h:    100 - in.Util5h,
			Remaining7d:    100 - in.Util7d,
			ActiveSessions: in.ActiveSessions,
			RateLimited:    in.RateLimited,
			Exhausted:      r.Exhausted,
			NeedsLogin:     r.NeedsLogin,
			Stale:          r.Stale,
			Resets5h:       in.Resets5h,
			Resets7d:       in.Resets7d,
			Burn5hPerHour:  in.Burn5hPerHour,
			Forecast:       forecast.Estimate5h(samples[i], r.Exhausted, now),
			ExtraEnabled:   latest.ExtraEnabled,
			ExtraUsed:      latest.ExtraUsed,
			ExtraLimit:     latest.ExtraLimit,
			Components:     r.Components,
		}
		if in.HasUsage {
			s.SampleAge = now.Sub(in.SampleTS)
		}
		out = append(out, s)
	}
	return out, nil
}
