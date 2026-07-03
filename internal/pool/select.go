package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// ErrNoAccounts means the pool is empty.
var ErrNoAccounts = errors.New("no accounts in the pool — run `ccp add`")

// ErrNoneAvailable means no account can serve: every account is rate-limited,
// or — for NoFallback callers — exhausted.
var ErrNoneAvailable = errors.New("no account is currently available (all exhausted or rate-limited)")

// ErrMountsNotReady means every account has capacity but none has a healthy
// mounted mirror (mid-migration, unmounted, or holder mid-replacement).
var ErrMountsNotReady = errors.New("no account is currently available: every account mirror is unmounted or being migrated (the mount holder may be mid-replacement) — retry shortly")

// SelectOptions tunes a selection.
type SelectOptions struct {
	// Live samples usage synchronously for accounts staler than FreshFor
	// (no-daemon path); false uses only cached samples.
	Live bool
	// FreshFor is the cache window for Live sampling.
	FreshFor time.Duration
	// Cwd is the caller's working directory, keying select stickiness.
	// Empty disables stickiness.
	Cwd string
	// PID > 0 marks the launching process as a session checkout, feeding the
	// sticky activity rules (`ccp run` execs claude in-place: its pid IS claude's).
	PID int
	// NoFallback returns ErrNoneAvailable instead of a least-bad exhausted pick;
	// set by --wait callers, which would discard the pick to keep waiting.
	NoFallback bool
}

// DefaultFreshFor is the default cache window for live selection.
const DefaultFreshFor = 60 * time.Second

// scanSessions is Select's live-session scan; a var so tests can stub the real `ps`.
var scanSessions = procscan.Scan

// SelectResult is a ranked selection outcome.
type SelectResult struct {
	Best     store.Account
	Result   score.Result
	Ranked   []score.Result
	Sticky   bool    // the pick honored a sticky record rather than the ranking
	HasUsage bool    // the pick has at least one usage sample (false = never sampled)
	Util5h   float64 // the pick's raw 5h percent used (0 when never sampled)
	Util7d   float64 // the pick's raw 7d percent used (0 when never sampled)
	// ExhaustedFallback means every account was exhausted and Best is the least-bad
	// pick (bills overage if enabled, else rate-limits until reset); warn loudly.
	ExhaustedFallback bool
	// ExtraEnabled reports whether the pick has pay-as-you-go overage billing enabled.
	ExtraEnabled bool
	// Scoped7dModel is the API display name of the pick's binding model-scoped
	// weekly bucket ("" when the pick has no scoped bucket); Scoped7dUtil is that
	// bucket's raw percent used. Both feed the select announce line.
	Scoped7dModel string
	Scoped7dUtil  float64
	// PinHeldAccount is the id of a manual pin whose account could not serve this
	// select (rate-limited, exhausted, or below the sticky headroom floor), nil
	// otherwise; callers must surface the bypass when Best differs from it.
	PinHeldAccount *int
	byID           map[int]store.Account
}

// Select scores all accounts and returns the best available one.
func (m *Manager) Select(ctx context.Context, opts SelectOptions) (*SelectResult, error) {
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	if len(accts) == 0 {
		return nil, ErrNoAccounts
	}

	sessions, scanErr := scanSessions(ctx) // best-effort; nil sessions on error

	if opts.Live {
		m.sampleStale(ctx, accts, sessions, opts.FreshFor)
	}

	now := time.Now()
	if scanErr == nil {
		// Self-heal dead-pid session rows (no daemon reconciles); else their cwd
		// pins never expire. Gated on scan success, not on non-empty sessions: a
		// clean scan finding zero claude processes must still close every row.
		_, _ = m.Store.CloseDeadSessions(procscan.AlivePIDs(sessions), now)
	}
	inputs := make([]score.Input, 0, len(accts))
	byID := make(map[int]store.Account, len(accts))
	inByID := make(map[int]score.Input, len(accts))
	extraByID := make(map[int]bool, len(accts))
	scopedModelByID := make(map[int]string, len(accts))
	for _, a := range accts {
		byID[a.ID] = a
		in, _, good, err := m.scoreInput(a, sessions, now)
		if err != nil {
			return nil, err
		}
		inByID[a.ID] = in
		extraByID[a.ID] = good != nil && good.ExtraEnabled
		if good != nil {
			scopedModelByID[a.ID] = good.Scoped7dModel
		}
		inputs = append(inputs, in)
	}

	ranked := score.Rank(inputs, now)
	pin, outcome := m.StickyPick(opts.Cwd, ranked, now)
	best := pin
	fallback := false
	if outcome != StickyBind {
		var ok bool
		best, ok = score.Pick(ranked)
		if !ok && !opts.NoFallback {
			best, ok = score.PickFallback(ranked)
			fallback = true
		}
		if !ok {
			return &SelectResult{Ranked: ranked, byID: byID}, ErrNoneAvailable
		}
	}
	// Don't re-record a held pin unless the ranking landed on it anyway. Best-effort:
	// stickiness must never fail a select.
	if !outcome.Held() || best.AccountID == pin.AccountID {
		_ = m.RecordSticky(opts.Cwd, best.AccountID, now)
	}
	if opts.PID > 0 {
		// Best-effort: a session row feeds the activity rules.
		_, _ = m.Store.OpenSession(best.AccountID, opts.PID, byID[best.AccountID].ConfigDir, opts.Cwd, now)
	}
	bi := inByID[best.AccountID]
	res := &SelectResult{
		Best: byID[best.AccountID], Result: best, Ranked: ranked,
		Sticky: outcome == StickyBind, HasUsage: bi.HasUsage, Util5h: bi.Util5h, Util7d: bi.Util7d,
		ExhaustedFallback: fallback, ExtraEnabled: extraByID[best.AccountID], byID: byID,
		Scoped7dModel: scopedModelByID[best.AccountID], Scoped7dUtil: bi.Util7dScoped,
	}
	if outcome == StickyHoldManual {
		held := pin.AccountID
		res.PinHeldAccount = &held
	}
	return res, nil
}

// sampleStale concurrently refreshes usage for accounts staler than freshFor.
// Accounts with a live session skip the token refresh — that session owns it.
func (m *Manager) sampleStale(ctx context.Context, accts []store.Account, sessions []procscan.Session, freshFor time.Duration) {
	if freshFor <= 0 {
		freshFor = DefaultFreshFor
	}
	now := time.Now()
	var wg sync.WaitGroup
	for _, a := range accts {
		if s, ok, _ := m.Store.LatestUsageSample(a.ID); ok && now.Sub(s.TS) < freshFor {
			continue
		}
		a := a
		allowRefresh := procscan.CountByConfigDir(sessions, a.ConfigDir) == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			// Daemonless path never busy-refreshes: only the daemon owns the
			// consecutive-401 streak that gates it.
			_, _, _ = m.SampleUsage(cctx, a, SampleOpts{AllowRefresh: allowRefresh})
		}()
	}
	wg.Wait()
}

// scoreInput assembles a score.Input for one account from cached state, the
// recent samples (newest first) for forecasts, and the last known-good sample
// (nil if never sampled cleanly).
func (m *Manager) scoreInput(a store.Account, sessions []procscan.Session, now time.Time) (score.Input, []store.UsageSample, *store.UsageSample, error) {
	in := score.Input{AccountID: a.ID}
	samples, err := m.Store.UsageSamplesSince(a.ID, now.Add(-forecast.Burn7dWindow))
	if err != nil {
		return in, nil, nil, err
	}
	var good *store.UsageSample
	if len(samples) > 0 {
		s := samples[0]
		in.HasUsage = true
		in.RateLimited = s.RateLimited
		in.Burn5hPerHour = forecast.Burn5h(samples, now)
		// Utilization, resets, timestamp, and overage source from the last good
		// sample, not samples[0]: a 429 poll records a zeroed rate_limited
		// placeholder as the newest row (load-bearing for daemon backoff), so
		// samples[0] would read 0%/no-overage for a rate-limited account.
		// ok=false leaves them honestly zero.
		if g, ok, gerr := m.Store.LatestGoodUsageSample(a.ID); gerr != nil {
			return in, nil, nil, fmt.Errorf("latest good usage sample for account %d: %w", a.ID, gerr)
		} else if ok {
			good = &g
			in.SampleTS = g.TS
			in.Util5h = g.Util5h
			in.Util7d = g.Util7d
			in.Resets5h = g.Resets5h
			in.Resets7d = g.Resets7d
			in.HasScoped7d = g.Scoped7dModel != ""
			in.Util7dScoped = g.Scoped7dUtil
			in.Resets7dScoped = g.Scoped7dResets
		}
	}
	in.ActiveSessions = procscan.CountByConfigDir(sessions, a.ConfigDir)
	if r, ok, _ := m.Store.LastRefresh(a.ID); ok && !r.OK {
		in.RefreshFailed = true
	}
	if h, err := m.Store.GetAuthHealth(a.ID); err == nil && h.NeedsLogin {
		in.NeedsLogin = true
	}
	return in, samples, good, nil
}

// PreflightRefresh refreshes the chosen account's token when it expires within
// RefreshLeadTime and the account is idle. Errors are returned but non-fatal.
func (m *Manager) PreflightRefresh(ctx context.Context, a store.Account) error {
	sessions, _ := procscan.Scan(ctx)
	idle := procscan.CountByConfigDir(sessions, a.ConfigDir) == 0
	if !idle {
		return nil
	}
	_, _, err := m.EnsureFreshToken(ctx, a, RefreshLeadTime, true)
	if err != nil && !errors.Is(err, ErrNeedsLogin) {
		return fmt.Errorf("preflight refresh: %w", err)
	}
	return err
}

// SoonestReset returns the earliest 5h reset across the pool, for `--wait`.
func (sr *SelectResult) SoonestReset() (time.Time, bool) {
	return score.SoonestReset(sr.Ranked)
}
