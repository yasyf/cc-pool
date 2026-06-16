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

// ErrMountsNotReady means every account has capacity but none has a mounted,
// healthy mirror right now — all are mid-migration, unmounted, or the mount
// holder is being replaced. Distinct from ErrNoneAvailable (a capacity verdict)
// so the surfaced message points at the mount layer, not at usage limits.
var ErrMountsNotReady = errors.New("no account is currently available: every account mirror is unmounted or being migrated (the mount holder may be mid-replacement) — retry shortly")

// SelectOptions tunes a selection.
type SelectOptions struct {
	// Live samples usage synchronously for accounts whose latest sample is
	// older than FreshFor (the M2/no-daemon path). When false, only cached
	// samples are used (the daemon keeps them fresh).
	Live bool
	// FreshFor is the cache window for Live sampling.
	FreshFor time.Duration
	// Cwd is the caller's working directory, keying select stickiness.
	// Empty disables stickiness.
	Cwd string
	// PID marks the launching process as a session checkout when > 0
	// (`ccp run` execs claude in-place, so its pid IS the claude pid),
	// feeding the sticky activity rules.
	PID int
	// NoFallback returns ErrNoneAvailable instead of a least-bad exhausted
	// pick. Set by --wait callers, which would discard the pick (and its
	// sticky rewrite) to keep waiting.
	NoFallback bool
}

// DefaultFreshFor is the default cache window for live selection.
const DefaultFreshFor = 60 * time.Second

// scanSessions is the live-session scan used by Select. Var so tests can
// substitute a canned scan — procscan runs the real `ps`, which would make
// session-reconciliation tests depend on the machine's process table.
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
	// ExhaustedFallback means every account was exhausted and Best is the
	// least-bad pick: it will bill extra-usage credits (if enabled) or
	// rate-limit until its reset. Callers must warn loudly.
	ExhaustedFallback bool
	// ExtraEnabled reports whether the pick has pay-as-you-go overage billing
	// enabled, for the fallback warning.
	ExtraEnabled bool
	// PinHeldAccount is the id of a manual pin whose account could not serve
	// this select (rate-limited, exhausted, or below the sticky headroom
	// floor); nil otherwise. The pin is kept — callers must surface that an
	// explicit pin was bypassed when Best differs from it.
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
		// Self-heal session bookkeeping when no daemon is around to reconcile:
		// rows held by dead pids would otherwise keep their cwd's pin alive
		// forever. Gated on the scan error, NOT on a nil slice — a successful
		// scan with zero claude processes is exactly the state where every
		// tracked row is dead and must be reaped. Best-effort, like every
		// other piece of select bookkeeping.
		_, _ = m.Store.CloseDeadSessions(procscan.AlivePIDs(sessions), now)
	}
	inputs := make([]score.Input, 0, len(accts))
	byID := make(map[int]store.Account, len(accts))
	inByID := make(map[int]score.Input, len(accts))
	extraByID := make(map[int]bool, len(accts))
	for _, a := range accts {
		byID[a.ID] = a
		in, samples, err := m.scoreInput(a, sessions, now)
		if err != nil {
			return nil, err
		}
		inByID[a.ID] = in
		extraByID[a.ID] = len(samples) > 0 && samples[0].ExtraEnabled
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
			// Every account is exhausted (or worse). Launch on the least-bad
			// exhausted one rather than refusing — the caller warns loudly.
			best, ok = score.PickFallback(ranked)
			fallback = true
		}
		if !ok {
			return &SelectResult{Ranked: ranked, byID: byID}, ErrNoneAvailable
		}
	}
	// A held pin stays untouched — unless the free ranking landed on the
	// pinned account anyway, in which case the select is genuine pin activity.
	// Best-effort: stickiness must never fail a select.
	if !outcome.Held() || best.AccountID == pin.AccountID {
		_ = m.RecordSticky(opts.Cwd, best.AccountID, now)
	}
	if opts.PID > 0 {
		// Best-effort, as above: a session row feeds the activity rules.
		_, _ = m.Store.OpenSession(best.AccountID, opts.PID, byID[best.AccountID].ConfigDir, opts.Cwd, now)
	}
	bi := inByID[best.AccountID]
	res := &SelectResult{Best: byID[best.AccountID], Result: best, Ranked: ranked,
		Sticky: outcome == StickyBind, HasUsage: bi.HasUsage, Util5h: bi.Util5h, Util7d: bi.Util7d,
		ExhaustedFallback: fallback, ExtraEnabled: extraByID[best.AccountID], byID: byID}
	if outcome == StickyHoldManual {
		held := pin.AccountID
		res.PinHeldAccount = &held
	}
	return res, nil
}

// sampleStale concurrently refreshes usage for accounts whose latest sample is
// older than freshFor. Accounts with a live session are sampled WITHOUT
// refreshing their token (that session owns refresh).
func (m *Manager) sampleStale(ctx context.Context, accts []store.Account, sessions []procscan.Session, freshFor time.Duration) {
	if freshFor <= 0 {
		freshFor = DefaultFreshFor
	}
	now := time.Now()
	var wg sync.WaitGroup
	for _, a := range accts {
		if s, ok, _ := m.Store.LatestUsageSample(a.ID); ok && now.Sub(s.TS) < freshFor {
			continue // fresh enough
		}
		a := a
		allowRefresh := procscan.CountByConfigDir(sessions, a.ConfigDir) == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			// The daemonless CLI path never busy-refreshes: only the daemon owns
			// the consecutive-401 streak that gates it.
			_, _, _ = m.SampleUsage(cctx, a, SampleOpts{AllowRefresh: allowRefresh})
		}()
	}
	wg.Wait()
}

// scoreInput assembles a score.Input for one account from cached state, also
// returning its recent samples (newest first) so callers can compute display
// forecasts and surface fields scoring ignores (extra usage). The slice is
// empty when the account was never sampled.
func (m *Manager) scoreInput(a store.Account, sessions []procscan.Session, now time.Time) (score.Input, []store.UsageSample, error) {
	in := score.Input{AccountID: a.ID}
	samples, err := m.Store.RecentUsageSamples(a.ID, forecast.BurnSampleLimit)
	if err != nil {
		return in, nil, err
	}
	if len(samples) > 0 {
		s := samples[0]
		in.HasUsage = true
		in.SampleTS = s.TS
		in.Util5h = s.Util5h
		in.Util7d = s.Util7d
		in.Resets5h = s.Resets5h
		in.Resets7d = s.Resets7d
		in.RateLimited = s.RateLimited
		in.Burn5hPerHour = forecast.Burn5h(samples, now)
	}
	in.ActiveSessions = procscan.CountByConfigDir(sessions, a.ConfigDir)
	if r, ok, _ := m.Store.LastRefresh(a.ID); ok && !r.OK {
		in.RefreshFailed = true
	}
	if h, err := m.Store.GetAuthHealth(a.ID); err == nil && h.NeedsLogin {
		in.NeedsLogin = true
	}
	return in, samples, nil
}

// PreflightRefresh refreshes the chosen account's token if it expires within
// RefreshLeadTime and the account is idle, so the launched session starts with
// a healthy token. Errors are returned but non-fatal to the caller.
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
