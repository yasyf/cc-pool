package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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

// DefaultFreshFor is the default cache window for live selection.
const DefaultFreshFor = 60 * time.Second

// sampleStale concurrently refreshes usage for accounts staler than freshFor.
// Accounts with a live session skip the token refresh — that session owns it.
// A failed scan (scanOK false) samples usage but refreshes nothing.
func (m *Manager) sampleStale(ctx context.Context, accts []store.Account, sessions []procscan.Session, scanOK bool, freshFor time.Duration) {
	if freshFor <= 0 {
		freshFor = DefaultFreshFor
	}
	if !scanOK {
		log.Printf("pool: procscan failed; sampling usage without any token refresh this pass")
	}
	now := time.Now()
	var wg sync.WaitGroup
	for _, a := range accts {
		if s, ok, _ := m.Store.LatestUsageSample(a.ID); ok && now.Sub(s.TS) < freshFor {
			continue
		}
		a := a
		allowRefresh := scanOK && procscan.CountByConfigDir(sessions, AccountPresentationDir(a.ID)) == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			// One-shot live sampling never busy-refreshes: only the daemon scheduler
			// owns the consecutive-401 streak that gates it.
			_, _, _, _ = m.SampleUsage(cctx, a, SampleOpts{AllowRefresh: allowRefresh})
		}()
	}
	wg.Wait()
}

// scoreInput assembles a score.Input for one account from cached state, the
// recent samples (newest first) for forecasts, the last known-good sample (nil
// if never sampled cleanly), and whether a needs-login is an awaiting-origin
// (synced peer copy) rather than an owned dead chain.
func (m *Manager) scoreInput(ctx context.Context, a store.Account, sessions []procscan.Session, now time.Time) (score.Input, []store.UsageSample, *store.UsageSample, bool, error) {
	in := score.Input{AccountID: a.ID}
	samples, err := m.Store.UsageSamplesSince(a.ID, now.Add(-forecast.Burn7dWindow))
	if err != nil {
		return in, nil, nil, false, err
	}
	var good *store.UsageSample
	if len(samples) > 0 {
		s := samples[0]
		in.RateLimited = s.RateLimited
		in.Burn5hPerHour = forecast.Burn5h(samples, now)
		// Utilization, resets, timestamp, and overage source from the last GOOD
		// sample, not samples[0]: a 429 poll records a zeroed rate_limited placeholder
		// as the newest row (load-bearing for daemon backoff), so samples[0] would read
		// 0% for a rate-limited account. HasUsage gates on the good sample too. See ccn
		// doc 36b05ef.
		if g, ok, gerr := m.Store.LatestGoodUsageSample(a.ID); gerr != nil {
			return in, nil, nil, false, fmt.Errorf("latest good usage sample for account %d: %w", a.ID, gerr)
		} else if ok {
			good = &g
			in.HasUsage = true
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
	in.ActiveSessions = procscan.CountByConfigDir(sessions, AccountPresentationDir(a.ID))
	if r, ok, _ := m.Store.LastRefresh(a.ID); ok && r.Category != store.RefreshSucceeded {
		in.RefreshFailed = true
	}
	if _, err := m.Store.CredentialQuarantine(a.ID); err == nil {
		in.CredentialQuarantined = true
		if m.Creds != nil {
			if _, resolveErr := m.credentialMutationObservation(ctx, a); resolveErr == nil {
				in.CredentialQuarantined = false
			} else if !errors.Is(resolveErr, ErrCredentialOperationQuarantined) {
				return in, nil, nil, false, fmt.Errorf(
					"resolve credential quarantine for account %d: %w", a.ID, resolveErr,
				)
			}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return in, nil, nil, false, fmt.Errorf("credential quarantine for account %d: %w", a.ID, err)
	}
	awaitingOrigin := false
	if h, err := m.Store.GetAuthHealth(a.ID); err == nil && h.NeedsLogin {
		in.NeedsLogin = true
		awaitingOrigin = h.Kind == store.AuthKindAwaitingOrigin
	}
	return in, samples, good, awaitingOrigin, nil
}

// PreflightRefresh refreshes the chosen account's token when it expires within
// RefreshLeadTime and the account is idle. ErrNeedsLogin and ErrUnrefreshable
// pass through unwrapped so selection can fail closed or pull a peer update.
func (m *Manager) PreflightRefresh(ctx context.Context, a store.Account) error {
	if m.Store != nil && m.Creds != nil {
		if _, err := m.credentialMutationObservation(ctx, a); err != nil {
			return fmt.Errorf("credential preflight: %w", err)
		}
	}
	sessions, err := m.scanSessions(ctx)
	if err != nil {
		// Fail closed: a failed scan can't prove idle — skip the refresh.
		log.Printf("acct-%d preflight refresh: procscan failed; skipping refresh: %v", a.ID, err)
		return nil
	}
	if procscan.CountByConfigDir(sessions, AccountPresentationDir(a.ID)) != 0 {
		return nil
	}
	_, _, ferr := m.EnsureFreshToken(ctx, a, RefreshLeadTime, true)
	if ferr != nil && !errors.Is(ferr, ErrNeedsLogin) && !errors.Is(ferr, ErrUnrefreshable) {
		return fmt.Errorf("preflight refresh: %w", ferr)
	}
	return ferr
}
