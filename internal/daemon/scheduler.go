package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/proc"
)

const (
	// The /usage and token endpoints rate-limit aggressively: 30–60s polling
	// can trip a 30+ minute 429 with no Retry-After.
	basePollInterval = 180 * time.Second
	pollJitter       = 30 * time.Second

	// Per-account spacing so N accounts don't hit the shared-IP bucket at once.
	perAccountSpacing = 2 * time.Second

	rateLimitBackoffBase = 3 * time.Minute
	rateLimitBackoffCap  = 15 * time.Minute

	needsLoginAfter = 3
	// Balances per-poll 401 spam against auto-recovery latency after `ccp login`.
	needsLoginPollInterval = 15 * time.Minute
)

func rlBackoff(streak int) time.Duration {
	return proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap}.After(streak)
}

func (s *Server) scheduler(ctx context.Context) {
	s.pollOnce(ctx)
	for {
		d := basePollInterval + jitter(pollJitter, time.Now().UnixNano())
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
			s.pollOnce(ctx)
		}
	}
}

func (s *Server) pollOnce(ctx context.Context) {
	// Every select until the next poll keys fuse readiness on this cache.
	s.holder.refresh(s.holderClient())

	// Reconcile only on a successful scan: AlivePIDs always returns a non-nil
	// map, so a failed scan would close every live session.
	sessions, err := procscan.Scan(ctx)
	if err != nil {
		s.log.Printf("procscan: %v", err)
	} else {
		switch n, err := s.m.Store.CloseDeadSessions(procscan.AlivePIDs(sessions), time.Now()); {
		case err != nil:
			s.log.Printf("close dead sessions: %v", err)
		case n > 0:
			s.log.Printf("reconciled %d ended session(s)", n)
		}
	}

	// Row hygiene only: StickyPick re-checks the activity rule on read (covers
	// the daemonless path).
	if _, err := s.m.Store.PruneSticky(time.Now().Add(-pool.StickyTTL)); err != nil {
		s.log.Printf("prune sticky: %v", err)
	}

	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		s.log.Printf("list accounts: %v", err)
		return
	}
	for i, a := range accts {
		if !s.beginPoll(a.ID) {
			continue
		}
		if s.pollAccount(ctx, sessions, i, a) {
			return
		}
	}

	// Deliberately skipped by the early returns above: generated_at means "time
	// of the last completed poll" and must go stale when polling is broken.
	if err := s.writeStatusSnapshot(ctx); err != nil {
		s.log.Printf("status snapshot: %v", err)
	}
}

// pollAccount polls one account under the claim taken by pollOnce, which it
// releases.
func (s *Server) pollAccount(ctx context.Context, sessions []procscan.Session, i int, a store.Account) (stop bool) {
	defer s.endPoll(a.ID)

	if last, ok, _ := s.m.Store.LatestUsageSample(a.ID); ok && last.RateLimited &&
		time.Since(last.TS) < rlBackoff(s.rlStreak[a.ID]) {
		return false
	}
	// The needs-login backoff needs its own clock: a 401 inserts no usage
	// sample, so it can't ride the rate-limit backoff above.
	if health, _ := s.m.Store.GetAuthHealth(a.ID); health.NeedsLogin || s.authStreak[a.ID] >= needsLoginAfter {
		if last, ok := s.lastAuthAttempt[a.ID]; ok && time.Since(last) < needsLoginPollInterval {
			return false
		}
	}
	if i > 0 {
		select {
		case <-ctx.Done():
			return true
		case <-time.After(perAccountSpacing):
		}
	}
	// Re-assert the overlay so long-lived setups pick up new top-level
	// ~/.claude entries without an explicit sync.
	if err := s.m.SyncOverlay(a); err != nil {
		s.log.Printf("acct-%02d overlay sync: %v", a.ID, err)
		// A fuse sync failure usually means the mount is down — heal now
		// instead of leaving the dir dead until restart. A File Provider sync
		// failure reconciles through the domain host instead (Health, then an
		// idempotent Setup); the two arms never mix.
		switch {
		case fuseBackedRow(a.OverlayKind):
			s.healFuse(ctx, a)
		case fpBackedRow(a.OverlayKind):
			s.reconcileFileProvider(ctx, a)
		}
	}

	// A reserved account's claude may not be procscan-visible yet — treat it
	// as busy so we don't refresh the token out from under the launch.
	idle := procscan.CountByConfigDir(sessions, a.ConfigDir) == 0 &&
		s.reservedCount(a.ID) == 0

	// A just-idled account may carry a token rotated by its session — adopt
	// before sampling.
	if idle {
		switch err := s.m.AdoptRotatedToken(ctx, a); {
		case err != nil:
			s.log.Printf("acct-%02d adopt rotated token: %v", a.ID, err)
		case fpBackedRow(a.OverlayKind):
			// The adoption rewrote the account's credential state; nudge the
			// domain (re-Ensure + re-link + Signal) so the OS re-enumerates the
			// rotated merged .claude.json instead of serving a stale replica.
			if prov := s.overlayForRow(a); prov != nil {
				if serr := prov.Sync(pool.ClaudeDir(), a.ConfigDir); serr != nil {
					s.log.Printf("acct-%02d file provider sync after token adoption: %v", a.ID, serr)
				}
			}
		}
	}

	// The prior-401 gate gives a lazily-waking session one full poll to
	// self-refresh first. Accepted gap: a busy, clock-fresh, server-revoked
	// token keeps 401ing (fetchUsage's busy guard requires expiry) so
	// needs-login waits for clock expiry — the live session owns recovery;
	// refreshing sooner could double-spend a refresh token it still needs.
	busyBySession := procscan.CountByConfigDir(sessions, a.ConfigDir) > 0
	opts := pool.SampleOpts{
		AllowRefresh:     idle,
		AllowBusyRefresh: busyBySession && s.authStreak[a.ID] >= 1,
	}

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, rateLimited, err := s.m.SampleUsage(cctx, a, opts)
	cancel()
	// rlStreak is scheduler-goroutine-local — no lock.
	if rateLimited {
		s.rlStreak[a.ID]++
	} else if err == nil {
		s.rlStreak[a.ID] = 0
	}
	s.handleAuthOutcome(a, err)
	return false
}

// Only a definitive ErrNeedsLogin flags needs-login: a plain 401 also
// surfaces transient (5xx/network) refresh failures, so the streak only
// throttles polling. Scheduler-goroutine-local — no lock.
func (s *Server) handleAuthOutcome(a store.Account, err error) {
	if err == nil {
		s.authStreak[a.ID] = 0
		if changed, cerr := s.m.Store.ClearNeedsLogin(a.ID); cerr != nil {
			s.log.Printf("acct-%02d clear needs-login: %v", a.ID, cerr)
		} else if changed {
			s.log.Printf("acct-%02d auth recovered; needs-login cleared", a.ID)
		}
		return
	}
	if errors.Is(err, pool.ErrNeedsLogin) {
		s.flagNeedsLogin(a, err)
		return
	}
	var ue *oauth.UsageError
	if errors.As(err, &ue) && ue.Unauthorized() {
		s.authStreak[a.ID]++
		s.lastAuthAttempt[a.ID] = time.Now()
		s.log.Printf("acct-%02d sample: %v", a.ID, err)
		return
	}
	s.log.Printf("acct-%02d sample: %v", a.ID, err)
}

// flagNeedsLogin stamps the attempt clock so the needs-login backoff engages.
func (s *Server) flagNeedsLogin(a store.Account, err error) {
	s.lastAuthAttempt[a.ID] = time.Now()
	changed, serr := s.m.Store.SetNeedsLogin(a.ID, time.Now(), err.Error())
	if serr != nil {
		s.log.Printf("acct-%02d set needs-login: %v", a.ID, serr)
		return
	}
	if changed {
		s.log.Printf("acct-%02d needs re-login — run `ccp login %d`: %v", a.ID, a.ID, err)
	}
}

// jitter is deterministic (seed-derived) rather than RNG-backed, for
// reproducibility.
func jitter(span time.Duration, seed int64) time.Duration {
	if span <= 0 {
		return 0
	}
	if seed < 0 {
		seed = -seed
	}
	return time.Duration(seed % int64(span))
}
