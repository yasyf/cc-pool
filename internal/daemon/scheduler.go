package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

const (
	// The /usage and token endpoints rate-limit aggressively: polling every
	// 30–60s can trip a 30+ minute 429 with no Retry-After. 180s + jitter keeps
	// us well clear while still feeling live for start-of-session selection.
	basePollInterval = 180 * time.Second
	pollJitter       = 30 * time.Second

	// Per-account spacing so N accounts don't hit the shared-IP bucket at once.
	perAccountSpacing = 2 * time.Second

	// Exponential rate-limit backoff: 3, 6, 12, … capped at 15 minutes.
	rateLimitBackoffBase = 3 * time.Minute
	rateLimitBackoffCap  = 15 * time.Minute

	// needsLoginAfter is how many consecutive unrecovered 401s flag an account
	// needs-login (a definitive ErrNeedsLogin flags it immediately). At 180s+
	// per poll, 3 is ~10 minutes of confirmed failure before we surface it.
	needsLoginAfter = 3
	// needsLoginPollInterval is how often a flagged account is re-sampled: often
	// enough to auto-recover within minutes of `ccp login`, rarely enough to
	// stop the per-poll 401 spam.
	needsLoginPollInterval = 15 * time.Minute
)

// rlBackoff returns the backoff duration for a given consecutive-429 streak.
func rlBackoff(streak int) time.Duration {
	return backoffAfter(streak, rateLimitBackoffBase, rateLimitBackoffCap)
}

// scheduler runs the periodic usage poll + idle-only refresh + session
// reconciliation until ctx is cancelled.
func (s *Server) scheduler(ctx context.Context) {
	// Prime immediately so status/select have data right away.
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

// pollOnce reconciles sessions, then samples usage for every account,
// refreshing the token only for idle accounts.
func (s *Server) pollOnce(ctx context.Context) {
	// Refresh the holder cache first: every select until the next poll keys
	// fuse readiness on it. (superviseHolder owns respawn policy; this is
	// only the cache.)
	s.holder.refresh(s.holderClient())

	// Only reconcile sessions on a successful scan: AlivePIDs always returns a
	// non-nil map, so reconciling off a failed (nil) scan would treat every PID
	// as dead and close every active session.
	sessions, err := procscan.Scan()
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

	// Row hygiene only: StickyPick checks the activity rule on read, which also
	// covers the daemonless path where no pruner runs. The prune itself is
	// activity-based — a pin with a live tracked session survives, and an idle
	// one dies a TTL after its last select or session end (see PruneSticky).
	if _, err := s.m.Store.PruneSticky(time.Now().Add(-pool.StickyTTL)); err != nil {
		s.log.Printf("prune sticky: %v", err)
	}

	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		s.log.Printf("list accounts: %v", err)
		return
	}
	for i, a := range accts {
		// Claim the account for this iteration. An overlay conversion owns
		// the dir while it runs (Sync, the fuse self-heal, and a refresh
		// would all race its move/teardown/mount sequence), and the claim
		// makes that exclusion two-sided: beginConvert refuses while the
		// scheduler holds the dir, closing the check-then-act window a plain
		// isConverting test would leave open.
		if !s.beginPoll(a.ID) {
			continue
		}
		if s.pollAccount(ctx, sessions, i, a) {
			return
		}
	}

	// Mirror the freshly-sampled view for out-of-process readers (the widget).
	// Deliberately skipped by the early returns above: generated_at means "time
	// of the last completed poll" and must go stale when polling is broken.
	if err := s.writeStatusSnapshot(ctx); err != nil {
		s.log.Printf("status snapshot: %v", err)
	}
}

// pollAccount runs one account's per-poll body — overlay re-assert, idle
// adopt/refresh, usage sample — under the poll claim taken by pollOnce, which
// it releases. It reports whether polling should stop (ctx cancelled).
func (s *Server) pollAccount(ctx context.Context, sessions []procscan.Session, i int, a store.Account) (stop bool) {
	defer s.endPoll(a.ID)

	// Respect an exponential rate-limit backoff keyed on the consecutive-429
	// streak for this account.
	if last, ok, _ := s.m.Store.LatestUsageSample(a.ID); ok && last.RateLimited &&
		time.Since(last.TS) < rlBackoff(s.rlStreak[a.ID]) {
		return false
	}
	// A flagged (needs-login) account backs off to needsLoginPollInterval: a
	// revoked account stops 401-spamming every poll but still auto-recovers
	// within minutes of `ccp login`. A 401 inserts no usage sample, so this
	// can't ride the rate-limit backoff above — it needs its own clock.
	if health, _ := s.m.Store.GetAuthHealth(a.ID); health.NeedsLogin {
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
	// ~/.claude entries without an explicit sync (symlink relinks; fuse
	// health-checks).
	if err := s.m.SyncOverlay(a); err != nil {
		s.log.Printf("acct-%02d overlay sync: %v", a.ID, err)
		// An unhealthy fuse overlay here usually means the mount isn't up —
		// the holder died, or the account was added while no holder was
		// reachable — heal it now instead of leaving the dir dead until
		// restart. healFuse classifies: transient holder conditions and a
		// pending TCC grant retry next poll; only a genuine mount failure
		// falls back to symlink, and only when the account is idle.
		if a.OverlayKind == string(overlay.KindFuse) {
			s.healFuse(a)
		}
	}

	// A reserved account was just handed out by handleSelect but its claude
	// is not yet visible to procscan — treat it as busy so we don't refresh
	// the token out from under the launching session.
	idle := procscan.CountByConfigDir(sessions, a.ConfigDir) == 0 &&
		s.reservedCount(a.ID) == 0

	// A previously checked-out account that is now idle may carry a token
	// rotated by its live session — adopt it (re-asserting our ACL) before
	// sampling.
	if idle {
		if err := s.m.AdoptRotatedToken(ctx, a); err != nil {
			s.log.Printf("acct-%02d adopt rotated token: %v", a.ID, err)
		}
	}

	// Busy-account refresh is permitted only when a procscan-visible session
	// (never a bare reservation, whose claude is mid-launch) holds the account
	// AND we have already seen a consecutive 401 — giving a lazily-waking
	// session a full poll to refresh its own token before we touch the chain.
	busyBySession := procscan.CountByConfigDir(sessions, a.ConfigDir) > 0
	opts := pool.SampleOpts{
		AllowRefresh:     idle,
		AllowBusyRefresh: busyBySession && s.authStreak[a.ID] >= 1,
	}

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, rateLimited, err := s.m.SampleUsage(cctx, a, opts)
	cancel()
	// rlStreak is scheduler-local (this goroutine only) — no lock needed.
	if rateLimited {
		s.rlStreak[a.ID]++
	} else if err == nil {
		s.rlStreak[a.ID] = 0
	}
	s.handleAuthOutcome(a, err)
	return false
}

// handleAuthOutcome folds one sample's outcome into the account's auth health:
// a success clears any needs-login flag and the 401 streak (logged once on the
// transition); a definitive ErrNeedsLogin flags immediately; a plain 401
// increments the streak and flags at needsLoginAfter; any other error is logged
// as before. Scheduler-goroutine-local — no lock.
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
		if s.authStreak[a.ID] >= needsLoginAfter {
			s.flagNeedsLogin(a, err)
			return
		}
		s.log.Printf("acct-%02d sample: %v", a.ID, err)
		return
	}
	s.log.Printf("acct-%02d sample: %v", a.ID, err)
}

// flagNeedsLogin marks an account needs-login, stamping the attempt clock so the
// backoff engages, and logs the remediation command once (on the transition).
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

// jitter returns a deterministic-ish jitter in [0, max) derived from seed,
// avoiding Math.random (unavailable / non-reproducible).
func jitter(max time.Duration, seed int64) time.Duration {
	if max <= 0 {
		return 0
	}
	if seed < 0 {
		seed = -seed
	}
	return time.Duration(seed % int64(max))
}
