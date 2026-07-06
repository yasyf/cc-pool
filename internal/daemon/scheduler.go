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

	// While a network outage is being probed, the scheduler drops to a cheap
	// short cadence so the fleet heals within ~1 minute of connectivity
	// returning, instead of waiting out a full basePollInterval.
	outagePollInterval = 20 * time.Second
	outageJitter       = 5 * time.Second

	// Per-account spacing so N accounts don't hit the shared-IP bucket at once.
	perAccountSpacing = 2 * time.Second

	// While an outage persists, log only every netProbeLogEvery-th canary network
	// error (~one line per 5 min at the short cadence) so a multi-hour outage
	// proves liveness without spamming the log.
	netProbeLogEvery = 15

	// recoveryAbandonThreshold cuts a recovery sweep short once this many
	// consecutive network-class failures accumulate with no account reached yet:
	// connectivity dropped again mid-sweep, so the remaining accounts are left to
	// heal on the next canary probe instead of each burning a sample timeout.
	recoveryAbandonThreshold = 3

	rateLimitBackoffBase = 3 * time.Minute
	rateLimitBackoffCap  = 15 * time.Minute

	needsLoginAfter = 3
	// Balances per-poll 401 spam against auto-recovery latency after `ccp login`.
	needsLoginPollInterval = 15 * time.Minute
)

func rlBackoff(streak int) time.Duration {
	return proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap}.After(streak)
}

// sampleOutcome classifies one account's poll for network-outage detection.
type sampleOutcome int

const (
	// outcomeSuccess: the usage endpoint answered — connectivity is proven.
	outcomeSuccess sampleOutcome = iota
	// outcomeNonNetwork: an auth, rate-limit, decode, or cancelled error. The
	// API still answered (or the caller stopped), so connectivity is proven.
	outcomeNonNetwork
	// outcomeNetwork: a transport-layer failure (oauth.ErrNetwork) — the only
	// outage signal.
	outcomeNetwork
)

// classifyOutcome maps a SampleUsage error to an outage-accounting outcome. A
// nil error is a proven-live sample; an oauth.ErrNetwork transport failure is
// the only outage signal; every other error still proves the API responded.
func classifyOutcome(err error) sampleOutcome {
	switch {
	case err == nil:
		return outcomeSuccess
	case errors.Is(err, oauth.ErrNetwork):
		return outcomeNetwork
	default:
		return outcomeNonNetwork
	}
}

// nextPollDelay returns the delay before the next poll: the short outage cadence
// (~20s ±5s) while a network outage is being probed, else the steady interval
// (basePollInterval + [0, pollJitter)). seed derives the deterministic jitter.
func nextPollDelay(outage bool, seed int64) time.Duration {
	if outage {
		return outagePollInterval - outageJitter + jitter(2*outageJitter, seed)
	}
	return basePollInterval + jitter(pollJitter, seed)
}

func (s *Server) scheduler(ctx context.Context) {
	s.pollOnce(ctx)
	for {
		// netOutage is scheduler-goroutine-local: pollOnce is the only writer and
		// runs in this goroutine, so reading it here needs no lock.
		d := nextPollDelay(s.netOutage, time.Now().UnixNano())
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
	sessions, err := s.scan(ctx)
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

	// While a network outage is in effect, poll only a single canary — the first
	// account not gated by backoff. A network-failing canary keeps the outage;
	// any non-network answer proves connectivity, flips recovery, and runs a full
	// recovery sweep of the remaining accounts in this same invocation. Recovery-
	// sweep samples are accounted too: if connectivity drops again mid-sweep the
	// sweep abandons early and re-enters outage below.
	spacing := s.pollSpacing
	if spacing == 0 {
		spacing = perAccountSpacing
	}
	recovery := false
	var attempts, netFails, sampled int
	for _, a := range accts {
		if ctx.Err() != nil {
			return
		}
		if !s.beginPoll(a.ID) {
			continue
		}
		if s.pollGated(a) {
			s.endPoll(a.ID)
			continue
		}
		// Space consecutive samples so N accounts don't burst the shared-IP
		// bucket; the sweep's first sample goes immediately.
		if sampled > 0 {
			select {
			case <-ctx.Done():
				s.endPoll(a.ID)
				return
			case <-time.After(spacing):
			}
		}
		canary := s.netOutage && !recovery
		outcome := s.pollAccount(ctx, sessions, a, recovery || canary)
		s.endPoll(a.ID)
		sampled++

		if canary {
			if outcome == outcomeNetwork {
				// Still down: one cheap failing request this short tick. Leave the
				// snapshot stale (polling is broken) and wait for the next canary.
				return
			}
			// Connectivity returned: leave outage mode and heal the whole fleet in
			// this same sweep. The canary is already sampled; the loop continues
			// through the remaining accounts under recovery semantics.
			s.netOutage = false
			s.log.Printf("network recovered; running a full recovery sweep of %d account(s)", len(accts))
			recovery = true
			continue
		}

		attempts++
		if outcome == outcomeNetwork {
			netFails++
		}
		// A recovery sweep failing network-class end to end means connectivity
		// dropped again after the canary briefly reached the API: stop burning a
		// sample timeout per remaining account and re-enter outage below.
		if recovery && netFails == attempts && netFails >= recoveryAbandonThreshold {
			break
		}
	}

	// Enter (or re-enter) outage when every attempted account this sweep failed
	// network-class: a normal sweep proves the outage, a recovery sweep proves
	// connectivity dropped again after the canary reached the API. Either way the
	// next tick is a cheap short-cadence canary probe.
	if attempts > 0 && netFails == attempts {
		s.netOutage = true
		s.netProbeLogSkip = 0
		if recovery {
			s.log.Printf("network dropped again mid-recovery: all %d re-polled account(s) failed to reach the API; resuming short-tick canary polling", attempts)
		} else {
			s.log.Printf("network outage: all %d polled account(s) failed to reach the API; short-tick canary polling until it returns", attempts)
		}
	}

	// Deliberately skipped while polling is broken (an outage or an early return
	// above): generated_at means "time of the last completed poll" and must go
	// stale when the fleet cannot be reached.
	if s.netOutage {
		return
	}
	if err := s.writeStatusSnapshot(ctx); err != nil {
		s.log.Printf("status snapshot: %v", err)
	}
}

// pollGated reports whether an account is currently backed off — a recent
// rate-limit sample still inside its exponential backoff, or a needs-login /
// exhausted-auth-streak account inside the needs-login interval — so pollOnce
// skips it with no network attempt (and it never serves as the outage canary).
// The needs-login backoff needs its own clock: a 401 inserts no usage sample,
// so it can't ride the rate-limit backoff. Scheduler-goroutine-local — no lock.
func (s *Server) pollGated(a store.Account) bool {
	if last, ok, _ := s.m.Store.LatestUsageSample(a.ID); ok && last.RateLimited &&
		time.Since(last.TS) < rlBackoff(s.rlStreak[a.ID]) {
		return true
	}
	if health, _ := s.m.Store.GetAuthHealth(a.ID); health.NeedsLogin || s.authStreak[a.ID] >= needsLoginAfter {
		if last, ok := s.lastAuthAttempt[a.ID]; ok && time.Since(last) < needsLoginPollInterval {
			return true
		}
	}
	return false
}

// pollAccount samples one account and reports the outcome for outage detection.
// The caller holds the poll claim, has cleared the backoff gates (pollGated),
// and owns inter-account spacing. recovery forces AllowBusyRefresh for a busy
// account regardless of the auth streak — the post-outage heal — which stays
// safe because fetchUsage's deep guard (post-401 + provably-expired +
// credential-unchanged re-read) still prevents a refresh-token double-spend; the
// streak gate is only a heuristic layer.
func (s *Server) pollAccount(ctx context.Context, sessions []procscan.Session, a store.Account, recovery bool) sampleOutcome {
	// Re-assert the overlay so long-lived setups pick up new top-level
	// ~/.claude entries without an explicit sync.
	if err := s.m.SyncOverlay(a); err != nil {
		s.log.Printf("acct-%02d overlay sync: %v", a.ID, err)
		// A fuse sync failure usually means the mount is down — heal now instead of
		// leaving the dir dead until restart. A File Provider sync failure is NOT
		// reconciled inline: the backoff-gated heal ticker (probe + recovery ladder)
		// owns FP recovery, so a Health+Setup on every failed poll would be the
		// reconcile storm (defect 3) this drop removes.
		if fuseBackedRow(a.OverlayKind) {
			s.healFuse(ctx, a)
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
	// refreshing sooner could double-spend a refresh token it still needs. A
	// recovery sweep bypasses the streak gate (fetchUsage's deep guard still
	// holds) so a busy account whose token expired during the outage heals now.
	busyBySession := procscan.CountByConfigDir(sessions, a.ConfigDir) > 0
	opts := pool.SampleOpts{
		AllowRefresh:     idle,
		AllowBusyRefresh: busyBySession && (recovery || s.authStreak[a.ID] >= 1),
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
	return classifyOutcome(err)
}

// handleAuthOutcome reacts to a sample's error. Only a definitive ErrNeedsLogin
// flags needs-login: a plain 401 also surfaces transient (5xx) refresh failures,
// so the streak only throttles polling. A network-class failure is an outage
// signal, not an auth event — pollOnce's outage detector owns the reaction here,
// so it touches no auth state. Scheduler-goroutine-local — no lock.
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
	if errors.Is(err, oauth.ErrNetwork) {
		// An outage keeps today's non-behavior everywhere else: no authStreak
		// bump, no needs-login flag, rlStreak untouched. Only the outage detector
		// (via classifyOutcome) reacts.
		s.logNetUnreachable(a, err)
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

// logNetUnreachable logs a probe's network-unreachable error. While an outage is
// established (netOutage set) it throttles to one line per netProbeLogEvery
// probes so a multi-hour canary loop doesn't spam ~3 lines/min; the surviving
// lines still prove the canary is alive. Scheduler-goroutine-local — no lock.
func (s *Server) logNetUnreachable(a store.Account, err error) {
	if s.netOutage {
		defer func() { s.netProbeLogSkip++ }()
		if s.netProbeLogSkip%netProbeLogEvery != 0 {
			return
		}
	}
	s.log.Printf("acct-%02d sample: network unreachable: %v", a.ID, err)
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
