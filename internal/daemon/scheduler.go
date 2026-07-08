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
)

// rateLimitBackoffBase/Cap and needsLoginAfter/needsLoginPollInterval (the 429
// streak backoff and the needs-login streak/throttle) live in policies.go — the
// self-heal policy substrate.

// authStreakPolicy, acctRateLimitPolicy, and poolRateLimitPolicy are the streak
// self-heal rows: the debounced needs-login gate (auth.streak) and the
// per-account / pool-wide 429 backoff gates. Each account folds onto one row
// keyed by its ConfigDir; the single pool-wide streak uses poolResource.
var (
	authStreakPolicy    = policies["auth.streak"]
	acctRateLimitPolicy = policies["ratelimit.acct"]
	poolRateLimitPolicy = policies["ratelimit.pool"]
)

// poolResource is the sentinel resource for the one pool-wide 429 streak row.
const poolResource = "pool"

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
	scanOK := err == nil
	if err != nil {
		// Fail closed: a failed scan can't prove any account idle, so this tick
		// treats every account as busy — no idle refresh, no adopt.
		s.log.Printf("procscan failed; treating all accounts as busy this tick: %v", err)
	} else {
		switch n, err := s.m.Store.CloseDeadSessions(procscan.AlivePIDs(sessions), time.Now()); {
		case err != nil:
			s.log.Printf("close dead sessions: %v", err)
		case n > 0:
			s.log.Printf("reconciled %d ended session(s)", n)
		}
	}

	// A cask upgrade replaces the widget bundle but never recycles a live appex,
	// leaving a frozen render. Reaped every poll, not just at startup: a formula
	// upgrade can restart the daemon before the cask swap lands.
	s.reconcileStaleWidget(ctx)

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
		// A 429 anywhere gates the rest of the sweep (shared-IP /usage bucket); the
		// outage canary is exempt — a 429 still proves reachability.
		canary := s.netOutage && !recovery
		if !canary && s.poolRateLimited() {
			break
		}
		if !s.cl.hold(a.ID) {
			continue
		}
		if s.pollGated(a) {
			s.cl.disownHold(a.ID)
			continue
		}
		// Space consecutive samples so N accounts don't burst the shared-IP
		// bucket; the sweep's first sample goes immediately.
		if sampled > 0 {
			select {
			case <-ctx.Done():
				s.cl.disownHold(a.ID)
				return
			case <-time.After(spacing):
			}
		}
		outcome := s.pollAccount(ctx, sessions, a, scanOK, recovery || canary)
		s.cl.disownHold(a.ID)
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

// poolRateLimited reports whether the pool-wide 429 gate is still inside its
// window: the ratelimit.pool row's backoff clock, armed by a 429 from the
// Retry-After hint if present, else the exponential backoff (see recordRateLimit).
func (s *Server) poolRateLimited() bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return !s.led.backoffElapsed(poolRateLimitPolicy, poolResource, time.Now())
}

// pollGated reports whether an account is currently backed off — a recent
// rate-limit sample still inside its exponential backoff, or a needs-login /
// exhausted-auth-streak account inside the needs-login interval — so pollOnce
// skips it with no network attempt (and it never serves as the outage canary).
// The needs-login backoff needs its own clock: a 401 inserts no usage sample,
// so it can't ride the rate-limit backoff. Store I/O runs outside ledMu; the
// streak reads take it around the bookkeeping only.
func (s *Server) pollGated(a store.Account) bool {
	dir := a.ConfigDir
	if last, ok, _ := s.m.Store.LatestUsageSample(a.ID); ok && last.RateLimited &&
		time.Since(last.TS) < rlBackoff(s.acctRateLimitStreak(dir)) {
		return true
	}
	health, _ := s.m.Store.GetAuthHealth(a.ID)
	return s.authThrottled(dir, health.NeedsLogin)
}

// acctRateLimitStreak reports dir's consecutive-429 count — the exponent for the
// per-account backoff window; 0 when the account is not rate-limited.
func (s *Server) acctRateLimitStreak(dir string) int {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	if l := s.led.peek(acctRateLimitPolicy, dir); l != nil {
		return l.attempts
	}
	return 0
}

// authThrottled reports whether dir's poll is inside the needs-login throttle:
// the persisted store verdict (needsLogin) or the transient-401 streak's fault
// has tripped AND the last auth attempt is within needsLoginPollInterval. The
// 15-minute cadence is the consumer-applied throttle the auth.streak gate trips.
func (s *Server) authThrottled(dir string, needsLogin bool) bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	l := s.led.peek(authStreakPolicy, dir)
	if !needsLogin && !(l != nil && l.faulted) {
		return false
	}
	return l != nil && !l.lastAt.IsZero() && time.Since(l.lastAt) < needsLoginPollInterval
}

// pollAccount samples one account and reports the outcome for outage detection.
// The caller holds the poll claim, has cleared the backoff gates (pollGated),
// and owns inter-account spacing. recovery forces AllowBusyRefresh for a busy
// account regardless of the auth streak — the post-outage heal — which stays
// safe because fetchUsage's deep guard (post-401 + provably-expired +
// credential-unchanged re-read) still prevents a refresh-token double-spend; the
// streak gate is only a heuristic layer.
func (s *Server) pollAccount(ctx context.Context, sessions []procscan.Session, a store.Account, scanOK, recovery bool) sampleOutcome {
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

	// A reserved account's claude may not be procscan-visible yet — treat it as
	// busy so we don't refresh under the launch. A failed scan also reads busy.
	idle := scanOK && procscan.CountByConfigDir(sessions, a.ConfigDir) == 0 &&
		s.cl.reservedCount(a.ID) == 0

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
	if busyBySession {
		// A live session keeps this host's lease alive so peers keep penalizing.
		s.sync.renewWhileBusy(ctx, a)
	}

	// Both refresh opts AND with the holder gate — a non-holder refresh is the
	// double-spend that forks a chain; evaluated only when a refresh is on the table.
	wantIdle := idle
	wantBusy := busyBySession && (recovery || s.authStreakActive(a.ConfigDir))
	allowRefresh := false
	if wantIdle || wantBusy {
		allowRefresh = s.sync.mayRefresh(ctx, a)
	}
	opts := pool.SampleOpts{
		AllowRefresh:     wantIdle && allowRefresh,
		AllowBusyRefresh: wantBusy && allowRefresh,
	}

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_, rateLimited, retryAfter, err := s.m.SampleUsage(cctx, a, opts)
	cancel()
	// A 429 books a backoff step on the per-account and pool-wide streaks (the
	// pool step prefers Retry-After over the computed backoff); a clean sample
	// clears both. A non-429 error leaves them untouched. Bookkeeping only under
	// ledMu — the SampleUsage I/O above already returned.
	if rateLimited {
		s.recordRateLimit(a.ConfigDir, retryAfter, time.Now())
	} else if err == nil {
		s.clearRateLimit(a.ConfigDir)
	}
	s.handleAuthOutcome(ctx, a, err)
	return classifyOutcome(err)
}

// recordRateLimit books one 429 for dir: it advances the per-account and
// pool-wide 429 backoff streaks, arming each gate's window. The pool window
// prefers the server's Retry-After hint (clamped to rateLimitBackoffCap) over the
// computed exponential backoff. Bookkeeping only — no I/O under ledMu.
func (s *Server) recordRateLimit(dir string, retryAfter time.Duration, now time.Time) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.attempt(acctRateLimitPolicy, dir, attemptPrimary, now)
	s.led.attempt(poolRateLimitPolicy, poolResource, attemptPrimary, now)
	if retryAfter > 0 {
		s.led.setNextDue(poolRateLimitPolicy, poolResource, now.Add(min(retryAfter, rateLimitBackoffCap)))
	}
}

// clearRateLimit drops dir's per-account 429 streak and the pool-wide streak — a
// clean sample proves the shared-IP bucket recovered.
func (s *Server) clearRateLimit(dir string) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.clear(acctRateLimitPolicy, dir)
	s.led.clear(poolRateLimitPolicy, poolResource)
}

// handleAuthOutcome reacts to a sample's error. Only a definitive ErrNeedsLogin
// flags needs-login: a plain 401 also surfaces transient (5xx) refresh failures,
// so the streak only throttles polling. A network-class failure is an outage
// signal, not an auth event — pollOnce's outage detector owns the reaction here,
// so it touches no auth state.
func (s *Server) handleAuthOutcome(ctx context.Context, a store.Account, err error) {
	if err == nil {
		s.authClearStreak(a.ConfigDir)
		if changed, cerr := s.m.Store.ClearNeedsLogin(a.ID); cerr != nil {
			s.log.Printf("acct-%02d clear needs-login: %v", a.ID, cerr)
		} else if changed {
			s.log.Printf("acct-%02d auth recovered; needs-login cleared", a.ID)
		}
		return
	}
	if errors.Is(err, pool.ErrNeedsLogin) {
		s.flagNeedsLogin(ctx, a, err)
		return
	}
	if errors.Is(err, oauth.ErrNetwork) {
		// An outage keeps today's non-behavior everywhere else: no auth-streak
		// strike, no needs-login flag, the 429 streaks untouched. Only the outage
		// detector (via classifyOutcome) reacts.
		s.logNetUnreachable(a, err)
		return
	}
	var ue *oauth.UsageError
	if errors.As(err, &ue) && ue.Unauthorized() {
		s.authStrike(a.ConfigDir, err)
		s.log.Printf("acct-%02d sample: %v", a.ID, err)
		return
	}
	s.log.Printf("acct-%02d sample: %v", a.ID, err)
}

// authClearStreak drops dir's transient-401 streak and its throttle clock on a
// clean sample — the account's auth recovered.
func (s *Server) authClearStreak(dir string) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.clear(authStreakPolicy, dir)
}

// authStrike advances dir's transient-401 streak toward the needs-login gate,
// latching faulted at needsLoginAfter, and stamps the throttle clock.
func (s *Server) authStrike(dir string, err error) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.strike(authStreakPolicy, dir, time.Now(), err)
}

// authStamp stamps dir's needs-login throttle clock without advancing the
// transient-401 streak — a definitive ErrNeedsLogin flags the persisted store
// verdict, not the streak, yet the 15-minute poll cadence still engages.
func (s *Server) authStamp(dir string, err error) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.stamp(authStreakPolicy, dir, time.Now(), err)
}

// authStreakActive reports whether dir carries at least one unrecovered
// transient 401 (the busy-refresh heuristic: a live session's expired token is
// worth a refresh attempt) — true from the first strike through the latched fault.
func (s *Server) authStreakActive(dir string) bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	l := s.led.peek(authStreakPolicy, dir)
	return l != nil && (l.strikes >= 1 || l.faulted)
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

// flagNeedsLogin stamps the attempt clock, then flags needs-login — unless the
// sync self-heal pulled a fresher chain, which skips the set (never clears). The
// clock stamp precedes syncHeal so ledMu is never held across the pull I/O.
func (s *Server) flagNeedsLogin(ctx context.Context, a store.Account, err error) {
	s.authStamp(a.ConfigDir, err)
	if s.syncHeal(ctx, a) {
		s.log.Printf("acct-%02d auth failed but sync pulled a fresher chain; retrying before flagging needs-login", a.ID)
		return
	}
	changed, serr := s.m.Store.SetNeedsLogin(a.ID, time.Now(), err.Error())
	if serr != nil {
		s.log.Printf("acct-%02d set needs-login: %v", a.ID, serr)
		return
	}
	if changed {
		s.log.Printf("acct-%02d needs re-login — run `ccp login %d`: %v", a.ID, a.ID, err)
	}
}

// syncHealTimeout bounds the self-heal's converge pull; a var so tests shrink it.
var syncHealTimeout = 15 * time.Second

// syncHeal runs one converge pull and reports whether a strictly fresher chain
// arrived from a peer; nil syncPull (sync disabled) reports false.
func (s *Server) syncHeal(ctx context.Context, a store.Account) bool {
	if s.syncPull == nil {
		return false
	}
	// A missing credential baselines at zero, so any pulled chain counts as fresher.
	var before int64
	if cred, _, err := s.m.ReadCredential(a); err == nil {
		before = cred.ClaudeAiOauth.ExpiresAt
	}
	hctx, cancel := context.WithTimeout(ctx, syncHealTimeout)
	defer cancel()
	if err := s.syncPull(hctx); err != nil {
		s.log.Printf("acct-%02d sync heal pull: %v", a.ID, err)
		return false
	}
	cred, _, err := s.m.ReadCredential(a)
	if err != nil {
		return false
	}
	return cred.ClaudeAiOauth.ExpiresAt > before
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
