package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
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
	// outcomeNoProbe: retained durable evidence returned without network I/O.
	// It proves neither an outage nor recovery.
	outcomeNoProbe
	// outcomeNetwork: a transport-layer failure (oauth.ErrNetwork) — the only
	// outage signal.
	outcomeNetwork
)

// classifyOutcome maps a SampleUsage error to an outage-accounting outcome. A
// nil error is a proven-live sample; retained durable evidence alone proves
// nothing; retained evidence carrying LiveProbe is classified from that probe.
// An oauth.ErrNetwork transport failure is the only outage signal; every other
// live error proves the API responded.
func classifyOutcome(err error) sampleOutcome {
	switch {
	case err == nil:
		return outcomeSuccess
	case errors.Is(err, pool.ErrCredentialOperationLiveProbe) &&
		errors.Is(err, oauth.ErrNetwork):
		return outcomeNetwork
	case errors.Is(err, pool.ErrCredentialOperationLiveProbe):
		return outcomeNonNetwork
	case errors.Is(err, pool.ErrCredentialOperationReplayed):
		return outcomeNoProbe
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
	s.runTable(ctx, s.newTick(ctx), pollTable)
}

// pruneStickyRows drops expired sticky-pin rows (hygiene only: StickyPick
// re-checks the activity rule on reads between scheduler passes).
func (s *Server) pruneStickyRows() {
	if _, err := s.m.Store.PruneSticky(time.Now().Add(-pool.StickyTTL)); err != nil {
		s.log.Printf("prune sticky: %v", err)
	}
}

// pollAccounts is the per-account sweep (the account.poll row). It reports whether
// the poll completed cleanly so a status snapshot should be written — false on any
// skip condition (ctx cancel, list-accounts failure, a still-down outage canary,
// or entering/re-entering outage). See ccn doc 36b05ef.
func (s *Server) pollAccounts(ctx context.Context, t *tick) bool {
	accts, err := s.m.Store.ListActiveAccounts()
	if err != nil {
		s.log.Printf("list accounts: %v", err)
		return false
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
	var noProbeAccount store.Account
	hasNoProbeAccount := false
	for _, a := range accts {
		if ctx.Err() != nil {
			return false
		}
		// A 429 anywhere gates the rest of the sweep (shared-IP /usage bucket); the
		// outage canary is exempt — a 429 still proves reachability.
		canary := s.netOutage && !recovery
		if !canary && s.poolRateLimited() {
			break
		}
		if s.pollGated(a) {
			continue
		}
		// Space consecutive samples so N accounts don't burst the shared-IP
		// bucket; the sweep's first sample goes immediately.
		if sampled > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(spacing):
			}
		}
		outcome := s.pollAccount(ctx, t, a, recovery || canary)
		sampled++

		if canary {
			if outcome == outcomeNoProbe {
				outcome = s.probeAccountReachability(ctx, a)
				if outcome == outcomeNoProbe {
					continue
				}
			}
			if outcome == outcomeNetwork {
				// Still down: one cheap failing request this short tick. Leave the
				// snapshot stale (polling is broken) and wait for the next canary.
				return false
			}
			// Connectivity returned: leave outage mode and heal the whole fleet in
			// this same sweep. The canary is already sampled; the loop continues
			// through the remaining accounts under recovery semantics.
			s.netOutage = false
			s.log.Printf("network recovered; running a full recovery sweep of %d account(s)", len(accts))
			recovery = true
			continue
		}

		if outcome == outcomeNoProbe {
			if !hasNoProbeAccount {
				noProbeAccount = a
				hasNoProbeAccount = true
			}
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

	// netOutage is deliberately process-local. After a restart, retained
	// credential evidence can make every account a no-probe result even though
	// no request in this process has established reachability. Before treating
	// that sweep as complete, issue one read-only probe against the first such
	// account. A probe that cannot run leaves the snapshot stale.
	if !recovery && !s.netOutage && attempts == 0 && hasNoProbeAccount {
		outcome := s.probeAccountReachability(ctx, noProbeAccount)
		if outcome == outcomeNoProbe {
			return false
		}
		attempts = 1
		if outcome == outcomeNetwork {
			netFails = 1
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
	return !s.netOutage
}

// poolRateLimited reports whether the pool-wide 429 gate is still inside its
// window: the ratelimit.pool row's backoff clock, armed by a 429 from the
// Retry-After hint if present, else the exponential backoff (see recordRateLimit).
func (s *Server) poolRateLimited() bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return !s.led.backoffElapsed(poolRateLimitPolicy, poolResource, time.Now())
}

// pollGated reports whether an account is currently backed off — inside its
// rate-limit exponential backoff, or a needs-login / exhausted-auth-streak account
// inside the needs-login interval — so pollOnce skips it with no network attempt
// (and it never serves as the outage canary). Store I/O runs outside ledMu; the
// streak reads take it only around the bookkeeping. See ccn doc 36b05ef.
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
	if !needsLogin && (l == nil || !l.faulted) {
		return false
	}
	return l != nil && !l.lastAt.IsZero() && time.Since(l.lastAt) < needsLoginPollInterval
}

// pollAccount samples one account and reports the outcome for outage detection.
// The caller has cleared the backoff gates (pollGated) and owns inter-account
// spacing. recovery forces AllowBusyRefresh for a busy account
// regardless of the auth streak (the post-outage heal); fetchUsage's deep guard
// still prevents a refresh-token double-spend. See ccn doc 36b05ef.
func (s *Server) pollAccount(ctx context.Context, t *tick, a store.Account, recovery bool) sampleOutcome {
	// A reserved account's claude may not be heartbeat-visible yet — treat it as
	// busy so we don't refresh under the launch. A failed heartbeat also makes
	// every dir non-idle while retaining last-known counts for diagnostics.
	presentationDir := pool.AccountPresentationDir(a.ID)
	idle := t.idle(presentationDir) && s.cl.reservedCount(a.ID) == 0

	// The prior-401 gate gives a lazily-waking session one full poll to
	// self-refresh first; a recovery sweep bypasses the streak gate (fetchUsage's
	// deep guard still holds). See ccn doc 36b05ef for the accepted gap.
	busyBySession := t.sessionCount(presentationDir) > 0

	// The refresh gate is structural, not a cross-host lease check: a synced
	// (refresh-token-free) credential cannot refresh (ensureFreshToken returns
	// ErrUnrefreshable), so only the origin — the one host holding the refresh
	// token — ever spends it.
	opts := pool.SampleOpts{
		AllowRefresh:     idle,
		AllowBusyRefresh: busyBySession && (recovery || s.authStreakActive(a.ConfigDir)),
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

func (s *Server) probeAccountReachability(ctx context.Context, a store.Account) sampleOutcome {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	probed, rateLimited, retryAfter, err := s.m.ProbeUsageReachability(probeCtx, a)
	cancel()
	if !probed {
		return outcomeNoProbe
	}
	if rateLimited {
		s.recordRateLimit(a.ConfigDir, retryAfter, time.Now())
		return outcomeNonNetwork
	}
	if err == nil {
		s.clearRateLimit(a.ConfigDir)
		return outcomeSuccess
	}
	if errors.Is(err, oauth.ErrNetwork) {
		s.logNetUnreachable(a, err)
	}
	return classifyOutcome(err)
}

// recordRateLimit books one 429 for dir: it advances the per-account and
// pool-wide 429 backoff streaks, arming each gate's window. The pool window
// prefers the server's Retry-After hint (clamped to rateLimitBackoffCap) over the
// computed exponential backoff. Bookkeeping only — no I/O under ledMu.
func (s *Server) recordRateLimit(dir string, retryAfter time.Duration, now time.Time) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.attempt(acctRateLimitPolicy, dir, now)
	s.led.attempt(poolRateLimitPolicy, poolResource, now)
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
	// An owned dead chain (ErrNeedsLogin) and an expired synced copy
	// (ErrUnrefreshable) both route into flagNeedsLogin, which classifies the kind
	// at persist time. ErrUnrefreshable may arrive wrapping a usage 401, so this
	// must precede the UsageError.Unauthorized() strike below.
	if errors.Is(err, pool.ErrNeedsLogin) || errors.Is(err, pool.ErrUnrefreshable) {
		s.flagNeedsLogin(ctx, a, err)
		return
	}
	if errors.Is(err, pool.ErrCredentialOperationReplayed) &&
		!errors.Is(err, pool.ErrCredentialOperationLiveProbe) {
		return
	}
	if errors.Is(err, oauth.ErrNetwork) {
		// An outage keeps today's non-behavior everywhere else: no auth-streak
		// strike, no needs-login flag, the 429 streaks untouched. Only the outage
		// detector (via classifyOutcome) reacts.
		s.logNetUnreachable(a, err)
		return
	}
	if errors.Is(err, pool.ErrCredentialOperationLiveProbe) {
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

// flagNeedsLogin stamps the attempt clock, pulls once from a peer, and ALWAYS
// decides this tick. A heal that improved the credential earns ONE inline
// resample (same tick, no second heal): a clean resample clears the flag,
// anything else persists it. The clock stamp precedes syncHeal so ledMu is
// never held across the pull I/O.
func (s *Server) flagNeedsLogin(ctx context.Context, a store.Account, err error) {
	s.authStamp(a.ConfigDir, err)
	if s.syncHeal(ctx, a) && s.resampleAfterHeal(ctx, a) {
		s.authClearStreak(a.ConfigDir)
		if _, cerr := s.m.Store.ClearNeedsLogin(a.ID); cerr != nil {
			s.log.Printf("acct-%02d clear needs-login after heal: %v", a.ID, cerr)
		} else {
			s.log.Printf("acct-%02d sync pulled a fresher chain and it sampled clean; auth recovered", a.ID)
		}
		return
	}
	kind, kindErr := s.authKind(ctx, a)
	reason := store.AuthReasonRequired
	digest := store.DigestReason(err.Error())
	if kindErr != nil {
		s.log.Printf("acct-%02d classify auth ownership: %v", a.ID, kindErr)
		kind = store.AuthKindUnverified
		reason = store.AuthReasonInternal
		digest = store.DigestReason(errors.Join(err, kindErr).Error())
	} else if kind == store.AuthKindAwaitingOrigin {
		reason = store.AuthReasonAwaitingOrigin
	}
	changed, serr := s.m.Store.SetNeedsLogin(
		a.ID,
		time.Now(),
		reason,
		digest,
		kind,
	)
	if serr != nil {
		s.log.Printf("acct-%02d set needs-login: %v", a.ID, serr)
		return
	}
	if changed {
		s.log.Printf("acct-%02d needs re-login — run `ccp login %d`: %v", a.ID, a.ID, err)
	}
}

// resampleAfterHeal re-samples once inline after a heal pulled a fresher chain:
// no refresh and no second heal. An expired synced pull is never healed —
// /usage may grace-serve it with a 200, so ErrUnrefreshable is re-checked here
// (SampleOpts{} skips the refresh classification that surfaces it).
func (s *Server) resampleAfterHeal(ctx context.Context, a store.Account) bool {
	cred, _, err := s.m.ReadCredential(ctx, a)
	if err != nil || (cred.Synced() && cred.Expired()) {
		return false
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, rateLimited, retryAfter, err := s.m.SampleUsage(cctx, a, pool.SampleOpts{})
	// The outer poll booked only the pre-heal auth error, so the resample must
	// arm/clear the 429 gates itself — same bookkeeping as pollAccount.
	if rateLimited {
		s.recordRateLimit(a.ConfigDir, retryAfter, time.Now())
	} else if err == nil {
		s.clearRateLimit(a.ConfigDir)
	}
	return err == nil
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
	if cred, _, err := s.m.ReadCredential(ctx, a); err == nil {
		before = cred.ClaudeAiOauth.ExpiresAt
	}
	hctx, cancel := context.WithTimeout(ctx, syncHealTimeout)
	defer cancel()
	if err := s.syncPull(hctx); err != nil {
		s.log.Printf("acct-%02d sync heal pull: %v", a.ID, err)
		return false
	}
	cred, _, err := s.m.ReadCredential(ctx, a)
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
