package daemon

import (
	"time"

	"github.com/yasyf/fusekit/proc"
)

// This file owns the daemon self-heal policy substrate: every debounce, backoff,
// breaker, and trip-action tuning the self-heal families apply, plus THE table
// that names them. The literals live here once (pinned by policies_test.go);
// every row references a constant, never a duplicated literal. Later phases port
// each self-heal family (fuse rows, FP domains, auth/rate-limit streaks) onto a
// ledger row keyed by its policy — this phase lands the substrate only.
//
// Cadence and sync-engine constants (tickers, poll intervals, lease/takeover
// windows) are NOT self-heal policy and stay in their own files.

// remountBackoffBase/remountBackoffCap bound the per-row fuse remount backoff;
// the cap stays under the 180s scheduler period so the heal is never the slower
// recovery path.
const (
	remountBackoffBase = 10 * time.Second
	remountBackoffCap  = 2 * time.Minute
)

// remountBreakerThreshold is the consecutive wedged/never-recovering heal
// failures before a fuse row stops retrying and retreats to symlink — endless
// remount churn can re-wedge the kernel (the kill-9 holder incident).
const remountBreakerThreshold = 5

// tccBreakerThreshold is the consecutive TCC-blocked heals (waiting on the macOS
// "Network Volumes" grant) before the row retreats to symlink; above
// remountBreakerThreshold since a TCC block is a benign wait, not a kernel hazard.
const tccBreakerThreshold = 6

// deepWedgeStrikes debounces the fuse deep-probe wedged verdict: one transient
// slow read under load must not un-vouch a mirror serving live sessions.
const deepWedgeStrikes = 2

// shallowDeadStrikes debounces fuse remount on the holder's List liveness — a
// bounded 2s stat that false-negatives under load.
const shallowDeadStrikes = 2

// fpWedgeStrikes debounces the File Provider wedged verdict: one transient slow
// read under materialization load must not un-vouch a domain serving live
// sessions.
const fpWedgeStrikes = 2

// fpRecoveryBreaker caps recovery attempts on one wedged domain; past it the
// breaker trips and heal parks the domain until reset.
const fpRecoveryBreaker = 5

// fpRecoveryBackoff spaces a wedged domain's recovery attempts: 30s after the
// first attempt, doubling to a 10m cap.
var fpRecoveryBackoff = proc.Backoff{Base: 30 * time.Second, Cap: 10 * time.Minute}

// needsLoginAfter is the consecutive definitive needs-login verdicts before the
// poll throttles; needsLoginPollInterval is the throttled cadence the gate
// consumer applies once it trips.
const (
	needsLoginAfter        = 3
	needsLoginPollInterval = 15 * time.Minute
)

// rateLimitBackoffBase/rateLimitBackoffCap bound the per-account and pool-wide
// 429 streak backoff; a 429's Retry-After overrides the computed window.
const (
	rateLimitBackoffBase = 3 * time.Minute
	rateLimitBackoffCap  = 30 * time.Minute
)

// fpAppEnsureBackoff is the fixed window between companion-app launch attempts:
// a crash-looping CCPoolStatus.app costs at most one loud `open -g` per window
// (never a spawn storm), and the booked next-due also fences a second launch out
// while a ~30s spawn is still in flight.
const fpAppEnsureBackoff = time.Minute

// fpOrphanReapStrikes is the consecutive confirmed sweeps a rowless registered
// File Provider domain must survive before it is deregistered (~30s at the heal
// cadence) — a debounce so a mid-add domain glimpsed between its reservation and
// its promoted row is never reaped.
const fpOrphanReapStrikes = 3

// fpOrphanReapBackoff is the fixed window spacing RemoveDomain retries after a
// failed reap; the strike verdict is kept across the wait.
var fpOrphanReapBackoff = proc.Backoff{Base: 5 * time.Minute, Cap: 5 * time.Minute}

// tripAction names what a consumer does when a ledger's breaker trips.
type tripAction int

const (
	// tripGate stops issuing the gated operation while the breaker holds (the
	// consumer applies its own re-poll cadence): auth and rate-limit streaks.
	tripGate tripAction = iota
	// tripPark leaves the faulted resource marked and stops recovering it until
	// reset: a wedged File Provider domain.
	tripPark
	// tripRetreat abandons the current backend for a safe fallback: a fuse row
	// falling back to symlink.
	tripRetreat
)

// policy is one self-heal family's tuning: a debounced fault verdict (via
// strikes reaching debounce, or a forced fault) followed by a backoff-spaced,
// breaker-capped recovery ladder (via attempts). With alt set, breaker caps the
// primary lane and alt caps a second, mutually-resetting lane (e.g. TCC);
// without alt, breaker caps the attempts clock itself, so strikes stays a pure
// debounce counter pre-fault attempts can never erode. A zero field means the
// family does not use that phase: debounce 0 ⇒ no debounce, breaker 0 ⇒ no
// breaker, alt 0 ⇒ no alt lane, zero backoff ⇒ no spacing.
type policy struct {
	name     string
	debounce int
	backoff  proc.Backoff
	breaker  int
	onTrip   tripAction
	alt      int
}

// policies is THE self-heal policy table — one row per family, each referencing
// the pinned constants above. A ledger row is keyed by its policy's name plus a
// resource (an account dir, id, or "pool").
var policies = map[string]policy{
	// fuse.remount: per-row remount backoff with two mutually-resetting breaker
	// lanes sharing one attempts clock — hazard (retreat at remountBreakerThreshold)
	// and TCC (retreat at tccBreakerThreshold). No debounce: an unvouched row is
	// already a fault, driven by the holder cache.
	"fuse.remount": {
		name:    "fuse.remount",
		backoff: proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap},
		breaker: remountBreakerThreshold,
		onTrip:  tripRetreat,
		alt:     tccBreakerThreshold,
	},
	// fuse.deepwedge: deep-probe wedge verdict debounce on the ticker path
	// (select-time forceWedge is a zero-debounce forced fault).
	"fuse.deepwedge": {name: "fuse.deepwedge", debounce: deepWedgeStrikes},
	// fuse.shallowdead: holder List-liveness remount debounce.
	"fuse.shallowdead": {name: "fuse.shallowdead", debounce: shallowDeadStrikes},
	// fp.domain: File Provider wedge debounce, then a backoff-spaced recovery
	// ladder parked at the breaker.
	"fp.domain": {
		name:     "fp.domain",
		debounce: fpWedgeStrikes,
		backoff:  fpRecoveryBackoff,
		breaker:  fpRecoveryBreaker,
		onTrip:   tripPark,
	},
	// auth.streak: consecutive definitive needs-login verdicts; the trip gates
	// polling (the 15m needsLoginPollInterval is cadence the consumer applies).
	"auth.streak": {name: "auth.streak", debounce: needsLoginAfter, onTrip: tripGate},
	// ratelimit.acct / ratelimit.pool: per-account and pool-wide 429 streak
	// backoff; the trip gates further sampling within the backoff window.
	"ratelimit.acct": {
		name:    "ratelimit.acct",
		backoff: proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap},
		onTrip:  tripGate,
	},
	"ratelimit.pool": {
		name:    "ratelimit.pool",
		backoff: proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap},
		onTrip:  tripGate,
	},
	// fp.app: companion-app ensure. No debounce (a down control socket is an
	// immediate fault) and no breaker (a File-Provider host must keep retrying a
	// dead app); the fixed backoff alone bounds a crash-loop to one launch per
	// window and, booked before each spawn, spaces overlapping ensure calls apart.
	"fp.app": {
		name:    "fp.app",
		backoff: proc.Backoff{Base: fpAppEnsureBackoff, Cap: fpAppEnsureBackoff},
	},
	// fp.orphan: orphaned-domain reap. The debounce is the confirmation ladder —
	// fpOrphanReapStrikes consecutive confirmed sweeps before the reap fires; no
	// breaker (a failed RemoveDomain retries forever, spaced by the backoff). The
	// row acts on the debounced fault directly, so onTrip is unused.
	"fp.orphan": {
		name:     "fp.orphan",
		debounce: fpOrphanReapStrikes,
		backoff:  fpOrphanReapBackoff,
	},
}
