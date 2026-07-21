package daemon

import (
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// This file owns the daemon's product-level auth and rate-limit policy tuning.
// Filesystem lifecycle and recovery policy belongs to FuseKit.

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

// policy is one product gate's debounce and backoff tuning.
type policy struct {
	name     string
	debounce int
	backoff  proc.Backoff
}

// policies is THE self-heal policy table — one row per family, each referencing
// the pinned constants above. A ledger row is keyed by its policy's name plus a
// resource (an account dir, id, or "pool").
var policies = map[string]policy{
	// auth.streak: consecutive definitive needs-login verdicts; the trip gates
	// polling (the 15m needsLoginPollInterval is cadence the consumer applies).
	"auth.streak": {name: "auth.streak", debounce: needsLoginAfter},
	// ratelimit.acct / ratelimit.pool: per-account and pool-wide 429 streak
	// backoff; the trip gates further sampling within the backoff window.
	"ratelimit.acct": {
		name:    "ratelimit.acct",
		backoff: proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap},
	},
	"ratelimit.pool": {
		name:    "ratelimit.pool",
		backoff: proc.Backoff{Base: rateLimitBackoffBase, Cap: rateLimitBackoffCap},
	},
}
