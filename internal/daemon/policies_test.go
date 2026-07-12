package daemon

// policies_test.go is the incident-scar contract: it pins every self-heal policy
// constant to its current literal so the daemon self-heal redesign (fpState →
// ledger, rowRetry → ledger, streaks → ledger, registry cutover) cannot silently
// drift a value. It owns VALUES only — behavior (breaker mutual reset,
// park-not-retreat, edge latches) stays owned by the existing behavior suites.
// The redesign will re-point identifiers; the literals must survive unchanged.
//
// healFuse's sentinel→outcome classification is deliberately NOT pinned here: it
// is an inline switch reachable only by driving mountFuse, with side-effectful
// arms (fallbackToSymlink) and combined-sentinel cases, so its membership can
// only be asserted through behavior — which deepprobe_test.go and server_test.go
// own.

import (
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
)

// TestPolicyConstantsPinned freezes each self-heal policy constant against its
// literal. A drift fails loud with both the live value and the expected one.
func TestPolicyConstantsPinned(t *testing.T) {
	cases := []struct {
		name string
		got  any
		want any
	}{
		// fuse.remount — per-row remount backoff and the two mutually-resetting
		// breakers. The breakers share one backoff clock and reset each other
		// (structural; owned by the behavior suites, not pinned here).
		{"fuse.remount/backoffBase", remountBackoffBase, 10 * time.Second},
		{"fuse.remount/backoffCap", remountBackoffCap, 2 * time.Minute},
		{"fuse.remount/hazardBreaker", remountBreakerThreshold, 5}, // escalateWedgedRow
		{"fuse.remount/tccBreaker", tccBreakerThreshold, 6},        // escalateTCCBlockedRow

		// fuse.deepwedge — deep-probe wedge verdict debounce on the ticker path.
		// select-time forceWedge is zero-debounce (structural, not a constant).
		{"fuse.deepwedge/strikes", deepWedgeStrikes, 2},

		// fuse.shallowdead — holder shallow-dead (List-liveness) remount debounce.
		{"fuse.shallowdead/strikes", shallowDeadStrikes, 2},

		// fp.domain — File Provider wedge debounce, recovery backoff, park breaker.
		{"fp.domain/strikes", fpWedgeStrikes, 2},
		{"fp.domain/recoveryBackoffBase", fpRecoveryBackoff.Base, 30 * time.Second},
		{"fp.domain/recoveryBackoffCap", fpRecoveryBackoff.Cap, 10 * time.Minute},
		{"fp.domain/recoveryBreaker", fpRecoveryBreaker, 5},

		// fp.app — companion-app ensure: the fixed launch backoff window (no
		// debounce, no breaker; structural, owned by the fpapp behavior suite).
		{"fp.app/ensureBackoff", fpAppEnsureBackoff, time.Minute},

		// fp.orphan — orphaned-domain reap: the confirmation debounce and the fixed
		// failed-remove retry backoff.
		{"fp.orphan/reapStrikes", fpOrphanReapStrikes, 3},
		{"fp.orphan/reapBackoffBase", fpOrphanReapBackoff.Base, 5 * time.Minute},
		{"fp.orphan/reapBackoffCap", fpOrphanReapBackoff.Cap, 5 * time.Minute},

		// auth.streak — definitive needs-login verdicts before polls throttle.
		{"auth.streak/needsLoginAfter", needsLoginAfter, 3},
		{"auth.streak/needsLoginPollInterval", needsLoginPollInterval, 15 * time.Minute},

		// ratelimit — per-account and pool-wide 429 streak backoff. A 429's
		// Retry-After overrides the computed window (that value is not pinnable).
		{"ratelimit/backoffBase", rateLimitBackoffBase, 3 * time.Minute},
		{"ratelimit/backoffCap", rateLimitBackoffCap, 30 * time.Minute},

		// tickers — heal cadence plus the steady/outage poll cadences with jitter.
		{"tickers/healInterval", defaultHealInterval, 10 * time.Second},
		{"tickers/basePollInterval", basePollInterval, 180 * time.Second},
		{"tickers/pollJitter", pollJitter, 30 * time.Second},
		{"tickers/outagePollInterval", outagePollInterval, 20 * time.Second},
		{"tickers/outageJitter", outageJitter, 5 * time.Second},

		// sync engine policy — NOT ported by the redesign; pinned only against
		// accidental drift during the registry cutover (B2). The external synckitd
		// mesh reconcile (~900s) lives outside the daemon and is not a daemon
		// constant; takeoverStaleAfter must stay well above that tick.
		{"sync-drift-guards/syncHealTimeout", syncHealTimeout, 15 * time.Second},
		{"sync-drift-guards/credMirrorQueueSize", credMirrorQueueSize, 64},
		{"sync-drift-guards/takeoverStaleAfter", takeoverStaleAfter, 35 * time.Minute},
		{"sync-drift-guards/holderLeaseDuration", holderLeaseDuration, 45 * time.Minute},
		{"sync-drift-guards/leaseRenewUnder", leaseRenewUnder, 20 * time.Minute},

		// registry cutover (B2) — minimum companion version gating FP hosting.
		// Compile-visible from daemon (it already imports pool); pinned here so a
		// bump is a deliberate edit to this contract.
		{"registry/minWidgetVersion", pool.MinWidgetVersion, "v0.44.0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("policy constant drifted: got %v (%T), want %v (%T)", tc.got, tc.got, tc.want, tc.want)
			}
		})
	}
}
