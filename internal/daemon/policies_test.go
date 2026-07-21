package daemon

// policies_test.go pins product auth, rate-limit, polling, and sync constants.

import (
	"testing"
	"time"
)

// TestPolicyConstantsPinned freezes each self-heal policy constant against its
// literal. A drift fails loud with both the live value and the expected one.
func TestPolicyConstantsPinned(t *testing.T) {
	cases := []struct {
		name string
		got  any
		want any
	}{
		// auth.streak — definitive needs-login verdicts before polls throttle.
		{"auth.streak/needsLoginAfter", needsLoginAfter, 3},
		{"auth.streak/needsLoginPollInterval", needsLoginPollInterval, 15 * time.Minute},

		// ratelimit — per-account and pool-wide 429 streak backoff. A 429's
		// Retry-After overrides the computed window (that value is not pinnable).
		{"ratelimit/backoffBase", rateLimitBackoffBase, 3 * time.Minute},
		{"ratelimit/backoffCap", rateLimitBackoffCap, 30 * time.Minute},

		// Poll cadences with jitter.
		{"tickers/basePollInterval", basePollInterval, 180 * time.Second},
		{"tickers/pollJitter", pollJitter, 30 * time.Second},
		{"tickers/outagePollInterval", outagePollInterval, 20 * time.Second},
		{"tickers/outageJitter", outageJitter, 5 * time.Second},

		// Sync engine heal-pull timeout.
		{"sync-drift-guards/syncHealTimeout", syncHealTimeout, 15 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("policy constant drifted: got %v (%T), want %v (%T)", tc.got, tc.got, tc.want, tc.want)
			}
		})
	}
}
