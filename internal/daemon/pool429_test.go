package daemon

import (
	"testing"
	"time"
)

// TestPollOncePool429GatesRestOfSweep pins the pool-wide 429 gate: one account's
// 429 skips the rest of the sweep, arms the pool streak, and never enters outage.
func TestPollOncePool429GatesRestOfSweep(t *testing.T) {
	s, fo := newOutageServer(t, 3)
	fo.rlByAT = map[string]bool{"at-1": true} // acct1 429s; acct2/acct3 would succeed

	s.pollOnce(t.Context())

	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("acct1 sampled %d time(s), want 1", got)
	}
	for _, at := range []string{"at-2", "at-3"} {
		if got := fo.usageCountFor(at); got != 0 {
			t.Fatalf("%s sampled %d time(s), want 0 — a 429 on acct1 must gate the rest of the sweep", at, got)
		}
	}
	if s.rlStreakPool != 1 {
		t.Fatalf("rlStreakPool = %d, want 1 after one 429 sweep", s.rlStreakPool)
	}
	if s.lastPool429.IsZero() {
		t.Fatal("lastPool429 must be stamped on a 429")
	}
	if s.netOutage {
		t.Fatal("a 429 is outcomeNonNetwork (the API answered) and must NOT enter outage mode")
	}
}

// TestPollOnceCleanSweepArmsNoPoolGate: no 429 → every account polled, gate disarmed.
func TestPollOnceCleanSweepArmsNoPoolGate(t *testing.T) {
	s, fo := newOutageServer(t, 3) // every account answers cleanly

	s.pollOnce(t.Context())

	if s.rlStreakPool != 0 {
		t.Fatalf("rlStreakPool = %d after a clean sweep, want 0", s.rlStreakPool)
	}
	if s.poolRateLimited() {
		t.Fatal("a clean sweep must not arm the pool gate")
	}
	for _, at := range []string{"at-1", "at-2", "at-3"} {
		if got := fo.usageCountFor(at); got != 1 {
			t.Fatalf("%s sampled %d time(s), want 1 (no gate on a clean sweep)", at, got)
		}
	}
}

// TestPollOncePool429GateExpiresAndResumes: an active gate skips the whole sweep;
// once the window elapses a clean sample resumes polling and clears the gate.
func TestPollOncePool429GateExpiresAndResumes(t *testing.T) {
	t.Run("active gate skips the whole sweep", func(t *testing.T) {
		s, fo := newOutageServer(t, 2) // both accounts would answer cleanly
		s.rlStreakPool = 1
		s.lastPool429 = time.Now() // fresh → inside rlBackoff(1)=3m

		s.pollOnce(t.Context())

		if got := fo.usageCallCount(); got != 0 {
			t.Fatalf("an active pool gate must skip every account; Usage called %d time(s), want 0", got)
		}
	})

	t.Run("expired gate resumes polling and clears", func(t *testing.T) {
		s, fo := newOutageServer(t, 2)
		s.rlStreakPool = 1
		s.lastPool429 = time.Now().Add(-rlBackoff(1) - time.Second) // window elapsed

		s.pollOnce(t.Context())

		if got := fo.usageCountFor("at-1"); got != 1 {
			t.Fatalf("acct1 sampled %d time(s) after the window, want 1", got)
		}
		if got := fo.usageCountFor("at-2"); got != 1 {
			t.Fatalf("acct2 sampled %d time(s), want 1 — a clean acct1 clears the pool gate", got)
		}
		if s.rlStreakPool != 0 {
			t.Fatalf("rlStreakPool = %d, want 0 after a clean sweep clears the gate", s.rlStreakPool)
		}
	})
}

// TestPollOncePool429RetryAfterOverridesBackoff pins that a Retry-After hint on
// the 429 sets the gate window in place of the computed exponential backoff.
func TestPollOncePool429RetryAfterOverridesBackoff(t *testing.T) {
	s, fo := newOutageServer(t, 2)
	fo.usage429 = true
	fo.retryAfter = time.Second // the server asks for 1s, far below rlBackoff(1)=3m

	s.pollOnce(t.Context()) // acct1 429s and arms the gate with the Retry-After hint

	if s.pool429RetryAfter != time.Second {
		t.Fatalf("pool429RetryAfter = %v, want 1s (the 429's Retry-After must reach the scheduler)", s.pool429RetryAfter)
	}
	// 2s after the 429: past the 1s Retry-After but inside the 3m backoff.
	s.lastPool429 = time.Now().Add(-2 * time.Second)
	if s.poolRateLimited() {
		t.Fatalf("Retry-After (1s) must override the computed backoff (%v): the gate should be expired 2s later", rlBackoff(1))
	}
	// Without the hint, the same elapsed time is still inside the computed backoff.
	s.pool429RetryAfter = 0
	if !s.poolRateLimited() {
		t.Fatal("without a Retry-After hint the computed 3m backoff must still gate 2s after the 429")
	}
}

// TestPollOncePool429Uses30mCapWindow pins that a deep 429 streak gates for the
// full 30m cap end to end: still gated at 29m, lifted past 31m.
func TestPollOncePool429Uses30mCapWindow(t *testing.T) {
	s, _ := newOutageServer(t, 2)
	s.rlStreakPool = 8 // deep streak → window clamped to the 30m cap

	s.lastPool429 = time.Now().Add(-29 * time.Minute) // inside the cap
	if !s.poolRateLimited() {
		t.Fatalf("a deep 429 streak must gate for the full %v cap; 29m in should still be gated", rateLimitBackoffCap)
	}
	s.lastPool429 = time.Now().Add(-31 * time.Minute) // past the cap
	if s.poolRateLimited() {
		t.Fatal("31m after the 429 — past the 30m cap — the gate must lift")
	}
}

// TestPollOncePool429RetryAfterClampedToCap: a hostile 24h Retry-After must not
// suspend the fleet for 24h — it is clamped to the 30m cap, so the gate lifts.
func TestPollOncePool429RetryAfterClampedToCap(t *testing.T) {
	s, _ := newOutageServer(t, 2)
	s.rlStreakPool = 1
	s.pool429RetryAfter = 24 * time.Hour // absurd server hint

	s.lastPool429 = time.Now().Add(-29 * time.Minute) // inside the 30m cap
	if !s.poolRateLimited() {
		t.Fatal("29m in, the clamped gate must still hold")
	}
	s.lastPool429 = time.Now().Add(-31 * time.Minute) // past the cap
	if s.poolRateLimited() {
		t.Fatalf("a 24h Retry-After must be clamped to the %v cap; 31m in, the gate must lift", rateLimitBackoffCap)
	}
}
