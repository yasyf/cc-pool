package daemon

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
)

const fpTestDir = "/Users/x/.cc-pool/accounts/acct-01"

func alwaysNonEmpty(string) bool { return true }
func alwaysEmpty(string) bool    { return false }

func TestFPStateRecordProbe(t *testing.T) {
	var (
		wedged  = overlay.ErrFPProbeWedged
		missing = overlay.ErrFPProbeMissing
		empty   = overlay.ErrFPProbeEmpty
		timeout = fmt.Errorf("%w: read did not answer within 5s", overlay.ErrFPProbeWedged)
	)
	cases := []struct {
		name          string
		synthNonEmpty func(string) bool
		seq           []error
		wantWedged    bool
		wantWedgeLog  bool
	}{
		{"healthy untouched", alwaysNonEmpty, []error{nil}, false, false},
		{"one strike not wedged", alwaysNonEmpty, []error{wedged}, false, false},
		{"wedged after two strikes", alwaysNonEmpty, []error{wedged, wedged}, true, true},
		{"further strikes stay wedged and log once", alwaysNonEmpty, []error{wedged, wedged, wedged}, true, true},
		{"missing never strikes", alwaysNonEmpty, []error{missing, missing, missing, missing, missing}, false, false},
		{"timeout classified as wedged strikes", alwaysNonEmpty, []error{timeout, timeout}, true, true},
		{"empty never strikes when synth empty", alwaysEmpty, []error{empty, empty, empty}, false, false},
		{"empty strikes when synth non-empty", alwaysNonEmpty, []error{empty, empty}, true, true},
		{"empty does not count toward strikes when synth empty", alwaysEmpty, []error{empty, wedged}, false, false},
		{"transient strike then recovery leaves healthy", alwaysNonEmpty, []error{wedged, nil, wedged}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := newFPState(tc.synthNonEmpty)
			var sawWedgeLog bool
			for _, e := range tc.seq {
				if msg := fp.recordProbe(fpTestDir, e); strings.Contains(msg, "marking wedged") {
					sawWedgeLog = true
				}
			}
			if got := fp.wedged(fpTestDir); got != tc.wantWedged {
				t.Errorf("wedged = %v, want %v", got, tc.wantWedged)
			}
			if sawWedgeLog != tc.wantWedgeLog {
				t.Errorf("wedge log fired = %v, want %v", sawWedgeLog, tc.wantWedgeLog)
			}
		})
	}
}

// wedgeIt drives dir to the wedged verdict via fpWedgeStrikes consecutive
// wedged probes.
func wedgeIt(t *testing.T, fp *fpState, dir string) {
	t.Helper()
	for i := 0; i < fpWedgeStrikes; i++ {
		fp.recordProbe(dir, overlay.ErrFPProbeWedged)
	}
	if !fp.wedged(dir) {
		t.Fatalf("want wedged after %d strikes", fpWedgeStrikes)
	}
}

func TestFPStateBackoffProgressionAndBreaker(t *testing.T) {
	fp := newFPState(alwaysNonEmpty)
	wedgeIt(t, fp, fpTestDir)

	now := time.Unix(0, 0)
	if !fp.due(fpTestDir, now) {
		t.Fatal("a wedged, never-attempted domain must be immediately due")
	}

	// 30s → 60s → 120s → 240s → 480s, then the breaker trips at 5.
	wantDelays := []time.Duration{
		30 * time.Second, 60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second,
	}
	for i, delay := range wantDelays {
		attempt := i + 1
		if !fp.due(fpTestDir, now) {
			t.Fatalf("attempt %d: want due at %v", attempt, now)
		}
		gotAttempt, tripped := fp.recordAttempt(fpTestDir, now)
		if gotAttempt != attempt {
			t.Fatalf("attempt count = %d, want %d", gotAttempt, attempt)
		}
		if want := attempt >= fpRecoveryBreaker; tripped != want {
			t.Fatalf("attempt %d: tripped = %v, want %v", attempt, tripped, want)
		}
		if fp.due(fpTestDir, now) {
			t.Fatalf("attempt %d: due immediately after recording, want spaced by %v", attempt, delay)
		}
		if attempt < fpRecoveryBreaker {
			if !fp.due(fpTestDir, now.Add(delay)) {
				t.Fatalf("attempt %d: want due once the %v backoff elapses", attempt, delay)
			}
			if fp.due(fpTestDir, now.Add(delay-time.Nanosecond)) {
				t.Fatalf("attempt %d: due before the %v backoff elapsed", attempt, delay)
			}
		}
		now = now.Add(delay)
	}

	if fp.due(fpTestDir, now.Add(24*time.Hour)) {
		t.Error("breaker tripped at 5 attempts: the domain must never be due again")
	}
	if !fp.wedged(fpTestDir) {
		t.Error("a breaker-parked domain must stay wedged so the select path excludes it")
	}
}

// TestFPRecoveryBackoffBounds pins the 30s→10m span of the recovery backoff.
func TestFPRecoveryBackoffBounds(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{1, 30 * time.Second},
		{2, 60 * time.Second},
		{3, 120 * time.Second},
		{6, 10 * time.Minute},   // clamped to the cap
		{100, 10 * time.Minute}, // stays clamped
	}
	for _, tc := range cases {
		if got := fpRecoveryBackoff.After(tc.failures); got != tc.want {
			t.Errorf("fpRecoveryBackoff.After(%d) = %v, want %v", tc.failures, got, tc.want)
		}
	}
}

func TestFPStateResetOnSuccessClearsStrikesAndBackoff(t *testing.T) {
	fp := newFPState(alwaysNonEmpty)
	wedgeIt(t, fp, fpTestDir)
	fp.recordAttempt(fpTestDir, time.Unix(0, 0))

	log := fp.recordProbe(fpTestDir, nil)
	if fp.wedged(fpTestDir) {
		t.Error("a nil probe must clear the wedge verdict")
	}
	if !strings.Contains(log, "recovered") {
		t.Errorf("recovery from wedged must log once; got %q", log)
	}
	if fp.due(fpTestDir, time.Unix(1<<40, 0)) {
		t.Error("a recovered domain is not wedged, so it must never be due")
	}
	// Strikes restart from zero — one post-recovery strike is not a wedge.
	fp.recordProbe(fpTestDir, overlay.ErrFPProbeWedged)
	if fp.wedged(fpTestDir) {
		t.Error("one strike after recovery must not be wedged (strikes reset)")
	}
}

func TestFPStateReset(t *testing.T) {
	fp := newFPState(alwaysNonEmpty)
	wedgeIt(t, fp, fpTestDir)
	fp.recordAttempt(fpTestDir, time.Unix(0, 0))

	fp.reset(fpTestDir)
	if fp.wedged(fpTestDir) {
		t.Error("reset must clear the wedge verdict")
	}
	if fp.due(fpTestDir, time.Unix(1<<40, 0)) {
		t.Error("reset must clear recovery so a non-wedged dir is never due")
	}
	fp.recordProbe(fpTestDir, overlay.ErrFPProbeWedged)
	if fp.wedged(fpTestDir) {
		t.Error("strikes must restart from zero after reset")
	}
}
