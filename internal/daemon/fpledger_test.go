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

// newFPLedgerServer builds a minimal Server with the FP self-heal ledger wired and
// the given synth seam — the standalone surface the retired fpState state-machine
// unit tests now drive (recordFPProbe / fpRecordAttempt / fpReset over s.led).
func newFPLedgerServer(synthNonEmpty func(string) bool) *Server {
	return &Server{led: newLedgers(), fpSynth: synthNonEmpty}
}

// fpDue mirrors the retired fpState.due: a domain is due for a recovery attempt
// only when it is wedged AND its recovery schedule permits another attempt.
func fpDue(s *Server, dir string, now time.Time) bool {
	return s.fpWedged(dir) && s.fpRecoveryDue(dir, now)
}

// wedgeIt drives dir to the wedged verdict via fpWedgeStrikes consecutive wedged
// probes.
func wedgeIt(t *testing.T, s *Server, dir string) {
	t.Helper()
	for i := 0; i < fpWedgeStrikes; i++ {
		s.recordFPProbe(dir, overlay.ErrFPProbeWedged)
	}
	if !s.fpWedged(dir) {
		t.Fatalf("want wedged after %d strikes", fpWedgeStrikes)
	}
}

func TestFPStateRecordProbe(t *testing.T) {
	var (
		wedged    = overlay.ErrFPProbeWedged
		missing   = overlay.ErrFPProbeMissing
		empty     = overlay.ErrFPProbeEmpty
		noVerdict = overlay.ErrFPProbeNoVerdict
		timeout   = fmt.Errorf("%w: read did not answer within 5s", overlay.ErrFPProbeWedged)
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
		{"no-verdict never strikes", alwaysNonEmpty, []error{noVerdict, noVerdict, noVerdict}, false, false},
		{"no-verdict between strikes does not reset the debounce", alwaysNonEmpty, []error{wedged, noVerdict, wedged}, true, true},
		{"no-verdict does not clear an established wedge", alwaysNonEmpty, []error{wedged, wedged, noVerdict}, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newFPLedgerServer(tc.synthNonEmpty)
			var sawWedgeLog bool
			for _, e := range tc.seq {
				if msg := s.recordFPProbe(fpTestDir, e); strings.Contains(msg, "marking wedged") {
					sawWedgeLog = true
				}
			}
			if got := s.fpWedged(fpTestDir); got != tc.wantWedged {
				t.Errorf("wedged = %v, want %v", got, tc.wantWedged)
			}
			if sawWedgeLog != tc.wantWedgeLog {
				t.Errorf("wedge log fired = %v, want %v", sawWedgeLog, tc.wantWedgeLog)
			}
		})
	}
}

func TestFPStateBackoffProgressionAndBreaker(t *testing.T) {
	s := newFPLedgerServer(alwaysNonEmpty)
	wedgeIt(t, s, fpTestDir)

	now := time.Unix(0, 0)
	if !fpDue(s, fpTestDir, now) {
		t.Fatal("a wedged, never-attempted domain must be immediately due")
	}

	// 30s → 60s → 120s → 240s → 480s, then the breaker trips at 5.
	wantDelays := []time.Duration{
		30 * time.Second, 60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second,
	}
	for i, delay := range wantDelays {
		attempt := i + 1
		if !fpDue(s, fpTestDir, now) {
			t.Fatalf("attempt %d: want due at %v", attempt, now)
		}
		gotAttempt, tripped := s.fpRecordAttempt(fpTestDir, now)
		if gotAttempt != attempt {
			t.Fatalf("attempt count = %d, want %d", gotAttempt, attempt)
		}
		if want := attempt >= fpRecoveryBreaker; tripped != want {
			t.Fatalf("attempt %d: tripped = %v, want %v", attempt, tripped, want)
		}
		if fpDue(s, fpTestDir, now) {
			t.Fatalf("attempt %d: due immediately after recording, want spaced by %v", attempt, delay)
		}
		if attempt < fpRecoveryBreaker {
			if !fpDue(s, fpTestDir, now.Add(delay)) {
				t.Fatalf("attempt %d: want due once the %v backoff elapses", attempt, delay)
			}
			if fpDue(s, fpTestDir, now.Add(delay-time.Nanosecond)) {
				t.Fatalf("attempt %d: due before the %v backoff elapsed", attempt, delay)
			}
		}
		now = now.Add(delay)
	}

	if fpDue(s, fpTestDir, now.Add(24*time.Hour)) {
		t.Error("breaker tripped at 5 attempts: the domain must never be due again")
	}
	if !s.fpWedged(fpTestDir) {
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
	s := newFPLedgerServer(alwaysNonEmpty)
	wedgeIt(t, s, fpTestDir)
	s.fpRecordAttempt(fpTestDir, time.Unix(0, 0))

	log := s.recordFPProbe(fpTestDir, nil)
	if s.fpWedged(fpTestDir) {
		t.Error("a nil probe must clear the wedge verdict")
	}
	if !strings.Contains(log, "recovered") {
		t.Errorf("recovery from wedged must log once; got %q", log)
	}
	if fpDue(s, fpTestDir, time.Unix(1<<40, 0)) {
		t.Error("a recovered domain is not wedged, so it must never be due")
	}
	// Strikes restart from zero — one post-recovery strike is not a wedge.
	s.recordFPProbe(fpTestDir, overlay.ErrFPProbeWedged)
	if s.fpWedged(fpTestDir) {
		t.Error("one strike after recovery must not be wedged (strikes reset)")
	}
}

func TestFPStateReset(t *testing.T) {
	s := newFPLedgerServer(alwaysNonEmpty)
	wedgeIt(t, s, fpTestDir)
	s.fpRecordAttempt(fpTestDir, time.Unix(0, 0))

	s.fpReset(fpTestDir)
	if s.fpWedged(fpTestDir) {
		t.Error("reset must clear the wedge verdict")
	}
	if fpDue(s, fpTestDir, time.Unix(1<<40, 0)) {
		t.Error("reset must clear recovery so a non-wedged dir is never due")
	}
	s.recordFPProbe(fpTestDir, overlay.ErrFPProbeWedged)
	if s.fpWedged(fpTestDir) {
		t.Error("strikes must restart from zero after reset")
	}
}
