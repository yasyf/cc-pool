package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/fileproviderd"
)

// parkFP drives a wedged dir past the recovery breaker so it is parked.
func parkFP(t *testing.T, s *Server, dir string) {
	t.Helper()
	for i := 0; i < fpRecoveryBreaker; i++ {
		s.fpRecordAttempt(dir, time.Now())
	}
	if !s.fpParked(dir) {
		t.Fatalf("want parked after %d attempts", fpRecoveryBreaker)
	}
}

// TestHealFPRowsCadence pins the per-tick probe cadence by state class: healthy
// rows shallow-probe every tick and deep-probe only on the fpDeepProbeInterval;
// a wedged-not-due row is skipped entirely; a due wedge takes one deep verdict; a
// parked row re-probes shallow only on the backoff-cap window.
func TestHealFPRowsCadence(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(t *testing.T, s *Server, dir string)
		wantShallow int
		wantDeep    int
	}{
		{
			name:        "healthy, deep not due: shallow only",
			setup:       func(t *testing.T, s *Server, dir string) { s.recordFPProbeClock(dir, time.Now()) },
			wantShallow: 1, wantDeep: 0,
		},
		{
			name:        "healthy, deep due: shallow then deep",
			setup:       func(t *testing.T, s *Server, dir string) {}, // never clocked -> deep due
			wantShallow: 1, wantDeep: 1,
		},
		{
			name: "wedged, not due: no probe (the ladder owns it)",
			setup: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				s.fpRecordAttempt(dir, time.Now()) // booked -> backing off, not due
			},
			wantShallow: 0, wantDeep: 0,
		},
		{
			name: "wedged, recovery due: deep only",
			setup: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				fpForceRecoveryDue(t, s, dir)
			},
			wantShallow: 0, wantDeep: 1,
		},
		{
			name: "parked, re-probe due: shallow only",
			setup: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				parkFP(t, s, dir) // unclocked -> parked re-probe due
			},
			wantShallow: 1, wantDeep: 0,
		},
		{
			name: "parked, re-probe not due: no probe",
			setup: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				parkFP(t, s, dir)
				s.recordFPProbeClock(dir, time.Now())
			},
			wantShallow: 0, wantDeep: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _, dirs, _ := newFPHealServer(t)
			s.fpBridgeReadyFn = func() bool { return true }
			swapFPDirLinked(t, func(string) bool { return true })
			var shallow, deep int
			swapFPDomainProbeShallow(t, func(_ context.Context, _ string) error { shallow++; return nil })
			swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return nil })
			tc.setup(t, s, dirs[1])

			s.healFPRows(t.Context())

			if shallow != tc.wantShallow || deep != tc.wantDeep {
				t.Fatalf("shallow=%d deep=%d, want shallow=%d deep=%d", shallow, deep, tc.wantShallow, tc.wantDeep)
			}
		})
	}
}

// TestHealFPRowsShallowFailureStrikes pins that a shallow wedge verdict strikes
// through the 2-strike debounce, exactly like a deep one.
func TestHealFPRowsShallowFailureStrikes(t *testing.T) {
	s, _, dirs, _ := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })
	swapFPDomainProbeShallow(t, func(_ context.Context, _ string) error { return overlay.ErrFPProbeWedged })

	s.healFPRows(t.Context())
	if s.fpWedged(dirs[1]) {
		t.Fatal("one shallow strike must not wedge (2-strike debounce)")
	}
	s.healFPRows(t.Context())
	if !s.fpWedged(dirs[1]) {
		t.Fatalf("%d consecutive shallow wedge strikes must mark the domain wedged", fpWedgeStrikes)
	}
}

// TestFPHealAttempt1FiresUnconditionalSignal pins that recovery attempt 1 does the
// non-destructive Sync AND the exported UNCONDITIONAL Signal (fingerprint-cache
// bypass) so a wedged domain is always nudged.
func TestFPHealAttempt1FiresUnconditionalSignal(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	wedgeIt(t, s, dirs[1])

	healFPStep(t, s, a, time.Unix(0, 0))

	if _, _, syncs, _ := fake.counts(); syncs != 1 {
		t.Fatalf("attempt 1 Sync: syncs=%d, want 1", syncs)
	}
	if n := fake.signalCount(); n != 1 {
		t.Fatalf("attempt 1 must fire exactly one unconditional Signal, got %d", n)
	}
}

// TestHealFPMissingDebouncesAppDown pins the P1(b) fallout: a Missing probe whose
// Health surfaces a down app debounces — never a reconcile, never a booked recovery
// attempt (the domain survives the app's death).
func TestHealFPMissingDebouncesAppDown(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.healthErr = fmt.Errorf("state domain: %w", fileproviderd.ErrAppUnavailable)

	s.healFPMissing(t.Context(), a, time.Unix(0, 0))

	if _, setups, _, _ := fake.counts(); setups != 0 {
		t.Fatalf("a down app must not reconcile (Setup): setups=%d, want 0", setups)
	}
	if got := s.fpAttemptsSoFar(dirs[1]); got != 0 {
		t.Fatalf("a down app must not book a recovery attempt: attemptsSoFar=%d, want 0", got)
	}
}

// TestReconcileFileProviderDebouncesAppDown pins that reconcile debounces a down app
// (fpDeferred) rather than piling a Setup on a domain the app cannot answer for.
func TestReconcileFileProviderDebouncesAppDown(t *testing.T) {
	s, a, _, fake := newFPHealServer(t)
	fake.healthErr = fmt.Errorf("state domain: %w", fileproviderd.ErrAppUnavailable)
	if !s.cl.hold(a.ID) {
		t.Fatalf("acct-%02d poll claim refused", a.ID)
	}
	defer s.cl.disownHold(a.ID)

	if got := s.reconcileFileProvider(t.Context(), a); got != fpDeferred {
		t.Fatalf("reconcile of a down-app domain = %v, want fpDeferred", got)
	}
	if _, setups, _, _ := fake.counts(); setups != 0 {
		t.Fatalf("a down app must not trigger a Setup: setups=%d, want 0", setups)
	}
}
