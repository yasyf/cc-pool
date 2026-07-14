package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
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
// parked row re-probes DEEP only on the backoff-cap window (shallow-listable is not
// proof a serve-stale domain recovered).
func TestHealFPRowsCadence(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(t *testing.T, s *Server, dir string)
		wantShallow int
		wantDeep    int
	}{
		{
			name:        "healthy, deep not due: shallow only",
			setup:       func(_ *testing.T, s *Server, dir string) { s.recordFPProbeClock(dir, time.Now()) },
			wantShallow: 1, wantDeep: 0,
		},
		{
			name:        "healthy, deep due: shallow then deep",
			setup:       func(_ *testing.T, _ *Server, _ string) {}, // never clocked -> deep due
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
			name: "parked, re-probe due: deep only",
			setup: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				parkFP(t, s, dir) // unclocked -> parked re-probe due
			},
			wantShallow: 0, wantDeep: 1,
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

// forceFPDeepProbeDue drops dir's periodic-probe clock so the next healthy tick
// runs the deep probe (which the real clock spaces by fpDeepProbeInterval — wall
// time a test cannot advance).
func forceFPDeepProbeDue(s *Server, dir string) {
	s.fpProbeClockMu.Lock()
	delete(s.fpProbeClock, dir)
	s.fpProbeClockMu.Unlock()
}

// TestHealFPRowsDeepOnlyFailureLatches is the G-X3 regression: a persistent deep-only
// wedge (shallow keeps succeeding, deep keeps failing) must still latch the wedge
// after fpWedgeStrikes deep failures — the interleaved clean shallow ticks must not
// erase the accumulating deep strike. A later clean deep probe then clears it.
func TestHealFPRowsDeepOnlyFailureLatches(t *testing.T) {
	s, _, dirs, _ := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })
	swapFPDomainProbeShallow(t, func(_ context.Context, _ string) error { return nil })              // shallow always OK
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { return overlay.ErrFPProbeEmpty }) // deep serve-stale on a non-empty synth

	for i := 0; i < fpWedgeStrikes; i++ {
		if s.fpWedged(dirs[1]) {
			t.Fatalf("wedged after %d deep strikes, want only after %d", i, fpWedgeStrikes)
		}
		forceFPDeepProbeDue(s, dirs[1]) // each tick both shallow-clears and deep-strikes
		s.healFPRows(t.Context())
	}
	if !s.fpWedged(dirs[1]) {
		t.Fatalf("%d consecutive deep failures interleaved with clean shallow ticks must latch the wedge", fpWedgeStrikes)
	}

	// A clean deep probe on the due window clears the deep wedge and its ladder.
	fpForceRecoveryDue(t, s, dirs[1])
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { return nil })
	s.healFPRows(t.Context())
	if s.fpWedged(dirs[1]) {
		t.Fatal("a clean deep probe must clear the deep wedge verdict")
	}
	if s.fpAttemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("recovery must reset the ladder: attemptsSoFar=%d, want 0", s.fpAttemptsSoFar(dirs[1]))
	}
}

// TestHealFPRowsShallowCleanKeepsDeepStrike is the narrow G-X3 unit: a clean shallow
// tick between two deep failures must not reset the single accumulated deep strike.
func TestHealFPRowsShallowCleanKeepsDeepStrike(t *testing.T) {
	s := newFPLedgerServer(alwaysNonEmpty)
	// One deep strike, below the debounce.
	s.recordFPProbe(fpTestDir, overlay.ErrFPProbeEmpty)
	if s.fpWedged(fpTestDir) {
		t.Fatal("one deep strike must not wedge")
	}
	// A clean shallow tick must NOT erase it (the old shared-ledger clear did).
	s.recordFPShallow(fpTestDir, nil)
	// The second deep strike now latches.
	s.recordFPProbe(fpTestDir, overlay.ErrFPProbeEmpty)
	if !s.fpWedged(fpTestDir) {
		t.Fatal("a clean shallow tick erased an accumulating deep strike: deep wedge never latched")
	}
}

// TestHealFPRowsParkedReprobesDeep is the G-X4 regression: a parked domain re-probes
// DEEP, so a shallow-listable but serve-stale domain stays parked, and only a clean
// deep un-parks it.
func TestHealFPRowsParkedReprobesDeep(t *testing.T) {
	newParked := func(t *testing.T) (*Server, map[int]string) {
		t.Helper()
		s, _, dirs, _ := newFPHealServer(t)
		s.fpBridgeReadyFn = func() bool { return true }
		swapFPDirLinked(t, func(string) bool { return true })
		wedgeIt(t, s, dirs[1])
		parkFP(t, s, dirs[1]) // unclocked -> parked re-probe due
		return s, dirs
	}

	t.Run("shallow-listable but deep-empty stays parked", func(t *testing.T) {
		s, dirs := newParked(t)
		var shallow, deep int
		swapFPDomainProbeShallow(t, func(_ context.Context, _ string) error { shallow++; return nil })
		swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return overlay.ErrFPProbeEmpty })

		s.healFPRows(t.Context())

		if shallow != 0 || deep != 1 {
			t.Fatalf("parked re-probe: shallow=%d deep=%d, want 0/1 (deep-only)", shallow, deep)
		}
		if !s.fpParked(dirs[1]) {
			t.Fatal("a deep-empty parked re-probe must keep the domain parked (shallow-listable is not serving)")
		}
	})

	t.Run("deep-clean un-parks", func(t *testing.T) {
		s, dirs := newParked(t)
		swapFPDomainProbe(t, func(_ context.Context, _ string) error { return nil })

		s.healFPRows(t.Context())

		if s.fpParked(dirs[1]) || s.fpWedged(dirs[1]) {
			t.Fatal("a clean deep parked re-probe must un-park and clear the wedge")
		}
	})
}

// TestHealFPRowsPrunesVanishedRowLedger is the G-X6 regression: the fp ledger and
// probe clock are pruned when an account row disappears, so a reused (gap-filled)
// account path never inherits stale parked state or a stale probe clock.
func TestHealFPRowsPrunesVanishedRowLedger(t *testing.T) {
	s, _, dirs, _ := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })
	dir := dirs[1]

	wedgeIt(t, s, dir)
	parkFP(t, s, dir)
	s.recordFPProbeClock(dir, time.Now())
	if !s.fpParked(dir) {
		t.Fatal("setup: want parked")
	}

	// The account row is converted off File Provider (leaves the FP row set).
	setRowKind(t, s, 1, fkoverlay.BackendSymlink)
	s.healFPRows(t.Context()) // a sweep with the row gone prunes its stale ledger + clock
	if s.fpParked(dir) || s.fpWedged(dir) {
		t.Fatal("a vanished FP row must have its stale ledger pruned")
	}

	// The path is reused by a fresh FP account: not parked, and deep-probed on the
	// first tick because its stale clock was pruned.
	setRowKind(t, s, 1, fkoverlay.BackendFileProvider)
	var deep int
	swapFPDomainProbeShallow(t, func(_ context.Context, _ string) error { return nil })
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return nil })

	s.healFPRows(t.Context())

	if s.fpParked(dir) {
		t.Fatal("a reused dir must not inherit stale parked state")
	}
	if deep != 1 {
		t.Fatalf("a reused dir's clock must be pruned so it deep-probes on the first tick: deep=%d, want 1", deep)
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
