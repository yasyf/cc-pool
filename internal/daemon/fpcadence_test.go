package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
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
// rows never deep-probe; a wedged-not-due row is skipped; a due wedge takes one
// deep verdict; and a parked row re-probes only on the backoff-cap window.
func TestHealFPRowsCadence(t *testing.T) {
	cases := []struct {
		name      string
		reconcile func(t *testing.T, s *Server, dir string)
		wantDeep  int
	}{
		{
			name:      "healthy, deep not due: no probe",
			reconcile: func(_ *testing.T, s *Server, dir string) { s.recordFPProbeClock(dir, time.Now()) },
			wantDeep:  0,
		},
		{
			name:      "healthy, never probed: no probe",
			reconcile: func(_ *testing.T, _ *Server, _ string) {},
			wantDeep:  0,
		},
		{
			name: "wedged, not due: no probe (the ladder owns it)",
			reconcile: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				s.fpRecordAttempt(dir, time.Now()) // booked -> backing off, not due
			},
			wantDeep: 0,
		},
		{
			name: "wedged, recovery due: deep only",
			reconcile: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				fpForceRecoveryDue(t, s, dir)
			},
			wantDeep: 1,
		},
		{
			name: "parked, re-probe due: deep only",
			reconcile: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				parkFP(t, s, dir) // unclocked -> parked re-probe due
			},
			wantDeep: 1,
		},
		{
			name: "parked, re-probe not due: no probe",
			reconcile: func(t *testing.T, s *Server, dir string) {
				wedgeIt(t, s, dir)
				parkFP(t, s, dir)
				s.recordFPProbeClock(dir, time.Now())
			},
			wantDeep: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _, dirs, _ := newFPHealServer(t)
			s.fpBridgeReadyFn = func() bool { return true }
			swapFPDirLinked(t, func(string) bool { return true })
			var deep int
			swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return nil })
			tc.reconcile(t, s, dirs[1])

			s.healFPRows(t.Context())

			if deep != tc.wantDeep {
				t.Fatalf("deep=%d, want %d", deep, tc.wantDeep)
			}
		})
	}
}

func TestHealFPRowsNeverProbesHealthyFleet(t *testing.T) {
	s, _, _, _ := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })
	for id := 2; id <= 20; id++ {
		if err := s.m.Store.UpsertAccount(store.Account{
			ID: id, ConfigDir: t.TempDir(), OverlayKind: string(fkoverlay.BackendFileProvider),
			KeychainService: "svc", KeychainAccount: "acct",
		}); err != nil {
			t.Fatal(err)
		}
	}
	deep := 0
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return nil })
	for range 5 {
		s.healFPRows(t.Context())
	}
	if deep != 0 {
		t.Fatalf("20 healthy FP rows triggered %d maintenance deep probes, want 0", deep)
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
		var deep int
		swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return overlay.ErrFPProbeEmpty })

		s.healFPRows(t.Context())

		if deep != 1 {
			t.Fatalf("parked re-probe: deep=%d, want 1", deep)
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
		t.Fatal("reconcile: want parked")
	}

	// The account row is converted off File Provider (leaves the FP row set).
	setRowKind(t, s, 1, fkoverlay.BackendSymlink)
	s.healFPRows(t.Context()) // a sweep with the row gone prunes its stale ledger + clock
	if s.fpParked(dir) || s.fpWedged(dir) {
		t.Fatal("a vanished FP row must have its stale ledger pruned")
	}

	// The path is reused by a fresh FP account: not parked and not maintenance-probed.
	setRowKind(t, s, 1, fkoverlay.BackendFileProvider)
	var deep int
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return nil })

	s.healFPRows(t.Context())

	if s.fpParked(dir) {
		t.Fatal("a reused dir must not inherit stale parked state")
	}
	if deep != 0 {
		t.Fatalf("a healthy reused dir must wait for selection validation: deep=%d, want 0", deep)
	}
}

// TestFPHealAttempt1FiresUnconditionalSignal pins that recovery attempt 1 does the
// non-destructive Reconcile AND the exported UNCONDITIONAL Signal (semantic-generation
// bypass) so a wedged domain is always nudged.
func TestFPHealAttempt1FiresUnconditionalSignal(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	wedgeIt(t, s, dirs[1])

	healFPStep(t, s, a, time.Unix(0, 0))

	if _, _, reasserts, _ := fake.counts(); reasserts != 1 {
		t.Fatalf("attempt 1 Reconcile: reasserts=%d, want 1", reasserts)
	}
	if n := fake.signalCount(); n != 1 {
		t.Fatalf("attempt 1 must fire exactly one unconditional Signal, got %d", n)
	}
}

// TestHealFPMissingDebouncesAppDown pins the P1(b) fallout: a Missing probe whose
// Check surfaces a down app debounces — never a reconcile, never a booked recovery
// attempt (the domain survives the app's death).
func TestHealFPMissingDebouncesAppDown(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.checkErr = fmt.Errorf("state domain: %w", fileproviderd.ErrAppUnavailable)

	s.healFPMissing(t.Context(), a, time.Unix(0, 0))

	if _, registrations, _, _ := fake.counts(); registrations != 0 {
		t.Fatalf("a down app must not reconcile (Reconcile): registrations=%d, want 0", registrations)
	}
	if got := s.fpAttemptsSoFar(dirs[1]); got != 0 {
		t.Fatalf("a down app must not book a recovery attempt: attemptsSoFar=%d, want 0", got)
	}
}

// TestReconcileFileProviderDebouncesAppDown pins that reconcile debounces a down app
// (fpDeferred) rather than piling a Reconcile on a domain the app cannot answer for.
func TestReconcileFileProviderDebouncesAppDown(t *testing.T) {
	s, a, _, fake := newFPHealServer(t)
	fake.checkErr = fmt.Errorf("state domain: %w", fileproviderd.ErrAppUnavailable)
	if !s.cl.hold(a.ID) {
		t.Fatalf("acct-%02d poll claim refused", a.ID)
	}
	defer s.cl.disownHold(a.ID)

	if got := s.reconcileFileProvider(t.Context(), a); got != fpDeferred {
		t.Fatalf("reconcile of a down-app domain = %v, want fpDeferred", got)
	}
	if _, registrations, _, _ := fake.counts(); registrations != 0 {
		t.Fatalf("a down app must not trigger a Reconcile: registrations=%d, want 0", registrations)
	}
}
