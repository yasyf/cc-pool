package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
)

// swapFPDomainProbe overrides the package-level FP probe seam for one test.
func swapFPDomainProbe(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := fpDomainProbe
	fpDomainProbe = fn
	t.Cleanup(func() { fpDomainProbe = prev })
}

// swapFPDirLinked overrides the "is a live bridge symlink" seam for one test.
func swapFPDirLinked(t *testing.T, fn func(string) bool) {
	t.Helper()
	prev := fpDirLinked
	fpDirLinked = fn
	t.Cleanup(func() { fpDirLinked = prev })
}

// swapFPAppexBounce overrides the extension-bounce seam for one test.
func swapFPAppexBounce(t *testing.T, fn func(context.Context) error) {
	t.Helper()
	prev := fpAppexBounce
	fpAppexBounce = fn
	t.Cleanup(func() { fpAppexBounce = prev })
}

// newFPHealServer builds an FP server with fp state wired and a synth that always
// reports non-empty, returning acct-1 (the fileprovider row) and its fake provider.
func newFPHealServer(t *testing.T) (*Server, store.Account, map[int]string, *fakeFPProv) {
	t.Helper()
	s, dirs, fake := newFPServer(t)
	s.fp = newFPState(alwaysNonEmpty)
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	return s, a, dirs, fake
}

// healFPStep drives one recovery-ladder step under the poll claim, as the heal
// ticker does. now controls the attempt clock.
func healFPStep(t *testing.T, s *Server, a store.Account, now time.Time) {
	t.Helper()
	if !s.beginPoll(a.ID) {
		t.Fatalf("acct-%02d poll claim refused", a.ID)
	}
	s.healFP(t.Context(), a, now)
	s.endPoll(a.ID)
}

// TestFPHealLadderEscalation pins the escalation order: attempt 1 is a
// non-destructive Sync, attempts 2–4 re-register the domain (Teardown+Setup).
func TestFPHealLadderEscalation(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	wedgeIt(t, s.fp, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now)
	if _, setups, syncs, teardowns := fake.counts(); syncs != 1 || setups != 0 || teardowns != 0 {
		t.Fatalf("attempt 1: syncs=%d setups=%d teardowns=%d, want 1/0/0 (Sync only)", syncs, setups, teardowns)
	}
	if s.fp.attemptsSoFar(dirs[1]) != 1 {
		t.Fatalf("attempt 1 not booked: attemptsSoFar=%d", s.fp.attemptsSoFar(dirs[1]))
	}

	for attempt := 2; attempt <= 4; attempt++ {
		healFPStep(t, s, a, now)
		_, setups, syncs, teardowns := fake.counts()
		wantRR := attempt - 1 // re-registers so far
		if syncs != 1 || setups != wantRR || teardowns != wantRR {
			t.Fatalf("attempt %d: syncs=%d setups=%d teardowns=%d, want 1/%d/%d (Sync + %d re-registers)", attempt, syncs, setups, teardowns, wantRR, wantRR, wantRR)
		}
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("row changed to %q before the breaker", kind)
	}
	if !s.fp.wedged(dirs[1]) {
		t.Fatal("domain must stay wedged across attempts 1-4 (only a probe success clears it)")
	}
}

// TestFPHealBreakerRetreatsWhenIdle pins the breaker (attempt 5): it bounces the
// extension, re-registers once more, then — with no live session — retreats the
// row to symlink and forgets its wedge state.
func TestFPHealBreakerRetreatsWhenIdle(t *testing.T) {
	s, a, dirs, _ := newFPHealServer(t)
	wedgeIt(t, s.fp, dirs[1])
	now := time.Unix(0, 0)
	for i := 0; i < fpRecoveryBreaker-1; i++ { // advance to attempts==4 (next is the breaker)
		s.fp.recordAttempt(dirs[1], now)
	}
	var bounced int
	swapFPAppexBounce(t, func(context.Context) error { bounced++; return nil })

	healFPStep(t, s, a, now)

	if bounced != 1 {
		t.Fatalf("extension bounce fired %d times, want exactly 1 at the breaker", bounced)
	}
	if kind := kindOf(t, s, 1); kind != "symlink" {
		t.Fatalf("breaker did not retreat to symlink: kind = %q", kind)
	}
	if s.fp.wedged(dirs[1]) {
		t.Fatal("a successful retreat must forget the wedge state (fp.reset)")
	}
}

// TestFPHealBreakerParksUnderLiveSessions pins that the breaker's symlink retreat
// DEFERS under a live session (existing retreatFPToSymlink semantics): the row
// stays on fileprovider, stays wedged, and the manual-recovery guidance is logged.
func TestFPHealBreakerParksUnderLiveSessions(t *testing.T) {
	s, a, dirs, _ := newFPHealServer(t)
	buf := &syncBuffer{}
	s.log = log.New(buf, "", 0)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	wedgeIt(t, s.fp, dirs[1])
	now := time.Unix(0, 0)
	for i := 0; i < fpRecoveryBreaker-1; i++ {
		s.fp.recordAttempt(dirs[1], now)
	}
	swapFPAppexBounce(t, func(context.Context) error { return nil })

	healFPStep(t, s, a, now)

	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("breaker retreated under a live session: kind = %q, want fileprovider (deferred)", kind)
	}
	if !s.fp.wedged(dirs[1]) {
		t.Fatal("a parked (deferred-retreat) domain must stay wedged so select keeps excluding it")
	}
	if !strings.Contains(buf.String(), "launchctl kickstart") {
		t.Fatalf("breaker park must log the manual fileproviderd-restart guidance; got:\n%s", buf.String())
	}
}

// TestFPHealReservationDefersReRegister pins that a pending select reservation
// defers a re-register step WITHOUT consuming a recovery attempt (the launching
// session's dir must not be remade under it).
func TestFPHealReservationDefersReRegister(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	wedgeIt(t, s.fp, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now) // attempt 1: Sync; attemptsSoFar -> 1
	if !s.tryReserve(1) {
		t.Fatal("could not reserve acct-1")
	}
	healFPStep(t, s, a, now) // attempt 2 would re-register, but the reservation defers it

	if _, setups, _, teardowns := fake.counts(); setups != 0 || teardowns != 0 {
		t.Fatalf("re-register ran under a reservation: setups=%d teardowns=%d, want 0/0", setups, teardowns)
	}
	if got := s.fp.attemptsSoFar(dirs[1]); got != 1 {
		t.Fatalf("a deferred step consumed an attempt: attemptsSoFar=%d, want 1", got)
	}
}

// TestFPHealReRegisterProceedsUnderLiveSessions pins the approved policy: a
// re-register PROCEEDS under live sessions (a wedged domain already fails their
// reads), unlike the symlink retreat which defers.
func TestFPHealReRegisterProceedsUnderLiveSessions(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	wedgeIt(t, s.fp, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now) // attempt 1: Sync
	healFPStep(t, s, a, now) // attempt 2: re-register, despite the live session

	if _, setups, _, teardowns := fake.counts(); setups != 1 || teardowns != 1 {
		t.Fatalf("re-register did not proceed under a live session: setups=%d teardowns=%d, want 1/1", setups, teardowns)
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("re-register must not change the row: kind = %q", kind)
	}
}

// TestFPHealReRegisterRetreatsOnCannotControl pins that a re-register whose Setup
// reports ErrCannotControl retreats to symlink inline (FP cannot serve here).
func TestFPHealReRegisterRetreatsOnCannotControl(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.setupErr = fmt.Errorf("file provider setup: %w", fileproviderd.ErrCannotControl)
	wedgeIt(t, s.fp, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now) // attempt 1: Sync
	healFPStep(t, s, a, now) // attempt 2: re-register -> ErrCannotControl -> retreat

	if kind := kindOf(t, s, 1); kind != "symlink" {
		t.Fatalf("ErrCannotControl re-register did not retreat to symlink: kind = %q", kind)
	}
	if s.fp.wedged(dirs[1]) {
		t.Fatal("retreat must forget the wedge state")
	}
}

// TestHealFPRowsDetectsWedgeThenRecovers pins the ticker-driven flow: two wedged
// probes cross the strike threshold (attempt 1 fires), and a later healthy probe
// clears the verdict and the ladder (idempotent stop-on-recovery).
func TestHealFPRowsDetectsWedgeThenRecovers(t *testing.T) {
	s, _, dirs, fake := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })

	var probed int
	swapFPDomainProbe(t, func(string) error { probed++; return overlay.ErrFPProbeWedged })

	s.healFPRows(t.Context()) // strike 1: not wedged yet, no heal
	if s.fp.wedged(dirs[1]) {
		t.Fatal("one strike must not wedge (2-strike debounce)")
	}
	s.healFPRows(t.Context()) // strike 2: wedged + due -> attempt 1 (Sync)

	if !s.fp.wedged(dirs[1]) {
		t.Fatal("two consecutive wedged probes must mark the domain wedged")
	}
	if _, _, syncs, _ := fake.counts(); syncs != 1 {
		t.Fatalf("attempt 1 (Sync) did not run on the wedge tick: syncs=%d, want 1", syncs)
	}
	if probed != 2 {
		t.Fatalf("probed %d times over two ticks, want 2", probed)
	}

	// The domain recovers: a healthy probe clears the verdict and the ladder.
	swapFPDomainProbe(t, func(string) error { return nil })
	s.healFPRows(t.Context())
	if s.fp.wedged(dirs[1]) {
		t.Fatal("a healthy probe must clear the wedge verdict")
	}
	if s.fp.attemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("recovery must reset the ladder: attemptsSoFar=%d, want 0", s.fp.attemptsSoFar(dirs[1]))
	}
}

// TestHealFPRowsBridgeDownSkipsProbing pins that no domain is probed while the FP
// bridge is down (a probe through a down bridge reads every domain as wedged).
func TestHealFPRowsBridgeDownSkipsProbing(t *testing.T) {
	s, _, _, _ := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return false }
	swapFPDirLinked(t, func(string) bool { return true })
	var probed int
	swapFPDomainProbe(t, func(string) error { probed++; return nil })

	s.healFPRows(t.Context())

	if probed != 0 {
		t.Fatalf("probed %d domains with the bridge down, want 0", probed)
	}
}

// TestHealFPRowsSkipsNonSymlinkDir pins that a File Provider row whose dir is not
// its live bridge symlink (a mid-conversion real dir) is not probed — the real
// fpDirLinked gate over newFPServer's real account dir.
func TestHealFPRowsSkipsNonSymlinkDir(t *testing.T) {
	s, _, _, _ := newFPHealServer(t) // acct-1's dir is a real dir, never a symlink
	s.fpBridgeReadyFn = func() bool { return true }
	var probed int
	swapFPDomainProbe(t, func(string) error { probed++; return nil })

	s.healFPRows(t.Context())

	if probed != 0 {
		t.Fatalf("probed a non-symlink (mid-conversion) FP dir %d times, want 0", probed)
	}
}

// TestSelectExcludesWedgedFPDomain pins the select-path guard: a wedged FP domain
// is never handed to a launching session — the free pick skips to a healthy
// account, and an explicit request for the wedged account is refused.
func TestSelectExcludesWedgedFPDomain(t *testing.T) {
	s, _, dirs, _ := newFPHealServer(t) // acct-1 FP (emptier), acct-2 symlink
	s.fp.forceWedge(dirs[1])

	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, NoMark: true, Cwd: "/proj"})
	if !resp.OK {
		t.Fatalf("select failed with a healthy fallback available: %+v", resp)
	}
	if resp.Dir != dirs[2] {
		t.Fatalf("select handed the wedged FP acct-1 (%s); want the healthy symlink acct-2 (%s)", resp.Dir, dirs[2])
	}

	one := 1
	resp = s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one, NoMark: true, Cwd: "/proj"})
	if resp.OK {
		t.Fatal("an explicit select of a wedged FP domain must be refused")
	}
	if !strings.Contains(resp.Error, "wedged") {
		t.Fatalf("refusal must name the wedge: %q", resp.Error)
	}
}

// TestProbeFPWinnerForceWedges pins the select-time live probe: a hard data-plane
// failure force-marks the domain wedged with no debounce, while a healthy probe
// reads ready and leaves the domain unmarked.
func TestProbeFPWinnerForceWedges(t *testing.T) {
	s, a, dirs, _ := newFPHealServer(t)

	swapFPDomainProbe(t, func(string) error { return overlay.ErrFPProbeWedged })
	if s.probeWinnerReady(a) {
		t.Fatal("probeWinnerReady must refuse a domain whose live probe hangs")
	}
	if !s.fp.wedged(dirs[1]) {
		t.Fatal("a single hard select-time probe failure must force-mark the domain wedged")
	}

	s.fp.reset(dirs[1])
	swapFPDomainProbe(t, func(string) error { return nil })
	if !s.probeWinnerReady(a) {
		t.Fatal("probeWinnerReady must accept a domain whose live probe succeeds")
	}
	if s.fp.wedged(dirs[1]) {
		t.Fatal("a healthy probe must not wedge the domain")
	}
}

// TestReconcileFileProviderBackoffGate pins defect-3's reconcile gate: reconcile
// defers to the heal ladder while it holds the domain wedged and is backing off
// (no free Health+Setup), but proceeds when the domain is due (or not wedged).
func TestReconcileFileProviderBackoffGate(t *testing.T) {
	t.Run("backing off defers reconcile", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		wedgeIt(t, s.fp, dirs[1])
		s.fp.recordAttempt(dirs[1], time.Now()) // schedules nextDue ~30s out: not due

		if got := s.reconcileFileProvider(t.Context(), a); got != fpDeferred {
			t.Fatalf("reconcile outcome = %d, want fpDeferred (heal ladder owns the wedged domain)", got)
		}
		if h, se, _, _ := fake.counts(); h != 0 || se != 0 {
			t.Fatalf("reconcile piled control ops on a backing-off domain: healths=%d setups=%d, want 0/0", h, se)
		}
	})

	t.Run("a due domain still reconciles", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		wedgeIt(t, s.fp, dirs[1]) // wedged but never attempted -> immediately due

		if got := s.reconcileFileProvider(t.Context(), a); got != fpHealthy {
			t.Fatalf("reconcile outcome = %d, want fpHealthy (a due domain is not gated)", got)
		}
		if h, _, _, _ := fake.counts(); h != 1 {
			t.Fatalf("a due domain must reconcile: healths=%d, want 1", h)
		}
	})
}

// TestHealFPMissingRepairsDeregisteredDomain pins the control-plane gap fix: a
// domain deregistered out from under the daemon probes ENOENT (Missing, which
// never strikes the wedge ladder), but its Health fails — so the Missing heal
// re-registers it (Setup) and resets any prior control-plane backoff.
func TestHealFPMissingRepairsDeregisteredDomain(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.healthErr = errors.New("no domain registered") // control plane broken
	s.fp.recordAttempt(dirs[1], time.Unix(0, 0))        // a prior attempt the successful repair must clear
	now := time.Unix(0, 0).Add(time.Hour)               // well past the seeded backoff -> due

	s.healFPMissing(t.Context(), a, now)

	if _, setups, _, _ := fake.counts(); setups != 1 {
		t.Fatalf("deregistered domain not re-registered: setups=%d, want 1", setups)
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("repaired domain must stay on file provider: kind=%q", kind)
	}
	if s.fp.attemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("a successful repair must reset the ladder: attemptsSoFar=%d, want 0", s.fp.attemptsSoFar(dirs[1]))
	}
}

// TestHealFPMissingCannotControlRetreatsToSymlink pins that when the masked
// control-plane failure is terminal (Setup reports ErrCannotControl), the Missing
// heal retreats the row to symlink and forgets its ladder state — the only path
// that could reach this ErrCannotControl arm post-startup.
func TestHealFPMissingCannotControlRetreatsToSymlink(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.healthErr = errors.New("no domain registered")
	fake.setupErr = fmt.Errorf("file provider setup: %w", fileproviderd.ErrCannotControl)
	now := time.Unix(0, 0)

	s.healFPMissing(t.Context(), a, now)

	if kind := kindOf(t, s, 1); kind != "symlink" {
		t.Fatalf("ErrCannotControl did not retreat to symlink: kind=%q", kind)
	}
	if s.fp.wedged(dirs[1]) {
		t.Fatal("a retreat must forget the ladder state")
	}
}

// TestHealFPMissingHealthyDoesNothing pins the benign case: a fresh, identity-less
// account probes Missing but its control plane is healthy, so the Missing heal
// does nothing — no reconcile, no attempt consumed, the row untouched.
func TestHealFPMissingHealthyDoesNothing(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t) // fake.healthErr nil -> Health OK
	now := time.Unix(0, 0)

	s.healFPMissing(t.Context(), a, now)

	if _, setups, syncs, teardowns := fake.counts(); setups != 0 || syncs != 0 || teardowns != 0 {
		t.Fatalf("benign Missing must not reconcile: setups=%d syncs=%d teardowns=%d, want 0/0/0", setups, syncs, teardowns)
	}
	if s.fp.attemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("benign Missing must consume no attempt: attemptsSoFar=%d, want 0", s.fp.attemptsSoFar(dirs[1]))
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("benign Missing must not change the row: kind=%q", kind)
	}
}

// TestHealFPMissingBacksOff pins that a Missing-triggered control-plane repair
// rides the wedge ladder's backoff: a second attempt inside the backoff window is
// skipped (no reconcile, no extra attempt), and it resumes once the backoff has
// elapsed.
func TestHealFPMissingBacksOff(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.healthErr = errors.New("no domain registered")
	fake.setupErr = fmt.Errorf("file provider setup: %w", fileproviderd.ErrBusy) // transient -> fpRetry, no reset
	now := time.Unix(0, 0)

	s.healFPMissing(t.Context(), a, now) // attempt 1: reconcile runs, transient Setup failure books the backoff
	if _, setups, _, _ := fake.counts(); setups != 1 {
		t.Fatalf("first Missing-triggered reconcile must run: setups=%d, want 1", setups)
	}
	if s.fp.attemptsSoFar(dirs[1]) != 1 {
		t.Fatalf("first attempt not booked: attemptsSoFar=%d, want 1", s.fp.attemptsSoFar(dirs[1]))
	}

	s.healFPMissing(t.Context(), a, now) // inside the backoff window: must NOT reconcile again
	if _, setups, _, _ := fake.counts(); setups != 1 {
		t.Fatalf("second reconcile ran inside the backoff window: setups=%d, want 1", setups)
	}
	if s.fp.attemptsSoFar(dirs[1]) != 1 {
		t.Fatalf("a backoff-skipped tick consumed an attempt: attemptsSoFar=%d, want 1", s.fp.attemptsSoFar(dirs[1]))
	}

	s.healFPMissing(t.Context(), a, now.Add(fpRecoveryBackoff.Cap+time.Second)) // past the backoff -> resumes
	if _, setups, _, _ := fake.counts(); setups != 2 {
		t.Fatalf("reconcile did not resume after the backoff elapsed: setups=%d, want 2", setups)
	}
}

// TestHealFPRowsMissingRoutesToControlPlaneHeal pins the ticker wiring: a Missing
// probe routes to the control-plane heal (reconcile on a failing Health), never to
// the wedge ladder, and never marks the domain wedged.
func TestHealFPRowsMissingRoutesToControlPlaneHeal(t *testing.T) {
	s, _, dirs, fake := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })
	fake.healthErr = errors.New("no domain registered")
	swapFPDomainProbe(t, func(string) error { return overlay.ErrFPProbeMissing })

	s.healFPRows(t.Context())

	if _, setups, _, _ := fake.counts(); setups != 1 {
		t.Fatalf("a Missing probe with a failing Health must reconcile via the control-plane heal: setups=%d, want 1", setups)
	}
	if s.fp.wedged(dirs[1]) {
		t.Fatal("a Missing probe must never mark the domain wedged")
	}
}
