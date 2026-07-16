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
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// setRowKind flips an account's stored overlay backend — the store half of a
// select/migrate conversion a test races against a heal that already read the row.
func setRowKind(t *testing.T, s *Server, id int, kind fkoverlay.Backend) {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = string(kind)
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
}

// swapFPDomainProbe overrides the package-level FP probe seam for one test. The
// seam takes a ctx (the control-op probe is a bounded socket round-trip); test
// closures ignore it.
func swapFPDomainProbe(t *testing.T, fn func(context.Context, string) error) {
	t.Helper()
	prev := fpDomainProbe
	fpDomainProbe = fn
	t.Cleanup(func() { fpDomainProbe = prev })
}

// fpForceRecoveryDue zeroes a wedged dir's backoff clock so the next healFPRows tick
// treats it as due for a recovery attempt (healFPRows reads wall-clock time.Now,
// which a test cannot advance).
func fpForceRecoveryDue(t *testing.T, s *Server, dir string) {
	t.Helper()
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	if l := s.led.peek(fpDomainPolicy, dir); l != nil {
		l.nextDue = time.Time{}
	}
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
	s.fpSynth = alwaysNonEmpty
	// Default the bridge self-test to serving so the non-retreat repair gate does
	// not block the repair-logic tests; the bridge-gate test overrides it.
	s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus { return FPBridgeStatus{Verdict: FPBridgeServing} }
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
	if !s.cl.hold(a.ID) {
		t.Fatalf("acct-%02d poll claim refused", a.ID)
	}
	s.healFP(t.Context(), a, now)
	s.cl.disownHold(a.ID)
}

// TestFPHealLadderEscalation pins the escalation order: attempt 1 is a
// non-destructive Reconcile, attempts 2–4 re-register the domain (Teardown+Reconcile).
func TestFPHealLadderEscalation(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	wedgeIt(t, s, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now)
	if _, registrations, reasserts, teardowns := fake.counts(); reasserts != 1 || registrations != 0 || teardowns != 0 {
		t.Fatalf("attempt 1: reasserts=%d registrations=%d teardowns=%d, want 1/0/0 (Reconcile only)", reasserts, registrations, teardowns)
	}
	if s.fpAttemptsSoFar(dirs[1]) != 1 {
		t.Fatalf("attempt 1 not booked: attemptsSoFar=%d", s.fpAttemptsSoFar(dirs[1]))
	}

	for attempt := 2; attempt <= 4; attempt++ {
		healFPStep(t, s, a, now)
		_, registrations, reasserts, teardowns := fake.counts()
		wantRR := attempt - 1 // re-registers so far
		if reasserts != 1 || registrations != wantRR || teardowns != wantRR {
			t.Fatalf("attempt %d: reasserts=%d registrations=%d teardowns=%d, want 1/%d/%d (Reconcile + %d re-registers)", attempt, reasserts, registrations, teardowns, wantRR, wantRR, wantRR)
		}
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("row changed to %q before the breaker", kind)
	}
	if !s.fpWedged(dirs[1]) {
		t.Fatal("domain must stay wedged across attempts 1-4 (only a probe success clears it)")
	}
}

// TestFPHealBreakerParksWhenIdle is the R1 regression pin: the breaker (attempt 5)
// bounces the extension and re-registers once more, then PARKS a
// wedged-but-controllable domain — it must NOT auto-retreat to symlink even when
// idle. A false wedge silently stranding an account on the symlink floor is the
// regression this rewrite closes; retreat is now operator-only (`ccp fp repair
// --retreat`). The row stays on fileprovider, stays wedged, and the park log names
// both repair levers and the fileproviderd kickstart.
func TestFPHealBreakerParksWhenIdle(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	buf := &syncBuffer{}
	s.log = log.New(buf, "", 0)
	// No live session: the OLD breaker would have retreated to symlink here.
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	wedgeIt(t, s, dirs[1])
	now := time.Unix(0, 0)
	for i := 0; i < fpRecoveryBreaker-1; i++ { // advance to attempts==4 (next is the breaker)
		s.fpRecordAttempt(dirs[1], now)
	}
	var bounced int
	swapFPAppexBounce(t, func(context.Context) error { bounced++; return nil })

	healFPStep(t, s, a, now)

	if bounced != 1 {
		t.Fatalf("extension bounce fired %d times, want exactly 1 at the breaker", bounced)
	}
	if _, registrations, _, teardowns := fake.counts(); registrations < 1 || teardowns < 1 {
		t.Fatalf("breaker must run one final re-register: registrations=%d teardowns=%d, want >=1/>=1", registrations, teardowns)
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("breaker auto-retreated a controllable domain (the R1 regression): kind = %q, want fileprovider (parked)", kind)
	}
	if !s.fpWedged(dirs[1]) {
		t.Fatal("a parked domain must stay wedged so select keeps excluding it")
	}
	log := buf.String()
	for _, frag := range []string{"ccp fp repair --account", "ccp fp repair --retreat --account", "launchctl kickstart"} {
		if !strings.Contains(log, frag) {
			t.Fatalf("breaker park log must name %q; got:\n%s", frag, log)
		}
	}
}

// TestFPHealBreakerParksUnderLiveSessions pins that the breaker parks (not
// retreats) with a live session bound to the dir too — same outcome as idle now
// that auto-retreat is gone, so a live session can never be the thing that
// distinguishes park from retreat.
func TestFPHealBreakerParksUnderLiveSessions(t *testing.T) {
	s, a, dirs, _ := newFPHealServer(t)
	buf := &syncBuffer{}
	s.log = log.New(buf, "", 0)
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	wedgeIt(t, s, dirs[1])
	now := time.Unix(0, 0)
	for i := 0; i < fpRecoveryBreaker-1; i++ {
		s.fpRecordAttempt(dirs[1], now)
	}
	swapFPAppexBounce(t, func(context.Context) error { return nil })

	healFPStep(t, s, a, now)

	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("breaker changed the row under a live session: kind = %q, want fileprovider (parked)", kind)
	}
	if !s.fpWedged(dirs[1]) {
		t.Fatal("a parked domain must stay wedged so select keeps excluding it")
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
	wedgeIt(t, s, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now) // attempt 1: Reconcile; attemptsSoFar -> 1
	if !s.cl.reserve(1) {
		t.Fatal("could not reserve acct-1")
	}
	healFPStep(t, s, a, now) // attempt 2 would re-register, but the reservation defers it

	if _, registrations, _, teardowns := fake.counts(); registrations != 0 || teardowns != 0 {
		t.Fatalf("re-register ran under a reservation: registrations=%d teardowns=%d, want 0/0", registrations, teardowns)
	}
	if got := s.fpAttemptsSoFar(dirs[1]); got != 1 {
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
	wedgeIt(t, s, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now) // attempt 1: Reconcile
	healFPStep(t, s, a, now) // attempt 2: re-register, despite the live session

	if _, registrations, _, teardowns := fake.counts(); registrations != 1 || teardowns != 1 {
		t.Fatalf("re-register did not proceed under a live session: registrations=%d teardowns=%d, want 1/1", registrations, teardowns)
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("re-register must not change the row: kind = %q", kind)
	}
}

// TestFPHealReRegisterRetreatsOnCannotControl pins that a re-register whose Reconcile
// reports ErrCannotControl retreats to symlink inline (FP cannot serve here).
func TestFPHealReRegisterRetreatsOnCannotControl(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.registerErr = fmt.Errorf("file provider reconcile: %w", fileproviderd.ErrCannotControl)
	wedgeIt(t, s, dirs[1])
	now := time.Unix(0, 0)

	healFPStep(t, s, a, now) // attempt 1: Reconcile
	healFPStep(t, s, a, now) // attempt 2: re-register -> ErrCannotControl -> retreat

	if kind := kindOf(t, s, 1); kind != "symlink" {
		t.Fatalf("ErrCannotControl re-register did not retreat to symlink: kind = %q", kind)
	}
	if s.fpWedged(dirs[1]) {
		t.Fatal("retreat must forget the wedge state")
	}
}

// TestHealFPRowsRecoversSelectionDetectedWedge pins the event-driven split:
// selection marks a wedge; maintenance deep-probes only that recovery row.
func TestHealFPRowsRecoversSelectionDetectedWedge(t *testing.T) {
	s, _, dirs, fake := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })

	wedgeIt(t, s, dirs[1])
	fpForceRecoveryDue(t, s, dirs[1])
	var deep int
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return overlay.ErrFPProbeWedged })
	s.healFPRows(t.Context())
	if _, _, reasserts, _ := fake.counts(); reasserts != 1 {
		t.Fatalf("attempt 1 (Reconcile) did not run on the wedge tick: reasserts=%d, want 1", reasserts)
	}
	if deep != 1 {
		t.Fatalf("wedged row deep-probed %d times, want 1", deep)
	}

	// The domain recovers. A wedged row is deep-probed only on the due window, so
	// force it due, then a healthy DEEP probe clears the verdict and the ladder.
	fpForceRecoveryDue(t, s, dirs[1])
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { deep++; return nil })
	s.healFPRows(t.Context())
	if s.fpWedged(dirs[1]) {
		t.Fatal("a healthy deep probe on the due window must clear the wedge verdict")
	}
	if s.fpAttemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("recovery must reset the ladder: attemptsSoFar=%d, want 0", s.fpAttemptsSoFar(dirs[1]))
	}
}

// TestHealFPRowsBridgeDownSkipsProbing pins that no domain is probed while the FP
// bridge is down (a probe through a down bridge reads every domain as wedged).
func TestHealFPRowsBridgeDownSkipsProbing(t *testing.T) {
	s, _, _, _ := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return false }
	swapFPDirLinked(t, func(string) bool { return true })
	var probed int
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { probed++; return nil })

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
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { probed++; return nil })

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
	s.fpForceWedge(dirs[1], overlay.ErrFPProbeWedged)

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

	swapFPDomainProbe(t, func(_ context.Context, _ string) error { return overlay.ErrFPProbeWedged })
	if s.probeWinnerReady(t.Context(), a) {
		t.Fatal("probeWinnerReady must refuse a domain whose live probe hangs")
	}
	if !s.fpWedged(dirs[1]) {
		t.Fatal("a single hard select-time probe failure must force-mark the domain wedged")
	}

	s.fpReset(dirs[1])
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { return nil })
	if !s.probeWinnerReady(t.Context(), a) {
		t.Fatal("probeWinnerReady must accept a domain whose live probe succeeds")
	}
	if s.fpWedged(dirs[1]) {
		t.Fatal("a healthy probe must not wedge the domain")
	}
}

// TestProbeFPWinnerNoVerdictStaysReady pins that a NoVerdict select-time probe (the
// companion app is busy, unreachable, restarting, or too old to answer the control
// op) reads READY without force-wedging: an app restart must never fleet-wedge every
// select, the exact failure the through-domain read caused.
func TestProbeFPWinnerNoVerdictStaysReady(t *testing.T) {
	s, a, dirs, _ := newFPHealServer(t)

	swapFPDomainProbe(t, func(_ context.Context, _ string) error { return overlay.ErrFPProbeNoVerdict })
	if !s.probeWinnerReady(t.Context(), a) {
		t.Fatal("a NoVerdict select-time probe must read ready (never fleet-wedge a select)")
	}
	if s.fpWedged(dirs[1]) {
		t.Fatal("a NoVerdict probe must NOT force-wedge the domain")
	}
}

// TestHealFPRowsNoVerdictSkipsTick pins that a NoVerdict heal probe is neither a
// strike nor a clear and never escalates the ladder: a previously-wedged domain
// stays wedged with no new recovery attempt, and no Reconcile/re-register fires.
func TestHealFPRowsNoVerdictSkipsTick(t *testing.T) {
	s, _, dirs, fake := newFPHealServer(t)
	s.fpBridgeReadyFn = func() bool { return true }
	swapFPDirLinked(t, func(string) bool { return true })
	wedgeIt(t, s, dirs[1]) // already wedged and due
	swapFPDomainProbe(t, func(_ context.Context, _ string) error { return overlay.ErrFPProbeNoVerdict })

	s.healFPRows(t.Context())

	if !s.fpWedged(dirs[1]) {
		t.Fatal("a NoVerdict tick must not clear an established wedge")
	}
	if s.fpAttemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("a NoVerdict tick must not book a recovery attempt: attemptsSoFar=%d, want 0", s.fpAttemptsSoFar(dirs[1]))
	}
	if _, registrations, reasserts, teardowns := fake.counts(); registrations != 0 || reasserts != 0 || teardowns != 0 {
		t.Fatalf("a NoVerdict tick must not escalate the ladder: registrations=%d reasserts=%d teardowns=%d, want 0/0/0", registrations, reasserts, teardowns)
	}
}

// TestReconcileFileProviderBackoffGate pins defect-3's reconcile gate: reconcile
// defers to the heal ladder while it holds the domain wedged and is backing off
// (no free Check+Reconcile), but proceeds when the domain is due (or not wedged).
func TestReconcileFileProviderBackoffGate(t *testing.T) {
	t.Run("backing off defers reconcile", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		wedgeIt(t, s, dirs[1])
		s.fpRecordAttempt(dirs[1], time.Now()) // schedules nextDue ~30s out: not due

		if got := s.reconcileFileProvider(t.Context(), a); got != fpDeferred {
			t.Fatalf("reconcile outcome = %d, want fpDeferred (heal ladder owns the wedged domain)", got)
		}
		if h, se, _, _ := fake.counts(); h != 0 || se != 0 {
			t.Fatalf("reconcile piled control ops on a backing-off domain: checks=%d registrations=%d, want 0/0", h, se)
		}
	})

	t.Run("a due domain still reconciles", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		wedgeIt(t, s, dirs[1]) // wedged but never attempted -> immediately due

		if got := s.reconcileFileProvider(t.Context(), a); got != fpHealthy {
			t.Fatalf("reconcile outcome = %d, want fpHealthy (a due domain is not gated)", got)
		}
		if h, _, _, _ := fake.counts(); h != 1 {
			t.Fatalf("a due domain must reconcile: checks=%d, want 1", h)
		}
	})
}

// TestHealFPMissingRepairsDeregisteredDomain pins the control-plane gap fix: a
// domain deregistered out from under the daemon probes ENOENT (Missing, which
// never strikes the wedge ladder), but its Check fails — so the Missing heal
// re-registers it (Reconcile) and resets any prior control-plane backoff.
func TestHealFPMissingRepairsDeregisteredDomain(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.checkErr = errors.New("no domain registered") // control plane broken
	s.fpRecordAttempt(dirs[1], time.Unix(0, 0))        // a prior attempt the successful repair must clear
	now := time.Unix(0, 0).Add(time.Hour)              // well past the seeded backoff -> due

	s.healFPMissing(t.Context(), a, now)

	if _, registrations, _, _ := fake.counts(); registrations != 1 {
		t.Fatalf("deregistered domain not re-registered: registrations=%d, want 1", registrations)
	}
	if kind := kindOf(t, s, 1); kind != "fileprovider" {
		t.Fatalf("repaired domain must stay on file provider: kind=%q", kind)
	}
	if s.fpAttemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("a successful repair must reset the ladder: attemptsSoFar=%d, want 0", s.fpAttemptsSoFar(dirs[1]))
	}
}

// TestHealFPMissingCannotControlRetreatsToSymlink pins that when the masked
// control-plane failure is terminal (Reconcile reports ErrCannotControl), the Missing
// heal retreats the row to symlink and forgets its ladder state — the only path
// that could reach this ErrCannotControl arm post-startup.
func TestHealFPMissingCannotControlRetreatsToSymlink(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t)
	fake.checkErr = errors.New("no domain registered")
	fake.registerErr = fmt.Errorf("file provider reconcile: %w", fileproviderd.ErrCannotControl)
	now := time.Unix(0, 0)

	s.healFPMissing(t.Context(), a, now)

	if kind := kindOf(t, s, 1); kind != "symlink" {
		t.Fatalf("ErrCannotControl did not retreat to symlink: kind=%q", kind)
	}
	if s.fpWedged(dirs[1]) {
		t.Fatal("a retreat must forget the ladder state")
	}
}

// TestHealFPMissingHealthyDoesNothing pins the benign case: a fresh, identity-less
// account probes Missing but its control plane is healthy, so the Missing heal
// does nothing — no reconcile, no attempt consumed, the row untouched.
func TestHealFPMissingHealthyDoesNothing(t *testing.T) {
	s, a, dirs, fake := newFPHealServer(t) // fake.checkErr nil -> Check OK
	now := time.Unix(0, 0)

	s.healFPMissing(t.Context(), a, now)

	if _, registrations, reasserts, teardowns := fake.counts(); registrations != 0 || reasserts != 0 || teardowns != 0 {
		t.Fatalf("benign Missing must not reconcile: registrations=%d reasserts=%d teardowns=%d, want 0/0/0", registrations, reasserts, teardowns)
	}
	if s.fpAttemptsSoFar(dirs[1]) != 0 {
		t.Fatalf("benign Missing must consume no attempt: attemptsSoFar=%d, want 0", s.fpAttemptsSoFar(dirs[1]))
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
	fake.checkErr = errors.New("no domain registered")
	fake.registerErr = fmt.Errorf("file provider reconcile: %w", fileproviderd.ErrBusy) // transient -> fpRetry, no reset
	now := time.Unix(0, 0)

	s.healFPMissing(t.Context(), a, now) // attempt 1: reconcile runs, transient Reconcile failure books the backoff
	if _, registrations, _, _ := fake.counts(); registrations != 1 {
		t.Fatalf("first Missing-triggered reconcile must run: registrations=%d, want 1", registrations)
	}
	if s.fpAttemptsSoFar(dirs[1]) != 1 {
		t.Fatalf("first attempt not booked: attemptsSoFar=%d, want 1", s.fpAttemptsSoFar(dirs[1]))
	}

	s.healFPMissing(t.Context(), a, now) // inside the backoff window: must NOT reconcile again
	if _, registrations, _, _ := fake.counts(); registrations != 1 {
		t.Fatalf("second reconcile ran inside the backoff window: registrations=%d, want 1", registrations)
	}
	if s.fpAttemptsSoFar(dirs[1]) != 1 {
		t.Fatalf("a backoff-skipped tick consumed an attempt: attemptsSoFar=%d, want 1", s.fpAttemptsSoFar(dirs[1]))
	}

	s.healFPMissing(t.Context(), a, now.Add(fpRecoveryBackoff.Cap+time.Second)) // past the backoff -> resumes
	if _, registrations, _, _ := fake.counts(); registrations != 2 {
		t.Fatalf("reconcile did not resume after the backoff elapsed: registrations=%d, want 2", registrations)
	}
}

// TestHealFPMissingRoutesToControlPlaneHeal pins the selection-detected Missing
// path: it repairs the control plane without marking the domain wedged.
func TestHealFPMissingRoutesToControlPlaneHeal(t *testing.T) {
	s, account, dirs, fake := newFPHealServer(t)
	fake.checkErr = errors.New("no domain registered")
	s.healFPMissing(t.Context(), account, time.Now())

	if _, registrations, _, _ := fake.counts(); registrations != 1 {
		t.Fatalf("a Missing probe with a failing Check must reconcile via the control-plane heal: registrations=%d, want 1", registrations)
	}
	if s.fpWedged(dirs[1]) {
		t.Fatal("a Missing probe must never mark the domain wedged")
	}
}

// TestHealFPRaceSkipsRowConvertedOffFP is the finding-1 regression: a select/migrate
// converts the row off File Provider between the UNCLAIMED probe that produced the
// heal's stale snapshot and the poll claim. The re-read under the claim must catch the
// flip and skip — never reconcile the domain (healFPMissing) or re-assert FP state via
// Reconcile (healFP attempt 1) for a row that is no longer File Provider — and clear the
// now-stale ladder.
func TestHealFPRaceSkipsRowConvertedOffFP(t *testing.T) {
	t.Run("healFPMissing skips a row converted off FP under the claim", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		fake.checkErr = errors.New("no domain registered") // a stale FP row would reconcile here
		s.fpRecordAttempt(dirs[1], time.Unix(0, 0))        // a prior attempt the skip must clear
		now := time.Unix(0, 0).Add(time.Hour)              // past the backoff -> due

		// The race: the row is symlink in SQLite; `a` is the pre-probe FP snapshot.
		setRowKind(t, s, 1, fkoverlay.BackendSymlink)

		s.healFPMissing(t.Context(), a, now)

		if h, se, _, _ := fake.counts(); h != 0 || se != 0 {
			t.Fatalf("healFPMissing acted through a converted row: checks=%d registrations=%d, want 0/0", h, se)
		}
		if kind := kindOf(t, s, 1); kind != "symlink" {
			t.Fatalf("healFPMissing disturbed a converted row: kind=%q, want symlink", kind)
		}
		if got := s.fpAttemptsSoFar(dirs[1]); got != 0 {
			t.Fatalf("healFPMissing left stale FP ladder state on a converted row: attemptsSoFar=%d, want 0", got)
		}
	})

	t.Run("healFP attempt 1 skips a row converted off FP under the claim", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		wedgeIt(t, s, dirs[1]) // a genuine wedge, so a stale FP row would run attempt 1 (Reconcile)
		now := time.Unix(0, 0)

		setRowKind(t, s, 1, fkoverlay.BackendSymlink) // converted in the claim gap

		healFPStep(t, s, a, now) // holds the poll claim, as the heal ticker does

		if _, _, reasserts, _ := fake.counts(); reasserts != 0 {
			t.Fatalf("healFP attempt 1 re-asserted FP state (Reconcile) through a converted row: reasserts=%d, want 0", reasserts)
		}
		if kind := kindOf(t, s, 1); kind != "symlink" {
			t.Fatalf("healFP disturbed a converted row: kind=%q, want symlink", kind)
		}
		if s.fpWedged(dirs[1]) {
			t.Fatal("healFP must clear the stale wedge ladder for a converted row")
		}
	})
}
