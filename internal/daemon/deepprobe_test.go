package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/mountd"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/procscan"
)

// swapDeepProbe overrides the daemon's deep-probe seam for one test, restoring
// it after. Tests using it must not run in parallel.
func swapDeepProbe(t *testing.T, fn func(dir string) error) {
	t.Helper()
	prev := deepProbe
	deepProbe = fn
	t.Cleanup(func() { deepProbe = prev })
}

// shrinkProbeInterval sets the periodic-probe throttle for one test so
// consecutive supervise ticks each re-probe (production is 30s; the supervisor
// ticks faster, so an unshrunk interval would let only one probe land per
// test). Restored after; no-parallel.
func shrinkProbeInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := deepProbeInterval
	deepProbeInterval = d
	t.Cleanup(func() { deepProbeInterval = prev })
}

func wedgeErr() error {
	return fmt.Errorf("%w: read parked past the probe timeout", overlay.ErrProbeWedged)
}

// TestRecordDeepStrikesThenWedged pins the debounce: a single failed periodic
// probe is a strike but not a wedge; two consecutive strikes flip the verdict
// (and log the transition once).
func TestRecordDeepStrikesThenWedged(t *testing.T) {
	var h holderState
	const dir = "/pool/acct-01"

	if msg := h.recordDeep(dir, wedgeErr()); msg != "" {
		t.Fatalf("first strike logged %q, want silent (a wedge needs two strikes)", msg)
	}
	if h.deepWedged(dir) {
		t.Fatal("deepWedged after one strike, want still healthy")
	}
	if msg := h.recordDeep(dir, wedgeErr()); msg == "" {
		t.Fatal("the second consecutive strike did not log the wedge transition")
	}
	if !h.deepWedged(dir) {
		t.Fatal("deepWedged false after two consecutive strikes, want wedged")
	}
}

// TestRecordDeepRecovery pins that one successful probe clears a wedge AND
// resets the strike count, so a single fresh failure does not immediately
// re-wedge on the back of pre-recovery strikes.
func TestRecordDeepRecovery(t *testing.T) {
	var h holderState
	const dir = "/pool/acct-01"
	h.recordDeep(dir, wedgeErr())
	h.recordDeep(dir, wedgeErr())
	if !h.deepWedged(dir) {
		t.Fatal("precondition: two strikes must wedge")
	}

	if msg := h.recordDeep(dir, nil); msg == "" {
		t.Error("recovery did not log the transition back to live")
	}
	if h.deepWedged(dir) {
		t.Fatal("deepWedged true after a successful probe, want the wedge cleared")
	}
	h.recordDeep(dir, wedgeErr())
	if h.deepWedged(dir) {
		t.Error("one strike after recovery re-wedged; a success must reset the strike count")
	}
}

// TestRecordDeepMissingIsNoVerdict pins ErrProbeMissing semantics: never a
// strike in either direction (it neither advances nor resets the count), so a
// pre-upgrade holder's mirror keeps working until it is naturally remounted.
func TestRecordDeepMissingIsNoVerdict(t *testing.T) {
	var h holderState
	const dir = "/pool/acct-01"
	missing := fmt.Errorf("%w: %s", overlay.ErrProbeMissing, dir)

	for i := 0; i < 3; i++ {
		if msg := h.recordDeep(dir, missing); msg != "" {
			t.Fatalf("ErrProbeMissing logged %q, want silent (no verdict)", msg)
		}
	}
	if h.deepWedged(dir) {
		t.Fatal("ErrProbeMissing wedged the verdict; it must never strike")
	}
	// No-verdict cuts both ways: a missing probe between two genuine failures
	// must not reset the count — fail, missing, fail wedges.
	h.recordDeep(dir, wedgeErr())
	h.recordDeep(dir, missing)
	h.recordDeep(dir, wedgeErr())
	if !h.deepWedged(dir) {
		t.Error("fail/missing/fail did not wedge; a missing probe must not reset strikes")
	}
}

// TestDeepWedgedFoldsIntoReadyAndHeldDead pins the readiness fold: a
// shallow-live mirror the daemon's probe marks wedged reads NOT ready (so
// selection excludes it) and held-dead with the wedge bit (so the supervisor
// remounts it). noteMounted (a fresh remount) clears the verdict.
func TestDeepWedgedFoldsIntoReadyAndHeldDead(t *testing.T) {
	var h holderState
	const dir = "/pool/acct-01"
	h.refresh(mountd.NewClient(startCannedHolder(t, []mountd.MountInfo{
		{Dir: dir, Base: "/b", Live: true},
	})))
	if !h.ready(dir) {
		t.Fatal("precondition: a shallow-live mount must read ready")
	}

	h.markDeepWedged(dir)
	if h.ready(dir) {
		t.Fatal("a deep-wedged mount still reads ready; the verdict must fold into ready")
	}
	if dead, wedged := h.heldDead(dir); !dead || !wedged {
		t.Fatalf("heldDead = (%v, %v), want (true, true) for a shallow-live deep-wedged mount", dead, wedged)
	}

	h.noteMounted(dir)
	if !h.ready(dir) {
		t.Fatal("a fresh remount did not clear the wedge verdict")
	}
}

// TestIdleMountsNeverPeriodicallyProbed is the load-bearing test for the
// high-traffic fix: a shallow-live mount backing NO live session is never
// deep-probed by the supervisor, no matter how many ticks elapse. If this
// regresses, the holder's old probe-every-mount waste returns to the daemon.
func TestIdleMountsNeverPeriodicallyProbed(t *testing.T) {
	s, dirs, _, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, []mountd.MountInfo{
		{Dir: dirs[1], Base: "/base", Live: true},
	})
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil } // idle: no sessions
	shrinkProbeInterval(t, 0)                                                              // due every tick — only the session gate can suppress probing
	var probes int32
	swapDeepProbe(t, func(string) error { atomic.AddInt32(&probes, 1); return nil })

	for i := 0; i < 5; i++ {
		s.superviseTick(t.Context())
	}
	if got := atomic.LoadInt32(&probes); got != 0 {
		t.Fatalf("deep probes against an idle mount = %d, want 0 (idle mounts are never periodically probed)", got)
	}
}

// TestInUseMountProbedAndHealedWithinInterval pins the periodic in-use path: a
// shallow-live mount backing a live session is deep-probed each due tick, two
// consecutive failures wedge it, and the same tick's heal remounts it (logging
// the wedge copy + relaunch guidance) and clears the verdict.
func TestInUseMountProbedAndHealedWithinInterval(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, []mountd.MountInfo{
		{Dir: dirs[1], Base: "/base", Live: true},
	})
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	shrinkProbeInterval(t, 0)
	swapDeepProbe(t, func(string) error { return wedgeErr() })
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	// Tick 1: one strike — still ready (debounce), no remount.
	s.superviseTick(t.Context())
	if fake.setupCount() != 0 {
		t.Fatalf("setups after one strike = %d, want 0 (a wedge needs two)", fake.setupCount())
	}
	// Tick 2: second strike wedges → not ready → remounted this pass.
	s.superviseTick(t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups after the wedge = %d, want the mirror remounted once", fake.setupCount())
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("remounted mirror not vouched for (noteMounted should clear the verdict)")
	}
	out := buf.String()
	if !strings.Contains(out, "wedged mirror (serves metadata but hangs reads)") {
		t.Fatalf("missing the wedge log copy:\n%s", out)
	}
	if !strings.Contains(out, "1 live session") || !strings.Contains(out, "relaunch") {
		t.Fatalf("missing the live-session count or relaunch guidance:\n%s", out)
	}
}

// TestProbeOnAssignRefusesIdleWedge pins the select-time probe — the ONLY
// probe of an idle mirror: a forced select of a shallow-live-but-wedged mount
// is refused (a single observed wedge is actionable, no debounce) and the
// verdict is marked so selection excludes it; the supervisor then remounts it.
func TestProbeOnAssignRefusesIdleWedge(t *testing.T) {
	s, dirs, fake, _ := newSuperviseServer(t)
	flipToFuse(t, s, 1)
	// The holder vouches for acct-1's mirror (shallow-live), so mountReady
	// passes — but the idle mirror is partially wedged, which only the
	// select-time deep probe catches.
	s.holder.noteMounted(dirs[1])
	swapDeepProbe(t, func(string) error { return wedgeErr() })

	one := 1
	resp := s.handleSelect(t.Context(), Request{Op: OpSelect, Account: &one, NoMark: true, Cwd: "/proj"})
	if resp.OK || !strings.Contains(resp.Error, "wedged") {
		t.Fatalf("forced select of an idle-wedged mirror = %+v, want a wedged refusal", resp)
	}
	if !s.holder.deepWedged(dirs[1]) {
		t.Fatal("the select-time probe did not mark the mirror wedged")
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("a wedged mirror still reads ready; selection must exclude it")
	}

	// The supervisor heals the wedge the select surfaced. The mount stays idle
	// (no session), so the periodic probe never runs — the markDeepWedged
	// verdict alone makes the row not-ready and drives the remount, which
	// clears the verdict.
	s.holderSocket = startCannedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}})
	s.superviseTick(t.Context())
	if fake.setupCount() == 0 {
		t.Fatal("supervisor did not remount the wedged mirror the select surfaced")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("mirror not vouched for after the supervisor remount")
	}
}
