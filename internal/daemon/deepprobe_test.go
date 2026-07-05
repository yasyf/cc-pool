package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/fusekit/mountd"
)

// Not parallel-safe.
func swapDeepProbe(t *testing.T, fn func(dir string) error) {
	t.Helper()
	prev := deepProbe
	deepProbe = fn
	t.Cleanup(func() { deepProbe = prev })
}

// Not parallel-safe.
func shrinkProbeInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := deepProbeInterval
	deepProbeInterval = d
	t.Cleanup(func() { deepProbeInterval = prev })
}

func wedgeErr() error {
	return fmt.Errorf("%w: read parked past the probe timeout", overlay.ErrProbeWedged)
}

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
	h.recordDeep(dir, wedgeErr())
	h.recordDeep(dir, missing)
	h.recordDeep(dir, wedgeErr())
	if !h.deepWedged(dir) {
		t.Error("fail/missing/fail did not wedge; a missing probe must not reset strikes")
	}
}

// TestRecordDeepStrikesOnPermissionDeniedProbe bridges the overlay probe seam to
// the daemon verdict for the 2026-07 incident: a permission-denied probe open (a
// dead-holder orphan answering EPERM) must STRIKE toward the wedged verdict, never
// fold into ErrProbeMissing's no-verdict class that let every broken mount read
// healthy.
func TestRecordDeepStrikesOnPermissionDeniedProbe(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits, so open(2) would not return EACCES")
	}
	dir := t.TempDir()
	// mode-0000 makes the probe open refuse with EACCES — the dead-holder orphan's shape.
	if err := os.WriteFile(filepath.Join(dir, overlay.ProbeFileName), []byte("x"), 0o000); err != nil {
		t.Fatal(err)
	}
	var h holderState
	for i := 0; i < deepWedgeStrikes; i++ {
		if msg := h.recordDeep(dir, overlay.DeepProbeWithin(dir)); i == deepWedgeStrikes-1 && msg == "" {
			t.Fatal("the wedge-threshold strike did not log the transition; a denied probe read as no-verdict")
		}
	}
	if !h.deepWedged(dir) {
		t.Fatalf("a permission-denied probe did not wedge after %d strikes; the EPERM orphan reads healthy", deepWedgeStrikes)
	}
}

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

func TestIdleMountsNeverPeriodicallyProbed(t *testing.T) {
	s, dirs, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, []mountd.MountInfo{
		{Dir: dirs[1], Base: "/base", Live: true},
	})
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	shrinkProbeInterval(t, 0) // due every tick — only the session gate can suppress probing
	var probes int32
	swapDeepProbe(t, func(string) error { atomic.AddInt32(&probes, 1); return nil })

	for i := 0; i < 5; i++ {
		healTick(t.Context(), s)
	}
	if got := atomic.LoadInt32(&probes); got != 0 {
		t.Fatalf("deep probes against an idle mount = %d, want 0 (idle mounts are never periodically probed)", got)
	}
}

func TestInUseMountProbedAndHealedWithinInterval(t *testing.T) {
	s, dirs, fake := newHealServer(t)
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

	healTick(t.Context(), s)
	if fake.setupCount() != 0 {
		t.Fatalf("setups after one strike = %d, want 0 (a wedge needs two)", fake.setupCount())
	}
	healTick(t.Context(), s)
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

func TestProbeOnAssignRefusesIdleWedge(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
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

	// Idle, so no periodic probe: the wedge verdict alone drives the remount.
	s.holderSocket = startCannedHolder(t, []mountd.MountInfo{{Dir: dirs[1], Base: "/base", Live: true}})
	healTick(t.Context(), s)
	if fake.setupCount() == 0 {
		t.Fatal("the heal loop did not remount the wedged mirror the select surfaced")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("mirror not vouched for after the heal-loop remount")
	}
}
