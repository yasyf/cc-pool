package overlay

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// drainForceUnmountProbes waits out the package-global force-unmount probe map
// so a test's parked syscall body exits before swapUnmountFn's cleanup restores
// the seam it reads (cleanups run LIFO). Register it FIRST (so it runs LAST)
// and have it close the release channel the parked body blocks on.
func drainForceUnmountProbes(t *testing.T, release chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		close(release)
		deadline := time.Now().Add(5 * time.Second)
		for forceUnmountProbes.Inflight() != 0 {
			if time.Now().After(deadline) {
				t.Error("parked force-unmount body never drained")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
}

// swapUnmountFn replaces the force-unmount syscall seam for one test.
func swapUnmountFn(t *testing.T, fn func(string, int) error) {
	t.Helper()
	prev := unmountFn
	unmountFn = fn
	t.Cleanup(func() { unmountFn = prev })
}

// swapForceUnmountTimeout shrinks the force-unmount bound for one test.
func swapForceUnmountTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := forceUnmountTimeout
	forceUnmountTimeout = d
	t.Cleanup(func() { forceUnmountTimeout = prev })
}

// TestForceUnmountCleanResult pins the happy path: a clean unmount returns nil
// and the syscall is issued for the right dir with MNT_FORCE.
func TestForceUnmountCleanResult(t *testing.T) {
	var (
		mu        sync.Mutex
		gotTarget string
		gotFlags  int
	)
	swapUnmountFn(t, func(target string, flags int) error {
		mu.Lock()
		gotTarget, gotFlags = target, flags
		mu.Unlock()
		return nil
	})

	if err := ForceUnmount("/pool/acct-01"); err != nil {
		t.Fatalf("ForceUnmount clean = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotTarget != "/pool/acct-01" {
		t.Fatalf("unmount target = %q, want /pool/acct-01", gotTarget)
	}
	if gotFlags != unix.MNT_FORCE {
		t.Fatalf("unmount flags = %#x, want MNT_FORCE %#x", gotFlags, unix.MNT_FORCE)
	}
}

// TestForceUnmountWrapsSyscallError pins that a syscall failure propagates
// wrapped (errors.Is-matchable), never swallowed.
func TestForceUnmountWrapsSyscallError(t *testing.T) {
	sentinel := errors.New("device busy")
	swapUnmountFn(t, func(string, int) error { return sentinel })

	err := ForceUnmount("/pool/acct-01")
	if !errors.Is(err, sentinel) {
		t.Fatalf("ForceUnmount err = %v, want it to wrap the syscall error", err)
	}
}

// TestForceUnmountBounded pins the load-bearing property: a wedged carcass
// whose unmount syscall never returns must NOT hang the caller — ForceUnmount
// returns ErrForceUnmountTimeout at the bound. The syscall goroutine is left
// parked until released (the StatProbes contract); the buffered result channel
// lets it exit cleanly when it finally answers.
func TestForceUnmountBounded(t *testing.T) {
	release := make(chan struct{})
	drainForceUnmountProbes(t, release)
	started := make(chan struct{})
	swapUnmountFn(t, func(string, int) error {
		close(started)
		<-release // a wedged carcass: the syscall parks forever
		return nil
	})
	swapForceUnmountTimeout(t, 50*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- ForceUnmount("/pool/acct-01") }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrForceUnmountTimeout) {
			t.Fatalf("wedged unmount err = %v, want ErrForceUnmountTimeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ForceUnmount hung on a wedged unmount instead of returning at the timeout")
	}
	<-started // the syscall goroutine did run
}

// TestForceUnmountJoinsRepeatedWedged pins the boundedness contract the doc
// promises and the daemon relies on: re-issuing ForceUnmount against the SAME
// permanently-wedged dir — exactly what forceUnmountOrphans does every
// supervision tick and escalateWedgedRow does every breaker window — shares the
// single already-parked syscall goroutine via the per-dir StatProbes join
// rather than spawning a fresh one per call. Without the join this leaks one
// parked goroutine per re-issue; with it, a carcass the kernel never MNT_FORCEs
// parks exactly one goroutine forever.
func TestForceUnmountJoinsRepeatedWedged(t *testing.T) {
	release := make(chan struct{})
	drainForceUnmountProbes(t, release)
	var calls atomic.Int32
	swapUnmountFn(t, func(string, int) error {
		calls.Add(1)
		<-release // a permanently-wedged carcass: the syscall never returns
		return nil
	})
	swapForceUnmountTimeout(t, 20*time.Millisecond)

	const (
		dir      = "/pool/acct-wedged"
		reissues = 5
	)
	for i := 0; i < reissues; i++ {
		if err := ForceUnmount(dir); !errors.Is(err, ErrForceUnmountTimeout) {
			t.Fatalf("re-issue %d: ForceUnmount = %v, want ErrForceUnmountTimeout", i, err)
		}
	}
	if got := forceUnmountProbes.Inflight(); got != 1 {
		t.Fatalf("parked probes after %d re-issues = %d, want exactly 1 (joined, not leaked)", reissues, got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("syscall invocations = %d, want exactly 1 — repeated force-unmounts must join the parked goroutine, not re-spawn", got)
	}
}
