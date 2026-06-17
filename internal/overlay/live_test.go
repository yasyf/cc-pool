package overlay

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// swapMountAlive seams mountAliveFn for one test, restoring it on cleanup.
// Tests using it must not run in parallel.
func swapMountAlive(t *testing.T, fn func(base, accountDir string) bool) {
	t.Helper()
	prev := mountAliveFn
	mountAliveFn = fn
	t.Cleanup(func() { mountAliveFn = prev })
}

// swapMountPollInterval seams mountPollInterval for one test, restoring it on
// cleanup. Tests using it must not run in parallel.
func swapMountPollInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := mountPollInterval
	mountPollInterval = d
	t.Cleanup(func() { mountPollInterval = prev })
}

// swapStatProbeTimeout seams statProbeTimeout for one test, restoring it on
// cleanup. Tests using it must not run in parallel.
func swapStatProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := statProbeTimeout
	statProbeTimeout = d
	t.Cleanup(func() { statProbeTimeout = prev })
}

// TestMountAliveWithin pins the bounded liveness probe: a healthy stat's
// verdict passes through both polarities, and a parked stat (a wedged
// mirror's uninterruptible sleep) reads NOT alive within the bound — even
// when the stat would eventually answer alive — instead of hanging the
// caller forever.
func TestMountAliveWithin(t *testing.T) {
	cases := []struct {
		name string
		dir  string // unique per case: aliveProbes joins in-flight probes by dir
		park bool   // stat blocks until the test releases it
		stat bool   // the stat's eventual verdict
		want bool
	}{
		{name: "healthy live probe reads alive", dir: "/probe/acct-live", stat: true, want: true},
		{name: "healthy dead probe reads not alive", dir: "/probe/acct-dead", stat: false, want: false},
		{name: "parked probe reads not alive within the bound", dir: "/probe/acct-parked", park: true, stat: true, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapStatProbeTimeout(t, 20*time.Millisecond)
			release := make(chan struct{})
			swapMountAlive(t, func(base, accountDir string) bool {
				if base != "base" || accountDir != tc.dir {
					t.Errorf("probe got (%q, %q), want (\"base\", %q)", base, accountDir, tc.dir)
				}
				if tc.park {
					<-release
				}
				return tc.stat
			})
			// Drain the parked probe body before swapMountAlive's cleanup
			// restores the seam it reads (cleanups run LIFO).
			t.Cleanup(func() {
				close(release)
				deadline := time.Now().Add(5 * time.Second)
				for aliveProbes.Inflight() != 0 {
					if time.Now().After(deadline) {
						t.Error("parked probe body never drained")
						return
					}
					time.Sleep(time.Millisecond)
				}
			})

			start := time.Now()
			got := MountAliveWithin("base", tc.dir)
			elapsed := time.Since(start)
			if got != tc.want {
				t.Errorf("MountAliveWithin = %v, want %v", got, tc.want)
			}
			// Well under the 2s production bound: returning at all while the
			// stat is parked is the property; the margin keeps it unflaky.
			if tc.park && elapsed >= time.Second {
				t.Errorf("MountAliveWithin returned after %v with a parked stat; the %v bound did not hold", elapsed, statProbeTimeout)
			}
		})
	}
}

func TestWaitMountedChecksAtDeadline(t *testing.T) {
	var calls atomic.Int32
	swapMountAlive(t, func(base, accountDir string) bool {
		calls.Add(1)
		if base != "base" || accountDir != "acct" {
			t.Errorf("probe got (%q, %q), want (\"base\", \"acct\")", base, accountDir)
		}
		return true
	})
	if !waitMounted("base", "acct", 0, nil) {
		t.Fatal("waitMounted = false, want true: a zero timeout must still run one at-deadline probe")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls = %d, want exactly 1 for timeout=0", got)
	}
}

func TestWaitMountedTimesOutBounded(t *testing.T) {
	var calls atomic.Int32
	swapMountAlive(t, func(base, accountDir string) bool {
		calls.Add(1)
		return false
	})
	if waitMounted("base", "acct", 0, nil) {
		t.Fatal("waitMounted = true, want false: the probe never saw a live mount")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls = %d, want exactly 1 for timeout=0 (no extra polls past the deadline)", got)
	}
}

func TestWaitMountedLateMountKept(t *testing.T) {
	swapMountPollInterval(t, time.Millisecond)
	const timeout = 25 * time.Millisecond
	start := time.Now()
	// flipAt is the earliest instant waitMounted's internal deadline can be;
	// the real one lands the call-overhead nanoseconds later. Flipping at
	// flipAt is deterministic against the probe-at-deadline contract (every
	// probe deciding at/after the real deadline sees live) and catches a
	// check-deadline-first implementation in all but the nanosecond-scale
	// skew window between flipAt and the real deadline — a probe deciding
	// inside that window would have passed under the old ordering too, and no
	// seam short of faking time inside waitMounted can close it.
	flipAt := start.Add(timeout)
	swapMountAlive(t, func(base, accountDir string) bool {
		return !time.Now().Before(flipAt)
	})
	if !waitMounted("base", "acct", timeout, nil) {
		t.Fatal("waitMounted = false, want true: a mount landing after the deadline must be kept by the final at-deadline probe")
	}
	if waited := time.Since(start); waited < timeout {
		t.Fatalf("waitMounted returned after %v, before the %v deadline — the late-flip path was not exercised", waited, timeout)
	}
}

func TestWaitMountedBailsOnServeExit(t *testing.T) {
	swapMountPollInterval(t, 50*time.Millisecond)

	t.Run("serve exit, dir never live -> bail false well before timeout", func(t *testing.T) {
		swapMountAlive(t, func(base, accountDir string) bool { return false })
		exited := make(chan struct{})
		close(exited) // the serve goroutine already returned: a hard mount(2) failure
		const timeout = 10 * time.Second
		start := time.Now()
		if waitMounted("base", "acct", timeout, exited) {
			t.Fatal("waitMounted = true, want false: serve loop exited and the dir is not live")
		}
		if waited := time.Since(start); waited >= timeout {
			t.Fatalf("waitMounted waited %v (>= the %v timeout); it did not bail on the closed serve-exit channel", waited, timeout)
		}
	})

	t.Run("serve exit, final probe sees the mount -> kept", func(t *testing.T) {
		var calls atomic.Int32
		swapMountAlive(t, func(base, accountDir string) bool {
			// Top-of-loop probe misses; the one final probe after serve-exit
			// catches a mount that landed in the same instant.
			return calls.Add(1) > 1
		})
		exited := make(chan struct{})
		close(exited)
		if !waitMounted("base", "acct", 10*time.Second, exited) {
			t.Fatal("waitMounted = false, want true: the final probe after serve-exit saw a live mount")
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("probe calls = %d, want 2 (top probe + one final probe on serve-exit)", got)
		}
	})
}

func TestMountWaitErrSentinels(t *testing.T) {
	const tccPhrase = "grant Network Volumes access once"
	const dir = "/pool/accounts/acct-01"
	const waited = 8 * time.Second
	cases := []struct {
		name          string
		proven        bool
		wantIs        error
		wantNotIs     error
		wantTCCPhrase bool
		wantWaited    bool
	}{
		{
			name:          "unproven grant reads as TCC with walkthrough",
			proven:        false,
			wantIs:        ErrMountNotLive,
			wantNotIs:     ErrMountTimeout,
			wantTCCPhrase: true,
		},
		{
			name:       "proven grant reads as transient timeout without TCC guidance",
			proven:     true,
			wantIs:     ErrMountTimeout,
			wantNotIs:  ErrMountNotLive,
			wantWaited: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mountWaitErr(dir, waited, tc.proven)
			if err == nil {
				t.Fatal("mountWaitErr returned nil")
			}
			if !errors.Is(err, tc.wantIs) {
				t.Errorf("errors.Is(err, %v) = false, want true; err = %v", tc.wantIs, err)
			}
			if errors.Is(err, tc.wantNotIs) {
				t.Errorf("errors.Is(err, %v) = true, want false; err = %v", tc.wantNotIs, err)
			}
			msg := err.Error()
			if got := strings.Contains(msg, tccPhrase); got != tc.wantTCCPhrase {
				t.Errorf("message contains %q = %v, want %v; msg = %q", tccPhrase, got, tc.wantTCCPhrase, msg)
			}
			// The System Settings pointer is TCC guidance: present iff the
			// grant is unproven. ErrMountTimeout's godoc forbids surfacing it.
			if got := strings.Contains(msg, "System Settings"); got != tc.wantTCCPhrase {
				t.Errorf("message contains \"System Settings\" = %v, want %v; msg = %q", got, tc.wantTCCPhrase, msg)
			}
			if strings.Contains(msg, "symlink is used until then") {
				t.Errorf("message carries the stale symlink-fallback claim; msg = %q", msg)
			}
			if !strings.Contains(msg, dir) {
				t.Errorf("message does not name the account dir %q; msg = %q", dir, msg)
			}
			if tc.wantWaited && !strings.Contains(msg, waited.String()) {
				t.Errorf("message does not mention the %v wait; msg = %q", waited, msg)
			}
		})
	}
}
