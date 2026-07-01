package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/proc"
)

func flipToFuse(t *testing.T, s *Server, id int) store.Account {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "nfs"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	return a
}

func flipToSymlink(t *testing.T, s *Server, id int) {
	t.Helper()
	a, err := s.m.Store.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = "symlink"
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
}

// newHealServer's holder socket lives under a short /tmp dir (macOS caps
// sun_path at 104 bytes) and starts dead.
func newHealServer(t *testing.T) (*Server, map[int]string, *fakeFuseProv) {
	t.Helper()
	s, dirs, fake := newMigrateServer(t)
	sockDir, err := os.MkdirTemp("/tmp", "ccp-heal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	s.holderSocket = filepath.Join(sockDir, "m.sock")
	s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
	return s, dirs, fake
}

// healTick runs one heal pass exactly as the healFuseRows ticker body does.
func healTick(ctx context.Context, s *Server) {
	s.holder.refresh(s.holderClient())
	s.retryUnvouchedFuseRows(ctx)
}

// startDegradedHolder serves Health at ver but drops every List reply —
// Client.Poll's "Degraded" shape.
func startDegradedHolder(t *testing.T, ver string) string {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-degr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req mountd.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close() // probe dial with no request body
				continue
			}
			if req.Op == mountd.OpList {
				_ = conn.Close()
				continue
			}
			_ = json.NewEncoder(conn).Encode(mountd.Response{OK: true, Version: ver})
			_ = conn.Close()
		}
	}()
	return socket
}

// mountTimeoutChain is the exact error chain RemoteProvider.Setup produces for a
// mount-up timeout.
func mountTimeoutChain() error {
	return fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountTimeout, mountd.ErrMountTimeout))
}

// mountFailedChain is the exact error chain a hard mount(2) rejection crosses
// the wire as.
func mountFailedChain() error {
	return fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountFailed, mountd.ErrMountFailed))
}

// TestRemountBackoffDoublesAndCaps pins the remount backoff: doubling per
// failure, capped at 2m — under the 180s scheduler period so heal is never the
// slower recovery path.
func TestRemountBackoffDoublesAndCaps(t *testing.T) {
	b := proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap}
	cases := map[int]time.Duration{
		1:  remountBackoffBase,
		2:  20 * time.Second,
		3:  40 * time.Second,
		4:  80 * time.Second,
		5:  remountBackoffCap,
		12: remountBackoffCap,
		0:  remountBackoffBase,
		-1: remountBackoffBase,
	}
	for failures, want := range cases {
		if got := b.After(failures); got != want {
			t.Errorf("proc.Backoff{remount}.After(%d) = %v, want %v", failures, got, want)
		}
	}
}

// TestHealTickRetriesUnvouchedRowWithBackoff pins the heal loop's per-row
// backoff: a window gates retries; a successful heal vouches and drops the entry.
func TestHealTickRetriesUnvouchedRowWithBackoff(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()

	healTick(t.Context(), s)
	if fake.setupCount() != 1 {
		t.Fatalf("setups after the first tick = %d, want 1", fake.setupCount())
	}
	if st := s.rowRetry[1]; st.failures != 1 || !st.retryAt.After(time.Now()) {
		t.Fatalf("rowRetry[1] = %+v, want one failure with a future retryAt", st)
	}

	healTick(t.Context(), s)
	if fake.setupCount() != 1 {
		t.Fatalf("setups inside the backoff window = %d, want still 1", fake.setupCount())
	}

	st := s.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.rowRetry[1] = st
	healTick(t.Context(), s)
	if fake.setupCount() != 2 || s.rowRetry[1].failures != 2 {
		t.Fatalf("after the rewound window: setups=%d failures=%d, want 2/2",
			fake.setupCount(), s.rowRetry[1].failures)
	}

	fake.setupErr = nil
	st = s.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.rowRetry[1] = st
	healTick(t.Context(), s)
	if fake.setupCount() != 3 {
		t.Fatalf("setups after clearing the failure = %d, want 3", fake.setupCount())
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("healed row not vouched for in the holder cache")
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("successful heal left a rowRetry entry")
	}
}

// TestHealTickRetrySkipsClaimedAccount pins that a claimed row is neither
// attempted nor penalized (a skip is not a failure) and is retried after release.
func TestHealTickRetrySkipsClaimedAccount(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	s.rowRetry = map[int]rowRetryState{1: {failures: 2, retryAt: time.Now().Add(-time.Second)}}
	if !s.beginPoll(1) {
		t.Fatal("beginPoll failed on a free account")
	}

	healTick(t.Context(), s)
	if fake.setupCount() != 0 {
		t.Fatal("the heal loop raced the claim owner")
	}
	if got := s.rowRetry[1].failures; got != 2 {
		t.Fatalf("failures after a skip = %d, want 2 unchanged", got)
	}

	s.endPoll(1)
	healTick(t.Context(), s)
	if fake.setupCount() != 1 {
		t.Fatalf("setups after release = %d, want 1", fake.setupCount())
	}
	if got := s.rowRetry[1].failures; got != 3 {
		t.Fatalf("failures after a real attempt = %d, want 3", got)
	}
}

// TestHealTickRetryLeavesConvertedRowAndPrunes pins that a row converted to
// symlink is never healed as fuse and its ledger entry is pruned.
func TestHealTickRetryLeavesConvertedRowAndPrunes(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	s.rowRetry = map[int]rowRetryState{1: {failures: 1, retryAt: time.Now().Add(-time.Second)}}
	flipToSymlink(t, s, 1)

	healTick(t.Context(), s)

	if fake.setupCount() != 0 {
		t.Fatal("a converted row was healed as fuse")
	}
	if len(s.rowRetry) != 0 {
		t.Fatalf("rowRetry = %v, want the converted row's entry pruned", s.rowRetry)
	}
}

// TestHealTickRetriesTCCBlockedRowUnderBackoff pins that a TCC-blocked row rides
// the same backoff with guidance surfaced, and the first post-grant mount clears it.
func TestHealTickRetriesTCCBlockedRowUnderBackoff(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)

	healTick(t.Context(), s)
	if fake.setupCount() != 1 || s.rowRetry[1].failures != 1 {
		t.Fatalf("after the first tick: setups=%d failures=%d, want 1/1",
			fake.setupCount(), s.rowRetry[1].failures)
	}
	if got := s.holder.wireStatus().TCCError; got == "" {
		t.Fatal("TCC guidance not surfaced for the blocked row")
	}
	healTick(t.Context(), s)
	if fake.setupCount() != 1 {
		t.Fatalf("setups inside the backoff window = %d, want still 1", fake.setupCount())
	}

	fake.setupErr = nil
	st := s.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.rowRetry[1] = st
	healTick(t.Context(), s)
	if !s.holder.ready(dirs[1]) {
		t.Fatal("granted row not mounted and vouched for")
	}
	if got := s.holder.wireStatus().TCCError; got != "" {
		t.Fatalf("TCCError after the successful mount = %q, want cleared via noteMounted", got)
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("successful heal left a rowRetry entry")
	}
}

// TestHealTickRemountsHeldDeadRow pins the held-dead heal: a dir the holder
// lists but cannot serve is logged with the deep-probe-picked copy and
// remounted through the ordinary healFuse path.
func TestHealTickRemountsHeldDeadRow(t *testing.T) {
	const (
		wedgeCopy = "wedged mirror (serves metadata but hangs reads)"
		deadCopy  = "dead mirror (fails reads outright; unmounted out of band or its fuse worker died?)"
	)
	cases := map[string]struct {
		wedged   bool
		wantCopy string
		notCopy  string
	}{
		"deep-wedged mirror logs the wedge copy": {
			wedged:   true,
			wantCopy: wedgeCopy,
			notCopy:  deadCopy,
		},
		"plain-dead mirror logs the dead copy, never the wedge copy": {
			wantCopy: deadCopy,
			notCopy:  wedgeCopy,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, dirs, fake := newHealServer(t)
			flipToFuse(t, s, 1)
			// Deep wedge stays shallow-live (Live=true); plain-dead fails it (Live=false).
			s.holderSocket = startCannedHolder(t, []mountd.MountInfo{
				{Dir: dirs[1], Base: "/base", Live: tc.wedged},
			})
			if tc.wedged {
				s.holder.markDeepWedged(dirs[1])
			} else {
				// Definitive-dead (not a liveness timeout) so deferShallowDead
				// proceeds without debounce.
				fake.healthErr = errors.New("not a mountpoint")
			}
			var buf bytes.Buffer
			s.log = log.New(&buf, "", 0)

			healTick(t.Context(), s)

			if fake.setupCount() != 1 {
				t.Fatalf("setups = %d, want the held-dead mirror remounted", fake.setupCount())
			}
			if !s.holder.ready(dirs[1]) {
				t.Fatal("remounted mirror not vouched for")
			}
			out := buf.String()
			if !strings.Contains(out, tc.wantCopy) {
				t.Fatalf("held-dead log line missing %q:\n%s", tc.wantCopy, out)
			}
			if strings.Contains(out, tc.notCopy) {
				t.Fatalf("held-dead log line carries the wrong copy %q:\n%s", tc.notCopy, out)
			}
			if !strings.Contains(out, "live session") {
				t.Fatalf("held-dead log line missing the session count:\n%s", out)
			}
			if _, ok := s.rowRetry[1]; ok {
				t.Fatal("successful remount left a rowRetry entry")
			}
		})
	}
}

// TestDeferShallowDead pins the corroboration gate: a holder-reported shallow-dead
// mirror is re-probed with the daemon's own Health before teardown.
func TestDeferShallowDead(t *testing.T) {
	timeout := fmt.Errorf("%w: slow", overlay.ErrLivenessTimeout)
	dead := errors.New("not a mountpoint")
	tests := []struct {
		name       string
		health     error
		peerAlive  bool
		wantDefers []bool // one entry per consecutive deferShallowDead call
	}{
		{name: "live corroboration never remounts", health: nil, peerAlive: true, wantDefers: []bool{true, true, true}},
		{name: "timeout+peer-alive debounces then remounts", health: timeout, peerAlive: true, wantDefers: []bool{true, false}},
		{name: "timeout+peer-gone remounts at once", health: timeout, peerAlive: false, wantDefers: []bool{false}},
		{name: "definitive dead remounts at once", health: dead, peerAlive: true, wantDefers: []bool{false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _, fake := newHealServer(t)
			flipToFuse(t, s, 1)
			fake.healthErr = tc.health
			s.peerAlive = func(string) bool { return tc.peerAlive }
			acct, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}
			for i, want := range tc.wantDefers {
				if got := s.deferShallowDead(acct); got != want {
					t.Fatalf("deferShallowDead call %d = %v, want %v", i+1, got, want)
				}
			}
		})
	}
}

// TestHealFuseRowsLoopTicksAndExits pins the loop plumbing: ticks run the
// per-account heal, and the goroutine exits on ctx cancellation.
func TestHealFuseRowsLoopTicksAndExits(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy, vouches for nothing
	s.healInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.healFuseRows(ctx); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for fake.setupCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the heal loop never remounted the fuse row")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("healFuseRows did not exit on ctx cancellation")
	}
}

// TestEvictionNeverDialsMountsSocket pins holder isolation: evicting a
// version-skewed daemon — clean step-down or wedged-orphan kill — must never
// touch the mount-holder socket.
func TestEvictionNeverDialsMountsSocket(t *testing.T) {
	tattle := func(t *testing.T) (string, *atomic.Int32) {
		t.Helper()
		sockDir, err := os.MkdirTemp("/tmp", "ccp-tattle")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
		socket := filepath.Join(sockDir, "m.sock")
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		var dials atomic.Int32
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				dials.Add(1)
				_ = conn.Close()
			}
		}()
		return socket, &dials
	}

	t.Run("clean step-down", func(t *testing.T) {
		guardKillSocketPeer(t)
		f := newFakeDaemon(t, "0.0.0-old", true)
		mounts, dials := tattle(t)
		s := testServer(f.socket, 3*time.Second)
		s.holderSocket = mounts
		ln, lock, err := s.listen()
		if err != nil {
			t.Fatalf("listen should evict the skewed daemon and bind: %v", err)
		}
		defer func() { _ = ln.Close() }()
		defer func() { _ = lock.Close() }()
		if got := dials.Load(); got != 0 {
			t.Fatalf("daemon eviction dialed the mounts socket %d time(s)", got)
		}
	})

	t.Run("wedged orphan killed", func(t *testing.T) {
		f := newFakeDaemon(t, "0.0.0-old", false)
		mounts, dials := tattle(t)
		setKillSocketPeer(t, func(socket string) (int, error) {
			if socket != f.socket {
				t.Errorf("kill aimed at %q, want the daemon socket %q", socket, f.socket)
			}
			_ = f.ln.Close() // the "kill" releases the daemon socket
			return 999001, nil
		})
		s := testServer(f.socket, 2*time.Second)
		s.holderSocket = mounts
		ln, lock, err := s.listen()
		if err != nil {
			t.Fatalf("listen should reap the wedged orphan and bind: %v", err)
		}
		defer func() { _ = ln.Close() }()
		defer func() { _ = lock.Close() }()
		if got := dials.Load(); got != 0 {
			t.Fatalf("daemon eviction dialed the mounts socket %d time(s)", got)
		}
	})
}

// swapForceUnmount swaps the global force-unmount seam; callers must not run in
// parallel.
func swapForceUnmount(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := forceUnmount
	forceUnmount = fn
	t.Cleanup(func() { forceUnmount = prev })
}

// driveRetryTicks runs n heal ticks, rewinding the per-row backoff before each
// so every tick makes a real attempt.
func driveRetryTicks(t *testing.T, s *Server, id, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if st, ok := s.rowRetry[id]; ok {
			st.retryAt = time.Now().Add(-time.Second)
			s.rowRetry[id] = st
		}
		healTick(t.Context(), s)
	}
}

// TestRemountBreakerThreshold pins the breaker const guard: the threshold must be
// >= 2 so a single transient mount failure never escalates.
func TestRemountBreakerThreshold(t *testing.T) {
	if remountBreakerThreshold < 2 {
		t.Fatalf("remountBreakerThreshold = %d, want >= 2 so a single transient mount failure never escalates", remountBreakerThreshold)
	}
}

// TestRemountBreakerEscalates pins the wedged-mount breaker: after
// remountBreakerThreshold consecutive wedged remounts it force-unmounts the
// carcass and converts the row to symlink.
func TestRemountBreakerEscalates(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain() // healRetry forever — the wedged shape
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var (
		mu        sync.Mutex
		unmounted []string
	)
	swapForceUnmount(t, func(dir string) error {
		mu.Lock()
		unmounted = append(unmounted, dir)
		mu.Unlock()
		return nil
	})
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	driveRetryTicks(t, s, 1, remountBreakerThreshold)

	mu.Lock()
	gotUnmounted := append([]string(nil), unmounted...)
	mu.Unlock()
	if len(gotUnmounted) != 1 || gotUnmounted[0] != dirs[1] {
		t.Fatalf("breaker force-unmounted %v, want exactly [%s]", gotUnmounted, dirs[1])
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after the breaker = %q, want symlink", got)
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("breaker left a rowRetry entry; the churn would continue")
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("breaker did not drop the holder-cache vouch for the converted dir")
	}
	if !strings.Contains(buf.String(), "never recovered") {
		t.Fatalf("breaker escalation not surfaced in the log:\n%s", buf.String())
	}
}

// TestHealDefersBreakerUnderLiveSession is the regression lock for the kernel
// panic (nfs_vinvalbuf2: ubc_msync failed): a dead mirror still backing a live
// session is left mounted and takes no hazard strike, so the wedged breaker can
// never fire on a busy mount.
func TestHealDefersBreakerUnderLiveSession(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.healthErr = errors.New("mirror is dead")
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}

	var (
		mu        sync.Mutex
		unmounted []string
	)
	swapForceUnmount(t, func(dir string) error {
		mu.Lock()
		unmounted = append(unmounted, dir)
		mu.Unlock()
		return nil
	})
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	driveRetryTicks(t, s, 1, remountBreakerThreshold+2)

	if got := fake.teardownCount(); got != 0 {
		t.Fatalf("teardowns under a live session = %d, want 0 (a busy mirror must never be torn down)", got)
	}
	mu.Lock()
	gotUnmounted := append([]string(nil), unmounted...)
	mu.Unlock()
	if len(gotUnmounted) != 0 {
		t.Fatalf("force-unmounted %v under a live session, want none (force-unmounting a busy NFS mirror panics the kernel)", gotUnmounted)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind under a live session = %q, want fuse (it must stay mounted, never retreat)", got)
	}
	if got := s.rowRetry[1].hazard; got != 0 {
		t.Fatalf("hazard count under a busy mount = %d, want 0 (the wedged breaker must be unreachable while busy)", got)
	}
	if !strings.Contains(buf.String(), "NOT force-unmounting") {
		t.Fatalf("deferral not surfaced in the log:\n%s", buf.String())
	}
}

// TestRemountBreakerHoldsUnderThreshold pins that fewer than threshold
// consecutive failures keep retrying without escalation.
func TestRemountBreakerHoldsUnderThreshold(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	driveRetryTicks(t, s, 1, remountBreakerThreshold-1)

	if unmounts != 0 {
		t.Fatalf("force-unmounts under the threshold = %d, want 0", unmounts)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind under the threshold = %q, want fuse (still retrying)", got)
	}
	if got := s.rowRetry[1].hazard; got != remountBreakerThreshold-1 {
		t.Fatalf("hazard count = %d, want %d (one short of the breaker)", got, remountBreakerThreshold-1)
	}
}

// TestRemountBreakerResetsOnMount pins that a successful mount clears the
// breaker's hazard count: a later failure restarts the count from one.
func TestRemountBreakerResetsOnMount(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	driveRetryTicks(t, s, 1, remountBreakerThreshold-2)
	if got := s.rowRetry[1].hazard; got != remountBreakerThreshold-2 {
		t.Fatalf("hazard before recovery = %d, want %d", got, remountBreakerThreshold-2)
	}

	fake.setupErr = nil
	driveRetryTicks(t, s, 1, 1)
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("a successful mount left a rowRetry entry")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("recovered row not vouched for")
	}

	fake.setupErr = mountTimeoutChain()
	driveRetryTicks(t, s, 1, 1)
	if got := s.rowRetry[1].hazard; got != 1 {
		t.Fatalf("hazard after a fresh failure = %d, want 1 (reset by the recovery)", got)
	}
	if unmounts != 0 {
		t.Fatalf("force-unmounts across a recovering row = %d, want 0", unmounts)
	}
}

// TestWedgeBreakerNeverEscalatesTCCRow pins that a TCC-blocked row (a clean
// not-mounted state, never a kernel wedge) never trips the wedged breaker: its
// hazard count stays 0 however many TCC blocks accrue.
func TestWedgeBreakerNeverEscalatesTCCRow(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive) // healTCCBlocked
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	// One short of the grant grace — already past the wedged breaker's
	// threshold (tccBreakerThreshold > remountBreakerThreshold).
	ticks := tccBreakerThreshold - 1
	driveRetryTicks(t, s, 1, ticks)

	if unmounts != 0 {
		t.Fatalf("TCC-blocked row force-unmounted %d time(s); the wedged breaker must never fire on it", unmounts)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("TCC-blocked row kind = %q, want fuse (still within the grant grace)", got)
	}
	st, ok := s.rowRetry[1]
	if !ok {
		t.Fatal("TCC-blocked row dropped its retry ledger before the grace expired")
	}
	if st.hazard != 0 {
		t.Fatalf("TCC hazard = %d, want 0 (never counts toward the wedged breaker even past its threshold)", st.hazard)
	}
	if st.tccBlocks != ticks {
		t.Fatalf("TCC blocks = %d, want %d (the grant grace counts these)", st.tccBlocks, ticks)
	}
	if st.failures != ticks {
		t.Fatalf("TCC failures = %d, want %d (kept backing off)", st.failures, ticks)
	}
}

// TestTCCBreakerEscalates pins the bounded grant grace: after
// tccBreakerThreshold consecutive TCC-blocked heals the row retreats to
// symlink so the account is usable.
func TestTCCBreakerEscalates(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)
	fakeOverlayMounted(t, func(string) bool { return false })

	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	driveRetryTicks(t, s, 1, tccBreakerThreshold)

	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after the TCC grace = %q, want symlink", got)
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("TCC breaker left a rowRetry entry; the churn would continue")
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("TCC breaker did not drop the holder-cache vouch for the converted dir")
	}
	if got := s.holder.wireStatus().TCCError; got != "" {
		t.Fatalf("TCC breaker left stale guidance %q; it must clear on retreat", got)
	}
	if !strings.Contains(buf.String(), "volume-access grant never landed") {
		t.Fatalf("TCC retreat not surfaced in the log:\n%s", buf.String())
	}
}

// TestTCCBreakerEscalatesUnderLiveSession pins that the live-session gate keys
// on a busy mount, not on sessions: a TCC-blocked row never mounted, so the
// retreat proceeds under a live session — no busy mirror, no panic hazard.
func TestTCCBreakerEscalatesUnderLiveSession(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)
	fakeOverlayMounted(t, func(string) bool { return false })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}

	driveRetryTicks(t, s, 1, tccBreakerThreshold)

	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after the TCC grace = %q, want symlink (a live session must not block the retreat)", got)
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("TCC breaker left a rowRetry entry under a live session")
	}
}

// TestTCCBreakerLateGrantPreventsFallback pins that a grant landing before the
// grace expires mounts the row and prevents the retreat.
func TestTCCBreakerLateGrantPreventsFallback(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)
	fakeOverlayMounted(t, func(string) bool { return false })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	driveRetryTicks(t, s, 1, tccBreakerThreshold-1)
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind one short of the grace = %q, want fuse (still waiting on the grant)", got)
	}
	if got := s.rowRetry[1].tccBlocks; got != tccBreakerThreshold-1 {
		t.Fatalf("tccBlocks = %d, want %d (one short of the grace)", got, tccBreakerThreshold-1)
	}

	fake.setupErr = nil
	driveRetryTicks(t, s, 1, 1)
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind after a late grant = %q, want fuse (it mounted, never retreated)", got)
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("a successful mount left a rowRetry entry")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("granted row not vouched for")
	}
	if got := s.holder.wireStatus().TCCError; got != "" {
		t.Fatalf("late grant left stale TCC guidance %q; a live mount must clear it", got)
	}
	if unmounts != 0 {
		t.Fatalf("force-unmounts across a late-granted row = %d, want 0", unmounts)
	}
}

// TestTCCBreakerThreshold pins the TCC grace const guard: a pending grant (a
// benign wait, not a kernel hazard) earns a longer grace, so tccBreakerThreshold
// must exceed remountBreakerThreshold.
func TestTCCBreakerThreshold(t *testing.T) {
	if tccBreakerThreshold <= remountBreakerThreshold {
		t.Fatalf("tccBreakerThreshold = %d, want > remountBreakerThreshold (%d): a pending grant earns a longer grace than a kernel wedge", tccBreakerThreshold, remountBreakerThreshold)
	}
}

// TestHealFuseMountFailedRetreatsImmediately pins that a hard mount rejection
// (ErrMountFailed) retreats to symlink on the first heal — a dead-end, not a
// pending grant.
func TestHealFuseMountFailedRetreatsImmediately(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountFailedChain()
	fakeOverlayMounted(t, func(string) bool { return false })

	driveRetryTicks(t, s, 1, 1)

	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after one hard-failure heal = %q, want symlink (immediate retreat, no TCC wait, no breaker countdown)", got)
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("hard-failure retreat did not drop the holder-cache vouch")
	}
}

// TestRetreatAllFuseRowsConvertsPoolToSymlink pins the whole-pool retreat:
// retreatAllFuseRows force-unmounts and converts every fuse row to symlink.
func TestRetreatAllFuseRowsConvertsPoolToSymlink(t *testing.T) {
	s, _, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	flipToFuse(t, s, 2)
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	fuse, err := s.fuseAccounts()
	if err != nil {
		t.Fatal(err)
	}
	s.retreatAllFuseRows(t.Context(), fuse, "child crash-looped")

	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("acct-01 kind after the retreat = %q, want symlink", got)
	}
	if got := kindOf(t, s, 2); got != "symlink" {
		t.Fatalf("acct-02 kind after the retreat = %q, want symlink", got)
	}
	if !strings.Contains(buf.String(), "falling back to symlink") {
		t.Fatalf("retreat not surfaced in the log:\n%s", buf.String())
	}
}

// TestRetreatBailsOnWedgedForceUnmount pins the wedged-unmount guard
// (convertRowToSymlink): a forced unmount that never completes leaves the row
// fuse, since ConvertOverlay's Teardown would otherwise re-spawn the very holder
// being retreated from.
func TestRetreatBailsOnWedgedForceUnmount(t *testing.T) {
	s, dirs, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
	swapForceUnmount(t, func(string) error { return errors.New("force-unmount timed out") })

	fuse, err := s.fuseAccounts()
	if err != nil {
		t.Fatal(err)
	}
	s.retreatAllFuseRows(t.Context(), fuse, "child crash-looped")

	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("acct-01 kind = %q, want fuse (a wedged force-unmount must NOT convert through a re-spawning Teardown)", got)
	}
}

// hostFuseCapable makes canSpawnHolder()/pool.CanHostFuse() pass. The capability
// gate probes the shared default holder socket (mountd.DefaultHolderSocket), not
// the injected s.holderSocket, so a canned holder is stood up there under a short
// HOME (macOS sun_path cap).
func hostFuseCapable(t *testing.T) {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".fusekit"), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", mountd.DefaultHolderSocket())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go serveCannedHolder(ln, nil)
}

// TestReconcileCapabilityGateRetreatsPoolWhenFuseUnavailable pins the startup
// capability gate: a probe rejected outright (ErrMountFailed) retreats every fuse
// row to symlink and records symlink as the new-account default.
func TestReconcileCapabilityGateRetreatsPoolWhenFuseUnavailable(t *testing.T) {
	s, _, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	flipToFuse(t, s, 2)
	hostFuseCapable(t)
	s.holderSocket = startCapabilityHolder(t, nil, mountd.ClassMountFailed, "fuse-t not loadable")
	fakeOverlayMounted(t, func(string) bool { return false })
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	s.reconcileOverlays(t.Context())

	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("acct-01 kind after the capability gate = %q, want symlink", got)
	}
	if got := kindOf(t, s, 2); got != "symlink" {
		t.Fatalf("acct-02 kind after the capability gate = %q, want symlink", got)
	}
	kind, ok, err := s.m.ConfiguredOverlayKind()
	if err != nil || !ok || kind != fkoverlay.BackendSymlink {
		t.Fatalf("new-account default = (%q, ok=%v, err=%v), want symlink", kind, ok, err)
	}
	if !strings.Contains(buf.String(), "fuse is unavailable on this machine") {
		t.Fatalf("capability retreat not surfaced in the log:\n%s", buf.String())
	}
}

// TestReconcileCapabilityGateProceedsWhenProbePending pins that a probe pending
// the Network Volumes grant (ErrTCCDenied) does not trip the gate: rows stay fuse
// for the per-row TCC grace and the new-account default is not flipped.
func TestReconcileCapabilityGateProceedsWhenProbePending(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	hostFuseCapable(t)
	s.holderSocket = startCapabilityHolder(t, nil, mountd.ClassTCC, "grant pending")
	// Health fails so reconcileAccount heals rather than adopting; the heal then TCC-blocks.
	fake.healthErr = errors.New("not a mountpoint")
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)
	fakeOverlayMounted(t, func(string) bool { return false })

	s.reconcileOverlays(t.Context())

	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("acct-01 kind after a PENDING probe = %q, want fuse (the gate must defer to the per-row grace)", got)
	}
	if kind, ok, _ := s.m.ConfiguredOverlayKind(); ok && kind == fkoverlay.BackendSymlink {
		t.Fatal("a pending probe flipped the new-account default to symlink; only a hard failure may")
	}
}

// TestSweepOrphanMountpointsClearsNonRowCarcass pins the startup orphan sweep: a
// mountpoint under the accounts dir owned by no row (a pre-row `ccp add` carcass)
// is force-unmounted, while a current row's dir is left to reconcile/heal.
func TestSweepOrphanMountpointsClearsNonRowCarcass(t *testing.T) {
	s, _, _ := newMigrateServer(t) // sets HOME to a temp dir, so AccountsDir() is hermetic
	if err := os.MkdirAll(pool.AccountsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	rowDir := filepath.Join(pool.AccountsDir(), "acct-01")
	orphan := filepath.Join(pool.AccountsDir(), "acct-07")
	for _, d := range []string{rowDir, orphan} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	fakeOverlayMounted(t, func(string) bool { return true })
	var unmounted []string
	swapForceUnmount(t, func(dir string) error {
		unmounted = append(unmounted, dir)
		return nil
	})

	s.sweepOrphanMountpoints(t.Context(), []store.Account{{ID: 1, ConfigDir: rowDir, OverlayKind: "nfs"}})

	if len(unmounted) != 1 || unmounted[0] != orphan {
		t.Fatalf("force-unmounted %v, want exactly the non-row orphan %q (the row dir must be left alone)", unmounted, orphan)
	}
}

// TestLiveSessionGate pins the shared force-unmount precondition: sessions on
// the dir read busy with their count, and a scan failure fails closed to busy.
func TestLiveSessionGate(t *testing.T) {
	const dir = "/pool/acct-01"
	tests := []struct {
		name     string
		sessions []procscan.Session
		scanErr  error
		wantBusy bool
		wantN    int
	}{
		{name: "sessions on the dir are busy with their count", sessions: []procscan.Session{{PID: 1, ConfigDir: dir}, {PID: 2, ConfigDir: dir}}, wantBusy: true, wantN: 2},
		{name: "a session elsewhere is idle", sessions: []procscan.Session{{PID: 1, ConfigDir: "/pool/acct-02"}}, wantBusy: false, wantN: 0},
		{name: "no sessions is idle", sessions: nil, wantBusy: false, wantN: 0},
		{name: "a scan failure fails closed to busy", scanErr: errors.New("ps exploded"), wantBusy: true, wantN: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := newHealServer(t)
			s.scanSessions = func(context.Context) ([]procscan.Session, error) { return tc.sessions, tc.scanErr }
			busy, n := s.liveSessionGate(t.Context(), dir)
			if busy != tc.wantBusy || n != tc.wantN {
				t.Fatalf("liveSessionGate = (%v, %d), want (%v, %d)", busy, n, tc.wantBusy, tc.wantN)
			}
		})
	}
}

// TestHealLoopUnmountGate pins mountFuse's teardown-before-remount gate: a
// mounted-but-dead mirror is torn down and remounted only when idle; under a
// live session — or a failed scan — it is left mounted with no hazard strike.
func TestHealLoopUnmountGate(t *testing.T) {
	tests := []struct {
		name          string
		scanKind      string
		wantTeardowns int
		wantSetups    int
	}{
		{name: "idle mirror is torn down and remounted", scanKind: "idle", wantTeardowns: 1, wantSetups: 1},
		{name: "busy mirror is left mounted", scanKind: "busy", wantTeardowns: 0, wantSetups: 0},
		{name: "scan failure leaves it mounted", scanKind: "err", wantTeardowns: 0, wantSetups: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, dirs, fake := newHealServer(t)
			flipToFuse(t, s, 1)
			s.holderSocket = startCannedHolder(t, nil)
			// Mounted-but-dead: mountFuse enters the teardown-before-remount branch.
			fake.healthErr = errors.New("mirror is dead")
			fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
			switch tc.scanKind {
			case "idle":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
			case "busy":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) {
					return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
				}
			case "err":
				s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, errors.New("ps exploded") }
			}

			driveRetryTicks(t, s, 1, 1)

			if got := fake.teardownCount(); got != tc.wantTeardowns {
				t.Fatalf("teardowns = %d, want %d", got, tc.wantTeardowns)
			}
			if got := fake.setupCount(); got != tc.wantSetups {
				t.Fatalf("setups = %d, want %d", got, tc.wantSetups)
			}
			// Idle drops the ledger; deferred backs off without a strike — the
			// hazard never accrues either way.
			if got := s.rowRetry[1].hazard; got != 0 {
				t.Fatalf("hazard = %d, want 0", got)
			}
		})
	}
}

// TestRetreatDefersUnderLiveSessionWhenMounted pins the retreat gate
// (convertRowToSymlink, via retreatAllFuseRows): a live mountpoint with a live
// session stays fuse, never force-unmounted.
func TestRetreatDefersUnderLiveSessionWhenMounted(t *testing.T) {
	s, dirs, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	fuse, err := s.fuseAccounts()
	if err != nil {
		t.Fatal(err)
	}
	s.retreatAllFuseRows(t.Context(), fuse, "child crash-looped")

	if unmounts != 0 {
		t.Fatalf("retreat force-unmounted a busy mounted mirror %d time(s), want 0 (would panic the kernel)", unmounts)
	}
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind after a deferred retreat = %q, want fuse (left mounted)", got)
	}
	if !strings.Contains(buf.String(), "symlink retreat deferred") {
		t.Fatalf("deferral not surfaced in the log:\n%s", buf.String())
	}
}

// TestSweepOrphanDefersUnderLiveSession pins that a rowless mountpoint a live
// claude is still bound to is not yet a carcass: the sweep leaves it mounted.
func TestSweepOrphanDefersUnderLiveSession(t *testing.T) {
	s, _, _ := newMigrateServer(t)
	if err := os.MkdirAll(pool.AccountsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(pool.AccountsDir(), "acct-07")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatal(err)
	}
	fakeOverlayMounted(t, func(string) bool { return true })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: orphan}}, nil
	}
	var unmounted []string
	swapForceUnmount(t, func(dir string) error {
		unmounted = append(unmounted, dir)
		return nil
	})

	s.sweepOrphanMountpoints(t.Context(), nil)

	if len(unmounted) != 0 {
		t.Fatalf("swept a busy orphan carcass %v, want none (force-unmounting a busy NFS mirror panics)", unmounted)
	}
}

// TestReconcileStaleMountpointDefersUnderLiveSession pins that a symlink row whose
// dir is unexpectedly a live mountpoint (aborted-rollback wreckage) is force-cleared
// only when idle; with a live claude bound, reconcileAccount leaves it.
func TestReconcileStaleMountpointDefersUnderLiveSession(t *testing.T) {
	s, dirs, _ := newMigrateServer(t) // acct-1 is a symlink row
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)

	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	s.reconcileAccount(t.Context(), a)

	if unmounts != 0 {
		t.Fatalf("reconcile force-unmounted a busy stale mountpoint %d time(s), want 0 (would panic the kernel)", unmounts)
	}
	if !strings.Contains(buf.String(), "stale mountpoint left under") {
		t.Fatalf("deferral not surfaced in the log:\n%s", buf.String())
	}
}
