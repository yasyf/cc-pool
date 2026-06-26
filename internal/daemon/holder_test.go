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

// flipToFuse flips an account row to the fuse kind, returning the fresh row.
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

// flipToSymlink flips an account row back to the symlink kind.
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

// newHealServer wires newMigrateServer for the steady-state heal-loop tests: the
// fake fuse provider behind the Manager seam, an idle session scan, and a holder
// socket under a short /tmp dir (macOS caps sun_path at 104 bytes) that starts
// dead — heal tests repoint it at a canned holder. cc-pool no longer owns the
// holder lifecycle, so there is no spawn seam or supervisor to wire.
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

// healTick runs one steady-state heal pass exactly as the healFuseRows ticker
// body does: refresh the shared-holder cache, then re-register (or retreat to
// symlink) every fuse row the holder cannot vouch for. cc-pool's in-process
// supervisor is gone — the shared holder's lifecycle is launchd's — so the tests
// drive the two heal steps directly instead of through the former superviseTick.
func healTick(s *Server, ctx context.Context) {
	s.holder.refresh(s.holderClient())
	s.retryUnvouchedFuseRows(ctx)
}

// startDegradedHolder serves Health at ver but drops every List reply (closing
// the connection) — the Client.Poll "Degraded" shape: a holder alive at a known
// version whose mount set is unreadable. It lets a refresh land on the degraded
// arm without a real holder. Returns the socket path.
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
				return // listener closed: defined exit
			}
			var req mountd.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close() // probe dial with no request body
				continue
			}
			if req.Op == mountd.OpList {
				_ = conn.Close() // drop the List reply: Health-ok + List-fail = Degraded
				continue
			}
			_ = json.NewEncoder(conn).Encode(mountd.Response{OK: true, Version: ver})
			_ = conn.Close()
		}
	}()
	return socket
}

// mountTimeoutChain is the exact error chain RemoteProvider.Setup produces
// for a mount-up timeout under a proven "Network Volumes" grant: the
// provider's wrap around overlayClass's dual-wrap of the wire sentinel.
func mountTimeoutChain() error {
	return fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountTimeout, mountd.ErrMountTimeout))
}

// mountFailedChain is the error a hard mount(2) rejection crosses the wire as:
// the provider's wrap around overlayClass's dual-wrap of the ClassMountFailed
// wire sentinel. healFuse must route it to an immediate symlink fallback, never
// the TCC wait.
func mountFailedChain() error {
	return fmt.Errorf("mount: %w", fmt.Errorf("%w: %w", overlay.ErrMountFailed, mountd.ErrMountFailed))
}

// TestRemountBackoffDoublesAndCaps pins the per-row remount backoff (now
// proc.Backoff with the remount constants): base-doubling per failure, capped at
// 2 minutes — deliberately under the 180s scheduler period, so the heal loop is
// never the slower recovery path.
func TestRemountBackoffDoublesAndCaps(t *testing.T) {
	b := proc.Backoff{Base: remountBackoffBase, Cap: remountBackoffCap}
	cases := map[int]time.Duration{
		1:  remountBackoffBase, // first failure -> base
		2:  20 * time.Second,   // doubled
		3:  40 * time.Second,   // doubled again
		4:  80 * time.Second,   // still under the cap
		5:  remountBackoffCap,  // 160s capped to 2min
		12: remountBackoffCap,  // stays capped
		0:  remountBackoffBase, // degenerate input never shrinks below base
		-1: remountBackoffBase, // negative input never shrinks below base
	}
	for failures, want := range cases {
		if got := b.After(failures); got != want {
			t.Errorf("proc.Backoff{remount}.After(%d) = %v, want %v", failures, got, want)
		}
	}
}

// TestHealTickRetriesUnvouchedRowWithBackoff pins the steady-state heal loop: a
// fuse row a healthy holder cannot vouch for is retried each tick under per-row
// backoff — attempts advance the failure count, the window gates the next tick,
// and a successful heal vouches and drops the ledger entry.
func TestHealTickRetriesUnvouchedRowWithBackoff(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy at our version, vouching for nothing
	fake.setupErr = mountTimeoutChain()

	// First heal tick: one attempt, booked as one failure with a window.
	healTick(s, t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups after the first tick = %d, want 1", fake.setupCount())
	}
	if st := s.rowRetry[1]; st.failures != 1 || !st.retryAt.After(time.Now()) {
		t.Fatalf("rowRetry[1] = %+v, want one failure with a future retryAt", st)
	}

	// Immediately ticking again sits inside the window: no attempt.
	healTick(s, t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups inside the backoff window = %d, want still 1", fake.setupCount())
	}

	// Window rewound: the retry runs and the failure count advances.
	st := s.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.rowRetry[1] = st
	healTick(s, t.Context())
	if fake.setupCount() != 2 || s.rowRetry[1].failures != 2 {
		t.Fatalf("after the rewound window: setups=%d failures=%d, want 2/2",
			fake.setupCount(), s.rowRetry[1].failures)
	}

	// Failure cleared: the next windowed attempt mounts, vouches, and drops
	// the ledger entry.
	fake.setupErr = nil
	st = s.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.rowRetry[1] = st
	healTick(s, t.Context())
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

// TestHealTickRetrySkipsClaimedAccount pins the skip-don't-race discipline on
// the steady-state loop: an eligible row someone else owns is neither attempted
// nor penalized — a skip is not a failure — and the next tick after release
// retries it.
func TestHealTickRetrySkipsClaimedAccount(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	// An eligible ledger entry whose window has passed…
	s.rowRetry = map[int]rowRetryState{1: {failures: 2, retryAt: time.Now().Add(-time.Second)}}
	// …on an account someone else owns.
	if !s.beginPoll(1) {
		t.Fatal("beginPoll failed on a free account")
	}

	healTick(s, t.Context())
	if fake.setupCount() != 0 {
		t.Fatal("the heal loop raced the claim owner")
	}
	if got := s.rowRetry[1].failures; got != 2 {
		t.Fatalf("failures after a skip = %d, want 2 unchanged", got)
	}

	// Released: the still-open window admits the next tick's attempt.
	s.endPoll(1)
	healTick(s, t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups after release = %d, want 1", fake.setupCount())
	}
	if got := s.rowRetry[1].failures; got != 3 {
		t.Fatalf("failures after a real attempt = %d, want 3", got)
	}
}

// TestHealTickRetryLeavesConvertedRowAndPrunes pins the ledger hygiene: a row
// that earned a retry entry while fuse and then converted to symlink is never
// healed as fuse, and its entry is pruned from the ledger.
func TestHealTickRetryLeavesConvertedRowAndPrunes(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	// The row earned a ledger entry while fuse…
	s.rowRetry = map[int]rowRetryState{1: {failures: 1, retryAt: time.Now().Add(-time.Second)}}
	// …then converted away.
	flipToSymlink(t, s, 1)

	healTick(s, t.Context())

	if fake.setupCount() != 0 {
		t.Fatal("a converted row was healed as fuse")
	}
	if len(s.rowRetry) != 0 {
		t.Fatalf("rowRetry = %v, want the converted row's entry pruned", s.rowRetry)
	}
}

// TestHealTickRetriesTCCBlockedRowUnderBackoff pins the post-grant story: a
// TCC-blocked row rides the same backoff — attempted, bounded, never hot — with
// the guidance surfaced on the wire, and the first successful mount after the
// grant clears it via noteMounted.
func TestHealTickRetriesTCCBlockedRowUnderBackoff(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)

	healTick(s, t.Context())
	if fake.setupCount() != 1 || s.rowRetry[1].failures != 1 {
		t.Fatalf("after the first tick: setups=%d failures=%d, want 1/1",
			fake.setupCount(), s.rowRetry[1].failures)
	}
	if got := s.holder.wireStatus().TCCError; got == "" {
		t.Fatal("TCC guidance not surfaced for the blocked row")
	}
	// Inside the window: bounded, not hot.
	healTick(s, t.Context())
	if fake.setupCount() != 1 {
		t.Fatalf("setups inside the backoff window = %d, want still 1", fake.setupCount())
	}

	// Grant landed: the next windowed attempt mounts, vouches, and clears the
	// guidance through noteMounted.
	fake.setupErr = nil
	st := s.rowRetry[1]
	st.retryAt = time.Now().Add(-time.Second)
	s.rowRetry[1] = st
	healTick(s, t.Context())
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
// NAMES in List but that is not servable is logged loudly — the daemon's
// deep-probe verdict picks the copy (a deep wedge is shallow-live but hangs
// reads; a plain-dead mirror fails its shallow liveness outright), the
// live-session count and relaunch guidance appear in both shapes — and remounted
// through the ordinary healFuse path.
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
			// A deep wedge is shallow-Live=true (it answers shallow stats) but
			// is marked wedged by the daemon's OWN deep probe; a plain-dead
			// mirror fails its shallow liveness (Live=false).
			s.holderSocket = startCannedHolder(t, []mountd.MountInfo{
				{Dir: dirs[1], Base: "/base", Live: tc.wedged},
			})
			if tc.wedged {
				s.holder.markDeepWedged(dirs[1])
			} else {
				// A plain-dead mirror fails its shallow liveness OUTRIGHT, so the
				// daemon's own corroboration (deferShallowDead) must read it
				// definitively dead — not a liveness timeout — for the held-dead
				// remount to fire without debounce.
				fake.healthErr = errors.New("not a mountpoint")
			}
			var buf bytes.Buffer
			s.log = log.New(&buf, "", 0)

			healTick(s, t.Context())

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
			if !strings.Contains(out, "live session") || !strings.Contains(out, "relaunch") {
				t.Fatalf("held-dead log line missing the session count or relaunch guidance:\n%s", out)
			}
			if _, ok := s.rowRetry[1]; ok {
				t.Fatal("successful remount left a rowRetry entry")
			}
		})
	}
}

// TestDeferShallowDead pins the corroboration gate that suppresses false-positive
// remounts: a holder-reported shallow-dead mirror (List Live=false) is re-probed
// with the daemon's own Health before the heal loop tears it down. A live or
// timed-out-but-peer-alive reading DEFERS (the holder's Live=false was a
// transient under-load false negative or mere slowness); a definitive dead
// reading, exhausted strikes, or a timeout with no live peer PROCEEDS (remount).
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

// TestHealFuseRowsLoopTicksAndExits pins the loop plumbing: ticks fire on the
// (shrunken) interval, the per-account heal actually runs from the loop and
// remounts the unvouched fuse row through the existing holder, and the goroutine
// exits on ctx cancellation.
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

// TestEvictionNeverDialsMountsSocket pins holder isolation on the daemon
// startup path: evicting a version-skewed DAEMON from the daemon socket —
// clean step-down or wedged-orphan kill — must never touch the mount-holder
// socket. The canned mounts listener tattles on any connection.
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
					return // listener closed: defined exit
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

// swapForceUnmount replaces the daemon's direct force-unmount seam for one
// test, restoring it after. Tests using it must not run in parallel.
func swapForceUnmount(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := forceUnmount
	forceUnmount = fn
	t.Cleanup(func() { forceUnmount = prev })
}

// driveRetryTicks runs n steady-state heal ticks against a settled holder,
// rewinding the per-row backoff window before each so every tick makes a real
// heal attempt (mirroring the heal cadence without sleeping).
func driveRetryTicks(t *testing.T, s *Server, id, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if st, ok := s.rowRetry[id]; ok {
			st.retryAt = time.Now().Add(-time.Second)
			s.rowRetry[id] = st
		}
		healTick(s, t.Context())
	}
}

// TestRemountBreakerThreshold pins the breaker const guard: a single transient
// mount failure must never escalate, so the threshold must be at least 2.
func TestRemountBreakerThreshold(t *testing.T) {
	if remountBreakerThreshold < 2 {
		t.Fatalf("remountBreakerThreshold = %d, want >= 2 so a single transient mount failure never escalates", remountBreakerThreshold)
	}
}

// TestRemountBreakerEscalates pins the wedged-mount circuit breaker: after
// exactly remountBreakerThreshold consecutive wedged remounts the breaker fires
// — force-unmounting the carcass, converting the row to symlink, dropping the
// ledger entry, dropping the holder-cache vouch, and surfacing it loudly.
func TestRemountBreakerEscalates(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy, vouches for nothing
	fake.setupErr = mountTimeoutChain()        // healRetry forever — the wedged shape
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

// TestRemountBreakerEscalatesUnderLiveSession pins that the breaker is UNGATED
// by live sessions: a never-recovering mount is a whole-machine hazard, not a
// healthy mount, so escalateWedgedRow force-unmounts the carcass and converts
// the row to symlink even with a live session on the dir (relaunch is the
// documented fix, exactly as the held-dead remount policy accepts). The
// idle/session gate belongs only to fallbackToSymlink on the genuine-mount path;
// a regression adding one here would reintroduce the freeze hazard the kill-9
// incident exposed — with every other breaker test running an idle scan, this is
// the only case that fails if it does.
func TestRemountBreakerEscalatesUnderLiveSession(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil) // healthy, vouches for nothing
	fake.setupErr = mountTimeoutChain()        // healRetry forever — the wedged shape
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })
	// A live session on the very dir the breaker is about to force down: a
	// session gate would consult it and bail; the breaker must not.
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

	driveRetryTicks(t, s, 1, remountBreakerThreshold)

	mu.Lock()
	gotUnmounted := append([]string(nil), unmounted...)
	mu.Unlock()
	if len(gotUnmounted) != 1 || gotUnmounted[0] != dirs[1] {
		t.Fatalf("breaker force-unmounted %v under a live session, want exactly [%s] (it must stay ungated)", gotUnmounted, dirs[1])
	}
	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after the breaker = %q, want symlink (a live session must not block escalation)", got)
	}
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("breaker left a rowRetry entry; the churn would continue")
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("breaker did not drop the holder-cache vouch for the converted dir")
	}
}

// TestRemountBreakerHoldsUnderThreshold pins that fewer than the threshold
// consecutive failures keep retrying — no escalation, the row stays fuse, and
// the ledger keeps counting.
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

// TestRemountBreakerResetsOnMount pins that a successful mount before the
// threshold clears the breaker's hazard count: a row that recovers never fires
// the breaker, and a later failure restarts the count from one.
func TestRemountBreakerResetsOnMount(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountTimeoutChain()
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	// A few wedged attempts short of the breaker…
	driveRetryTicks(t, s, 1, remountBreakerThreshold-2)
	if got := s.rowRetry[1].hazard; got != remountBreakerThreshold-2 {
		t.Fatalf("hazard before recovery = %d, want %d", got, remountBreakerThreshold-2)
	}

	// …then the mount comes up: the ledger entry is dropped entirely.
	fake.setupErr = nil
	driveRetryTicks(t, s, 1, 1)
	if _, ok := s.rowRetry[1]; ok {
		t.Fatal("a successful mount left a rowRetry entry")
	}
	if !s.holder.ready(dirs[1]) {
		t.Fatal("recovered row not vouched for")
	}

	// A fresh wedge starts the hazard count over from one — never near the
	// threshold — so the recovered row never carries stale breaker progress.
	fake.setupErr = mountTimeoutChain()
	driveRetryTicks(t, s, 1, 1)
	if got := s.rowRetry[1].hazard; got != 1 {
		t.Fatalf("hazard after a fresh failure = %d, want 1 (reset by the recovery)", got)
	}
	if unmounts != 0 {
		t.Fatalf("force-unmounts across a recovering row = %d, want 0", unmounts)
	}
}

// TestWedgeBreakerNeverEscalatesTCCRow pins the load-bearing exclusion: a
// TCC-blocked row is a CLEAN not-mounted state, never a kernel wedge, so it must
// never trip the WEDGED breaker (remountBreakerThreshold) — its hazard count
// stays 0 no matter how many consecutive TCC blocks accrue. The grant instead
// gets a bounded grace through the SEPARATE tccBreakerThreshold
// (TestTCCBreakerEscalates); here we drive right up to but not across that grace
// — already past the wedged threshold — to prove the wedge breaker stays silent.
func TestWedgeBreakerNeverEscalatesTCCRow(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive) // healTCCBlocked
	fakeOverlayMounted(t, func(dir string) bool { return dir == dirs[1] })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	// One short of the grant grace, and (since tccBreakerThreshold >
	// remountBreakerThreshold) already well past the wedged breaker's threshold —
	// which must NOT fire.
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

// TestTCCBreakerEscalates pins the bounded grant grace: after tccBreakerThreshold
// consecutive TCC-blocked heals the daemon stops waiting and retreats the row to
// symlink so the account is usable — dropping the ledger, the holder vouch, and
// the stale TCC guidance, and surfacing it loudly. A TCC-blocked dir never came
// up, so it is not a mountpoint and no force-unmount is needed.
func TestTCCBreakerEscalates(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive) // healTCCBlocked
	fakeOverlayMounted(t, func(string) bool { return false })        // never came up

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

// TestTCCBreakerEscalatesUnderLiveSession pins that the TCC grace breaker, like
// the wedged breaker, is UNGATED by live sessions: once the grace expires the row
// retreats to symlink even with a session on the dir (escalateRowToSymlink skips
// the idle gate; the mount never came up, so the retreat repairs the bare dir the
// session is on).
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

// TestTCCBreakerLateGrantPreventsFallback pins the desktop case: a grant that
// lands before the grace expires mounts the row and prevents the retreat — the
// row stays fuse, the ledger clears, and the TCC guidance clears.
func TestTCCBreakerLateGrantPreventsFallback(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = fmt.Errorf("mount: %w", overlay.ErrMountNotLive)
	fakeOverlayMounted(t, func(string) bool { return false })

	var unmounts int
	swapForceUnmount(t, func(string) error { unmounts++; return nil })

	// One short of the grace: still waiting, no retreat.
	driveRetryTicks(t, s, 1, tccBreakerThreshold-1)
	if got := kindOf(t, s, 1); got != "nfs" {
		t.Fatalf("row kind one short of the grace = %q, want fuse (still waiting on the grant)", got)
	}
	if got := s.rowRetry[1].tccBlocks; got != tccBreakerThreshold-1 {
		t.Fatalf("tccBlocks = %d, want %d (one short of the grace)", got, tccBreakerThreshold-1)
	}

	// The human grants Network Volumes: the next heal mounts the row.
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

// TestTCCBreakerThreshold pins the TCC grace const guard: a pending grant must
// get a LONGER grace than the wedged breaker (a benign wait, not a kernel
// hazard), so the threshold must exceed remountBreakerThreshold.
func TestTCCBreakerThreshold(t *testing.T) {
	if tccBreakerThreshold <= remountBreakerThreshold {
		t.Fatalf("tccBreakerThreshold = %d, want > remountBreakerThreshold (%d): a pending grant earns a longer grace than a kernel wedge", tccBreakerThreshold, remountBreakerThreshold)
	}
}

// TestHealFuseMountFailedRetreatsImmediately pins that a hard mount rejection
// (ErrMountFailed — fuse-t cannot mount on this machine) retreats to symlink on
// the FIRST heal, with no TCC wait and no breaker countdown: it is a dead-end,
// not a pending grant.
func TestHealFuseMountFailedRetreatsImmediately(t *testing.T) {
	s, dirs, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	s.holderSocket = startCannedHolder(t, nil)
	fake.setupErr = mountFailedChain()
	fakeOverlayMounted(t, func(string) bool { return false }) // never came up

	driveRetryTicks(t, s, 1, 1) // ONE tick

	if got := kindOf(t, s, 1); got != "symlink" {
		t.Fatalf("row kind after one hard-failure heal = %q, want symlink (immediate retreat, no TCC wait, no breaker countdown)", got)
	}
	if s.holder.ready(dirs[1]) {
		t.Fatal("hard-failure retreat did not drop the holder-cache vouch")
	}
}

// TestRetreatAllFuseRowsConvertsPoolToSymlink pins the whole-pool symlink retreat
// primitive: retreatAllFuseRows force-unmounts and converts every fuse row to the
// always-available symlink overlay, logged loudly. It is the shared retreat the
// startup capability gate drives when fuse is unusable for the whole pool.
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

// TestRetreatBailsOnWedgedForceUnmount pins the wedged-unmount guard shared by
// every symlink-retreat path (convertRowToSymlink): when the forced unmount never
// completes, the row is left fuse rather than handed to ConvertOverlay — whose
// Teardown would see the dir still mounted and re-spawn the very holder being
// retreated from (the wedged-carcass churn the kill-9 incident exposed).
func TestRetreatBailsOnWedgedForceUnmount(t *testing.T) {
	s, dirs, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	// The dir reads as a (wedged) mountpoint whose forced unmount never
	// completes — the exact edge where an un-guarded ConvertOverlay would
	// re-spawn the holder through its Teardown.
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

// hostFuseCapable makes canSpawnHolder()/pool.CanHostFuse() report that this
// machine can host fuse, so the startup capability gate is reached at all. The
// gate probes the shared DEFAULT holder socket (mountd.DefaultHolderSocket), not
// the daemon's injected s.holderSocket — so this stands a canned holder there,
// under a short HOME whose sun_path fits macOS's 104-byte cap. Without it the
// gate short-circuits before ever consulting the probe verdict on s.holderSocket.
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
// capability gate: when the holder is reachable, serves no live mount, and a
// capability probe is REJECTED OUTRIGHT (ErrMountFailed — fuse-t cannot mount
// here), reconcileOverlays retreats EVERY fuse row to symlink in one pass and
// records symlink as the new-account default — instead of churning a doomed mount
// per account.
func TestReconcileCapabilityGateRetreatsPoolWhenFuseUnavailable(t *testing.T) {
	s, _, _ := newHealServer(t)
	flipToFuse(t, s, 1)
	flipToFuse(t, s, 2)
	hostFuseCapable(t)
	// Holder reachable, no live mounts, probe hard-fails.
	s.holderSocket = startCapabilityHolder(t, nil, mountd.ClassMountFailed, "fuse-t not loadable")
	fakeOverlayMounted(t, func(string) bool { return false }) // nothing mounted
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

// TestReconcileCapabilityGateProceedsWhenProbePending pins that a probe merely
// PENDING the Network Volumes grant (ErrTCCDenied, not ErrMountFailed) does NOT
// trip the gate: the rows stay fuse for the per-account heal + bounded TCC grace
// (a desktop user may still grant), and the new-account default is NOT flipped.
func TestReconcileCapabilityGateProceedsWhenProbePending(t *testing.T) {
	s, _, fake := newHealServer(t)
	flipToFuse(t, s, 1)
	hostFuseCapable(t) // reach the gate so the PENDING probe verdict is actually consulted
	s.holderSocket = startCapabilityHolder(t, nil, mountd.ClassTCC, "grant pending")
	// Health fails so reconcileAccount heals rather than adopting; the heal then
	// TCC-blocks, leaving the row fuse within its grace.
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

// TestSweepOrphanMountpointsClearsNonRowCarcass pins the startup orphan sweep:
// a mountpoint under the accounts dir that no account row owns — a pre-row
// `ccp add` carcass whose holder died and whose add never finalized — is
// force-unmounted, while a mountpoint that IS a current row's dir is left for
// the row-driven reconcile/heal. Without this sweep nothing row-driven ever
// names the orphan dir, so the wedged carcass would linger (a whole-machine
// hazard).
func TestSweepOrphanMountpointsClearsNonRowCarcass(t *testing.T) {
	s, _, _ := newMigrateServer(t) // sets HOME to a temp dir, so AccountsDir() is hermetic
	if err := os.MkdirAll(pool.AccountsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	rowDir := filepath.Join(pool.AccountsDir(), "acct-01") // owned by a current row
	orphan := filepath.Join(pool.AccountsDir(), "acct-07") // a pre-row add carcass: no row
	for _, d := range []string{rowDir, orphan} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Both dirs read as mountpoints; only the orphan (no row) may be cleared.
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
