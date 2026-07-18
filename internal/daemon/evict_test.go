package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

const (
	takeoverChildSocketEnv = "CCP_TAKEOVER_TEST_SOCKET"
	takeoverChildReadyEnv  = "CCP_TAKEOVER_TEST_READY"
	takeoverChildEventsEnv = "CCP_TAKEOVER_TEST_EVENTS"
)

func TestTakeoverProtoSkewChild(t *testing.T) {
	socket := os.Getenv(takeoverChildSocketEnv)
	ready := os.Getenv(takeoverChildReadyEnv)
	events := os.Getenv(takeoverChildEventsEnv)
	if socket == "" || ready == "" || events == "" {
		t.Skip("child-only helper; driven by TestListenEvictsLiveProtoSkewedIncumbent")
	}

	lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: socket is under the parent test's temp dir.
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ready, []byte("1"), 0o600); err != nil { //nolint:gosec // G703: ready is under the parent test's temp dir.
		t.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		var req Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			_ = conn.Close()
			continue
		}
		if err := json.NewEncoder(conn).Encode(Response{Proto: ProtocolVersion + 1, OK: true, Version: "0.0.0-old"}); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		_ = conn.Close()
		f, err := os.OpenFile(events, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // G304: events is under the parent test's temp dir.
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintf(f, "%s %d\n", req.Op, time.Now().UnixNano()); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// fakeDaemon stands in for a live daemon holding the socket. macOS caps
// sun_path at 104 bytes, so sockets live under short /tmp dirs.
type fakeDaemon struct {
	ln            net.Listener
	socket        string
	version       string
	releaseOnStop bool
	lock          *os.File // optional flock on socket+".lock", released with the socket
	mu            sync.Mutex
	ops           []Op
}

func newFakeDaemon(t *testing.T, ver string, releaseOnStop bool) *fakeDaemon {
	return newFakeDaemonOpts(t, ver, releaseOnStop, false)
}

func newFlockedFakeDaemon(t *testing.T, ver string, releaseOnStop bool) *fakeDaemon {
	return newFakeDaemonOpts(t, ver, releaseOnStop, true)
}

func newFakeDaemonOpts(t *testing.T, ver string, releaseOnStop, flocked bool) *fakeDaemon {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-fake")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDaemon{ln: ln, socket: socket, version: ver, releaseOnStop: releaseOnStop}
	if flocked {
		lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: socket is under the test's own temp dir
		if err != nil {
			t.Fatal(err)
		}
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		f.lock = lock
		t.Cleanup(func() { _ = f.lock.Close() })
	}
	go f.serve()
	t.Cleanup(func() { _ = f.ln.Close() })
	return f
}

func (f *fakeDaemon) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		var req Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			_ = conn.Close() // probe dial (e.g. PeerPID getsockopt) with no request body
			continue
		}
		f.mu.Lock()
		f.ops = append(f.ops, req.Op)
		f.mu.Unlock()
		_ = json.NewEncoder(conn).Encode(Response{Proto: ProtocolVersion, OK: true, Version: f.version})
		_ = conn.Close()
		if req.Op == OpShutdown && f.releaseOnStop {
			// Lock before socket: by the time a successor observes the socket gone,
			// the flock is free.
			if f.lock != nil {
				_ = f.lock.Close()
			}
			_ = f.ln.Close()
			return
		}
	}
}

// received reports whether the fake ever decoded a request for op.
func (f *fakeDaemon) received(op Op) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.ops {
		if o == op {
			return true
		}
	}
	return false
}

func testServer(socket string, evict time.Duration) *Server {
	return &Server{
		cl:           newClaims(),
		socket:       socket,
		log:          log.New(io.Discard, "", 0),
		evictTimeout: evict,
	}
}

// TestListenStepsAsideForSameVersionHolder pins newer-wins for a tie: a live
// same-version holder makes the successor step aside (errStepAside), never
// evict, and its socket stays untouched. Real daemon.Run over the daemonPeer.
func TestListenStepsAsideForSameVersionHolder(t *testing.T) {
	f := newFakeDaemon(t, version.String(), false)
	setSocketPeerPID(t, func(string) (int, error) { return 999001, nil })
	s := testServer(f.socket, time.Second)
	if _, _, err := s.listen(t.Context()); !errors.Is(err, errStepAside) {
		t.Fatalf("same-version holder: err = %v, want errStepAside", err)
	}
	if resp, err := (&Client{socket: f.socket}).Health(); err != nil || resp.Version != version.String() {
		t.Fatalf("stepping aside disturbed the incumbent: resp=%+v err=%v", resp, err)
	}
}

// TestListenEvictsLiveProtoSkewedIncumbent drives listen through daemon.Run's
// complete RequestDaemon ladder against a separate socket-owning process. Both
// lifecycle replies carry another protocol: takeover must read the Health,
// receive the Shutdown ACK, wait the grace, re-read Health, revalidate the real
// child PID, and SIGKILL it before SingleEntrant binds.
func TestListenEvictsLiveProtoSkewedIncumbent(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ccp-takeover")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "d.sock")
	ready := filepath.Join(dir, "ready")
	events := filepath.Join(dir, "events")

	//nolint:gosec // G204: re-execs this test binary with a fixed child-only test selector.
	child := exec.Command(os.Args[0], "-test.run=^TestTakeoverProtoSkewChild$", "-test.v")
	child.Env = append(os.Environ(),
		takeoverChildSocketEnv+"="+socket,
		takeoverChildReadyEnv+"="+ready,
		takeoverChildEventsEnv+"="+events,
	)
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	reaped := false
	t.Cleanup(func() {
		if reaped {
			return
		}
		_ = child.Process.Kill()
		_ = child.Wait()
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("incumbent child never became ready; output:\n%s", output.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	const grace = 300 * time.Millisecond
	s := testServer(socket, grace)
	ln, lock, err := s.listen(t.Context())
	if err != nil {
		t.Fatalf("listen through live proto-skewed incumbent: %v; child output:\n%s", err, output.String())
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()

	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()
	var waitErr error
	select {
	case waitErr = <-waited:
		reaped = true
	case <-time.After(2 * time.Second):
		_ = child.Process.Kill()
		waitErr = <-waited
		reaped = true
		t.Fatalf("listen returned Bind while the incumbent child was still live; wait=%v", waitErr)
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("incumbent exit = %v, want SIGKILL", waitErr)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("incumbent exit status = %v, want SIGKILL; output:\n%s", exitErr.Sys(), output.String())
	}

	body, err := os.ReadFile(events) //nolint:gosec // G304: events is under this test's temp dir.
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 3 {
		t.Fatalf("takeover lifecycle events = %q, want health, shutdown ACK, health", body)
	}
	wantOps := []Op{OpHealth, OpShutdown, OpHealth}
	stamps := make([]int64, len(lines))
	for i, line := range lines {
		var op Op
		if _, err := fmt.Sscanf(line, "%s %d", &op, &stamps[i]); err != nil {
			t.Fatalf("parse takeover event %q: %v", line, err)
		}
		if op != wantOps[i] {
			t.Fatalf("takeover event %d = %s, want %s; all=%q", i, op, wantOps[i], body)
		}
	}
	if elapsed := time.Duration(stamps[2] - stamps[1]); elapsed < grace {
		t.Fatalf("second Health followed Shutdown ACK after %s, want at least grace %s", elapsed, grace)
	}
}

// TestListenBindsOnTakeoverBind pins the Bind outcome: the Evict closure returns
// (true, nil) and listen binds over the (uncontended) incumbent socket.
func TestListenBindsOnTakeoverBind(t *testing.T) {
	f := newFakeDaemon(t, "0.0.0-old", true)
	setRunTakeover(t, func(context.Context, daemon.TakeoverConfig) (daemon.Outcome, error) {
		return daemon.Bind, nil
	})
	s := testServer(f.socket, 3*time.Second)
	ln, lock, err := s.listen(t.Context())
	if err != nil {
		t.Fatalf("Bind outcome should evict and bind, got err = %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
	if ln == nil {
		t.Fatal("nil listener after a Bind outcome")
	}
}

// TestListenPropagatesTakeoverError: a takeover that errors (e.g. release
// timeout) fails the start; the successor exits and launchd retries.
func TestListenPropagatesTakeoverError(t *testing.T) {
	f := newFakeDaemon(t, "0.0.0-old", false)
	wantErr := errors.New("release timeout")
	setRunTakeover(t, func(context.Context, daemon.TakeoverConfig) (daemon.Outcome, error) {
		return 0, wantErr
	})
	s := testServer(f.socket, 500*time.Millisecond)
	if _, _, err := s.listen(t.Context()); !errors.Is(err, wantErr) {
		t.Fatalf("takeover error: listen err = %v, want %v", err, wantErr)
	}
}

// TestListenEvictsFlockedSkewedDaemon: a Bind outcome over a still-flocked
// incumbent polls the flock (evictee's drain outlives its socket) then binds.
func TestListenEvictsFlockedSkewedDaemon(t *testing.T) {
	f := newFlockedFakeDaemon(t, "0.0.0-old", false)
	setRunTakeover(t, func(context.Context, daemon.TakeoverConfig) (daemon.Outcome, error) {
		_ = f.lock.Close() // the takeover evicted the incumbent: release its flock
		_ = f.ln.Close()
		return daemon.Bind, nil
	})
	s := testServer(f.socket, 3*time.Second)
	ln, lock, err := s.listen(t.Context())
	if err != nil {
		t.Fatalf("Bind over a flocked incumbent should pollLock then bind: %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
}

// TestListenWaitsOutEvictedPeersFlockDrain pins the post-evict lock poll: a
// fresh evictee drains non-cancellable startup work before its flock frees, so
// the successor polls rather than failing fast.
func TestListenWaitsOutEvictedPeersFlockDrain(t *testing.T) {
	f := newFlockedFakeDaemon(t, "0.0.0-old", false)
	setRunTakeover(t, func(context.Context, daemon.TakeoverConfig) (daemon.Outcome, error) {
		_ = f.ln.Close()
		go func() { time.Sleep(400 * time.Millisecond); _ = f.lock.Close() }()
		return daemon.Bind, nil
	})
	s := testServer(f.socket, 2*time.Second)
	ln, lock, err := s.listen(t.Context())
	if err != nil {
		t.Fatalf("listen must wait out the evicted peer's flock drain: %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
}

// TestListenEvictedPeerNeverReleasesFlock: a peer that frees its socket but
// never its flock must fail the start within the evict bound, not wait forever.
func TestListenEvictedPeerNeverReleasesFlock(t *testing.T) {
	f := newFlockedFakeDaemon(t, "0.0.0-old", false)
	setRunTakeover(t, func(context.Context, daemon.TakeoverConfig) (daemon.Outcome, error) {
		_ = f.ln.Close() // socket released, flock deliberately kept
		return daemon.Bind, nil
	})
	s := testServer(f.socket, 400*time.Millisecond)
	if _, _, err := s.listen(t.Context()); !errors.Is(err, proc.ErrLockStillHeld) {
		t.Fatalf("flock never released: err = %v, want proc.ErrLockStillHeld", err)
	}
}

func TestHandleShutdownEndsServe(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	sockDir, err := os.MkdirTemp("/tmp", "ccp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	s := &Server{
		m:            &pool.Manager{Store: st},
		socket:       filepath.Join(sockDir, "d.sock"),
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		evictTimeout: defaultEvictTimeout,
		cl:           newClaims(),
		led:          newLedgers(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { err := s.serve(ctx); _ = st.Close(); done <- err }()

	cl := &Client{socket: s.socket}
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("daemon socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}

	resp, err := cl.Shutdown()
	if err != nil || !resp.OK {
		t.Fatalf("shutdown: resp = %+v, err = %v", resp, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after OpShutdown")
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after shutdown")
	}
}

func TestWaitGone(t *testing.T) {
	sockDir, err := os.MkdirTemp("/tmp", "ccp-wg")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "d.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	cl := &Client{socket: socket}

	if cl.WaitGone(300 * time.Millisecond) {
		t.Fatal("WaitGone reported gone while the socket is live")
	}
	_ = ln.Close()
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("WaitGone did not report gone after the socket was closed")
	}
}

// TestListenRefusedWhileLockHeld: a flock loser must not touch the socket path —
// unlinking the winner's socket leaves an invisible daemon.
func TestListenRefusedWhileLockHeld(t *testing.T) {
	t.Run("mid-start peer (no health answer) waits out the flock", func(t *testing.T) {
		sockDir, err := os.MkdirTemp("/tmp", "ccp-lk")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
		socket := filepath.Join(sockDir, "d.sock")
		lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: socket is under the test's own temp dir
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Close() }()
		// A held flock with no bound socket: a mid-start daemon. The takeover finds
		// no socket to probe (Bind), so SingleEntrant polls the held flock.
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			t.Fatal(err)
		}

		s := testServer(socket, 300*time.Millisecond)
		if _, _, err := s.listen(t.Context()); !errors.Is(err, proc.ErrLockStillHeld) {
			t.Fatalf("listen with the daemon lock held = %v, want proc.ErrLockStillHeld", err)
		}
		if _, statErr := os.Stat(socket); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("a losing daemon must not create (or have removed) the socket; stat err = %v", statErr)
		}
	})

	t.Run("same-version flocked peer steps aside, socket untouched", func(t *testing.T) {
		f := newFlockedFakeDaemon(t, version.String(), false)
		setSocketPeerPID(t, func(string) (int, error) { return 999001, nil })
		s := testServer(f.socket, time.Second)
		if _, _, err := s.listen(t.Context()); !errors.Is(err, errStepAside) {
			t.Fatalf("same-version flocked peer = %v, want errStepAside", err)
		}
		c := &Client{socket: f.socket}
		if resp, err := c.Health(); err != nil || resp.Version != version.String() {
			t.Fatalf("winner's socket disturbed by the step-aside loser: resp=%+v err=%v", resp, err)
		}
	})
}

// TestCrashedDaemonLockAndSocketReclaimed: the flock died with the crashed
// process, so a fresh daemon reclaims the leftover lock file and socket.
func TestCrashedDaemonLockAndSocketReclaimed(t *testing.T) {
	sockDir, err := os.MkdirTemp("/tmp", "ccp-crash")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "d.sock")
	if err := os.WriteFile(socket+".lock", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	s := testServer(socket, time.Second)
	ln2, lock, err := s.listen(t.Context())
	if err != nil {
		t.Fatalf("listen over a crashed daemon's leavings: %v", err)
	}
	defer func() { _ = ln2.Close() }()
	defer func() { _ = lock.Close() }()
	if _, err := os.Stat(socket + ".lock"); err != nil {
		t.Fatalf("lock file must survive (never unlinked): %v", err)
	}
}
