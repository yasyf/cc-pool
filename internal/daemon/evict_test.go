package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/version"
)

// fakeDaemon stands in for a live daemon holding the socket. macOS caps
// sun_path at 104 bytes, so sockets live under short /tmp dirs.
type fakeDaemon struct {
	ln            net.Listener
	socket        string
	version       string
	releaseOnStop bool
	lock          *os.File      // optional flock on socket+".lock", released with the socket
	lockDelay     time.Duration // >0: on shutdown, release the socket first and the flock this much later
}

func newFakeDaemon(t *testing.T, ver string, releaseOnStop bool) *fakeDaemon {
	return newFakeDaemonOpts(t, ver, releaseOnStop, false, 0)
}

func newFlockedFakeDaemon(t *testing.T, ver string, releaseOnStop bool) *fakeDaemon {
	return newFakeDaemonOpts(t, ver, releaseOnStop, true, 0)
}

// newFlockedFakeDaemonLateLockRelease models a dying daemon: its listener
// closes at ctx-cancel; its flock, serve's last defer, frees delay later.
func newFlockedFakeDaemonLateLockRelease(t *testing.T, ver string, delay time.Duration) *fakeDaemon {
	return newFakeDaemonOpts(t, ver, true, true, delay)
}

func newFakeDaemonOpts(t *testing.T, ver string, releaseOnStop, flocked bool, lockDelay time.Duration) *fakeDaemon {
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
	f := &fakeDaemon{ln: ln, socket: socket, version: ver, releaseOnStop: releaseOnStop, lockDelay: lockDelay}
	if flocked {
		lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: socket is under the test's own t.TempDir()
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
			_ = conn.Close() // probe dial (e.g. WaitGone) with no request body
			continue
		}
		_ = json.NewEncoder(conn).Encode(Response{Proto: ProtocolVersion, OK: true, Version: f.version})
		_ = conn.Close()
		if req.Op == OpShutdown && f.releaseOnStop {
			if f.lockDelay > 0 {
				_ = f.ln.Close()
				time.Sleep(f.lockDelay)
				if f.lock != nil {
					_ = f.lock.Close()
				}
				return
			}
			// Lock before socket: by the time a successor's WaitGone observes
			// the socket gone, the flock is free.
			if f.lock != nil {
				_ = f.lock.Close()
			}
			_ = f.ln.Close()
			return
		}
	}
}

func testServer(socket string, evict time.Duration) *Server {
	return &Server{
		cl:           newClaims(),
		socket:       socket,
		log:          log.New(io.Discard, "", 0),
		evictTimeout: evict,
	}
}

// TestListenRefusesSameVersionHolder pins that a live same-version holder is
// refused, never evicted.
func TestListenRefusesSameVersionHolder(t *testing.T) {
	f := newFakeDaemon(t, version.String(), false)
	s := testServer(f.socket, time.Second)
	if _, _, err := s.listen(); err == nil || !strings.Contains(err.Error(), "same version") {
		t.Fatalf("listen against a same-version holder: err = %v, want a 'same version' refusal", err)
	}
}

func TestListenEvictsSkewedHolder(t *testing.T) {
	guardKillSocketPeer(t)
	f := newFakeDaemon(t, "0.0.0-old", true)
	s := testServer(f.socket, 3*time.Second)
	ln, lock, err := s.listen()
	if err != nil {
		t.Fatalf("listen should evict the skewed holder and bind, got err = %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
	if ln == nil {
		t.Fatal("listen returned a nil listener after eviction")
	}
}

// TestListenSkewedHolderIgnoresShutdown: a holder that acks but never releases
// times out rather than wedging, so the successor exits and launchd retries.
func TestListenSkewedHolderIgnoresShutdown(t *testing.T) {
	guardKillSocketPeer(t)
	f := newFakeDaemon(t, "0.0.0-old", false)
	s := testServer(f.socket, 500*time.Millisecond)
	if _, _, err := s.listen(); err == nil || !strings.Contains(err.Error(), "did not release") {
		t.Fatalf("listen against a holder that ignores shutdown: err = %v, want a 'did not release' timeout", err)
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

// guardKillSocketPeer swaps killSocketPeer for a no-op so a listen()/evictPeer
// path can never signal a real daemon on the developer's machine.
func guardKillSocketPeer(t *testing.T) {
	t.Helper()
	setKillSocketPeer(t, func(string) (int, error) { return 0, nil })
}

// TestListenRefusedWhileLockHeld: a flock loser must not touch the socket
// path — unlinking the winner's socket leaves an invisible daemon.
func TestListenRefusedWhileLockHeld(t *testing.T) {
	t.Run("mid-start peer (no health answer) refused", func(t *testing.T) {
		sockDir, err := os.MkdirTemp("/tmp", "ccp-lk")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
		socket := filepath.Join(sockDir, "d.sock")
		lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: socket is under the test's own t.TempDir()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lock.Close() }()
		// flock contends between open file descriptions even in one process:
		// this stands in for a mid-start daemon that has not bound its socket.
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			t.Fatal(err)
		}

		s := testServer(socket, time.Second)
		if _, _, err := s.listen(); !errors.Is(err, proc.ErrPeerStarting) {
			t.Fatalf("listen with the daemon lock held = %v, want proc.ErrPeerStarting", err)
		}
		if _, statErr := os.Stat(socket); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("a losing daemon must not create (or have removed) the socket; stat err = %v", statErr)
		}
	})

	t.Run("same-version flocked peer refused, socket untouched", func(t *testing.T) {
		guardKillSocketPeer(t)
		f := newFlockedFakeDaemon(t, version.String(), false)
		s := testServer(f.socket, time.Second)
		if _, _, err := s.listen(); err == nil || !strings.Contains(err.Error(), "same version") {
			t.Fatalf("listen against a same-version flocked peer = %v, want a 'same version' refusal", err)
		}
		c := &Client{socket: f.socket}
		if resp, err := c.Health(); err != nil || resp.Version != version.String() {
			t.Fatalf("winner's socket disturbed by the refused loser: resp=%+v err=%v", resp, err)
		}
	})
}

func TestListenEvictsFlockedSkewedDaemon(t *testing.T) {
	guardKillSocketPeer(t)
	f := newFlockedFakeDaemon(t, "0.0.0-old", true)
	s := testServer(f.socket, 3*time.Second)
	ln, lock, err := s.listen()
	if err != nil {
		t.Fatalf("listen should evict the flocked skewed daemon and bind: %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
}

// TestListenWaitsOutEvictedPeersFlockDrain pins the post-evict lock poll: a
// fresh evictee (launchd KeepAlive race) drains non-cancellable startup work
// before its flock frees, so the successor polls rather than failing fast.
func TestListenWaitsOutEvictedPeersFlockDrain(t *testing.T) {
	guardKillSocketPeer(t)
	f := newFlockedFakeDaemonLateLockRelease(t, "0.0.0-old", 400*time.Millisecond)
	s := testServer(f.socket, 2*time.Second)
	ln, lock, err := s.listen()
	if err != nil {
		t.Fatalf("listen must wait out the evicted peer's flock drain: %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
}

// TestListenEvictedPeerNeverReleasesFlock: a peer that frees its socket but
// never its flock must fail the start within the evict bound, not wait forever.
func TestListenEvictedPeerNeverReleasesFlock(t *testing.T) {
	guardKillSocketPeer(t)
	f := newFlockedFakeDaemonLateLockRelease(t, "0.0.0-old", time.Hour)
	s := testServer(f.socket, 400*time.Millisecond)
	if _, _, err := s.listen(); !errors.Is(err, proc.ErrLockStillHeld) {
		t.Fatalf("listen with the peer's flock never released = %v, want proc.ErrLockStillHeld", err)
	}
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
	ln2, lock, err := s.listen()
	if err != nil {
		t.Fatalf("listen over a crashed daemon's leavings: %v", err)
	}
	defer func() { _ = ln2.Close() }()
	defer func() { _ = lock.Close() }()
	if _, err := os.Stat(socket + ".lock"); err != nil {
		t.Fatalf("lock file must survive (never unlinked): %v", err)
	}
}

// TestEvictHolderKillsWedgedOrphan: a holder that acks OpShutdown but never
// releases is hard-killed via its socket peer PID.
func TestEvictHolderKillsWedgedOrphan(t *testing.T) {
	f := newFakeDaemon(t, "0.0.0-old", false)
	var gotSocket string
	setKillSocketPeer(t, func(socket string) (int, error) {
		gotSocket = socket
		_ = f.ln.Close() // the "kill" releases the socket so the successor can rebind
		return 999001, nil
	})

	s := testServer(f.socket, 3*time.Second)
	ln, lock, err := s.listen()
	if err != nil {
		t.Fatalf("listen should reap the wedged orphan and bind, got err = %v", err)
	}
	defer func() { _ = ln.Close() }()
	defer func() { _ = lock.Close() }()
	if gotSocket != f.socket {
		t.Fatalf("killSocketPeer got socket %q, want the held daemon socket %q", gotSocket, f.socket)
	}
}
