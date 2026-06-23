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

// fakeDaemon is a minimal stand-in for a running daemon holding the socket: it
// answers Health/Shutdown with a fixed version and, on OpShutdown, optionally
// stops listening (releasing the socket) to mimic a daemon stepping down. macOS
// caps sun_path at 104 bytes, so the socket lives under a short /tmp dir.
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

// newFlockedFakeDaemon is newFakeDaemon holding the lifetime flock on
// socket+".lock", like a real post-flock daemon; serve releases the lock just
// before the socket on a shutdown step-down.
func newFlockedFakeDaemon(t *testing.T, ver string, releaseOnStop bool) *fakeDaemon {
	return newFakeDaemonOpts(t, ver, releaseOnStop, true, 0)
}

// newFlockedFakeDaemonLateLockRelease is newFlockedFakeDaemon, but on shutdown
// it releases the socket FIRST and holds the flock for delay afterwards — the
// shape of a real dying daemon, whose listener closes at ctx-cancel time while
// the flock (serve's last defer) waits out the goroutine drain.
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
			return // listener closed: defined exit
		}
		var req Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			_ = conn.Close() // probe dial (e.g. WaitGone) with no request body
			continue
		}
		_ = json.NewEncoder(conn).Encode(Response{OK: true, Version: f.version})
		_ = conn.Close()
		if req.Op == OpShutdown && f.releaseOnStop {
			if f.lockDelay > 0 {
				// Socket first, flock later: a real dying daemon's listener
				// closes at ctx-cancel time, but its flock — serve's last
				// defer — is released only after the goroutine drain.
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
			_ = f.ln.Close() // release the socket so a successor can rebind
			return
		}
	}
}

func testServer(socket string, evict time.Duration) *Server {
	return &Server{
		socket:       socket,
		log:          log.New(io.Discard, "", 0),
		evictTimeout: evict,
	}
}

// TestListenRefusesSameVersionHolder pins the genuine-double-start guard: a live
// holder at our own version is refused, never evicted.
func TestListenRefusesSameVersionHolder(t *testing.T) {
	f := newFakeDaemon(t, version.String(), false)
	s := testServer(f.socket, time.Second)
	if _, _, err := s.listen(); err == nil || !strings.Contains(err.Error(), "same version") {
		t.Fatalf("listen against a same-version holder: err = %v, want a 'same version' refusal", err)
	}
}

// TestListenEvictsSkewedHolder pins the load-bearing fix: a version-skewed
// holder that steps down on OpShutdown is evicted and the successor binds.
func TestListenEvictsSkewedHolder(t *testing.T) {
	guardKillSocketPeer(t) // the holder releases on shutdown, but keep real signals off the table
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

// TestListenSkewedHolderIgnoresShutdown pins the bounded-wait failure: a skewed
// holder that acknowledges but never releases times out (rather than wedging),
// so the successor exits and launchd retries.
func TestListenSkewedHolderIgnoresShutdown(t *testing.T) {
	guardKillSocketPeer(t)                    // KillSocketPeer is reached here; the stub pins that no real process is signalled
	f := newFakeDaemon(t, "0.0.0-old", false) // acks OpShutdown but keeps listening
	s := testServer(f.socket, 500*time.Millisecond)
	if _, _, err := s.listen(); err == nil || !strings.Contains(err.Error(), "did not release") {
		t.Fatalf("listen against a holder that ignores shutdown: err = %v, want a 'did not release' timeout", err)
	}
}

// TestHandleShutdownEndsServe pins the OpShutdown op end-to-end: a real serve
// loop steps down on the request, returns, and releases the socket.
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
		m:               &pool.Manager{Store: st},
		socket:          filepath.Join(sockDir, "d.sock"),
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		evictTimeout:    defaultEvictTimeout,
		reservations:    map[int]time.Time{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
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

// TestWaitGone pins both arms: false while the socket is live, true once it is
// closed.
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

// guardKillSocketPeer swaps killSocketPeer for a no-op for the duration of a
// test, so a real daemon on the developer's machine can never be signalled by
// a listen()/evictPeer path.
func guardKillSocketPeer(t *testing.T) {
	t.Helper()
	setKillSocketPeer(t, func(string) (int, error) { return 0, nil })
}

// TestListenRefusedWhileLockHeld pins the flock that closes the start race: a
// daemon that cannot take socket+".lock" must refuse WITHOUT touching the
// socket path — its os.Remove on a believed-stale socket is exactly the
// hazard the lock exists to prevent (the loser would unlink the winner's
// freshly-bound socket, leaving an invisible daemon running its own scheduler
// and holder supervisor).
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
		// flock contends between two open file descriptions even in one
		// process, so holding it here stands in for a concurrently starting
		// daemon that won the lock but has not bound its socket yet.
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
		// The loser must not have unlinked the winner's live socket.
		c := &Client{socket: f.socket}
		if resp, err := c.Health(); err != nil || resp.Version != version.String() {
			t.Fatalf("winner's socket disturbed by the refused loser: resp=%+v err=%v", resp, err)
		}
	})
}

// TestListenEvictsFlockedSkewedDaemon pins the skewed-peer leg of the lock
// path: a version-skewed daemon HOLDING the flock (a flock-aware older build)
// is evicted, its death releases the lock, and the successor's one retry takes
// it and binds.
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
// real evicted daemon releases its socket at ctx-cancel time but its flock
// only at process exit, after its goroutine drain — for a freshly started
// evictee (the launchd KeepAlive race this path exists for) that drain
// includes seconds of non-cancellable startup work. The successor must poll
// the lock for the evict bound, not fail on the first post-evict attempt.
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

// TestListenEvictedPeerNeverReleasesFlock is the bounded-wait negative: an
// evicted peer that releases its socket but never its flock fails the start
// within the evict bound, loudly, instead of waiting forever.
func TestListenEvictedPeerNeverReleasesFlock(t *testing.T) {
	guardKillSocketPeer(t)
	f := newFlockedFakeDaemonLateLockRelease(t, "0.0.0-old", time.Hour)
	s := testServer(f.socket, 400*time.Millisecond)
	if _, _, err := s.listen(); !errors.Is(err, proc.ErrLockStillHeld) {
		t.Fatalf("listen with the peer's flock never released = %v, want proc.ErrLockStillHeld", err)
	}
}

// TestCrashedDaemonLockAndSocketReclaimed: a crashed daemon leaves both its
// lock file and its socket file behind. The flock died with the process, so a
// fresh daemon must reclaim both. The lock file is never unlinked — only
// re-flocked.
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

// TestEvictHolderKillsWedgedOrphan pins the new self-heal: a skewed holder that
// acks OpShutdown but never releases is hard-killed by its socket peer PID, and
// the successor then binds. killSocketPeer is stubbed so no real process is
// touched; the stubbed kill closes the fake's listener to model the orphan dying.
func TestEvictHolderKillsWedgedOrphan(t *testing.T) {
	f := newFakeDaemon(t, "0.0.0-old", false) // acks shutdown but never releases on its own
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
