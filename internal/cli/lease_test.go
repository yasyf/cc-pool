package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
	"golang.org/x/sys/unix"
)

// tempLeaseRoot points the lease root at a temp dir, never ~/.fusekit.
func tempLeaseRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "leases")
	swapVar(t, &leaseRoot, func() (string, error) { return root, nil })
	return root
}

// tempAgentDir points the lease-agent registry dir at a temp dir, never ~/.cc-pool.
func tempAgentDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "lease-agents")
	swapVar(t, &leaseAgentDir, func() (string, error) { return dir, nil })
	return dir
}

// TestAcquireSessionLease pins the run/login handout path: acquireSessionLease
// holds the lease on the account's SessionLeaseDir and releasing frees it.
func TestAcquireSessionLease(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := tempLeaseRoot(t)
	a := store.Account{ID: 3, ConfigDir: pool.AccountDir(3), OverlayKind: "symlink"}
	dir := pool.SessionLeaseDir(a)

	h, err := acquireSessionLease(a)
	if err != nil {
		t.Fatalf("acquireSessionLease: %v", err)
	}
	if held, _, _ := lease.Probe(root, dir); !held {
		t.Fatalf("lease on %s not held after Acquire", dir)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatalf("lease on %s still held after Close", dir)
	}
}

// TestProbeLeasedDir pins the post-Acquire probe. A live dir with no probe file is
// benign for a SYMLINK/FP row (missing = no verdict) but FATAL for a FUSE row (a
// fuse mount must serve the probe file — a bare unmounted dir is refused, G4). An
// absent dir and a PARTIAL wedge both abort loudly, pointing at ccp doctor.
func TestProbeLeasedDir(t *testing.T) {
	live := t.TempDir()
	if err := probeLeasedDir(live, false); err != nil {
		t.Fatalf("probeLeasedDir(symlink row, no probe file) = %v, want nil", err)
	}
	switch err := probeLeasedDir(live, true); {
	case err == nil, !errors.Is(err, overlay.ErrProbeMissing), !strings.Contains(err.Error(), "ccp doctor"):
		t.Fatalf("probeLeasedDir(fuse row, no probe file) = %v, want an ErrProbeMissing abort naming ccp doctor", err)
	}

	if err := probeLeasedDir(filepath.Join(t.TempDir(), "gone"), false); err == nil || !strings.Contains(err.Error(), "ccp doctor") {
		t.Fatalf("probeLeasedDir on an absent dir = %v, want an abort naming ccp doctor", err)
	}

	wedged := t.TempDir()
	// A short probe file stands in for the documented partial wedge: metadata
	// answers but the sequential read returns fewer than ProbeFileSize bytes.
	if err := os.WriteFile(filepath.Join(wedged, overlay.ProbeFileName), []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	switch err := probeLeasedDir(wedged, false); {
	case err == nil, !errors.Is(err, overlay.ErrProbeWedged), !strings.Contains(err.Error(), "ccp doctor"):
		t.Fatalf("probeLeasedDir on a short probe file = %v, want an ErrProbeWedged abort naming ccp doctor", err)
	}
}

// TestProbeSessionLease pins that the post-Acquire probe runs on the dir claude
// reads (the account ConfigDir) and is row-shape-aware: a symlink dir with no probe
// passes, an absent mount aborts, and an unmounted legacy fuse row (a bare dir with
// no probe file) is REFUSED (G4).
func TestProbeSessionLease(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	present := store.Account{ID: 1, ConfigDir: pool.AccountDir(1), OverlayKind: "symlink"}
	if err := os.MkdirAll(present.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := probeSessionLease(present); err != nil {
		t.Fatalf("probeSessionLease on an existing symlink dir = %v, want nil", err)
	}

	absent := store.Account{ID: 2, ConfigDir: pool.AccountDir(2), OverlayKind: "nfs"}
	if err := probeSessionLease(absent); err == nil || !strings.Contains(err.Error(), "ccp doctor") {
		t.Fatalf("probeSessionLease on an absent mount = %v, want an abort naming ccp doctor", err)
	}

	bareFuse := store.Account{ID: 3, ConfigDir: pool.AccountDir(3), OverlayKind: "nfs"}
	if err := os.MkdirAll(bareFuse.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	switch err := probeSessionLease(bareFuse); {
	case err == nil, !errors.Is(err, overlay.ErrProbeMissing):
		t.Fatalf("probeSessionLease on an unmounted legacy fuse dir = %v, want an ErrProbeMissing refusal", err)
	}
}

type fakeWaiter struct {
	waited, closed bool
	drainQueued    bool
	drainErr       error
}

func (w *fakeWaiter) Wait() error          { w.waited = true; return nil }
func (w *fakeWaiter) Close() error         { w.closed = true; return nil }
func (w *fakeWaiter) Drain() (bool, error) { return w.drainQueued, w.drainErr }

// readReady drains the readiness pipe the agent wrote and closed.
func readReady(t *testing.T, r *os.File) string {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read readiness pipe: %v", err)
	}
	return strings.TrimSpace(string(b))
}

// TestRunLeaseAgentFailsOnLeaderGoneAtRegister pins G7: a leader already gone when
// the agent installs its exit watch never gets an ok whose lease would release the
// instant we begin watching — the agent fails loud over the readiness pipe and
// releases the lease.
func TestRunLeaseAgentFailsOnLeaderGoneAtRegister(t *testing.T) {
	root := tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &procStartTime, func(int) (int64, error) { return 123, nil })
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return nil, ErrNoProc })

	r, w, _ := os.Pipe()
	dir := "/pool/acct-01"
	err := runLeaseAgent(4242, 123, 1, dir, "", false, w)
	if err == nil || !strings.Contains(err.Error(), "exited before the lease was ready") {
		t.Fatalf("runLeaseAgent on an already-gone leader = %v, want a leader-gone failure", err)
	}
	if got := readReady(t, r); !strings.HasPrefix(got, "err:") {
		t.Fatalf("readiness = %q, want an err: signal (no ok before the exit watch is installed)", got)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatal("lease still held after ESRCH-at-register; the agent must release and exit")
	}
}

// TestRunLeaseAgentFailsOnPidReuse pins G7's revalidation: the leader's start-time
// captured by the launcher (100) no longer matches after the watch is installed
// (999), so the agent fails loud rather than signaling ok and pinning a stranger's
// pid.
func TestRunLeaseAgentFailsOnPidReuse(t *testing.T) {
	root := tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &procStartTime, func(int) (int64, error) { return 999, nil })
	waiter := &fakeWaiter{}
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return waiter, nil })

	r, w, _ := os.Pipe()
	dir := "/pool/acct-02"
	err := runLeaseAgent(4242, 100, 2, dir, "", false, w)
	if err == nil || !strings.Contains(err.Error(), "reused") {
		t.Fatalf("runLeaseAgent on a reused pid = %v, want a pid-reuse failure", err)
	}
	if got := readReady(t, r); !strings.HasPrefix(got, "err:") {
		t.Fatalf("readiness = %q, want an err: signal", got)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatal("lease still held after a pid-reuse mismatch; the agent must release")
	}
	if waiter.waited {
		t.Fatal("agent waited on a reused pid instead of releasing")
	}
	if !waiter.closed {
		t.Fatal("agent leaked the exit watch on a pid-reuse release")
	}
}

// TestRunLeaseAgentKqueueQueuedDeathFailsBeforeOk pins J2's kqueue leg: a leader whose
// NOTE_EXIT is ALREADY queued on the installed watch by drain time — a still-observable
// zombie whose start-time passes revalidation (100 == wantStart) — yields err, not ok,
// and releases the lease; ok never precedes a knowable death on the kqueue path either.
func TestRunLeaseAgentKqueueQueuedDeathFailsBeforeOk(t *testing.T) {
	root := tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &procStartTime, func(int) (int64, error) { return 100, nil }) // zombie: start-time still matches
	waiter := &fakeWaiter{drainQueued: true}
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return waiter, nil })

	r, w, _ := os.Pipe()
	dir := "/pool/acct-kqueue-queued"
	err := runLeaseAgent(4242, 100, 1, dir, "", false, w)
	if err == nil || !strings.Contains(err.Error(), "exited before the lease was ready") {
		t.Fatalf("kqueue path with a queued death = %v, want a leader-gone failure", err)
	}
	if got := readReady(t, r); !strings.HasPrefix(got, "err:") {
		t.Fatalf("readiness = %q, want an err: signal (no ok when NOTE_EXIT is already queued)", got)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatal("lease still held after a queued-death failure; the agent must release")
	}
	if waiter.waited {
		t.Fatal("agent waited on an already-dead leader instead of releasing")
	}
	if !waiter.closed {
		t.Fatal("agent leaked the exit watch on a queued-death release")
	}
}

// TestRunLeaseAgentHoldsUntilExit pins the happy path: a matching start-time after
// the watch is installed means the pid is the leader the launcher meant, so the
// agent writes its advisory slot, signals ok, and waits on the leader's exit.
func TestRunLeaseAgentHoldsUntilExit(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &procStartTime, func(int) (int64, error) { return 100, nil })
	waiter := &fakeWaiter{}
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return waiter, nil })

	r, w, _ := os.Pipe()
	if err := runLeaseAgent(4242, 100, 3, "/pool/acct-03", "", false, w); err != nil {
		t.Fatalf("runLeaseAgent = %v, want nil", err)
	}
	if got := readReady(t, r); got != "ok" {
		t.Fatalf("readiness = %q, want ok", got)
	}
	if !waiter.waited {
		t.Fatal("agent did not wait on the leader's exit for a matching pid")
	}
}

// TestRunLeaseAgentHandshakeFailure pins the no-handout contract: a post-acquire
// probe failure is signaled over the readiness pipe (err, not ok) and returned, and
// the lease is released so nothing is handed out unprotected.
func TestRunLeaseAgentHandshakeFailure(t *testing.T) {
	root := tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &procStartTime, func(int) (int64, error) { return 100, nil })

	r, w, _ := os.Pipe()
	dir := "/pool/acct-04"
	probe := filepath.Join(t.TempDir(), "wedged-and-absent")
	err := runLeaseAgent(4242, 100, 4, dir, probe, false, w)
	if err == nil || !strings.Contains(err.Error(), "ccp doctor") {
		t.Fatalf("runLeaseAgent with a failing probe = %v, want a probe-abort error", err)
	}
	if got := readReady(t, r); !strings.HasPrefix(got, "err:") {
		t.Fatalf("readiness = %q, want an err: signal", got)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatal("lease still held after a probe failure; the agent must release")
	}
}

// TestSlotAdvisorySemantics pins the advisory-slot contract: a live+ready+identity-
// matched slot lets a spawner SKIP (slotCovered true), while ANY doubt — absent, stale
// (no live flock), or still initializing (no ready marker) — reads false so the spawner
// forks a harmless duplicate. removeOwnSlot removes only the agent's own slot.
func TestSlotAdvisorySemantics(t *testing.T) {
	tempAgentDir(t)
	const leader, start = 4242, int64(100)
	key := "/pool/acct-advisory"

	if slotCovered(leader, start, key) {
		t.Fatal("slotCovered on an absent slot = true, want false (spawn on doubt)")
	}

	// A live, ready slot ⇒ skip. writeAdvisorySlot holds the flock in THIS process, so a
	// second open's LOCK_EX conflicts (per-open-description) — the live-holder signal.
	slot := writeAdvisorySlot(leader, start, key)
	if slot == nil {
		t.Fatal("writeAdvisorySlot returned nil for a writable dir")
	}
	if !slotCovered(leader, start, key) {
		t.Fatal("slotCovered on a live+ready slot = false, want true (skip the fork)")
	}

	removeOwnSlot(slot) // removes only its own slot ⇒ absent again ⇒ doubt
	if slotCovered(leader, start, key) {
		t.Fatal("slotCovered after removeOwnSlot = true, want false (own slot removed)")
	}

	agentDir, err := leaseAgentDir()
	if err != nil {
		t.Fatal(err)
	}
	slotPath := filepath.Join(agentDir, leaseAgentSlotKey(leader, start, key))

	// A stale ready file with NO live holder ⇒ doubt (a crashed agent left it behind).
	if err := os.WriteFile(slotPath, []byte(slotReadyMarker+" 999 100 "+key), 0o600); err != nil {
		t.Fatal(err)
	}
	if slotCovered(leader, start, key) {
		t.Fatal("slotCovered on a stale (unheld) ready file = true, want false")
	}
	_ = os.Remove(slotPath)

	// A held-but-INITIALIZING slot (flock held, no ready marker) ⇒ doubt.
	initing, err := os.OpenFile(slotPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = initing.Close() }()
	if err := syscall.Flock(int(initing.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := initing.WriteString("initializing"); err != nil {
		t.Fatal(err)
	}
	if slotCovered(leader, start, key) {
		t.Fatal("slotCovered on a held slot without the ready marker = true, want false (still initializing)")
	}
}

// TestSpawnLeaseAgentIdempotentSkip pins the spawner fast path: spawnLeaseAgent
// resolves the SESSION LEADER (not getppid) and skips the fork entirely when a live
// READY advisory slot already covers that leader+key.
func TestSpawnLeaseAgentIdempotentSkip(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempLeaseRoot(t)
	tempAgentDir(t)
	const leader = 4242
	const start = int64(777)
	swapVar(t, &procSessionLeader, func() (int, error) { return leader, nil })
	swapVar(t, &procStartTime, func(int) (int64, error) { return start, nil })

	a := store.Account{ID: 6, ConfigDir: pool.AccountDir(6), OverlayKind: "symlink"}
	// A live READY agent already advertises this session+key: writeAdvisorySlot holds
	// the slot flock in THIS process for the test's duration.
	slot := writeAdvisorySlot(leader, start, pool.SessionLeaseDir(a))
	if slot == nil {
		t.Fatal("writeAdvisorySlot returned nil")
	}
	defer removeOwnSlot(slot)

	// The slot is covered, so spawnLeaseAgent must be a no-op (never fork a second
	// agent, never block on a readiness handshake that would never come).
	if err := spawnLeaseAgent(a); err != nil {
		t.Fatalf("spawnLeaseAgent with a covered slot = %v, want nil (idempotent skip)", err)
	}
}

// TestSpawnLeaseAgentNoLeaderFailsClosed pins G8: a degenerate session leader (pid
// <= 1) has nothing to tie a lease to, so the spawner FAILS CLOSED rather than
// handing out an unprotected dir.
func TestSpawnLeaseAgentNoLeaderFailsClosed(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	swapVar(t, &procSessionLeader, func() (int, error) { return 1, nil })
	a := store.Account{ID: 7, ConfigDir: pool.AccountDir(7), OverlayKind: "symlink"}
	if err := spawnLeaseAgent(a); err == nil || !strings.Contains(err.Error(), "session leader") {
		t.Fatalf("spawnLeaseAgent with no session leader = %v, want a fail-closed error", err)
	}
}

// TestSpawnLeaseAgentDeadLeaderFailsClosed pins G8's second leg: a session leader
// whose pid is already gone (ErrNoProc) also fails closed.
func TestSpawnLeaseAgentDeadLeaderFailsClosed(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempAgentDir(t)
	swapVar(t, &procSessionLeader, func() (int, error) { return 4242, nil })
	swapVar(t, &procStartTime, func(int) (int64, error) { return 0, ErrNoProc })
	a := store.Account{ID: 8, ConfigDir: pool.AccountDir(8), OverlayKind: "symlink"}
	if err := spawnLeaseAgent(a); err == nil || !strings.Contains(err.Error(), "is gone") {
		t.Fatalf("spawnLeaseAgent with a dead session leader = %v, want a fail-closed error", err)
	}
}

// TestAwaitAgentReady pins the launcher-side handshake: ok proceeds, err: fails
// with the message, and an EOF before ok (the agent died) fails.
func TestAwaitAgentReady(t *testing.T) {
	cases := map[string]struct {
		write   string
		wantErr string
	}{
		"ok proceeds":            {"ok\n", ""},
		"err surfaces the cause": {"err: mount wedged\n", "mount wedged"},
		"eof before ok fails":    {"", "exited before acquiring"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, w, _ := os.Pipe()
			if tc.write != "" {
				_, _ = io.WriteString(w, tc.write)
			}
			_ = w.Close()
			err := awaitAgentReady(r, nil)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("awaitAgentReady = %v, want nil", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("awaitAgentReady = %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestAwaitAgentReadyTimeoutKillsChild pins G10: when a spawned agent never reports
// readiness, the launcher's deadline kills and reaps the child, then fails loud. The
// child's advisory slot (if any) needs no cleanup: a dead child's flock releases, so a
// later spawner reads it stale and simply spawns anew.
func TestAwaitAgentReadyTimeoutKillsChild(t *testing.T) {
	swapVar(t, &leaseReadyTimeout, 150*time.Millisecond)

	// A child that never signals readiness (sleeps well past the deadline).
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatalf("start stub child: %v", err)
	}

	r, w, _ := os.Pipe()
	defer func() { _ = w.Close() }() // stays open: no readiness, no EOF → the deadline fires
	defer func() { _ = r.Close() }()

	err := awaitAgentReady(r, child)
	if err == nil || !strings.Contains(err.Error(), "did not become ready") {
		t.Fatalf("awaitAgentReady on a stuck child = %v, want a timeout error", err)
	}
	if child.ProcessState == nil {
		t.Fatal("child was not reaped on timeout (ProcessState nil); the deadline must kill+wait it")
	}
	if child.ProcessState.Success() {
		t.Fatal("child exited successfully; the timeout must have killed it")
	}
}

// TestAcquireStableReDerivesOnShapeFlip pins H1: when the row shape flips between the
// initial derive and Acquire (a legacy→mux migration lands mid-acquire), the helper
// releases the obsolete key and lands the held lease on the NEW key.
func TestAcquireStableReDerivesOnShapeFlip(t *testing.T) {
	root := tempLeaseRoot(t)
	key1, key2 := "/pool/acct-01-legacy", "/pool/acct-01-mux"
	var calls int
	derive := func() string {
		calls++
		if calls == 1 {
			return key1 // the initial, pre-migration derive
		}
		return key2 // migration landed: every re-derive returns the mux subtree key
	}
	h, key, err := acquireStable(pool.HolderOwner, derive)
	if err != nil {
		t.Fatalf("acquireStable = %v, want nil", err)
	}
	defer func() { _ = h.Close() }()
	if key != key2 {
		t.Fatalf("acquireStable landed on %q, want the post-migration key %q", key, key2)
	}
	if held, _, _ := lease.Probe(root, key2); !held {
		t.Fatal("lease not held on the post-migration key")
	}
	if held, _, _ := lease.Probe(root, key1); held {
		t.Fatal("lease still held on the obsolete pre-migration key")
	}
}

// TestAcquireStableFailsWhenShapeNeverStable pins H1's bound: a shape that keeps
// flipping past the attempt bound fails loud rather than looping forever.
func TestAcquireStableFailsWhenShapeNeverStable(t *testing.T) {
	root := tempLeaseRoot(t)
	var calls int
	derive := func() string {
		calls++
		return fmt.Sprintf("/pool/acct-%d", calls) // a new key every call: never stable
	}
	h, key, err := acquireStable(pool.HolderOwner, derive)
	if err == nil {
		_ = h.Close()
		t.Fatal("acquireStable on an ever-changing shape = nil, want a fail-loud error")
	}
	if !strings.Contains(err.Error(), "kept changing") {
		t.Fatalf("acquireStable error = %v, want a 'kept changing' failure", err)
	}
	if held, _, _ := lease.Probe(root, key); held {
		t.Fatal("acquireStable left a lease held on its final failed key")
	}
}

// TestRunLeaseAgentPollFallbackLeaderDiesPreOk pins H4: on the kqueue-unavailable
// (EMFILE/ENFILE) fallback, a leader already gone at the poll watcher's first check
// yields err — not ok — and the lease releases; ok never precedes a live watcher.
func TestRunLeaseAgentPollFallbackLeaderDiesPreOk(t *testing.T) {
	root := tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return nil, errors.New("too many open files") })
	swapVar(t, &procStartTime, func(int) (int64, error) { return 0, ErrNoProc })

	r, w, _ := os.Pipe()
	dir := "/pool/acct-poll-dead"
	err := runLeaseAgent(4242, 100, 1, dir, "", false, w)
	if err == nil || !strings.Contains(err.Error(), "exited before the lease was ready") {
		t.Fatalf("poll-fallback with a dead leader = %v, want a leader-gone failure", err)
	}
	if got := readReady(t, r); !strings.HasPrefix(got, "err:") {
		t.Fatalf("readiness = %q, want an err: signal (no ok before the poll watcher confirms the leader)", got)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatal("lease still held after a poll-fallback dead-leader failure; the agent must release")
	}
}

// TestRunLeaseAgentPollFallbackQueuedDeathFailsBeforeOk pins J2: on the poll fallback,
// a leader that is alive at the watcher's first check but whose death is ALREADY queued
// on exited by drain time yields err — not ok — and the lease releases; ok never
// precedes a knowable death.
func TestRunLeaseAgentPollFallbackQueuedDeathFailsBeforeOk(t *testing.T) {
	root := tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return nil, errors.New("too many open files") })
	// Alive for the watcher's first check (started<-nil), but the poll loop's kill(0)
	// reports the leader gone, so the death is queued on exited before ok.
	swapVar(t, &procStartTime, func(int) (int64, error) { return 100, nil })
	swapVar(t, &procKill, func(int, syscall.Signal) error { return syscall.ESRCH })
	// Make the queued death deterministically observable at the drain.
	swapVar(t, &pollDrainHook, func(exited <-chan error) {
		for len(exited) == 0 {
			runtime.Gosched()
		}
	})

	r, w, _ := os.Pipe()
	dir := "/pool/acct-poll-queued"
	err := runLeaseAgent(4242, 100, 1, dir, "", false, w)
	if err == nil || !strings.Contains(err.Error(), "exited before the lease was ready") {
		t.Fatalf("poll-fallback with a queued death = %v, want a leader-gone failure", err)
	}
	if got := readReady(t, r); !strings.HasPrefix(got, "err:") {
		t.Fatalf("readiness = %q, want an err: signal (no ok when the death is already queued)", got)
	}
	if held, _, _ := lease.Probe(root, dir); held {
		t.Fatal("lease still held after a queued-death failure; the agent must release")
	}
}

// TestRunLeaseAgentPollFallbackHoldsThenExits pins the H4 happy leg: the poll watcher
// starts, passes its first check with NO queued death, ok is signaled, and a leader
// that then goes away makes the agent exit cleanly.
func TestRunLeaseAgentPollFallbackHoldsThenExits(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	tempLeaseRoot(t)
	tempAgentDir(t)
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return nil, errors.New("too many open files") })
	var mu sync.Mutex
	alive := true
	swapVar(t, &procStartTime, func(int) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		if alive {
			return 100, nil
		}
		return 0, ErrNoProc
	})
	// Alive keeps the poll watcher sleeping (no queued death at the drain), so ok is
	// signaled; flipping it dead makes the next poll return and the agent exit.
	swapVar(t, &procKill, func(int, syscall.Signal) error {
		mu.Lock()
		defer mu.Unlock()
		if alive {
			return nil
		}
		return syscall.ESRCH
	})

	r, w, _ := os.Pipe()
	done := make(chan error, 1)
	go func() { done <- runLeaseAgent(4242, 100, 3, "/pool/acct-poll-live", "", false, w) }()
	if got := readReady(t, r); got != "ok" {
		t.Fatalf("readiness = %q, want ok (watcher live and no queued death before ok)", got)
	}
	mu.Lock()
	alive = false // the leader exits; the next poll returns and the agent exits cleanly
	mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("poll-fallback happy path = %v, want nil after the leader exits", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not exit after the leader went away")
	}
}

// TestRunLeaseAgentAdvisorySlotBestEffort pins the advisory-slot contract from the
// agent side: a slot the agent cannot write (an unwritable registry dir) NEVER fails
// the launch — the agent still signals ok and holds the lease, because the slot is an
// optimization, not a correctness mechanism.
func TestRunLeaseAgentAdvisorySlotBestEffort(t *testing.T) {
	tempLeaseRoot(t)
	// Point the registry dir at a path whose parent is a FILE, so MkdirAll fails and
	// no slot can be written.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	swapVar(t, &leaseAgentDir, func() (string, error) { return filepath.Join(blocker, "agents"), nil })
	swapVar(t, &procStartTime, func(int) (int64, error) { return 100, nil })
	waiter := &fakeWaiter{}
	swapVar(t, &registerProcExit, func(int) (procWaiter, error) { return waiter, nil })

	r, w, _ := os.Pipe()
	if err := runLeaseAgent(4242, 100, 5, "/pool/acct-noslot", "", false, w); err != nil {
		t.Fatalf("runLeaseAgent with an unwritable slot dir = %v, want nil (slot is best-effort)", err)
	}
	if got := readReady(t, r); got != "ok" {
		t.Fatalf("readiness = %q, want ok (a missing advisory slot never fails the launch)", got)
	}
	if !waiter.waited {
		t.Fatal("agent did not wait on the leader; a best-effort slot must not abort the watch")
	}
}

// TestLeaseReadyTimeoutExceedsSequentialWorstCase pins J3: the readiness deadline must
// exceed the agent's FULL sequential worst case — leaseKeyDeriveAttempts rounds of
// Acquire plus the stat and deep probe — so a healthy-but-slow init (even one that
// re-derives across a migration) is never killed.
func TestLeaseReadyTimeoutExceedsSequentialWorstCase(t *testing.T) {
	worst := leaseKeyDeriveAttempts*leaseAcquireBound + leaseProbeTimeout + overlay.DeepProbeBound
	if leaseReadyTimeout <= worst {
		t.Fatalf("leaseReadyTimeout %s must exceed the full sequential worst case %s (deriveAttempts×acquire + stat + deep-probe)", leaseReadyTimeout, worst)
	}
}

// TestSweepInheritedFDsExcept pins J6: a lease agent spawned with an extra inherited
// non-CLOEXEC fd drops it (fd 4 here) while keeping its fd-3 readiness pipe. It runs in
// a helper subprocess so the sweep operates on a controlled fd table.
func TestSweepInheritedFDsExcept(t *testing.T) {
	if os.Getenv("FDSWEEP_EXCEPT3_HELPER") == "1" {
		// Child: fd 3 (readiness pipe) must survive; fd 4 (a stray inherited non-CLOEXEC
		// fd standing in for an unrelated lease) must be swept.
		if err := SweepInheritedFDsExcept(3); err != nil {
			os.Exit(3)
		}
		if _, err := unix.FcntlInt(3, unix.F_GETFD, 0); err != nil {
			os.Exit(4) // fd 3 wrongly closed
		}
		if _, err := unix.FcntlInt(4, unix.F_GETFD, 0); err == nil {
			os.Exit(5) // stray fd 4 wrongly survived
		}
		os.Exit(0)
	}

	r3, w3, err := os.Pipe() // → child fd 3 (readiness pipe)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r3.Close() }()
	defer func() { _ = w3.Close() }()
	r4, w4, err := os.Pipe() // → child fd 4 (stray inherited fd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r4.Close() }()
	defer func() { _ = w4.Close() }()

	cmd := exec.Command(os.Args[0], "-test.run", "^TestSweepInheritedFDsExcept$")
	cmd.Env = append(os.Environ(), "FDSWEEP_EXCEPT3_HELPER=1")
	cmd.ExtraFiles = []*os.File{w3, r4} // non-CLOEXEC in the child at fd 3 and fd 4
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper exit = %v (want fd 3 kept, fd 4 swept)\n%s", err, out)
	}
}

// TestSpawnedLeaseAgentReadyFD pins the sweep-gating predicate main reads from os.Args
// before cobra parses: a --ready-fd on a lease-agent invocation is the ONLY signal that
// preserves fd 3 across the inherited-fd sweep.
func TestSpawnedLeaseAgentReadyFD(t *testing.T) {
	cases := map[string]struct {
		args   []string
		wantFD int
		wantOK bool
	}{
		"spawned, spaced flag": {[]string{"lease-agent", "--dir", "/x", "--ready-fd", "3"}, 3, true},
		"spawned, equals flag": {[]string{"lease-agent", "--ready-fd=3"}, 3, true},
		"manual, no flag":      {[]string{"lease-agent", "--pid", "0", "--dir", ""}, 0, false},
		"not a lease agent":    {[]string{"status", "--ready-fd", "3"}, 0, false},
		"zero fd is absent":    {[]string{"lease-agent", "--ready-fd", "0"}, 0, false},
		"dangling flag":        {[]string{"lease-agent", "--ready-fd"}, 0, false},
		"non-numeric fd":       {[]string{"lease-agent", "--ready-fd", "x"}, 0, false},
		// The single source is base-10, so a base-0 value cobra WOULD accept (0x3→3) is
		// rejected — the sweep never preserved fd 3, so the child must not adopt it.
		"base-0 fd rejected": {[]string{"lease-agent", "--ready-fd=0x3"}, 0, false},
		// Repeated flags take the FIRST occurrence (cobra takes the last); adoption reuses
		// this same result, so the two sites stay in lockstep whatever the value is.
		"repeated flag takes first": {[]string{"lease-agent", "--ready-fd", "4", "--ready-fd", "3"}, 4, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fd, ok := SpawnedLeaseAgentReadyFD(tc.args)
			if fd != tc.wantFD || ok != tc.wantOK {
				t.Fatalf("SpawnedLeaseAgentReadyFD(%q) = (%d, %v), want (%d, %v)", tc.args, fd, ok, tc.wantFD, tc.wantOK)
			}
		})
	}
}

// TestLeaseAgentArgsReadyFD pins the spawn-side half of F-3 finding-2: the argv the real
// spawn site emits round-trips through the SAME single source the child adopts and main's
// sweep preserves, resolving to readyPipeFD. Drop --ready-fd from leaseAgentArgs and the
// child would sweep fd 3 and the handshake would time out — this fails fast instead.
func TestLeaseAgentArgsReadyFD(t *testing.T) {
	args := leaseAgentArgs(4321, 99, 2, "/lease/key", "/cfg", true)
	fd, ok := SpawnedLeaseAgentReadyFD(args)
	if !ok || fd != readyPipeFD {
		t.Fatalf("SpawnedLeaseAgentReadyFD(spawn argv) = (%d, %v), want (%d, true) — spawn site must emit --ready-fd %d", fd, ok, readyPipeFD, readyPipeFD)
	}
}

// TestLeaseAgentReadyFDGate pins F-3: the fd-3 readiness handshake fires ONLY when the
// launcher declares it with --ready-fd. A manual `ccp lease-agent` (no flag) must leave
// its inherited fd 3 untouched — the crash the finding caught was signalReady writing to
// and closing an unrelated inherited fd 3. Both arms use a degenerate leader so RunE
// fails fast at the pid/dir guard (the signalReady call site) without any lease work.
// The child runs in a helper subprocess so fd 3 is a controlled sentinel pipe.
func TestLeaseAgentReadyFDGate(t *testing.T) {
	switch os.Getenv("LEASEAGENT_FD3_HELPER") {
	case "manual":
		runLeaseAgentFD3Helper([]string{"--pid", "0", "--dir", ""})
	case "wired":
		runLeaseAgentFD3Helper([]string{"--pid", "0", "--dir", "", "--ready-fd", "3"})
	case "base0":
		// cobra parses --ready-fd=0x3 as 3 (base-0), but the single source (base-10) rejects
		// it, so the sweep never preserved fd 3 — adoption must skip the handshake, not write.
		runLeaseAgentFD3Helper([]string{"--pid", "0", "--dir", "", "--ready-fd=0x3"})
	}

	cases := []struct {
		name      string
		mode      string
		wantWrite bool
	}{
		{"manual invocation leaves fd 3 untouched", "manual", false},
		{"wired invocation drives the fd-3 handshake", "wired", true},
		{"base-0 ready-fd is not adopted (single-sourced with the sweep)", "base0", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r3, w3, err := os.Pipe() // sentinel readiness pipe at the child's fd 3
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r3.Close() }()

			cmd := exec.Command(os.Args[0], "-test.run", "^TestLeaseAgentReadyFDGate$")
			cmd.Env = append(os.Environ(), "LEASEAGENT_FD3_HELPER="+tc.mode)
			cmd.ExtraFiles = []*os.File{w3}
			out, cerr := cmd.CombinedOutput()
			_ = w3.Close() // drop the parent's writer so a silent child reads EOF, not a block

			// Both arms fail the pid/dir guard, so the helper exits 7 (a clean error return).
			// Anything else — notably a runtime-fatal signal — is the F-3 crash, not the guard.
			var exit *exec.ExitError
			if !errors.As(cerr, &exit) || exit.ExitCode() != 7 {
				t.Fatalf("helper exit = %v (want clean guard-failure exit 7, not a crash)\n%s", cerr, out)
			}

			b, err := io.ReadAll(r3)
			if err != nil {
				t.Fatalf("read sentinel fd 3: %v", err)
			}
			switch {
			case tc.wantWrite && !strings.HasPrefix(strings.TrimSpace(string(b)), "err:"):
				t.Fatalf("wired lease-agent wrote %q to fd 3, want the handshake's err: verdict", b)
			case !tc.wantWrite && len(b) != 0:
				t.Fatalf("manual lease-agent wrote %q to fd 3; a no-flag invocation must never touch the readiness pipe", b)
			}
		})
	}
}

// runLeaseAgentFD3Helper runs the real lease-agent command in a helper subprocess and
// exits 7 on the expected guard failure so the parent can tell a clean error return from
// a crash. It never returns.
func runLeaseAgentFD3Helper(args []string) {
	// RunE adopts the readiness fd from SpawnedLeaseAgentReadyFD(os.Args[1:]) — the same
	// single source main's sweep uses — so drive it through a real lease-agent os.Args, not
	// just cobra's SetArgs copy (which carries no subcommand token for the parser to gate on).
	os.Args = append([]string{"ccp", leaseAgentSubcommand}, args...)
	cmd := newLeaseAgentCmd()
	cmd.SetArgs(args)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	if err := cmd.Execute(); err != nil {
		os.Exit(7)
	}
	os.Exit(0)
}
