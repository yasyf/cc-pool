package pool

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// bumpUnderLock takes the flock at lockPath, reads counterPath as an int,
// increments it, and writes it back. The Gosched widens the read→write window so
// a missing lock would surface as a lost update.
func bumpUnderLock(t *testing.T, lockPath, counterPath string) {
	t.Helper()
	h, err := flockAcquire(context.Background(), lockPath)
	if err != nil {
		t.Errorf("acquire: %v", err)
		return
	}
	defer h.release()
	b, err := os.ReadFile(counterPath) //nolint:gosec // G304: counterPath is under the test's own t.TempDir()
	if err != nil {
		t.Errorf("read counter: %v", err)
		return
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	runtime.Gosched()
	if err := os.WriteFile(counterPath, []byte(strconv.Itoa(n+1)), 0o600); err != nil {
		t.Errorf("write counter: %v", err)
	}
}

// TestFlockSerializesCriticalSection proves the advisory lock makes the
// read→modify→write critical section mutually exclusive. flock excludes between
// open file descriptions, so two goroutines each opening their own fd is the
// same mechanism as two processes — a lost update here would mean the lock does
// not serialize at all.
func TestFlockSerializesCriticalSection(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "counter.lock")
	counterPath := filepath.Join(dir, "counter")
	if err := os.WriteFile(counterPath, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	const iterations = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				bumpUnderLock(t, lockPath, counterPath)
			}
		}()
	}
	wg.Wait()

	b, err := os.ReadFile(counterPath) //nolint:gosec // G304: counterPath is under the test's own t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	got, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	if want := goroutines * iterations; got != want {
		t.Fatalf("counter = %d, want %d — lost updates mean the flock did not serialize", got, want)
	}
}

// TestFlockRespectsContext verifies a contended acquire observes ctx
// cancellation promptly instead of blocking in the syscall forever.
func TestFlockRespectsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.lock")
	held, err := flockAcquire(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer held.release()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := flockAcquire(ctx, path); err == nil {
		t.Fatal("flockAcquire succeeded while the lock was held; want a ctx error")
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("flockAcquire took %v to honor a 50ms deadline", waited)
	}
}

const (
	flockChildLockEnv  = "CCP_FLOCK_TEST_LOCK"
	flockChildReadyEnv = "CCP_FLOCK_TEST_READY"
	flockChildHold     = 700 * time.Millisecond
)

// TestFlockChildHolds is the child half of TestFlockCrossProcess: it acquires the
// lock, signals readiness via a file, holds for flockChildHold, then releases.
// Without the env handshake it is a no-op so a normal `go test` run skips it.
func TestFlockChildHolds(t *testing.T) {
	lockPath := os.Getenv(flockChildLockEnv)
	readyPath := os.Getenv(flockChildReadyEnv)
	if lockPath == "" || readyPath == "" {
		t.Skip("child-only helper; driven by TestFlockCrossProcess")
	}
	h, err := flockAcquire(context.Background(), lockPath)
	if err != nil {
		t.Fatalf("child acquire: %v", err)
	}
	//nolint:gosec // G703: readyPath is under the test's own t.TempDir(), not external input
	if err := os.WriteFile(readyPath, []byte("1"), 0o600); err != nil {
		t.Fatalf("child signal ready: %v", err)
	}
	time.Sleep(flockChildHold)
	h.release()
}

// TestFlockCrossProcess is the real proof: a child PROCESS holds the lock while
// this process tries to acquire it, and the acquire must block until the child
// releases. Re-execs the test binary running only TestFlockChildHolds.
func TestFlockCrossProcess(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "x.lock")
	readyPath := filepath.Join(dir, "ready")

	//nolint:gosec // G204: re-execs this test binary (os.Args[0]) with fixed flags to take the flock from a separate process
	child := exec.Command(os.Args[0], "-test.run=^TestFlockChildHolds$", "-test.v")
	child.Env = append(os.Environ(),
		flockChildLockEnv+"="+lockPath,
		flockChildReadyEnv+"="+readyPath)
	var out bytes.Buffer
	child.Stdout, child.Stderr = &out, &out
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Wait() })

	// Wait for the child to acquire and signal it holds the lock.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child never signaled ready; output:\n%s", out.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	h, err := flockAcquire(context.Background(), lockPath)
	if err != nil {
		t.Fatalf("parent acquire: %v; child output:\n%s", err, out.String())
	}
	waited := time.Since(start)
	h.release()
	if waited < 300*time.Millisecond {
		t.Fatalf("parent acquired in %v without blocking — flock is not excluding across processes; child output:\n%s", waited, out.String())
	}
}
