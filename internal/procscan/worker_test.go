package procscan

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit"
)

// scopedRunner runs disposable commands in one daemonkit process scope, the
// same substrate the daemon's pool uses.
type scopedRunner struct{ scope daemonkit.Ctx }

func (r scopedRunner) Run(
	ctx context.Context, request workerexec.CommandRequest,
) (workerexec.CommandResult, error) {
	runCtx, cancel := context.WithTimeout(ctx, request.TotalTimeout)
	defer cancel()
	result, err := r.scope.Run(runCtx, daemonkit.Cmd{
		Path: request.Path, Args: request.Args, Dir: request.Dir,
		Env: request.Env, Stdin: request.Stdin, MaxOutput: 1 << 20,
		Exec: daemonkit.ServingSameUser(),
	})
	return workerexec.CommandResult{
		Stdout: result.Stdout, Stderr: result.Stderr, ExitCode: result.Exit.Code,
	}, err
}

func scanTestRunner(t *testing.T, recordPath string) scopedRunner {
	t.Helper()
	scopeCtx, cancelScope := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancelScope)
	owned, err := daemonkit.OwnProcesses(scopeCtx, recordPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		if err := owned.Close(closeCtx); err != nil {
			t.Errorf("close test process scope: %v", err)
		}
	})
	return scopedRunner{scope: owned.Ctx(scopeCtx)}
}

func TestWorkerScannerCancellationKillsTheWedgedWorker(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "wedged-worker")
	pidPath := filepath.Join(dir, "wedged-worker.pid")
	body := "#!/bin/sh\necho $$ > " + strconv.Quote(pidPath) + "\ntrap '' TERM\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(script, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o700); err != nil { //nolint:gosec // G302: private test script needs its owner execute bit
		t.Fatal(err)
	}
	scanner, err := NewWorkerScanner(scanTestRunner(t, filepath.Join(dir, "workers.db")), script)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := scanner.Scan(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Scan error = %v, want deadline exceeded", err)
	}
	assertProcessGone(t, readWedgedPID(t, pidPath))
}

func readWedgedPID(t *testing.T, path string) int {
	t.Helper()
	payload, err := os.ReadFile(path) // #nosec G304 -- test-owned beneath t.TempDir().
	if err != nil {
		t.Fatalf("wedged worker never published its pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func assertProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("wedged worker %d survived cancellation", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
