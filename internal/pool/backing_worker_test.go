package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	daemonproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

type inlineBackingTaskRunner struct{}

func (inlineBackingTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	if !IsBackingWorkerInvocation(task.Args) {
		return errors.New("unexpected pool test worker task")
	}
	return RunBackingWorker(ctx, task.Stdin, task.Stdout)
}

func installTestBackingRunner(manager *Manager) {
	manager.taskRunner = inlineBackingTaskRunner{}
	manager.workerExecutable = "test-worker"
}

func writePoolTestExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("pool test executable requires a clean absolute path")
	}
	// #nosec G302 G304 -- this validated test-only path must be owner-executable.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(contents); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackingWorkerRejectsMismatchedPresentationIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if _, err := accountBackingPath(18, AccountDir(19)); err == nil {
		t.Fatal("account backing worker accepted a mismatched presentation path")
	}
}

func TestBackingWorkerRemovesOnlyDirectAccountDirectory(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	path := AccountBackingDir(18)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := removeAccountBackingDirect(path); err != nil {
		t.Fatal(err)
	}
	if err := removeAccountBackingDirect(path); err != nil {
		t.Fatalf("repeated removal: %v", err)
	}
}

func TestPrepareAccountBackingDeadlineKillsReapsAndUntracks(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	script := filepath.Join(home, "wedged-account-worker")
	pidPath := filepath.Join(home, "wedged-account-worker.pid")
	writePoolTestExecutable(
		t,
		script,
		"#!/bin/sh\necho $$ > "+strconv.Quote(pidPath)+"\ntrap '' TERM\nwhile :; do sleep 1; done\n",
	)
	recordStore := &daemonproc.FileStore{Path: filepath.Join(home, "workers.json")}
	reaper := &daemonproc.Reaper{
		Store: recordStore, Generation: "account-backing-worker-test",
	}
	workers, err := supervise.NewPool(1, reaper)
	if err != nil {
		t.Fatal(err)
	}
	workersSettled := false
	t.Cleanup(func() {
		if workersSettled {
			return
		}
		workers.Close()
		workers.Cancel()
		if waitErr := workers.Wait(context.Background()); waitErr != nil {
			t.Errorf("settle account backing workers: %v", waitErr)
		}
	})
	manager := &Manager{taskRunner: workers, workerExecutable: script}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, prepareErr := manager.prepareAccountBacking(
			ctx, 18, AccountDir(18), ClaudeJSONPath(),
		)
		result <- prepareErr
	}()
	var identity daemonproc.Identity
	for identity.PID == 0 {
		// #nosec G304 -- pidPath is test-owned beneath t.TempDir().
		payload, readErr := os.ReadFile(pidPath)
		if readErr == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(payload)))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			identity, err = daemonproc.Probe(pid)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatal(readErr)
		}
		select {
		case <-ctx.Done():
			t.Fatal("wedged account worker did not publish its process identity")
		case <-time.After(5 * time.Millisecond):
		}
	}
	err = <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("prepare account backing error = %v, want deadline exceeded", err)
	}
	workers.Close()
	workers.Cancel()
	waitErr := workers.Wait(context.Background())
	workersSettled = true
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	records, err := recordStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable worker records after cancellation = %+v", records)
	}
	if current, probeErr := daemonproc.Probe(identity.PID); probeErr == nil &&
		current.Boot == identity.Boot && current.StartTime == identity.StartTime {
		t.Fatalf("exact wedged account worker survived cancellation: %+v", current)
	}
}
