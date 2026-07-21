package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	if err := os.WriteFile(
		script,
		[]byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"),
		0o700,
	); err != nil {
		t.Fatal(err)
	}
	recordStore := &daemonproc.FileStore{Path: filepath.Join(home, "workers.json")}
	reaper := &daemonproc.Reaper{
		Store: recordStore, Generation: "account-backing-worker-test",
	}
	workers, err := supervise.NewPool(1, reaper)
	if err != nil {
		t.Fatal(err)
	}
	manager := &Manager{taskRunner: workers, workerExecutable: script}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = manager.prepareAccountBacking(
		ctx, 18, AccountDir(18), ClaudeJSONPath(),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("prepare account backing error = %v, want deadline exceeded", err)
	}
	workers.Close()
	workers.Cancel()
	if err := workers.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	records, err := recordStore.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable worker records after cancellation = %+v", records)
	}
}
