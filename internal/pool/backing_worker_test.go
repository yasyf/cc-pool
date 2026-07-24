package pool

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	daemonproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

type inlineBackingTaskRunner struct{}

func (inlineBackingTaskRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	if !IsBackingWorkerInvocation(task.Args) {
		return worker.CommandResult{}, errors.New("unexpected pool test worker task")
	}
	var output bytes.Buffer
	err := RunBackingWorker(ctx, bytes.NewReader(task.Stdin), &output)
	return worker.CommandResult{Stdout: output.Bytes()}, err
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

func TestBackingWorkerUsesOnlyOpaqueAccountIdentity(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	got, err := accountBackingPath(18)
	if err != nil {
		t.Fatal(err)
	}
	if want := AccountBackingDir(18); got != want {
		t.Fatalf("account backing = %q, want %q", got, want)
	}
	if _, err := accountBackingPath(0); err == nil {
		t.Fatal("account backing worker accepted an invalid account ID")
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
	workers := activatedPoolTestWorkers(t, recordStore.Path, 1)
	manager := &Manager{taskRunner: workers, workerExecutable: script}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, prepareErr := manager.prepareAccountBacking(ctx, 18, ClaudeJSONPath())
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
			identity, readErr = daemonproc.Probe(pid)
			if readErr != nil {
				t.Fatal(readErr)
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
	prepareErr := <-result
	if !errors.Is(prepareErr, context.DeadlineExceeded) {
		t.Fatalf("prepare account backing error = %v, want deadline exceeded", prepareErr)
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
