package creds

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/supervise"
)

type blockingFileTaskRunner struct {
	active      atomic.Int32
	sawDeadline atomic.Bool
	sawSecret   atomic.Bool
}

func (runner *blockingFileTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	runner.active.Add(1)
	defer runner.active.Add(-1)
	if _, ok := ctx.Deadline(); ok {
		runner.sawDeadline.Store(true)
	}
	for _, arg := range task.Args {
		if arg == "at-secret" || arg == "rt-secret" {
			runner.sawSecret.Store(true)
		}
	}
	_, _ = io.ReadAll(task.Stdin)
	<-ctx.Done()
	return ctx.Err()
}

func TestFileStoreCancellationLeavesNoWorkerOrPipeWriter(t *testing.T) {
	runner := &blockingFileTaskRunner{}
	fileStore := FileStore{
		ConfigDir: t.TempDir(), Runner: runner, WorkerExecutable: "test-worker",
	}
	credential := &Credential{
		ClaudeAiOauth: OAuth{
			AccessToken: "at-secret", RefreshToken: "rt-secret", ExpiresAt: 1,
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- fileStore.Write(ctx, credential) }()
	for runner.active.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Write error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled FileStore.Write did not return")
	}
	if got := runner.active.Load(); got != 0 {
		t.Fatalf("active workers = %d, want 0", got)
	}
	if !runner.sawDeadline.Load() {
		t.Fatal("worker context carried no deadline")
	}
	if runner.sawSecret.Load() {
		t.Fatal("credential secret appeared in worker argv")
	}
}

func TestFileStoreRequiresDisposableWorker(t *testing.T) {
	fileStore := FileStore{ConfigDir: t.TempDir()}
	if _, err := fileStore.Read(t.Context()); err == nil {
		t.Fatal("FileStore.Read without worker succeeded")
	}
}

func TestFileStoreInProcessRunnerOwnsInputWriter(t *testing.T) {
	fileStore := FileStore{
		ConfigDir: t.TempDir(), Runner: testTaskRunner{}, WorkerExecutable: "in-process",
	}
	for range 100 {
		if _, err := fileStore.Read(t.Context()); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Read absent credential = %v, want ErrNotFound", err)
		}
	}
}

func TestFileStoreWorkerReturnsMalformedBytesForTypedParsing(t *testing.T) {
	configDir := t.TempDir()
	if err := os.WriteFile(FileCredentialPath(configDir), []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	fileStore := FileStore{
		ConfigDir: configDir, Runner: testTaskRunner{}, WorkerExecutable: "in-process",
	}
	if _, err := fileStore.Read(t.Context()); err == nil || !strings.Contains(err.Error(), "parse credential blob") {
		t.Fatalf("Read malformed credential = %v, want parse credential blob", err)
	}
}
