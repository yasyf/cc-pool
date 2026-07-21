package procscan

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	dkproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func TestWorkerScannerCancellationKillsReapsAndUntracks(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "wedged-worker")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	store := &dkproc.FileStore{Path: filepath.Join(dir, "workers.json")}
	reaper := &dkproc.Reaper{Store: store, Generation: "procscan-worker-test"}
	workers, err := supervise.NewPool(1, reaper)
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := NewWorkerScanner(workers, script)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := scanner.Scan(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Scan error = %v, want deadline exceeded", err)
	}
	workers.Close()
	workers.Cancel()
	if err := workers.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable worker records after cancellation = %+v", records)
	}
}

func TestWorkerOutputIsBounded(t *testing.T) {
	var output boundedWorkerBuffer
	payload := make([]byte, maxWorkerOutput*2)
	if n, err := output.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if got := output.Len(); got != maxWorkerOutput {
		t.Fatalf("buffer size = %d, want %d", got, maxWorkerOutput)
	}
}
