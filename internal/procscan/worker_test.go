package procscan

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	dkproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func TestWorkerScannerCancellationKillsReapsAndUntracks(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "wedged-worker")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o700); err != nil { //nolint:gosec // G302: private test script needs its owner execute bit
		t.Fatal(err)
	}
	store := &dkproc.FileStore{Path: filepath.Join(dir, "workers.json")}
	digest := sha256.Sum256([]byte(t.Name()))
	var generation dkproc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	reaper := &dkproc.Reaper{Store: store, Generation: generation}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, reaper)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := workers.ClaimRuntime()
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := claim.Close(context.Background()); err != nil {
			t.Errorf("close worker claim: %v", err)
		}
	})
	scanner, err := NewWorkerScanner(workers, script)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := scanner.Scan(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Scan error = %v, want deadline exceeded", err)
	}
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable worker records after cancellation = %+v", records)
	}
}
