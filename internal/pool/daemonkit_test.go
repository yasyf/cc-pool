package pool

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func poolTestGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func activatedPoolTestWorkers(t *testing.T, path string, capacity int) *worker.Pool {
	t.Helper()
	workers, err := worker.NewPool(worker.Config{
		Capacity: capacity, QueueCapacity: capacity, MaxTotalRun: time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, &proc.Reaper{
		Store: &proc.FileStore{Path: path}, Generation: poolTestGeneration(t.Name()),
	})
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := claim.Close(ctx); err != nil {
			t.Errorf("close test worker claim: %v", err)
		}
	})
	return workers
}
