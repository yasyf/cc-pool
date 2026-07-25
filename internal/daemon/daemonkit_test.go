package daemon

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

func daemonTestGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func activatedDaemonTestWorkers(t *testing.T, capacity int) *worker.Pool {
	t.Helper()
	workers, err := worker.NewPool(worker.Config{
		Capacity: capacity, QueueCapacity: capacity, MaxTotalRun: 3 * time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers-v1.db")},
		Generation: daemonTestGeneration(t.Name()),
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := workers.ClaimRuntime(trust.VerifierWorkerBudgets())
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
