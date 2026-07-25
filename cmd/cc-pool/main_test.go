package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

func TestMain(m *testing.M) {
	if handled, err := trust.RunVerifierChild(os.Args[1:], os.Stdout); handled {
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

func TestVerifierChildEntrypointCompletesProbe(t *testing.T) {
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.db")},
		Generation: proc.OwnerGeneration{1},
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: time.Minute,
		MaxStdinBytes: 1 << 20, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}, reaper)
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
			t.Errorf("close worker claim: %v", err)
		}
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	verifier := trust.ProcessVerifier{Runner: claim, Executable: executable}
	if err := verifier.Probe(ctx, peer.Identity{UID: os.Geteuid()}); err != nil {
		t.Fatalf("probe verifier child: %v", err)
	}
}
