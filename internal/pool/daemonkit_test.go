package pool

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/proc"
)

func poolTestGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func poolTestRunner(t *testing.T, recordPath string) *workerRuntime {
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
	runtime, err := newWorkerRuntime(owned.Ctx(scopeCtx))
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}
