package pool

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

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
