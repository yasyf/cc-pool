package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit"
)

const daemonTestScopeTimeout = 8 * time.Second

// daemonTestScope mints one process-ownership scope in the test's temp dir,
// the substrate every disposable-command fixture runs on.
func daemonTestScope(t *testing.T) daemonkit.Ctx {
	t.Helper()
	openCtx, cancel := context.WithTimeout(context.Background(), daemonTestScopeTimeout)
	defer cancel()
	owned, err := daemonkit.OwnProcesses(openCtx, t.TempDir()+"/processes.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), daemonTestScopeTimeout)
		defer cancel()
		if err := owned.Close(ctx); err != nil {
			t.Errorf("close test ownership scope: %v", err)
		}
	})
	return owned.Ctx(context.Background())
}

// daemonTestRunner is the disposable-command runner daemon fixtures install as
// Server.disposableWorkers.
func daemonTestRunner(t *testing.T) workerexec.Runner {
	t.Helper()
	return scopeRunner{scope: daemonTestScope(t)}
}
