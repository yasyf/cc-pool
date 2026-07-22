package hostsync

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func newHostSyncTestWorkers(t *testing.T) *supervise.Pool {
	t.Helper()
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers-v1.json")},
		Generation: "hostsync-test",
	}
	workers, err := supervise.NewPool(2, reaper)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		workers.Close()
		workers.Cancel()
		if err := workers.Wait(context.Background()); err != nil {
			t.Errorf("wait host-sync test workers: %v", err)
		}
	})
	return workers
}
