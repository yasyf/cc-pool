package pool

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/worker"
)

const hostSyncCommandTimeout = 10 * time.Minute

// OpenHostSyncWorker opens child-local state with its own claimed disposable workers.
func OpenHostSyncWorker(ctx context.Context) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	workers, scanner, err := newWorkerRuntimeAt(
		ctx, HostSyncWorkerStorePath(), HostSyncChildStorePath(), true,
	)
	if err != nil {
		return nil, err
	}
	db, err := store.Open(DBPath())
	if err != nil {
		_ = workers.close(ctx)
		return nil, err
	}
	authority, err := NewWorkerAuthority(workers.pool, workers.executable, workers.owner)
	if err != nil {
		_ = workers.close(ctx)
		_ = db.Close()
		return nil, err
	}
	manager, err := NewManager(db, oauth.New(), scanner.Scan, authority)
	if err != nil {
		_ = workers.close(ctx)
		_ = db.Close()
		return nil, err
	}
	manager.workers = workers
	return manager, nil
}

// RunHostSyncCommand runs one child-local command inside the tracked host-sync group.
func (m *Manager) RunHostSyncCommand(
	ctx context.Context,
	path string,
	args ...string,
) error {
	if m.workers == nil || m.taskRunner == nil {
		return errors.New("host-sync command requires disposable worker ownership")
	}
	if path != "synckitd" {
		return errors.New("host-sync command is not an approved executable")
	}
	executable, err := exec.LookPath(path)
	if err != nil {
		return fmt.Errorf("resolve host-sync command: %w", err)
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return errors.New("host-sync command did not resolve to a clean absolute executable")
	}
	_, err = m.taskRunner.Run(ctx, worker.CommandRequest{
		Path: executable, Dir: workerexec.TempDir(), Args: args,
		TotalTimeout: hostSyncCommandTimeout,
	})
	return err
}
