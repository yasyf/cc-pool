package pool

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit"
)

const hostSyncCommandTimeout = 10 * time.Minute

// OpenHostSyncWorker opens child-local state with its own disposable workers,
// bound to the process scope synckit's helper runtime handed Prepare.
func OpenHostSyncWorker(scope daemonkit.Ctx) (*Manager, error) {
	db, err := store.Open(DBPath())
	if err != nil {
		return nil, err
	}
	workers, err := newWorkerRuntime(scope)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	scanner, err := procscan.NewWorkerScanner(workers, workers.executable)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	owner, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	authority, err := NewWorkerAuthority(workers, workers.executable, owner)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	manager, err := NewManager(db, oauth.New(), scanner.Scan, authority)
	if err != nil {
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
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("host-sync command requires a clean absolute executable")
	}
	_, err := m.taskRunner.Run(ctx, workerexec.CommandRequest{
		Path: path, Dir: workerexec.TempDir(), Args: args,
		TotalTimeout: hostSyncCommandTimeout,
	})
	return err
}
