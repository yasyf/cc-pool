package pool

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"golang.org/x/sys/unix"
)

// OpenHostSyncWorker opens child-local state inside an already tracked,
// killable host-sync process group with its own recovered disposable workers.
func OpenHostSyncWorker(ctx context.Context, owner proc.Record) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		return nil, fmt.Errorf("bind host-sync worker identity: %w", err)
	}
	sessionID, err := unix.Getsid(0)
	if err != nil {
		return nil, fmt.Errorf("resolve host-sync worker session: %w", err)
	}
	if err := validateHostSyncWorkerOwner(owner, identity, sessionID); err != nil {
		return nil, err
	}
	workers, scanner, err := newWorkerRuntimeAt(ctx, HostSyncWorkerStorePath(), true)
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
	return m.taskRunner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          executable,
		Args:          args,
	})
}

func validateHostSyncWorkerOwner(
	owner proc.Record,
	identity proc.Identity,
	sessionID int,
) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if owner.RecoveryClass != proc.RecoverySourceOwner || !owner.ProcessGroup ||
		owner.SessionID != owner.PID || sessionID != owner.PID {
		return errors.New("host-sync worker requires an exact tracked process-group owner")
	}
	if owner.PID != identity.PID || owner.StartTime != identity.StartTime ||
		owner.Boot != identity.Boot {
		return proc.ErrIdentityChanged
	}
	return nil
}
