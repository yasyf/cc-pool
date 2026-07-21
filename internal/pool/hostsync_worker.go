package pool

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"golang.org/x/sys/unix"
)

const hostSyncInlineWorkerExecutable = "cc-pool-host-sync-inline"

// OpenHostSyncWorker opens child-local state inside an already tracked,
// killable host-sync process group. It never creates a nested worker pool.
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
	db, err := store.Open(DBPath())
	if err != nil {
		return nil, err
	}
	runner := hostSyncInlineTaskRunner{}
	scanner, err := procscan.NewWorkerScanner(runner, hostSyncInlineWorkerExecutable)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	authority := newInlineWorkerAuthority(runner, hostSyncInlineWorkerExecutable, owner)
	manager, err := NewManager(db, oauth.New(), scanner.Scan, authority)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return manager, nil
}

// RunHostSyncCommand runs one child-local command inside the tracked host-sync group.
func (m *Manager) RunHostSyncCommand(
	ctx context.Context,
	path string,
	args ...string,
) error {
	if m.workerAuthority == nil || !m.workerAuthority.inline {
		return errors.New("host-sync command requires inline child authority")
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
	return m.workerAuthority.runner.Run(ctx, supervise.Task{
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

type hostSyncInlineTaskRunner struct{}

func (hostSyncInlineTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	if err := task.RecoveryClass.Validate(); err != nil {
		return err
	}
	switch {
	case IsBackingWorkerInvocation(task.Args):
		return RunBackingWorker(ctx, task.Stdin, task.Stdout)
	case IsCredentialCASWorkerInvocation(task.Args):
		return RunCredentialCASWorker(ctx, task.Stdin, task.Stdout)
	case creds.IsFileWorkerInvocation(task.Args):
		return creds.RunFileWorker(ctx, task.Stdin, task.Stdout)
	case procscan.IsWorkerInvocation(task.Args):
		return procscan.RunWorker(ctx, task.Stdin, task.Stdout)
	}
	if task.Path == "" {
		return errors.New("host-sync child task path is required")
	}
	if !filepath.IsAbs(task.Path) || filepath.Clean(task.Path) != task.Path {
		return errors.New("host-sync child task requires a clean absolute executable")
	}
	// #nosec G204 -- task.Path is a validated absolute synckitd, security(1), or test path.
	command := exec.CommandContext(ctx, task.Path, task.Args...)
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = task.Stdin
	command.Stdout = task.Stdout
	command.Stderr = task.Stderr
	return command.Run()
}
