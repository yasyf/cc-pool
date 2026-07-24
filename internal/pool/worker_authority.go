package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/proc"
)

// WorkerAuthority binds one Manager to an exact live process owner and task boundary.
type WorkerAuthority struct {
	runner     workerexec.Runner
	executable string
	owner      proc.Record
}

// NewWorkerAuthority validates a parent daemon's durable worker authority.
func NewWorkerAuthority(
	runner workerexec.Runner,
	executable string,
	owner proc.Record,
) (WorkerAuthority, error) {
	if runner == nil || executable == "" {
		return WorkerAuthority{}, errors.New("worker authority requires runner and executable")
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		return WorkerAuthority{}, err
	}
	if err := validateCurrentWorkerOwner(owner, identity); err != nil {
		return WorkerAuthority{}, err
	}
	if owner.RecoveryID != CredentialOwnerRecoveryID || owner.ProcessGroup {
		return WorkerAuthority{}, errors.New("parent worker authority cannot be a disposable process group")
	}
	return WorkerAuthority{runner: runner, executable: executable, owner: owner}, nil
}

func validateCurrentWorkerOwner(owner proc.Record, identity proc.Identity) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if owner.PID != identity.PID || owner.StartTime != identity.StartTime ||
		owner.Boot != identity.Boot {
		return proc.ErrIdentityChanged
	}
	if !owner.AuditToken.IsZero() && owner.AuditToken != identity.AuditToken {
		return proc.ErrIdentityChanged
	}
	if owner.Executable != "" && owner.Executable != identity.Executable {
		return proc.ErrIdentityChanged
	}
	return nil
}

// NewManager builds a worker-bound manager without creating or owning a worker pool.
func NewManager(
	st *store.Store,
	refresher Refresher,
	scanSessions func(context.Context) ([]procscan.Session, error),
	authority WorkerAuthority,
) (*Manager, error) {
	if st == nil || refresher == nil || scanSessions == nil ||
		authority.runner == nil || authority.executable == "" {
		return nil, errors.New("worker-bound manager requires store, OAuth, scanner, and authority")
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		return nil, err
	}
	if err := validateCurrentWorkerOwner(authority.owner, identity); err != nil {
		return nil, fmt.Errorf("validate worker-bound manager owner: %w", err)
	}
	if authority.owner.RecoveryID != CredentialOwnerRecoveryID || authority.owner.ProcessGroup {
		return nil, errors.New("worker authority kind does not match process ownership")
	}
	manager := &Manager{
		Store:            st,
		OAuth:            refresher,
		Creds:            sysCredentials{runner: authority.runner},
		ScanSessions:     scanSessions,
		workerAuthority:  &authority,
		taskRunner:       authority.runner,
		workerExecutable: authority.executable,
	}
	manager.credentialCAS = manager.runCredentialCAS
	return manager, nil
}
