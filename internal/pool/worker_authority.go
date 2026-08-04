package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
)

// WorkerAuthority binds one Manager to its disposable-command boundary and the
// owner identity its fenced writes carry.
type WorkerAuthority struct {
	runner     workerexec.Runner
	executable string
	owner      store.OwnerRecord
}

// NewWorkerAuthority validates a parent daemon's durable worker authority.
func NewWorkerAuthority(
	runner workerexec.Runner,
	executable string,
	owner store.OwnerRecord,
) (WorkerAuthority, error) {
	if runner == nil || executable == "" {
		return WorkerAuthority{}, errors.New("worker authority requires runner and executable")
	}
	if err := owner.Validate(); err != nil {
		return WorkerAuthority{}, err
	}
	return WorkerAuthority{runner: runner, executable: executable, owner: owner}, nil
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
	if err := authority.owner.Validate(); err != nil {
		return nil, fmt.Errorf("validate worker-bound manager owner: %w", err)
	}
	manager := &Manager{
		Store:            st,
		OAuth:            refresher,
		Creds:            sysCredentials{runner: authority.runner},
		ScanSessions:     scanSessions,
		owner:            authority.owner,
		workerAuthority:  &authority,
		taskRunner:       authority.runner,
		workerExecutable: authority.executable,
	}
	manager.credentialCAS = manager.runCredentialCAS
	return manager, nil
}
