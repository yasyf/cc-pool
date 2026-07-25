package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

const (
	disposableWorkerLimit = 4
	childProcessLimit     = 8
	workerCloseTimeout    = 30 * time.Second
	workerMaxTotalRun     = 31 * time.Minute
	workerMaxInput        = 16 << 20
	workerMaxOutput       = 16 << 20
)

// CredentialOwnerRecoveryID is the sole durable owner identity for credential mutations.
// #nosec G101 -- this is a public process-recovery identifier, not credential material.
const CredentialOwnerRecoveryID proc.RecoveryID = "com.yasyf.cc-pool.credential-owner.v1"

type workerRuntime struct {
	pool       *worker.Pool
	children   *proc.Manager
	reaper     *proc.Reaper
	owner      proc.Record
	claim      *worker.RuntimeClaim
	executable string
}

func newWorkerRuntime(ctx context.Context) (*workerRuntime, *procscan.WorkerScanner, error) {
	return newWorkerRuntimeAt(ctx, DisposableWorkerStorePath(), ChildProcessStorePath(), false)
}

func newWorkerRuntimeAt(
	ctx context.Context,
	workerStorePath string,
	childStorePath string,
	activate bool,
) (*workerRuntime, *procscan.WorkerScanner, error) {
	runtime, err := newProcessRuntimeAt(workerStorePath, childStorePath)
	if err != nil {
		return nil, nil, err
	}
	workers := runtime.pool
	workerReaper := runtime.reaper
	if activate {
		claim, claimErr := workers.ClaimRuntime(trust.VerifierWorkerBudgets())
		if claimErr != nil {
			return nil, nil, claimErr
		}
		runtime.claim = claim
		if err := claim.Recover(ctx); err != nil {
			_ = claim.Release(context.WithoutCancel(ctx))
			return nil, nil, err
		}
		if err := claim.Activate(); err != nil {
			_ = claim.Release(context.WithoutCancel(ctx))
			return nil, nil, err
		}
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		_ = runtime.close(ctx)
		return nil, nil, fmt.Errorf("bind worker owner process: %w", err)
	}
	owner, err := workerReaper.TrackIdentity(ctx, identity, CredentialOwnerRecoveryID)
	if err != nil {
		_ = runtime.close(ctx)
		return nil, nil, fmt.Errorf("track credential owner process: %w", err)
	}
	runtime.owner = owner
	scanner, err := procscan.NewWorkerScanner(workers, runtime.executable)
	if err != nil {
		_ = runtime.close(ctx)
		return nil, nil, err
	}
	return runtime, scanner, nil
}

func newProcessRuntimeAt(workerStorePath, childStorePath string) (*workerRuntime, error) {
	workerReaper, err := newWorkerReaper(workerStorePath)
	if err != nil {
		return nil, err
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: disposableWorkerLimit, QueueCapacity: disposableWorkerLimit,
		MaxTotalRun: workerMaxTotalRun, MaxStdinBytes: workerMaxInput,
		MaxStdoutBytes: workerMaxOutput, MaxStderrBytes: workerMaxOutput,
	}, workerReaper)
	if err != nil {
		return nil, err
	}
	childReaper, err := newWorkerReaper(childStorePath)
	if err != nil {
		return nil, err
	}
	children, err := proc.NewManager(childProcessLimit, childReaper)
	if err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve disposable worker executable: %w", err)
	}
	return &workerRuntime{
		pool: workers, children: children, reaper: workerReaper, executable: executable,
	}, nil
}

// HostSyncHelperOwners are the resident helper's independent daemonkit process owners.
type HostSyncHelperOwners struct{ runtime *workerRuntime }

// OpenHostSyncHelperOwners constructs unclaimed owners for Synckit's helper runtime.
func OpenHostSyncHelperOwners() (*HostSyncHelperOwners, error) {
	runtime, err := newProcessRuntimeAt(HostSyncHelperWorkerStorePath(), HostSyncHelperChildStorePath())
	if err != nil {
		return nil, err
	}
	return &HostSyncHelperOwners{runtime: runtime}, nil
}

// Workers returns the helper's daemonkit worker pool.
func (o *HostSyncHelperOwners) Workers() *worker.Pool { return o.runtime.pool }

// Children returns the helper's daemonkit child manager.
func (o *HostSyncHelperOwners) Children() *proc.Manager { return o.runtime.children }

// Executable returns the exact helper binary used for disposable host-sync operations.
func (o *HostSyncHelperOwners) Executable() string { return o.runtime.executable }

func newWorkerReaper(path string) (*proc.Reaper, error) {
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return nil, err
	}
	return &proc.Reaper{Store: &proc.FileStore{Path: path}, Generation: generation}, nil
}

func (runtime *workerRuntime) close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerCloseTimeout)
	defer cancel()
	var result error
	if runtime.claim != nil {
		result = runtime.claim.Close(cleanupCtx)
	}
	if runtime.owner.Validate() == nil {
		result = errors.Join(result, runtime.reaper.Untrack(cleanupCtx, runtime.owner))
	}
	return result
}
