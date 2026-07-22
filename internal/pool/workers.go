package pool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const (
	disposableWorkerLimit = 4
	workerCloseTimeout    = 30 * time.Second
)

type workerRuntime struct {
	pool       *supervise.Pool
	reaper     *proc.Reaper
	owner      proc.Record
	executable string
}

var (
	processGenerationOnce sync.Once
	processGeneration     string
	processGenerationErr  error
)

func currentProcessGeneration() (string, error) {
	processGenerationOnce.Do(func() {
		var generation [16]byte
		if _, err := rand.Read(generation[:]); err != nil {
			processGenerationErr = fmt.Errorf("generate process generation: %w", err)
			return
		}
		processGeneration = hex.EncodeToString(generation[:])
	})
	return processGeneration, processGenerationErr
}

func newWorkerRuntime(
	ctx context.Context,
) (*workerRuntime, *procscan.WorkerScanner, error) {
	return newWorkerRuntimeAt(ctx, DisposableWorkerStorePath(), false)
}

func newWorkerRuntimeAt(
	ctx context.Context,
	storePath string,
	ackRecoveredTasks bool,
) (*workerRuntime, *procscan.WorkerScanner, error) {
	reaper, err := newWorkerReaper(storePath)
	if err != nil {
		return nil, nil, err
	}
	workers, err := supervise.NewPool(disposableWorkerLimit, reaper)
	if err != nil {
		return nil, nil, err
	}
	if err := workers.Recover(ctx); err != nil {
		_ = closeWorkerPool(ctx, workers)
		return nil, nil, err
	}
	if ackRecoveredTasks {
		_, err := reaper.RecoverReapReceipts(
			ctx,
			proc.RecoveryTask,
			func(context.Context, proc.ReapReceipt) error { return nil },
		)
		if err != nil {
			_ = closeWorkerPool(ctx, workers)
			return nil, nil, fmt.Errorf("settle recovered disposable tasks: %w", err)
		}
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		_ = closeWorkerPool(ctx, workers)
		return nil, nil, fmt.Errorf("bind worker owner process: %w", err)
	}
	owner, err := reaper.TrackIdentity(ctx, identity, proc.RecoveryTask)
	if err != nil {
		_ = closeWorkerPool(ctx, workers)
		return nil, nil, fmt.Errorf("track credential owner process: %w", err)
	}
	runtime := &workerRuntime{pool: workers, reaper: reaper, owner: owner}
	executable, err := os.Executable()
	if err != nil {
		_ = runtime.close(ctx)
		return nil, nil, fmt.Errorf("resolve disposable worker executable: %w", err)
	}
	runtime.executable = executable
	scanner, err := procscan.NewWorkerScanner(workers, executable)
	if err != nil {
		_ = runtime.close(ctx)
		return nil, nil, err
	}
	return runtime, scanner, nil
}

func newWorkerReaper(path string) (*proc.Reaper, error) {
	generation, err := currentProcessGeneration()
	if err != nil {
		return nil, err
	}
	return &proc.Reaper{
		Store: &proc.FileStore{Path: path}, Generation: generation,
	}, nil
}

func closeWorkerPool(ctx context.Context, workers *supervise.Pool) error {
	workers.Close()
	workers.Cancel()
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerCloseTimeout)
	defer cancel()
	return workers.Wait(waitCtx)
}

func (runtime *workerRuntime) close(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	waitErr := closeWorkerPool(ctx, runtime.pool)
	untrackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerCloseTimeout)
	defer cancel()
	untrackErr := runtime.reaper.Untrack(untrackCtx, runtime.owner)
	return errors.Join(waitErr, untrackErr)
}
