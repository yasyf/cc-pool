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

const disposableWorkerLimit = 4

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
	reaper, err := newWorkerReaper(DisposableWorkerStorePath())
	if err != nil {
		return nil, nil, err
	}
	workers, err := supervise.NewPool(disposableWorkerLimit, reaper)
	if err != nil {
		return nil, nil, err
	}
	if err := workers.Recover(ctx); err != nil {
		workers.Close()
		workers.Cancel()
		_ = workers.Wait(context.Background())
		return nil, nil, err
	}
	identity, err := proc.CurrentIdentity()
	if err != nil {
		workers.Close()
		workers.Cancel()
		_ = workers.Wait(context.Background())
		return nil, nil, fmt.Errorf("bind credential owner process: %w", err)
	}
	owner, err := reaper.TrackIdentity(ctx, identity, proc.RecoveryTask)
	if err != nil {
		workers.Close()
		workers.Cancel()
		_ = workers.Wait(context.Background())
		return nil, nil, fmt.Errorf("track credential owner process: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		workers.Close()
		workers.Cancel()
		_ = workers.Wait(context.Background())
		return nil, nil, fmt.Errorf("resolve disposable worker executable: %w", err)
	}
	scanner, err := procscan.NewWorkerScanner(workers, executable)
	if err != nil {
		workers.Close()
		workers.Cancel()
		_ = workers.Wait(context.Background())
		return nil, nil, err
	}
	return &workerRuntime{
		pool: workers, reaper: reaper, owner: owner,
		executable: executable,
	}, scanner, nil
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

func (runtime *workerRuntime) close() error {
	if runtime == nil {
		return nil
	}
	runtime.pool.Close()
	runtime.pool.Cancel()
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	waitErr := runtime.pool.Wait(waitCtx)
	untrackErr := runtime.reaper.Untrack(context.Background(), runtime.owner)
	return errors.Join(waitErr, untrackErr)
}
