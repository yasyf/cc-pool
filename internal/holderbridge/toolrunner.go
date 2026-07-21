package holderbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const toolRunnerCloseTimeout = 30 * time.Second

// ToolRunner owns one ephemeral daemonkit pool for bounded holder tooling.
type ToolRunner struct {
	pool      *supervise.Pool
	directory string

	closeOnce sync.Once
	closeErr  error
}

// NewToolRunner creates an isolated durable runner for packaging or verification.
func NewToolRunner(ctx context.Context) (*ToolRunner, error) {
	directory, err := os.MkdirTemp("", "cc-pool-holder-tools-")
	if err != nil {
		return nil, fmt.Errorf("holderbridge: create tool recovery directory: %w", err)
	}
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return nil, errors.Join(
			fmt.Errorf("holderbridge: generate tool runner generation: %w", err),
			os.RemoveAll(directory),
		)
	}
	reaper := &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "processes.db")},
		Generation: hex.EncodeToString(generation[:]),
	}
	workers, err := supervise.NewPool(1, reaper)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("holderbridge: create tool runner: %w", err), os.RemoveAll(directory),
		)
	}
	if err := workers.Recover(ctx); err != nil {
		workers.Close()
		workers.Cancel()
		return nil, errors.Join(
			fmt.Errorf("holderbridge: recover tool runner: %w", err),
			workers.Wait(context.WithoutCancel(ctx)),
			os.RemoveAll(directory),
		)
	}
	return &ToolRunner{pool: workers, directory: directory}, nil
}

// Run executes one task through the bounded daemonkit pool.
func (r *ToolRunner) Run(ctx context.Context, task supervise.Task) error {
	return r.pool.Run(ctx, task)
}

// Close settles the worker pool and removes its empty recovery store.
func (r *ToolRunner) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.pool.Close()
		r.pool.Cancel()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), toolRunnerCloseTimeout)
		defer cancel()
		if err := r.pool.Wait(ctx); err != nil {
			r.closeErr = err
			return
		}
		r.closeErr = os.RemoveAll(r.directory)
	})
	return r.closeErr
}

var _ supervise.TaskRunner = (*ToolRunner)(nil)
