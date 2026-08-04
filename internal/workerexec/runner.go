// Package workerexec defines cc-pool's narrow disposable-command boundary.
package workerexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// TempDir returns the exact cleaned process temporary directory.
func TempDir() string { return filepath.Clean(os.TempDir()) }

var (
	// ErrCapacity means every execution and queue slot is occupied.
	ErrCapacity = errors.New("workerexec: capacity exhausted")
	// ErrTimedOut means the queue and execution deadline elapsed.
	ErrTimedOut = errors.New("workerexec: timed out")
)

// CommandRequest is copied and validated before it can enter the queue.
type CommandRequest struct {
	Path         string
	Dir          string
	Args         []string
	Env          []string
	Stdin        []byte
	TotalTimeout time.Duration
}

// CommandResult is an immutable command observation.
type CommandResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// Runner executes one bounded disposable command and returns its immutable result.
type Runner interface {
	Run(context.Context, CommandRequest) (CommandResult, error)
}
