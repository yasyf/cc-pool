// Package workerexec defines cc-pool's narrow disposable-command boundary.
package workerexec

import (
	"context"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/worker"
)

// TempDir returns the exact cleaned process temporary directory.
func TempDir() string { return filepath.Clean(os.TempDir()) }

// Runner executes one bounded daemonkit command and returns its immutable result.
type Runner interface {
	Run(context.Context, worker.CommandRequest) (worker.CommandResult, error)
}
