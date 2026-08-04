package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit"
)

const (
	disposableWorkerLimit = 4
	disposableWorkerQueue = 4
	workerMaxTotalRun     = 31 * time.Minute
	workerMaxInput        = 16 << 20
	workerMaxOutput       = 16 << 20
)

// workerRuntime bounds cc-pool's disposable commands and runs them in the
// daemon's own process scope, so a crashed generation's children come back as
// Ctx.Reclaimed instead of orphaning under a second record file.
type workerRuntime struct {
	scope      daemonkit.Ctx
	admission  chan struct{}
	execution  chan struct{}
	executable string
}

func newWorkerRuntime(scope daemonkit.Ctx) (*workerRuntime, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve disposable worker executable: %w", err)
	}
	return &workerRuntime{
		scope:      scope,
		admission:  make(chan struct{}, disposableWorkerLimit+disposableWorkerQueue),
		execution:  make(chan struct{}, disposableWorkerLimit),
		executable: executable,
	}, nil
}

// Run admits one disposable command, then executes it under the daemon's
// process scope. A shortfall is never accepted: the worker protocol parses
// stdout as an encoded result, so a truncated stream is corruption.
func (runtime *workerRuntime) Run(
	ctx context.Context, request workerexec.CommandRequest,
) (workerexec.CommandResult, error) {
	if err := validateWorkerRequest(request); err != nil {
		return workerexec.CommandResult{}, err
	}
	select {
	case runtime.admission <- struct{}{}:
		defer func() { <-runtime.admission }()
	default:
		return workerexec.CommandResult{}, workerexec.ErrCapacity
	}
	runCtx, cancel := context.WithTimeout(ctx, request.TotalTimeout)
	defer cancel()
	select {
	case runtime.execution <- struct{}{}:
		defer func() { <-runtime.execution }()
	case <-runCtx.Done():
		return workerexec.CommandResult{}, runCause(runCtx.Err())
	}
	result, err := runtime.scope.Run(runCtx, daemonkit.Cmd{
		Path:      request.Path,
		Args:      request.Args,
		Dir:       request.Dir,
		Env:       request.Env,
		Stdin:     request.Stdin,
		MaxOutput: workerMaxOutput,
		Exec:      daemonkit.ServingSameUser(),
	})
	observed := workerexec.CommandResult{
		Stdout: result.Stdout, Stderr: result.Stderr, ExitCode: result.Exit.Code,
	}
	if err != nil {
		return observed, runCause(err)
	}
	return observed, nil
}

func validateWorkerRequest(request workerexec.CommandRequest) error {
	if len(request.Stdin) > workerMaxInput {
		return fmt.Errorf(
			"disposable worker stdin is %d bytes, over the %d maximum",
			len(request.Stdin), workerMaxInput,
		)
	}
	if request.TotalTimeout <= 0 || request.TotalTimeout > workerMaxTotalRun {
		return fmt.Errorf(
			"disposable worker total timeout %s must be positive and within %s",
			request.TotalTimeout, workerMaxTotalRun,
		)
	}
	return nil
}

func runCause(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(workerexec.ErrTimedOut, err)
	}
	return err
}
