package creds

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/daemonkit/supervise"
)

type testTaskRunner struct{}

func (testTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	if IsFileWorkerInvocation(task.Args) {
		return RunFileWorker(ctx, task.Stdin, task.Stdout)
	}
	if !filepath.IsAbs(task.Path) || filepath.Clean(task.Path) != task.Path {
		return errors.New("test task executable must be a clean absolute path")
	}
	// #nosec G204 -- task.Path is a clean absolute test fixture executable or /usr/bin/security.
	command := exec.CommandContext(ctx, task.Path, task.Args...)
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = task.Stdin
	command.Stdout = task.Stdout
	command.Stderr = task.Stderr
	return command.Run()
}
