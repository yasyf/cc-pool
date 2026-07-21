package creds

import (
	"context"
	"os/exec"

	"github.com/yasyf/daemonkit/supervise"
)

type testTaskRunner struct{}

func (testTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	if IsFileWorkerInvocation(task.Args) {
		return RunFileWorker(ctx, task.Stdin, task.Stdout)
	}
	command := exec.CommandContext(ctx, task.Path, task.Args...)
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = task.Stdin
	command.Stdout = task.Stdout
	command.Stderr = task.Stderr
	return command.Run()
}
