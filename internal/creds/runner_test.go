package creds

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/daemonkit/worker"
)

type testTaskRunner struct{}

func (testTaskRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	if !filepath.IsAbs(task.Path) || filepath.Clean(task.Path) != task.Path {
		return worker.CommandResult{}, errors.New("test task executable must be a clean absolute path")
	}
	// #nosec G204 -- task.Path is a clean absolute test fixture executable or /usr/bin/security.
	command := exec.CommandContext(ctx, task.Path, task.Args...)
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = bytes.NewReader(task.Stdin)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return worker.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, err
}
