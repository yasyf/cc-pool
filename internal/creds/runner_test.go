package creds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/yasyf/daemonkit/worker"
)

type testTaskRunner struct{}

func (testTaskRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	if !filepath.IsAbs(task.Path) || filepath.Clean(task.Path) != task.Path {
		return worker.CommandResult{}, errors.New("test task executable must be a clean absolute path")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return worker.CommandResult{}, err
	}
	if len(task.Env) != 1 || task.Env[0] != "HOME="+home {
		return worker.CommandResult{}, fmt.Errorf("test task environment is not exact: %v", task.Env)
	}
	// #nosec G204 -- task.Path is a clean absolute test fixture executable or /usr/bin/security.
	command := exec.CommandContext(ctx, task.Path, task.Args...)
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = bytes.NewReader(task.Stdin)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err = command.Run()
	return worker.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, err
}
