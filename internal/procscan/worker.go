package procscan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	daemonproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const workerArgument = "__procscan-worker"

const maxWorkerOutput = 1 << 20

type boundedWorkerBuffer struct {
	bytes.Buffer
}

func (buffer *boundedWorkerBuffer) Write(p []byte) (int, error) {
	original := len(p)
	remaining := maxWorkerOutput - buffer.Len()
	if remaining <= 0 {
		return original, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	_, err := buffer.Buffer.Write(p)
	return original, err
}

type workerRunner interface {
	Run(context.Context, supervise.Task) error
}

type workerRequest struct {
	Executable string `json:"executable,omitempty"`
}

type workerResponse struct {
	Sessions []Session `json:"sessions,omitempty"`
	Procs    []Proc    `json:"procs,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// WorkerScanner executes context-unaware process inspection in disposable workers.
type WorkerScanner struct {
	runner     workerRunner
	executable string
}

// NewWorkerScanner binds process inspection to one daemonkit worker pool.
func NewWorkerScanner(runner workerRunner, executable string) (*WorkerScanner, error) {
	if runner == nil || executable == "" {
		return nil, errors.New("procscan: worker runner and executable are required")
	}
	return &WorkerScanner{runner: runner, executable: executable}, nil
}

// Scan executes one complete session scan in a disposable worker.
func (s *WorkerScanner) Scan(ctx context.Context) ([]Session, error) {
	response, err := s.run(ctx, workerRequest{})
	return response.Sessions, err
}

// ProcsByExecutable executes one exact-path scan in a disposable worker.
func (s *WorkerScanner) ProcsByExecutable(ctx context.Context, executable string) ([]Proc, error) {
	if executable == "" {
		return nil, nil
	}
	response, err := s.run(ctx, workerRequest{Executable: executable})
	return response.Procs, err
}

func (s *WorkerScanner) run(ctx context.Context, request workerRequest) (workerResponse, error) {
	input, err := os.CreateTemp("", "cc-pool-procscan-*.json")
	if err != nil {
		return workerResponse{}, err
	}
	name := input.Name()
	defer func() { _ = os.Remove(name) }()
	if err := json.NewEncoder(input).Encode(request); err != nil {
		_ = input.Close()
		return workerResponse{}, err
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		_ = input.Close()
		return workerResponse{}, err
	}
	var output, stderr boundedWorkerBuffer
	runErr := s.runner.Run(ctx, supervise.Task{
		RecoveryClass: daemonproc.RecoveryTask,
		Path:          s.executable, Args: []string{workerArgument}, Stdin: input, Stdout: &output, Stderr: &stderr,
	})
	_ = input.Close()
	if runErr != nil {
		return workerResponse{}, fmt.Errorf("procscan worker: %w: %s", runErr, stderr.String())
	}
	var response workerResponse
	if err := json.NewDecoder(&output).Decode(&response); err != nil {
		return workerResponse{}, fmt.Errorf("decode procscan worker response: %w", err)
	}
	if response.Error != "" {
		return workerResponse{}, errors.New(response.Error)
	}
	return response, nil
}

// IsWorkerInvocation reports whether args request the internal disposable worker.
func IsWorkerInvocation(args []string) bool {
	return len(args) == 1 && args[0] == workerArgument
}

// RunWorker serves one internal disposable worker request.
func RunWorker(ctx context.Context, input io.Reader, output io.Writer) error {
	var request workerRequest
	if err := json.NewDecoder(input).Decode(&request); err != nil {
		return err
	}
	response := workerResponse{}
	var err error
	if request.Executable == "" {
		response.Sessions, err = scan(ctx, listProcs, procArgs)
	} else {
		response.Procs, err = byExecutable(ctx, request.Executable, listProcs, procArgs)
	}
	if err != nil {
		response.Error = err.Error()
	}
	return json.NewEncoder(output).Encode(response)
}
