package procscan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/worker"
)

const workerArgument = "__procscan-worker"

const procscanWorkerTimeout = 30 * time.Second

type workerRequest struct {
	Executable string `json:"executable,omitempty"`
}

type workerResponse struct {
	Snapshot Snapshot `json:"snapshot,omitzero"`
	Procs    []Proc   `json:"procs,omitempty"`
	Error    string   `json:"error,omitempty"`
}

// WorkerScanner executes context-unaware process inspection in disposable workers.
type WorkerScanner struct {
	runner     workerexec.Runner
	executable string
}

// NewWorkerScanner binds process inspection to one daemonkit worker pool.
func NewWorkerScanner(runner workerexec.Runner, executable string) (*WorkerScanner, error) {
	if runner == nil || executable == "" {
		return nil, errors.New("procscan: worker runner and executable are required")
	}
	return &WorkerScanner{runner: runner, executable: executable}, nil
}

// Scan executes one complete session scan in a disposable worker.
func (s *WorkerScanner) Scan(ctx context.Context) ([]Session, error) {
	snapshot, err := s.Snapshot(ctx)
	return snapshot.Sessions, err
}

// Snapshot executes one complete process observation in a disposable worker.
func (s *WorkerScanner) Snapshot(ctx context.Context) (Snapshot, error) {
	response, err := s.run(ctx, workerRequest{})
	return response.Snapshot, err
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
	input, err := json.Marshal(request)
	if err != nil {
		return workerResponse{}, err
	}
	result, runErr := s.runner.Run(ctx, worker.CommandRequest{
		Path: s.executable, Dir: workerexec.TempDir(), Args: []string{workerArgument},
		Stdin: input, TotalTimeout: procscanWorkerTimeout,
	})
	if runErr != nil {
		return workerResponse{}, fmt.Errorf("procscan worker: %w: %s", runErr, string(result.Stderr))
	}
	var response workerResponse
	if err := json.NewDecoder(bytes.NewReader(result.Stdout)).Decode(&response); err != nil {
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
		response.Snapshot, err = scanSnapshot(ctx, listProcs, procArgs)
	} else {
		response.Procs, err = byExecutable(ctx, request.Executable, listProcs, procArgs)
	}
	if err != nil {
		response.Error = err.Error()
	}
	return json.NewEncoder(output).Encode(response)
}
