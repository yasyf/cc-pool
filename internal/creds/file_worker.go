package creds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	daemonproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const (
	credentialFileWorkerArgument = "__credential-file-worker"
	credentialFileTaskTimeout    = 30 * time.Second
)

type credentialFileOperation string

const (
	credentialFileRead   credentialFileOperation = "read"
	credentialFileWrite  credentialFileOperation = "write"
	credentialFileDelete credentialFileOperation = "delete"
)

type credentialFileRequest struct {
	Operation credentialFileOperation `json:"operation"`
	ConfigDir string                  `json:"config_dir"`
	Data      []byte                  `json:"data,omitempty"`
}

type credentialFileResponse struct {
	Data      []byte `json:"data,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

func (f FileStore) run(
	ctx context.Context,
	operation credentialFileOperation,
	credential *Credential,
) (*Credential, error) {
	if f.Runner == nil || f.WorkerExecutable == "" {
		return nil, errors.New("credential file worker runner and executable are required")
	}
	request := credentialFileRequest{Operation: operation, ConfigDir: f.ConfigDir}
	if credential != nil {
		data, err := credential.Marshal()
		if err != nil {
			return nil, err
		}
		request.Data = data
	}
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return nil, fmt.Errorf("encode credential file worker request: %w", err)
	}
	var output, stderr boundedBuffer
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, credentialFileTaskTimeout)
		defer cancel()
	}
	if err := runCredentialFileTask(
		ctx,
		f.Runner,
		f.WorkerExecutable,
		input.Bytes(),
		&output,
		&stderr,
	); err != nil {
		return nil, fmt.Errorf("credential file worker: %w: %s", err, stderr.String())
	}
	var response credentialFileResponse
	if err := json.NewDecoder(&output).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode credential file worker response: %w", err)
	}
	switch response.ErrorCode {
	case "":
	case "not_found":
		return nil, ErrNotFound
	case "no_tokens":
		return nil, ErrNoTokens
	default:
		return nil, errors.New(response.Error)
	}
	if operation != credentialFileRead {
		return nil, nil
	}
	return parseCredential(response.Data)
}

// IsFileWorkerInvocation reports whether args request the credential file worker.
func IsFileWorkerInvocation(args []string) bool {
	return len(args) == 1 && args[0] == credentialFileWorkerArgument
}

// RunFileWorker serves one credential file request in a disposable child.
func RunFileWorker(_ context.Context, input io.Reader, output io.Writer) error {
	var request credentialFileRequest
	if err := json.NewDecoder(io.LimitReader(input, maxSecurityOutput+1)).Decode(&request); err != nil {
		return fmt.Errorf("decode credential file request: %w", err)
	}
	response := credentialFileResponse{}
	var err error
	switch request.Operation {
	case credentialFileRead:
		response.Data, err = os.ReadFile(FileCredentialPath(request.ConfigDir)) //nolint:gosec // child reads one cc-pool-owned account credential
	case credentialFileWrite:
		err = writeCredentialFile(FileCredentialPath(request.ConfigDir), request.Data)
	case credentialFileDelete:
		err = os.Remove(FileCredentialPath(request.ConfigDir))
	default:
		return errors.New("invalid credential file operation")
	}
	if errors.Is(err, os.ErrNotExist) {
		err = nil
		if request.Operation == credentialFileDelete {
		} else {
			response.ErrorCode = "not_found"
		}
	}
	if err != nil {
		response.ErrorCode = "io"
		response.Error = err.Error()
	}
	return json.NewEncoder(output).Encode(response)
}

func runCredentialFileTask(
	ctx context.Context,
	runner TaskRunner,
	executable string,
	input []byte,
	stdout, stderr io.Writer,
) error {
	stdin, inputWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create credential worker input pipe: %w", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := inputWriter.Write(input)
		writeDone <- errors.Join(writeErr, inputWriter.Close())
	}()
	runErr := runner.Run(ctx, supervise.Task{
		RecoveryClass: daemonproc.RecoveryTask,
		Path:          executable,
		Args:          []string{credentialFileWorkerArgument},
		Stdin:         stdin,
		Stdout:        stdout,
		Stderr:        stderr,
	})
	_ = stdin.Close()
	writeErr := <-writeDone
	return errors.Join(runErr, writeErr)
}
