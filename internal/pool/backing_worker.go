package pool

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
	backingWorkerArgument = "__account-backing-worker"
	backingWorkerTimeout  = 30 * time.Second
	maxBackingWorkerIO    = 1 << 20
)

type backingWorkerOperation string

const (
	backingWorkerPrepare       backingWorkerOperation = "prepare"
	backingWorkerReadIdentity  backingWorkerOperation = "read-identity"
	backingWorkerReadOAuth     backingWorkerOperation = "read-oauth"
	backingWorkerWriteIdentity backingWorkerOperation = "write-identity"
	backingWorkerRemove        backingWorkerOperation = "remove"
)

type backingWorkerRequest struct {
	Operation    backingWorkerOperation `json:"operation"`
	AccountID    int                    `json:"account_id"`
	ConfigDir    string                 `json:"config_dir"`
	SourcePath   string                 `json:"source_path,omitempty"`
	OAuthAccount json.RawMessage        `json:"oauth_account,omitempty"`
}

type backingWorkerResponse struct {
	Seed         SeedOutcome     `json:"seed,omitempty"`
	OAuthAccount json.RawMessage `json:"oauth_account,omitempty"`
	Identity     *Identity       `json:"identity,omitempty"`
	ErrorCode    string          `json:"error_code,omitempty"`
	Error        string          `json:"error,omitempty"`
}

func (m *Manager) runBackingWorker(
	ctx context.Context,
	request backingWorkerRequest,
) (backingWorkerResponse, error) {
	if m.taskRunner == nil || m.workerExecutable == "" {
		return backingWorkerResponse{}, errors.New("account backing worker is unavailable")
	}
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return backingWorkerResponse{}, fmt.Errorf("encode backing worker request: %w", err)
	}
	stdin, inputWriter, err := os.Pipe()
	if err != nil {
		return backingWorkerResponse{}, fmt.Errorf("create backing worker input pipe: %w", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := inputWriter.Write(input.Bytes())
		writeDone <- errors.Join(writeErr, inputWriter.Close())
	}()
	var output, stderr boundedBackingBuffer
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, backingWorkerTimeout)
		defer cancel()
	}
	runErr := m.taskRunner.Run(ctx, supervise.Task{
		RecoveryClass: daemonproc.RecoveryTask,
		Path:          m.workerExecutable,
		Args:          []string{backingWorkerArgument},
		Stdin:         stdin,
		Stdout:        &output,
		Stderr:        &stderr,
	})
	_ = stdin.Close()
	if runErr != nil {
		_ = inputWriter.Close()
	}
	writeErr := <-writeDone
	if runErr != nil && errors.Is(writeErr, os.ErrClosed) {
		writeErr = nil
	}
	if err := errors.Join(runErr, writeErr); err != nil {
		return backingWorkerResponse{}, fmt.Errorf("account backing worker: %w: %s", err, stderr.String())
	}
	var response backingWorkerResponse
	if err := json.NewDecoder(&output).Decode(&response); err != nil {
		return backingWorkerResponse{}, fmt.Errorf("decode backing worker response: %w", err)
	}
	switch response.ErrorCode {
	case "":
		return response, nil
	case "no_identity":
		return backingWorkerResponse{}, ErrNoIdentity
	default:
		return backingWorkerResponse{}, errors.New(response.Error)
	}
}

func (m *Manager) prepareAccountBacking(
	ctx context.Context,
	accountID int,
	configDir, sourcePath string,
) (SeedOutcome, error) {
	response, err := m.runBackingWorker(ctx, backingWorkerRequest{
		Operation:  backingWorkerPrepare,
		AccountID:  accountID,
		ConfigDir:  configDir,
		SourcePath: sourcePath,
	})
	if err != nil {
		return "", err
	}
	switch response.Seed {
	case SeedCopied, SeedNoSource, SeedKeptExisting:
		return response.Seed, nil
	default:
		return "", errors.New("account backing worker returned an invalid seed outcome")
	}
}

func (m *Manager) removeAccountBacking(ctx context.Context, accountID int, configDir string) error {
	_, err := m.runBackingWorker(ctx, backingWorkerRequest{
		Operation: backingWorkerRemove,
		AccountID: accountID,
		ConfigDir: configDir,
	})
	return err
}

type boundedBackingBuffer struct {
	bytes.Buffer
}

func (buffer *boundedBackingBuffer) Write(p []byte) (int, error) {
	original := len(p)
	remaining := maxBackingWorkerIO - buffer.Len()
	if remaining <= 0 {
		return original, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	_, err := buffer.Buffer.Write(p)
	return original, err
}

// IsBackingWorkerInvocation reports whether args request account backing I/O.
func IsBackingWorkerInvocation(args []string) bool {
	return len(args) == 1 && args[0] == backingWorkerArgument
}

// RunBackingWorker serves one validated account backing request.
func RunBackingWorker(_ context.Context, input io.Reader, output io.Writer) error {
	var request backingWorkerRequest
	if err := json.NewDecoder(io.LimitReader(input, maxBackingWorkerIO+1)).Decode(&request); err != nil {
		return fmt.Errorf("decode account backing request: %w", err)
	}
	response := backingWorkerResponse{}
	backing, err := accountBackingPath(request.AccountID, request.ConfigDir)
	if err == nil {
		switch request.Operation {
		case backingWorkerPrepare:
			if request.SourcePath != ClaudeJSONPath() {
				err = errors.New("account backing seed source is not canonical .claude.json")
				break
			}
			response.Seed, err = prepareAccountBackingDirect(backing, request.SourcePath)
		case backingWorkerReadIdentity:
			response.Identity, err = accountIdentityDirect(backing)
		case backingWorkerReadOAuth:
			response.OAuthAccount, response.Identity, err = accountOAuthDirect(backing)
		case backingWorkerWriteIdentity:
			err = writeIdentityDirect(backing, request.OAuthAccount)
		case backingWorkerRemove:
			err = removeAccountBackingDirect(backing)
		default:
			err = errors.New("invalid account backing operation")
		}
	}
	if errors.Is(err, ErrNoIdentity) {
		response.ErrorCode = "no_identity"
	} else if err != nil {
		response.ErrorCode = "io"
		response.Error = err.Error()
	}
	return json.NewEncoder(output).Encode(response)
}

func accountBackingPath(accountID int, configDir string) (string, error) {
	if accountID < 1 {
		return "", errors.New("account backing request requires an account ID")
	}
	want := AccountDir(accountID)
	if configDir != want {
		return "", fmt.Errorf("account config dir %q does not match %q", configDir, want)
	}
	return AccountBackingDir(accountID), nil
}

func prepareAccountBackingDirect(backing, sourcePath string) (SeedOutcome, error) {
	info, err := os.Lstat(backing)
	switch {
	case err == nil:
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("account backing %s is not a private directory", backing)
		}
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(backing, 0o700); err != nil {
			return "", fmt.Errorf("create account backing %s: %w", backing, err)
		}
	default:
		return "", fmt.Errorf("inspect account backing %s: %w", backing, err)
	}
	return seedClaudeJSON(backing, sourcePath)
}

func removeAccountBackingDirect(backing string) error {
	info, err := os.Lstat(backing)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect account backing %s: %w", backing, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse to remove non-directory account backing %s", backing)
	}
	if err := os.RemoveAll(backing); err != nil {
		return fmt.Errorf("remove account backing %s: %w", backing, err)
	}
	return nil
}
