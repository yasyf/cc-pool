package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const (
	writeWorkerArgument          = "__credential-write-worker"
	writePublicationProtocol     = "cc-pool.credential-write-publication.v1"
	credentialWriteWorkerTimeout = 30 * time.Second
	credentialWriteWorkerMaxIO   = 1 << 20
)

type credentialWritePublication struct {
	Protocol    string               `json:"protocol"`
	AccountUUID string               `json:"account_uuid,omitempty"`
	Chain       *hostsync.ChainStamp `json:"chain,omitempty"`
}

type credentialWriteWorkerRequest struct {
	OperationID      store.CredentialOperationID `json:"operation_id"`
	RegistryPath     string                      `json:"registry_path"`
	RegistryLockPath string                      `json:"registry_lock_path"`
	StampDir         string                      `json:"stamp_dir"`
	AccountUUID      string                      `json:"account_uuid"`
	Chain            hostsync.ChainStamp         `json:"chain"`
}

type credentialWriteWorkerResponse struct {
	OperationID store.CredentialOperationID `json:"operation_id"`
}

type credentialWriteSettler struct {
	runner           supervise.TaskRunner
	workerExecutable string
	enabled          func() (bool, error)
	registry         hostsync.RegistryFile
	stampDir         string
	self             string
}

func newCredentialWriteSettler(
	runner supervise.TaskRunner,
	workerExecutable string,
	enabled func() (bool, error),
	registry hostsync.RegistryFile,
	stampDir, self string,
) *credentialWriteSettler {
	return &credentialWriteSettler{
		runner: runner, workerExecutable: workerExecutable, enabled: enabled,
		registry: registry, stampDir: stampDir, self: self,
	}
}

func (settler *credentialWriteSettler) Settle(
	ctx context.Context,
	settlement pool.CredentialWriteSettlement,
) error {
	publication, err := decodeCredentialWritePublication(settlement.PublicationPayload)
	if err != nil {
		return err
	}
	if publication.Chain == nil {
		return nil
	}
	if settler.enabled == nil {
		return errors.New("credential write settlement policy is unavailable")
	}
	enabled, err := settler.enabled()
	if err != nil {
		return fmt.Errorf("read credential write settlement policy: %w", err)
	}
	if !enabled {
		return nil
	}
	if settlement.OperationID == (store.CredentialOperationID{}) {
		return errors.New("credential write settlement requires an operation ID")
	}
	if settler.runner == nil || settler.workerExecutable == "" {
		return errors.New("credential write settlement worker is unavailable")
	}
	request := credentialWriteWorkerRequest{
		OperationID:  settlement.OperationID,
		RegistryPath: settler.registry.Path, RegistryLockPath: settler.registry.LockPath,
		StampDir: settler.stampDir, AccountUUID: publication.AccountUUID,
		Chain: *publication.Chain,
	}
	response, err := settler.runWorker(ctx, request)
	if err != nil {
		return err
	}
	if response.OperationID != settlement.OperationID {
		return errors.New("credential write settlement worker returned a mismatched operation ID")
	}
	return nil
}

func credentialWritePublicationBuilder(self string) pool.CredentialWritePublicationBuilder {
	return func(
		account store.Account,
		credential *creds.Credential,
		operationID store.CredentialOperationID,
		committedAt time.Time,
	) ([]byte, error) {
		if operationID == (store.CredentialOperationID{}) {
			return nil, errors.New("credential write publication requires an operation ID")
		}
		publication := credentialWritePublication{
			Protocol: writePublicationProtocol,
		}
		if account.AccountUUID != "" && credential.HasRefreshToken() {
			publication.AccountUUID = account.AccountUUID
			publication.Chain = &hostsync.ChainStamp{
				Origin: self, ExpiresAt: credential.ClaudeAiOauth.ExpiresAt,
				Hash: creds.AccessHash(credential), RotatedAt: committedAt.UnixMilli(),
			}
		}
		return json.Marshal(publication)
	}
}

func decodeCredentialWritePublication(payload []byte) (credentialWritePublication, error) {
	var publication credentialWritePublication
	if len(payload) == 0 {
		return publication, errors.New("credential write settlement requires an exact publication payload")
	}
	if err := decodeCredentialWriteWorkerJSON(bytes.NewReader(payload), &publication); err != nil {
		return publication, fmt.Errorf("decode credential write publication: %w", err)
	}
	if publication.Protocol != writePublicationProtocol {
		return publication, errors.New("credential write publication protocol mismatch")
	}
	if publication.Chain == nil {
		if publication.AccountUUID != "" {
			return publication, errors.New("credential write no-op publication names an account")
		}
		return publication, nil
	}
	if publication.AccountUUID == "" || publication.Chain.Origin == "" || publication.Chain.RotatedAt == 0 {
		return publication, errors.New("credential write publication is incomplete")
	}
	return publication, nil
}

func (settler *credentialWriteSettler) runWorker(
	ctx context.Context,
	request credentialWriteWorkerRequest,
) (credentialWriteWorkerResponse, error) {
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return credentialWriteWorkerResponse{}, fmt.Errorf("encode credential write worker request: %w", err)
	}
	stdin, inputWriter, err := os.Pipe()
	if err != nil {
		return credentialWriteWorkerResponse{}, fmt.Errorf("create credential write worker input: %w", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := inputWriter.Write(input.Bytes())
		writeDone <- errors.Join(writeErr, inputWriter.Close())
	}()
	var output, stderr credentialWriteWorkerBuffer
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, credentialWriteWorkerTimeout)
		defer cancel()
	}
	runErr := settler.runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          settler.workerExecutable,
		Args:          []string{writeWorkerArgument},
		Stdin:         stdin,
		Stdout:        &output,
		Stderr:        &stderr,
	})
	_ = stdin.Close()
	_ = inputWriter.Close()
	writeErr := <-writeDone
	if err := errors.Join(runErr, writeErr); err != nil {
		return credentialWriteWorkerResponse{}, fmt.Errorf("credential write settlement worker: %w: %s", err, stderr.String())
	}
	if output.overflow {
		return credentialWriteWorkerResponse{}, errors.New("credential write settlement worker response exceeded its limit")
	}
	var response credentialWriteWorkerResponse
	if err := decodeCredentialWriteWorkerJSON(&output, &response); err != nil {
		return credentialWriteWorkerResponse{}, fmt.Errorf("decode credential write worker response: %w", err)
	}
	return response, nil
}

type credentialWriteWorkerBuffer struct {
	bytes.Buffer
	overflow bool
}

func (buffer *credentialWriteWorkerBuffer) Write(input []byte) (int, error) {
	length := len(input)
	remaining := credentialWriteWorkerMaxIO - buffer.Len()
	if len(input) > remaining {
		buffer.overflow = true
	}
	if remaining > 0 {
		if len(input) > remaining {
			input = input[:remaining]
		}
		_, _ = buffer.Buffer.Write(input)
	}
	return length, nil
}

// IsCredentialWriteWorkerInvocation reports whether args names exactly the
// credential-write settlement child role.
func IsCredentialWriteWorkerInvocation(args []string) bool {
	return len(args) == 1 && args[0] == writeWorkerArgument
}

// RunCredentialWriteWorker applies one exact host-sync credential publication.
func RunCredentialWriteWorker(ctx context.Context, input io.Reader, output io.Writer) error {
	var request credentialWriteWorkerRequest
	if err := decodeCredentialWriteWorkerJSON(input, &request); err != nil {
		return fmt.Errorf("decode credential write worker request: %w", err)
	}
	if err := validateCredentialWriteWorkerRequest(request); err != nil {
		return err
	}
	committedAt := time.UnixMilli(request.Chain.RotatedAt)
	service := &hostsync.Service{
		Registry: &hostsync.RegistryFile{
			Path: request.RegistryPath, LockPath: request.RegistryLockPath,
		},
		StampDir: request.StampDir,
		Now:      func() time.Time { return committedAt },
	}
	if err := service.NoteCredWrite(ctx, request.AccountUUID, request.Chain); err != nil {
		return fmt.Errorf("publish credential write: %w", err)
	}
	registry, err := service.Registry.Load()
	if err != nil {
		return err
	}
	entry, present := registry[request.AccountUUID]
	if present && entry.Present() && entry.Value.Chain == request.Chain {
		stampPath := filepath.Join(request.StampDir, request.AccountUUID, "stamp")
		expected := strconv.FormatInt(committedAt.UnixNano(), 10)
		stamp, readErr := os.ReadFile(stampPath) //nolint:gosec // validated cc-pool-owned worker path
		switch {
		case readErr == nil && string(stamp) == expected:
		case readErr == nil || errors.Is(readErr, os.ErrNotExist):
			if err := service.TouchStamp(request.AccountUUID); err != nil {
				return fmt.Errorf("publish credential write stamp: %w", err)
			}
		default:
			return fmt.Errorf("read credential write stamp: %w", readErr)
		}
		if err := syncCredentialWriteStamp(stampPath, request.StampDir); err != nil {
			return err
		}
	}
	return json.NewEncoder(output).Encode(credentialWriteWorkerResponse{
		OperationID: request.OperationID,
	})
}

func syncCredentialWriteStamp(path, stampDir string) error {
	file, err := os.Open(path) //nolint:gosec // validated cc-pool-owned worker path
	if err != nil {
		return fmt.Errorf("open credential write stamp: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync credential write stamp: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close credential write stamp: %w", err)
	}
	for _, directory := range []string{filepath.Dir(path), stampDir} {
		dir, err := os.Open(directory) //nolint:gosec // validated cc-pool-owned worker path
		if err != nil {
			return fmt.Errorf("open credential write stamp directory: %w", err)
		}
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("sync credential write stamp directory: %w", err)
		}
	}
	return nil
}

func validateCredentialWriteWorkerRequest(request credentialWriteWorkerRequest) error {
	if request.OperationID == (store.CredentialOperationID{}) {
		return errors.New("credential write worker requires an operation ID")
	}
	if request.AccountUUID == "" || request.AccountUUID == "." ||
		filepath.Base(request.AccountUUID) != request.AccountUUID {
		return errors.New("credential write worker requires a path-safe account UUID")
	}
	if request.Chain.Origin == "" {
		return errors.New("credential write worker requires an exact chain origin")
	}
	for name, path := range map[string]string{
		"registry": request.RegistryPath, "registry lock": request.RegistryLockPath,
		"stamp directory": request.StampDir,
	} {
		if path == "" || !filepath.IsAbs(path) {
			return fmt.Errorf("credential write worker requires an absolute %s path", name)
		}
	}
	registry := hostsync.NewRegistryFile(pool.SyncDir())
	if request.RegistryPath != registry.Path || request.RegistryLockPath != registry.LockPath ||
		request.StampDir != pool.SyncStampsDir() {
		return errors.New("credential write worker paths do not match the canonical sync layout")
	}
	return nil
}

func decodeCredentialWriteWorkerJSON(input io.Reader, value any) error {
	data, err := io.ReadAll(io.LimitReader(input, credentialWriteWorkerMaxIO+1))
	if err != nil {
		return err
	}
	if len(data) > credentialWriteWorkerMaxIO {
		return errors.New("credential write worker JSON exceeded its limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("credential write worker received multiple JSON values")
		}
		return err
	}
	return nil
}
