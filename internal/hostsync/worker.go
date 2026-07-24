package hostsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/synckit/syncservice"
)

const (
	hostSyncWorkerArgument = "__host-sync-worker"
	hostSyncWorkerProtocol = "cc-pool.hostsync.worker.v1"
	maxHostSyncWorkerFrame = 12 << 20
	hostSyncWorkerTimeout  = 2 * time.Minute
)

var (
	// ErrAuthKindUnverified means the worker could not prove exact chain ownership.
	ErrAuthKindUnverified = errors.New("hostsync: auth-kind ownership is unverified")
	// ErrAuthKindOriginMissing means a present registry chain names no owner.
	ErrAuthKindOriginMissing = errors.New("hostsync: auth-kind registry origin is missing")
	// ErrAuthKindOriginForeign means a registry chain owner is outside the exact mesh.
	ErrAuthKindOriginForeign = errors.New("hostsync: auth-kind registry origin is foreign")
)

type workerOperation string

const (
	workerCapabilities workerOperation = "capabilities"
	workerList         workerOperation = "list"
	workerReconcile    workerOperation = "reconcile"
	workerExport       workerOperation = "export"
	workerApply        workerOperation = "apply"
	workerAuthKind     workerOperation = "auth-kind"
)

type workerRequest struct {
	Protocol  string          `json:"protocol"`
	Operation workerOperation `json:"operation"`
	Params    json.RawMessage `json:"params"`
}

type workerResponse struct {
	Protocol  string          `json:"protocol"`
	Operation workerOperation `json:"operation"`
	Result    json.RawMessage `json:"result,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
	Error     string          `json:"error,omitempty"`
}

type workerOriginParams struct {
	Origin string `json:"origin"`
}

type workerAuthKindParams struct {
	AccountID   int    `json:"account_id"`
	AccountUUID string `json:"account_uuid"`
}

// WorkerRuntime is the complete host-sync operation surface reconstructed in
// the disposable child.
type WorkerRuntime struct {
	Consumer syncservice.SyncConsumer
	AuthKind func(context.Context, int, string) (store.AuthKind, error)
}

// WorkerRuntimeScope owns operation-specific resources until run returns.
type WorkerRuntimeScope func(
	ctx context.Context,
	run func(WorkerRuntime) error,
) error

// WorkerClient executes each host-sync operation in one daemonkit worker group.
type WorkerClient struct {
	runner     workerexec.Runner
	executable string
	lane       chan struct{}
	timeout    time.Duration
}

// NewWorkerClient binds host-sync operations to one daemonkit worker pool.
func NewWorkerClient(runner workerexec.Runner, executable string) (*WorkerClient, error) {
	if runner == nil || executable == "" {
		return nil, errors.New("hostsync: worker runner and executable are required")
	}
	return &WorkerClient{
		runner: runner, executable: executable, lane: make(chan struct{}, 1),
		timeout: hostSyncWorkerTimeout,
	}, nil
}

// Capabilities executes the complete capability read in a disposable child.
func (c *WorkerClient) Capabilities(ctx context.Context) (syncservice.Capabilities, error) {
	var result syncservice.Capabilities
	err := c.do(ctx, workerCapabilities, struct{}{}, &result)
	return result, err
}

// List executes the complete watch-list read in a disposable child.
func (c *WorkerClient) List(ctx context.Context) ([]syncservice.WatchItem, error) {
	var result []syncservice.WatchItem
	err := c.do(ctx, workerList, struct{}{}, &result)
	return result, err
}

// Reconcile executes one complete convergence pass in a disposable child.
func (c *WorkerClient) Reconcile(ctx context.Context, origin string) (syncservice.ReconcileResult, error) {
	var result syncservice.ReconcileResult
	err := c.do(ctx, workerReconcile, workerOriginParams{Origin: origin}, &result)
	return result, err
}

// Export reads one immutable revisioned snapshot in a disposable child.
func (c *WorkerClient) Export(ctx context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	var result syncservice.ChangeEnvelope
	err := c.do(ctx, workerExport, request, &result)
	return result, err
}

// Apply merges and reconciles one delivery-bound change in a disposable child.
func (c *WorkerClient) Apply(ctx context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	var result syncservice.ApplyResult
	err := c.do(ctx, workerApply, change, &result)
	return result, err
}

// AuthKind classifies one account from child-owned registry and credential reads.
func (c *WorkerClient) AuthKind(ctx context.Context, accountID int, uuid string) (store.AuthKind, error) {
	var result store.AuthKind
	err := c.do(ctx, workerAuthKind, workerAuthKindParams{
		AccountID: accountID, AccountUUID: uuid,
	}, &result)
	return result, err
}

func (c *WorkerClient) do(ctx context.Context, operation workerOperation, params, result any) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	select {
	case c.lane <- struct{}{}:
		defer func() { <-c.lane }()
	case <-ctx.Done():
		return ctx.Err()
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("hostsync: encode %s params: %w", operation, err)
	}
	request := workerRequest{
		Protocol: hostSyncWorkerProtocol, Operation: operation, Params: paramsJSON,
	}
	var framed bytes.Buffer
	if err := writeWorkerFrame(&framed, request); err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return errors.New("hostsync: resolve exact worker home")
	}
	command, runErr := c.runner.Run(ctx, worker.CommandRequest{
		Path: c.executable, Dir: workerexec.TempDir(), Args: []string{hostSyncWorkerArgument},
		Env: []string{"HOME=" + home}, Stdin: framed.Bytes(), TotalTimeout: c.timeout,
	})
	if runErr != nil {
		return fmt.Errorf("hostsync: %s worker: %w: %s", operation, runErr, string(command.Stderr))
	}
	var response workerResponse
	if err := readWorkerFrame(bytes.NewReader(command.Stdout), &response); err != nil {
		return fmt.Errorf("hostsync: decode %s worker response: %w", operation, err)
	}
	if response.Protocol != hostSyncWorkerProtocol || response.Operation != operation {
		return errors.New("hostsync: worker response identity mismatch")
	}
	if response.Error != "" || response.ErrorCode != "" {
		if response.Error == "" {
			return errors.New("hostsync: worker returned an empty coded error")
		}
		switch response.ErrorCode {
		case "sync-disabled":
			return ErrSyncDisabled
		case "auth-kind-origin-missing":
			return fmt.Errorf("%w: %s", ErrAuthKindOriginMissing, response.Error)
		case "auth-kind-origin-foreign":
			return fmt.Errorf("%w: %s", ErrAuthKindOriginForeign, response.Error)
		case "auth-kind-unverified":
			return fmt.Errorf("%w: %s", ErrAuthKindUnverified, response.Error)
		case "":
			return errors.New(response.Error)
		default:
			return errors.New("hostsync: worker returned an unknown error code")
		}
	}
	if len(response.Result) == 0 {
		return errors.New("hostsync: worker returned no result")
	}
	if err := decodeExactJSON(response.Result, result); err != nil {
		return fmt.Errorf("hostsync: decode %s result: %w", operation, err)
	}
	return nil
}

// IsWorkerInvocation reports whether args request the exact host-sync worker role.
func IsWorkerInvocation(args []string) bool {
	return len(args) == 1 && args[0] == hostSyncWorkerArgument
}

// RunWorker serves one exact, bounded host-sync worker operation.
func RunWorker(ctx context.Context, input io.Reader, output io.Writer, scope WorkerRuntimeScope) error {
	if scope == nil {
		return errors.New("hostsync: worker runtime scope is required")
	}
	var request workerRequest
	if err := readWorkerFrame(input, &request); err != nil {
		return fmt.Errorf("hostsync: decode worker request: %w", err)
	}
	if request.Protocol != hostSyncWorkerProtocol {
		return errors.New("hostsync: worker protocol mismatch")
	}
	var result any
	err := scope(ctx, func(runtime WorkerRuntime) error {
		if runtime.Consumer == nil || runtime.AuthKind == nil {
			return errors.New("hostsync: complete worker runtime is required")
		}
		var executeErr error
		result, executeErr = executeWorkerOperation(ctx, runtime, request)
		return executeErr
	})
	response := workerResponse{
		Protocol: hostSyncWorkerProtocol, Operation: request.Operation,
	}
	if err != nil {
		response.Error = err.Error()
		switch {
		case errors.Is(err, ErrSyncDisabled):
			response.ErrorCode = "sync-disabled"
		case errors.Is(err, ErrAuthKindOriginMissing):
			response.ErrorCode = "auth-kind-origin-missing"
		case errors.Is(err, ErrAuthKindOriginForeign):
			response.ErrorCode = "auth-kind-origin-foreign"
		case request.Operation == workerAuthKind:
			response.ErrorCode = "auth-kind-unverified"
		}
	} else {
		response.Result, err = json.Marshal(result)
		if err != nil {
			return fmt.Errorf("hostsync: encode %s result: %w", request.Operation, err)
		}
	}
	return writeWorkerFrame(output, response)
}

func executeWorkerOperation(ctx context.Context, runtime WorkerRuntime, request workerRequest) (any, error) {
	switch request.Operation {
	case workerCapabilities:
		if err := requireEmptyParams(request.Params); err != nil {
			return nil, err
		}
		return runtime.Consumer.Capabilities(ctx)
	case workerList:
		if err := requireEmptyParams(request.Params); err != nil {
			return nil, err
		}
		return runtime.Consumer.List(ctx)
	case workerReconcile:
		var params workerOriginParams
		if err := decodeExactJSON(request.Params, &params); err != nil {
			return nil, err
		}
		return runtime.Consumer.Reconcile(ctx, params.Origin)
	case workerExport:
		var params syncservice.ExportRequest
		if err := decodeExactJSON(request.Params, &params); err != nil {
			return nil, err
		}
		return runtime.Consumer.Export(ctx, params)
	case workerApply:
		var params syncservice.ChangeEnvelope
		if err := decodeExactJSON(request.Params, &params); err != nil {
			return nil, err
		}
		return runtime.Consumer.Apply(ctx, params)
	case workerAuthKind:
		var params workerAuthKindParams
		if err := decodeExactJSON(request.Params, &params); err != nil {
			return nil, err
		}
		if params.AccountID <= 0 || params.AccountUUID == "" {
			return nil, errors.New("hostsync: auth-kind requires an exact account")
		}
		return runtime.AuthKind(ctx, params.AccountID, params.AccountUUID)
	default:
		return nil, errors.New("hostsync: invalid worker operation")
	}
}

func requireEmptyParams(raw json.RawMessage) error {
	var params struct{}
	return decodeExactJSON(raw, &params)
}

func writeWorkerFrame(output io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("hostsync: marshal worker frame: %w", err)
	}
	payloadSize := len(payload)
	if payloadSize == 0 || payloadSize > maxHostSyncWorkerFrame || uint64(payloadSize) > math.MaxUint32 {
		return errors.New("hostsync: worker frame size is invalid")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(payloadSize))
	if _, err := output.Write(header[:]); err != nil {
		return fmt.Errorf("hostsync: write worker frame header: %w", err)
	}
	if _, err := output.Write(payload); err != nil {
		return fmt.Errorf("hostsync: write worker frame payload: %w", err)
	}
	return nil
}

func readWorkerFrame(input io.Reader, value any) error {
	var header [4]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return err
	}
	size := int(binary.BigEndian.Uint32(header[:]))
	if size < 1 || size > maxHostSyncWorkerFrame {
		return errors.New("hostsync: worker frame size is invalid")
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(input, payload); err != nil {
		return err
	}
	var trailing [1]byte
	n, err := input.Read(trailing[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		return errors.New("hostsync: worker frame has trailing bytes")
	}
	return decodeExactJSON(payload, value)
}

func decodeExactJSON(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("hostsync: worker JSON has trailing values")
	}
	return nil
}

var _ syncservice.SyncConsumer = (*WorkerClient)(nil)
