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
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

const (
	hostSyncWorkerArgument = "__host-sync-worker"
	hostSyncWorkerProtocol = "cc-pool.hostsync.worker.v1"
	maxHostSyncWorkerFrame = 8 << 20
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
	workerSync         workerOperation = "sync"
	workerGetState     workerOperation = "get-state"
	workerFetch        workerOperation = "fetch-credential"
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
	Fetch    rpc.Handler
	AuthKind func(context.Context, int, string) (store.AuthKind, error)
}

// WorkerClient executes each host-sync operation in one daemonkit worker group.
type WorkerClient struct {
	runner     supervise.TaskRunner
	executable string
	lane       chan struct{}
	timeout    time.Duration
}

// NewWorkerClient binds host-sync operations to one daemonkit worker pool.
func NewWorkerClient(runner supervise.TaskRunner, executable string) (*WorkerClient, error) {
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

// Sync executes one complete convergence pass in a disposable child.
func (c *WorkerClient) Sync(ctx context.Context, origin string) (syncservice.SyncResult, error) {
	var result syncservice.SyncResult
	err := c.do(ctx, workerSync, workerOriginParams{Origin: origin}, &result)
	return result, err
}

// GetState executes the complete registry read in a disposable child.
func (c *WorkerClient) GetState(ctx context.Context) (syncservice.RawRegistry, error) {
	var result syncservice.RawRegistry
	err := c.do(ctx, workerGetState, struct{}{}, &result)
	return result, err
}

// FetchCredentialHandler executes one complete credential fetch in a disposable child.
func (c *WorkerClient) FetchCredentialHandler(ctx context.Context, params map[string]any) (any, error) {
	var result any
	err := c.do(ctx, workerFetch, params, &result)
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
	input, inputWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("hostsync: create worker input pipe: %w", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := inputWriter.Write(framed.Bytes())
		writeDone <- errors.Join(writeErr, inputWriter.Close())
	}()
	var output, stderr boundedWorkerBuffer
	runErr := c.runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoverySourceOwner,
		Path:          c.executable,
		Args:          []string{hostSyncWorkerArgument},
		Stdin:         input,
		Stdout:        &output,
		Stderr:        &stderr,
	})
	_ = input.Close()
	_ = inputWriter.Close()
	writeErr := <-writeDone
	if err := errors.Join(runErr, writeErr); err != nil {
		return fmt.Errorf("hostsync: %s worker: %w: %s", operation, err, stderr.String())
	}
	var response workerResponse
	if err := readWorkerFrame(bytes.NewReader(output.Bytes()), &response); err != nil {
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
func RunWorker(ctx context.Context, input io.Reader, output io.Writer, runtime WorkerRuntime) error {
	if runtime.Consumer == nil || runtime.Fetch == nil || runtime.AuthKind == nil {
		return errors.New("hostsync: complete worker runtime is required")
	}
	var request workerRequest
	if err := readWorkerFrame(input, &request); err != nil {
		return fmt.Errorf("hostsync: decode worker request: %w", err)
	}
	if request.Protocol != hostSyncWorkerProtocol {
		return errors.New("hostsync: worker protocol mismatch")
	}
	result, err := executeWorkerOperation(ctx, runtime, request)
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
	case workerSync:
		var params workerOriginParams
		if err := decodeExactJSON(request.Params, &params); err != nil {
			return nil, err
		}
		return runtime.Consumer.Sync(ctx, params.Origin)
	case workerGetState:
		if err := requireEmptyParams(request.Params); err != nil {
			return nil, err
		}
		return runtime.Consumer.GetState(ctx)
	case workerFetch:
		var params map[string]any
		if err := decodeExactJSON(request.Params, &params); err != nil {
			return nil, err
		}
		return runtime.Fetch(ctx, params)
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

type boundedWorkerBuffer struct {
	bytes.Buffer
}

func (buffer *boundedWorkerBuffer) Write(payload []byte) (int, error) {
	if len(payload) > maxHostSyncWorkerFrame+4-buffer.Len() {
		return 0, errors.New("hostsync: worker output exceeds frame bound")
	}
	return buffer.Buffer.Write(payload)
}

var _ syncservice.SyncConsumer = (*WorkerClient)(nil)
