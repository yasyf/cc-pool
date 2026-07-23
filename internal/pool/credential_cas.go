package pool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	daemonproc "github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const (
	casWorkerArgument          = "__credential-cas-worker"
	credentialCASWorkerTimeout = 30 * time.Second
	credentialCASWorkerMaxIO   = 1 << 20
)

var errCredentialCASConflict = errors.New("credential changed before compare-and-swap")

// CredentialCASRequest is the exact v1 request accepted by the disposable
// credential compare-and-swap worker.
type CredentialCASRequest struct {
	AccountID       int                           `json:"account_id"`
	ConfigDir       string                        `json:"config_dir"`
	KeychainService string                        `json:"keychain_service"`
	KeychainAccount string                        `json:"keychain_account"`
	Source          creds.Source                  `json:"source"`
	Expected        store.CredentialExternalState `json:"expected"`
	Credential      []byte                        `json:"credential"`
	DeleteOther     bool                          `json:"delete_other"`
	DeleteTarget    bool                          `json:"delete_target"`
	DeleteAll       bool                          `json:"delete_all"`
	Refresh         bool                          `json:"refresh"`
	UserAgent       string                        `json:"user_agent,omitempty"`
	RollbackTarget  []byte                        `json:"rollback_target,omitempty"`
}

// CredentialCASResponse is the exact v1 result emitted by the disposable
// credential compare-and-swap worker.
type CredentialCASResponse struct {
	Before              store.CredentialExternalState `json:"before"`
	After               store.CredentialExternalState `json:"after"`
	Credential          []byte                        `json:"credential,omitempty"`
	ErrorCode           string                        `json:"error_code,omitempty"`
	Error               string                        `json:"error,omitempty"`
	RefreshStatus       int                           `json:"refresh_status,omitempty"`
	RefreshDigest       [32]byte                      `json:"refresh_digest,omitempty"`
	RefreshInvalidGrant bool                          `json:"refresh_invalid_grant,omitempty"`
}

type credentialCASProof struct {
	Before     store.CredentialExternalState
	After      store.CredentialExternalState
	Credential *creds.Credential
}

type credentialCASMutation struct {
	Target         creds.Source
	Credential     *creds.Credential
	DeleteOther    bool
	DeleteTarget   bool
	DeleteAll      bool
	Refresh        bool
	RollbackTarget *creds.Credential
}

type credentialCASFunc func(
	context.Context,
	store.Account,
	store.CredentialExternalState,
	credentialCASMutation,
) (credentialCASProof, error)

func (m *Manager) runCredentialCAS(
	ctx context.Context,
	account store.Account,
	expected store.CredentialExternalState,
	mutation credentialCASMutation,
) (credentialCASProof, error) {
	if m.taskRunner == nil || m.workerExecutable == "" {
		return credentialCASProof{}, errors.New("credential CAS worker is unavailable")
	}
	var payload []byte
	var err error
	if mutation.Credential != nil {
		payload, err = mutation.Credential.Marshal()
		if err != nil {
			return credentialCASProof{}, err
		}
	}
	var rollback []byte
	if mutation.RollbackTarget != nil {
		rollback, err = mutation.RollbackTarget.Marshal()
		if err != nil {
			return credentialCASProof{}, err
		}
	}
	request := CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Source: mutation.Target, Expected: expected, Credential: payload,
		DeleteOther: mutation.DeleteOther, DeleteTarget: mutation.DeleteTarget,
		DeleteAll: mutation.DeleteAll, Refresh: mutation.Refresh, RollbackTarget: rollback,
	}
	if mutation.Refresh {
		request.UserAgent = oauth.CurrentUserAgent()
	}
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return credentialCASProof{}, fmt.Errorf("encode credential CAS request: %w", err)
	}
	stdin, inputWriter, err := os.Pipe()
	if err != nil {
		return credentialCASProof{}, fmt.Errorf("create credential CAS input pipe: %w", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := inputWriter.Write(input.Bytes())
		writeDone <- errors.Join(writeErr, inputWriter.Close())
	}()
	var output, stderr boundedBackingBuffer
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, credentialCASWorkerTimeout)
		defer cancel()
	}
	runErr := m.taskRunner.Run(ctx, supervise.Task{
		RecoveryClass: daemonproc.RecoveryTask,
		Path:          m.workerExecutable, Args: []string{casWorkerArgument},
		Stdin: stdin, Stdout: &output, Stderr: &stderr,
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
		return credentialCASProof{}, fmt.Errorf("credential CAS worker: %w: %s", err, stderr.String())
	}
	var response CredentialCASResponse
	if err := decodeCredentialCASJSON(&output, &response); err != nil {
		return credentialCASProof{}, fmt.Errorf("decode credential CAS response: %w", err)
	}
	proof := credentialCASProof{Before: response.Before, After: response.After}
	if len(response.Credential) != 0 {
		var credential creds.Credential
		if err := json.Unmarshal(response.Credential, &credential); err != nil {
			return proof, fmt.Errorf("decode credential CAS result: %w", err)
		}
		proof.Credential = &credential
	}
	switch response.ErrorCode {
	case "":
		if mutation.Refresh != (proof.Credential != nil) {
			return proof, errors.New("credential CAS result mode does not match request")
		}
		return proof, nil
	case "conflict":
		return proof, errCredentialCASConflict
	case "refresh":
		return proof, &oauth.RefreshError{
			Status: response.RefreshStatus, ResponseDigest: response.RefreshDigest,
			ConfirmedInvalidGrant: response.RefreshInvalidGrant,
		}
	case "network":
		return proof, fmt.Errorf("%w: %s", oauth.ErrNetwork, response.Error)
	default:
		return proof, errors.New(response.Error)
	}
}

// IsCredentialCASWorkerInvocation reports whether args request one exact
// refresh-lock-coordinated credential mutation.
func IsCredentialCASWorkerInvocation(args []string) bool {
	return len(args) == 1 && args[0] == casWorkerArgument
}

// RunCredentialCASWorker performs one compare-and-swap while holding both
// lock names used by Claude Code for the same explicit config directory.
func RunCredentialCASWorker(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
) (returnErr error) {
	request, err := DecodeCredentialCASRequest(input)
	if err != nil {
		return err
	}
	if err := validateCredentialCASRequest(request); err != nil {
		return err
	}
	lease, err := acquireCredentialRefreshLocks(ctx, request.AccountID, request.ConfigDir)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, lease.Release(ctx))
	}()

	runner := credentialCASDirectRunner{}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	stores := credentialCASStores(request, runner, executable)
	before, err := observeCredentialCASState(ctx, stores)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{ErrorCode: "io", Error: err.Error()})
	}
	if !sameStoreObservation(before, request.Expected) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "conflict", Error: errCredentialCASConflict.Error(),
		})
	}
	if request.Refresh {
		return refreshCredentialCAS(ctx, output, request, stores, before)
	}
	if request.DeleteAll {
		return deleteAllCredentialCAS(ctx, output, stores, before)
	}
	if request.DeleteTarget {
		beforeTarget := before.Keychain
		beforeOther := before.File
		if request.Source == creds.SourceFile {
			beforeTarget = before.File
			beforeOther = before.Keychain
		}
		if beforeTarget.State != store.CredentialSlotPresent {
			return WriteCredentialCASResponse(output, CredentialCASResponse{
				Before: before, After: before, ErrorCode: "conflict",
				Error: "credential CAS delete target is not exactly readable",
			})
		}
		if err := stores[request.Source].Delete(ctx); err != nil {
			return WriteCredentialCASResponse(output, CredentialCASResponse{
				Before: before, After: before, ErrorCode: "io", Error: err.Error(),
			})
		}
		after, err := observeCredentialCASState(ctx, stores)
		if err != nil {
			return WriteCredentialCASResponse(output, CredentialCASResponse{
				Before: before, ErrorCode: "io", Error: err.Error(),
			})
		}
		afterTarget := after.Keychain
		afterOther := after.File
		if request.Source == creds.SourceFile {
			afterTarget = after.File
			afterOther = after.Keychain
		}
		if afterTarget.State != store.CredentialSlotEmpty ||
			!sameCredentialCASSlot(beforeOther, afterOther) {
			return WriteCredentialCASResponse(output, CredentialCASResponse{
				Before: before, After: after, ErrorCode: "io",
				Error: "credential CAS delete verification failed",
			})
		}
		return WriteCredentialCASResponse(output, CredentialCASResponse{Before: before, After: after})
	}
	var credential creds.Credential
	if err := json.Unmarshal(request.Credential, &credential); err != nil {
		return errors.New("credential CAS payload is invalid")
	}
	if err := stores[request.Source].Write(ctx, &credential); err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	afterWrite, err := observeCredentialCASState(ctx, stores)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	wantDigest := store.CredentialDigest(sha256.Sum256(request.Credential))
	written := afterWrite.Keychain
	unchanged := before.File
	actualUnchanged := afterWrite.File
	if request.Source == creds.SourceFile {
		written = afterWrite.File
		unchanged = before.Keychain
		actualUnchanged = afterWrite.Keychain
	}
	if written.State != store.CredentialSlotPresent || written.Digest == nil ||
		*written.Digest != wantDigest || !sameCredentialCASSlot(unchanged, actualUnchanged) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: afterWrite, ErrorCode: "io",
			Error: "credential CAS write verification failed",
		})
	}
	if !request.DeleteOther {
		return WriteCredentialCASResponse(output, CredentialCASResponse{Before: before, After: afterWrite})
	}
	source := otherSource(request.Source)
	if err := stores[source].Delete(ctx); err != nil {
		rollbackErr := rollbackCredentialCASTarget(ctx, stores[request.Source], request.RollbackTarget)
		afterRollback, observeErr := observeCredentialCASState(ctx, stores)
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: afterRollback, ErrorCode: "io",
			Error: errors.Join(
				fmt.Errorf("delete credential CAS source: %w", err),
				rollbackErr,
				observeErr,
			).Error(),
		})
	}
	after, err := observeCredentialCASState(ctx, stores)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	written = after.Keychain
	deleted := after.File
	if request.Source == creds.SourceFile {
		written = after.File
		deleted = after.Keychain
	}
	if written.State != store.CredentialSlotPresent || written.Digest == nil ||
		*written.Digest != wantDigest || deleted.State != store.CredentialSlotEmpty {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: after, ErrorCode: "io",
			Error: "credential CAS move verification failed",
		})
	}
	return WriteCredentialCASResponse(output, CredentialCASResponse{Before: before, After: after})
}

func refreshCredentialCAS(
	ctx context.Context,
	output io.Writer,
	request CredentialCASRequest,
	stores map[creds.Source]creds.Store,
	before store.CredentialExternalState,
) error {
	previous, err := stores[request.Source].Read(ctx)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	if !previous.HasRefreshToken() {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: "credential has no refresh token",
		})
	}
	client, err := oauth.NewWithUserAgent(request.UserAgent)
	if err != nil {
		return err
	}
	response, err := client.Refresh(
		ctx, fmt.Sprintf("acct-%d", request.AccountID), previous.ClaudeAiOauth.RefreshToken,
	)
	if err != nil {
		return writeCredentialCASRefreshError(output, before, err)
	}
	next := *previous
	next.ClaudeAiOauth.AccessToken = response.AccessToken
	if response.RefreshToken != "" {
		next.ClaudeAiOauth.RefreshToken = response.RefreshToken
	}
	next.ClaudeAiOauth.ExpiresAt = response.Expiry(time.Now()).UnixMilli()
	payload, err := next.Marshal()
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	if err := stores[request.Source].Write(ctx, &next); err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	after, err := observeCredentialCASState(ctx, stores)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	written := after.Keychain
	unchanged := before.File
	actualUnchanged := after.File
	if request.Source == creds.SourceFile {
		written = after.File
		unchanged = before.Keychain
		actualUnchanged = after.Keychain
	}
	wantDigest := store.CredentialDigest(sha256.Sum256(payload))
	if written.State != store.CredentialSlotPresent || written.Digest == nil ||
		*written.Digest != wantDigest || !sameCredentialCASSlot(unchanged, actualUnchanged) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: after, ErrorCode: "io",
			Error: "credential refresh write verification failed",
		})
	}
	return WriteCredentialCASResponse(output, CredentialCASResponse{
		Before: before, After: after, Credential: payload,
	})
}

func writeCredentialCASRefreshError(
	output io.Writer,
	before store.CredentialExternalState,
	err error,
) error {
	var refreshErr *oauth.RefreshError
	if errors.As(err, &refreshErr) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "refresh", Error: refreshErr.Error(),
			RefreshStatus: refreshErr.Status, RefreshDigest: refreshErr.ResponseDigest,
			RefreshInvalidGrant: refreshErr.ConfirmedInvalidGrant,
		})
	}
	if errors.Is(err, oauth.ErrNetwork) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "network", Error: err.Error(),
		})
	}
	return WriteCredentialCASResponse(output, CredentialCASResponse{
		Before: before, After: before, ErrorCode: "io", Error: err.Error(),
	})
}

func deleteAllCredentialCAS(
	ctx context.Context,
	output io.Writer,
	stores map[creds.Source]creds.Store,
	before store.CredentialExternalState,
) error {
	if !credentialStateReadable(before) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "conflict",
			Error: "credential CAS delete-all state is not exactly readable",
		})
	}
	for _, source := range []creds.Source{creds.SourceKeychain, creds.SourceFile} {
		if credentialSourceSlot(before, source).State == store.CredentialSlotEmpty {
			continue
		}
		if err := stores[source].Delete(ctx); err != nil {
			after, observeErr := observeCredentialCASState(ctx, stores)
			return WriteCredentialCASResponse(output, CredentialCASResponse{
				Before: before, After: after, ErrorCode: "io",
				Error: errors.Join(err, observeErr).Error(),
			})
		}
	}
	after, err := observeCredentialCASState(ctx, stores)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	if !credentialStateEmpty(after) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: after, ErrorCode: "io",
			Error: "credential CAS delete-all verification failed",
		})
	}
	return WriteCredentialCASResponse(output, CredentialCASResponse{Before: before, After: after})
}

func rollbackCredentialCASTarget(
	ctx context.Context,
	target creds.Store,
	rollbackPayload []byte,
) error {
	if len(rollbackPayload) == 0 {
		if err := target.Delete(ctx); err != nil && !errors.Is(err, creds.ErrNotFound) {
			return fmt.Errorf("delete credential CAS rollback target: %w", err)
		}
		return nil
	}
	var credential creds.Credential
	if err := json.Unmarshal(rollbackPayload, &credential); err != nil {
		return errors.New("credential CAS rollback payload is invalid")
	}
	if err := target.Write(ctx, &credential); err != nil {
		return fmt.Errorf("restore credential CAS rollback target: %w", err)
	}
	return nil
}

func sameCredentialCASSlot(
	left, right store.CredentialSlotObservation,
) bool {
	if left.State != right.State || (left.Digest == nil) != (right.Digest == nil) {
		return false
	}
	return left.Digest == nil || *left.Digest == *right.Digest
}

func credentialCASStores(
	request CredentialCASRequest,
	runner creds.TaskRunner,
	executable string,
) map[creds.Source]creds.Store {
	return map[creds.Source]creds.Store{
		creds.SourceKeychain: creds.KeychainItem{
			Service: request.KeychainService, Account: request.KeychainAccount, Runner: runner,
		},
		creds.SourceFile: creds.FileStore{
			ConfigDir: AccountBackingDir(request.AccountID),
			Runner:    runner, WorkerExecutable: executable,
		},
	}
}

func observeCredentialCASState(
	ctx context.Context,
	stores map[creds.Source]creds.Store,
) (store.CredentialExternalState, error) {
	var state store.CredentialExternalState
	for _, source := range []creds.Source{creds.SourceKeychain, creds.SourceFile} {
		slot, err := observeCredentialCASSlot(ctx, stores[source])
		if err != nil {
			return store.CredentialExternalState{}, err
		}
		if source == creds.SourceKeychain {
			state.Keychain = slot
		} else {
			state.File = slot
		}
	}
	if _, err := state.Digest(); err != nil {
		return store.CredentialExternalState{}, err
	}
	return state, nil
}

func observeCredentialCASSlot(
	ctx context.Context,
	credentialStore creds.Store,
) (store.CredentialSlotObservation, error) {
	credential, err := credentialStore.Read(ctx)
	switch creds.ClassifyRead(err) {
	case creds.ReadEmpty:
		return store.CredentialSlotObservation{State: store.CredentialSlotEmpty}, nil
	case creds.ReadPresent:
		payload, marshalErr := credential.Marshal()
		if marshalErr != nil {
			return store.CredentialSlotObservation{}, marshalErr
		}
		digest := store.CredentialDigest(sha256.Sum256(payload))
		return store.CredentialSlotObservation{
			State: store.CredentialSlotPresent, Digest: &digest,
		}, nil
	case creds.ReadUnsearchable:
		return store.CredentialSlotObservation{State: store.CredentialSlotUnsearchable}, nil
	case creds.ReadFatal:
		return store.CredentialSlotObservation{State: store.CredentialSlotUnreadable}, nil
	default:
		return store.CredentialSlotObservation{}, errors.New("credential read classification is invalid")
	}
}

func validateCredentialCASRequest(request CredentialCASRequest) error {
	if request.AccountID < 1 || !filepath.IsAbs(request.ConfigDir) ||
		filepath.Clean(request.ConfigDir) != request.ConfigDir || strings.ContainsRune(request.ConfigDir, 0) {
		return errors.New("credential CAS account path is not one exact absolute presentation path")
	}
	if request.KeychainService != creds.ServiceName(request.ConfigDir) ||
		request.KeychainAccount == "" || filepath.Base(request.KeychainAccount) != request.KeychainAccount {
		return errors.New("credential CAS Keychain identity is not canonical")
	}
	if request.Source != creds.SourceKeychain && request.Source != creds.SourceFile {
		return errors.New("credential CAS source is invalid")
	}
	modes := 0
	if len(request.Credential) != 0 {
		modes++
	}
	if request.DeleteTarget {
		modes++
	}
	if request.DeleteAll {
		modes++
	}
	if request.Refresh {
		modes++
	}
	if modes != 1 || request.DeleteOther && (request.DeleteTarget || request.DeleteAll || request.Refresh) {
		return errors.New("credential CAS mutation mode is invalid")
	}
	if request.Refresh != (request.UserAgent != "") {
		return errors.New("credential CAS refresh User-Agent is invalid")
	}
	if request.Refresh {
		if _, err := oauth.NewWithUserAgent(request.UserAgent); err != nil {
			return err
		}
	}
	if len(request.Credential) > credentialCASWorkerMaxIO {
		return errors.New("credential CAS payload is invalid")
	}
	if !request.DeleteOther && len(request.RollbackTarget) != 0 {
		return errors.New("credential CAS rollback requires a move")
	}
	if len(request.RollbackTarget) > credentialCASWorkerMaxIO {
		return errors.New("credential CAS rollback payload is invalid")
	}
	if _, err := request.Expected.Digest(); err != nil {
		return err
	}
	if request.DeleteOther {
		wantDigest := store.CredentialDigest(sha256.Sum256(request.Credential))
		source := request.Expected.Keychain
		target := request.Expected.File
		if request.Source == creds.SourceKeychain {
			source = request.Expected.File
			target = request.Expected.Keychain
		}
		if source.State != store.CredentialSlotPresent || source.Digest == nil ||
			*source.Digest != wantDigest {
			return errors.New("credential CAS move source does not match its payload")
		}
		if target.State == store.CredentialSlotPresent {
			if target.Digest == nil || len(request.RollbackTarget) == 0 ||
				store.CredentialDigest(sha256.Sum256(request.RollbackTarget)) != *target.Digest {
				return errors.New("credential CAS move rollback does not match its target")
			}
		} else if len(request.RollbackTarget) != 0 {
			return errors.New("credential CAS move rollback targets an empty slot")
		}
	}
	return validateCredentialLockDirectory(request.ConfigDir)
}

// DecodeCredentialCASRequest decodes one exact v1 worker request frame.
func DecodeCredentialCASRequest(input io.Reader) (CredentialCASRequest, error) {
	var request CredentialCASRequest
	if err := decodeCredentialCASJSON(input, &request); err != nil {
		return CredentialCASRequest{}, fmt.Errorf("decode credential CAS request: %w", err)
	}
	return request, nil
}

// WriteCredentialCASResponse writes one exact v1 worker response.
func WriteCredentialCASResponse(output io.Writer, response CredentialCASResponse) error {
	return json.NewEncoder(output).Encode(response)
}

func decodeCredentialCASJSON(input io.Reader, value any) error {
	payload, err := io.ReadAll(io.LimitReader(input, credentialCASWorkerMaxIO+1))
	if err != nil {
		return err
	}
	if len(payload) > credentialCASWorkerMaxIO {
		return errors.New("credential CAS frame exceeded its limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("credential CAS frame contains multiple values")
		}
		return err
	}
	return nil
}

type credentialCASDirectRunner struct{}

func (credentialCASDirectRunner) Run(ctx context.Context, task supervise.Task) error {
	if creds.IsFileWorkerInvocation(task.Args) {
		return creds.RunFileWorker(ctx, task.Stdin, task.Stdout)
	}
	if task.Path == "" {
		return errors.New("credential CAS nested task has no executable")
	}
	if !filepath.IsAbs(task.Path) || filepath.Clean(task.Path) != task.Path {
		return errors.New("credential CAS nested task requires a clean absolute executable")
	}
	// #nosec G204 -- task.Path is a validated absolute security(1) or test-fixture path.
	command := exec.CommandContext(ctx, task.Path, task.Args...)
	command.Dir = task.Dir
	command.Env = task.Env
	command.Stdin = task.Stdin
	command.Stdout = task.Stdout
	command.Stderr = task.Stderr
	return command.Run()
}
