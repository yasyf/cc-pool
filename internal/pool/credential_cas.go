package pool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/worker"
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
	Expected        store.CredentialExternalState `json:"expected"`
	Credential      []byte                        `json:"credential"`
	Delete          bool                          `json:"delete"`
	Refresh         bool                          `json:"refresh"`
	UserAgent       string                        `json:"user_agent,omitempty"`
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
	Credential *creds.Credential
	Delete     bool
	Refresh    bool
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
	request := CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Expected: expected, Credential: payload,
		Delete: mutation.Delete, Refresh: mutation.Refresh,
	}
	if mutation.Refresh {
		request.UserAgent = oauth.CurrentUserAgent()
	}
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return credentialCASProof{}, fmt.Errorf("encode credential CAS request: %w", err)
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, credentialCASWorkerTimeout)
		defer cancel()
	}
	result, runErr := m.taskRunner.Run(ctx, worker.CommandRequest{
		Path: m.workerExecutable, Dir: workerexec.TempDir(), Args: []string{casWorkerArgument},
		Stdin: input.Bytes(), TotalTimeout: credentialCASWorkerTimeout,
	})
	if runErr != nil {
		return credentialCASProof{}, fmt.Errorf("credential CAS worker: %w: %s", runErr, string(result.Stderr))
	}
	var response CredentialCASResponse
	if err := decodeCredentialCASJSON(bytes.NewReader(result.Stdout), &response); err != nil {
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
	var canonicalCredential creds.CanonicalCredential
	if len(request.Credential) != 0 {
		canonicalCredential, err = creds.CanonicalizeCredentialForWrite(request.Credential)
		if err != nil {
			return fmt.Errorf("credential CAS payload is invalid: %w", err)
		}
	}
	lease, err := acquireCredentialRefreshLocks(ctx, request.AccountID, request.ConfigDir)
	if err != nil {
		return err
	}
	defer func() {
		returnErr = errors.Join(returnErr, lease.Release(ctx))
	}()

	runner := credentialCASDirectRunner{}
	credentialStore := credentialCASStore(request, runner)
	before, err := observeCredentialCASState(ctx, credentialStore)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{ErrorCode: "io", Error: err.Error()})
	}
	if !sameStoreObservation(before, request.Expected) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "conflict", Error: errCredentialCASConflict.Error(),
		})
	}
	if request.Refresh {
		return refreshCredentialCAS(ctx, output, request, credentialStore, before)
	}
	if request.Delete {
		return deleteCredentialCAS(ctx, output, credentialStore, before)
	}
	canonicalBytes := canonicalCredential.Bytes()
	if err := credentialStore.WriteCanonical(ctx, canonicalCredential); err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	afterWrite, err := observeCredentialCASState(ctx, credentialStore)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	wantDigest := store.CredentialDigest(sha256.Sum256(canonicalBytes))
	written := afterWrite.Keychain
	if written.State != store.CredentialSlotPresent || written.Digest == nil ||
		*written.Digest != wantDigest {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: afterWrite, ErrorCode: "io",
			Error: "credential CAS write verification failed",
		})
	}
	return WriteCredentialCASResponse(output, CredentialCASResponse{Before: before, After: afterWrite})
}

func refreshCredentialCAS(
	ctx context.Context,
	output io.Writer,
	request CredentialCASRequest,
	credentialStore creds.Store,
	before store.CredentialExternalState,
) error {
	previous, err := credentialStore.Read(ctx)
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
	if err := credentialStore.Write(ctx, &next); err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	after, err := observeCredentialCASState(ctx, credentialStore)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	written := after.Keychain
	wantDigest := store.CredentialDigest(sha256.Sum256(payload))
	if written.State != store.CredentialSlotPresent || written.Digest == nil ||
		*written.Digest != wantDigest {
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

func deleteCredentialCAS(
	ctx context.Context,
	output io.Writer,
	credentialStore creds.Store,
	before store.CredentialExternalState,
) error {
	if !credentialStateReadable(before) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: before, ErrorCode: "conflict",
			Error: "credential CAS delete state is not exactly readable",
		})
	}
	if before.Keychain.State != store.CredentialSlotEmpty {
		if err := credentialStore.Delete(ctx); err != nil {
			after, observeErr := observeCredentialCASState(ctx, credentialStore)
			return WriteCredentialCASResponse(output, CredentialCASResponse{
				Before: before, After: after, ErrorCode: "io",
				Error: errors.Join(err, observeErr).Error(),
			})
		}
	}
	after, err := observeCredentialCASState(ctx, credentialStore)
	if err != nil {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, ErrorCode: "io", Error: err.Error(),
		})
	}
	if !credentialStateEmpty(after) {
		return WriteCredentialCASResponse(output, CredentialCASResponse{
			Before: before, After: after, ErrorCode: "io",
			Error: "credential CAS delete verification failed",
		})
	}
	return WriteCredentialCASResponse(output, CredentialCASResponse{Before: before, After: after})
}

func credentialCASStore(
	request CredentialCASRequest,
	runner creds.TaskRunner,
) creds.KeychainItem {
	return creds.KeychainItem{
		Service: request.KeychainService, Account: request.KeychainAccount, Runner: runner,
	}
}

func observeCredentialCASState(
	ctx context.Context,
	credentialStore creds.Store,
) (store.CredentialExternalState, error) {
	slot, err := observeCredentialCASSlot(ctx, credentialStore)
	if err != nil {
		return store.CredentialExternalState{}, err
	}
	state := store.CredentialExternalState{Keychain: slot}
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
	modes := 0
	if len(request.Credential) != 0 {
		modes++
	}
	if request.Delete {
		modes++
	}
	if request.Refresh {
		modes++
	}
	if modes != 1 {
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
	if _, err := request.Expected.Digest(); err != nil {
		return err
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

func (credentialCASDirectRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	if task.Path == "" {
		return worker.CommandResult{}, errors.New("credential CAS nested task has no executable")
	}
	if !filepath.IsAbs(task.Path) || filepath.Clean(task.Path) != task.Path {
		return worker.CommandResult{}, errors.New("credential CAS nested task requires a clean absolute executable")
	}
	// #nosec G204 -- task.Path is a validated absolute security(1) or test-fixture path.
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
