package pool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/daemonkit/worker"
)

type credentialCASTestTaskRunner struct{}

type credentialCASTestResult struct {
	response CredentialCASResponse
	err      error
}

func (credentialCASTestTaskRunner) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	if len(task.Args) != 1 || task.Args[0] != casWorkerArgument {
		return worker.CommandResult{}, errors.New("unexpected credential CAS test task")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return worker.CommandResult{}, err
	}
	if len(task.Env) != 1 || task.Env[0] != "HOME="+home {
		return worker.CommandResult{}, fmt.Errorf("credential CAS worker environment is not exact: %v", task.Env)
	}
	var output bytes.Buffer
	err = RunCredentialCASWorker(ctx, bytes.NewReader(task.Stdin), &output)
	return worker.CommandResult{Stdout: output.Bytes()}, err
}

func TestCredentialCASKeychainWrite(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceKeychain].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	next := credentialCASCredential("next")
	response := runCredentialCASTestWorker(t, credentialCASTestRequest(t, account, creds.SourceKeychain, expected, next))
	if response.ErrorCode != "" || !sameStoreObservation(response.Before, expected) {
		t.Fatalf("credential CAS response = %+v", response)
	}
	got, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil || !sameTokens(got, next) {
		t.Fatalf("credential after CAS = %+v, %v", got, err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASWritesAndVerifiesReorderedJSONCanonically(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("reordered"))
	request.Credential = []byte(`{"claudeAiOauth":{"expiresAt":1800000000000,"refreshToken":"refresh-reordered","accessToken":"access-reordered"}}`)
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" {
		t.Fatalf("credential CAS response = %+v", response)
	}
	canonical, err := credentialCASCredential("reordered").Marshal()
	if err != nil {
		t.Fatal(err)
	}
	assertCredentialCASCanonicalWrite(t, response, canonical)
}

func TestCredentialCASOmitsExplicitEmptyOptionalFieldCanonically(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("empty-optional"))
	request.Credential = []byte(`{"claudeAiOauth":{"accessToken":"access-empty-optional","refreshToken":"","expiresAt":1800000000000}}`)
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" {
		t.Fatalf("credential CAS response = %+v", response)
	}
	canonicalCredential := credentialCASCredential("empty-optional")
	canonicalCredential.ClaudeAiOauth.RefreshToken = ""
	canonical, err := canonicalCredential.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	assertCredentialCASCanonicalWrite(t, response, canonical)
}

func TestCredentialCASRejectsUnknownCredentialFieldsBeforeWrite(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("unknown"))
	request.Credential = []byte(`{"claudeAiOauth":{"accessToken":"access-unknown","expiresAt":1800000000000,"unknown":true}}`)
	if _, err := runCredentialCASTestWorkerResult(t.Context(), request); err == nil {
		t.Fatal("credential CAS accepted an unknown inner field")
	}
	if _, err := os.Stat(os.Getenv("CCP_CAS_KEYCHAIN_ITEM")); !errors.Is(err, os.ErrNotExist) { //nolint:gosec // G703: fixture path is test-owned.
		t.Fatalf("credential CAS wrote rejected payload: %v", err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASLostResponseResumeKeepsCanonicalDigest(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("resume"))
	request.Credential = []byte(`{"claudeAiOauth":{"expiresAt":1800000000000,"refreshToken":"","accessToken":"access-resume"}}`)
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		t.Fatal(err)
	}
	if err := RunCredentialCASWorker(t.Context(), &input, io.Discard); err != nil {
		t.Fatal(err)
	}
	observed, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request.Expected = observed
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" {
		t.Fatalf("credential CAS resume response = %+v", response)
	}
	canonicalCredential := credentialCASCredential("resume")
	canonicalCredential.ClaudeAiOauth.RefreshToken = ""
	canonical, err := canonicalCredential.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	assertCredentialCASCanonicalWrite(t, response, canonical)
	wantDigest := store.CredentialDigest(sha256.Sum256(canonical))
	if observed.Keychain.Digest == nil || *observed.Keychain.Digest != wantDigest ||
		response.After.Keychain.Digest == nil || *response.After.Keychain.Digest != wantDigest {
		t.Fatalf("canonical digest changed across lost response: before=%+v after=%+v", observed, response.After)
	}
}

func assertCredentialCASCanonicalWrite(
	t *testing.T,
	response CredentialCASResponse,
	canonical []byte,
) {
	t.Helper()
	written, err := os.ReadFile(os.Getenv("CCP_CAS_KEYCHAIN_ITEM")) //nolint:gosec // G703: fixture path is test-owned.
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(written, canonical) {
		t.Fatalf("credential bytes = %s, want %s", written, canonical)
	}
	wantDigest := store.CredentialDigest(sha256.Sum256(canonical))
	if response.After.Keychain.Digest == nil || *response.After.Keychain.Digest != wantDigest {
		t.Fatalf("credential digest = %+v, want %x", response.After.Keychain.Digest, wantDigest)
	}
}

func TestCredentialCASDeletesOnlyAnExactReadableTarget(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	deleted := credentialCASCredential("deleted")
	if err := stores[creds.SourceKeychain].Write(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, deleted)
	request.Credential = nil
	request.Delete = true
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" {
		t.Fatalf("credential CAS delete = %+v", response)
	}
	if _, err := stores[creds.SourceKeychain].Read(t.Context()); creds.ClassifyRead(err) != creds.ReadEmpty {
		t.Fatalf("credential CAS delete target remained: %v", err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASDeleteExactStateUnderOneLock(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	if err := stores[creds.SourceKeychain].Write(t.Context(), credentialCASCredential("keychain")); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestBaseRequest(t, account)
	request.Expected = expected
	request.Delete = true
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" || !credentialStateEmpty(response.After) {
		t.Fatalf("credential CAS delete = %+v", response)
	}
	if _, err := stores[creds.SourceKeychain].Read(t.Context()); creds.ClassifyRead(err) != creds.ReadEmpty {
		t.Fatalf("delete retained Keychain credential: %v", err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASDeletesThroughStableInstanceExecutionPath(t *testing.T) {
	account, _ := newCredentialCASFixture(t)
	wantConfigDir, err := AccountConfigDir(account.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	wantService, err := AccountKeychainService(account.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	publicPath, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	if account.ConfigDir != wantConfigDir || account.KeychainService != wantService ||
		publicPath == account.ConfigDir {
		t.Fatalf("credential CAS identity = %+v public=%q", account, publicPath)
	}
	request := credentialCASTestBaseRequest(t, account)
	credentialStore := credentialCASStore(request, credentialCASDirectRunner{})
	if err := credentialStore.Write(t.Context(), credentialCASCredential("old-public-path")); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	request.Expected = expected
	request.Delete = true
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" || !credentialStateEmpty(response.After) {
		t.Fatalf("stable credential CAS delete = %+v", response)
	}
	if _, err := credentialStore.Read(t.Context()); creds.ClassifyRead(err) != creds.ReadEmpty {
		t.Fatalf("stable credential remained: %v", err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
	if target, err := os.Readlink(account.ConfigDir); err != nil || target != publicPath {
		t.Fatalf("credential CAS changed execution link: target=%q err=%v", target, err)
	}
}

func TestCredentialCASWaitsForClaudeLockAndNeverClobbersRacingWriter(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceKeychain].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(account.ConfigDir, ".oauth_refresh.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}
	contended := make(chan struct{})
	resume := make(chan struct{})
	var contendedOnce sync.Once
	var resumeOnce sync.Once
	credentialLockFailpoint = func(checkpoint string) {
		if checkpoint != "target-contended-0" {
			return
		}
		contendedOnce.Do(func() {
			close(contended)
			<-resume
		})
	}
	t.Cleanup(func() {
		resumeOnce.Do(func() { close(resume) })
		credentialLockFailpoint = nil
	})

	workerContext, cancelWorker := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancelWorker)
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("cas"))
	response := make(chan credentialCASTestResult, 1)
	go func() {
		result, err := runCredentialCASTestWorkerResult(workerContext, request)
		response <- credentialCASTestResult{response: result, err: err}
	}()
	select {
	case <-contended:
	case workerResult := <-response:
		if workerResult.err != nil {
			t.Fatalf("credential CAS worker before contention: %v", workerResult.err)
		}
		t.Fatalf("credential CAS worker completed before contention: %+v", workerResult.response)
	}
	racing := credentialCASCredential("claude")
	if err := stores[creds.SourceKeychain].Write(t.Context(), racing); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	resumeOnce.Do(func() { close(resume) })
	workerResult := <-response
	if workerResult.err != nil {
		t.Fatal(workerResult.err)
	}
	result := workerResult.response
	if result.ErrorCode != "conflict" {
		t.Fatalf("racing writer result = %+v, want conflict", result)
	}
	got, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil || !sameTokens(got, racing) {
		t.Fatalf("Claude credential was clobbered: %+v, %v", got, err)
	}
}

func TestCredentialCASCanceledWaiterWritesNothing(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceKeychain].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(account.ConfigDir, ".oauth_refresh.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("next"))
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := RunCredentialCASWorker(ctx, &input, &bytes.Buffer{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled waiter = %v", err)
	}
	got, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil || !sameTokens(got, initial) {
		t.Fatalf("canceled waiter changed credential: %+v, %v", got, err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialCASNeverRetiresOldClaudeLock(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceKeychain].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(account.ConfigDir, ".oauth_refresh.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}
	staleAt := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(lock, staleAt, staleAt); err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceKeychain, expected, credentialCASCredential("blocked"))
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := RunCredentialCASWorker(ctx, &input, &bytes.Buffer{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("credential CAS old live-lock wait = %v, want deadline", err)
	}
	got, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil || !sameTokens(got, initial) {
		t.Fatalf("old live lock allowed credential mutation: %+v, %v", got, err)
	}
	if info, err := os.Lstat(lock); err != nil || !info.IsDir() {
		t.Fatalf("old live Claude lock changed: %+v, %v", info, err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialCASRefreshHoldsClaudeLocksThroughPostAndWrite(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceKeychain].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	realConfigDir, err := filepath.EvalSymlinks(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	lockPaths := []string{filepath.Join(account.ConfigDir, ".oauth_refresh.lock"), realConfigDir + ".lock"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("User-Agent"); got != "credential-cas-test/1" {
			t.Errorf("refresh User-Agent = %q", got)
		}
		for _, path := range lockPaths {
			if info, err := os.Lstat(path); err != nil || !info.IsDir() {
				t.Errorf("refresh POST did not hold %s: %+v, %v", path, info, err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"access-refreshed","refresh_token":"refresh-refreshed","expires_in":3600}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv("CLAUDE_POOL_TOKEN_URL", server.URL)
	request := credentialCASTestBaseRequest(t, account)
	request.Expected = expected
	request.Refresh = true
	request.UserAgent = "credential-cas-test/1"
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" || len(response.Credential) == 0 {
		t.Fatalf("credential CAS refresh = %+v", response)
	}
	got, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got.ClaudeAiOauth.AccessToken != "access-refreshed" ||
		got.ClaudeAiOauth.RefreshToken != "refresh-refreshed" {
		t.Fatalf("refreshed credential = %+v", got.ClaudeAiOauth)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASRefreshPreservesTypedInvalidGrant(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceKeychain].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores[creds.SourceKeychain])
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":"invalid_grant"}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv("CLAUDE_POOL_TOKEN_URL", server.URL)
	request := credentialCASTestBaseRequest(t, account)
	request.Expected = expected
	request.Refresh = true
	request.UserAgent = "credential-cas-test/1"
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "refresh" || response.RefreshStatus != http.StatusBadRequest ||
		!response.RefreshInvalidGrant || response.RefreshDigest == ([32]byte{}) {
		t.Fatalf("typed invalid_grant response = %+v", response)
	}
	got, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil || !sameTokens(got, initial) {
		t.Fatalf("invalid_grant changed credential: %+v, %v", got, err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
	manager := &Manager{taskRunner: credentialCASTestTaskRunner{}, workerExecutable: "test-worker"}
	publicPath, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.runCredentialCAS(t.Context(), account, expected, credentialCASMutation{
		ExpectedPublicPath: publicPath, Refresh: true,
	})
	var refreshErr *oauth.RefreshError
	if !errors.As(err, &refreshErr) || refreshErr.Status != http.StatusBadRequest ||
		!refreshErr.ConfirmedInvalidGrant || refreshErr.ResponseDigest == ([32]byte{}) {
		t.Fatalf("typed parent invalid_grant = %#v, %v", refreshErr, err)
	}
}

func newCredentialCASFixture(
	t *testing.T,
) (store.Account, map[creds.Source]creds.Store) {
	t.Helper()
	home := t.TempDir()
	testhome.Sandbox(t, home)
	t.Setenv("USER", "credential-cas-test")
	itemPath := filepath.Join(t.TempDir(), "keychain-item")
	t.Setenv("CCP_CAS_KEYCHAIN_ITEM", itemPath)
	t.Setenv("CLAUDE_POOL_SECURITY_BIN", writeCredentialCASFakeSecurity(t, itemPath))
	const instanceID = "0123456789abcdef0123456789abcdef"
	publicPath := filepath.Join(home, "Library", "CloudStorage", "CCPool", "account-1")
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(instanceID, publicPath); err != nil {
		t.Fatal(err)
	}
	configDir, err := AccountConfigDir(instanceID)
	if err != nil {
		t.Fatal(err)
	}
	service, err := AccountKeychainService(instanceID)
	if err != nil {
		t.Fatal(err)
	}
	account := store.Account{
		ID: 1, InstanceID: instanceID, Generation: 1,
		ConfigDir: configDir, KeychainService: service,
		KeychainAccount: "credential-cas-test",
	}
	if err := os.MkdirAll(AccountBackingDir(account.ID), 0o700); err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestBaseRequest(t, account)
	return account, map[creds.Source]creds.Store{
		creds.SourceKeychain: credentialCASStore(request, credentialCASDirectRunner{}),
	}
}

func credentialCASTestRequest(
	t *testing.T,
	account store.Account,
	_ creds.Source,
	expected store.CredentialExternalState,
	credential *creds.Credential,
) CredentialCASRequest {
	t.Helper()
	payload, err := credential.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return CredentialCASRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, ConfigDir: account.ConfigDir,
		ExpectedPublicPath: credentialCASTestPublicPath(t, account),
		KeychainService:    account.KeychainService, KeychainAccount: account.KeychainAccount,
		Expected: expected, Credential: payload,
	}
}

func credentialCASTestBaseRequest(t *testing.T, account store.Account) CredentialCASRequest {
	t.Helper()
	return CredentialCASRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, ConfigDir: account.ConfigDir,
		ExpectedPublicPath: credentialCASTestPublicPath(t, account),
		KeychainService:    account.KeychainService, KeychainAccount: account.KeychainAccount,
	}
}

func credentialCASTestPublicPath(t *testing.T, account store.Account) string {
	t.Helper()
	publicPath, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	return publicPath
}

func runCredentialCASTestWorker(t *testing.T, request CredentialCASRequest) CredentialCASResponse {
	t.Helper()
	response, err := runCredentialCASTestWorkerResult(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func runCredentialCASTestWorkerResult(
	ctx context.Context,
	request CredentialCASRequest,
) (CredentialCASResponse, error) {
	var input, output bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return CredentialCASResponse{}, err
	}
	if err := RunCredentialCASWorker(ctx, &input, &output); err != nil {
		return CredentialCASResponse{}, err
	}
	var response CredentialCASResponse
	if err := decodeCredentialCASJSON(&output, &response); err != nil {
		return CredentialCASResponse{}, err
	}
	return response, nil
}

func credentialCASCredential(suffix string) *creds.Credential {
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "access-" + suffix
	credential.ClaudeAiOauth.RefreshToken = "refresh-" + suffix
	credential.ClaudeAiOauth.ExpiresAt = 1_800_000_000_000
	return credential
}

func assertCredentialCASLocksGone(t *testing.T, configDir string) {
	t.Helper()
	realConfigDir, err := filepath.EvalSymlinks(configDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(configDir, ".oauth_refresh.lock"), realConfigDir + ".lock"} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) { //nolint:gosec // G703: fixture paths are test-owned.
			t.Fatalf("credential CAS lock remained at %s: %v", path, err)
		}
	}
}

func writeCredentialCASFakeSecurity(t *testing.T, itemPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security")
	script := "#!/bin/sh\nset -eu\nitem=" + strconv.Quote(itemPath) + `
command="$1"
shift
case "$command" in
  list-keychains)
    printf '"%s/Library/Keychains/login.keychain-db"\n' "$HOME"
    ;;
  find-generic-password)
    if [ ! -f "$item" ]; then
      echo 'The specified item could not be found in the keychain.' >&2
      exit 44
    fi
    cat "$item"
    printf '\n'
    ;;
  add-generic-password)
    hex=''
    while [ "$#" -gt 0 ]; do
      if [ "$1" = '-X' ]; then hex="$2"; shift 2; else shift; fi
    done
    printf '%s' "$hex" | xxd -r -p > "$item"
    ;;
  delete-generic-password)
    rm -f "$item"
    ;;
  *) exit 64 ;;
esac
`
	writePoolTestExecutable(t, path, script)
	return path
}
