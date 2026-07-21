package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/supervise"
)

type credentialCASTestTaskRunner struct{}

func (credentialCASTestTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	if len(task.Args) != 1 || task.Args[0] != casWorkerArgument {
		return errors.New("unexpected credential CAS test task")
	}
	return RunCredentialCASWorker(ctx, task.Stdin, task.Stdout)
}

func TestCredentialCASFileAndKeychainVariants(t *testing.T) {
	for _, source := range []creds.Source{creds.SourceFile, creds.SourceKeychain} {
		t.Run(source.String(), func(t *testing.T) {
			account, stores := newCredentialCASFixture(t)
			initial := credentialCASCredential("initial")
			if err := stores[source].Write(t.Context(), initial); err != nil {
				t.Fatal(err)
			}
			expected, err := observeCredentialCASState(t.Context(), stores)
			if err != nil {
				t.Fatal(err)
			}
			next := credentialCASCredential("next")
			response := runCredentialCASTestWorker(t, credentialCASTestRequest(t, account, source, expected, next))
			if response.ErrorCode != "" || !sameStoreObservation(response.Before, expected) {
				t.Fatalf("credential CAS response = %+v", response)
			}
			got, err := stores[source].Read(t.Context())
			if err != nil || !sameTokens(got, next) {
				t.Fatalf("credential after CAS = %+v, %v", got, err)
			}
			assertCredentialCASLocksGone(t, account.ConfigDir)
		})
	}
}

func TestCredentialCASMovesUnderOneRefreshLock(t *testing.T) {
	for _, source := range []creds.Source{creds.SourceKeychain, creds.SourceFile} {
		t.Run(source.String(), func(t *testing.T) {
			account, stores := newCredentialCASFixture(t)
			credential := credentialCASCredential("move")
			if err := stores[source].Write(t.Context(), credential); err != nil {
				t.Fatal(err)
			}
			expected, err := observeCredentialCASState(t.Context(), stores)
			if err != nil {
				t.Fatal(err)
			}
			target := otherSource(source)
			request := credentialCASTestRequest(t, account, target, expected, credential)
			request.DeleteOther = true
			response := runCredentialCASTestWorker(t, request)
			if response.ErrorCode != "" {
				t.Fatalf("credential CAS move = %+v", response)
			}
			got, err := stores[target].Read(t.Context())
			if err != nil || !sameTokens(got, credential) {
				t.Fatalf("credential CAS move target = %+v, %v", got, err)
			}
			if _, err := stores[source].Read(t.Context()); creds.ClassifyRead(err) != creds.ReadEmpty {
				t.Fatalf("credential CAS move source remained: %v", err)
			}
			assertCredentialCASLocksGone(t, account.ConfigDir)
		})
	}
}

func TestCredentialCASDeletesOnlyAnExactReadableTarget(t *testing.T) {
	for _, target := range []creds.Source{creds.SourceKeychain, creds.SourceFile} {
		t.Run(target.String(), func(t *testing.T) {
			account, stores := newCredentialCASFixture(t)
			deleted := credentialCASCredential("deleted")
			kept := credentialCASCredential("kept")
			if err := stores[target].Write(t.Context(), deleted); err != nil {
				t.Fatal(err)
			}
			if err := stores[otherSource(target)].Write(t.Context(), kept); err != nil {
				t.Fatal(err)
			}
			expected, err := observeCredentialCASState(t.Context(), stores)
			if err != nil {
				t.Fatal(err)
			}
			request := credentialCASTestRequest(t, account, target, expected, deleted)
			request.Credential = nil
			request.DeleteTarget = true
			response := runCredentialCASTestWorker(t, request)
			if response.ErrorCode != "" {
				t.Fatalf("credential CAS delete = %+v", response)
			}
			if _, err := stores[target].Read(t.Context()); creds.ClassifyRead(err) != creds.ReadEmpty {
				t.Fatalf("credential CAS delete target remained: %v", err)
			}
			got, err := stores[otherSource(target)].Read(t.Context())
			if err != nil || !sameTokens(got, kept) {
				t.Fatalf("credential CAS delete changed other slot: %+v, %v", got, err)
			}
			assertCredentialCASLocksGone(t, account.ConfigDir)
		})
	}
}

func TestCredentialCASDeleteAllExactStateUnderOneLock(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	for source, credential := range map[creds.Source]*creds.Credential{
		creds.SourceKeychain: credentialCASCredential("keychain"),
		creds.SourceFile:     credentialCASCredential("file"),
	} {
		if err := stores[source].Write(t.Context(), credential); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
	if err != nil {
		t.Fatal(err)
	}
	response := runCredentialCASTestWorker(t, CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Expected: expected, DeleteAll: true,
	})
	if response.ErrorCode != "" || !credentialStateEmpty(response.After) {
		t.Fatalf("credential CAS delete-all = %+v", response)
	}
	for source, credentialStore := range stores {
		if _, err := credentialStore.Read(t.Context()); creds.ClassifyRead(err) != creds.ReadEmpty {
			t.Fatalf("delete-all retained %s credential: %v", source, err)
		}
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASRefusesUnreadableDeleteTarget(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	path := creds.FileCredentialPath(AccountBackingDir(account.ID))
	if err := os.WriteFile(path, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
	if err != nil {
		t.Fatal(err)
	}
	request := CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Source: creds.SourceFile, Expected: expected, DeleteTarget: true,
	}
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "conflict" {
		t.Fatalf("unreadable credential CAS delete = %+v", response)
	}
	// #nosec G304 -- path is the credential file inside this test's temporary account directory.
	if got, err := os.ReadFile(path); err != nil || string(got) != "not-json" {
		t.Fatalf("unreadable delete target changed: %q, %v", got, err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASMoveDeleteFailureRestoresExactTarget(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	source := credentialCASCredential("source")
	rollback := credentialCASCredential("rollback")
	if err := stores[creds.SourceKeychain].Write(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if err := stores[creds.SourceFile].Write(t.Context(), rollback); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
	if err != nil {
		t.Fatal(err)
	}
	failDelete := filepath.Join(t.TempDir(), "fail-delete")
	if err := os.WriteFile(failDelete, []byte("fail"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CCP_CAS_KEYCHAIN_DELETE_FAIL", failDelete)
	request := credentialCASTestRequest(t, account, creds.SourceFile, expected, source)
	request.DeleteOther = true
	request.RollbackTarget, err = rollback.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "io" || !sameStoreObservation(response.Before, response.After) {
		t.Fatalf("credential CAS failed move = %+v", response)
	}
	gotSource, err := stores[creds.SourceKeychain].Read(t.Context())
	if err != nil || !sameTokens(gotSource, source) {
		t.Fatalf("credential CAS failed move source = %+v, %v", gotSource, err)
	}
	gotTarget, err := stores[creds.SourceFile].Read(t.Context())
	if err != nil || !sameTokens(gotTarget, rollback) {
		t.Fatalf("credential CAS failed move rollback = %+v, %v", gotTarget, err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
}

func TestCredentialCASWaitsForClaudeLockAndNeverClobbersRacingWriter(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceFile].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(account.ConfigDir, ".oauth_refresh.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}

	response := make(chan CredentialCASResponse, 1)
	go func() {
		response <- runCredentialCASTestWorker(
			t, credentialCASTestRequest(t, account, creds.SourceFile, expected, credentialCASCredential("cas")),
		)
	}()
	time.Sleep(75 * time.Millisecond)
	racing := credentialCASCredential("claude")
	if err := stores[creds.SourceFile].Write(t.Context(), racing); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	result := <-response
	if result.ErrorCode != "conflict" {
		t.Fatalf("racing writer result = %+v, want conflict", result)
	}
	got, err := stores[creds.SourceFile].Read(t.Context())
	if err != nil || !sameTokens(got, racing) {
		t.Fatalf("Claude credential was clobbered: %+v, %v", got, err)
	}
}

func TestCredentialCASCanceledWaiterWritesNothing(t *testing.T) {
	account, stores := newCredentialCASFixture(t)
	initial := credentialCASCredential("initial")
	if err := stores[creds.SourceFile].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(account.ConfigDir, ".oauth_refresh.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}
	request := credentialCASTestRequest(t, account, creds.SourceFile, expected, credentialCASCredential("next"))
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := RunCredentialCASWorker(ctx, &input, &bytes.Buffer{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled waiter = %v", err)
	}
	got, err := stores[creds.SourceFile].Read(t.Context())
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
	if err := stores[creds.SourceFile].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
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
	request := credentialCASTestRequest(t, account, creds.SourceFile, expected, credentialCASCredential("blocked"))
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := RunCredentialCASWorker(ctx, &input, &bytes.Buffer{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("credential CAS old live-lock wait = %v, want deadline", err)
	}
	got, err := stores[creds.SourceFile].Read(t.Context())
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
	if err := stores[creds.SourceFile].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
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
	request := CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Source: creds.SourceFile, Expected: expected, Refresh: true,
		UserAgent: "credential-cas-test/1",
	}
	response := runCredentialCASTestWorker(t, request)
	if response.ErrorCode != "" || len(response.Credential) == 0 {
		t.Fatalf("credential CAS refresh = %+v", response)
	}
	got, err := stores[creds.SourceFile].Read(t.Context())
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
	if err := stores[creds.SourceFile].Write(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	expected, err := observeCredentialCASState(t.Context(), stores)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":"invalid_grant"}`)
	}))
	t.Cleanup(server.Close)
	t.Setenv("CLAUDE_POOL_TOKEN_URL", server.URL)
	response := runCredentialCASTestWorker(t, CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Source: creds.SourceFile, Expected: expected, Refresh: true,
		UserAgent: "credential-cas-test/1",
	})
	if response.ErrorCode != "refresh" || response.RefreshStatus != http.StatusBadRequest ||
		!response.RefreshInvalidGrant || response.RefreshDigest == ([32]byte{}) {
		t.Fatalf("typed invalid_grant response = %+v", response)
	}
	got, err := stores[creds.SourceFile].Read(t.Context())
	if err != nil || !sameTokens(got, initial) {
		t.Fatalf("invalid_grant changed credential: %+v, %v", got, err)
	}
	assertCredentialCASLocksGone(t, account.ConfigDir)
	manager := &Manager{taskRunner: credentialCASTestTaskRunner{}, workerExecutable: "test-worker"}
	_, err = manager.runCredentialCAS(t.Context(), account, expected, credentialCASMutation{
		Target: creds.SourceFile, Refresh: true,
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
	t.Setenv("HOME", home)
	t.Setenv("USER", "credential-cas-test")
	t.Setenv("CLAUDE_POOL_SECURITY_BIN", writeCredentialCASFakeSecurity(t))
	t.Setenv("CCP_CAS_KEYCHAIN_ITEM", filepath.Join(t.TempDir(), "keychain-item"))
	account := store.Account{
		ID: 1, ConfigDir: AccountDir(1), KeychainService: creds.ServiceName(AccountDir(1)),
		KeychainAccount: "credential-cas-test",
	}
	for _, directory := range []string{account.ConfigDir, AccountBackingDir(account.ID)} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	request := CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
	}
	return account, credentialCASStores(request, credentialCASDirectRunner{}, executable)
}

func credentialCASTestRequest(
	t *testing.T,
	account store.Account,
	source creds.Source,
	expected store.CredentialExternalState,
	credential *creds.Credential,
) CredentialCASRequest {
	t.Helper()
	payload, err := credential.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return CredentialCASRequest{
		AccountID: account.ID, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		Source: source, Expected: expected, Credential: payload,
	}
}

func runCredentialCASTestWorker(t *testing.T, request CredentialCASRequest) CredentialCASResponse {
	t.Helper()
	var input, output bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		t.Fatal(err)
	}
	if err := RunCredentialCASWorker(t.Context(), &input, &output); err != nil {
		t.Fatal(err)
	}
	var response CredentialCASResponse
	if err := decodeCredentialCASJSON(&output, &response); err != nil {
		t.Fatal(err)
	}
	return response
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
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("credential CAS lock remained at %s: %v", path, err)
		}
	}
}

func writeCredentialCASFakeSecurity(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security")
	script := `#!/bin/sh
set -eu
item="$CCP_CAS_KEYCHAIN_ITEM"
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
    if [ -n "${CCP_CAS_KEYCHAIN_DELETE_FAIL:-}" ] && [ -f "$CCP_CAS_KEYCHAIN_DELETE_FAIL" ]; then
      exit 70
    fi
    rm -f "$item"
    ;;
  *) exit 64 ;;
esac
`
	writePoolTestExecutable(t, path, script)
	return path
}
