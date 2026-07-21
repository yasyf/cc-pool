package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

type credentialWriteTaskRunnerFunc func(context.Context, supervise.Task) error

func (run credentialWriteTaskRunnerFunc) Run(ctx context.Context, task supervise.Task) error {
	return run(ctx, task)
}

func testCredentialWriteSettlement(
	t *testing.T,
	origin string,
	operationID store.CredentialOperationID,
	account store.Account,
	credential *creds.Credential,
	committedAt time.Time,
) pool.CredentialWriteSettlement {
	t.Helper()
	payload, err := credentialWritePublicationBuilder(origin)(
		account, credential, operationID, committedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return pool.CredentialWriteSettlement{
		OperationID: operationID, PublicationPayload: payload,
	}
}

func TestCredentialWriteSettlementIsWorkerBackedAndExactIdempotent(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	registry := hostsync.NewRegistryFile(pool.SyncDir())
	stampDir := pool.SyncStampsDir()
	seed := &hostsync.Service{Registry: registry, StampDir: stampDir}
	if err := seed.PublishAccount(t.Context(), hostsync.AccountValue{
		UUID: "account-uuid",
		Chain: hostsync.ChainStamp{
			Origin: "prior-host", ExpiresAt: 100, Hash: "prior", RotatedAt: 50,
		},
	}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	runner := credentialWriteTaskRunnerFunc(func(ctx context.Context, task supervise.Task) error {
		calls.Add(1)
		if task.RecoveryClass != proc.RecoveryTask || task.Path != "credential-worker" ||
			len(task.Args) != 1 || task.Args[0] != credentialWriteWorkerArgument {
			t.Fatalf("worker task = %+v", task)
		}
		return RunCredentialWriteWorker(ctx, task.Stdin, task.Stdout)
	})
	settler := newCredentialWriteSettler(
		runner, "credential-worker", func() (bool, error) { return true, nil },
		*registry, stampDir, "origin-host",
	)
	credential := creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "access-token"
	credential.ClaudeAiOauth.RefreshToken = "refresh-token"
	credential.ClaudeAiOauth.ExpiresAt = 200
	committedAt := time.Unix(1_700_000_000, 987_654_321)
	settlement := testCredentialWriteSettlement(
		t, "origin-host", store.CredentialOperationID{1},
		store.Account{ID: 1, AccountUUID: "account-uuid"}, &credential, committedAt,
	)
	if err := settler.Settle(t.Context(), settlement); err != nil {
		t.Fatal(err)
	}
	wantChain := hostsync.ChainStamp{
		Origin: "origin-host", ExpiresAt: 200, Hash: creds.AccessHash(&credential),
		RotatedAt: committedAt.UnixMilli(),
	}
	reg, err := registry.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := reg["account-uuid"].Value.Chain; got != wantChain {
		t.Fatalf("published chain = %+v, want %+v", got, wantChain)
	}
	registryBytes, err := os.ReadFile(registry.Path)
	if err != nil {
		t.Fatal(err)
	}
	stampPath := filepath.Join(stampDir, "account-uuid", "stamp")
	stampBytes, err := os.ReadFile(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	fixedModTime := time.Unix(123, 0)
	if err := os.Chtimes(registry.Path, fixedModTime, fixedModTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(stampPath, fixedModTime, fixedModTime); err != nil {
		t.Fatal(err)
	}
	registryInfo, err := os.Stat(registry.Path)
	if err != nil {
		t.Fatal(err)
	}
	stampInfo, err := os.Stat(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := settler.Settle(t.Context(), settlement); err != nil {
		t.Fatal(err)
	}
	afterRegistry, err := os.ReadFile(registry.Path)
	if err != nil {
		t.Fatal(err)
	}
	afterStamp, err := os.ReadFile(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	afterStampInfo, err := os.Stat(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	afterRegistryInfo, err := os.Stat(registry.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterRegistry, registryBytes) || !bytes.Equal(afterStamp, stampBytes) ||
		!afterRegistryInfo.ModTime().Equal(registryInfo.ModTime()) ||
		!afterStampInfo.ModTime().Equal(stampInfo.ModTime()) {
		t.Fatal("exact settlement replay rewrote registry or stamp state")
	}
	if calls.Load() != 2 {
		t.Fatalf("worker calls = %d, want one exact call per settlement attempt", calls.Load())
	}
}

func TestCredentialWriteWorkerRepairsRegistrySavedBeforeStamp(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	registry := hostsync.NewRegistryFile(pool.SyncDir())
	stampDir := pool.SyncStampsDir()
	committedAt := time.UnixMilli(1_700_000_000_123)
	chain := hostsync.ChainStamp{
		Origin: "origin-host", ExpiresAt: 200, Hash: "access-hash",
		RotatedAt: committedAt.UnixMilli(),
	}
	seed := &hostsync.Service{
		Registry: registry, StampDir: stampDir, Now: func() time.Time { return committedAt },
	}
	if err := seed.PublishAccount(t.Context(), hostsync.AccountValue{
		UUID: "account-uuid", Chain: chain,
	}); err != nil {
		t.Fatal(err)
	}
	stampPath := filepath.Join(stampDir, "account-uuid", "stamp")
	if err := os.Remove(stampPath); err != nil {
		t.Fatal(err)
	}
	request := credentialWriteWorkerRequest{
		OperationID: store.CredentialOperationID{2}, RegistryPath: registry.Path,
		RegistryLockPath: registry.LockPath, StampDir: stampDir,
		AccountUUID: "account-uuid", Chain: chain,
	}
	run := func() {
		t.Helper()
		var input, output bytes.Buffer
		if err := json.NewEncoder(&input).Encode(request); err != nil {
			t.Fatal(err)
		}
		if err := RunCredentialWriteWorker(t.Context(), &input, &output); err != nil {
			t.Fatal(err)
		}
		var response credentialWriteWorkerResponse
		if err := json.NewDecoder(&output).Decode(&response); err != nil {
			t.Fatal(err)
		}
		if response.OperationID != request.OperationID {
			t.Fatalf("worker operation ID = %x, want %x", response.OperationID, request.OperationID)
		}
	}
	run()
	wantStamp := []byte("1700000000123000000")
	stamp, err := os.ReadFile(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stamp, wantStamp) {
		t.Fatalf("repaired stamp = %q, want %q", stamp, wantStamp)
	}
	info, err := os.Stat(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	fixedModTime := time.Unix(123, 0)
	if err := os.Chtimes(stampPath, fixedModTime, fixedModTime); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	run()
	after, err := os.Stat(stampPath)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(info.ModTime()) {
		t.Fatal("exact worker replay rewrote an already published stamp")
	}
}

func TestCredentialWriteSettlementStructuralNoopsPrecedePolicy(t *testing.T) {
	var policyCalls atomic.Int32
	t.Setenv("HOME", t.TempDir())
	registry := hostsync.NewRegistryFile(pool.SyncDir())
	settler := newCredentialWriteSettler(
		nil, "", func() (bool, error) {
			policyCalls.Add(1)
			return false, errors.New("policy must not be read")
		},
		*registry, pool.SyncStampsDir(), "origin-host",
	)
	owned := creds.Credential{}
	owned.ClaudeAiOauth.RefreshToken = "refresh"
	withoutRefresh := creds.Credential{}
	for name, settlement := range map[string]pool.CredentialWriteSettlement{
		"uuid absent": testCredentialWriteSettlement(
			t, "origin-host", store.CredentialOperationID{1}, store.Account{}, &owned, time.Now(),
		),
		"refresh absent": testCredentialWriteSettlement(
			t, "origin-host", store.CredentialOperationID{2},
			store.Account{AccountUUID: "uuid"}, &withoutRefresh, time.Now(),
		),
	} {
		t.Run(name, func(t *testing.T) {
			if err := settler.Settle(t.Context(), settlement); err != nil {
				t.Fatal(err)
			}
		})
	}
	if policyCalls.Load() != 0 {
		t.Fatalf("structural no-op policy reads = %d", policyCalls.Load())
	}
}

func TestCredentialWriteSettlementPublicationPolicyIsExact(t *testing.T) {
	metadataErr := errors.New("sync metadata unavailable")
	credential := creds.Credential{}
	credential.ClaudeAiOauth.RefreshToken = "refresh"
	settlement := testCredentialWriteSettlement(
		t, "origin-host", store.CredentialOperationID{8},
		store.Account{AccountUUID: "uuid"}, &credential,
		time.UnixMilli(1_700_000_000_000),
	)
	for name, tc := range map[string]struct {
		enabled bool
		readErr error
		wantErr error
	}{
		"explicitly disabled": {},
		"metadata unreadable": {readErr: metadataErr, wantErr: metadataErr},
	} {
		t.Run(name, func(t *testing.T) {
			var workerCalls atomic.Int32
			runner := credentialWriteTaskRunnerFunc(func(context.Context, supervise.Task) error {
				workerCalls.Add(1)
				return errors.New("worker must not run")
			})
			t.Setenv("HOME", t.TempDir())
			registry := hostsync.NewRegistryFile(pool.SyncDir())
			settler := newCredentialWriteSettler(
				runner, "credential-worker",
				func() (bool, error) { return tc.enabled, tc.readErr },
				*registry, pool.SyncStampsDir(), "origin-host",
			)
			err := settler.Settle(t.Context(), settlement)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("settlement policy result = %v, want %v", err, tc.wantErr)
			}
			if workerCalls.Load() != 0 {
				t.Fatalf("settlement policy worker calls = %d", workerCalls.Load())
			}
		})
	}
}

func TestCredentialWriteSettlementPropagatesCancellationAndRejectsWrongResponse(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	registry := hostsync.NewRegistryFile(pool.SyncDir())
	credential := creds.Credential{}
	credential.ClaudeAiOauth.RefreshToken = "refresh"
	settlement := testCredentialWriteSettlement(
		t, "origin-host", store.CredentialOperationID{3},
		store.Account{AccountUUID: "uuid"}, &credential,
		time.UnixMilli(1_700_000_000_000),
	)
	cancelRunner := credentialWriteTaskRunnerFunc(func(ctx context.Context, task supervise.Task) error {
		_, _ = io.Copy(io.Discard, task.Stdin)
		return ctx.Err()
	})
	settler := newCredentialWriteSettler(
		cancelRunner, "credential-worker", func() (bool, error) { return true, nil },
		*registry, pool.SyncStampsDir(), "origin-host",
	)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := settler.Settle(ctx, settlement); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled settlement = %v, want context cancellation", err)
	}

	wrongResponseRunner := credentialWriteTaskRunnerFunc(func(_ context.Context, task supervise.Task) error {
		_, _ = io.Copy(io.Discard, task.Stdin)
		return json.NewEncoder(task.Stdout).Encode(credentialWriteWorkerResponse{
			OperationID: store.CredentialOperationID{9},
		})
	})
	settler.runner = wrongResponseRunner
	if err := settler.Settle(t.Context(), settlement); err == nil {
		t.Fatal("settlement accepted a mismatched worker operation ID")
	}
}

func TestCredentialWriteWorkerRejectsInexactInvocation(t *testing.T) {
	if !IsCredentialWriteWorkerInvocation([]string{credentialWriteWorkerArgument}) ||
		IsCredentialWriteWorkerInvocation(nil) ||
		IsCredentialWriteWorkerInvocation([]string{credentialWriteWorkerArgument, "extra"}) {
		t.Fatal("credential worker invocation predicate is not exact")
	}
	input := bytes.NewBufferString(`{"operation_id":[1],"unknown":true}`)
	if err := RunCredentialWriteWorker(t.Context(), input, io.Discard); err == nil {
		t.Fatal("credential worker accepted an unknown request field")
	}
}
