package pool

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/workerexec"
	"github.com/yasyf/daemonkit/proc"
)

type rejectingTestTaskRunner struct{}

func (rejectingTestTaskRunner) Run(context.Context, workerexec.CommandRequest) (workerexec.CommandResult, error) {
	return workerexec.CommandResult{}, errors.New("unexpected test worker task")
}

func bindTestWorkerAuthority(t *testing.T, manager *Manager, generation string) proc.Record {
	t.Helper()
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	owner := proc.Record{
		RecoveryID: CredentialOwnerRecoveryID,
		PID:        identity.PID,
		StartTime:  identity.StartTime,
		Boot:       identity.Boot,
		Comm:       identity.Comm,
		Executable: identity.Executable,
		AuditToken: identity.AuditToken,
		Generation: poolTestGeneration("test-worker-authority-" + generation),
	}
	if err := owner.Validate(); err != nil {
		t.Fatal(err)
	}
	manager.workerAuthority = &WorkerAuthority{
		runner: rejectingTestTaskRunner{}, executable: identity.Executable, owner: owner,
	}
	var credentialClaim sync.Mutex
	manager.ClaimCredentialMutation = func(int) (func(), error) {
		credentialClaim.Lock()
		return credentialClaim.Unlock, nil
	}
	manager.credentialCAS = testCredentialCAS(manager)
	if manager.BuildCredentialWritePublication == nil {
		manager.BuildCredentialWritePublication = func(
			store.Account,
			*creds.Credential,
			store.CredentialOperationID,
			time.Time,
		) ([]byte, error) {
			return []byte(`{"test":"credential-write"}`), nil
		}
	}
	if manager.SettleCredentialWrite == nil {
		manager.SettleCredentialWrite = func(context.Context, CredentialWriteSettlement) error {
			return nil
		}
	}
	return owner
}

func testCredentialCAS(manager *Manager) credentialCASFunc {
	return func(
		ctx context.Context,
		account store.Account,
		expected store.CredentialExternalState,
		mutation credentialCASMutation,
	) (credentialCASProof, error) {
		before, err := manager.credentialObservationAt(
			ctx, account, mutation.ExpectedPublicPath,
		)
		if err != nil {
			return credentialCASProof{}, err
		}
		proof := credentialCASProof{Before: before, After: before}
		if !sameStoreObservation(before, expected) {
			return proof, errCredentialCASConflict
		}
		if mutation.Refresh {
			target, err := manager.credentialStore(account, mutation.ExpectedPublicPath)
			if err != nil {
				return proof, err
			}
			previous, err := target.Read(ctx)
			if err != nil {
				return proof, err
			}
			response, err := manager.OAuth.Refresh(
				ctx, "acct-test", previous.ClaudeAiOauth.RefreshToken,
			)
			if err != nil {
				return proof, err
			}
			next := *previous
			next.ClaudeAiOauth.AccessToken = response.AccessToken
			if response.RefreshToken != "" {
				next.ClaudeAiOauth.RefreshToken = response.RefreshToken
			}
			next.ClaudeAiOauth.ExpiresAt = response.Expiry(time.Now()).UnixMilli()
			if err := target.Write(ctx, &next); err != nil {
				return proof, err
			}
			proof.Credential = &next
		} else if mutation.Delete {
			target, err := manager.credentialStore(account, mutation.ExpectedPublicPath)
			if err != nil {
				return proof, err
			}
			if err := target.Delete(ctx); err != nil {
				return proof, err
			}
		} else {
			if mutation.Credential == nil {
				return proof, errors.New("test credential CAS mutation is empty")
			}
			target, err := manager.credentialStore(account, mutation.ExpectedPublicPath)
			if err != nil {
				return proof, err
			}
			if err := target.Write(ctx, mutation.Credential); err != nil {
				return proof, err
			}
		}
		after, err := manager.credentialObservationAt(
			ctx, account, mutation.ExpectedPublicPath,
		)
		proof.After = after
		if err != nil {
			return proof, err
		}
		return proof, nil
	}
}

func TestCredentialOwnerRecordFailsClosedWithoutWorkerAuthority(t *testing.T) {
	if _, err := (&Manager{}).credentialOwnerRecord(); err == nil {
		t.Fatal("credential owner synthesized without exact worker authority")
	}

	manager := &Manager{}
	want := bindTestWorkerAuthority(t, manager, "exact-owner")
	got, err := manager.credentialOwnerRecord()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("credential owner = %+v, want %+v", got, want)
	}
}
