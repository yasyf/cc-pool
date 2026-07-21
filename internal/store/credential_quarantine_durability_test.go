package store

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestCredentialQuarantineTokenChainBindingIsExactAndIdempotent(t *testing.T) {
	first := openTest(t)
	second := openSecondStore(t, first)
	account := credentialOperationTestAccount(t, first)
	request := credentialOperationTestRequest(
		t,
		account,
		CredentialOperationEnsureFresh,
		CredentialTargetAll,
		credentialOperationTestState("before", ""),
		"bind-token-chain",
		credentialOperationTestOwner("owner"),
	)
	quarantine, err := first.QuarantineCredential(QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest:     request.LocatorDigest, FileLocatorDigest: request.FileLocatorDigest,
		Observation: request.Expected, Reason: CredentialResultAmbiguous,
		FailureClass: CredentialFailureInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if quarantine.TokenChainDigest != nil {
		t.Fatal("new quarantine unexpectedly had a token-chain binding")
	}
	digest := credentialOperationTestDigest("token-chain")
	timestampMismatch := quarantine
	timestampMismatch.CreatedAt = quarantine.CreatedAt.Add(time.Nanosecond)
	if _, err := second.BindCredentialQuarantineTokenChain(timestampMismatch, digest); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("timestamp-mismatched token-chain bind = %v, want state error", err)
	}
	if err := second.AcknowledgeCredentialQuarantine(timestampMismatch); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("timestamp-mismatched quarantine acknowledgement = %v, want state error", err)
	}
	if err := second.ClearCredentialQuarantine(timestampMismatch); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("timestamp-mismatched quarantine clear = %v, want state error", err)
	}

	for name, candidate := range map[string]*Store{"first": first, "second": second} {
		bound, err := candidate.BindCredentialQuarantineTokenChain(quarantine, digest)
		if err != nil {
			t.Fatalf("%s idempotent bind: %v", name, err)
		}
		if bound.TokenChainDigest == nil || *bound.TokenChainDigest != digest {
			t.Fatalf("%s bound quarantine did not retain the exact digest: %+v", name, bound)
		}
	}

	mismatch := credentialOperationTestDigest("different-token-chain")
	if _, err := first.BindCredentialQuarantineTokenChain(quarantine, mismatch); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("conflicting token-chain bind = %v, want state error", err)
	}
	stored, err := second.CredentialQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TokenChainDigest == nil || *stored.TokenChainDigest != digest {
		t.Fatalf("stored token-chain digest = %v, want %x", stored.TokenChainDigest, digest)
	}
}

func TestCredentialQuarantineBindRacesExactAcknowledgeAndClear(t *testing.T) {
	first := openTest(t)
	second := openSecondStore(t, first)
	account := credentialOperationTestAccount(t, first)
	request := credentialOperationTestRequest(
		t,
		account,
		CredentialOperationEnsureFresh,
		CredentialTargetAll,
		credentialOperationTestState("before", ""),
		"bind-clear-race",
		credentialOperationTestOwner("owner"),
	)
	begin, err := first.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	applying, err := first.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome := credentialOperationTestState("after", "")
	applied, err := first.MarkCredentialOperationApplied(
		applying.Fence(),
		outcome,
		CredentialTerminalQuarantined,
		CredentialResultAmbiguous,
		CredentialFailureInternal,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.CommitCredentialOperation(
		applied.Fence(), outcome, nil, time.Now().Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	quarantine, err := first.CredentialQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	digest := credentialOperationTestDigest("racing-token-chain")

	type bindResult struct {
		bound CredentialQuarantine
		err   error
	}
	const binders = 8
	start := make(chan struct{})
	boundForResolution := make(chan CredentialQuarantine, binders)
	bindResults := make(chan bindResult, binders)
	for i := range binders {
		candidate := first
		if i%2 == 1 {
			candidate = second
		}
		go func() {
			<-start
			bound, err := candidate.BindCredentialQuarantineTokenChain(quarantine, digest)
			if err == nil {
				boundForResolution <- bound
			}
			bindResults <- bindResult{bound: bound, err: err}
		}()
	}
	type resolutionResult struct {
		acknowledge error
		clear       error
	}
	resolutionDone := make(chan resolutionResult, 2)
	go func() {
		bounds := make([]CredentialQuarantine, 0, 2)
		for len(bounds) < 2 {
			select {
			case bound := <-boundForResolution:
				bounds = append(bounds, bound)
			case <-time.After(time.Second):
				resolutionDone <- resolutionResult{
					acknowledge: errors.New("fewer than two binds succeeded before resolution deadline"),
				}
				resolutionDone <- resolutionResult{
					acknowledge: errors.New("fewer than two binds succeeded before resolution deadline"),
				}
				return
			}
		}
		resolveStart := make(chan struct{})
		for index, candidate := range []*Store{first, second} {
			bound := bounds[index]
			go func() {
				<-resolveStart
				acknowledgeErr := candidate.AcknowledgeCredentialQuarantine(bound)
				clearErr := candidate.ClearCredentialQuarantine(bound)
				resolutionDone <- resolutionResult{acknowledge: acknowledgeErr, clear: clearErr}
			}()
		}
		close(resolveStart)
	}()
	close(start)

	for range binders {
		result := <-bindResults
		if result.err == nil {
			if result.bound.TokenChainDigest == nil || *result.bound.TokenChainDigest != digest {
				t.Fatalf("successful bind returned a half-observed row: %+v", result.bound)
			}
			continue
		}
		if !errors.Is(result.err, sql.ErrNoRows) &&
			!errors.Is(result.err, ErrCredentialOperationState) {
			t.Fatalf("racing bind returned non-exact error: %v", result.err)
		}
	}
	successfulClears := 0
	for range 2 {
		resolution := <-resolutionDone
		if resolution.acknowledge != nil &&
			!errors.Is(resolution.acknowledge, sql.ErrNoRows) &&
			!errors.Is(resolution.acknowledge, ErrCredentialOperationState) {
			t.Fatalf("exact acknowledgement: %v", resolution.acknowledge)
		}
		if resolution.clear == nil {
			successfulClears++
		} else if !errors.Is(resolution.clear, sql.ErrNoRows) &&
			!errors.Is(resolution.clear, ErrCredentialOperationState) {
			t.Fatalf("exact clear: %v", resolution.clear)
		}
	}
	if successfulClears != 1 {
		t.Fatalf("successful exact clears = %d, want 1", successfulClears)
	}
	if _, err := first.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantine after racing exact resolution = %v", err)
	}
}
