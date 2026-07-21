package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestCredentialOperationLifecycleAndPostCommitAdmission(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "refresh-request", credentialOperationTestOwner("owner"))

	begin, err := s.BeginCredentialOperation(request)
	if err != nil || !begin.Created || begin.Active == nil || begin.Receipt != nil {
		t.Fatalf("begin = %+v err=%v", begin, err)
	}
	operation := *begin.Active
	joined, err := s.BeginCredentialOperation(request)
	if err != nil || joined.Created || joined.Active == nil || joined.Active.Token != operation.Token {
		t.Fatalf("join = %+v err=%v", joined, err)
	}
	publication := []byte(`{"version":1,"test":"refresh"}`)
	if _, err := s.MarkCredentialOperationApplying(operation.Fence(), publication); err != nil {
		t.Fatal(err)
	}
	outcome := credentialOperationTestState("after", "")
	if _, err := s.MarkCredentialOperationApplied(
		operation.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultRefreshed, CredentialFailureNone, publication,
	); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitCredentialOperation(operation.Fence(), outcome, nil, now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OperationID != request.OperationID || receipt.Result != CredentialResultRefreshed {
		t.Fatalf("receipt = %+v", receipt)
	}
	if _, err := s.CredentialOperation(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("active operation survived commit: %v", err)
	}

	// A retry after the owner committed but before its response was delivered
	// must replay the immutable semantic receipt, never allocate a new token.
	replayed, err := s.BeginCredentialOperation(request)
	if err != nil || replayed.Active != nil || replayed.Receipt == nil || replayed.Created {
		t.Fatalf("post-commit admission = %+v err=%v", replayed, err)
	}
	if !reflect.DeepEqual(*replayed.Receipt, receipt) {
		t.Fatalf("post-commit receipt = %+v, want %+v", replayed.Receipt, receipt)
	}
	replayedReceipt, err := s.CommitCredentialOperation(operation.Fence(), outcome, nil, now.Add(10*time.Minute))
	if err != nil || !reflect.DeepEqual(replayedReceipt, receipt) {
		t.Fatalf("lost commit response = %+v err=%v", replayedReceipt, err)
	}
	changed := credentialOperationTestState("unrelated", "")
	conflict, err := s.CommitCredentialOperation(operation.Fence(), changed, nil, now.Add(10*time.Minute))
	if !errors.Is(err, ErrCredentialOperationState) || !reflect.DeepEqual(conflict, receipt) {
		t.Fatalf("conflicting immutable receipt = %+v err=%v", conflict, err)
	}
	nextRequest := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		outcome, "next-refresh-request", credentialOperationTestOwner("owner"),
	)
	blocked, err := s.BeginCredentialOperation(nextRequest)
	if !errors.Is(err, ErrCredentialOperationSettlementRequired) ||
		blocked.Receipt == nil || blocked.Receipt.OperationID != receipt.OperationID {
		t.Fatalf("new operation bypassed pending write settlement: begin=%+v err=%v", blocked, err)
	}
	if err := s.AcknowledgeCredentialOperation(receipt.Token); err != nil {
		t.Fatal(err)
	}
	admitted, err := s.BeginCredentialOperation(nextRequest)
	if err != nil || !admitted.Created || admitted.Active == nil {
		t.Fatalf("post-settlement operation not admitted: begin=%+v err=%v", admitted, err)
	}
	if err := s.AbandonPreparedCredentialOperation(admitted.Active.Fence()); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialPublicationPayloadIsBoundedExactAndImmutable(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "publication-exact",
		credentialOperationTestOwner("publication-owner"),
	)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"version":1,"account_uuid":"exact","chain":{"expires_at":42}}`)
	applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), payload)
	if err != nil {
		t.Fatal(err)
	}
	outcome := credentialOperationTestState("after", "")
	applied, err := s.MarkCredentialOperationApplied(
		applying.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultRefreshed, CredentialFailureNone, payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	wantPayload := []byte(`{"version":1,"account_uuid":"exact","chain":{"expires_at":42}}`)
	if !bytes.Equal(applied.PublicationPayload, wantPayload) {
		t.Fatalf("applied payload = %q, want %q", applied.PublicationPayload, wantPayload)
	}
	if _, err := s.MarkCredentialOperationApplied(
		applying.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultRefreshed, CredentialFailureNone, wantPayload,
	); err != nil {
		t.Fatalf("exact applied replay: %v", err)
	}
	if _, err := s.MarkCredentialOperationApplied(
		applying.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultRefreshed, CredentialFailureNone, []byte(`{"version":1,"different":true}`),
	); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("conflicting applied payload = %v, want ErrCredentialOperationState", err)
	}
	receipt, err := s.CommitCredentialOperation(
		applied.Fence(), outcome, nil, now.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(receipt.PublicationPayload, wantPayload) {
		t.Fatalf("receipt payload = %q, want %q", receipt.PublicationPayload, wantPayload)
	}
	receipt.PublicationPayload[0] = 'Y'
	stored, err := s.CredentialOperationReceiptByID(request.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored.PublicationPayload, wantPayload) {
		t.Fatalf("stored payload mutated through caller alias: %q", stored.PublicationPayload)
	}
	replayed, err := s.CommitCredentialOperation(
		applied.Fence(), outcome, nil, now.Add(2*time.Minute),
	)
	if err != nil || !reflect.DeepEqual(replayed, stored) {
		t.Fatalf("lost-response replay = %+v err=%v, want %+v", replayed, err, stored)
	}
}

func TestCredentialPublicationPayloadSurvivesApplyingCrash(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "credential-publication.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "publication-crash",
		credentialOperationTestOwner("publication-crash-owner"),
	)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"version":1,"operation":"before-external-io"}`)
	applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	applying, err = s.StageCredentialOperationPublication(applying.Fence(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.StageCredentialOperationPublication(
		applying.Fence(), []byte(`{"version":1,"operation":"changed"}`),
	); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("changed staged publication payload = %v, want ErrCredentialOperationState", err)
	}
	payload[0] = 'X'
	wantPayload := []byte(`{"version":1,"operation":"before-external-io"}`)
	if !bytes.Equal(applying.PublicationPayload, wantPayload) {
		t.Fatalf("applying payload = %q, want %q", applying.PublicationPayload, wantPayload)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	recovered, err := s.CredentialOperationByToken(applying.Token)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recovered.PublicationPayload, wantPayload) {
		t.Fatalf("recovered applying payload = %q, want %q", recovered.PublicationPayload, wantPayload)
	}
	outcome := credentialOperationTestState("after", "")
	if _, err := s.MarkCredentialOperationApplied(
		recovered.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultRefreshed, CredentialFailureNone, []byte(`{"version":1,"operation":"changed"}`),
	); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("changed post-I/O publication payload = %v, want ErrCredentialOperationState", err)
	}
	applied, err := s.MarkCredentialOperationApplied(
		recovered.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultRefreshed, CredentialFailureNone, wantPayload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(applied.PublicationPayload, wantPayload) {
		t.Fatalf("applied payload = %q, want %q", applied.PublicationPayload, wantPayload)
	}
}

func TestCredentialPublicationPayloadTerminalContract(t *testing.T) {
	tests := []struct {
		name    string
		result  CredentialResultCategory
		payload []byte
	}{
		{name: "publishing result requires payload", result: CredentialResultInstalled},
		{name: "non-publishing result forbids payload", result: CredentialResultDone, payload: []byte(`{"version":1}`)},
		{name: "publishing payload is bounded", result: CredentialResultMoved, payload: bytes.Repeat([]byte{'x'}, CredentialPublicationPayloadMaxBytes+1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			account := credentialOperationTestAccount(t, s)
			request := credentialOperationTestRequest(
				t, account, CredentialOperationMove, CredentialTargetFile,
				credentialOperationTestState("before", ""), test.name,
				credentialOperationTestOwner(test.name),
			)
			begin, err := s.BeginCredentialOperation(request)
			if err != nil {
				t.Fatal(err)
			}
			applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.MarkCredentialOperationApplied(
				applying.Fence(), applying.Expected, CredentialTerminalSucceeded,
				test.result, CredentialFailureNone, test.payload,
			); err == nil {
				t.Fatal("invalid publication payload was accepted")
			}
		})
	}
}

func TestCredentialFailureClassRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		kind    CredentialOperationKind
		target  CredentialTarget
		status  CredentialTerminalStatus
		result  CredentialResultCategory
		failure CredentialFailureClass
	}{
		{
			name: "internal failure", kind: CredentialOperationAdoptRotated,
			target: CredentialTargetKeychain, status: CredentialTerminalFailed,
			result: CredentialResultFailed, failure: CredentialFailureInternal,
		},
		{
			name: "refresh unauthorized", kind: CredentialOperationEnsureFresh,
			target: CredentialTargetAll, status: CredentialTerminalFailed,
			result: CredentialResultFailed, failure: CredentialFailureRefreshUnauthorized,
		},
		{
			name: "refresh rejected", kind: CredentialOperationRefreshCurrent,
			target: CredentialTargetAll, status: CredentialTerminalFailed,
			result: CredentialResultFailed, failure: CredentialFailureRefreshRejected,
		},
		{
			name: "internal quarantine", kind: CredentialOperationMove,
			target: CredentialTargetFile, status: CredentialTerminalQuarantined,
			result: CredentialResultAmbiguous, failure: CredentialFailureInternal,
		},
		{
			name: "network ambiguity", kind: CredentialOperationEnsureFresh,
			target: CredentialTargetAll, status: CredentialTerminalQuarantined,
			result: CredentialResultAmbiguous, failure: CredentialFailureNetwork,
		},
		{
			name: "server ambiguity", kind: CredentialOperationRefreshCurrent,
			target: CredentialTargetAll, status: CredentialTerminalQuarantined,
			result: CredentialResultAmbiguous, failure: CredentialFailureRefreshServer,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			now := time.Unix(1_900_000_000, int64(index+1))
			s.now = func() time.Time { return now }
			account := credentialOperationTestAccount(t, s)
			request := credentialOperationTestRequest(
				t, account, test.kind, test.target,
				credentialOperationTestState("before", ""), test.name,
				credentialOperationTestOwner(test.name),
			)
			begin, err := s.BeginCredentialOperation(request)
			if err != nil {
				t.Fatal(err)
			}
			applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
			if err != nil {
				t.Fatal(err)
			}
			outcome := applying.Expected
			applied, err := s.MarkCredentialOperationApplied(
				applying.Fence(), outcome, test.status, test.result, test.failure, nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			second := openSecondStore(t, s)
			reopened, err := second.CredentialOperationByToken(applied.Token)
			if err != nil {
				t.Fatal(err)
			}
			if reopened.FailureClass != test.failure || reopened.TerminalStatus != test.status ||
				reopened.Result != test.result {
				t.Fatalf("reopened operation = %+v", reopened)
			}
			receipt, err := second.CommitCredentialOperation(
				reopened.Fence(), outcome, nil, now.Add(time.Minute),
			)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := s.CredentialOperationReceiptByID(receipt.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.FailureClass != test.failure || stored.TerminalStatus != test.status ||
				stored.Result != test.result {
				t.Fatalf("stored receipt = %+v", stored)
			}
		})
	}
}

func TestCredentialFailureClassRejectsInvalidCombinations(t *testing.T) {
	tests := []struct {
		name    string
		kind    CredentialOperationKind
		status  CredentialTerminalStatus
		result  CredentialResultCategory
		failure CredentialFailureClass
	}{
		{
			name: "success with failure", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalSucceeded, result: CredentialResultUnchanged,
			failure: CredentialFailureInternal,
		},
		{
			name: "failure without class", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalFailed, result: CredentialResultFailed,
			failure: CredentialFailureNone,
		},
		{
			name: "failed quarantine result", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalFailed, result: CredentialResultAmbiguous,
			failure: CredentialFailureInternal,
		},
		{
			name: "quarantine without class", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalQuarantined, result: CredentialResultAmbiguous,
			failure: CredentialFailureNone,
		},
		{
			name: "refresh class on move", kind: CredentialOperationMove,
			status: CredentialTerminalFailed, result: CredentialResultFailed,
			failure: CredentialFailureRefreshUnauthorized,
		},
		{
			name: "network is not definitive failure", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalFailed, result: CredentialResultFailed,
			failure: CredentialFailureNetwork,
		},
		{
			name: "server is not definitive failure", kind: CredentialOperationRefreshCurrent,
			status: CredentialTerminalFailed, result: CredentialResultFailed,
			failure: CredentialFailureRefreshServer,
		},
		{
			name: "unauthorized is not ambiguous", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalQuarantined, result: CredentialResultAmbiguous,
			failure: CredentialFailureRefreshUnauthorized,
		},
		{
			name: "unknown class", kind: CredentialOperationEnsureFresh,
			status: CredentialTerminalFailed, result: CredentialResultFailed,
			failure: CredentialFailureClass("arbitrary"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			account := credentialOperationTestAccount(t, s)
			target := CredentialTargetAll
			if test.kind == CredentialOperationMove {
				target = CredentialTargetFile
			} else if test.kind != CredentialOperationEnsureFresh &&
				test.kind != CredentialOperationRefreshCurrent {
				target = CredentialTargetKeychain
			}
			request := credentialOperationTestRequest(
				t, account, test.kind, target, credentialOperationTestState("before", ""),
				test.name, credentialOperationTestOwner(test.name),
			)
			begin, err := s.BeginCredentialOperation(request)
			if err != nil {
				t.Fatal(err)
			}
			applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.MarkCredentialOperationApplied(
				applying.Fence(), applying.Expected, test.status, test.result, test.failure, nil,
			); err == nil {
				t.Fatal("invalid terminal failure combination was accepted")
			}
		})
	}
}

func TestCredentialFailureClassExactIdempotency(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "failure-idempotency",
		credentialOperationTestOwner("failure-idempotency"),
	)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := s.MarkCredentialOperationApplied(
		applying.Fence(), applying.Expected, CredentialTerminalFailed,
		CredentialResultFailed, CredentialFailureRefreshUnauthorized, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.MarkCredentialOperationApplied(
		applying.Fence(), applying.Expected, CredentialTerminalFailed,
		CredentialResultFailed, CredentialFailureRefreshUnauthorized, nil,
	)
	if err != nil || !reflect.DeepEqual(replayed, applied) {
		t.Fatalf("exact applied replay = %+v err=%v, want %+v", replayed, err, applied)
	}
	if _, err := s.MarkCredentialOperationApplied(
		applying.Fence(), applying.Expected, CredentialTerminalFailed,
		CredentialResultFailed, CredentialFailureRefreshRejected, nil,
	); !errors.Is(err, ErrCredentialOperationState) {
		t.Fatalf("changed applied failure class = %v, want ErrCredentialOperationState", err)
	}
	if _, err := s.db.Exec(
		`UPDATE credential_operations SET failure_class=NULL WHERE token=?`, applied.Token,
	); err == nil {
		t.Fatal("SQLite accepted a failure class inconsistent with the terminal status")
	}
	if _, err := s.db.Exec(
		`UPDATE credential_operations SET result_category='ambiguous' WHERE token=?`, applied.Token,
	); err == nil {
		t.Fatal("SQLite accepted a result inconsistent with the terminal status")
	}

	recoveryOwner := credentialOperationTestOwner("failure-resolve")
	retirement, verifier := credentialOperationTestRetirement(t, applied.Owner, recoveryOwner)
	taken, err := s.TakeoverCredentialOperation(
		t.Context(), applied.Fence(), recoveryOwner, retirement, verifier,
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.ResolveCredentialOperation(
		taken.Fence(), taken.Expected, CredentialTerminalFailed, CredentialResultFailed,
		CredentialFailureRefreshUnauthorized, nil, now.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := s.ResolveCredentialOperation(
		taken.Fence(), taken.Expected, CredentialTerminalFailed, CredentialResultFailed,
		CredentialFailureRefreshUnauthorized, nil, now.Add(2*time.Minute),
	)
	if err != nil || !reflect.DeepEqual(exact, receipt) {
		t.Fatalf("exact resolve replay = %+v err=%v, want %+v", exact, err, receipt)
	}
	conflict, err := s.ResolveCredentialOperation(
		taken.Fence(), taken.Expected, CredentialTerminalFailed, CredentialResultFailed,
		CredentialFailureRefreshRejected, nil, now.Add(2*time.Minute),
	)
	if !errors.Is(err, ErrCredentialOperationState) || !reflect.DeepEqual(conflict, receipt) {
		t.Fatalf("changed resolve failure class = %+v err=%v", conflict, err)
	}
}

func TestResolveCredentialOperationPublicationPayloadReplayIsExact(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationMove, CredentialTargetFile,
		credentialOperationTestState("before", ""), "resolve-publication",
		credentialOperationTestOwner("resolve-owner"),
	)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	recoveryOwner := credentialOperationTestOwner("resolve-recovery")
	retirement, verifier := credentialOperationTestRetirement(t, applying.Owner, recoveryOwner)
	taken, err := s.TakeoverCredentialOperation(
		t.Context(), applying.Fence(), recoveryOwner, retirement, verifier,
	)
	if err != nil {
		t.Fatal(err)
	}
	outcome := credentialOperationTestState("", "after")
	payload := []byte(`{"version":1,"recovered":true}`)
	receipt, err := s.ResolveCredentialOperation(
		taken.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultMoved, CredentialFailureNone, payload, now.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.ResolveCredentialOperation(
		taken.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultMoved, CredentialFailureNone, payload, now.Add(2*time.Minute),
	)
	if err != nil || !reflect.DeepEqual(replayed, receipt) {
		t.Fatalf("exact resolve replay = %+v err=%v, want %+v", replayed, err, receipt)
	}
	conflict, err := s.ResolveCredentialOperation(
		taken.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultMoved, CredentialFailureNone,
		[]byte(`{"version":1,"recovered":false}`), now.Add(2*time.Minute),
	)
	if !errors.Is(err, ErrCredentialOperationState) || !reflect.DeepEqual(conflict, receipt) {
		t.Fatalf("conflicting resolve replay = %+v err=%v", conflict, err)
	}
}

func TestCredentialOperationSemanticIntentFencesActiveAndReceiptReplay(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "active"
		if terminal {
			name = "receipt"
		}
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			now := time.Unix(1_900_000_000, 0)
			s.now = func() time.Time { return now }
			account := credentialOperationTestAccount(t, s)
			request := credentialOperationTestRequest(
				t, account, CredentialOperationAdoptRotated, CredentialTargetKeychain,
				credentialOperationTestState("before", ""), "semantic-intent",
				credentialOperationTestOwner("owner"),
			)
			begin, err := s.BeginCredentialOperation(request)
			if err != nil {
				t.Fatal(err)
			}
			table := "credential_operations"
			if terminal {
				if _, err := s.CommitPreparedCredentialOperation(
					begin.Active.Fence(), begin.Active.Expected,
					CredentialResultDone, now.Add(time.Minute),
				); err != nil {
					t.Fatal(err)
				}
				table = "credential_operation_receipts"
			}
			changed := credentialOperationTestDigest("different-semantic-intent")
			if _, err := s.db.Exec(
				"UPDATE "+table+" SET intent_digest=? WHERE operation_id=?",
				changed[:], request.OperationID[:],
			); err != nil {
				t.Fatal(err)
			}
			if _, err := s.BeginCredentialOperation(request); !errors.Is(err, ErrCredentialOperationRecoveryRequired) {
				t.Fatalf("mismatched %s semantic intent admitted: %v", name, err)
			}
		})
	}
}

func TestCredentialOperationOwnerEpochSurvivesArbitraryAge(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	owner := credentialOperationTestOwner("owner-a")
	other := credentialOperationTestOwner("owner-b")
	request := credentialOperationTestRequest(
		t, account, CredentialOperationMove, CredentialTargetFile,
		credentialOperationTestState("source", ""), "move-file", owner)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	operation := *begin.Active
	retirement, verifier := credentialOperationTestRetirement(t, owner, other)
	wrongFence := CredentialOperationFence{Token: operation.Token, Owner: other, Epoch: operation.OwnerEpoch}
	if _, err := s.MarkCredentialOperationApplying(wrongFence, nil); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("wrong owner transition = %v", err)
	}
	if err := s.AbandonPreparedCredentialOperation(wrongFence); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("wrong owner abandon = %v", err)
	}
	if _, err := s.TakeoverCredentialOperation(
		t.Context(), operation.Fence(), other, proc.ReapReceipt{}, nil,
	); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("takeover without exact retirement proof = %v", err)
	}
	taken, err := s.TakeoverCredentialOperation(
		t.Context(), operation.Fence(), other, retirement, verifier,
	)
	if err != nil || taken.OwnerEpoch != operation.OwnerEpoch+1 || !sameCredentialOwner(taken.Owner, other) {
		t.Fatalf("immediate verified takeover = %+v err=%v", taken, err)
	}
	if _, err := s.TakeoverCredentialOperation(
		t.Context(), operation.Fence(), other, retirement, verifier,
	); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("stale takeover fence = %v", err)
	}
	recoveredTakeover, err := s.CredentialOperationByToken(operation.Token)
	if err != nil || recoveredTakeover.OwnerEpoch != taken.OwnerEpoch ||
		!sameCredentialOwner(recoveredTakeover.Owner, other) {
		t.Fatalf("lost takeover response recovery = %+v err=%v", recoveredTakeover, err)
	}
	if _, err := s.MarkCredentialOperationApplying(operation.Fence(), nil); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("stale owner after takeover = %v", err)
	}
	publication := []byte(`{"version":1,"test":"move"}`)
	if _, err := s.MarkCredentialOperationApplying(taken.Fence(), publication); err != nil {
		t.Fatal(err)
	}
	outcome := credentialOperationTestState("", "source")
	if _, err := s.MarkCredentialOperationApplied(
		taken.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultMoved, CredentialFailureNone, publication,
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(36 * time.Minute)
	if _, err := s.CommitCredentialOperation(taken.Fence(), outcome, nil, now.Add(10*time.Minute)); err != nil {
		t.Fatalf("aged exact-owner commit = %v", err)
	}
}

func TestCredentialOperationAgeAloneCannotTakeover(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	owner := credentialOperationTestOwner("expired-owner")
	request := credentialOperationTestRequest(
		t, account, CredentialOperationMove, CredentialTargetFile,
		credentialOperationTestState("source", ""), "expired-move", owner)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(36 * time.Minute)
	if _, err := s.TakeoverCredentialOperation(
		t.Context(), begin.Active.Fence(), credentialOperationTestOwner("new-owner"),
		proc.ReapReceipt{}, nil,
	); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("aged lane takeover without proof = %v", err)
	}
}

func TestCredentialOperationGenerationAndLocatorDriftPreservesEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(Account) Account
	}{
		{
			name: "generation",
			mutate: func(account Account) Account {
				account.ConfigDir += "-replacement"
				return account
			},
		},
		{
			name: "locator",
			mutate: func(account Account) Account {
				account.KeychainService += ".replacement"
				return account
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			now := time.Unix(1_900_000_000, 0)
			s.now = func() time.Time { return now }
			account := credentialOperationTestAccount(t, s)
			request := credentialOperationTestRequest(
				t, account, CredentialOperationInstallSynced, CredentialTargetFile,
				credentialOperationTestState("before", ""), "install", credentialOperationTestOwner("owner"))
			begin, err := s.BeginCredentialOperation(request)
			if err != nil {
				t.Fatal(err)
			}
			operation := *begin.Active
			publication := []byte(`{"version":1,"test":"install"}`)
			if _, err := s.MarkCredentialOperationApplying(operation.Fence(), publication); err != nil {
				t.Fatal(err)
			}
			if err := s.UpsertAccount(test.mutate(account)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.MarkCredentialOperationApplied(
				operation.Fence(), credentialOperationTestState("before", "after"),
				CredentialTerminalSucceeded, CredentialResultInstalled,
				CredentialFailureNone, publication,
			); !errors.Is(err, ErrAccountGenerationChanged) {
				t.Fatalf("transition after drift = %v", err)
			}
			now = now.Add(time.Minute)
			recoveryOwner := credentialOperationTestOwner("recovery")
			retirement, verifier := credentialOperationTestRetirement(t, operation.Owner, recoveryOwner)
			taken, err := s.TakeoverCredentialOperation(
				t.Context(), operation.Fence(), recoveryOwner, retirement, verifier,
			)
			if err != nil {
				t.Fatal(err)
			}
			actual := credentialOperationTestState("before", "ambiguous")
			receipt, err := s.ResolveCredentialOperation(
				taken.Fence(), actual, CredentialTerminalQuarantined,
				CredentialResultAmbiguous, CredentialFailureInternal, nil, now.Add(10*time.Minute),
			)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.AccountInstanceID != operation.AccountInstanceID ||
				receipt.AccountGeneration != operation.AccountGeneration ||
				receipt.LocatorDigest != operation.LocatorDigest {
				t.Fatalf("receipt lost original fence = %+v", receipt)
			}
			if _, err := s.CredentialOperationReceiptByID(operation.OperationID); err != nil {
				t.Fatalf("drift receipt missing: %v", err)
			}
			if err := s.DeleteAccount(account.ID); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
				t.Fatalf("delete erased drift evidence: %v", err)
			}
		})
	}
}

func TestDeleteAccountRejectsActiveAndUnacknowledgedEvidence(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationAdoptRotated, CredentialTargetKeychain,
		credentialOperationTestState("same", ""), "adopt", credentialOperationTestOwner("owner"))
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	operation := *begin.Active
	if err := s.DeleteAccount(account.ID); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
		t.Fatalf("delete active = %v", err)
	}
	receipt, err := s.CommitPreparedCredentialOperation(
		operation.Fence(), operation.Expected, CredentialResultDone, now.Add(10*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(account.ID); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
		t.Fatalf("delete unacknowledged receipt = %v", err)
	}
	if err := s.AcknowledgeCredentialOperation(receipt.Token); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := s.CredentialOperationReceipt(receipt.Token)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.AcknowledgedAt.IsZero() {
		t.Fatal("receipt acknowledgement was not persisted")
	}
	replay, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Receipt == nil || replay.Active != nil || replay.Receipt.OperationID != request.OperationID {
		t.Fatalf("acknowledged receipt was not replayed: %+v", replay)
	}
	if err := s.DeleteAccount(account.ID); err != nil {
		t.Fatalf("delete after acknowledgement: %v", err)
	}
	if _, err := s.GetAccount(account.ID); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("tombstoned account remained visible: %v", err)
	}
	if err := s.UpsertAccount(account); err == nil {
		t.Fatal("upsert resurrected a tombstoned account identity")
	}
	replacement := account
	replacement.ID++
	if err := s.UpsertAccount(replacement); err != nil {
		t.Fatalf("tombstone retained live config-dir uniqueness: %v", err)
	}
	replay, err = s.BeginCredentialOperation(request)
	if err != nil || replay.Receipt == nil || replay.Receipt.OperationID != request.OperationID {
		t.Fatalf("tombstoned account lost receipt replay: %+v err=%v", replay, err)
	}
}

func TestCredentialOperationAcknowledgementPreservesMultiwaiterLostResponse(t *testing.T) {
	first := openTest(t)
	second := openSecondStore(t, first)
	now := time.Unix(1_900_000_000, 0)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, first)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("same", ""), "refresh", credentialOperationTestOwner("owner"))
	begin, err := first.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := first.CommitPreparedCredentialOperation(
		begin.Active.Fence(), begin.Active.Expected, CredentialResultUnchanged,
		now.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.AcknowledgeCredentialOperation(receipt.Token); err != nil {
		t.Fatal(err)
	}
	if err := second.AcknowledgeCredentialOperation(receipt.Token); err != nil {
		t.Fatalf("lost acknowledgement response was not idempotent: %v", err)
	}
	for name, candidate := range map[string]*Store{"first": first, "second": second} {
		t.Run(name, func(t *testing.T) {
			replayed, err := candidate.BeginCredentialOperation(request)
			if err != nil {
				t.Fatal(err)
			}
			if replayed.Receipt == nil || replayed.Receipt.OperationID != receipt.OperationID ||
				replayed.Receipt.Result != CredentialResultUnchanged {
				t.Fatalf("lost-response waiter did not receive immutable receipt: %+v", replayed)
			}
		})
	}
	if deleted, err := first.DeleteExpiredCredentialOperationReceipts(1); err != nil || deleted != 0 {
		t.Fatalf("receipt was collected inside post-ack window: deleted=%d err=%v", deleted, err)
	}
	now = now.Add(credentialReceiptPostAckRetention + time.Minute)
	if deleted, err := first.DeleteExpiredCredentialOperationReceipts(1); err != nil || deleted != 1 {
		t.Fatalf("acknowledged receipt was not collected after retention: deleted=%d err=%v", deleted, err)
	}
}

func TestCredentialQuarantineRetainsAcknowledgedReceiptUntilExplicitClear(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "ambiguous-refresh",
		credentialOperationTestOwner("owner"),
	)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	actual := credentialOperationTestState("after", "")
	applied, err := s.MarkCredentialOperationApplied(
		applying.Fence(), actual, CredentialTerminalQuarantined,
		CredentialResultAmbiguous, CredentialFailureInternal, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CommitCredentialOperation(
		applied.Fence(), actual, nil, now.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	quarantine, err := s.CredentialQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClearCredentialQuarantine(quarantine); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
		t.Fatalf("unacknowledged receipt did not fence quarantine clear: %v", err)
	}
	if err := s.AcknowledgeCredentialQuarantine(quarantine); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeCredentialQuarantine(quarantine); err != nil {
		t.Fatalf("idempotent quarantine acknowledgement: %v", err)
	}
	mismatched := quarantine
	mismatched.FailureClass = CredentialFailureNetwork
	if err := s.ClearCredentialQuarantine(mismatched); err == nil {
		t.Fatal("failure-class-mismatched quarantine clear succeeded")
	}
	retained, err := s.CredentialQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !sameCredentialQuarantine(retained, quarantine) {
		t.Fatalf("failure-class-mismatched clear changed quarantine: got %+v want %+v", retained, quarantine)
	}
	now = now.Add(credentialReceiptPostAckRetention + 2*time.Minute)
	if deleted, err := s.DeleteExpiredCredentialOperationReceipts(1); err != nil || deleted != 0 {
		t.Fatalf("quarantined receipt collected: deleted=%d err=%v", deleted, err)
	}
	if err := s.ClearCredentialQuarantine(quarantine); err != nil {
		t.Fatal(err)
	}
	if deleted, err := s.DeleteExpiredCredentialOperationReceipts(1); err != nil || deleted != 1 {
		t.Fatalf("cleared receipt retention deleted=%d err=%v", deleted, err)
	}
}

func TestCredentialOperationConcurrentConflictingSettlementIsImmutable(t *testing.T) {
	first := openTest(t)
	second := openSecondStore(t, first)
	now := time.Unix(1_900_000_000, 0)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, first)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationInstallSynced, CredentialTargetFile,
		credentialOperationTestState("before", ""), "install", credentialOperationTestOwner("owner"))
	begin, err := first.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	operation := *begin.Active
	publication := []byte(`{"version":1,"test":"install"}`)
	if _, err := first.MarkCredentialOperationApplying(operation.Fence(), publication); err != nil {
		t.Fatal(err)
	}
	outcome := credentialOperationTestState("before", "installed")
	if _, err := first.MarkCredentialOperationApplied(
		operation.Fence(), outcome, CredentialTerminalSucceeded,
		CredentialResultInstalled, CredentialFailureNone, publication,
	); err != nil {
		t.Fatal(err)
	}
	type result struct {
		receipt CredentialOperationReceipt
		err     error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, candidate := range []struct {
		store   *Store
		outcome CredentialExternalState
	}{
		{first, outcome},
		{second, credentialOperationTestState("different", "installed")},
	} {
		wait.Add(1)
		go func(candidate struct {
			store   *Store
			outcome CredentialExternalState
		},
		) {
			defer wait.Done()
			receipt, err := candidate.store.CommitCredentialOperation(
				operation.Fence(), candidate.outcome, nil, now.Add(10*time.Minute),
			)
			results <- result{receipt: receipt, err: err}
		}(candidate)
	}
	wait.Wait()
	close(results)
	var succeeded, conflicted int
	var winner CredentialOperationReceipt
	for result := range results {
		switch {
		case result.err == nil:
			succeeded++
			winner = result.receipt
		case errors.Is(result.err, ErrCredentialOperationState),
			errors.Is(result.err, ErrCredentialOperationRecoveryRequired):
			conflicted++
		default:
			t.Fatalf("settlement error = %v", result.err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("settlements succeeded=%d conflicted=%d", succeeded, conflicted)
	}
	persisted, err := first.CredentialOperationReceipt(operation.Token)
	if err != nil || !reflect.DeepEqual(persisted, winner) {
		t.Fatalf("persisted receipt = %+v err=%v, winner %+v", persisted, err, winner)
	}
	conflicting := credentialOperationTestState("different", "installed")
	replayed, err := second.CommitCredentialOperation(
		operation.Fence(), conflicting, nil, now.Add(10*time.Minute),
	)
	if !errors.Is(err, ErrCredentialOperationState) || !reflect.DeepEqual(replayed, persisted) {
		t.Fatalf("conflicting replay receipt = %+v err=%v", replayed, err)
	}
}

func TestCredentialOperationRejectsSecretBearingStructuralFields(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	const canary = "sk-ant-secret-canary"
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "refresh", credentialOperationTestOwner("owner"))
	request.Kind = CredentialOperationKind(canary)
	if _, err := s.BeginCredentialOperation(request); err == nil {
		t.Fatal("secret-bearing operation kind was accepted")
	}
	request = credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "refresh", credentialOperationTestOwner("owner"))
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkCredentialOperationApplied(
		begin.Active.Fence(), begin.Active.Expected, CredentialTerminalSucceeded,
		CredentialResultCategory(canary), CredentialFailureNone, nil,
	); err == nil {
		t.Fatal("secret-bearing result category was accepted")
	}
	if _, err := s.MarkCredentialOperationApplied(
		begin.Active.Fence(), begin.Active.Expected, CredentialTerminalFailed,
		CredentialResultFailed, CredentialFailureClass(canary), nil,
	); err == nil {
		t.Fatal("secret-bearing failure class was accepted")
	}
	if _, err := s.QuarantineCredential(QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest: CredentialLocatorDigest(
			account.KeychainService, account.KeychainAccount, account.ConfigDir,
		),
		FileLocatorDigest: CredentialFileLocatorDigest(account.ConfigDir),
		Observation:       begin.Active.Expected, Reason: CredentialResultCategory(canary),
		FailureClass: CredentialFailureInternal,
	}); err == nil {
		t.Fatal("secret-bearing quarantine reason was accepted")
	}
	path := storeDatabasePath(t, s)
	if _, err := s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	directory, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := directory.ReadFile(filepath.Base(path))
	if err := errors.Join(readErr, directory.Close()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), canary) {
		t.Fatal("secret-bearing structural value reached SQLite")
	}
}

func TestCredentialOperationOwnerPagingBoundary(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	owner := credentialOperationTestOwner("page-owner")
	for id := 1; id <= CredentialOperationPageLimit+1; id++ {
		account := credentialOperationTestAccountID(t, s, id)
		request := credentialOperationTestRequest(
			t, account, CredentialOperationDropDivergent, CredentialTargetKeychain,
			credentialOperationTestState(fmt.Sprintf("credential-%d", id), ""),
			fmt.Sprintf("drop-%d", id), owner)
		if begin, err := s.BeginCredentialOperation(request); err != nil || !begin.Created {
			t.Fatalf("begin account %d = %+v err=%v", id, begin, err)
		}
	}
	first, more, err := s.CredentialOperationsOwnedBy(owner, 0, CredentialOperationPageLimit)
	if err != nil || len(first) != CredentialOperationPageLimit || !more {
		t.Fatalf("first page len=%d more=%v err=%v", len(first), more, err)
	}
	second, more, err := s.CredentialOperationsOwnedBy(
		owner, first[len(first)-1].AccountID, CredentialOperationPageLimit,
	)
	if err != nil || len(second) != 1 || more {
		t.Fatalf("second page len=%d more=%v err=%v", len(second), more, err)
	}
	if second[0].AccountID != CredentialOperationPageLimit+1 {
		t.Fatalf("second page account=%d", second[0].AccountID)
	}
	if _, _, err := s.CredentialOperationsOwnedBy(owner, 0, CredentialOperationPageLimit+1); err == nil {
		t.Fatal("oversized owner page was accepted")
	}
}

func TestUnacknowledgedCredentialWriteReceiptPaging(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	for id := 1; id <= 2; id++ {
		account := credentialOperationTestAccountID(t, s, id)
		request := credentialOperationTestRequest(
			t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
			credentialOperationTestState(fmt.Sprintf("before-%d", id), ""),
			fmt.Sprintf("refresh-%d", id), credentialOperationTestOwner("receipt-page"),
		)
		begin, err := s.BeginCredentialOperation(request)
		if err != nil {
			t.Fatal(err)
		}
		publication := []byte(fmt.Sprintf(`{"version":1,"account":%d}`, id))
		applying, err := s.MarkCredentialOperationApplying(begin.Active.Fence(), publication)
		if err != nil {
			t.Fatal(err)
		}
		outcome := credentialOperationTestState(fmt.Sprintf("after-%d", id), "")
		applied, err := s.MarkCredentialOperationApplied(
			applying.Fence(), outcome,
			CredentialTerminalSucceeded, CredentialResultRefreshed,
			CredentialFailureNone, publication,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.CommitCredentialOperation(
			applied.Fence(), outcome, nil, now.Add(time.Minute),
		); err != nil {
			t.Fatal(err)
		}
	}
	first, more, err := s.UnacknowledgedCredentialWriteReceipts(0, 1)
	if err != nil || !more || len(first) != 1 || first[0].AccountID != 1 {
		t.Fatalf("first receipt page = %+v more=%v err=%v", first, more, err)
	}
	second, more, err := s.UnacknowledgedCredentialWriteReceipts(first[0].AccountID, 1)
	if err != nil || more || len(second) != 1 || second[0].AccountID != 2 {
		t.Fatalf("second receipt page = %+v more=%v err=%v", second, more, err)
	}
}

func TestCredentialOperationReceiptGCIsBounded(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	for id := 1; id <= CredentialOperationPageLimit+1; id++ {
		account := credentialOperationTestAccountID(t, s, id)
		request := credentialOperationTestRequest(
			t, account, CredentialOperationAdoptRotated, CredentialTargetKeychain,
			credentialOperationTestState(fmt.Sprintf("credential-%d", id), ""),
			fmt.Sprintf("adopt-%d", id), credentialOperationTestOwner("gc-owner"))
		begin, err := s.BeginCredentialOperation(request)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := s.CommitPreparedCredentialOperation(
			begin.Active.Fence(), begin.Active.Expected,
			CredentialResultDone, now.Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.AcknowledgeCredentialOperation(receipt.Token); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(credentialReceiptPostAckRetention + time.Minute)
	if _, err := s.DeleteExpiredCredentialOperationReceipts(CredentialOperationPageLimit + 1); err == nil {
		t.Fatal("oversized receipt GC was accepted")
	}
	deleted, err := s.DeleteExpiredCredentialOperationReceipts(CredentialOperationPageLimit)
	if err != nil || deleted != CredentialOperationPageLimit {
		t.Fatalf("first receipt GC deleted=%d err=%v", deleted, err)
	}
	deleted, err = s.DeleteExpiredCredentialOperationReceipts(CredentialOperationPageLimit)
	if err != nil || deleted != 1 {
		t.Fatalf("second receipt GC deleted=%d err=%v", deleted, err)
	}
	account := credentialOperationTestAccountID(t, s, CredentialOperationPageLimit+2)
	request := credentialOperationTestRequest(
		t, account, CredentialOperationAdoptRotated, CredentialTargetKeychain,
		credentialOperationTestState("unacknowledged", ""), "unacknowledged",
		credentialOperationTestOwner("gc-owner"))
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitPreparedCredentialOperation(
		begin.Active.Fence(), begin.Active.Expected, CredentialResultDone, now.Add(time.Minute),
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(credentialReceiptPostAckRetention + time.Minute)
	if deleted, err := s.DeleteExpiredCredentialOperationReceipts(1); err != nil || deleted != 0 {
		t.Fatalf("unacknowledged receipt was collected: deleted=%d err=%v", deleted, err)
	}
}

func TestPendingAddCredentialCompensationAdmissionAndCommit(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	request, mutation := pendingAddCompensationTestRequest(t, s, now)
	begin, err := s.BeginCredentialOperation(request)
	if err != nil || !begin.Created || begin.Active == nil {
		t.Fatalf("pending compensation begin = %+v err=%v", begin, err)
	}
	evidenceQuery := CredentialOperationEvidenceQuery{
		AccountID: request.AccountID, AccountInstanceID: request.AccountInstanceID,
		AccountGeneration: request.AccountGeneration, LocatorDigest: request.LocatorDigest,
		FileLocatorDigest: request.FileLocatorDigest, Kind: request.Kind, Target: request.Target,
		IntentDigest: request.IntentDigest,
	}
	activeEvidence, receiptEvidence, err := s.CredentialOperationEvidence(evidenceQuery)
	if err != nil || activeEvidence == nil || receiptEvidence != nil ||
		activeEvidence.OperationID != request.OperationID {
		t.Fatalf("active compensation evidence = %+v %+v err=%v", activeEvidence, receiptEvidence, err)
	}
	operation, err := s.MarkCredentialOperationApplying(
		begin.Active.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	empty := credentialOperationTestState("", "")
	operation, err = s.MarkCredentialOperationApplied(
		operation.Fence(), empty, CredentialTerminalSucceeded,
		CredentialResultDone, CredentialFailureNone, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitCredentialOperation(
		operation.Fence(), empty, nil, now.Add(10*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.AccountInstanceID != mutation.AccountInstanceID ||
		receipt.AccountGeneration != mutation.AccountGeneration ||
		receipt.Kind != CredentialOperationCompensate || receipt.Target != CredentialTargetAll {
		t.Fatalf("pending compensation receipt = %+v", receipt)
	}
	activeEvidence, receiptEvidence, err = s.CredentialOperationEvidence(evidenceQuery)
	if err != nil || activeEvidence != nil || receiptEvidence == nil ||
		receiptEvidence.OperationID != request.OperationID {
		t.Fatalf("receipt compensation evidence = %+v %+v err=%v", activeEvidence, receiptEvidence, err)
	}
	if err := s.AcknowledgeCredentialOperation(receipt.Token); err != nil {
		t.Fatal(err)
	}
	activeEvidence, receiptEvidence, err = s.CredentialOperationEvidence(evidenceQuery)
	if err != nil || activeEvidence != nil || receiptEvidence == nil ||
		receiptEvidence.AcknowledgedAt.IsZero() {
		t.Fatalf("acknowledged compensation evidence = %+v %+v err=%v", activeEvidence, receiptEvidence, err)
	}
}

func TestPendingAddCredentialCompensationRejectsCrossJournalMismatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, *BeginCredentialOperationRequest, AccountMutation)
	}{
		{
			name: "absent mutation",
			mutate: func(t *testing.T, s *Store, _ *BeginCredentialOperationRequest, mutation AccountMutation) {
				t.Helper()
				if _, err := s.db.Exec(
					`DELETE FROM account_mutations WHERE operation_id=?`, mutation.OperationID[:],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wrong mutation state",
			mutate: func(t *testing.T, s *Store, _ *BeginCredentialOperationRequest, mutation AccountMutation) {
				t.Helper()
				if _, err := s.db.Exec(
					`UPDATE account_mutations SET state='publishing' WHERE operation_id=?`,
					mutation.OperationID[:],
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "written digest",
			mutate: func(t *testing.T, _ *Store, request *BeginCredentialOperationRequest, _ AccountMutation) {
				t.Helper()
				request.Expected = credentialOperationTestState("different", "")
				rederiveCredentialOperationTestID(t, request)
			},
		},
		{
			name: "locator",
			mutate: func(t *testing.T, _ *Store, request *BeginCredentialOperationRequest, _ AccountMutation) {
				t.Helper()
				request.FileLocatorDigest = credentialOperationTestDigest("different-file-locator")
				rederiveCredentialOperationTestID(t, request)
			},
		},
		{
			name: "intent",
			mutate: func(t *testing.T, _ *Store, request *BeginCredentialOperationRequest, _ AccountMutation) {
				t.Helper()
				request.IntentDigest = credentialOperationTestDigest("different-intent")
				rederiveCredentialOperationTestID(t, request)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := openTest(t)
			now := time.Unix(1_900_000_000, 0)
			s.now = func() time.Time { return now }
			request, mutation := pendingAddCompensationTestRequest(t, s, now)
			test.mutate(t, s, &request, mutation)
			if _, err := s.BeginCredentialOperation(request); !errors.Is(err, ErrAccountRemoving) {
				t.Fatalf("mismatched pending compensation admitted: %v", err)
			}
		})
	}
}

func pendingAddCompensationTestRequest(
	t *testing.T,
	s *Store,
	now time.Time,
) (BeginCredentialOperationRequest, AccountMutation) {
	t.Helper()
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("add-owner"))
	if err != nil {
		t.Fatal(err)
	}
	configDir := "/tmp/pending-compensation"
	service := "pending-service"
	account := "pending-account"
	fileLocator := CredentialFileLocatorDigest(configDir)
	locator := credentialCompositeLocatorDigest(service, account, fileLocator)
	expectedState := credentialOperationTestState("", "")
	expectedDigest, err := expectedState.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutationRequest := BeginAccountMutationRequest{
		AccountID: reservation.ID, Kind: AccountMutationAdd,
		AccountInstanceID: reservation.InstanceID, AccountGeneration: reservation.Generation,
		LocatorDigest: locator, ExpectedCredentialDigest: expectedDigest,
		IntentDigest: credentialOperationTestDigest("add-intent"), ConfigDir: configDir,
		KeychainService: service, KeychainAccount: account,
		Owner: credentialOperationTestOwner("add-owner"),
	}
	mutationRequest.OperationID, err = NewAccountMutationID(
		mutationRequest.AccountID, mutationRequest.AccountInstanceID,
		mutationRequest.AccountGeneration, mutationRequest.Kind, mutationRequest.LocatorDigest,
		mutationRequest.ExpectedCredentialDigest, mutationRequest.IntentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := s.BeginAccountMutation(t.Context(), mutationRequest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := s.MarkAccountMutationInputProvided(
		begin.Active.Fence(), credentialOperationTestDigest("input"),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	writtenState := credentialOperationTestState("written", "")
	writtenDigest, err := writtenState.Digest()
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplied(fence, writtenDigest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginAccountRemoval(reservation.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitAccountMutation(
		fence, now.Add(10*time.Minute),
	); !errors.Is(err, ErrAccountMutationSuperseded) {
		t.Fatalf("prepare compensation: %v", err)
	}
	mutation, err := s.AccountMutation(mutationRequest.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	intent := credentialCompensationIntentDigest(writtenDigest)
	operationID, err := NewCredentialOperationID(
		reservation.InstanceID, reservation.Generation,
		CredentialOperationCompensate, CredentialTargetAll,
		locator, writtenState, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	return BeginCredentialOperationRequest{
		OperationID: operationID, AccountID: reservation.ID,
		AccountInstanceID: reservation.InstanceID, AccountGeneration: reservation.Generation,
		LocatorDigest: locator, FileLocatorDigest: fileLocator,
		Owner: credentialOperationTestOwner("compensation-owner"),
		Kind:  CredentialOperationCompensate, Target: CredentialTargetAll,
		IntentDigest: intent, Expected: writtenState,
	}, mutation
}

func rederiveCredentialOperationTestID(t *testing.T, request *BeginCredentialOperationRequest) {
	t.Helper()
	operationID, err := NewCredentialOperationID(
		request.AccountInstanceID, request.AccountGeneration, request.Kind, request.Target,
		request.LocatorDigest, request.Expected, request.IntentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.OperationID = operationID
}

func credentialOperationTestAccount(t *testing.T, s *Store) Account {
	return credentialOperationTestAccountID(t, s, 1)
}

func credentialOperationTestAccountID(t *testing.T, s *Store, id int) Account {
	t.Helper()
	if err := s.UpsertAccount(Account{
		ID: id, ConfigDir: fmt.Sprintf("/tmp/acct-%d", id),
		KeychainService: fmt.Sprintf("service-%d", id),
		KeychainAccount: fmt.Sprintf("account-%d", id),
		CreatedAt:       time.Unix(1_800_000_000, 0),
	}); err != nil {
		t.Fatal(err)
	}
	account, err := s.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}
	return account
}

func credentialOperationTestRequest(
	t *testing.T,
	account Account,
	kind CredentialOperationKind,
	target CredentialTarget,
	expected CredentialExternalState,
	intent string,
	owner proc.Record,
) BeginCredentialOperationRequest {
	t.Helper()
	fileLocator := CredentialFileLocatorDigest(account.ConfigDir)
	locator := CredentialLocatorDigest(
		account.KeychainService, account.KeychainAccount, account.ConfigDir,
	)
	intentDigest := credentialOperationTestDigest(intent)
	operationID, err := NewCredentialOperationID(
		account.InstanceID, account.Generation, kind, target, locator, expected, intentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	return BeginCredentialOperationRequest{
		OperationID: operationID, AccountID: account.ID,
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest: locator, FileLocatorDigest: fileLocator,
		Owner: owner, Kind: kind, Target: target,
		IntentDigest: intentDigest, Expected: expected,
	}
}

func credentialOperationTestState(keychain, file string) CredentialExternalState {
	return CredentialExternalState{
		Keychain: credentialOperationTestSlot(keychain),
		File:     credentialOperationTestSlot(file),
	}
}

func credentialOperationTestSlot(value string) CredentialSlotObservation {
	if value == "" {
		return CredentialSlotObservation{State: CredentialSlotEmpty}
	}
	digest := credentialOperationTestDigest(value)
	return CredentialSlotObservation{State: CredentialSlotPresent, Digest: &digest}
}

func credentialOperationTestDigest(value string) CredentialDigest {
	digest := sha256.Sum256([]byte(value))
	return CredentialDigest(digest)
}

func credentialOperationTestOwner(generation string) proc.Record {
	return proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           42, StartTime: "1.0", Boot: "test-boot", Comm: "cc-pool", Generation: generation,
	}
}

func credentialOperationTestRetirement(
	t *testing.T,
	owner proc.Record,
	newOwner proc.Record,
) (proc.ReapReceipt, *proc.Reaper) {
	t.Helper()
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	if err := store.Add(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginReap(t.Context(), owner, newOwner.Generation); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(
		t.Context(), owner, newOwner.Generation, proc.ReapAbsent,
	)
	if err != nil {
		t.Fatal(err)
	}
	return receipt, &proc.Reaper{Store: store, Generation: newOwner.Generation}
}

func storeDatabasePath(t *testing.T, s *Store) string {
	t.Helper()
	rows, err := s.db.Query(`PRAGMA database_list`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close database list: %v", err)
		}
	}()
	for rows.Next() {
		var sequence int
		var name, path string
		if err := rows.Scan(&sequence, &name, &path); err != nil {
			t.Fatal(err)
		}
		if name == "main" {
			return path
		}
	}
	t.Fatal("main SQLite database path not found")
	return ""
}

func openSecondStore(t *testing.T, first *Store) *Store {
	t.Helper()
	second, err := Open(storeDatabasePath(t, first))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	return second
}
