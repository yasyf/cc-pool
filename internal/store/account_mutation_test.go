package store

import (
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestAccountMutationIDDomainIsHardReset(t *testing.T) {
	if accountMutationIDDomain != "cc-pool:account-mutation:v1" {
		t.Fatalf("account mutation ID domain = %q", accountMutationIDDomain)
	}
}

func TestCredentialMutationsRejectLiveSession(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccount(t, s)
	if err := activateSelectionForTest(s, SelectionActivation{
		Token: nextStoreTestToken(), AccountID: account.ID,
		ExpectedInstanceID: account.InstanceID, ExpectedGeneration: account.Generation,
		Process:           ProcessIdentity{PID: 42, StartedAt: time.Now().Add(-time.Minute)},
		ConfigDir:         account.ConfigDir,
		FileProviderLease: storeTestLeaseReceipt("credential-mutation"),
	}); err != nil {
		t.Fatal(err)
	}
	credentialRequest := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetKeychain,
		credentialOperationTestState("before", ""), "live-session",
		credentialOperationTestOwner("live-session-credential"),
	)
	if _, err := s.BeginCredentialOperation(credentialRequest); !errors.Is(err, ErrAccountSessionActive) {
		t.Fatalf("credential operation with live session = %v", err)
	}
	mutationRequest := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin,
		credentialOperationTestOwner("live-session-account-mutation"),
	)
	if _, err := s.BeginAccountMutation(t.Context(), mutationRequest); !errors.Is(err, ErrAccountSessionActive) {
		t.Fatalf("account mutation with live session = %v", err)
	}
}

func TestAccountMutationAddPublishesExactReservationAndReplaysReceipt(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil || !begin.Created || begin.Active == nil || begin.Active.State != AccountMutationAwaitingPresentation {
		t.Fatalf("begin = %+v err=%v", begin, err)
	}
	operation := bindAccountMutationTestPresentation(t, s, *begin.Active)
	byKind, err := s.ActiveAccountMutationByKind(AccountMutationAdd)
	if err != nil || byKind.OperationID != operation.OperationID {
		t.Fatalf("active add lookup = %+v err=%v", byKind, err)
	}
	input := credentialOperationTestDigest("ephemeral-auth-input")
	fence, err := s.MarkAccountMutationInputProvided(operation.Fence(), input)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	written := credentialOperationTestDigest("written-credential")
	fence, err = s.MarkAccountMutationApplied(fence, written)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAccountMutationMetadata(fence, "verified-label", ""); !errors.Is(err, ErrAccountMutationState) {
		t.Fatalf("empty account UUID metadata = %v, want ErrAccountMutationState", err)
	}
	fence, err = s.SetAccountMutationMetadata(fence, "verified-label", "verified-uuid")
	if err != nil {
		t.Fatal(err)
	}
	attached, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil || attached.Active == nil || attached.Active.OwnerEpoch != fence.OwnerEpoch {
		t.Fatalf("retry after metadata update did not attach: %+v err=%v", attached, err)
	}
	fence, err = s.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitAccountMutation(fence, now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Terminal != AccountMutationCommitted || !receipt.HasInput ||
		receipt.InputDigest != input || !receipt.CredentialWritten ||
		receipt.WrittenCredentialDigest != written || receipt.AccountInstanceID != reservation.InstanceID ||
		receipt.AccountGeneration != reservation.Generation || receipt.Label != "verified-label" ||
		receipt.AccountUUID != "verified-uuid" || receipt.OutcomeDigest != written ||
		!receipt.HasPresentationIdentity || receipt.PresentationIdentity != operation.PresentationIdentity {
		t.Fatalf("receipt lost exact evidence: %+v", receipt)
	}
	if !receipt.PublicationPending {
		t.Fatal("committed add was admitted before credential publication settled")
	}
	byScope, err := s.UnacknowledgedAccountMutationReceipt(AccountMutationAdd, 0)
	if err != nil || !reflect.DeepEqual(byScope, receipt) {
		t.Fatalf("add scope receipt = %+v err=%v", byScope, err)
	}
	account, err := s.GetAccount(reservation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if account.InstanceID != reservation.InstanceID || account.Generation != reservation.Generation ||
		account.ConfigDir != operation.ConfigDir || account.KeychainService != operation.KeychainService ||
		account.KeychainAccount != operation.KeychainAccount {
		t.Fatalf("published account = %+v", account)
	}
	presentation, err := s.AccountPresentation(account.ID)
	if err != nil || presentation.AccountInstanceID != account.InstanceID ||
		presentation.AccountGeneration != account.Generation ||
		presentation.Identity != operation.PresentationIdentity {
		t.Fatalf("published presentation = %+v err=%v", presentation, err)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("publication-pending active accounts = %+v err=%v", active, err)
	}
	if err := s.AcknowledgeAccountMutationReceipt(receipt.OperationID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("publication-pending receipt acknowledgement = %v", err)
	}
	if err := s.MarkAccountMutationPublicationSettled(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	receipt, err = s.AccountMutationReceipt(receipt.OperationID)
	if err != nil || receipt.PublicationPending {
		t.Fatalf("settled publication receipt = %+v err=%v", receipt, err)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 1 || active[0].ID != account.ID {
		t.Fatalf("settled active accounts = %+v err=%v", active, err)
	}
	replay, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil || replay.Receipt == nil || replay.Active != nil ||
		!reflect.DeepEqual(*replay.Receipt, receipt) {
		t.Fatalf("receipt replay = %+v err=%v", replay, err)
	}
	if err := s.DeleteAccount(account.ID); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
		t.Fatalf("unacknowledged account receipt did not fence deletion: %v", err)
	}
	if err := s.AcknowledgeAccountMutationReceipt(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	acknowledged, err := s.AccountMutationReceipt(receipt.OperationID)
	if err != nil || acknowledged.AcknowledgedAt.IsZero() {
		t.Fatalf("acknowledged receipt = %+v err=%v", acknowledged, err)
	}
	replay, err = s.BeginAccountMutation(t.Context(), request)
	if err != nil || replay.Receipt == nil {
		t.Fatalf("acknowledged receipt was not retained: %+v err=%v", replay, err)
	}
	if deleted, err := s.DeleteExpiredAccountMutationReceipts(1); err != nil || deleted != 0 {
		t.Fatalf("receipt collected inside post-ack window: deleted=%d err=%v", deleted, err)
	}
	now = now.Add(credentialReceiptPostAckRetention + time.Minute)
	if deleted, err := s.DeleteExpiredAccountMutationReceipts(1); err != nil || deleted != 1 {
		t.Fatalf("receipt not collected after post-ack window: deleted=%d err=%v", deleted, err)
	}
}

func TestAccountMutationAddCommitRejectsEmptyAccountUUID(t *testing.T) {
	s := openTest(t)
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	request.AccountUUID = ""
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	mutation := bindAccountMutationTestPresentation(t, s, *begin.Active)
	fence, err := s.MarkAccountMutationInputProvided(
		mutation.Fence(), credentialOperationTestDigest("input"),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplied(fence, credentialOperationTestDigest("written"))
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitAccountMutation(fence, time.Now().Add(time.Hour)); !errors.Is(err, ErrAccountMutationState) {
		t.Fatalf("empty account UUID committed: %v", err)
	}
	if _, err := s.GetAccount(reservation.ID); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("empty-UUID add published account: %v", err)
	}
}

func TestBindAccountMutationPresentationRejectsSubstitutedTenant(t *testing.T) {
	s := openTest(t)
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	publicPath := "/File Provider/CCPool/account"
	proof := presentationTestProof(Account{
		InstanceID: reservation.InstanceID, Generation: reservation.Generation,
	}, publicPath, "activation-test")
	proof.TenantID = "account-0123456789abcdef0123456789abcdef"
	_, err = s.BindAccountMutationPresentation(
		begin.Active.Fence(), proof, publicPath, "service", "account",
		credentialOperationTestDigest("locator"), credentialOperationTestDigest("expected"),
	)
	if !errors.Is(err, ErrAccountPresentationEvidence) {
		t.Fatalf("substituted tenant bind = %v", err)
	}
	active, err := s.AccountMutation(request.OperationID)
	if err != nil || active.State != AccountMutationAwaitingPresentation || active.HasPresentationIdentity {
		t.Fatalf("mutation changed after rejected proof = %+v err=%v", active, err)
	}
}

func TestCancelUnboundAccountMutationReleasesExactReservation(t *testing.T) {
	s := openTest(t)
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil || begin.Active == nil || begin.Active.State != AccountMutationAwaitingPresentation {
		t.Fatalf("begin = %+v err=%v", begin, err)
	}
	if err := s.CancelUnboundAccountMutation(begin.Active.Fence()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AccountMutation(request.OperationID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("canceled mutation remained active: %v", err)
	}
	replacement, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID != reservation.ID || replacement.InstanceID == reservation.InstanceID {
		t.Fatalf("reservation not released exactly: old=%+v replacement=%+v", reservation, replacement)
	}
	if err := s.CancelUnboundAccountMutation(begin.Active.Fence()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale cancel = %v, want missing mutation", err)
	}
}

func TestAccountMutationExactPendingFenceRejectsStaleReservation(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	stale, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, stale, AccountMutationAdd)
	if err := s.ReleaseAccountIndex(stale); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginAccountMutation(t.Context(), request); !errors.Is(err, ErrAccountGenerationChanged) {
		t.Fatalf("released add reservation admitted: %v", err)
	}
	current, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	if current.ID != stale.ID || current.InstanceID == stale.InstanceID {
		t.Fatalf("reservation reuse = old %+v current %+v", stale, current)
	}
	if _, err := s.BeginAccountMutation(t.Context(), request); !errors.Is(err, ErrAccountGenerationChanged) {
		t.Fatalf("stale add reservation admitted: %v", err)
	}
}

func TestAccountMutationReloginPublishesVerifiedMetadata(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccountID(t, s, 10)
	request := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin,
		credentialOperationTestOwner("relogin-owner"),
	)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := s.MarkAccountMutationInputProvided(
		begin.Active.Fence(), credentialOperationTestDigest("relogin-input"),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplied(
		fence, credentialOperationTestDigest("relogin-written"),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.SetAccountMutationMetadata(fence, "new-label", "new-account-uuid")
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitAccountMutation(fence, now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.GetAccount(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.InstanceID != account.InstanceID || updated.Generation != account.Generation ||
		updated.Label != "new-label" || updated.AccountUUID != "new-account-uuid" {
		t.Fatalf("published relogin metadata = %+v", updated)
	}
	if receipt.Label != updated.Label || receipt.AccountUUID != updated.AccountUUID {
		t.Fatalf("relogin receipt metadata = %+v account=%+v", receipt, updated)
	}
}

func TestRearmAccountMutationInputIsFencedAndLostResponseIdempotent(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	operation := bindAccountMutationTestPresentation(t, s, *begin.Active)
	firstInput := credentialOperationTestDigest("first-input")
	fence, err := s.MarkAccountMutationInputProvided(operation.Fence(), firstInput)
	if err != nil {
		t.Fatal(err)
	}
	applying, err := s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	rearmed, err := s.RearmAccountMutationInput(applying, operation.ExpectedCredentialDigest)
	if err != nil {
		t.Fatal(err)
	}
	if rearmed.OwnerEpoch != applying.OwnerEpoch+1 {
		t.Fatalf("rearmed fence = %+v applying=%+v", rearmed, applying)
	}
	mutation, err := s.AccountMutation(request.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if mutation.State != AccountMutationAwaitingInput || !mutation.HasInput ||
		mutation.InputDigest != firstInput || !reflect.DeepEqual(mutation.Fence(), rearmed) {
		t.Fatalf("rearmed mutation = %+v", mutation)
	}
	replay, err := s.RearmAccountMutationInput(applying, operation.ExpectedCredentialDigest)
	if err != nil || !reflect.DeepEqual(replay, rearmed) {
		t.Fatalf("lost response replay = %+v err=%v want=%+v", replay, err, rearmed)
	}
	if _, err := s.RearmAccountMutationInput(rearmed, operation.ExpectedCredentialDigest); !errors.Is(err, ErrAccountMutationState) {
		t.Fatalf("awaiting-input rearm repeated with current fence: %v", err)
	}
	secondInput := credentialOperationTestDigest("second-input")
	if _, err := s.MarkAccountMutationInputProvided(rearmed, secondInput); err != nil {
		t.Fatal(err)
	}
	mutation, err = s.AccountMutation(request.OperationID)
	if err != nil || mutation.InputDigest != secondInput {
		t.Fatalf("replacement input digest = %+v err=%v", mutation, err)
	}
}

func TestRearmAccountMutationInputRejectsDriftAndNonInteractiveKind(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccountID(t, s, 10)
	relogin := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin,
		credentialOperationTestOwner("rearm-relogin"),
	)
	begin, err := s.BeginAccountMutation(t.Context(), relogin)
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
	if _, err := s.RearmAccountMutationInput(
		fence, credentialOperationTestDigest("drifted"),
	); !errors.Is(err, ErrAccountMutationRecoveryRequired) {
		t.Fatalf("drifted rearm = %v", err)
	}
	stale := fence
	stale.OwnerEpoch--
	if _, err := s.RearmAccountMutationInput(
		stale, begin.Active.ExpectedCredentialDigest,
	); !errors.Is(err, ErrAccountMutationFence) {
		t.Fatalf("stale rearm = %v", err)
	}
}

func TestAccountMutationCancellationIsPreBoundaryOnly(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	operation := bindAccountMutationTestPresentation(t, s, *begin.Active)
	receipt, err := s.ResolveAccountMutation(
		operation.Fence(), AccountMutationAborted,
		operation.ExpectedCredentialDigest, nil, now.Add(10*time.Minute),
	)
	if err != nil || receipt.Terminal != AccountMutationAborted || receipt.CredentialWritten {
		t.Fatalf("awaiting-input abort = %+v err=%v", receipt, err)
	}
	if _, err := s.AccountMutation(request.OperationID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("aborted lane remained active: %v", err)
	}
	replacement, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID != reservation.ID || replacement.InstanceID == reservation.InstanceID {
		t.Fatalf("aborted reservation was not released exactly: old=%+v replacement=%+v", reservation, replacement)
	}
	if _, err := s.ActiveAccountMutationByKind(AccountMutationRelogin); !errors.Is(err, ErrAccountMutationState) {
		t.Fatalf("non-global kind lookup succeeded: %v", err)
	}

	account := credentialOperationTestAccountID(t, s, 10)
	existing := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin,
		credentialOperationTestOwner("relogin-owner"),
	)
	started, err := s.BeginAccountMutation(t.Context(), existing)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := s.MarkAccountMutationInputProvided(
		started.Active.Fence(), started.Active.ExpectedCredentialDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveAccountMutation(
		fence, AccountMutationAborted,
		started.Active.ExpectedCredentialDigest, nil, now.Add(10*time.Minute),
	); !errors.Is(err, ErrAccountMutationState) {
		t.Fatalf("post-boundary abort = %v", err)
	}
}

func TestAccountMutationConcurrentSameIDCoalesces(t *testing.T) {
	first := openTest(t)
	second := openSecondStore(t, first)
	now := time.Unix(1_900_000_000, 0)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }
	reservation, err := first.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	results := make(chan BeginAccountMutationResult, 2)
	errorsOut := make(chan error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for _, candidate := range []*Store{first, second} {
		wait.Add(1)
		go func(candidate *Store) {
			defer wait.Done()
			<-start
			result, err := candidate.BeginAccountMutation(t.Context(), request)
			results <- result
			errorsOut <- err
		}(candidate)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatalf("concurrent begin: %v", err)
		}
	}
	created := 0
	for result := range results {
		if result.Active == nil || result.Active.OperationID != request.OperationID {
			t.Fatalf("coalesced result = %+v", result)
		}
		if result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created results = %d, want 1", created)
	}
}

func TestAccountMutationSecondAddAttachesAndReleasesLosingReservation(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	firstReservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := accountMutationTestRequest(
		t, firstReservation, AccountMutationAdd,
	)
	first, err := s.BeginAccountMutation(t.Context(), firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondReservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := accountMutationTestRequest(
		t, secondReservation, AccountMutationAdd,
	)
	attached, err := s.BeginAccountMutation(t.Context(), secondRequest)
	if !errors.Is(err, ErrAccountMutationBusy) || attached.Active == nil ||
		attached.Active.OperationID != first.Active.OperationID {
		t.Fatalf("second add = %+v err=%v", attached, err)
	}
	pending, err := s.PendingAddIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pending, []int{firstReservation.ID}) {
		t.Fatalf("pending reservations = %v", pending)
	}
}

func TestAccountRemovalWinsPublishSequenceAndForcesCompensation(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	request := accountMutationTestRequest(t, reservation, AccountMutationAdd)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	operation := bindAccountMutationTestPresentation(t, s, *begin.Active)
	fence, err := s.MarkAccountMutationInputProvided(
		operation.Fence(), credentialOperationTestDigest("input"),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplied(fence, credentialOperationTestDigest("written"))
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	removal, err := s.BeginAccountRemoval(reservation.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if removal.RegistrySequence <= begin.Active.RegistrySequence ||
		removal.AccountInstanceID != reservation.InstanceID {
		t.Fatalf("removal ordering = %+v mutation=%+v", removal, begin.Active)
	}
	reattached, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil || reattached.Active == nil ||
		reattached.Active.OperationID != request.OperationID {
		t.Fatalf("exact retry did not attach after removal fence: %+v err=%v", reattached, err)
	}
	if _, err := s.CommitAccountMutation(fence, now.Add(10*time.Minute)); !errors.Is(err, ErrAccountMutationSuperseded) {
		t.Fatalf("publish did not lose to removal: %v", err)
	}
	replayedRemoval, err := s.BeginAccountRemoval(reservation.ID, true)
	if err != nil || replayedRemoval != removal {
		t.Fatalf("compensating removal replay = %+v err=%v, want %+v", replayedRemoval, err, removal)
	}
	compensating, err := s.AccountMutation(request.OperationID)
	if err != nil || compensating.State != AccountMutationCompensating ||
		compensating.OwnerEpoch != fence.OwnerEpoch+1 {
		t.Fatalf("compensating mutation = %+v err=%v", compensating, err)
	}
	receipt, err := s.ResolveAccountMutation(
		compensating.Fence(), AccountMutationSuperseded,
		compensating.ExpectedCredentialDigest, nil, now.Add(10*time.Minute),
	)
	if err != nil || receipt.Terminal != AccountMutationSuperseded ||
		receipt.WrittenCredentialDigest != compensating.WrittenCredentialDigest {
		t.Fatalf("superseded receipt = %+v err=%v", receipt, err)
	}
	if _, err := s.GetAccount(reservation.ID); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("losing add published account: %v", err)
	}
	if _, err := accountRemovalByID(s.db, reservation.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("pending-only removal survived compensation: %v", err)
	}
	replacement, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID != reservation.ID || replacement.InstanceID == reservation.InstanceID {
		t.Fatalf("superseded reservation was not released exactly: old=%+v replacement=%+v", reservation, replacement)
	}
	replay, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil || replay.Receipt == nil || replay.Active != nil ||
		replay.Receipt.Terminal != AccountMutationSuperseded {
		t.Fatalf("mutation did not replay removal-winner receipt: %+v err=%v", replay, err)
	}
	request.IntentDigest = credentialOperationTestDigest("new-intent")
	request.OperationID, err = NewPendingAddMutationID(
		request.AccountID, request.AccountInstanceID, request.AccountGeneration,
		request.IntentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginAccountMutation(t.Context(), request); !errors.Is(err, ErrAccountGenerationChanged) {
		t.Fatalf("stale mutation restarted after removal completed: %v", err)
	}
}

func TestQuarantinedAddSurvivesAckRestartAndResolvesExactly(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	_, mutation := pendingAddCompensationTestRequest(t, s, now)
	quarantinedState := credentialOperationTestState("drifted", "")
	quarantinedDigest, err := quarantinedState.Digest()
	if err != nil {
		t.Fatal(err)
	}
	quarantineRequest := AccountMutationQuarantine{
		Observation: quarantinedState,
		Reason:      CredentialResultChangedUnderfoot,
	}
	receipt, err := s.ResolveAccountMutation(
		mutation.Fence(), AccountMutationQuarantined, quarantinedDigest,
		&quarantineRequest, now.Add(10*time.Minute),
	)
	if err != nil || !receipt.HasQuarantine ||
		receipt.QuarantineReason != CredentialResultChangedUnderfoot {
		t.Fatalf("quarantined account receipt = %+v err=%v", receipt, err)
	}
	quarantine, err := s.CredentialQuarantine(mutation.AccountID)
	if err != nil {
		t.Fatal(err)
	}
	blocked := BeginAccountMutationRequest{
		AccountID: mutation.AccountID, Kind: AccountMutationAdd,
		AccountInstanceID: mutation.AccountInstanceID, AccountGeneration: mutation.AccountGeneration,
		IntentDigest: credentialOperationTestDigest("blocked-after-quarantine"),
		Owner:        mutation.Owner,
	}
	blocked.OperationID, err = NewPendingAddMutationID(
		blocked.AccountID, blocked.AccountInstanceID, blocked.AccountGeneration,
		blocked.IntentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginAccountMutation(t.Context(), blocked); !errors.Is(err, ErrAccountMutationRecoveryRequired) {
		t.Fatalf("quarantined add admitted new mutation: %v", err)
	}
	unrelated, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	if unrelated.ID == mutation.AccountID {
		t.Fatalf("quarantined pending id was reused: %+v", unrelated)
	}
	if err := s.AcknowledgeAccountMutationReceipt(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(credentialReceiptPostAckRetention + 11*time.Minute)
	if deleted, err := s.DeleteExpiredAccountMutationReceipts(1); err != nil || deleted != 0 {
		t.Fatalf("quarantined receipt collected: deleted=%d err=%v", deleted, err)
	}
	path := storeDatabasePath(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	if err := s.ClearCredentialQuarantine(quarantine); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
		t.Fatalf("general clear bypassed pending add evidence: %v", err)
	}
	expectedState := credentialOperationTestState("", "")
	resolution := ResolveQuarantinedAddRequest{
		OperationID: receipt.OperationID, Quarantine: quarantine, Observed: expectedState,
	}
	if err := s.ResolveQuarantinedAdd(resolution); err != nil {
		t.Fatal(err)
	}
	if err := s.ResolveQuarantinedAdd(resolution); err != nil {
		t.Fatalf("lost recovery response did not replay: %v", err)
	}
	resolved, err := s.AccountMutationReceipt(receipt.OperationID)
	if err != nil || resolved.Resolution != AccountMutationCompensatedRelease ||
		resolved.ResolutionObservedDigest != mutation.ExpectedCredentialDigest || resolved.ResolvedAt.IsZero() {
		t.Fatalf("resolved account receipt = %+v err=%v", resolved, err)
	}
	if _, err := s.CredentialQuarantine(mutation.AccountID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantine survived resolution: %v", err)
	}
	if _, err := accountRemovalByID(s.db, mutation.AccountID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("removal survived resolution: %v", err)
	}
	pending, err := s.PendingAddIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pending, []int{unrelated.ID}) {
		t.Fatalf("pending ids after resolution = %v", pending)
	}
	reused, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
	if err != nil {
		t.Fatal(err)
	}
	if reused.ID != mutation.AccountID || reused.InstanceID == mutation.AccountInstanceID {
		t.Fatalf("resolved id not safely reusable: old=%+v reused=%+v", mutation, reused)
	}
	if deleted, err := s.DeleteExpiredAccountMutationReceipts(1); err != nil || deleted != 0 {
		t.Fatalf("resolution receipt collected inside replay window: deleted=%d err=%v", deleted, err)
	}
	now = now.Add(credentialReceiptPostAckRetention + time.Minute)
	if deleted, err := s.DeleteExpiredAccountMutationReceipts(1); err != nil || deleted != 1 {
		t.Fatalf("resolved receipt not collected after replay window: deleted=%d err=%v", deleted, err)
	}
}

func TestAccountMutationTakeoverFencesExactOwnerEpoch(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	owner := credentialOperationTestOwner("registry-owner")
	request := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin, owner,
	)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	newOwner := credentialOperationTestOwner("registry-recovery")
	wrongFence := begin.Active.Fence()
	wrongFence.Owner = newOwner
	if _, err := s.TakeoverAccountMutation(
		t.Context(), wrongFence, newOwner,
	); !errors.Is(err, ErrAccountMutationFence) {
		t.Fatalf("takeover without the row's exact owner bytes = %v", err)
	}
	taken, err := s.TakeoverAccountMutation(t.Context(), begin.Active.Fence(), newOwner)
	if err != nil || taken.OwnerEpoch != begin.Active.OwnerEpoch+1 ||
		!bytes.Equal(taken.Owner, newOwner) {
		t.Fatalf("immediate takeover = %+v err=%v", taken, err)
	}
	if _, err := s.MarkAccountMutationApplying(begin.Active.Fence()); !errors.Is(err, ErrAccountMutationFence) {
		t.Fatalf("stale owner transition = %v", err)
	}
}

func TestAccountMutationAgeAloneCannotTakeover(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	account := credentialOperationTestAccount(t, s)
	owner := credentialOperationTestOwner("registry-expired-owner")
	request := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin, owner,
	)
	begin, err := s.BeginAccountMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	stale := begin.Active.Fence()
	if _, err := s.TakeoverAccountMutation(
		t.Context(), stale, credentialOperationTestOwner("registry-new-owner"),
	); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, err := s.TakeoverAccountMutation(
		t.Context(), stale, credentialOperationTestOwner("registry-late-owner"),
	); !errors.Is(err, ErrAccountMutationFence) {
		t.Fatalf("aged stale-fence takeover = %v", err)
	}
}

func TestAccountMutationsOwnedByPagesExactOwner(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	owner := credentialOperationTestOwner("registry-page-owner")
	other := credentialOperationTestOwner("registry-page-other")
	for id := 1; id <= 4; id++ {
		account := credentialOperationTestAccountID(t, s, id)
		requestOwner := owner
		if id == 3 {
			requestOwner = other
		}
		request := existingAccountMutationTestRequest(
			t, account, AccountMutationRelogin, requestOwner,
		)
		if _, err := s.BeginAccountMutation(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	page, more, err := s.AccountMutationsOwnedBy(owner, 0, 2)
	if err != nil || !more || len(page) != 2 || page[0].AccountID != 1 || page[1].AccountID != 2 {
		t.Fatalf("first owner page = %+v more=%v err=%v", page, more, err)
	}
	page, more, err = s.AccountMutationsOwnedBy(owner, page[len(page)-1].AccountID, 2)
	if err != nil || more || len(page) != 1 || page[0].AccountID != 4 {
		t.Fatalf("second owner page = %+v more=%v err=%v", page, more, err)
	}
	if _, _, err := s.AccountMutationsOwnedBy(owner, 0, 0); !errors.Is(err, ErrAccountMutationState) {
		t.Fatalf("invalid owner page admitted: %v", err)
	}
}

func accountMutationTestRequest(
	t *testing.T,
	reservation PendingAccountReservation,
	kind AccountMutationKind,
) BeginAccountMutationRequest {
	t.Helper()
	request := BeginAccountMutationRequest{
		AccountID: reservation.ID, Kind: kind,
		AccountInstanceID: reservation.InstanceID, AccountGeneration: reservation.Generation,
		IntentDigest: credentialOperationTestDigest("intent"),
		Label:        "label", AccountUUID: "uuid",
		Owner: credentialOperationTestOwner("registry-owner"),
	}
	operationID, err := NewPendingAddMutationID(
		request.AccountID, request.AccountInstanceID, request.AccountGeneration,
		request.IntentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.OperationID = operationID
	return request
}

func bindAccountMutationTestPresentation(
	t *testing.T,
	s *Store,
	mutation AccountMutation,
) AccountMutation {
	t.Helper()
	_, err := s.BindAccountMutationPresentation(
		mutation.Fence(), presentationTestProof(Account{
			InstanceID: mutation.AccountInstanceID, Generation: mutation.AccountGeneration,
		}, "/File Provider/CCPool/account", "activation-test"),
		"/File Provider/CCPool/account", "service", "account",
		credentialOperationTestDigest("locator"), credentialOperationTestDigest("expected"),
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := s.AccountMutation(mutation.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func existingAccountMutationTestRequest(
	t *testing.T,
	account Account,
	kind AccountMutationKind,
	owner OwnerRecord,
) BeginAccountMutationRequest {
	t.Helper()
	request := BeginAccountMutationRequest{
		AccountID: account.ID, Kind: kind,
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest:            credentialOperationTestDigest("existing-locator"),
		ExpectedCredentialDigest: credentialOperationTestDigest("existing-expected"),
		IntentDigest:             credentialOperationTestDigest("existing-intent"),
		ConfigDir:                account.ConfigDir, KeychainService: account.KeychainService,
		KeychainAccount: account.KeychainAccount, Label: account.Label,
		AccountUUID: account.AccountUUID, Owner: owner,
	}
	operationID, err := NewAccountMutationID(
		request.AccountID, request.AccountInstanceID, request.AccountGeneration,
		request.Kind, request.LocatorDigest, request.ExpectedCredentialDigest, request.IntentDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.OperationID = operationID
	return request
}
