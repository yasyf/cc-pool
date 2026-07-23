package pool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

func TestRecoverRetiredCredentialOwnersCancellationReentryJoinsWedgedGeneration(t *testing.T) {
	manager := &Manager{}
	oldCtx, oldCancel := context.WithCancel(context.Background())
	oldDone := make(chan struct{})
	releasedOld := false
	defer func() {
		if !releasedOld {
			close(oldDone)
		}
	}()
	manager.recoveryCancel = oldCancel
	manager.recoveryDone = oldDone

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- manager.RecoverRetiredCredentialOwners(t.Context())
	}()
	select {
	case <-oldCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("first recovery did not cancel the wedged generation")
	}

	manager.recoveryMu.Lock()
	firstDone := manager.recoveryDone
	manager.recoveryMu.Unlock()
	if firstDone == nil || firstDone == oldDone {
		t.Fatal("first recovery generation was not published")
	}

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- manager.RecoverRetiredCredentialOwners(t.Context())
	}()
	deadline := time.Now().Add(time.Second)
	var secondDone chan struct{}
	for time.Now().Before(deadline) {
		manager.recoveryMu.Lock()
		secondDone = manager.recoveryDone
		manager.recoveryMu.Unlock()
		if secondDone != nil && secondDone != firstDone {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if secondDone == nil || secondDone == firstDone {
		t.Fatal("replacement recovery generation was not published")
	}

	lockAcquired := make(chan struct{})
	go func() {
		manager.recoveryMu.Lock()
		close(lockAcquired)
		manager.recoveryMu.Unlock()
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("recovery mutex remained held while joining the wedged generation")
	}
	for name, result := range map[string]<-chan error{
		"first": firstResult, "second": secondResult,
	} {
		select {
		case err := <-result:
			t.Fatalf("%s recovery returned before the predecessor joined: %v", name, err)
		default:
		}
	}

	close(oldDone)
	releasedOld = true
	for name, result := range map[string]<-chan error{
		"first": firstResult, "second": secondResult,
	} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s recovery: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s recovery did not finish after the predecessor joined", name)
		}
	}
	select {
	case <-firstDone:
	default:
		t.Fatal("superseded recovery generation did not finish")
	}
	select {
	case <-secondDone:
	default:
		t.Fatal("active recovery generation did not finish")
	}
	manager.recoveryMu.Lock()
	defer manager.recoveryMu.Unlock()
	if manager.recoveryCancel != nil || manager.recoveryDone != nil {
		t.Fatal("finished recovery generation remained published")
	}
}

func TestCredentialOperationTwoManagersJoinAndReplayImmutableReceipt(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-join", KeychainAccount: "account-join",
	})
	credentials := credstest.NewFake()
	first := credentialRecoveryManager(t, st, credentials, "join-first")
	second := credentialRecoveryManager(t, st, credentials, "join-second")

	started := make(chan struct{})
	release := make(chan struct{})
	type outcome struct {
		result struct{}
		err    error
	}
	firstDone := make(chan outcome, 1)
	secondDone := make(chan outcome, 1)
	var executions atomic.Int32
	go func() {
		result, err := runCredentialOperation(
			context.Background(), first, account, store.CredentialOperationAdoptRotated,
			unitCredentialOperationCodec(
				store.CredentialTargetKeychain,
			),
			func(context.Context, *credentialOperationBoundary) (struct{}, error) {
				executions.Add(1)
				close(started)
				<-release
				return struct{}{}, nil
			},
			"same-request",
		)
		firstDone <- outcome{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first manager did not enter the credential operation")
	}
	operation, err := st.CredentialOperation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		result, err := runCredentialOperation(
			context.Background(), second, account, store.CredentialOperationAdoptRotated,
			unitCredentialOperationCodec(
				store.CredentialTargetKeychain,
			),
			func(context.Context, *credentialOperationBoundary) (struct{}, error) {
				executions.Add(1)
				return struct{}{}, nil
			},
			"same-request",
		)
		secondDone <- outcome{result: result, err: err}
	}()

	select {
	case got := <-secondDone:
		t.Fatalf("joining manager returned before the owner settled: %v", got.err)
	case <-time.After(75 * time.Millisecond):
	}
	close(release)
	for name, done := range map[string]<-chan outcome{"owner": firstDone, "joiner": secondDone} {
		select {
		case got := <-done:
			if got.err != nil {
				t.Fatalf("%s result: %v", name, got.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not receive the settled result", name)
		}
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("operation executions = %d, want 1", got)
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded || receipt.Result != store.CredentialResultDone {
		t.Fatalf("receipt = %+v", receipt)
	}
	_, err = st.ResolveCredentialOperation(
		operation.Fence(),
		receipt.Outcome,
		store.CredentialTerminalFailed,
		store.CredentialResultFailed,
		store.CredentialFailureInternal,
		nil,
		time.Now().Add(time.Minute),
	)
	if !errors.Is(err, store.ErrCredentialOperationOwner) {
		t.Fatalf("receipt mutation err=%v, want stale recovery fence rejected", err)
	}
	unchanged, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil || !reflect.DeepEqual(unchanged, receipt) {
		t.Fatalf("receipt after mutation = %+v err=%v, want immutable %+v", unchanged, err, receipt)
	}
}

func TestCredentialOperationRetryAfterLostResponseReplaysReceipt(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-lost-response", KeychainAccount: "account-lost-response",
	})
	credentials := credstest.NewFake()
	first := credentialRecoveryManager(t, st, credentials, "lost-response-first")
	second := credentialRecoveryManager(t, st, credentials, "lost-response-second")
	var executions atomic.Int32
	kind := store.CredentialOperationAdoptRotated
	target := store.CredentialTargetKeychain
	intent := credentialIntentDigest(kind, "same-request")
	expected, err := first.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	filePath := creds.FileCredentialPath(account.ConfigDir)
	locator := store.CredentialLocatorDigest(
		account.KeychainService, account.KeychainAccount, filePath,
	)
	operationID, err := store.NewCredentialOperationID(
		account.InstanceID, account.Generation, kind, target, locator, expected, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	codec := unitCredentialOperationCodec(target)
	apply := func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
		executions.Add(1)
		if err := boundary.Cross(ctx); err != nil {
			return struct{}{}, err
		}
		credentials.Put(
			account.KeychainService,
			account.KeychainAccount,
			datedCred("installed-after-boundary", time.Hour),
		)
		return struct{}{}, nil
	}
	if _, err := executeCredentialOperation(
		t.Context(),
		first,
		account,
		kind,
		operationID,
		locator,
		store.CredentialFileLocatorDigest(filePath),
		intent,
		expected,
		codec,
		first.credentialMutationObservation,
		apply,
	); err != nil {
		t.Fatal(err)
	}
	// Model a reply lost after the first process durably committed: the second
	// manager has the exact semantic request, not the first process's random token.
	if _, err := executeCredentialOperation(
		t.Context(),
		second,
		account,
		kind,
		operationID,
		locator,
		store.CredentialFileLocatorDigest(filePath),
		intent,
		expected,
		codec,
		second.credentialMutationObservation,
		apply,
	); err != nil {
		t.Fatal(err)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("operation executions after lost response = %d, want 1", got)
	}
	receipt, err := st.CredentialOperationReceiptByID(operationID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.AcknowledgedAt.IsZero() {
		t.Fatal("delivered credential receipt remained unacknowledged")
	}
	if err := st.DeleteAccount(account.ID); err != nil {
		t.Fatalf("acknowledged receipt blocked account deletion: %v", err)
	}
	if _, err := executeCredentialOperation(
		t.Context(),
		second,
		account,
		kind,
		operationID,
		locator,
		store.CredentialFileLocatorDigest(filePath),
		intent,
		expected,
		codec,
		second.credentialMutationObservation,
		apply,
	); err != nil {
		t.Fatalf("post-delete receipt replay: %v", err)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("post-delete receipt replay executions = %d, want 1", got)
	}
}

func TestCredentialOperationLiveOwnerReopenOnlyJoins(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-live", KeychainAccount: "account-live",
	})
	credentials := credstest.NewFake()
	owner := credentialRecoveryManager(t, st, credentials, "live-owner")
	reopened := credentialRecoveryManager(t, st, credentials, "reopened-manager")
	before, err := owner.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	kind := store.CredentialOperationAdoptRotated
	target := store.CredentialTargetKeychain
	intent := credentialIntentDigest(kind, "same-request")
	operation := beginCredentialOperation(
		t, owner, account, kind, target, intent, before,
	)
	operation, err = st.MarkCredentialOperationApplying(
		operation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Millisecond)
	defer cancel()
	var executed atomic.Bool
	_, err = runCredentialOperation(
		ctx, reopened, account, kind, unitCredentialOperationCodec(target),
		func(context.Context, *credentialOperationBoundary) (struct{}, error) {
			executed.Store(true)
			return struct{}{}, nil
		},
		"same-request",
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reopened manager join = %v, want deadline", err)
	}
	if executed.Load() {
		t.Fatal("reopened manager re-executed a live owner's operation")
	}
	current, err := st.CredentialOperationByToken(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != store.CredentialOperationApplying || current.Owner != owner.workers.owner {
		t.Fatalf("live operation mutated = %+v", current)
	}
	if _, err := st.CredentialOperationReceipt(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("live operation unexpectedly settled: %v", err)
	}
}

func TestCredentialOperationExpiredLiveOwnerIsNeverTakenOverWithoutReceipt(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-stale-owner", KeychainAccount: "account-stale-owner",
	})
	credentials := credstest.NewFake()
	owner := credentialRecoveryManager(t, st, credentials, "stale-owner")
	recovery := credentialRecoveryManager(t, st, credentials, "recovery-owner")
	before, err := owner.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	kind := store.CredentialOperationAdoptRotated
	target := store.CredentialTargetKeychain
	operation := beginRetiredCredentialOperation(
		t,
		owner,
		account,
		kind,
		target,
		credentialIntentDigest(kind, "stale-request"),
		before,
	)
	var executions atomic.Int32
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Millisecond)
	defer cancel()
	_, err = runCredentialOperation(
		ctx,
		recovery,
		account,
		kind,
		unitCredentialOperationCodec(target),
		func(context.Context, *credentialOperationBoundary) (struct{}, error) {
			executions.Add(1)
			return struct{}{}, nil
		},
		"stale-request",
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired live-owner wait = %v, want deadline", err)
	}
	if executions.Load() != 0 {
		t.Fatal("stale-owner recovery re-executed the external operation")
	}
	current, err := st.CredentialOperationByToken(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if current.OwnerEpoch != operation.OwnerEpoch || !reflect.DeepEqual(current.Owner, owner.workers.owner) {
		t.Fatalf("expired live-owner lane changed = %+v", current)
	}
	if _, err := st.CredentialOperationReceipt(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expired live-owner unexpectedly settled: %v", err)
	}
}

func TestCredentialOperationMismatchedRetirementReceiptCannotTakeOver(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-mismatched-receipt", KeychainAccount: "account-mismatched-receipt",
	})
	owner := credentialRecoveryManager(t, st, credstest.NewFake(), "receipt-owner")
	recovery := credentialRecoveryManager(t, st, owner.Creds, "receipt-recovery")
	before, err := owner.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	kind := store.CredentialOperationAdoptRotated
	operation := beginRetiredCredentialOperation(
		t,
		owner,
		account,
		kind,
		store.CredentialTargetKeychain,
		credentialIntentDigest(kind, "mismatched-receipt"),
		before,
	)
	wrongOwner := operation.Owner
	wrongOwner.Generation += "-other"
	receipt, verifier := credentialRetirementReceipt(
		t, wrongOwner, recovery.workers.owner.Generation,
	)
	recovery.workers.reaper = verifier
	if err := recovery.recoverCredentialOperation(t.Context(), operation, receipt); !errors.Is(err, store.ErrCredentialOperationOwner) {
		t.Fatalf("mismatched retirement receipt = %v, want owner rejection", err)
	}
	current, err := st.CredentialOperationByToken(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if current.OwnerEpoch != operation.OwnerEpoch || !reflect.DeepEqual(current.Owner, operation.Owner) {
		t.Fatalf("mismatched receipt mutated lane = %+v", current)
	}
}

func TestCredentialOperationRetirementReceiptTakesOverImmediately(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-immediate-receipt", KeychainAccount: "account-immediate-receipt",
	})
	owner := credentialRecoveryManager(t, st, credstest.NewFake(), "immediate-receipt-owner")
	recovery := credentialRecoveryManager(t, st, owner.Creds, "immediate-receipt-recovery")
	before, err := owner.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	kind := store.CredentialOperationAdoptRotated
	operation := beginCredentialOperation(
		t,
		owner,
		account,
		kind,
		store.CredentialTargetKeychain,
		credentialIntentDigest(kind, "immediate-receipt"),
		before,
	)
	operation, err = st.MarkCredentialOperationApplying(
		operation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	foreignBoundary := &credentialOperationBoundary{
		manager: recovery, account: account, operation: operation, expected: before,
	}
	if err := foreignBoundary.Cross(t.Context()); !errors.Is(err, store.ErrCredentialOperationOwner) {
		t.Fatalf("foreign owner reused stored fence = %v, want owner rejection", err)
	}
	unchanged, err := st.CredentialOperationByToken(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Owner != operation.Owner || unchanged.OwnerEpoch != operation.OwnerEpoch {
		t.Fatalf("foreign owner changed lane without receipt = %+v", unchanged)
	}
	receipt, verifier := credentialRetirementReceipt(
		t, operation.Owner, recovery.workers.owner.Generation,
	)
	recovery.workers.reaper = verifier
	if err := recovery.recoverCredentialOperation(t.Context(), operation, receipt); err != nil {
		t.Fatalf("immediate receipt recovery: %v", err)
	}
	terminal, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.TerminalStatus != store.CredentialTerminalQuarantined ||
		terminal.Result != store.CredentialResultAmbiguous ||
		terminal.OwnerEpoch != operation.OwnerEpoch+1 {
		t.Fatalf("immediate receipt recovery = %+v", terminal)
	}
}

func TestRetirementReceiptWaitsForCredentialAndAccountMutationLanes(t *testing.T) {
	st := openTestStore(t)
	credentialAccount := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-shared-receipt-credential", KeychainAccount: "account-shared-receipt-credential",
	})
	mutationAccount := persistTestAccount(t, st, store.Account{
		ID: 2, ConfigDir: t.TempDir(), KeychainService: "service-shared-receipt-mutation", KeychainAccount: "account-shared-receipt-mutation",
	})
	owner := credentialRecoveryManager(t, st, credstest.NewFake(), "shared-receipt-owner")
	recovery := credentialRecoveryManager(t, st, owner.Creds, "shared-receipt-recovery")
	credentialBefore, err := owner.credentialObservation(t.Context(), credentialAccount)
	if err != nil {
		t.Fatal(err)
	}
	credentialOperation := beginCredentialOperation(
		t,
		owner,
		credentialAccount,
		store.CredentialOperationAdoptRotated,
		store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationAdoptRotated, "shared-receipt"),
		credentialBefore,
	)
	mutationBefore, err := owner.credentialObservation(t.Context(), mutationAccount)
	if err != nil {
		t.Fatal(err)
	}
	mutationExpected, err := mutationBefore.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutationFile := creds.FileCredentialPath(mutationAccount.ConfigDir)
	mutationLocator := store.CredentialLocatorDigest(
		mutationAccount.KeychainService, mutationAccount.KeychainAccount, mutationFile,
	)
	mutationIntent := credentialIntentDigest(store.CredentialOperationAdoptRotated, "account-mutation")
	mutationID, err := store.NewAccountMutationID(
		mutationAccount.ID, mutationAccount.InstanceID, mutationAccount.Generation,
		store.AccountMutationRelogin, mutationLocator, mutationExpected, mutationIntent,
	)
	if err != nil {
		t.Fatal(err)
	}
	mutationBegin, err := st.BeginAccountMutation(t.Context(), store.BeginAccountMutationRequest{
		OperationID: mutationID, AccountID: mutationAccount.ID, Kind: store.AccountMutationRelogin,
		AccountInstanceID: mutationAccount.InstanceID, AccountGeneration: mutationAccount.Generation,
		LocatorDigest: mutationLocator, ExpectedCredentialDigest: mutationExpected,
		IntentDigest: mutationIntent, ConfigDir: mutationAccount.ConfigDir,
		KeychainService: mutationAccount.KeychainService, KeychainAccount: mutationAccount.KeychainAccount,
		Owner: owner.workers.owner,
	})
	if err != nil || mutationBegin.Active == nil {
		t.Fatalf("begin account mutation = %+v err=%v", mutationBegin, err)
	}
	receipt, verifier := credentialRetirementReceipt(
		t, owner.workers.owner, recovery.workers.owner.Generation,
	)
	recovery.workers.reaper = verifier
	remaining, err := recovery.recoverCredentialOwnerPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !remaining {
		t.Fatal("retirement receipt was acknowledged while account mutation remained")
	}
	if _, err := st.CredentialOperationByToken(credentialOperation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("prepared credential lane after recovery = %v", err)
	}
	page, err := verifier.ReapReceipts(
		t.Context(), proc.RecoveryTask, proc.ReapReceiptCursor{}, 1,
	)
	if err != nil || len(page.Receipts) != 1 || page.Receipts[0].Digest != receipt.Digest {
		t.Fatalf("retained shared receipt = %+v err=%v", page, err)
	}
	taken, _, err := recovery.TakeoverRetiredAccountMutationPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(taken) != 1 || taken[0].OperationID != mutationID ||
		taken[0].Owner != recovery.workers.owner ||
		taken[0].OwnerEpoch != mutationBegin.Active.OwnerEpoch+1 {
		t.Fatalf("taken account mutation = %+v", taken)
	}
	remaining, err = recovery.recoverCredentialOwnerPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if remaining {
		t.Fatal("retirement recovery remained after both old-owner lane classes cleared")
	}
	page, err = verifier.ReapReceipts(
		t.Context(), proc.RecoveryTask, proc.ReapReceiptCursor{}, 1,
	)
	if err != nil || len(page.Receipts) != 0 {
		t.Fatalf("acknowledged receipt remained = %+v err=%v", page, err)
	}
}

func TestSourceOwnerReceiptsRecoverEveryCredentialLaneBeforeExactPrefixAck(t *testing.T) {
	st := openTestStore(t)
	credentialAccount := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-source-credential", KeychainAccount: "account-source-credential",
	})
	mutationAccount := persistTestAccount(t, st, store.Account{
		ID: 2, ConfigDir: t.TempDir(), KeychainService: "service-source-mutation", KeychainAccount: "account-source-mutation",
	})
	old := credentialRecoveryManager(t, st, credstest.NewFake(), "source-receipt-owner")
	old.workers.owner = syntheticSourceOwner(t, 1)
	recovery := credentialRecoveryManager(t, st, old.Creds, "source-receipt-recovery")

	credentialBefore, err := old.credentialObservation(t.Context(), credentialAccount)
	if err != nil {
		t.Fatal(err)
	}
	credentialOperation := beginCredentialOperation(
		t,
		old,
		credentialAccount,
		store.CredentialOperationAdoptRotated,
		store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationAdoptRotated, "source-receipt"),
		credentialBefore,
	)
	credentialOperation, err = st.MarkCredentialOperationApplying(credentialOperation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := st.ReserveAccountIndex(old.workers.owner)
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID == 0 {
		t.Fatal("pending-add fixture was not created")
	}

	mutationBefore, err := old.credentialObservation(t.Context(), mutationAccount)
	if err != nil {
		t.Fatal(err)
	}
	mutationExpected, err := mutationBefore.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutationFile := creds.FileCredentialPath(mutationAccount.ConfigDir)
	mutationLocator := store.CredentialLocatorDigest(
		mutationAccount.KeychainService, mutationAccount.KeychainAccount, mutationFile,
	)
	mutationIntent := credentialIntentDigest(store.CredentialOperationAdoptRotated, "source-account-mutation")
	mutationID, err := store.NewAccountMutationID(
		mutationAccount.ID, mutationAccount.InstanceID, mutationAccount.Generation,
		store.AccountMutationRelogin, mutationLocator, mutationExpected, mutationIntent,
	)
	if err != nil {
		t.Fatal(err)
	}
	mutationBegin, err := st.BeginAccountMutation(t.Context(), store.BeginAccountMutationRequest{
		OperationID: mutationID, AccountID: mutationAccount.ID, Kind: store.AccountMutationRelogin,
		AccountInstanceID: mutationAccount.InstanceID, AccountGeneration: mutationAccount.Generation,
		LocatorDigest: mutationLocator, ExpectedCredentialDigest: mutationExpected,
		IntentDigest: mutationIntent, ConfigDir: mutationAccount.ConfigDir,
		KeychainService: mutationAccount.KeychainService, KeychainAccount: mutationAccount.KeychainAccount,
		Owner: old.workers.owner,
	})
	if err != nil || mutationBegin.Active == nil {
		t.Fatalf("begin account mutation = %+v err=%v", mutationBegin, err)
	}

	reaperGeneration := recovery.workers.owner.Generation
	receiptPath := filepath.Join(t.TempDir(), "source-owner-recovery.db")
	receiptStore := &proc.FileStore{Path: receiptPath, MaxOutstanding: 2}
	firstReceipt := commitCredentialRetirementReceipt(t, receiptStore, old.workers.owner, reaperGeneration)
	settledOwner := syntheticSourceOwner(t, 2)
	secondReceipt := commitCredentialRetirementReceipt(t, receiptStore, settledOwner, reaperGeneration)
	reopened := &proc.FileStore{Path: receiptPath, MaxOutstanding: 2}
	recovery.workers.reaper = &proc.Reaper{Store: reopened, Generation: reaperGeneration}
	blockedOwner := syntheticSourceOwner(t, 3)
	if err := reopened.Add(t.Context(), blockedOwner); !errors.Is(err, proc.ErrReceiptBacklog) {
		t.Fatalf("unacknowledged source receipts did not retain admission backpressure: %v", err)
	}

	remaining, err := recovery.recoverCredentialOwnerPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !remaining {
		t.Fatal("source receipt was acknowledged while its account mutation remained")
	}
	if _, err := st.CredentialOperationByToken(credentialOperation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source-owned credential lane after recovery = %v", err)
	}
	credentialReceipt, err := st.CredentialOperationReceipt(credentialOperation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if credentialReceipt.TerminalStatus != store.CredentialTerminalQuarantined ||
		credentialReceipt.Result != store.CredentialResultAmbiguous {
		t.Fatalf("source-owned credential receipt = %+v", credentialReceipt)
	}
	pendingRows, _, err := st.PendingAddReservationsOwnedBy(old.workers.owner, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingRows) != 0 {
		t.Fatalf("source-owned pending add survived recovery = %+v", pendingRows)
	}
	page, err := recovery.workers.reaper.ReapReceipts(
		t.Context(), proc.RecoverySourceOwner, proc.ReapReceiptCursor{}, 3,
	)
	if err != nil || len(page.Receipts) != 2 ||
		page.Receipts[0].Digest != firstReceipt.Digest ||
		page.Receipts[1].Digest != secondReceipt.Digest {
		t.Fatalf("source receipt prefix after blocked recovery = %+v err=%v", page, err)
	}

	taken, _, err := recovery.TakeoverRetiredAccountMutationPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(taken) != 1 || taken[0].OperationID != mutationID ||
		taken[0].Owner != recovery.workers.owner ||
		taken[0].OwnerEpoch != mutationBegin.Active.OwnerEpoch+1 {
		t.Fatalf("source-owned account mutation takeover = %+v", taken)
	}
	remaining, err = recovery.recoverCredentialOwnerPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if remaining {
		t.Fatal("source receipt recovery remained after every old-owner liability cleared")
	}
	page, err = recovery.workers.reaper.ReapReceipts(
		t.Context(), proc.RecoverySourceOwner, proc.ReapReceiptCursor{}, 1,
	)
	if err != nil || len(page.Receipts) != 0 {
		t.Fatalf("acknowledged source receipts remained = %+v err=%v", page, err)
	}
	floor, err := reopened.ReapReceiptFloor(t.Context(), proc.RecoverySourceOwner)
	if err != nil || floor.Sequence != secondReceipt.Sequence {
		t.Fatalf("source receipt floor = %+v err=%v, want sequence %d", floor, err, secondReceipt.Sequence)
	}
	if err := reopened.Add(t.Context(), blockedOwner); err != nil {
		t.Fatalf("source receipt acknowledgement did not reopen admission: %v", err)
	}
}

func TestExpiredEnsureFreshQuarantineAutoClearsOnExternalReplacement(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-refresh-recovery", KeychainAccount: "account-refresh-recovery",
	})
	credentials := credstest.NewFake()
	credentials.Put(account.KeychainService, account.KeychainAccount, datedCred("before", time.Minute))
	refresher := &countingRecoveryRefresher{}
	manager := credentialRecoveryManager(t, st, credentials, "refresh-owner")
	manager.OAuth = refresher
	before, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginRetiredCredentialOperation(
		t,
		manager,
		account,
		store.CredentialOperationEnsureFresh,
		store.CredentialTargetAll,
		credentialIntentDigest(
			store.CredentialOperationEnsureFresh, time.Minute.String(), "true",
		),
		before,
	)
	if err := recoverExpiredCredentialOperation(t, manager, operation); err != nil {
		t.Fatal(err)
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalQuarantined || receipt.Result != store.CredentialResultAmbiguous {
		t.Fatalf("receipt = %+v", receipt)
	}
	if _, err := freshCredentialOperationCodec().replay(
		t.Context(), manager, account, receipt,
	); !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("replay = %v, want quarantine", err)
	}
	if got := refresher.refreshes.Load(); got != 0 {
		t.Fatalf("OAuth refreshes during recovery/replay = %d, want 0", got)
	}
	if _, _, err := manager.EnsureFreshToken(t.Context(), account, RefreshLeadTime, true); !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("refresh against unchanged quarantine = %v, want quarantine", err)
	}
	if got := refresher.refreshes.Load(); got != 0 {
		t.Fatalf("OAuth refreshes against unchanged quarantine = %d, want 0", got)
	}
	replacement := datedCred("external-login", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, replacement)
	credential, _, err := manager.EnsureFreshToken(t.Context(), account, 0, false)
	if err != nil || credential.ClaudeAiOauth.AccessToken != replacement.ClaudeAiOauth.AccessToken {
		t.Fatalf("replacement did not reconcile quarantine: credential=%+v err=%v", credential, err)
	}
	if _, err := st.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("replacement left quarantine behind: %v", err)
	}
	retained, err := st.CredentialOperationReceipt(receipt.Token)
	if err != nil || retained.AcknowledgedAt.IsZero() {
		t.Fatalf("quarantine receipt was not acknowledged: receipt=%+v err=%v", retained, err)
	}
}

func TestExpiredEnsureFreshDoesNotClaimConcurrentReplacement(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-refresh-replaced", KeychainAccount: "account-refresh-replaced",
	})
	credentials := credstest.NewFake()
	original := datedCred("before", time.Minute)
	credentials.Put(account.KeychainService, account.KeychainAccount, original)
	refresher := &countingRecoveryRefresher{}
	manager := credentialRecoveryManager(t, st, credentials, "refresh-replaced-owner")
	manager.OAuth = refresher
	before, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginRetiredCredentialOperation(
		t,
		manager,
		account,
		store.CredentialOperationEnsureFresh,
		store.CredentialTargetAll,
		credentialIntentDigest(
			store.CredentialOperationEnsureFresh, time.Minute.String(), "true",
		),
		before,
	)
	replacement := datedCred("unrelated-login", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, replacement)
	if err := recoverExpiredCredentialOperation(t, manager, operation); err != nil {
		t.Fatal(err)
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalQuarantined || receipt.Result != store.CredentialResultAmbiguous {
		t.Fatalf("concurrent replacement receipt = %+v, want quarantine", receipt)
	}
	if got := refresher.refreshes.Load(); got != 0 {
		t.Fatalf("OAuth refreshes during replacement recovery = %d, want 0", got)
	}
}

func TestRefreshBoundaryFailureSettlesAndDoesNotRepeat(t *testing.T) {
	for _, tc := range []struct {
		name           string
		refresher      *boundaryFailureRefresher
		wantTerminal   error
		wantQuarantine bool
		checkReplay    func(*testing.T, error)
	}{
		{
			name: "transient response",
			refresher: &boundaryFailureRefresher{
				err: errors.Join(oauth.ErrNetwork, errors.New("connection reset")),
			},
			checkReplay: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, oauth.ErrNetwork) {
					t.Fatalf("replayed network failure = %v", err)
				}
			},
			wantTerminal:   ErrCredentialOperationQuarantined,
			wantQuarantine: true,
		},
		{
			name: "server response",
			refresher: &boundaryFailureRefresher{
				err: &oauth.RefreshError{Status: http.StatusServiceUnavailable},
			},
			checkReplay: func(t *testing.T, err error) {
				t.Helper()
				var refreshErr *oauth.RefreshError
				if !errors.As(err, &refreshErr) || refreshErr.Status < http.StatusInternalServerError {
					t.Fatalf("replayed server failure = %v", err)
				}
			},
			wantTerminal:   ErrCredentialOperationQuarantined,
			wantQuarantine: true,
		},
		{
			name: "caller cancellation",
			refresher: &boundaryFailureRefresher{
				waitForCancellation: true,
				started:             make(chan struct{}),
			},
			wantTerminal: ErrCredentialOperationQuarantined, wantQuarantine: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			account := persistTestAccount(t, st, store.Account{
				ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-boundary-failure", KeychainAccount: "account-boundary-failure",
			})
			credentials := credstest.NewFake()
			credentials.Put(account.KeychainService, account.KeychainAccount, datedCred("owned", time.Minute))
			manager := credentialRecoveryManager(t, st, credentials, "boundary-failure-owner")
			manager.OAuth = tc.refresher

			ctx := t.Context()
			var cancel context.CancelFunc
			if tc.refresher.waitForCancellation {
				ctx, cancel = context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() {
					_, _, err := manager.EnsureFreshToken(ctx, account, RefreshLeadTime, true)
					done <- err
				}()
				select {
				case <-tc.refresher.started:
				case <-time.After(time.Second):
					t.Fatal("refresh did not cross the OAuth boundary")
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, ErrCredentialOperationQuarantined) {
						t.Fatalf("cancelled refresh = %v, want quarantine", err)
					}
				case <-time.After(time.Second):
					t.Fatal("cancelled refresh did not settle")
				}
			} else {
				_, _, firstErr := manager.EnsureFreshToken(ctx, account, RefreshLeadTime, true)
				if tc.wantQuarantine && !errors.Is(firstErr, ErrCredentialOperationQuarantined) {
					t.Fatalf("ambiguous refresh was not quarantined: %v", firstErr)
				}
				if tc.checkReplay != nil {
					tc.checkReplay(t, firstErr)
				}
			}

			if got := tc.refresher.calls.Load(); got != 1 {
				t.Fatalf("OAuth calls after first attempt = %d, want 1", got)
			}
			_, _, replayErr := manager.EnsureFreshToken(t.Context(), account, RefreshLeadTime, true)
			if !errors.Is(replayErr, tc.wantTerminal) ||
				!errors.Is(replayErr, ErrCredentialOperationReplayed) {
				t.Fatalf("retained refresh evidence = %v, want %v + replay marker", replayErr, tc.wantTerminal)
			}
			if tc.checkReplay != nil {
				tc.checkReplay(t, replayErr)
			}
			if got := tc.refresher.calls.Load(); got != 1 {
				t.Fatalf("OAuth calls after retry = %d, want 1", got)
			}
			_, quarantineErr := st.CredentialQuarantine(account.ID)
			if tc.wantQuarantine != (quarantineErr == nil) {
				t.Fatalf("quarantine err = %v, want present=%t", quarantineErr, tc.wantQuarantine)
			}
		})
	}
}

func TestRefreshUnauthorizedIsTransientAndReplayable(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-refresh-unauthorized", KeychainAccount: "account-refresh-unauthorized",
	})
	credentials := credstest.NewFake()
	original := datedCred("owned", time.Minute)
	credentials.Put(account.KeychainService, account.KeychainAccount, original)
	refresher := &boundaryFailureRefresher{
		err: &oauth.RefreshError{Status: http.StatusUnauthorized},
	}
	manager := credentialRecoveryManager(t, st, credentials, "refresh-unauthorized-owner")
	manager.OAuth = refresher

	credential, refreshed, err := manager.EnsureFreshToken(
		t.Context(), account, RefreshLeadTime, true,
	)
	if refreshed || credential == nil {
		t.Fatalf("first refresh = credential=%v refreshed=%t", credential, refreshed)
	}
	if errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("plain refresh 401 classified as needs-login: %v", err)
	}
	var refreshErr *oauth.RefreshError
	if !errors.As(err, &refreshErr) || refreshErr.Status != http.StatusUnauthorized {
		t.Fatalf("first refresh error = %v, want refresh 401", err)
	}
	stored, ok := credentials.Get(account.KeychainService, account.KeychainAccount)
	if !ok || stored.ClaudeAiOauth.RefreshToken != original.ClaudeAiOauth.RefreshToken {
		t.Fatalf("plain refresh 401 changed stored credential: %+v", stored)
	}

	_, _, replayErr := manager.EnsureFreshToken(t.Context(), account, RefreshLeadTime, true)
	if errors.Is(replayErr, ErrNeedsLogin) {
		t.Fatalf("replayed refresh 401 classified as needs-login: %v", replayErr)
	}
	refreshErr = nil
	if !errors.Is(replayErr, ErrCredentialOperationFailed) ||
		!errors.As(replayErr, &refreshErr) || refreshErr.Status != http.StatusUnauthorized {
		t.Fatalf("replayed refresh error = %v, want failed receipt plus refresh 401", replayErr)
	}
	if got := refresher.calls.Load(); got != 1 {
		t.Fatalf("OAuth calls after replay = %d, want 1", got)
	}
}

func TestCredentialQuarantineGatesEveryMutation(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-quarantine-gate", KeychainAccount: "account-quarantine-gate",
	})
	credentials := credstest.NewFake()
	owned := datedCred("owned", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, owned)
	manager := credentialRecoveryManager(t, st, credentials, "quarantine-gate-owner")
	actual, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	actualDigest, err := actual.Digest()
	if err != nil {
		t.Fatal(err)
	}
	filePath := creds.FileCredentialPath(account.ConfigDir)
	if _, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialLocatorDigest(
			account.KeychainService, account.KeychainAccount, filePath,
		),
		FileLocatorDigest: store.CredentialFileLocatorDigest(filePath),
		Observation:       actual,
		Reason:            store.CredentialResultAmbiguous,
		FailureClass:      store.CredentialFailureInternal,
	}); err != nil {
		t.Fatal(err)
	}
	syncedCredential := datedCred("synced", 2*time.Hour)
	syncedCredential.ClaudeAiOauth.RefreshToken = ""
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"move", func() error {
			_, err := manager.MoveCredential(t.Context(), account, creds.SourceFile)
			return err
		}},
		{"drop-divergent-copy", func() error {
			return manager.DropDivergentCopy(t.Context(), account)
		}},
		{"adopt-rotated", func() error {
			return manager.AdoptRotatedToken(t.Context(), account)
		}},
		{"install-synced", func() error {
			_, err := manager.InstallSyncedCredential(t.Context(), account, syncedCredential)
			return err
		}},
		{"ensure-fresh", func() error {
			_, _, err := manager.EnsureFreshToken(t.Context(), account, 0, false)
			return err
		}},
		{"refresh-current", func() error {
			_, err := manager.refreshCurrentCredentialOperation(
				t.Context(), account, creds.SourceKeychain, owned,
			)
			return err
		}},
		{"compensate", func() error {
			return manager.CompensateCredentialState(t.Context(), account, actualDigest)
		}},
		{"remove-account", func() error {
			return manager.Remove(t.Context(), account.ID, true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if !errors.Is(err, ErrCredentialOperationQuarantined) {
				t.Fatalf("quarantined %s = %v, want quarantine", tc.name, err)
			}
		})
	}
	replacement := datedCred("replacement", 2*time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, replacement)
	observed, err := manager.credentialMutationObservation(t.Context(), account)
	if err != nil {
		t.Fatalf("reconcile replacement: %v", err)
	}
	if sameStoreObservation(observed, actual) {
		t.Fatal("replacement did not advance credential observation")
	}
	if _, err := st.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantine after replacement = %v", err)
	}
}

func TestCompensateCredentialStateDeletesOnlyExactWrittenState(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-compensate", KeychainAccount: "account-compensate",
	})
	credentials := credstest.NewFake()
	written := datedCred("written", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, written)
	writeRecoveryFileCredential(t, account, written)
	manager := credentialRecoveryManager(t, st, credentials, "compensate-owner")
	state, err := manager.CredentialExternalState(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	exactDigest, err := state.Digest()
	if err != nil {
		t.Fatal(err)
	}

	wrongDigest := exactDigest
	wrongDigest[0] ^= 0xff
	if err := manager.CompensateCredentialState(t.Context(), account, wrongDigest); !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("mismatched compensation = %v, want changed-underfoot", err)
	}
	if _, ok := credentials.Get(account.KeychainService, account.KeychainAccount); !ok {
		t.Fatal("mismatched compensation deleted Keychain credential")
	}
	if !fileCredentialExistsForTest(account.ConfigDir) {
		t.Fatal("mismatched compensation deleted file credential")
	}
	if got := credentials.DeletedServices(); len(got) != 0 {
		t.Fatalf("mismatched compensation Keychain deletes = %v, want none", got)
	}

	if err := manager.CompensateCredentialState(t.Context(), account, exactDigest); err != nil {
		t.Fatalf("exact compensation: %v", err)
	}
	if _, ok := credentials.Get(account.KeychainService, account.KeychainAccount); ok {
		t.Fatal("exact compensation retained Keychain credential")
	}
	if fileCredentialExistsForTest(account.ConfigDir) {
		t.Fatal("exact compensation retained file credential")
	}
	if err := manager.CompensateCredentialState(t.Context(), account, exactDigest); err != nil {
		t.Fatalf("idempotent compensation retry: %v", err)
	}
}

func TestCompensateQuarantinedCredentialStateRejectsFailureClassMismatch(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-quarantine-compensate", KeychainAccount: "account-quarantine-compensate",
	})
	credentials := credstest.NewFake()
	written := datedCred("quarantine-compensate", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, written)
	manager := credentialRecoveryManager(t, st, credentials, "quarantine-compensate-owner")
	state, err := manager.CredentialExternalState(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	exactDigest, err := state.Digest()
	if err != nil {
		t.Fatal(err)
	}
	filePath := creds.FileCredentialPath(account.ConfigDir)
	quarantine, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialLocatorDigest(
			account.KeychainService, account.KeychainAccount, filePath,
		),
		FileLocatorDigest: store.CredentialFileLocatorDigest(filePath),
		Observation:       state,
		Reason:            store.CredentialResultAmbiguous,
		FailureClass:      store.CredentialFailureInternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	mismatched := quarantine
	mismatched.FailureClass = store.CredentialFailureNetwork
	if err := manager.CompensateQuarantinedCredentialState(
		t.Context(), account, mismatched, exactDigest,
	); !errors.Is(err, store.ErrCredentialOperationState) {
		t.Fatalf("failure-class-mismatched compensation = %v, want ErrCredentialOperationState", err)
	}
	if _, ok := credentials.Get(account.KeychainService, account.KeychainAccount); !ok {
		t.Fatal("failure-class-mismatched compensation deleted Keychain credential")
	}
	if got := credentials.DeletedServices(); len(got) != 0 {
		t.Fatalf("failure-class-mismatched compensation deletes = %v, want none", got)
	}
}

func TestExpiredCompensationRecoveryFinishesExactPartialDelete(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-compensate-recovery", KeychainAccount: "account-compensate-recovery",
	})
	credentials := credstest.NewFake()
	written := datedCred("recovery-written", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, written)
	writeRecoveryFileCredential(t, account, written)
	manager := credentialRecoveryManager(t, st, credentials, "compensate-recovery-owner")
	before, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	exactDigest, err := before.Digest()
	if err != nil {
		t.Fatal(err)
	}
	operation := beginRetiredCredentialOperation(
		t,
		manager,
		account,
		store.CredentialOperationCompensate,
		store.CredentialTargetAll,
		credentialIntentDigest(store.CredentialOperationCompensate, string(exactDigest[:])),
		before,
	)
	credentials.Remove(account.KeychainService, account.KeychainAccount)
	if err := recoverExpiredCredentialOperation(t, manager, operation); err != nil {
		t.Fatalf("recover partial compensation: %v", err)
	}
	if _, ok := credentials.Get(account.KeychainService, account.KeychainAccount); ok {
		t.Fatal("compensation recovery retained Keychain credential")
	}
	if fileCredentialExistsForTest(account.ConfigDir) {
		t.Fatal("compensation recovery retained file credential")
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded || receipt.Result != store.CredentialResultDone {
		t.Fatalf("compensation recovery receipt = %+v", receipt)
	}
}

func TestExpiredAddCompensationRecoversFromAccountMutationSubject(t *testing.T) {
	st := openTestStore(t)
	credentials := credstest.NewFake()
	manager := credentialRecoveryManager(t, st, credentials, "pending-compensation-owner")
	reservation, err := st.ReserveAccountIndex(manager.workers.owner)
	if err != nil {
		t.Fatal(err)
	}
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation,
		ConfigDir: t.TempDir(), KeychainService: "service-pending-compensation",
		KeychainAccount: "account-pending-compensation",
	}
	empty, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest, err := empty.Digest()
	if err != nil {
		t.Fatal(err)
	}
	filePath := creds.FileCredentialPath(account.ConfigDir)
	locator := store.CredentialLocatorDigest(
		account.KeychainService, account.KeychainAccount, filePath,
	)
	accountIntent := credentialIntentDigest(store.CredentialOperationCompensate, "pending-add")
	accountOperationID, err := store.NewPendingAddMutationID(
		account.ID, account.InstanceID, account.Generation, accountIntent,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := st.BeginAccountMutation(t.Context(), store.BeginAccountMutationRequest{
		OperationID: accountOperationID, AccountID: account.ID, Kind: store.AccountMutationAdd,
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		IntentDigest: accountIntent, Owner: manager.workers.owner,
	})
	if err != nil || begin.Active == nil {
		t.Fatalf("begin pending Add = %+v err=%v", begin, err)
	}
	fence, err := st.BindAccountMutationPresentation(
		begin.Active.Fence(), account.ConfigDir, account.KeychainService,
		account.KeychainAccount, locator, emptyDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationInputProvided(
		fence, credentialIntentDigest(store.CredentialOperationCompensate, "input"),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	written := datedCred("pending-written", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, written)
	writeRecoveryFileCredential(t, account, written)
	writtenState, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	writtenDigest, err := writtenState.Digest()
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.SetAccountMutationMetadata(fence, "pending", "pending-account-uuid")
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplied(fence, writtenDigest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.BeginAccountRemoval(account.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitAccountMutation(fence, time.Now().Add(time.Minute)); !errors.Is(err, store.ErrAccountMutationSuperseded) {
		t.Fatalf("supersede pending Add = %v", err)
	}

	operation := beginRetiredCredentialOperation(
		t,
		manager,
		account,
		store.CredentialOperationCompensate,
		store.CredentialTargetAll,
		credentialIntentDigest(store.CredentialOperationCompensate, string(writtenDigest[:])),
		writtenState,
	)
	credentials.Remove(account.KeychainService, account.KeychainAccount)
	if err := recoverExpiredCredentialOperation(t, manager, operation); err != nil {
		t.Fatalf("recover pending Add compensation: %v", err)
	}
	if fileCredentialExistsForTest(account.ConfigDir) {
		t.Fatal("pending Add recovery retained file credential")
	}
	credentials.KeychainFaults.Read = errors.New("receipt replay must not read Keychain")
	credentials.FileFaults.Read = errors.New("receipt replay must not read file credential")
	if err := manager.CompensateCredentialState(t.Context(), account, writtenDigest); err != nil {
		t.Fatalf("replay recovered pending Add compensation: %v", err)
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded ||
		receipt.Result != store.CredentialResultDone || receipt.AcknowledgedAt.IsZero() {
		t.Fatalf("pending Add compensation receipt = %+v", receipt)
	}
}

func TestFreshReceiptReplayRejectsChangedCredential(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-replay-drift", KeychainAccount: "account-replay-drift",
	})
	credentials := credstest.NewFake()
	original := datedCred("receipt", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, original)
	manager := credentialRecoveryManager(t, st, credentials, "replay-drift-owner")
	before, err := manager.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginCredentialOperation(
		t,
		manager,
		account,
		store.CredentialOperationEnsureFresh,
		store.CredentialTargetAll,
		credentialIntentDigest(store.CredentialOperationEnsureFresh, "inspect", "replay"),
		before,
	)
	receipt, err := st.CommitPreparedCredentialOperation(
		operation.Fence(), before, store.CredentialResultUnchanged, time.Now().Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	credentials.Put(account.KeychainService, account.KeychainAccount, datedCred("later-login", 2*time.Hour))
	result, err := freshCredentialOperationCodec().replay(
		t.Context(), manager, account, receipt,
	)
	if !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("replay after credential drift = %+v err=%v, want quarantine", result, err)
	}
}

func TestCredentialOperationCancellationReleasesFlightAndLeavesReceipt(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-cancel", KeychainAccount: "account-cancel",
	})
	manager := credentialRecoveryManager(t, st, credstest.NewFake(), "cancel-owner")
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := runCredentialOperation(
			ctx,
			manager,
			account,
			store.CredentialOperationAdoptRotated,
			unitCredentialOperationCodec(
				store.CredentialTargetKeychain,
			),
			func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
				if err := boundary.Cross(ctx); err != nil {
					return struct{}{}, err
				}
				close(started)
				<-ctx.Done()
				return struct{}{}, ctx.Err()
			},
		)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("operation did not start")
	}
	operation, err := st.CredentialOperation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled operation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled operation did not settle")
	}
	manager.credentialMu.Lock()
	flights := len(manager.credentialFlights)
	manager.credentialMu.Unlock()
	if flights != 0 {
		t.Fatalf("in-memory credential flights after cancellation = %d", flights)
	}
	if _, err := st.CredentialOperationByToken(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("active lane after cancellation = %v", err)
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalFailed || receipt.Result != store.CredentialResultFailed {
		t.Fatalf("cancellation receipt = %+v", receipt)
	}
	if _, err := unitCredentialOperationCodec(
		store.CredentialTargetKeychain,
	).replay(t.Context(), manager, account, receipt); !errors.Is(err, ErrCredentialOperationFailed) {
		t.Fatalf("receipt replay = %v, want failed-operation sentinel", err)
	}
	if _, err := runCredentialOperation(
		t.Context(),
		manager,
		account,
		store.CredentialOperationDropDivergent,
		unitCredentialOperationCodec(
			store.CredentialTargetKeychain,
		),
		func(context.Context, *credentialOperationBoundary) (struct{}, error) { return struct{}{}, nil },
	); err != nil {
		t.Fatalf("operation lane was not reusable after cancellation: %v", err)
	}
}

func TestCredentialFailureReceiptPreservesClassification(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantClass    store.CredentialFailureClass
		status       store.CredentialTerminalStatus
		wantTerminal error
		check        func(*testing.T, error)
	}{
		{
			name: "network", err: errors.Join(oauth.ErrNetwork, errors.New("dial failed")),
			wantClass: store.CredentialFailureNetwork,
			status:    store.CredentialTerminalFailed, wantTerminal: ErrCredentialOperationFailed,
			check: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, oauth.ErrNetwork) {
					t.Fatalf("replay error = %v, want network classification", err)
				}
			},
		},
		{
			name: "refresh unauthorized", err: &oauth.RefreshError{Status: http.StatusUnauthorized},
			wantClass: store.CredentialFailureRefreshUnauthorized,
			status:    store.CredentialTerminalFailed, wantTerminal: ErrCredentialOperationFailed,
			check: func(t *testing.T, err error) {
				t.Helper()
				var refreshErr *oauth.RefreshError
				if !errors.As(err, &refreshErr) || refreshErr.Status != http.StatusUnauthorized {
					t.Fatalf("replay error = %v, want refresh unauthorized", err)
				}
			},
		},
		{
			name: "refresh rejected", err: &oauth.RefreshError{Status: http.StatusForbidden},
			wantClass: store.CredentialFailureRefreshRejected,
			status:    store.CredentialTerminalFailed, wantTerminal: ErrCredentialOperationFailed,
			check: func(t *testing.T, err error) {
				t.Helper()
				var refreshErr *oauth.RefreshError
				if !errors.As(err, &refreshErr) || refreshErr.Status != http.StatusForbidden {
					t.Fatalf("replay error = %v, want refresh rejection", err)
				}
			},
		},
		{
			name: "refresh server", err: &oauth.RefreshError{Status: http.StatusBadGateway},
			wantClass: store.CredentialFailureRefreshServer,
			status:    store.CredentialTerminalFailed, wantTerminal: ErrCredentialOperationFailed,
			check: func(t *testing.T, err error) {
				t.Helper()
				var refreshErr *oauth.RefreshError
				if !errors.As(err, &refreshErr) || refreshErr.Status < http.StatusInternalServerError {
					t.Fatalf("replay error = %v, want refresh server failure", err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class := credentialFailureClass(test.err)
			if class != test.wantClass {
				t.Fatalf("class = %q, want %q", class, test.wantClass)
			}
			_, err := replayCredentialReceiptFailure[struct{}](store.CredentialOperationReceipt{
				TerminalStatus: test.status,
				Result:         store.CredentialResultFailed,
				FailureClass:   class,
			})
			if !errors.Is(err, test.wantTerminal) {
				t.Fatalf("replay error = %v, want terminal sentinel %v", err, test.wantTerminal)
			}
			test.check(t, err)
		})
	}
}

func credentialRecoveryManager(
	t *testing.T,
	st *store.Store,
	credentials Credentials,
	generation string,
) *Manager {
	t.Helper()
	manager := &Manager{Store: st, Creds: credentials}
	owner := bindTestWorkerAuthority(t, manager, "credential-recovery-"+generation)
	manager.workers = &workerRuntime{owner: owner}
	return manager
}

func beginRetiredCredentialOperation(
	t *testing.T,
	manager *Manager,
	account store.Account,
	kind store.CredentialOperationKind,
	target store.CredentialTarget,
	intent store.CredentialDigest,
	before store.CredentialExternalState,
) store.CredentialOperation {
	t.Helper()
	operation := beginCredentialOperation(
		t, manager, account, kind, target, intent, before,
	)
	var err error
	operation, err = manager.Store.MarkCredentialOperationApplying(
		operation.Fence(), []byte(`{"version":1,"test":"staged-before-external-io"}`))
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func beginCredentialOperation(
	t *testing.T,
	manager *Manager,
	account store.Account,
	kind store.CredentialOperationKind,
	target store.CredentialTarget,
	intent store.CredentialDigest,
	before store.CredentialExternalState,
) store.CredentialOperation {
	t.Helper()
	filePath := creds.FileCredentialPath(account.ConfigDir)
	locator := store.CredentialLocatorDigest(
		account.KeychainService, account.KeychainAccount, filePath,
	)
	operationID, err := store.NewCredentialOperationID(
		account.InstanceID, account.Generation, kind, target, locator, before, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := manager.Store.BeginCredentialOperation(store.BeginCredentialOperationRequest{
		OperationID: operationID,
		AccountID:   account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest:     locator,
		FileLocatorDigest: store.CredentialFileLocatorDigest(filePath),
		Owner:             manager.workers.owner,
		Kind:              kind,
		Target:            target,
		IntentDigest:      intent,
		Expected:          before,
	})
	if err != nil || !begin.Created || begin.Active == nil {
		t.Fatalf("begin = %+v err=%v", begin, err)
	}
	return *begin.Active
}

func recoverExpiredCredentialOperation(
	t *testing.T,
	owner *Manager,
	operation store.CredentialOperation,
) error {
	t.Helper()
	recovery := credentialRecoveryManager(
		t, owner.Store, owner.Creds, "retired-"+operation.Token[:8],
	)
	receipt, verifier := credentialRetirementReceipt(
		t, operation.Owner, recovery.workers.owner.Generation,
	)
	recovery.workers.reaper = verifier
	return recovery.recoverCredentialOperation(t.Context(), operation, receipt)
}

func writeRecoveryFileCredential(t *testing.T, account store.Account, credential *creds.Credential) {
	t.Helper()
	if err := writeFileCredentialForTest(account.ConfigDir, credential); err != nil {
		t.Fatal(err)
	}
}

func credentialRetirementReceipt(
	t *testing.T,
	owner proc.Record,
	reaperGeneration string,
) (proc.ReapReceipt, *proc.Reaper) {
	t.Helper()
	receiptStore := &proc.FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	receipt := commitCredentialRetirementReceipt(t, receiptStore, owner, reaperGeneration)
	return receipt, &proc.Reaper{Store: receiptStore, Generation: reaperGeneration}
}

func commitCredentialRetirementReceipt(
	t *testing.T,
	receiptStore *proc.FileStore,
	owner proc.Record,
	reaperGeneration string,
) proc.ReapReceipt {
	t.Helper()
	if err := receiptStore.Add(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if err := receiptStore.BeginReap(t.Context(), owner, reaperGeneration); err != nil {
		t.Fatal(err)
	}
	receipt, err := receiptStore.CommitReap(
		t.Context(), owner, reaperGeneration, proc.ReapAbsent,
	)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func syntheticSourceOwner(t *testing.T, sequence int) proc.Record {
	t.Helper()
	pid := 41000 + sequence
	owner := proc.Record{
		RecoveryClass: proc.RecoverySourceOwner,
		PID:           pid,
		StartTime:     fmt.Sprintf("source-owner-%d", sequence),
		Boot:          "source-owner-test-boot",
		Comm:          "source-owner-test",
		Generation:    fmt.Sprintf("source-owner-generation-%d", sequence),
		ProcessGroup:  true,
		SessionID:     pid,
	}
	if err := owner.Validate(); err != nil {
		t.Fatal(err)
	}
	return owner
}

type countingRecoveryRefresher struct{ refreshes atomic.Int32 }

func (f *countingRecoveryRefresher) Refresh(context.Context, string, string) (*oauth.TokenResponse, error) {
	f.refreshes.Add(1)
	return nil, errors.New("unexpected OAuth refresh")
}

func (*countingRecoveryRefresher) Usage(context.Context, string) (*oauth.Usage, error) {
	return nil, errors.New("unexpected OAuth usage")
}

type boundaryFailureRefresher struct {
	calls               atomic.Int32
	err                 error
	waitForCancellation bool
	started             chan struct{}
}

func (f *boundaryFailureRefresher) Refresh(ctx context.Context, _, _ string) (*oauth.TokenResponse, error) {
	f.calls.Add(1)
	if f.waitForCancellation {
		close(f.started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, f.err
}

func (*boundaryFailureRefresher) Usage(context.Context, string) (*oauth.Usage, error) {
	return nil, errors.New("unexpected OAuth usage")
}
