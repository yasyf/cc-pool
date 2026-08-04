package pool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func TestClaimForeignLanesHonorsCancellationAndRepresentsForeignRows(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-claim-cancel", KeychainAccount: "account-claim-cancel",
	})
	credentials := credstest.NewFake()
	old := credentialRecoveryManager(t, st, credentials, "claim-cancel-old")
	recovery := credentialRecoveryManager(t, st, credentials, "claim-cancel-new")
	before, err := old.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	kind := store.CredentialOperationAdoptRotated
	operation := beginCredentialOperation(
		t, old, account, kind, store.CredentialTargetKeychain,
		credentialIntentDigest(kind, "claim-cancel"), before,
	)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := recovery.ClaimForeignLanes(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled claim pass = %v, want context.Canceled", err)
	}
	untouched, err := st.CredentialOperationByToken(operation.Token)
	if err != nil || untouched.OwnerEpoch != operation.OwnerEpoch ||
		!bytes.Equal(untouched.Owner, operation.Owner) {
		t.Fatalf("cancelled claim mutated lane = %+v err=%v", untouched, err)
	}

	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CredentialOperationByToken(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("re-presented prepared lane after claim = %v, want abandoned", err)
	}
	if _, err := st.CredentialOperationReceipt(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("abandoned prepared lane left a receipt: %v", err)
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
			context.Background(), first, account, store.CredentialOperationCompensate,
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
			context.Background(), second, account, store.CredentialOperationCompensate,
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
	adopted := datedCred("adopted-after-boundary", time.Hour)
	credentials.Put(account.KeychainService, account.KeychainAccount, adopted)
	kind := store.CredentialOperationAdoptRotated
	target := store.CredentialTargetKeychain
	intent := credentialIntentDigest(kind, "same-request")
	expected, err := first.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	locator := store.CredentialKeychainLocatorDigest(
		account.KeychainService, account.KeychainAccount,
	)
	operationID, err := store.NewCredentialOperationID(
		account.InstanceID, account.Generation,
		account.ConfigDir, account.KeychainService, account.KeychainAccount,
		kind, target, locator, expected, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	codec := adoptRotatedCredentialOperationCodec(target)
	apply := func(ctx context.Context, boundary *credentialOperationBoundary) (struct{}, error) {
		executions.Add(1)
		if err := boundary.recordCredentialWrite(adopted); err != nil {
			return struct{}{}, err
		}
		if err := boundary.Cross(ctx); err != nil {
			return struct{}{}, err
		}
		credentials.Put(
			account.KeychainService,
			account.KeychainAccount,
			adopted,
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

func TestCredentialRemovalRecoversOwnerDeathAfterDelete(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st := openTestStore(t)
	credentials := credstest.NewFake()
	owner := credentialRecoveryManager(t, st, credentials, "remove-owner")
	account := persistTestAccount(t, st, store.Account{
		ID: 1, KeychainAccount: "account-remove-recovery",
	})
	presentation, err := st.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	credentials.Put(
		account.KeychainService, account.KeychainAccount,
		datedCred("remove-recovery", time.Hour),
	)
	before, err := owner.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := store.CredentialRemovalIntentDigest(
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount,
	)
	if err != nil {
		t.Fatal(err)
	}
	removal, err := st.BeginAccountRemoval(account.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginCredentialOperation(
		t, owner, account, store.CredentialOperationRemove,
		store.CredentialTargetKeychain, intent, before,
	)
	operation, err = st.MarkCredentialOperationApplying(operation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	credentials.Remove(account.KeychainService, account.KeychainAccount)

	recovery := credentialRecoveryManager(t, st, credentials, "remove-recovery")
	recovery.ClaimCredentialMutation = func(int) (func(), error) {
		return func() {}, nil
	}
	if err := recovery.recoverCredentialOperation(t.Context(), operation); err != nil {
		t.Fatalf("recover exact delete: %v", err)
	}
	terminal, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.TerminalStatus != store.CredentialTerminalSucceeded ||
		terminal.Result != store.CredentialResultDone {
		t.Fatalf("recovered removal = %+v", terminal)
	}
	if err := recovery.removeCredentialForAccountRemovalAt(
		t.Context(), account, presentation.Identity.PublicPath,
	); err != nil {
		t.Fatalf("replay recovered receipt: %v", err)
	}
	if err := st.DeleteAccount(removal.AccountID); err != nil {
		t.Fatalf("finalize recovered account removal: %v", err)
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
		operation.Fence(), nil,
	)
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
	if current.State != store.CredentialOperationApplying || !bytes.Equal(current.Owner, owner.owner) {
		t.Fatalf("live operation mutated = %+v", current)
	}
	if _, err := st.CredentialOperationReceipt(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("live operation unexpectedly settled: %v", err)
	}
}

func TestCredentialOperationExpiredLiveOwnerLaneIsNeverClaimedByJoiners(t *testing.T) {
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
	if current.OwnerEpoch != operation.OwnerEpoch || !bytes.Equal(current.Owner, owner.owner) {
		t.Fatalf("expired live-owner lane changed = %+v", current)
	}
	if _, err := st.CredentialOperationReceipt(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expired live-owner unexpectedly settled: %v", err)
	}
}

func TestCredentialOperationStaleFenceClaimCannotTakeOver(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-stale-fence", KeychainAccount: "account-stale-fence",
	})
	owner := credentialRecoveryManager(t, st, credstest.NewFake(), "stale-fence-owner")
	recovery := credentialRecoveryManager(t, st, owner.Creds, "stale-fence-recovery")
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
		credentialIntentDigest(kind, "stale-fence"),
		before,
	)
	interloper, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := st.TakeoverCredentialOperation(operation.Fence(), interloper)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.recoverCredentialOperation(t.Context(), operation); !errors.Is(err, store.ErrCredentialOperationOwner) {
		t.Fatalf("stale-fence claim = %v, want owner rejection", err)
	}
	current, err := st.CredentialOperationByToken(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if current.OwnerEpoch != claimed.OwnerEpoch || !bytes.Equal(current.Owner, interloper) {
		t.Fatalf("stale-fence claim mutated lane = %+v", current)
	}
}

func TestCredentialOperationClaimTakesOverImmediately(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-immediate-claim", KeychainAccount: "account-immediate-claim",
	})
	owner := credentialRecoveryManager(t, st, credstest.NewFake(), "immediate-claim-owner")
	recovery := credentialRecoveryManager(t, st, owner.Creds, "immediate-claim-recovery")
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
		credentialIntentDigest(kind, "immediate-claim"),
		before,
	)
	operation, err = st.MarkCredentialOperationApplying(
		operation.Fence(), nil,
	)
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
	if !bytes.Equal(unchanged.Owner, operation.Owner) || unchanged.OwnerEpoch != operation.OwnerEpoch {
		t.Fatalf("foreign owner changed lane without a claim = %+v", unchanged)
	}
	if err := recovery.recoverCredentialOperation(t.Context(), operation); err != nil {
		t.Fatalf("immediate claim recovery: %v", err)
	}
	terminal, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.TerminalStatus != store.CredentialTerminalQuarantined ||
		terminal.Result != store.CredentialResultAmbiguous ||
		terminal.OwnerEpoch != operation.OwnerEpoch+1 {
		t.Fatalf("immediate claim recovery = %+v", terminal)
	}
}

func TestClaimForeignLanesAndMutationPageClearEveryForeignClass(t *testing.T) {
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
	mutationLocator := store.CredentialKeychainLocatorDigest(
		mutationAccount.KeychainService, mutationAccount.KeychainAccount,
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
		Owner: owner.owner,
	})
	if err != nil || mutationBegin.Active == nil {
		t.Fatalf("begin account mutation = %+v err=%v", mutationBegin, err)
	}
	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CredentialOperationByToken(credentialOperation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("prepared credential lane after claim = %v", err)
	}
	foreignMutations, _, err := st.AccountMutationsNotOwnedBy(recovery.owner, 0, 8)
	if err != nil || len(foreignMutations) != 1 {
		t.Fatalf("foreign mutations after credential claim = %+v err=%v", foreignMutations, err)
	}
	taken, more, err := recovery.TakeoverRetiredAccountMutationPage(t.Context())
	if err != nil || more {
		t.Fatalf("mutation takeover page more=%v err=%v", more, err)
	}
	if len(taken) != 1 || taken[0].OperationID != mutationID ||
		!bytes.Equal(taken[0].Owner, recovery.owner) ||
		taken[0].OwnerEpoch != mutationBegin.Active.OwnerEpoch+1 {
		t.Fatalf("taken account mutation = %+v", taken)
	}
	redelivered, more, err := recovery.TakeoverRetiredAccountMutationPage(t.Context())
	if err != nil || more || len(redelivered) != 0 {
		t.Fatalf("claimed mutation re-delivered = %+v more=%v err=%v", redelivered, more, err)
	}
	if remaining, _, err := st.CredentialOperationsNotOwnedBy(recovery.owner, 0, 8); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("foreign credential lanes remained = %+v err=%v", remaining, err)
	}
}

// The golden is one literal v0.20.9 owner_record: a proc.Record encoded by
// daemonkit@v0.20.9's Record.MarshalJSON from the module cache, Validate- and
// round-trip-verified under that exact codec before capture.
const poolUpgradeGoldenOwner = `{"recovery_id":"com.yasyf.cc-pool.credential-owner.v1","pid":4242,"start_time":"1722700000.123456","boot":"9f2a6c1e-5b4d-4e3a-8890-abcdef012345","comm":"cc-pool","executable":"/opt/homebrew/Cellar/cc-pool/0.20.9/bin/cc-pool","audit_token":[245,1,0,0,245,1,0,0,245,1,0,0,20,0,0,0,245,1,0,0,146,16,0,0,109,135,1,0,105,122,0,0],"generation":"5aa1b2c3d4e5f60718293a4b5c6d7e8f","process_group":false,"session_id":0,"role":"","operation_id":"","stop_session":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"preparation_nonce":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"runtime_protocol":0,"target_process_generation":null,"stop_authority_state":"","expires_unix_milli":0}`

// TestClaimForeignLanesAdoptsVZeroTwentyNineRows is §B's pool-level arm: rows
// whose owner_record bytes are a literal v0.20.9 proc.Record are claimed by a
// v2 successor exactly as any dead generation's rows, with the pending-add
// retirement proof hook and account-mutation transfer intact.
// TODO(dk-v021 integration): add the daemon-boot arm once Lane D's spec exists —
// the cross-era gate (RemoveUnmarked + legacy lock) plus Serve → Start →
// ClaimForeignLanes against a live v0.20.9 incumbent.
func TestClaimForeignLanesAdoptsVZeroTwentyNineRows(t *testing.T) {
	st := openTestStore(t)
	credentialAccount := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(), KeychainService: "service-source-credential", KeychainAccount: "account-source-credential",
	})
	mutationAccount := persistTestAccount(t, st, store.Account{
		ID: 2, ConfigDir: t.TempDir(), KeychainService: "service-source-mutation", KeychainAccount: "account-source-mutation",
	})
	golden := store.OwnerRecord(poolUpgradeGoldenOwner)
	old := credentialRecoveryManager(t, st, credstest.NewFake(), "source-golden-owner")
	old.owner = golden
	recovery := credentialRecoveryManager(t, st, old.Creds, "source-golden-recovery")
	installTestBackingRunner(recovery)

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
		credentialIntentDigest(store.CredentialOperationAdoptRotated, "source-golden"),
		credentialBefore,
	)
	credentialOperation, err = st.MarkCredentialOperationApplying(credentialOperation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(credentialOperation.Owner, golden) {
		t.Fatalf("seeded lane owner = %q, want golden v0.20.9 bytes", credentialOperation.Owner)
	}
	pending, err := st.ReserveAccountIndex(golden)
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID == 0 {
		t.Fatal("pending-add fixture was not created")
	}
	installTestBackingRunner(old)
	home, err := Home()
	if err != nil {
		t.Fatal(err)
	}
	pendingPublicPath := filepath.Join(
		home, "Library", "CloudStorage", "pending-"+pending.InstanceID,
	)
	if err := os.MkdirAll(pendingPublicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	preparedPending, err := old.PrepareReservedAdd(t.Context(), pending, pendingPublicPath)
	if err != nil {
		t.Fatal(err)
	}
	pendingMarker := filepath.Join(pendingPublicPath, "survives-recovery")
	if err := os.WriteFile(pendingMarker, []byte("public"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovery.RetirePendingAdd = func(
		ctx context.Context,
		reservation store.PendingAccountReservation,
	) (PendingAddRetirementProof, error) {
		if err := ctx.Err(); err != nil {
			return PendingAddRetirementProof{}, err
		}
		return PendingAddRetirementProof{
			AccountID: reservation.ID, AccountInstanceID: reservation.InstanceID,
			AccountGeneration: reservation.Generation, PublicPath: preparedPending.PublicPath,
		}, nil
	}

	mutationBefore, err := old.credentialObservation(t.Context(), mutationAccount)
	if err != nil {
		t.Fatal(err)
	}
	mutationExpected, err := mutationBefore.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutationLocator := store.CredentialKeychainLocatorDigest(
		mutationAccount.KeychainService, mutationAccount.KeychainAccount,
	)
	mutationIntent := credentialIntentDigest(store.CredentialOperationAdoptRotated, "source-golden-mutation")
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
		Owner: golden,
	})
	if err != nil || mutationBegin.Active == nil {
		t.Fatalf("begin account mutation = %+v err=%v", mutationBegin, err)
	}

	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CredentialOperationByToken(credentialOperation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("golden-owned lane after claim = %v", err)
	}
	credentialReceipt, err := st.CredentialOperationReceipt(credentialOperation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if credentialReceipt.TerminalStatus != store.CredentialTerminalQuarantined ||
		credentialReceipt.Result != store.CredentialResultAmbiguous ||
		credentialReceipt.OwnerEpoch != credentialOperation.OwnerEpoch+1 ||
		!bytes.Equal(credentialReceipt.Owner, recovery.owner) {
		t.Fatalf("claimed golden lane receipt = %+v", credentialReceipt)
	}
	pendingRows, _, err := st.PendingAddReservationsNotOwnedBy(recovery.owner, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingRows) != 0 {
		t.Fatalf("golden-owned pending add survived the claim = %+v", pendingRows)
	}
	// #nosec G304 -- pendingMarker is created beneath this test's private temporary root.
	if got, err := os.ReadFile(pendingMarker); err != nil || string(got) != "public" {
		t.Fatalf("pending public target after claim = %q err=%v", got, err)
	}
	reused, err := st.ReserveAccountIndex(recovery.owner)
	if err != nil || reused.ID != pending.ID {
		t.Fatalf("reservation after proven retirement = %+v err=%v", reused, err)
	}
	if err := st.ReleaseAccountIndex(reused); err != nil {
		t.Fatal(err)
	}

	taken, _, err := recovery.TakeoverRetiredAccountMutationPage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(taken) != 1 || taken[0].OperationID != mutationID ||
		!bytes.Equal(taken[0].Owner, recovery.owner) ||
		taken[0].OwnerEpoch != mutationBegin.Active.OwnerEpoch+1 {
		t.Fatalf("golden account mutation takeover = %+v", taken)
	}
	if remaining, _, err := st.CredentialOperationsNotOwnedBy(recovery.owner, 0, 8); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("foreign credential lanes remained = %+v err=%v", remaining, err)
	}
	if remaining, _, err := st.AccountMutationsNotOwnedBy(recovery.owner, 0, 8); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("foreign account mutations remained = %+v err=%v", remaining, err)
	}
	if remaining, _, err := st.PendingAddReservationsNotOwnedBy(recovery.owner, 0, 8); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("foreign pending adds remained = %+v err=%v", remaining, err)
	}
}

func TestClaimForeignLanesRetainsPendingAddWhenRetirementIsAmbiguous(t *testing.T) {
	st := openTestStore(t)
	old := credentialRecoveryManager(t, st, credstest.NewFake(), "pending-retirement-old")
	recovery := credentialRecoveryManager(t, st, old.Creds, "pending-retirement-new")
	reservation, err := st.ReserveAccountIndex(old.owner)
	if err != nil {
		t.Fatal(err)
	}
	retirementErr := errors.New("tenant retirement unavailable")
	recovery.RetirePendingAdd = func(
		context.Context,
		store.PendingAccountReservation,
	) (PendingAddRetirementProof, error) {
		return PendingAddRetirementProof{}, retirementErr
	}
	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatalf("ambiguous retirement failed the claim pass: %v", err)
	}
	pending, _, err := st.PendingAddReservationsNotOwnedBy(recovery.owner, 0, 1)
	if err != nil || len(pending) != 1 || pending[0].ID != reservation.ID ||
		!bytes.Equal(pending[0].Owner, old.owner) {
		t.Fatalf("retained reservation = %+v err=%v", pending, err)
	}
	next, err := st.ReserveAccountIndex(recovery.owner)
	if err != nil || next.ID == reservation.ID {
		t.Fatalf("reservation reused after ambiguous retirement = %+v err=%v", next, err)
	}
	represented, _, err := st.PendingAddReservationsNotOwnedBy(recovery.owner, 0, 8)
	if err != nil || len(represented) != 1 || represented[0].ID != reservation.ID {
		t.Fatalf("deferred reservation not re-presented = %+v err=%v", represented, err)
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
		store.CredentialTargetKeychain,
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
		store.CredentialTargetKeychain,
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
	if _, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		),
		Observation:  actual,
		Reason:       store.CredentialResultAmbiguous,
		FailureClass: store.CredentialFailureInternal,
	}); err != nil {
		t.Fatal(err)
	}
	syncedCredential := datedCred("synced", 2*time.Hour)
	syncedCredential.ClaudeAiOauth.RefreshToken = ""
	presentation, err := manager.Store.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func() error
		want error
	}{
		{"adopt-rotated", func() error {
			return manager.AdoptRotatedToken(t.Context(), account)
		}, ErrCredentialOperationQuarantined},
		{"install-synced", func() error {
			_, err := manager.InstallSyncedCredential(t.Context(), account, syncedCredential)
			return err
		}, ErrCredentialOperationQuarantined},
		{"ensure-fresh", func() error {
			_, _, err := manager.EnsureFreshToken(t.Context(), account, 0, false)
			return err
		}, ErrCredentialOperationQuarantined},
		{"refresh-current", func() error {
			_, err := manager.refreshCurrentCredentialOperation(
				t.Context(), account, creds.SourceKeychain, owned,
			)
			return err
		}, ErrCredentialOperationQuarantined},
		{"compensate", func() error {
			return manager.CompensateCredentialState(t.Context(), account, actualDigest)
		}, ErrCredentialOperationQuarantined},
		{"remove-account", func() error {
			removal, err := manager.Store.BeginAccountRemoval(account.ID, true)
			if err != nil {
				return err
			}
			return manager.FinishAccountRemoval(
				t.Context(), removal, presentation.Identity.PublicPath,
			)
		}, store.ErrCredentialOperationEvidenceActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if !errors.Is(err, tc.want) {
				t.Fatalf("quarantined %s = %v, want %v", tc.name, err, tc.want)
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
	if got := credentials.DeletedServices(); len(got) != 0 {
		t.Fatalf("mismatched compensation Keychain deletes = %v, want none", got)
	}

	if err := manager.CompensateCredentialState(t.Context(), account, exactDigest); err != nil {
		t.Fatalf("exact compensation: %v", err)
	}
	if _, ok := credentials.Get(account.KeychainService, account.KeychainAccount); ok {
		t.Fatal("exact compensation retained Keychain credential")
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
	quarantine, err := st.QuarantineCredential(store.QuarantineCredentialRequest{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		),
		Observation:  state,
		Reason:       store.CredentialResultAmbiguous,
		FailureClass: store.CredentialFailureInternal,
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
		store.CredentialTargetKeychain,
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
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded || receipt.Result != store.CredentialResultDone {
		t.Fatalf("compensation recovery receipt = %+v", receipt)
	}
}

func TestExpiredAddCompensationRecoversFromAccountMutationSubject(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st := openTestStore(t)
	credentials := credstest.NewFake()
	manager := credentialRecoveryManager(t, st, credentials, "pending-compensation-owner")
	reservation, err := st.ReserveAccountIndex(manager.owner)
	if err != nil {
		t.Fatal(err)
	}
	configDir, err := AccountConfigDir(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	keychainService, err := AccountKeychainService(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	publicPath := testFileProviderPublicPath(reservation.ID)
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(reservation.InstanceID, publicPath); err != nil {
		t.Fatal(err)
	}
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation,
		ConfigDir: configDir, KeychainService: keychainService,
		KeychainAccount: "account-pending-compensation",
	}
	empty := store.CredentialExternalState{
		Keychain: store.CredentialSlotObservation{State: store.CredentialSlotEmpty},
	}
	emptyDigest, err := empty.Digest()
	if err != nil {
		t.Fatal(err)
	}
	locator := store.CredentialKeychainLocatorDigest(
		account.KeychainService, account.KeychainAccount,
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
		IntentDigest: accountIntent, Owner: manager.owner,
	})
	if err != nil || begin.Active == nil {
		t.Fatalf("begin pending Add = %+v err=%v", begin, err)
	}
	fence, err := st.BindAccountMutationPresentation(
		begin.Active.Fence(), poolTestPresentationProof(reservation, publicPath),
		account.ConfigDir, account.KeychainService,
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
		store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationCompensate, string(writtenDigest[:])),
		writtenState,
	)
	credentials.Remove(account.KeychainService, account.KeychainAccount)
	if err := recoverExpiredCredentialOperation(t, manager, operation); err != nil {
		t.Fatalf("recover pending Add compensation: %v", err)
	}
	credentials.KeychainFaults.Read = errors.New("receipt replay must not read Keychain")
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
		store.CredentialTargetKeychain,
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
		store.CredentialOperationCompensate,
		unitCredentialOperationCodec(
			store.CredentialTargetKeychain,
		),
		func(context.Context, *credentialOperationBoundary) (struct{}, error) { return struct{}{}, nil },
		"reused-after-cancellation",
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
	bindTestWorkerAuthority(t, manager, "credential-recovery-"+generation)
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
		operation.Fence(), []byte(`{"version":1,"test":"staged-before-external-io"}`),
	)
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
	locator := store.CredentialKeychainLocatorDigest(
		account.KeychainService, account.KeychainAccount,
	)
	operationID, err := store.NewCredentialOperationID(
		account.InstanceID, account.Generation,
		account.ConfigDir, account.KeychainService, account.KeychainAccount,
		kind, target, locator, before, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := manager.Store.BeginCredentialOperation(store.BeginCredentialOperationRequest{
		OperationID: operationID,
		AccountID:   account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation,
		ConfigDir:         account.ConfigDir,
		KeychainService:   account.KeychainService,
		KeychainAccount:   account.KeychainAccount,
		LocatorDigest:     locator,
		Owner:             manager.owner,
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
	return recovery.recoverCredentialOperation(t.Context(), operation)
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

func TestStrandedCredentialRecoveryFencesAdmissionAndRetries(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(),
		KeychainService: "service-stranded", KeychainAccount: "account-stranded",
	})
	credentials := credstest.NewFake()
	old := credentialRecoveryManager(t, st, credentials, "stranded-old")
	recovery := credentialRecoveryManager(t, st, credentials, "stranded-new")
	installTestBackingRunner(recovery)

	before, err := old.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginCredentialOperation(
		t, old, account, store.CredentialOperationEnsureFresh, store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationEnsureFresh, "stranded"), before,
	)
	operation, err = st.MarkCredentialOperationApplying(operation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = st.MarkCredentialOperationApplied(
		operation.Fence(), before, store.CredentialTerminalSucceeded,
		store.CredentialResultUnchanged, store.CredentialFailureNone, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	linkTarget, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatalf("claim pass failed instead of deferring: %v", err)
	}
	claimed, err := st.CredentialOperationByToken(operation.Token)
	if err != nil || !bytes.Equal(claimed.Owner, recovery.owner) ||
		claimed.OwnerEpoch != operation.OwnerEpoch+1 {
		t.Fatalf("stranded lane after claim = %+v err=%v", claimed, err)
	}
	if _, err := recovery.retryStrandedCredentialRecovery(t.Context(), account.ID); !errors.Is(err, ErrCredentialRecoveryPending) {
		t.Fatalf("fenced admission = %v, want ErrCredentialRecoveryPending", err)
	}
	fenced := recovery.StrandedCredentialRecoveries()
	if len(fenced) != 1 || fenced[0].AccountID != account.ID ||
		fenced[0].Token != operation.Token || fenced[0].Cause == "" {
		t.Fatalf("fenced accounts = %+v", fenced)
	}

	if err := os.Symlink(linkTarget, account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	cleared, err := recovery.retryStrandedCredentialRecovery(t.Context(), account.ID)
	if err != nil || !cleared {
		t.Fatalf("healed retry cleared=%v err=%v", cleared, err)
	}
	if _, err := st.CredentialOperationByToken(operation.Token); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stranded lane survived retry = %v", err)
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil || receipt.TerminalStatus != store.CredentialTerminalSucceeded ||
		receipt.Result != store.CredentialResultUnchanged ||
		!bytes.Equal(receipt.Owner, recovery.owner) {
		t.Fatalf("settled receipt = %+v err=%v", receipt, err)
	}
	if cleared, err := recovery.retryStrandedCredentialRecovery(t.Context(), account.ID); err != nil || cleared {
		t.Fatalf("unfenced account = cleared=%v err=%v", cleared, err)
	}
	if remaining := recovery.StrandedCredentialRecoveries(); len(remaining) != 0 {
		t.Fatalf("fenced accounts after recovery = %+v", remaining)
	}
	after, err := recovery.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatalf("healed account observation = %v", err)
	}
	admitted := beginCredentialOperation(
		t, recovery, account, store.CredentialOperationEnsureFresh, store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationEnsureFresh, "stranded-after"), after,
	)
	if admitted.Token == "" || !bytes.Equal(admitted.Owner, recovery.owner) {
		t.Fatalf("healed account admission = %+v", admitted)
	}
}

func TestStrandedRetryFencesObservedOperationsAndWaits(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(),
		KeychainService: "service-fence-doors", KeychainAccount: "account-fence-doors",
	})
	credentials := credstest.NewFake()
	old := credentialRecoveryManager(t, st, credentials, "fence-doors-old")
	recovery := credentialRecoveryManager(t, st, credentials, "fence-doors-new")
	before, err := old.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginRetiredCredentialOperation(
		t, old, account, store.CredentialOperationEnsureFresh, store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationEnsureFresh, "fence-doors"), before,
	)
	linkTarget, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatal(err)
	}

	recovery.ClaimCredentialMutation = func(int) (func(), error) { return func() {}, nil }
	_, err = runCredentialOperationObserved(
		t.Context(), recovery, account, store.CredentialOperationEnsureFresh,
		unitCredentialOperationCodec(store.CredentialTargetKeychain),
		recovery.credentialObservation,
		func(context.Context, *credentialOperationBoundary) (struct{}, error) {
			return struct{}{}, nil
		},
	)
	if !errors.Is(err, ErrCredentialRecoveryPending) {
		t.Fatalf("observed operation on a fenced account = %v, want ErrCredentialRecoveryPending", err)
	}
	if _, err := recovery.waitCredentialOperation(t.Context(), operation.Token); !errors.Is(err, ErrCredentialRecoveryPending) {
		t.Fatalf("wait on a stranded token = %v, want ErrCredentialRecoveryPending", err)
	}
	if err := os.Symlink(linkTarget, account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.admitSyncedCredential(
		t.Context(), account,
		store.FileProviderPresentationIdentity{}, store.FileProviderPresentationIdentity{},
		store.CredentialDigest{}, "",
	); errors.Is(err, ErrCredentialRecoveryPending) {
		t.Fatalf("synced admission still pending after heal = %v", err)
	}
	if remaining := recovery.StrandedCredentialRecoveries(); len(remaining) != 0 {
		t.Fatalf("synced admission did not heal the fence = %+v", remaining)
	}
}

func TestStrandedRetrySettlementIsSingleFlight(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(),
		KeychainService: "service-single-flight", KeychainAccount: "account-single-flight",
	})
	credentials := credstest.NewFake()
	old := credentialRecoveryManager(t, st, credentials, "single-flight-old")
	recovery := credentialRecoveryManager(t, st, credentials, "single-flight-new")
	before, err := old.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	operation := beginCredentialOperation(
		t, old, account, store.CredentialOperationEnsureFresh, store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationEnsureFresh, "single-flight"), before,
	)
	operation, err = st.MarkCredentialOperationApplying(operation.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	operation, err = st.MarkCredentialOperationApplied(
		operation.Fence(), before, store.CredentialTerminalSucceeded,
		store.CredentialResultUnchanged, store.CredentialFailureNone, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	linkTarget, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linkTarget, account.ConfigDir); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]error, 8)
	clears := make([]bool, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clears[i], results[i] = recovery.retryStrandedCredentialRecovery(t.Context(), account.ID)
		}()
	}
	wg.Wait()
	for i, retryErr := range results {
		if retryErr != nil || !clears[i] {
			t.Fatalf("concurrent retry %d = cleared=%v err=%v", i, clears[i], retryErr)
		}
	}
	receipt, err := st.CredentialOperationReceipt(operation.Token)
	if err != nil || receipt.TerminalStatus != store.CredentialTerminalSucceeded ||
		receipt.Result != store.CredentialResultUnchanged {
		t.Fatalf("racing settlements corrupted the receipt = %+v err=%v", receipt, err)
	}
	if _, err := st.CredentialQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("spurious quarantine after concurrent retries = %v", err)
	}
	if remaining := recovery.StrandedCredentialRecoveries(); len(remaining) != 0 {
		t.Fatalf("strand survived = %+v", remaining)
	}
}

func TestQuarantinedAccountRefusesObservedOperationsWithoutPanic(t *testing.T) {
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 1, ConfigDir: t.TempDir(),
		KeychainService: "service-quarantine-arm", KeychainAccount: "account-quarantine-arm",
	})
	credentials := credstest.NewFake()
	old := credentialRecoveryManager(t, st, credentials, "quarantine-arm-old")
	recovery := credentialRecoveryManager(t, st, credentials, "quarantine-arm-new")
	before, err := old.credentialObservation(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	beginRetiredCredentialOperation(
		t, old, account, store.CredentialOperationEnsureFresh, store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationEnsureFresh, "quarantine-arm"), before,
	)
	linkTarget, err := os.Readlink(account.ConfigDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	if err := recovery.ClaimForeignLanes(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linkTarget, account.ConfigDir); err != nil {
		t.Fatal(err)
	}
	cleared, err := recovery.retryStrandedCredentialRecovery(t.Context(), account.ID)
	if err != nil || !cleared {
		t.Fatalf("healed retry cleared=%v err=%v", cleared, err)
	}

	recovery.ClaimCredentialMutation = func(int) (func(), error) { return func() {}, nil }
	_, err = runCredentialOperationObserved(
		t.Context(), recovery, account, store.CredentialOperationEnsureFresh,
		unitCredentialOperationCodec(store.CredentialTargetKeychain),
		recovery.credentialObservation,
		func(context.Context, *credentialOperationBoundary) (struct{}, error) {
			return struct{}{}, nil
		},
	)
	if !errors.Is(err, store.ErrCredentialOperationRecoveryRequired) {
		t.Fatalf("operation on a quarantined account = %v, want recovery-required refusal", err)
	}
}
