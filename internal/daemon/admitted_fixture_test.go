package daemon

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

func admitDaemonTestAccount(t *testing.T, st *store.Store, requested store.Account) store.Account {
	t.Helper()
	return admitDaemonTestAccountAtPublicPath(
		t, st, requested, testFileProviderPublicPath(requested.ID),
	)
}

func admitDaemonTestAccountAtPublicPath(
	t *testing.T,
	st *store.Store,
	requested store.Account,
	publicPath string,
) store.Account {
	t.Helper()
	owner := proc.Record{
		RecoveryID: pool.CredentialOwnerRecoveryID,
		PID:        42,
		StartTime:  "1.0",
		Boot:       "test-boot",
		Comm:       "cc-pool-test",
		Generation: daemonTestGeneration(fmt.Sprintf("admitted-account-%d", requested.ID)),
	}
	reservation, err := st.ReserveAccountIndex(owner)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ID != requested.ID {
		t.Fatalf("admit test account: reserved id %d, want %d", reservation.ID, requested.ID)
	}
	configDir, err := pool.AccountConfigDir(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	keychainService, err := pool.AccountKeychainService(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := pool.EnsureAccountConfigDir(reservation.InstanceID, publicPath); err != nil {
		t.Fatal(err)
	}
	keychainAccount := requested.KeychainAccount
	if keychainAccount == "" {
		keychainAccount = fmt.Sprintf("test-account-%d", requested.ID)
	}
	label := requested.Label
	if label == "" {
		label = fmt.Sprintf("test-label-%d", requested.ID)
	}
	accountUUID := requested.AccountUUID
	if accountUUID == "" {
		accountUUID = "test-uuid-" + reservation.InstanceID
	}
	intent := daemonTestCredentialDigest("admit-intent-" + reservation.InstanceID)
	operationID, err := store.NewPendingAddMutationID(
		reservation.ID,
		reservation.InstanceID,
		reservation.Generation,
		intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := st.BeginAccountMutation(t.Context(), store.BeginAccountMutationRequest{
		OperationID: operationID,
		AccountID:   reservation.ID, Kind: store.AccountMutationAdd,
		AccountInstanceID: reservation.InstanceID, AccountGeneration: reservation.Generation,
		IntentDigest: intent, Label: label, AccountUUID: accountUUID,
		Owner: owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !begin.Created || begin.Active == nil {
		t.Fatalf("admit test account: begin = %+v", begin)
	}
	proof := daemonFixturePresentationProof(reservation, publicPath)
	fence, err := st.BindAccountMutationPresentation(
		begin.Active.Fence(),
		proof,
		configDir,
		keychainService,
		keychainAccount,
		store.CredentialKeychainLocatorDigest(keychainService, keychainAccount),
		daemonTestCredentialDigest("admit-expected-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationInputProvided(fence, daemonTestCredentialDigest("admit-input-"+reservation.InstanceID))
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplied(fence, daemonTestCredentialDigest("admit-written-"+reservation.InstanceID))
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.SetAccountMutationMetadata(fence, label, accountUUID)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := st.CommitAccountMutation(fence, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkAccountMutationPublicationSettled(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := st.AcknowledgeAccountMutationReceipt(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetNeedsLogin(
		requested.ID,
		time.Now(),
		store.AuthReasonInternal,
		store.DigestReason("explicit healthy test account"),
		store.AuthKindOwned,
	); err != nil {
		t.Fatal(err)
	}
	if cleared, err := st.ClearNeedsLogin(requested.ID); err != nil || !cleared {
		t.Fatalf("admit test account: clear explicit auth state = %v, %v", cleared, err)
	}
	account, err := st.GetAccount(requested.ID)
	if err != nil {
		t.Fatal(err)
	}
	return account
}

func daemonTestCredentialDigest(value string) store.CredentialDigest {
	return store.CredentialDigest(sha256.Sum256([]byte(value)))
}

func daemonFixturePresentationProof(
	reservation store.PendingAccountReservation,
	configDir string,
) store.FileProviderPresentationIdentity {
	identity, err := projectPreparationIdentity(daemonTestPreparationProof(store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation,
	}, configDir))
	if err != nil {
		panic(err)
	}
	return identity
}
