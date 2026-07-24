package hostsync

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func admitHostsyncTestAccount(
	t *testing.T,
	manager *pool.Manager,
	requested store.Account,
) store.Account {
	t.Helper()
	owner, err := manager.MutationOwner()
	if err != nil {
		t.Fatal(err)
	}
	for {
		reservation, err := manager.Store.ReserveAccountIndex(owner)
		if err != nil {
			t.Fatal(err)
		}
		if reservation.ID > requested.ID {
			t.Fatalf("admit hostsync test account: requested id %d is already consumed", requested.ID)
		}
		candidate := requested
		if reservation.ID != requested.ID {
			candidate = store.Account{ID: reservation.ID}
		}
		account := commitHostsyncTestAccount(t, manager.Store, reservation, candidate)
		if reservation.ID == requested.ID {
			return account
		}
		if err := manager.Store.DeleteAccount(account.ID); err != nil {
			t.Fatalf("retire filler account %d: %v", account.ID, err)
		}
	}
}

func commitHostsyncTestAccount(
	t *testing.T,
	st *store.Store,
	reservation store.PendingAccountReservation,
	requested store.Account,
) store.Account {
	t.Helper()
	configDir := requested.ConfigDir
	if configDir == "" {
		configDir = fmt.Sprintf("/tmp/cc-pool-hostsync-test/acct-%02d", reservation.ID)
	}
	keychainService := requested.KeychainService
	if keychainService == "" {
		keychainService = fmt.Sprintf("hostsync-test-service-%d", reservation.ID)
	}
	keychainAccount := requested.KeychainAccount
	if keychainAccount == "" {
		keychainAccount = fmt.Sprintf("hostsync-test-account-%d", reservation.ID)
	}
	label := requested.Label
	if label == "" {
		label = fmt.Sprintf("hostsync-test-label-%d", reservation.ID)
	}
	accountUUID := requested.AccountUUID
	if accountUUID == "" {
		accountUUID = "hostsync-test-uuid-" + reservation.InstanceID
	}
	intent := hostsyncTestDigest("admit-intent-" + reservation.InstanceID)
	operationID, err := store.NewPendingAddMutationID(
		reservation.ID, reservation.InstanceID, reservation.Generation, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := st.BeginAccountMutation(t.Context(), store.BeginAccountMutationRequest{
		OperationID: operationID,
		AccountID:   reservation.ID, Kind: store.AccountMutationAdd,
		AccountInstanceID: reservation.InstanceID, AccountGeneration: reservation.Generation,
		IntentDigest: intent, Label: label, AccountUUID: accountUUID,
		Owner: reservation.Owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !begin.Created || begin.Active == nil {
		t.Fatalf("admit hostsync test account: begin = %+v", begin)
	}
	proof := hostsyncTestPresentationProof(reservation, configDir)
	fence, err := st.BindAccountMutationPresentation(
		begin.Active.Fence(), proof, configDir, keychainService, keychainAccount,
		store.CredentialKeychainLocatorDigest(keychainService, keychainAccount),
		hostsyncTestDigest("admit-expected-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationInputProvided(
		fence, hostsyncTestDigest("admit-input-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplied(
		fence, hostsyncTestDigest("admit-written-"+reservation.InstanceID),
	)
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
	receipt, err := st.CommitAccountMutation(fence, time.Now().Add(time.Hour))
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
		reservation.ID, time.Now(), store.AuthReasonInternal,
		store.DigestReason("explicit healthy hostsync test account"), store.AuthKindOwned,
	); err != nil {
		t.Fatal(err)
	}
	if cleared, err := st.ClearNeedsLogin(reservation.ID); err != nil || !cleared {
		t.Fatalf("admit hostsync test account: clear auth state = %v, %v", cleared, err)
	}
	account, err := st.GetAccount(reservation.ID)
	if err != nil {
		t.Fatal(err)
	}
	return account
}

func hostsyncTestDigest(value string) store.CredentialDigest {
	return store.CredentialDigest(sha256.Sum256([]byte(value)))
}

func hostsyncTestPresentationProof(
	reservation store.PendingAccountReservation,
	publicPath string,
) store.FileProviderPresentationIdentity {
	tenantID := "account-" + reservation.InstanceID
	return store.FileProviderPresentationIdentity{
		TenantID: tenantID, DomainID: "domain-" + reservation.InstanceID,
		Generation: reservation.Generation, PublicPath: publicPath,
	}
}
