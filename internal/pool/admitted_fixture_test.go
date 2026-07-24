package pool

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

func admitPoolTestAccount(t *testing.T, st *store.Store, requested store.Account) store.Account {
	t.Helper()
	var fillers []store.Account
	for {
		owner := proc.Record{
			RecoveryID: CredentialOwnerRecoveryID,
			PID:        42,
			StartTime:  "1.0",
			Boot:       "test-boot",
			Comm:       "cc-pool-test",
			Generation: poolTestGeneration(fmt.Sprintf("admitted-account-%d", requested.ID)),
		}
		reservation, err := st.ReserveAccountIndex(owner)
		if err != nil {
			t.Fatal(err)
		}
		if reservation.ID > requested.ID {
			t.Fatalf("admit test account: requested id %d is already consumed", requested.ID)
		}
		candidate := requested
		if reservation.ID != requested.ID {
			candidate = store.Account{ID: reservation.ID}
		}
		account := commitPoolTestAccount(t, st, candidate, reservation, owner)
		if reservation.ID != requested.ID {
			fillers = append(fillers, account)
			continue
		}
		for _, filler := range fillers {
			if err := st.DeleteAccount(filler.ID); err != nil {
				t.Fatalf("retire filler account %d: %v", filler.ID, err)
			}
		}
		return account
	}
}

func commitPoolTestAccount(
	t *testing.T,
	st *store.Store,
	requested store.Account,
	reservation store.PendingAccountReservation,
	owner proc.Record,
) store.Account {
	return commitPoolTestAccountAtPresentation(t, st, requested, reservation, owner, "")
}

func commitPoolTestAccountAtPresentation(
	t *testing.T,
	st *store.Store,
	requested store.Account,
	reservation store.PendingAccountReservation,
	owner proc.Record,
	publicPath string,
) store.Account {
	t.Helper()
	home, err := Home()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(
		filepath.Clean(home), filepath.Clean(os.TempDir())+string(os.PathSeparator),
	) {
		t.Setenv("HOME", t.TempDir())
	}
	configDir, err := AccountConfigDir(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	keychainService, err := AccountKeychainService(reservation.InstanceID)
	if err != nil {
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
	intent := poolTestCredentialDigest("admit-intent-" + reservation.InstanceID)
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
	if publicPath == "" {
		publicPath = filepath.Join(
			mustHome(), "Library", "CloudStorage", "CCPoolStatus-"+AccountDirName(reservation.ID),
		)
	}
	if err := os.MkdirAll(publicPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := EnsureAccountConfigDir(reservation.InstanceID, publicPath); err != nil {
		t.Fatal(err)
	}
	proof := poolTestPresentationProof(reservation, publicPath)
	fence, err := st.BindAccountMutationPresentation(
		begin.Active.Fence(),
		proof,
		configDir,
		keychainService,
		keychainAccount,
		store.CredentialKeychainLocatorDigest(keychainService, keychainAccount),
		poolTestCredentialDigest("admit-expected-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationInputProvided(fence, poolTestCredentialDigest("admit-input-"+reservation.InstanceID))
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = st.MarkAccountMutationApplied(fence, poolTestCredentialDigest("admit-written-"+reservation.InstanceID))
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

func poolTestCredentialDigest(value string) store.CredentialDigest {
	return store.CredentialDigest(sha256.Sum256([]byte(value)))
}

func poolTestPresentationProof(
	reservation store.PendingAccountReservation,
	publicPath string,
) store.FileProviderPresentationIdentity {
	return store.FileProviderPresentationIdentity{
		TenantID:   "account-" + reservation.InstanceID,
		DomainID:   "domain-" + reservation.InstanceID,
		Generation: reservation.Generation, PublicPath: publicPath,
	}
}
