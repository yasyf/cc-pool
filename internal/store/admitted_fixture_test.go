package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func admitTestAccount(t *testing.T, s *Store, requested Account) Account {
	t.Helper()
	if requested.ID < 1 {
		t.Fatalf("admit test account: invalid requested id %d", requested.ID)
	}
	for {
		reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner(
			fmt.Sprintf("admitted-account-%d", requested.ID),
		))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.ID > requested.ID {
			t.Fatalf("admit test account: requested id %d is already consumed", requested.ID)
		}

		account := requested
		if reservation.ID != requested.ID {
			account = Account{ID: reservation.ID}
		}
		account = commitTestAccount(t, s, reservation, account)
		if reservation.ID == requested.ID {
			return account
		}
		if err := s.DeleteAccount(account.ID); err != nil {
			t.Fatalf("retire filler account %d: %v", account.ID, err)
		}
	}
}

func commitTestAccount(
	t *testing.T,
	s *Store,
	reservation PendingAccountReservation,
	requested Account,
) Account {
	t.Helper()
	configDir := requested.ConfigDir
	if configDir == "" {
		configDir = fmt.Sprintf("/tmp/cc-pool-test/acct-%02d", reservation.ID)
	} else if !filepath.IsAbs(configDir) {
		configDir = filepath.Join("/tmp/cc-pool-test", configDir)
	}
	keychainService := requested.KeychainService
	if keychainService == "" {
		keychainService = fmt.Sprintf("test-service-%d", reservation.ID)
	}
	keychainAccount := requested.KeychainAccount
	if keychainAccount == "" {
		keychainAccount = fmt.Sprintf("test-account-%d", reservation.ID)
	}
	label := requested.Label
	if label == "" {
		label = fmt.Sprintf("test-label-%d", reservation.ID)
	}
	accountUUID := requested.AccountUUID
	if accountUUID == "" {
		accountUUID = "test-uuid-" + reservation.InstanceID
	}
	intent := credentialOperationTestDigest("admit-intent-" + reservation.InstanceID)
	operationID, err := NewPendingAddMutationID(
		reservation.ID,
		reservation.InstanceID,
		reservation.Generation,
		intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := s.BeginAccountMutation(t.Context(), BeginAccountMutationRequest{
		OperationID: operationID,
		AccountID:   reservation.ID, Kind: AccountMutationAdd,
		AccountInstanceID: reservation.InstanceID, AccountGeneration: reservation.Generation,
		IntentDigest: intent, Label: label, AccountUUID: accountUUID,
		Owner: reservation.Owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !begin.Created || begin.Active == nil {
		t.Fatalf("admit test account: begin = %+v", begin)
	}
	proof := presentationTestProof(Account{
		InstanceID: reservation.InstanceID,
		Generation: reservation.Generation,
	}, configDir, "test-activation-"+reservation.InstanceID)
	fence, err := s.BindAccountMutationPresentation(
		begin.Active.Fence(),
		proof,
		configDir,
		keychainService,
		keychainAccount,
		CredentialKeychainLocatorDigest(keychainService, keychainAccount),
		credentialOperationTestDigest("admit-expected-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationInputProvided(
		fence,
		credentialOperationTestDigest("admit-input-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationApplied(
		fence,
		credentialOperationTestDigest("admit-written-"+reservation.InstanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.SetAccountMutationMetadata(fence, label, accountUUID)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = s.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CommitAccountMutation(fence, s.now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MarkAccountMutationPublicationSettled(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeAccountMutationReceipt(receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetNeedsLogin(
		reservation.ID,
		s.now(),
		AuthReasonInternal,
		DigestReason("explicit healthy test account"),
		AuthKindOwned,
	); err != nil {
		t.Fatal(err)
	}
	cleared, err := s.ClearNeedsLogin(reservation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !cleared {
		t.Fatal("admit test account: explicit auth state was not cleared")
	}
	account, err := s.GetAccount(reservation.ID)
	if err != nil {
		t.Fatal(err)
	}
	return account
}
