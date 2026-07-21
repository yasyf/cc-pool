package store

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestBeginAccountRemovalRejectsActiveSessionUntilClosed(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccount(t, s)
	started := time.Unix(1_900_000_000, 0)
	sessionID := activateTestSession(t, s, account.ID, 4242, "/project", started)

	if _, err := s.BeginAccountRemoval(account.ID, true); !errors.Is(err, ErrAccountSessionActive) {
		t.Fatalf("begin removal with active session = %v, want ErrAccountSessionActive", err)
	}
	if _, err := accountRemovalByID(s.db, account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected removal persisted an intent: %v", err)
	}
	if err := s.CloseSession(sessionID, started.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginAccountRemoval(account.ID, true); err != nil {
		t.Fatalf("begin removal after session close: %v", err)
	}
}

func TestBeginAccountRemovalRejectsAdmittedExternalOperations(t *testing.T) {
	t.Run("account mutation", func(t *testing.T) {
		s := openTest(t)
		now := time.Unix(1_900_000_000, 0)
		s.now = func() time.Time { return now }
		account := credentialOperationTestAccount(t, s)
		request := existingAccountMutationTestRequest(
			t, account, AccountMutationSyncInstall,
			credentialOperationTestOwner("removal-account-mutation"), now.Add(time.Minute),
		)
		begin, err := s.BeginAccountMutation(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); !errors.Is(err, ErrAccountMutationBusy) {
			t.Fatalf("begin removal with account mutation = %v, want ErrAccountMutationBusy", err)
		}
		receipt, err := s.ResolveAccountMutation(
			begin.Active.Fence(), AccountMutationAborted,
			begin.Active.ExpectedCredentialDigest, nil, now.Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
			t.Fatalf("begin removal with unacknowledged account receipt = %v, want ErrCredentialOperationEvidenceActive", err)
		}
		if err := s.AcknowledgeAccountMutationReceipt(receipt.OperationID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); err != nil {
			t.Fatalf("begin removal after account mutation settlement: %v", err)
		}
	})

	t.Run("credential operation", func(t *testing.T) {
		s := openTest(t)
		now := time.Unix(1_900_000_000, 0)
		s.now = func() time.Time { return now }
		account := credentialOperationTestAccount(t, s)
		request := credentialOperationTestRequest(
			t, account, CredentialOperationAdoptRotated, CredentialTargetKeychain,
			credentialOperationTestState("before", ""), "removal-credential-operation",
			credentialOperationTestOwner("removal-credential-operation"),
		)
		begin, err := s.BeginCredentialOperation(request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); !errors.Is(err, ErrCredentialOperationBusy) {
			t.Fatalf("begin removal with credential operation = %v, want ErrCredentialOperationBusy", err)
		}
		receipt, err := s.CommitPreparedCredentialOperation(
			begin.Active.Fence(), begin.Active.Expected, CredentialResultDone, now.Add(time.Minute),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
			t.Fatalf("begin removal with unacknowledged credential receipt = %v, want ErrCredentialOperationEvidenceActive", err)
		}
		if err := s.AcknowledgeCredentialOperation(receipt.Token); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); err != nil {
			t.Fatalf("begin removal after credential operation settlement: %v", err)
		}
	})

	t.Run("credential quarantine", func(t *testing.T) {
		s := openTest(t)
		account := credentialOperationTestAccount(t, s)
		quarantine, err := s.QuarantineCredential(QuarantineCredentialRequest{
			AccountID: account.ID, AccountInstanceID: account.InstanceID,
			AccountGeneration: account.Generation,
			LocatorDigest: CredentialLocatorDigest(
				account.KeychainService, account.KeychainAccount, account.ConfigDir,
			),
			FileLocatorDigest: CredentialFileLocatorDigest(account.ConfigDir),
			Observation:       credentialOperationTestState("ambiguous", ""),
			Reason:            CredentialResultAmbiguous,
			FailureClass:      CredentialFailureInternal,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); !errors.Is(err, ErrCredentialOperationEvidenceActive) {
			t.Fatalf("begin removal with credential quarantine = %v, want ErrCredentialOperationEvidenceActive", err)
		}
		if err := s.ClearCredentialQuarantine(quarantine); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(account.ID, true); err != nil {
			t.Fatalf("begin removal after quarantine reconciliation: %v", err)
		}
	})
}

func TestBeginAccountRemovalOnlyAllowsSettledMatchingPendingAdd(t *testing.T) {
	for _, state := range []AccountMutationState{
		AccountMutationAwaitingInput,
		AccountMutationApplying,
		AccountMutationApplied,
	} {
		t.Run(string(state), func(t *testing.T) {
			s := openTest(t)
			now := time.Unix(1_900_000_000, 0)
			s.now = func() time.Time { return now }
			reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
			if err != nil {
				t.Fatal(err)
			}
			request := accountMutationTestRequest(t, reservation, AccountMutationAdd, now.Add(time.Minute))
			begin, err := s.BeginAccountMutation(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			fence := begin.Active.Fence()
			if state == AccountMutationApplying || state == AccountMutationApplied {
				fence, err = s.MarkAccountMutationInputProvided(fence, credentialOperationTestDigest("input"))
				if err != nil {
					t.Fatal(err)
				}
				fence, err = s.MarkAccountMutationApplying(fence)
				if err != nil {
					t.Fatal(err)
				}
			}
			if state == AccountMutationApplied {
				if _, err := s.MarkAccountMutationApplied(fence, credentialOperationTestDigest("written")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.BeginAccountRemoval(reservation.ID, true); !errors.Is(err, ErrAccountMutationBusy) {
				t.Fatalf("begin removal with %s Add = %v, want ErrAccountMutationBusy", state, err)
			}
			if _, err := accountRemovalByID(s.db, reservation.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("rejected %s Add removal persisted an intent: %v", state, err)
			}
		})
	}

	t.Run("publishing subject drift", func(t *testing.T) {
		s := openTest(t)
		now := time.Unix(1_900_000_000, 0)
		s.now = func() time.Time { return now }
		reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("registry-owner"))
		if err != nil {
			t.Fatal(err)
		}
		request := accountMutationTestRequest(t, reservation, AccountMutationAdd, now.Add(time.Minute))
		begin, err := s.BeginAccountMutation(t.Context(), request)
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
		fence, err = s.MarkAccountMutationApplied(fence, credentialOperationTestDigest("written"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.MarkAccountMutationPublishing(fence); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(
			`UPDATE pending_adds SET owner_record=? WHERE id=?`,
			mustEncodeCredentialOwner(credentialOperationTestOwner("foreign-owner")), reservation.ID,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginAccountRemoval(reservation.ID, true); !errors.Is(err, ErrAccountMutationBusy) {
			t.Fatalf("begin removal after pending Add subject drift = %v, want ErrAccountMutationBusy", err)
		}
	})
}

func TestConcurrentSelectionActivationAndRemovalAdmitExactlyOne(t *testing.T) {
	first := openTest(t)
	second := openSecondStore(t, first)
	now := time.Unix(1_900_000_000, 0)
	account := credentialOperationTestAccount(t, first)
	activation := SelectionActivation{
		Token: nextStoreTestToken(), AccountID: account.ID,
		ExpectedInstanceID: account.InstanceID, ExpectedGeneration: account.Generation,
		Process:   ProcessIdentity{PID: 4242, StartedAt: now.Add(-time.Minute)},
		ConfigDir: account.ConfigDir, Cwd: "/project", At: now,
	}
	start := make(chan struct{})
	activationResult := make(chan error, 1)
	removalResult := make(chan error, 1)
	go func() {
		<-start
		activationResult <- first.ActivateSelection(activation)
	}()
	go func() {
		<-start
		_, err := second.BeginAccountRemoval(account.ID, true)
		removalResult <- err
	}()
	close(start)
	activationErr := <-activationResult
	removalErr := <-removalResult

	switch {
	case activationErr == nil:
		if !errors.Is(removalErr, ErrAccountSessionActive) {
			t.Fatalf("activation won, removal = %v, want ErrAccountSessionActive", removalErr)
		}
		if count, err := first.ActiveSessionCount(account.ID); err != nil || count != 1 {
			t.Fatalf("activation winner session count = %d err=%v", count, err)
		}
	case removalErr == nil:
		if !errors.Is(activationErr, ErrAccountRemoving) {
			t.Fatalf("removal won, activation = %v, want ErrAccountRemoving", activationErr)
		}
		if count, err := first.ActiveSessionCount(account.ID); err != nil || count != 0 {
			t.Fatalf("removal winner session count = %d err=%v", count, err)
		}
	default:
		t.Fatalf("neither activation nor removal won: activation=%v removal=%v", activationErr, removalErr)
	}
}

func TestAccountRemovalIntentFencesActiveFleetAndSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool-v1.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 2; id++ {
		if err := s.UpsertAccount(Account{
			ID: id, ConfigDir: fmt.Sprintf("/presentation/acct-%02d", id),
			KeychainService: "service", KeychainAccount: "account", CreatedAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	account, err := s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	removal, err := s.BeginAccountRemoval(1, true)
	if err != nil {
		t.Fatal(err)
	}
	if removal.AccountInstanceID != account.InstanceID || removal.AccountGeneration != account.Generation || !removal.DeleteCredential {
		t.Fatalf("removal = %+v, account = %+v", removal, account)
	}
	activation := SelectionActivation{
		Token: nextStoreTestToken(), AccountID: account.ID,
		ExpectedInstanceID: account.InstanceID, ExpectedGeneration: account.Generation,
		Process:   ProcessIdentity{PID: 4242, StartedAt: time.Now().Add(-time.Minute)},
		ConfigDir: account.ConfigDir,
	}
	if err := s.ActivateSelection(activation); !errors.Is(err, ErrAccountRemoving) {
		t.Fatalf("activate selection after removal = %v, want ErrAccountRemoving", err)
	}
	mutationRequest := existingAccountMutationTestRequest(
		t, account, AccountMutationSyncInstall,
		credentialOperationTestOwner("post-removal-account-mutation"), time.Now().Add(time.Minute),
	)
	if _, err := s.BeginAccountMutation(t.Context(), mutationRequest); !errors.Is(err, ErrAccountRemoving) {
		t.Fatalf("begin account mutation after removal = %v, want ErrAccountRemoving", err)
	}
	credentialRequest := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetAll,
		credentialOperationTestState("before", ""), "post-removal-credential-operation",
		credentialOperationTestOwner("post-removal-credential-operation"),
	)
	if _, err := s.BeginCredentialOperation(credentialRequest); !errors.Is(err, ErrAccountRemoving) {
		t.Fatalf("begin credential operation after removal = %v, want ErrAccountRemoving", err)
	}
	active, err := s.ListActiveAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != 2 {
		t.Fatalf("active accounts = %+v, want acct-02 only", active)
	}
	if _, err := s.BeginAccountRemoval(1, false); err == nil {
		t.Fatal("conflicting removal intent succeeded")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	removals, err := allAccountRemovals(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(removals) != 1 || removals[0] != removal {
		t.Fatalf("removals = %+v, want %+v", removals, removal)
	}
	if err := s.DeleteAccount(1); err != nil {
		t.Fatal(err)
	}
	removals, err = allAccountRemovals(t, s)
	if err != nil || len(removals) != 0 {
		t.Fatalf("removals after delete = %+v, err=%v", removals, err)
	}
}

func allAccountRemovals(t *testing.T, s *Store) ([]AccountRemoval, error) {
	t.Helper()
	var removals []AccountRemoval
	for after := 0; ; {
		page, err := s.PageAccountRemovals(t.Context(), after, AccountRemovalPageLimit)
		if err != nil {
			return nil, err
		}
		removals = append(removals, page.Removals...)
		if page.Next == 0 {
			return removals, nil
		}
		after = page.Next
	}
}
