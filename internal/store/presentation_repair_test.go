package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestAccountPresentationRepairPreservesAccountIdentity(t *testing.T) {
	s := openTest(t)
	account := insertDesiredPresentationTestAccount(t, s, 1)
	previous := presentationTestIdentity(account, "/presentation/previous")
	if err := s.BindDesiredAccountPresentation(account, previous); err != nil {
		t.Fatal(err)
	}
	target := previous
	target.PublicPath = "/presentation/target"
	if err := s.ObserveAccountPresentation(account, target); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatalf("observe target = %v", err)
	}
	repair, err := s.StageAccountPresentationRepair(account, target)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.StageAccountPresentationRepair(account, target)
	if err != nil || !sameAccountPresentationRepair(replayed, repair) {
		t.Fatalf("stage replay = %+v err=%v", replayed, err)
	}
	before, err := s.GetAccount(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := s.CommitAccountPresentationRepair(repair)
	if err != nil {
		t.Fatal(err)
	}
	after, err := s.GetAccount(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before != after || after.InstanceID != account.InstanceID || after.Generation != account.Generation ||
		after.ConfigDir != account.ConfigDir || after.KeychainService != account.KeychainService ||
		after.KeychainAccount != account.KeychainAccount || after.AccountUUID != account.AccountUUID {
		t.Fatalf("account identity changed: before=%+v after=%+v", before, after)
	}
	if committed.Identity != target || committed.AccountGeneration != account.Generation {
		t.Fatalf("committed presentation = %+v", committed)
	}
	if _, err := s.AccountPresentationRepair(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("repair remains: %v", err)
	}
	if _, err := s.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantine remains: %v", err)
	}
	replayedCommit, err := s.CommitAccountPresentationRepair(repair)
	if err != nil || replayedCommit.Identity != target {
		t.Fatalf("commit replay = %+v err=%v", replayedCommit, err)
	}
}

func TestPendingAccountPresentationRepairSurvivesRestart(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "pool-v1.db")
	s, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	account := insertDesiredPresentationTestAccount(t, s, 1)
	previous := presentationTestIdentity(account, "/presentation/previous")
	if err := s.BindDesiredAccountPresentation(account, previous); err != nil {
		t.Fatal(err)
	}
	target := previous
	target.PublicPath = "/presentation/target"
	if err := s.ObserveAccountPresentation(account, target); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatal(err)
	}
	repair, err := s.StageAccountPresentationRepair(account, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	repairs, err := reopened.PendingAccountPresentationRepairs()
	if err != nil || len(repairs) != 1 || !sameAccountPresentationRepair(repairs[0], repair) {
		t.Fatalf("pending repairs = %+v err=%v", repairs, err)
	}
}

func TestAccountPresentationRepairRejectsForeignIdentity(t *testing.T) {
	for name, mutate := range map[string]func(*FileProviderPresentationIdentity){
		"tenant":     func(identity *FileProviderPresentationIdentity) { identity.TenantID += "-foreign" },
		"domain":     func(identity *FileProviderPresentationIdentity) { identity.DomainID += "-foreign" },
		"generation": func(identity *FileProviderPresentationIdentity) { identity.Generation++ },
	} {
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			account := insertDesiredPresentationTestAccount(t, s, 1)
			previous := presentationTestIdentity(account, "/presentation/previous")
			if err := s.BindDesiredAccountPresentation(account, previous); err != nil {
				t.Fatal(err)
			}
			target := previous
			target.PublicPath = "/presentation/target"
			mutate(&target)
			if err := s.ObserveAccountPresentation(account, target); !errors.Is(err, ErrAccountPresentationQuarantined) {
				t.Fatal(err)
			}
			if _, err := s.StageAccountPresentationRepair(account, target); !errors.Is(err, ErrAccountPresentationEvidence) {
				t.Fatalf("stage foreign identity = %v", err)
			}
		})
	}
}

func TestAccountPresentationRepairFencesLiveSessionAndCredentialOperation(t *testing.T) {
	for _, blocker := range []struct {
		name  string
		block func(*testing.T, *Store, Account)
	}{
		{name: "session", block: func(t *testing.T, s *Store, account Account) {
			_, err := s.db.Exec(
				`INSERT INTO sessions(selection_token,account_id,account_instance_id,account_generation,
				 pid,process_started_at,config_dir,started_at,file_provider_lease_state,
				 file_provider_lease_receipt,file_provider_lease_expires_at)
				 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
				"0123456789abcdef0123456789abcdef", account.ID, account.InstanceID,
				account.Generation, 42, 1, account.ConfigDir, 1, "active", []byte("lease"), 2,
			)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "credential operation", block: func(t *testing.T, s *Store, account Account) {
			request := credentialOperationTestRequest(
				t, account, CredentialOperationEnsureFresh, CredentialTargetKeychain,
				credentialOperationTestState("repair-busy", ""), "repair-busy",
				credentialOperationTestOwner("repair-busy"),
			)
			if _, err := s.BeginCredentialOperation(request); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(blocker.name, func(t *testing.T) {
			s := openTest(t)
			account := insertDesiredPresentationTestAccount(t, s, 1)
			previous := presentationTestIdentity(account, "/presentation/previous")
			if err := s.BindDesiredAccountPresentation(account, previous); err != nil {
				t.Fatal(err)
			}
			blocker.block(t, s, account)
			target := previous
			target.PublicPath = "/presentation/target"
			if err := s.ObserveAccountPresentation(account, target); !errors.Is(err, ErrAccountPresentationQuarantined) {
				t.Fatal(err)
			}
			if _, err := s.StageAccountPresentationRepair(account, target); !errors.Is(err, ErrAccountPresentationBusy) {
				t.Fatalf("stage with %s = %v", blocker.name, err)
			}
		})
	}
}
