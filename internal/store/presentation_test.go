package store

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestListActiveAccountsExcludesAccountWithoutExactPresentation(t *testing.T) {
	s := openTest(t)
	account := insertDesiredPresentationTestAccount(t, s, 1)
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("active accounts without presentation = %+v err=%v", active, err)
	}
	if err := s.ObserveAccountPresentation(account, presentationTestIdentity(account, account.ConfigDir)); !errors.Is(err, ErrAccountPresentationEvidence) {
		t.Fatalf("first observation = %v, want presentation evidence error", err)
	}
}

func TestBindDesiredAccountPresentationIsLostResponseAndRestartIdempotent(t *testing.T) {
	s := openTest(t)
	account := insertDesiredPresentationTestAccount(t, s, 1)
	identity := presentationTestIdentity(account, account.ConfigDir)
	if err := s.BindDesiredAccountPresentation(account, identity); err != nil {
		t.Fatal(err)
	}
	if err := s.BindDesiredAccountPresentation(account, identity); err != nil {
		t.Fatalf("lost-response replay: %v", err)
	}
	bound, err := s.AccountPresentation(account.ID)
	if err != nil || bound.Identity != identity {
		t.Fatalf("binding = %+v err=%v, want %+v", bound, err, identity)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 1 ||
		active[0].ID != account.ID || active[0].InstanceID != account.InstanceID ||
		active[0].Generation != account.Generation || active[0].ConfigDir != account.ConfigDir {
		t.Fatalf("active accounts = %+v err=%v, want %+v", active, err, account)
	}
}

func TestBindDesiredAccountPresentationRejectsIdentityMismatch(t *testing.T) {
	tests := map[string]func(*FileProviderPresentationIdentity){
		"tenant":     func(identity *FileProviderPresentationIdentity) { identity.TenantID = "account-other" },
		"domain":     func(identity *FileProviderPresentationIdentity) { identity.DomainID = "" },
		"generation": func(identity *FileProviderPresentationIdentity) { identity.Generation++ },
		"public path": func(identity *FileProviderPresentationIdentity) {
			identity.PublicPath += "-other"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			account := insertDesiredPresentationTestAccount(t, s, 1)
			identity := presentationTestIdentity(account, account.ConfigDir)
			mutate(&identity)
			if err := s.BindDesiredAccountPresentation(account, identity); !errors.Is(err, ErrAccountPresentationEvidence) {
				t.Fatalf("bind mismatch = %v, want evidence error", err)
			}
		})
	}
}

func TestObserveAccountPresentationValidatesBoundIdentity(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccountID(t, s, 1)
	identity := presentationTestIdentity(account, account.ConfigDir)
	if err := s.ObserveAccountPresentation(account, identity); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("matching observation quarantined: %v", err)
	}
	drifted := identity
	drifted.DomainID += "-other"
	if err := s.ObserveAccountPresentation(account, drifted); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatalf("drifted observation = %v, want quarantine", err)
	}
	quarantine, err := s.AccountPresentationQuarantine(account.ID)
	if err != nil || quarantine.Observed != drifted || quarantine.Reason != AccountPresentationDomainIDDrift {
		t.Fatalf("quarantine = %+v err=%v", quarantine, err)
	}
}

func TestPendingSyncedAdmissionFailsClosedForSelectionAndOriginPublication(t *testing.T) {
	s := openTest(t)
	account := admitTestAccount(t, s, Account{
		ID: 1, ConfigDir: "/presentation/acct-01",
		KeychainService: "service-1", KeychainAccount: "account-1",
	})
	presentation, err := s.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetNeedsLogin(
		account.ID, time.Now(), AuthReasonAwaitingOrigin,
		DigestReason("pending synced admission"), AuthKindAwaitingOrigin,
	); err != nil {
		t.Fatal(err)
	}
	fence := SyncedCredentialAdmissionFence{
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest:       CredentialKeychainLocatorDigest(account.KeychainService, account.KeychainAccount),
		ExternalStateDigest: credentialOperationTestDigest("pending-external"),
		TokenChainDigest:    credentialOperationTestDigest("pending-token-chain"),
		AccessHashDigest:    credentialOperationTestDigest("pending-access"),
	}
	if _, err := s.StageSyncedAccountAdmission(account, presentation.Identity, presentation.Identity, fence); err != nil {
		t.Fatal(err)
	}
	request := credentialOperationTestRequest(
		t, account, CredentialOperationEnsureFresh, CredentialTargetKeychain,
		credentialOperationTestState("pending-admission", ""),
		"pending-admission", credentialOperationTestOwner("pending-admission"),
	)
	if begin, err := s.BeginCredentialOperation(request); !errors.Is(err, ErrAwaitingOriginAdmission) || begin.Active != nil || begin.Receipt != nil {
		t.Fatalf("credential mutation with pending admission = %+v err=%v", begin, err)
	}
	if _, err := s.db.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, reason='none',
		 digest=zeroblob(32), kind='owned' WHERE account_id=?`, account.ID,
	); err != nil {
		t.Fatal(err)
	}
	if err := s.SelectionEligible(account); !errors.Is(err, ErrAccountSelectionIneligible) {
		t.Fatalf("selection with pending admission = %v", err)
	}
	if origins, err := s.ListPublishableOrigins(); err != nil || len(origins) != 0 {
		t.Fatalf("publishable origins with pending admission = %+v err=%v", origins, err)
	}
}

func insertDesiredPresentationTestAccount(t *testing.T, s *Store, id int) Account {
	t.Helper()
	account := Account{
		ID: id, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 1,
		ConfigDir: "/tmp/account-1", KeychainService: "service-1",
		KeychainAccount: "account-1", AccountUUID: "uuid-1",
	}
	if _, err := s.db.Exec(
		`INSERT INTO accounts(id,instance_id,generation,config_dir,keychain_service,keychain_account,account_uuid,created_at)
		 VALUES(?,?,?,?,?,?,?,1)`,
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount, account.AccountUUID,
	); err != nil {
		t.Fatal(err)
	}
	return account
}

func presentationTestIdentity(account Account, publicPath string) FileProviderPresentationIdentity {
	return FileProviderPresentationIdentity{
		TenantID:   "account-" + account.InstanceID,
		DomainID:   "cc-pool-account-" + account.InstanceID,
		Generation: account.Generation,
		PublicPath: publicPath,
	}
}

func presentationTestProof(account Account, publicPath, _ string) FileProviderPresentationIdentity {
	return presentationTestIdentity(account, publicPath)
}
