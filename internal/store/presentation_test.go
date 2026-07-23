package store

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestListActiveAccountsExcludesAccountWithoutExactPresentation(t *testing.T) {
	s := openTest(t)
	account := Account{
		ID: 1, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 1,
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
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("active accounts without presentation = %+v err=%v", active, err)
	}
	proof := presentationTestProof(account, account.ConfigDir, "activation-1")
	if err := s.ObserveAccountPresentation(account, proof); !errors.Is(err, ErrAccountPresentationEvidence) {
		t.Fatalf("first observation = %v, want presentation evidence error", err)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("raw row became active through observation = %+v err=%v", active, err)
	}
}

func TestListDesiredAccountsIncludesUnpresentedExactAccount(t *testing.T) {
	s := openTest(t)
	account := insertDesiredPresentationTestAccount(t, s, 1)
	desired, err := s.ListDesiredAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(desired) != 1 || desired[0] != account {
		t.Fatalf("desired accounts = %+v, want %+v", desired, account)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("active accounts = %+v err=%v, want none", active, err)
	}
}

func TestBindDesiredAccountPresentationIsLostResponseAndRestartIdempotent(t *testing.T) {
	s := openTest(t)
	account := insertDesiredPresentationTestAccount(t, s, 1)
	proof := presentationTestProof(account, account.ConfigDir, "activation-1")
	expected := presentationTestIdentity(proof)
	if err := s.BindDesiredAccountPresentation(account, expected, proof); err != nil {
		t.Fatal(err)
	}
	if err := s.BindDesiredAccountPresentation(account, expected, proof); err != nil {
		t.Fatalf("lost-response replay: %v", err)
	}
	proof.FileProvider.ActivationGeneration = "activation-2"
	if err := s.BindDesiredAccountPresentation(account, expected, proof); err != nil {
		t.Fatalf("restart activation refresh: %v", err)
	}
	bound, err := s.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Proof != proof {
		t.Fatalf("bound proof = %+v, want %+v", bound.Proof, proof)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 1 || active[0] != account {
		t.Fatalf("active accounts = %+v err=%v, want %+v", active, err, account)
	}
}

func TestBindDesiredAccountPresentationQuarantinesIdentityMismatch(t *testing.T) {
	tests := map[string]struct {
		mutate func(*PresentationPreparationProof)
		reason AccountPresentationQuarantineReason
	}{
		"tenant": {func(p *PresentationPreparationProof) {
			p.CatalogTenantID = "account-other"
			p.FileProvider.TenantID = "account-other"
		}, AccountPresentationTenantIDDrift},
		"domain": {func(p *PresentationPreparationProof) {
			p.FileProvider.DomainID = "domain-other"
		}, AccountPresentationDomainIDDrift},
		"generation": {func(p *PresentationPreparationProof) {
			p.CatalogGeneration++
			p.FileProvider.Generation++
		}, AccountPresentationGenerationDrift},
		"public path": {func(p *PresentationPreparationProof) {
			p.FileProvider.PublicPath += "-other"
		}, AccountPresentationPublicPathDrift},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			account := insertDesiredPresentationTestAccount(t, s, 1)
			proof := presentationTestProof(account, account.ConfigDir, "activation-1")
			expected := presentationTestIdentity(proof)
			test.mutate(&proof)
			if err := s.BindDesiredAccountPresentation(account, expected, proof); !errors.Is(err, ErrAccountPresentationQuarantined) {
				t.Fatalf("bind mismatch = %v, want quarantine", err)
			}
			quarantine, err := s.AccountPresentationQuarantine(account.ID)
			if err != nil || quarantine.Reason != test.reason || quarantine.Proof != proof {
				t.Fatalf("quarantine = %+v err=%v, want reason %s", quarantine, err, test.reason)
			}
			if desired, err := s.ListDesiredAccounts(); err != nil || len(desired) != 0 {
				t.Fatalf("desired after quarantine = %+v err=%v", desired, err)
			}
			if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
				t.Fatalf("active after quarantine = %+v err=%v", active, err)
			}
		})
	}
}

func TestObserveAccountPresentationBindsExactEvidenceAndRefreshesActivation(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccountID(t, s, 1)
	proof := presentationTestProof(account, account.ConfigDir, "activation-1")
	if err := s.ObserveAccountPresentation(account, proof); err != nil {
		t.Fatal(err)
	}
	proof.FileProvider.ActivationGeneration = "activation-2"
	if err := s.ObserveAccountPresentation(account, proof); err != nil {
		t.Fatal(err)
	}
	bound, err := s.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if bound.AccountInstanceID != account.InstanceID ||
		bound.AccountGeneration != account.Generation || bound.Proof != proof {
		t.Fatalf("binding = %+v, want proof %+v", bound, proof)
	}
	if _, err := s.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("matching observation quarantined: %v", err)
	}
}

func TestPendingSyncedAdmissionFailsClosedForSelectionAndOriginPublication(t *testing.T) {
	s := openTest(t)
	account := admitTestAccount(t, s, Account{
		ID: 1, ConfigDir: "/presentation/acct-01",
		KeychainService: "service-1", KeychainAccount: "account-1",
	})
	bound, err := s.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	fresh := bound.Proof
	fresh.FileProvider.ActivationGeneration = "pending-admission"
	if _, err := s.SetNeedsLogin(
		account.ID, time.Now(), AuthReasonAwaitingOrigin,
		DigestReason("pending synced admission"), AuthKindAwaitingOrigin,
	); err != nil {
		t.Fatal(err)
	}
	fence := SyncedCredentialAdmissionFence{
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest: CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		),
		ExternalStateDigest: credentialOperationTestDigest("pending-external"),
		TokenChainDigest:    credentialOperationTestDigest("pending-token-chain"),
		AccessHashDigest:    credentialOperationTestDigest("pending-access"),
	}
	if _, err := s.StageSyncedAccountAdmission(account, bound.Proof, fresh, fence); err != nil {
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
		 digest=zeroblob(32), kind='owned' WHERE account_id=?`,
		account.ID,
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

func TestObserveAccountPresentationQuarantinesFullPathDriftEvidence(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccountID(t, s, 1)
	initial := presentationTestProof(account, account.ConfigDir, "activation-1")
	if err := s.ObserveAccountPresentation(account, initial); err != nil {
		t.Fatal(err)
	}
	drifted := presentationTestProof(account, "/File Provider/CCPool/drifted", "activation-2")
	if err := s.ObserveAccountPresentation(account, drifted); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatalf("drift observation = %v", err)
	}
	quarantine, err := s.AccountPresentationQuarantine(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if quarantine.AccountInstanceID != account.InstanceID ||
		quarantine.AccountGeneration != account.Generation ||
		quarantine.ExpectedConfigDir != account.ConfigDir ||
		quarantine.Proof != drifted || quarantine.Reason != AccountPresentationPublicPathDrift {
		t.Fatalf("quarantine = %+v", quarantine)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 0 {
		t.Fatalf("active accounts after quarantine = %+v err=%v", active, err)
	}
	if err := s.ObserveAccountPresentation(account, initial); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatalf("matching proof silently cleared quarantine: %v", err)
	}
}

func TestObserveAccountPresentationQuarantinesIdentityAndGenerationDrift(t *testing.T) {
	for name, test := range map[string]struct {
		mutate func(*PresentationPreparationProof)
		reason AccountPresentationQuarantineReason
	}{
		"tenant": {func(p *PresentationPreparationProof) {
			p.CatalogTenantID = "account-other"
			p.FileProvider.TenantID = "account-other"
		}, AccountPresentationTenantIDDrift},
		"domain": {func(p *PresentationPreparationProof) {
			p.FileProvider.DomainID = "domain-other"
		}, AccountPresentationDomainIDDrift},
		"generation": {func(p *PresentationPreparationProof) {
			p.CatalogGeneration++
			p.FileProvider.Generation++
		}, AccountPresentationGenerationDrift},
	} {
		t.Run(name, func(t *testing.T) {
			s := openTest(t)
			account := credentialOperationTestAccountID(t, s, 1)
			initial := presentationTestProof(account, account.ConfigDir, "activation-1")
			if err := s.ObserveAccountPresentation(account, initial); err != nil {
				t.Fatal(err)
			}
			test.mutate(&initial)
			if err := s.ObserveAccountPresentation(account, initial); !errors.Is(err, ErrAccountPresentationQuarantined) {
				t.Fatalf("observation = %v", err)
			}
			quarantine, err := s.AccountPresentationQuarantine(account.ID)
			if err != nil || quarantine.Reason != test.reason {
				t.Fatalf("quarantine = %+v err=%v, want %s", quarantine, err, test.reason)
			}
		})
	}
}

func TestValidatePresentationPreparationProofAdvance(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccountID(t, s, 1)
	current := presentationTestProof(account, account.ConfigDir, "activation-1")

	activation := current
	activation.FileProvider.ActivationGeneration = "activation-2"
	if err := ValidatePresentationPreparationProofAdvance(current, activation); err != nil {
		t.Fatalf("activation refresh: %v", err)
	}
	advanced := activation
	advanced.Requested++
	advanced.Desired, advanced.Observed = advanced.Requested, advanced.Requested
	advanced.Verified, advanced.Applied = advanced.Requested, advanced.Requested
	advanced.CatalogRevision = advanced.Requested
	advanced.SourceRevision++
	advanced.ChangeID, advanced.OperationID = "change-advanced", "operation-advanced"
	if err := ValidatePresentationPreparationProofAdvance(current, advanced); err != nil {
		t.Fatalf("revision advance: %v", err)
	}

	tests := map[string]func(*PresentationPreparationProof){
		"public path": func(p *PresentationPreparationProof) { p.FileProvider.PublicPath += "-other" },
		"tenant": func(p *PresentationPreparationProof) {
			p.CatalogTenantID, p.FileProvider.TenantID = "account-other", "account-other"
		},
		"domain":    func(p *PresentationPreparationProof) { p.FileProvider.DomainID = "domain-other" },
		"authority": func(p *PresentationPreparationProof) { p.SourceAuthority = "source-other" },
		"rollback": func(p *PresentationPreparationProof) {
			p.Requested--
			p.Desired, p.Observed, p.Verified, p.Applied = p.Requested, p.Requested, p.Requested, p.Requested
			p.CatalogRevision = p.Requested
		},
		"causal drift": func(p *PresentationPreparationProof) { p.ChangeID = "change-other" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			next := current
			mutate(&next)
			if err := ValidatePresentationPreparationProofAdvance(current, next); !errors.Is(err, ErrAccountPresentationEvidence) {
				t.Fatalf("validation = %v, want presentation evidence error", err)
			}
		})
	}
}

func presentationTestProof(account Account, publicPath, activation string) PresentationPreparationProof {
	return PresentationPreparationProof{
		CatalogTenantID: "account-" + account.InstanceID, CatalogGeneration: account.Generation,
		Requested: 7, Desired: 7, Observed: 7, Verified: 7, Applied: 7,
		SourceAuthority: "source-authority", SourceRevision: 5, CatalogRevision: 7,
		ChangeID: "change-id", OperationID: "operation-id",
		PresentationKind: PresentationKindFileProvider,
		FileProvider: FileProviderPreparationProof{
			TenantID: "account-" + account.InstanceID, DomainID: "domain-" + account.InstanceID,
			Generation: account.Generation, ActivationGeneration: activation, PublicPath: publicPath,
		},
	}
}

func presentationTestIdentity(proof PresentationPreparationProof) FileProviderPresentationIdentity {
	return FileProviderPresentationIdentity{
		TenantID: proof.FileProvider.TenantID, DomainID: proof.FileProvider.DomainID,
		Generation: proof.FileProvider.Generation, PublicPath: proof.FileProvider.PublicPath,
	}
}

func insertDesiredPresentationTestAccount(t *testing.T, s *Store, id int) Account {
	t.Helper()
	account := Account{
		ID: id, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 1,
		ConfigDir: "/tmp/account-1", KeychainService: "service-1",
		KeychainAccount: "account-1", AccountUUID: "uuid-1", CreatedAt: time.Unix(1, 0),
	}
	if _, err := s.db.Exec(
		`INSERT INTO accounts(id,instance_id,generation,config_dir,keychain_service,keychain_account,account_uuid,created_at)
		 VALUES(?,?,?,?,?,?,?,?)`,
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount, account.AccountUUID, account.CreatedAt.Unix(),
	); err != nil {
		t.Fatal(err)
	}
	return account
}
