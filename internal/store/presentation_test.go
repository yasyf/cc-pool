package store

import (
	"database/sql"
	"errors"
	"testing"
)

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

func TestRebindAccountPresentationBumpsGenerationAndRequiresQuarantine(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccountID(t, s, 1)
	initial := presentationTestProof(account, account.ConfigDir, "activation-1")
	if err := s.ObserveAccountPresentation(account, initial); err != nil {
		t.Fatal(err)
	}
	newPath := "/File Provider/CCPool/rebound"
	drifted := presentationTestProof(account, newPath, "activation-2")
	if err := s.ObserveAccountPresentation(account, drifted); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatalf("drift observation = %v", err)
	}
	proof := drifted
	proof.CatalogGeneration = account.Generation + 1
	proof.FileProvider.Generation = account.Generation + 1
	updated, err := s.RebindAccountPresentation(
		account, proof, "new-keychain-service", "new-keychain-account",
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated.InstanceID != account.InstanceID || updated.Generation != account.Generation+1 ||
		updated.ConfigDir != newPath || updated.KeychainService != "new-keychain-service" ||
		updated.KeychainAccount != "new-keychain-account" {
		t.Fatalf("updated account = %+v", updated)
	}
	bound, err := s.AccountPresentation(account.ID)
	if err != nil || bound.AccountGeneration != updated.Generation || bound.Proof != proof {
		t.Fatalf("rebound evidence = %+v err=%v", bound, err)
	}
	if _, err := s.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rebind retained quarantine: %v", err)
	}
	if active, err := s.ListActiveAccounts(); err != nil || len(active) != 1 || active[0].Generation != updated.Generation {
		t.Fatalf("active accounts after rebind = %+v err=%v", active, err)
	}
}

func TestRebindAccountPresentationRejectsActiveMutation(t *testing.T) {
	s := openTest(t)
	account := credentialOperationTestAccountID(t, s, 1)
	initial := presentationTestProof(account, account.ConfigDir, "activation-1")
	if err := s.ObserveAccountPresentation(account, initial); err != nil {
		t.Fatal(err)
	}
	request := existingAccountMutationTestRequest(
		t, account, AccountMutationRelogin, credentialOperationTestOwner("rebind-owner"),
	)
	if _, err := s.BeginAccountMutation(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	drifted := presentationTestProof(account, "/File Provider/CCPool/rebound", "activation-2")
	if err := s.ObserveAccountPresentation(account, drifted); !errors.Is(err, ErrAccountPresentationQuarantined) {
		t.Fatal(err)
	}
	drifted.CatalogGeneration++
	drifted.FileProvider.Generation++
	if _, err := s.RebindAccountPresentation(
		account, drifted, "new-service", "new-account",
	); !errors.Is(err, ErrAccountPresentationBusy) {
		t.Fatalf("rebind with active mutation = %v", err)
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
