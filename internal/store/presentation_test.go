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
