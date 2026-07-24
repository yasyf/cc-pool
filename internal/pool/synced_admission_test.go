package pool

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestPromoteSyncedAddLostResponseResolvesWithoutCleanup(t *testing.T) {
	manager := newAccountManager(t)
	reservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "CloudStorage", "cc-pool-acct-01")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	prospective := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID,
		Generation: reservation.Generation, ConfigDir: configDir,
	}
	pending, err := manager.PrepareReservedSyncedAdd(
		t.Context(), reservation, syncedAdmissionProof(prospective, "activation-promotion"),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantConfigDir, err := AccountConfigDir(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	wantService, err := AccountKeychainService(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.ConfigDir != wantConfigDir || pending.PublicPath != configDir ||
		pending.KeychainService != wantService {
		t.Fatalf("synced pending identity = %+v", pending)
	}
	assertLinkTarget(t, pending.ConfigDir, configDir)
	identity := json.RawMessage(`{"accountUuid":"promotion-uuid"}`)
	if err := manager.WriteIdentity(t.Context(), reservation.ID, configDir, identity); err != nil {
		t.Fatal(err)
	}
	promoteSyncedAddFailpoint = func(checkpoint string) error {
		if checkpoint == "after-commit" {
			return errors.New("injected post-commit response loss")
		}
		return nil
	}
	t.Cleanup(func() { promoteSyncedAddFailpoint = nil })
	if account, err := manager.PromoteSyncedAdd(
		t.Context(), pending, "peer", "promotion-uuid",
	); account != nil || err == nil {
		t.Fatalf("lost response account=%+v err=%v", account, err)
	}
	resolved, exact, err := manager.ResolvePromotedSyncedAdd(
		pending, "peer", "promotion-uuid",
	)
	if err != nil || !exact || resolved == nil || resolved.AccountUUID != "promotion-uuid" {
		t.Fatalf("resolved account=%+v exact=%v err=%v", resolved, exact, err)
	}
	if _, err := os.Stat(AccountBackingDir(reservation.ID)); err != nil {
		t.Fatalf("committed backing missing after response loss: %v", err)
	}
}

func TestAdmitSyncedCredentialRejectsCredentialChangedUnderfoot(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*credstest.Fake, store.Account, *creds.Credential)
	}{
		{name: "deleted", mutate: func(fake *credstest.Fake, account store.Account, _ *creds.Credential) {
			fake.Remove(account.KeychainService, account.KeychainAccount)
		}},
		{name: "refresh-bearing", mutate: func(fake *credstest.Fake, account store.Account, credential *creds.Credential) {
			changed := *credential
			changed.ClaudeAiOauth.RefreshToken = "refresh-underfoot"
			fake.Put(account.KeychainService, account.KeychainAccount, &changed)
		}},
		{name: "access-changed", mutate: func(fake *credstest.Fake, account store.Account, credential *creds.Credential) {
			changed := *credential
			changed.ClaudeAiOauth.AccessToken = "access-underfoot"
			fake.Put(account.KeychainService, account.KeychainAccount, &changed)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			manager, fake, account, currentProof, freshProof, credential, _ := syncedAdmissionFixture(t)
			syncedAdmissionFailpoint = func(checkpoint string) {
				if checkpoint == "before-final-observation" {
					mutation.mutate(fake, account, credential)
				}
			}
			t.Cleanup(func() { syncedAdmissionFailpoint = nil })
			admitted, err := manager.AdmitSyncedCredential(
				t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
			)
			if admitted || !errors.Is(err, ErrCredentialChangedUnderfoot) {
				t.Fatalf("admit changed credential = %v err=%v", admitted, err)
			}
			assertSyncedAdmissionPending(t, manager.Store, account, currentProof)
		})
	}
}

func TestAdmitSyncedCredentialPersistsExactEvidenceAndRetriesAfterReopen(t *testing.T) {
	manager, fake, account, currentProof, freshProof, credential, databasePath := syncedAdmissionFixture(t)
	syncedAdmissionFailpoint = func(checkpoint string) {
		if checkpoint == "before-final-observation" {
			fake.Remove(account.KeychainService, account.KeychainAccount)
		}
	}
	t.Cleanup(func() { syncedAdmissionFailpoint = nil })
	admitted, err := manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	syncedAdmissionFailpoint = nil
	if admitted || !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("first admission = %v err=%v", admitted, err)
	}
	assertSyncedAdmissionPending(t, manager.Store, account, currentProof)

	if err := manager.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	manager.Store = reopened
	fake.Put(account.KeychainService, account.KeychainAccount, credential)
	admitted, err = manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if err != nil || !admitted {
		t.Fatalf("retry after reopen = %v err=%v", admitted, err)
	}
	health, err := reopened.GetAuthHealth(account.ID)
	if err != nil || health.NeedsLogin || health.Kind != store.AuthKindOwned {
		t.Fatalf("admitted health = %+v err=%v", health, err)
	}
	presentation, err := reopened.AccountPresentation(account.ID)
	if err != nil || presentation.Identity != freshProof {
		t.Fatalf("admitted proof = %+v err=%v", presentation, err)
	}
	evidence, err := reopened.SyncedCredentialAdmission(account)
	if err != nil || evidence.AccountID != account.ID || evidence.AccessHashDigest == ([32]byte{}) {
		t.Fatalf("admission evidence = %+v err=%v", evidence, err)
	}
	firstAccessDigest := evidence.AccessHashDigest
	if _, err := reopened.SetNeedsLogin(
		account.ID, time.Now(), store.AuthReasonAwaitingOrigin,
		store.DigestReason("origin rotated"), store.AuthKindAwaitingOrigin,
	); err != nil {
		t.Fatal(err)
	}
	rotated := *credential
	rotated.ClaudeAiOauth.AccessToken = "synced-access-rotated"
	fake.Put(account.KeychainService, account.KeychainAccount, &rotated)
	nextProof := freshProof
	admitted, err = manager.AdmitSyncedCredential(
		t.Context(), account, freshProof, nextProof, creds.AccessHash(&rotated),
	)
	if err != nil || !admitted {
		t.Fatalf("same-generation re-admission = %v err=%v", admitted, err)
	}
	evidence, err = reopened.SyncedCredentialAdmission(account)
	if err != nil || evidence.AccessHashDigest == firstAccessDigest {
		t.Fatalf("rotated admission evidence = %+v err=%v", evidence, err)
	}
	if err := reopened.DeleteAccount(account.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.SyncedCredentialAdmission(account); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("admission evidence survived account teardown: %v", err)
	}
}

func TestAdmitSyncedCredentialRetainsLiabilityAcrossExternalReplacementWindows(t *testing.T) {
	for _, checkpoint := range []string{
		"after-stage",
		"after-post-stage-observation",
		"after-candidate",
		"before-settle-observation",
	} {
		t.Run(checkpoint, func(t *testing.T) {
			manager, fake, account, currentProof, freshProof, credential, _ := syncedAdmissionFixture(t)
			var eligibilityErr error
			var origins []store.Account
			var originsErr error
			syncedAdmissionFailpoint = func(got string) {
				if got != checkpoint {
					return
				}
				if checkpoint == "after-candidate" {
					eligibilityErr = manager.Store.SelectionEligible(account)
					origins, originsErr = manager.Store.ListPublishableOrigins()
				}
				replacement := *credential
				replacement.ClaudeAiOauth.AccessToken = "replacement-" + checkpoint
				replacement.ClaudeAiOauth.RefreshToken = "owned-" + checkpoint
				fake.Put(account.KeychainService, account.KeychainAccount, &replacement)
			}
			t.Cleanup(func() { syncedAdmissionFailpoint = nil })
			admitted, err := manager.AdmitSyncedCredential(
				t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
			)
			syncedAdmissionFailpoint = nil
			if admitted || !errors.Is(err, ErrCredentialChangedUnderfoot) {
				t.Fatalf("replacement admission = %v err=%v", admitted, err)
			}
			if checkpoint == "after-candidate" {
				if !errors.Is(eligibilityErr, store.ErrAccountSelectionIneligible) {
					t.Fatalf("pre-PID selection at candidate = %v", eligibilityErr)
				}
				if originsErr != nil || len(origins) != 0 {
					t.Fatalf("origin publication at candidate = %+v err=%v", origins, originsErr)
				}
			}
			assertSyncedAdmissionLiability(t, manager.Store, account, freshProof)
		})
	}
}

func TestAdmitSyncedCredentialRecoversLostStageCandidateAndSettleResponses(t *testing.T) {
	for _, checkpoint := range []string{"after-stage", "after-candidate", "after-settle"} {
		t.Run(checkpoint, func(t *testing.T) {
			manager, _, account, currentProof, freshProof, credential, databasePath := syncedAdmissionFixture(t)
			lost := errors.New("injected " + checkpoint + " response loss")
			syncedAdmissionResultFailpoint = func(got string) error {
				if got == checkpoint {
					return lost
				}
				return nil
			}
			t.Cleanup(func() { syncedAdmissionResultFailpoint = nil })
			admitted, err := manager.AdmitSyncedCredential(
				t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
			)
			if admitted || !errors.Is(err, lost) {
				t.Fatalf("lost response admission = %v err=%v", admitted, err)
			}
			switch checkpoint {
			case "after-stage":
				assertSyncedAdmissionLiability(t, manager.Store, account, freshProof)
			case "after-candidate":
				assertSyncedAdmissionCandidate(t, manager.Store, account, freshProof)
			case "after-settle":
				assertSyncedAdmissionFinal(t, manager.Store, account, freshProof)
			}
			if err := manager.Store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			manager.Store = reopened
			syncedAdmissionResultFailpoint = nil
			admitted, err = manager.AdmitSyncedCredential(
				t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
			)
			if err != nil || !admitted {
				t.Fatalf("retry after %s restart = %v err=%v", checkpoint, admitted, err)
			}
			assertSyncedAdmissionFinal(t, reopened, account, freshProof)
		})
	}
}

func TestAdmitSyncedCredentialReopensCandidateAfterReplacementAndLostResponse(t *testing.T) {
	manager, fake, account, currentProof, freshProof, credential, databasePath := syncedAdmissionFixture(t)
	lost := errors.New("injected candidate response loss")
	syncedAdmissionFailpoint = func(got string) {
		if got != "after-candidate" {
			return
		}
		replacement := *credential
		replacement.ClaudeAiOauth.AccessToken = "replacement-after-candidate-loss"
		replacement.ClaudeAiOauth.RefreshToken = "owned-after-candidate-loss"
		fake.Put(account.KeychainService, account.KeychainAccount, &replacement)
	}
	syncedAdmissionResultFailpoint = func(got string) error {
		if got == "after-candidate" {
			return lost
		}
		return nil
	}
	t.Cleanup(func() {
		syncedAdmissionFailpoint = nil
		syncedAdmissionResultFailpoint = nil
	})
	admitted, err := manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if admitted || !errors.Is(err, lost) {
		t.Fatalf("lost candidate response admission = %v err=%v", admitted, err)
	}
	assertSyncedAdmissionCandidate(t, manager.Store, account, freshProof)
	if err := manager.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	manager.Store = reopened
	syncedAdmissionFailpoint = nil
	syncedAdmissionResultFailpoint = nil
	admitted, err = manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if admitted || !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("replacement recovery admission = %v err=%v", admitted, err)
	}
	assertSyncedAdmissionLiability(t, reopened, account, freshProof)
}

func TestAdmitSyncedCredentialInvalidatesSettledEvidenceAfterReplacementAndLostResponse(t *testing.T) {
	manager, fake, account, currentProof, freshProof, credential, databasePath := syncedAdmissionFixture(t)
	lost := errors.New("injected settle response loss")
	syncedAdmissionFailpoint = func(got string) {
		if got != "after-settle" {
			return
		}
		replacement := *credential
		replacement.ClaudeAiOauth.AccessToken = "replacement-after-settle-loss"
		replacement.ClaudeAiOauth.RefreshToken = "owned-after-settle-loss"
		fake.Put(account.KeychainService, account.KeychainAccount, &replacement)
	}
	syncedAdmissionResultFailpoint = func(got string) error {
		if got == "after-settle" {
			return lost
		}
		return nil
	}
	t.Cleanup(func() {
		syncedAdmissionFailpoint = nil
		syncedAdmissionResultFailpoint = nil
	})
	admitted, err := manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if admitted || !errors.Is(err, lost) {
		t.Fatalf("lost settle response admission = %v err=%v", admitted, err)
	}
	assertSyncedAdmissionFinal(t, manager.Store, account, freshProof)
	activatePoolTestSession(t, manager, account.ID, os.Getpid(), "", time.Now())
	if err := manager.Store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	manager.Store = reopened
	syncedAdmissionFailpoint = nil
	syncedAdmissionResultFailpoint = nil
	admitted, err = manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if admitted || !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("settled replacement recovery = %v err=%v", admitted, err)
	}
	assertSyncedAdmissionLiability(t, reopened, account, freshProof)
	if active, err := reopened.ActiveSessionCount(account.ID); err != nil || active != 1 {
		t.Fatalf("existing session after drift reconciliation = %d err=%v", active, err)
	}
}

func TestAdmitSyncedCredentialRefusesActiveCredentialOperation(t *testing.T) {
	manager, _, account, currentProof, freshProof, credential, _ := syncedAdmissionFixture(t)
	observation, err := manager.CredentialExternalState(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	beginCredentialOperation(
		t, manager, account, store.CredentialOperationAdoptRotated,
		store.CredentialTargetKeychain,
		credentialIntentDigest(store.CredentialOperationAdoptRotated, "admission-race"),
		observation,
	)
	admitted, err := manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if admitted || !errors.Is(err, store.ErrAccountPresentationBusy) {
		t.Fatalf("admit with active credential operation = %v err=%v", admitted, err)
	}
	assertSyncedAdmissionPending(t, manager.Store, account, currentProof)
}

func TestAdmitSyncedCredentialRefusesLiveSession(t *testing.T) {
	manager, _, account, currentProof, freshProof, credential, _ := syncedAdmissionFixture(t)
	admitted, err := manager.AdmitSyncedCredential(
		t.Context(), account, currentProof, freshProof, creds.AccessHash(credential),
	)
	if err != nil || !admitted {
		t.Fatalf("establish selectable admission = %v err=%v", admitted, err)
	}
	activatePoolTestSession(t, manager, account.ID, os.Getpid(), "", time.Now())
	if _, err := manager.Store.SetNeedsLogin(
		account.ID, time.Now(), store.AuthReasonAwaitingOrigin,
		store.DigestReason("origin rotated during live session"), store.AuthKindAwaitingOrigin,
	); err != nil {
		t.Fatal(err)
	}
	nextProof := freshProof
	admitted, err = manager.AdmitSyncedCredential(
		t.Context(), account, freshProof, nextProof, creds.AccessHash(credential),
	)
	if admitted || !errors.Is(err, store.ErrAccountSessionActive) {
		t.Fatalf("admit with live session = %v err=%v", admitted, err)
	}
	health, err := manager.Store.GetAuthHealth(account.ID)
	if err != nil || !health.NeedsLogin || health.Kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("live-session admission health = %+v err=%v", health, err)
	}
	presentation, err := manager.Store.AccountPresentation(account.ID)
	if err != nil || presentation.Identity != freshProof {
		t.Fatalf("live-session admission proof = %+v err=%v", presentation, err)
	}
	if _, err := manager.Store.SyncedCredentialAdmission(account); err != nil {
		t.Fatalf("prior exact admission evidence was lost: %v", err)
	}
}

func syncedAdmissionFixture(
	t *testing.T,
) (*Manager, *credstest.Fake, store.Account, store.FileProviderPresentationIdentity, store.FileProviderPresentationIdentity, *creds.Credential, string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	databasePath := filepath.Join(t.TempDir(), "synced-admission-v1.db")
	st, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	fake := credstest.NewFake()
	manager := credentialRecoveryManager(t, st, fake, "synced-admission")
	installTestBackingRunner(manager)
	if _, err := manager.Init(); err != nil {
		t.Fatal(err)
	}
	reservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(home, "Library", "CloudStorage", "cc-pool-acct-01")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	account := store.Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID,
		Generation: reservation.Generation, ConfigDir: configDir,
		KeychainService: "synced-service", KeychainAccount: creds.AccountLabel(),
		Label: "synced", AccountUUID: "synced-uuid", CreatedAt: time.Now(),
	}
	currentProof := syncedAdmissionProof(account, "activation-A")
	if err := st.PromoteReservedSyncedAccount(reservation, account, currentProof); err != nil {
		t.Fatal(err)
	}
	freshProof := currentProof
	credential := &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken: "synced-access", ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	}}
	fake.Put(account.KeychainService, account.KeychainAccount, credential)
	return manager, fake, account, currentProof, freshProof, credential, databasePath
}

func syncedAdmissionProof(
	account store.Account,
	_ string,
) store.FileProviderPresentationIdentity {
	tenantID := "account-" + account.InstanceID
	return store.FileProviderPresentationIdentity{
		TenantID: tenantID, DomainID: "domain-" + account.InstanceID,
		Generation: account.Generation, PublicPath: account.ConfigDir,
	}
}

func assertSyncedAdmissionPending(
	t *testing.T,
	st *store.Store,
	account store.Account,
	wantProof store.FileProviderPresentationIdentity,
) {
	t.Helper()
	health, err := st.GetAuthHealth(account.ID)
	if err != nil || !health.NeedsLogin || health.Kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("pending health = %+v err=%v", health, err)
	}
	presentation, err := st.AccountPresentation(account.ID)
	if err != nil || presentation.Identity != wantProof {
		t.Fatalf("pending proof = %+v err=%v", presentation, err)
	}
	if _, err := st.SyncedCredentialAdmission(account); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("pending admission evidence error = %v, want sql.ErrNoRows", err)
	}
}

func assertSyncedAdmissionLiability(
	t *testing.T,
	st *store.Store,
	account store.Account,
	wantProof store.FileProviderPresentationIdentity,
) {
	t.Helper()
	health, err := st.GetAuthHealth(account.ID)
	if err != nil || !health.NeedsLogin || health.Kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("pending liability health = %+v err=%v", health, err)
	}
	presentation, err := st.AccountPresentation(account.ID)
	if err != nil || presentation.Identity != wantProof {
		t.Fatalf("pending liability proof = %+v err=%v", presentation, err)
	}
	pending, err := st.PendingSyncedCredentialAdmission(account)
	if err != nil || pending.AccountID != account.ID || pending.Finalized ||
		!pending.CandidateAt.IsZero() || pending.ExternalStateDigest == ([32]byte{}) {
		t.Fatalf("pending liability = %+v err=%v", pending, err)
	}
	if _, err := st.SyncedCredentialAdmission(account); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("final evidence exists beside pending liability: %v", err)
	}
	if err := st.SelectionEligible(account); !errors.Is(err, store.ErrAccountSelectionIneligible) {
		t.Fatalf("pending liability was selection eligible: %v", err)
	}
	if origins, err := st.ListPublishableOrigins(); err != nil || len(origins) != 0 {
		t.Fatalf("pending liability was publishable: %+v err=%v", origins, err)
	}
}

func assertSyncedAdmissionCandidate(
	t *testing.T,
	st *store.Store,
	account store.Account,
	wantProof store.FileProviderPresentationIdentity,
) {
	t.Helper()
	health, err := st.GetAuthHealth(account.ID)
	if err != nil || !health.NeedsLogin || health.Kind != store.AuthKindAwaitingOrigin {
		t.Fatalf("candidate health = %+v err=%v", health, err)
	}
	presentation, err := st.AccountPresentation(account.ID)
	if err != nil || presentation.Identity != wantProof {
		t.Fatalf("candidate proof = %+v err=%v", presentation, err)
	}
	pending, err := st.PendingSyncedCredentialAdmission(account)
	if err != nil || pending.AccountID != account.ID || pending.CandidateAt.IsZero() ||
		pending.ExternalStateDigest == ([32]byte{}) {
		t.Fatalf("candidate evidence = %+v err=%v", pending, err)
	}
	if _, err := st.SyncedCredentialAdmission(account); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("final evidence exists beside candidate: %v", err)
	}
	if err := st.SelectionEligible(account); !errors.Is(err, store.ErrAccountSelectionIneligible) {
		t.Fatalf("candidate was selection eligible: %v", err)
	}
	if origins, err := st.ListPublishableOrigins(); err != nil || len(origins) != 0 {
		t.Fatalf("candidate was publishable: %+v err=%v", origins, err)
	}
}

func assertSyncedAdmissionFinal(
	t *testing.T,
	st *store.Store,
	account store.Account,
	wantProof store.FileProviderPresentationIdentity,
) {
	t.Helper()
	health, err := st.GetAuthHealth(account.ID)
	if err != nil || health.NeedsLogin || health.Kind != store.AuthKindOwned {
		t.Fatalf("final admission health = %+v err=%v", health, err)
	}
	presentation, err := st.AccountPresentation(account.ID)
	if err != nil || presentation.Identity != wantProof {
		t.Fatalf("final admission proof = %+v err=%v", presentation, err)
	}
	if _, err := st.SyncedCredentialAdmission(account); err != nil {
		t.Fatalf("final admission evidence: %v", err)
	}
	if _, err := st.PendingSyncedCredentialAdmission(account); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("pending liability survived final admission: %v", err)
	}
}
