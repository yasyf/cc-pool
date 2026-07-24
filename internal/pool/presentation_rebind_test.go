package pool

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

func TestAccountPresentationRebindSourceEvidenceRecordsExactOldSlot(t *testing.T) {
	for _, test := range []struct {
		name      string
		keychain  bool
		expired   bool
		wantState store.CredentialSlotState
	}{
		{name: "present", keychain: true, wantState: store.CredentialSlotPresent},
		{name: "empty", wantState: store.CredentialSlotEmpty},
		{name: "expired-present", keychain: true, expired: true, wantState: store.CredentialSlotPresent},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			credentials := credstest.NewFake()
			manager := credentialRecoveryManager(t, openTestStore(t), credentials, "source-evidence")
			path := filepath.Join(home, "File Provider", "CCPool", "old")
			account := store.Account{
				ID: 1, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 1,
				ConfigDir: path, KeychainService: creds.ServiceName(path), KeychainAccount: "owner",
			}
			credential := presentationRebindCredential(test.name)
			if test.expired {
				credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
			}
			if test.keychain {
				credentials.Put(account.KeychainService, account.KeychainAccount, credential)
			}
			locator, state, digest, err := manager.AccountPresentationRebindSourceEvidence(t.Context(), account)
			if err != nil {
				t.Fatal(err)
			}
			if locator != store.CredentialKeychainLocatorDigest(
				account.KeychainService, account.KeychainAccount,
			) || state != test.wantState {
				t.Fatalf("source evidence locator=%x state=%s digest=%x", locator, state, digest)
			}
			if test.wantState == store.CredentialSlotPresent &&
				digest != credentialTokenChainDigest(credential) {
				t.Fatalf("present source digest=%x", digest)
			}
			if test.wantState == store.CredentialSlotEmpty && digest != (store.CredentialDigest{}) {
				t.Fatalf("empty source digest=%x", digest)
			}
		})
	}
}

func TestFinalizeAccountPresentationRebindDeletesExactOldKeychainBeforeAdmission(t *testing.T) {
	manager, credentials, mutation, oldCredential, newCredential := presentationRebindFixture(t)
	receipt, err := manager.FinalizeAccountPresentationRebind(
		t.Context(), mutation, time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Terminal != store.AccountMutationCommitted ||
		receipt.Kind != store.AccountMutationPresentationRebind {
		t.Fatalf("receipt = %+v", receipt)
	}
	if _, ok := credentials.Get(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount); ok {
		t.Fatal("old Keychain credential survived rebind")
	}
	if got, ok := credentials.Get(mutation.KeychainService, mutation.KeychainAccount); !ok ||
		credentialTokenChainDigest(got) != credentialTokenChainDigest(newCredential) {
		t.Fatalf("new Keychain credential = %+v present=%v", got, ok)
	}
	if credentialTokenChainDigest(oldCredential) != mutation.PreviousCredentialDigest {
		t.Fatal("fixture did not preserve exact old digest")
	}
	if _, err := manager.Store.AccountPresentationQuarantine(mutation.AccountID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rebind retained quarantine: %v", err)
	}
}

func TestFinalizeAccountPresentationRebindRetriesBeforeAndAfterOldDeletion(t *testing.T) {
	t.Run("before delete", func(t *testing.T) {
		manager, credentials, mutation, _, _ := presentationRebindFixture(t)
		original := manager.credentialCAS
		manager.credentialCAS = func(
			context.Context,
			store.Account,
			store.CredentialExternalState,
			credentialCASMutation,
		) (credentialCASProof, error) {
			return credentialCASProof{}, errors.New("worker stopped before delete")
		}
		if _, err := manager.FinalizeAccountPresentationRebind(
			t.Context(), mutation, time.Now().Add(time.Hour),
		); err == nil {
			t.Fatal("pre-delete worker failure admitted rebind")
		}
		if _, ok := credentials.Get(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount); !ok {
			t.Fatal("pre-delete failure removed old credential")
		}
		manager.credentialCAS = original
		if _, err := manager.FinalizeAccountPresentationRebind(
			t.Context(), mutation, time.Now().Add(time.Hour),
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("after delete", func(t *testing.T) {
		manager, credentials, mutation, _, _ := presentationRebindFixture(t)
		credentials.Remove(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount)
		if _, err := manager.FinalizeAccountPresentationRebind(
			t.Context(), mutation, time.Now().Add(time.Hour),
		); err != nil {
			t.Fatal(err)
		}
	})
}

func TestFinalizeAccountPresentationRebindRefusesCredentialDrift(t *testing.T) {
	for _, which := range []string{"old", "new", "new-after-old-delete"} {
		t.Run(which, func(t *testing.T) {
			manager, credentials, mutation, _, _ := presentationRebindFixture(t)
			changed := presentationRebindCredential("changed")
			switch which {
			case "old":
				credentials.Put(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount, changed)
			case "new":
				credentials.Put(mutation.KeychainService, mutation.KeychainAccount, changed)
			case "new-after-old-delete":
				credentials.Remove(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount)
				credentials.Put(mutation.KeychainService, mutation.KeychainAccount, changed)
			}
			if _, err := manager.FinalizeAccountPresentationRebind(
				t.Context(), mutation, time.Now().Add(time.Hour),
			); !errors.Is(err, ErrCredentialChangedUnderfoot) {
				t.Fatalf("%s drift = %v", which, err)
			}
			if _, err := manager.Store.AccountPresentationQuarantine(mutation.AccountID); err != nil {
				t.Fatalf("%s drift cleared quarantine: %v", which, err)
			}
			if active, err := manager.Store.AccountMutation(mutation.OperationID); err != nil ||
				active.State != store.AccountMutationRebindPublished {
				t.Fatalf("%s drift lost journal: %+v err=%v", which, active, err)
			}
		})
	}
}

func TestFinalizeAccountPresentationRebindCASRefusesRacingOldCredential(t *testing.T) {
	manager, credentials, mutation, _, _ := presentationRebindFixture(t)
	original := manager.credentialCAS
	changed := presentationRebindCredential("racing-old")
	manager.credentialCAS = func(
		ctx context.Context,
		account store.Account,
		expected store.CredentialExternalState,
		change credentialCASMutation,
	) (credentialCASProof, error) {
		credentials.Put(account.KeychainService, account.KeychainAccount, changed)
		return original(ctx, account, expected, change)
	}
	if _, err := manager.FinalizeAccountPresentationRebind(
		t.Context(), mutation, time.Now().Add(time.Hour),
	); !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("racing old credential = %v", err)
	}
	got, ok := credentials.Get(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount)
	if !ok || credentialTokenChainDigest(got) != credentialTokenChainDigest(changed) {
		t.Fatalf("racing old credential was deleted: %+v present=%v", got, ok)
	}
}

func TestFinalizeAccountPresentationRebindEmptyOldSlotIsReplayable(t *testing.T) {
	manager, credentials, mutation, _, _ := presentationRebindFixtureWithOldState(
		t, store.CredentialSlotEmpty, false,
	)
	receipt, err := manager.FinalizeAccountPresentationRebind(
		t.Context(), mutation, time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.PreviousCredentialState != store.CredentialSlotEmpty ||
		receipt.PreviousCredentialDigest != (store.CredentialDigest{}) {
		t.Fatalf("empty old-slot receipt = %+v", receipt)
	}
	if _, ok := credentials.Get(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount); ok {
		t.Fatal("empty old slot unexpectedly appeared")
	}
	replayed, err := manager.FinalizeAccountPresentationRebind(
		t.Context(), mutation, time.Now().Add(time.Hour),
	)
	if err != nil || replayed.OperationID != receipt.OperationID {
		t.Fatalf("empty old-slot replay = %+v err=%v", replayed, err)
	}
}

func TestFinalizeAccountPresentationRebindRejectsCredentialAppearingInEmptyOldSlot(t *testing.T) {
	manager, credentials, mutation, _, _ := presentationRebindFixtureWithOldState(
		t, store.CredentialSlotEmpty, false,
	)
	appeared := presentationRebindCredential("appeared")
	credentials.Put(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount, appeared)
	if _, err := manager.FinalizeAccountPresentationRebind(
		t.Context(), mutation, time.Now().Add(time.Hour),
	); !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("credential appearing in empty old slot = %v", err)
	}
	got, ok := credentials.Get(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount)
	if !ok || credentialTokenChainDigest(got) != credentialTokenChainDigest(appeared) {
		t.Fatalf("appeared credential was changed: %+v present=%v", got, ok)
	}
}

func TestFinalizeAccountPresentationRebindDeletesExpiredPresentOldSlot(t *testing.T) {
	manager, credentials, mutation, _, _ := presentationRebindFixtureWithOldState(
		t, store.CredentialSlotPresent, true,
	)
	if _, err := manager.FinalizeAccountPresentationRebind(
		t.Context(), mutation, time.Now().Add(time.Hour),
	); err != nil {
		t.Fatal(err)
	}
	if _, ok := credentials.Get(mutation.PreviousKeychainService, mutation.PreviousKeychainAccount); ok {
		t.Fatal("expired present old credential survived finalization")
	}
}

func presentationRebindFixture(
	t *testing.T,
) (*Manager, *credstest.Fake, store.AccountMutation, *creds.Credential, *creds.Credential) {
	t.Helper()
	return presentationRebindFixtureWithOldState(t, store.CredentialSlotPresent, false)
}

func presentationRebindFixtureWithOldState(
	t *testing.T,
	oldState store.CredentialSlotState,
	expired bool,
) (*Manager, *credstest.Fake, store.AccountMutation, *creds.Credential, *creds.Credential) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	credentials := credstest.NewFake()
	manager := credentialRecoveryManager(t, openTestStore(t), credentials, "presentation-rebind")
	oldPath := filepath.Join(home, "File Provider", "CCPool", "old")
	account := persistTestAccount(t, manager.Store, store.Account{
		ID: 1, ConfigDir: oldPath, KeychainService: creds.ServiceName(oldPath),
		KeychainAccount: "old-account",
	})
	newPath := filepath.Join(home, "File Provider", "CCPool", "new")
	drifted := presentationRebindProof(account, newPath, "activation-observed")
	if err := manager.Store.ObserveAccountPresentation(account, drifted); !errors.Is(err, store.ErrAccountPresentationQuarantined) {
		t.Fatalf("seed presentation quarantine: %v", err)
	}
	proof := drifted
	proof.Generation++
	oldCredential := presentationRebindCredential("old")
	if expired {
		oldCredential.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	}
	newCredential := presentationRebindCredential("new")
	if oldState == store.CredentialSlotPresent {
		credentials.Put(account.KeychainService, account.KeychainAccount, oldCredential)
	} else if oldState != store.CredentialSlotEmpty {
		t.Fatalf("invalid old state %q", oldState)
	}
	newService := creds.ServiceName(newPath)
	newAccount := creds.AccountLabel()
	previousLocator, previousCredentialState, previousCredential, err := manager.AccountPresentationRebindSourceEvidence(
		t.Context(), account,
	)
	if err != nil {
		t.Fatal(err)
	}
	if previousCredentialState != oldState {
		t.Fatalf("source state = %q, want %q", previousCredentialState, oldState)
	}
	intent := store.CredentialDigest{1}
	operationID, err := store.NewPresentationRebindMutationID(
		account.ID, account.InstanceID, account.Generation+1, previousLocator,
		previousCredentialState, previousCredential, intent,
	)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := manager.MutationOwner()
	if err != nil {
		t.Fatal(err)
	}
	begin, err := manager.Store.BeginAccountMutation(t.Context(), store.BeginAccountMutationRequest{
		OperationID: operationID, AccountID: account.ID,
		Kind: store.AccountMutationPresentationRebind, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation + 1, IntentDigest: intent,
		Label: account.Label, AccountUUID: account.AccountUUID,
		PreviousConfigDir: account.ConfigDir, PreviousKeychainService: account.KeychainService,
		PreviousKeychainAccount: account.KeychainAccount, PreviousLocatorDigest: previousLocator,
		PreviousCredentialState:  previousCredentialState,
		PreviousCredentialDigest: previousCredential, Owner: owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	empty := store.CredentialExternalState{
		Keychain: store.CredentialSlotObservation{State: store.CredentialSlotEmpty},
	}
	expected, err := empty.Digest()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := manager.Store.BindAccountMutationPresentation(
		begin.Active.Fence(), proof, newPath, newService, newAccount,
		store.CredentialKeychainLocatorDigest(newService, newAccount), expected,
	)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = manager.Store.MarkAccountMutationInputProvided(fence, store.CredentialDigest{3})
	if err != nil {
		t.Fatal(err)
	}
	fence, err = manager.Store.MarkAccountMutationApplying(fence)
	if err != nil {
		t.Fatal(err)
	}
	credentials.Put(newService, newAccount, newCredential)
	applying, err := manager.Store.AccountMutation(fence.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	written, err := manager.VerifyAccountPresentationRebindCredential(t.Context(), applying)
	if err != nil {
		t.Fatal(err)
	}
	if written != credentialTokenChainDigest(newCredential) {
		t.Fatalf("verified target digest = %x", written)
	}
	fence, err = manager.Store.MarkAccountMutationApplied(fence, written)
	if err != nil {
		t.Fatal(err)
	}
	fence, err = manager.Store.MarkAccountMutationPublishing(fence)
	if err != nil {
		t.Fatal(err)
	}
	fence, _, err = manager.Store.PublishAccountPresentationRebind(fence)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := manager.Store.AccountMutation(fence.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	return manager, credentials, mutation, oldCredential, newCredential
}

func presentationRebindProof(
	account store.Account,
	publicPath string,
	_ string,
) store.FileProviderPresentationIdentity {
	return store.FileProviderPresentationIdentity{
		TenantID: "account-" + account.InstanceID, DomainID: "domain-" + account.InstanceID,
		Generation: account.Generation, PublicPath: publicPath,
	}
}

func presentationRebindCredential(suffix string) *creds.Credential {
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "access-" + suffix
	credential.ClaudeAiOauth.RefreshToken = "refresh-" + suffix
	credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	return credential
}
