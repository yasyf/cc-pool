package pool

import (
	"context"
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

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	result, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = result.Close() })
	return result
}

func persistTestAccount(t *testing.T, st *store.Store, account store.Account) store.Account {
	t.Helper()
	return admitPoolTestAccount(t, st, account)
}

func newAccountManager(t *testing.T) *Manager {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	manager := credentialRecoveryManager(
		t, openTestStore(t), credstest.NewFake(), "account-manager",
	)
	manager.ClaimCredentialMutation = func(int) (func(), error) {
		return func() {}, nil
	}
	installTestBackingRunner(manager)
	if _, err := manager.Init(); err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestPrepareAddUsesPlainPrivateBackingAndReservation(t *testing.T) {
	manager := newAccountManager(t)
	firstReservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	firstPath := filepath.Join(t.TempDir(), "CloudStorage", "account-1")
	first, err := manager.PrepareReservedAdd(t.Context(), firstReservation, firstPath)
	if err != nil {
		t.Fatal(err)
	}
	wantConfigDir, err := AccountConfigDir(firstReservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	wantService, err := AccountKeychainService(firstReservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reservation.ID != 1 || first.ConfigDir != wantConfigDir ||
		first.PublicPath != firstPath || first.KeychainService != wantService {
		t.Fatalf("first pending = %+v", first)
	}
	assertLinkTarget(t, first.ConfigDir, first.PublicPath)
	info, err := os.Lstat(AccountBackingDir(first.Reservation.ID))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("private backing = %v, %v", info, err)
	}
	if _, err := os.Lstat(first.PublicPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pool mutated presentation path %s: %v", first.PublicPath, err)
	}
	if first.ClaudeJSONSeed != SeedNoSource {
		t.Fatalf("seed = %q", first.ClaudeJSONSeed)
	}
	secondReservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.PrepareReservedAdd(
		t.Context(), secondReservation, filepath.Join(t.TempDir(), "CloudStorage", "account-2"),
	)
	if err != nil || second.Reservation.ID != 2 {
		t.Fatalf("second pending = %+v, %v", second, err)
	}
	if err := manager.AbandonAdd(t.Context(), first, pendingRetirementProof(first)); err != nil {
		t.Fatal(err)
	}
	reusedReservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	reused, err := manager.PrepareReservedAdd(t.Context(), reusedReservation, firstPath)
	if err != nil || reused.Reservation.ID != 1 {
		t.Fatalf("reused pending = %+v, %v", reused, err)
	}
	if reused.Reservation.InstanceID == first.Reservation.InstanceID ||
		reused.ConfigDir == first.ConfigDir || reused.KeychainService == first.KeychainService {
		t.Fatalf("numeric ID reuse aliased immutable execution identity: first=%+v reused=%+v", first, reused)
	}
	assertLinkTarget(t, reused.ConfigDir, firstPath)
}

func TestRetainedAddKeepsExactReservationAndExecutionIdentity(t *testing.T) {
	manager := newAccountManager(t)
	pendingReservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(t.TempDir(), "CloudStorage", "account-1")
	pending, err := manager.PrepareReservedAdd(t.Context(), pendingReservation, configDir)
	if err != nil {
		t.Fatal(err)
	}
	identity := []byte(`{"oauthAccount":{"accountUuid":"u-kept"}}`)
	if err := os.WriteFile(
		privateClaudeJSONPath(AccountBackingDir(pending.Reservation.ID)), identity, 0o600,
	); err != nil {
		t.Fatal(err)
	}
	retryReservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	if retryReservation.ID == pending.Reservation.ID {
		t.Fatalf("retained reservation %d was reused", pending.Reservation.ID)
	}
	assertLinkTarget(t, pending.ConfigDir, pending.PublicPath)
	if raw, err := os.ReadFile(privateClaudeJSONPath(AccountBackingDir(pending.Reservation.ID))); err != nil ||
		string(raw) != string(identity) {
		t.Fatalf("retained login state = %q err=%v", raw, err)
	}
}

func TestAbandonAddJournalsExactCredentialRemoval(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "present"}[present], func(t *testing.T) {
			manager := newAccountManager(t)
			fake := manager.Creds.(*credstest.Fake)
			pending := prepareRemovalTestAdd(t, manager)
			if err := os.MkdirAll(pending.PublicPath, 0o700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(pending.PublicPath, "target-survives")
			if err := os.WriteFile(marker, []byte("target"), 0o600); err != nil {
				t.Fatal(err)
			}
			if present {
				fake.Put(pending.KeychainService, creds.AccountLabel(), datedCred("retire", time.Hour))
			}

			if err := manager.AbandonAdd(
				t.Context(), pending, pendingRetirementProof(pending),
			); err != nil {
				t.Fatal(err)
			}
			assertRemovalReceipt(t, manager.Store, pending)
			if _, err := os.Lstat(pending.ConfigDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stable execution link survived retirement: %v", err)
			}
			if raw, err := os.ReadFile(marker); err != nil || string(raw) != "target" {
				t.Fatalf("presentation target changed: %q err=%v", raw, err)
			}
			if _, ok := fake.Get(pending.KeychainService, creds.AccountLabel()); ok {
				t.Fatal("credential survived exact removal")
			}
			if _, err := os.Stat(AccountBackingDir(pending.Reservation.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backing survived removal: %v", err)
			}
			reused, err := manager.ReserveAdd()
			if err != nil || reused.ID != pending.Reservation.ID {
				t.Fatalf("reservation was not released: %+v err=%v", reused, err)
			}
		})
	}
}

func TestAbandonAddLostDeleteResponseReplaysReceipt(t *testing.T) {
	manager := newAccountManager(t)
	fake := manager.Creds.(*credstest.Fake)
	pending := prepareRemovalTestAdd(t, manager)
	fake.Put(pending.KeychainService, creds.AccountLabel(), datedCred("lost-response", time.Hour))
	baseCAS := manager.credentialCAS
	calls := 0
	manager.credentialCAS = func(
		ctx context.Context,
		account store.Account,
		expected store.CredentialExternalState,
		mutation credentialCASMutation,
	) (credentialCASProof, error) {
		calls++
		proof, err := baseCAS(ctx, account, expected, mutation)
		if err == nil && calls == 1 {
			return proof, errors.New("simulated lost delete response")
		}
		return proof, err
	}

	proof := pendingRetirementProof(pending)
	if err := manager.AbandonAdd(t.Context(), pending, proof); err == nil {
		t.Fatal("lost response unexpectedly reported success")
	}
	if _, ok := fake.Get(pending.KeychainService, creds.AccountLabel()); ok {
		t.Fatal("credential was not deleted before response loss")
	}
	assertLinkTarget(t, pending.ConfigDir, pending.PublicPath)
	if _, err := os.Stat(AccountBackingDir(pending.Reservation.ID)); err != nil {
		t.Fatalf("first attempt removed backing before durable replay: %v", err)
	}
	if err := manager.AbandonAdd(t.Context(), pending, proof); err != nil {
		t.Fatalf("receipt replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("credential CAS executions = %d, want one", calls)
	}
	assertRemovalReceipt(t, manager.Store, pending)
}

func TestAbandonAddReplaysEveryRetiredCleanupBoundary(t *testing.T) {
	for _, stage := range []string{"after-credential", "after-unlink", "after-backing"} {
		t.Run(stage, func(t *testing.T) {
			manager := newAccountManager(t)
			pending := prepareRemovalTestAdd(t, manager)
			fake := manager.Creds.(*credstest.Fake)
			fake.Put(pending.KeychainService, creds.AccountLabel(), datedCred(stage, time.Hour))
			if err := os.MkdirAll(pending.PublicPath, 0o700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(pending.PublicPath, "target-survives")
			if err := os.WriteFile(marker, []byte(stage), 0o600); err != nil {
				t.Fatal(err)
			}
			abandonAddFailpoint = func(current string) error {
				if current == stage {
					if current == "after-credential" {
						assertLinkTarget(t, pending.ConfigDir, pending.PublicPath)
						if _, ok := fake.Get(pending.KeychainService, creds.AccountLabel()); ok {
							t.Fatal("credential survived the credential cleanup boundary")
						}
					}
					return errors.New("injected cleanup crash")
				}
				return nil
			}
			t.Cleanup(func() { abandonAddFailpoint = nil })
			proof := pendingRetirementProof(pending)
			if err := manager.AbandonAdd(t.Context(), pending, proof); err == nil {
				t.Fatal("cleanup crash reported success")
			}
			next, err := manager.ReserveAdd()
			if err != nil || next.ID == pending.Reservation.ID {
				t.Fatalf("cleanup crash reused reservation: next=%+v err=%v", next, err)
			}
			if err := manager.Store.ReleaseAccountIndex(next); err != nil {
				t.Fatal(err)
			}
			abandonAddFailpoint = nil
			if err := manager.AbandonAdd(t.Context(), pending, proof); err != nil {
				t.Fatalf("cleanup replay: %v", err)
			}
			reused, err := manager.ReserveAdd()
			if err != nil || reused.ID != pending.Reservation.ID {
				t.Fatalf("cleanup replay did not release exact reservation: %+v err=%v", reused, err)
			}
			if raw, err := os.ReadFile(marker); err != nil || string(raw) != stage {
				t.Fatalf("presentation target after replay = %q err=%v", raw, err)
			}
		})
	}
}

func TestAbandonAddReplacementRacePreservesReplacement(t *testing.T) {
	manager := newAccountManager(t)
	fake := manager.Creds.(*credstest.Fake)
	pending := prepareRemovalTestAdd(t, manager)
	fake.Put(pending.KeychainService, creds.AccountLabel(), datedCred("original", time.Hour))
	replacement := datedCred("replacement", 2*time.Hour)
	baseCAS := manager.credentialCAS
	manager.credentialCAS = func(
		ctx context.Context,
		account store.Account,
		expected store.CredentialExternalState,
		mutation credentialCASMutation,
	) (credentialCASProof, error) {
		fake.Put(account.KeychainService, account.KeychainAccount, replacement)
		return baseCAS(ctx, account, expected, mutation)
	}

	err := manager.AbandonAdd(t.Context(), pending, pendingRetirementProof(pending))
	if !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("replacement race = %v, want quarantine", err)
	}
	got, ok := fake.Get(pending.KeychainService, creds.AccountLabel())
	if !ok || got.ClaudeAiOauth.AccessToken != replacement.ClaudeAiOauth.AccessToken {
		t.Fatalf("replacement was not preserved: %+v", got)
	}
	ids, idsErr := manager.Store.PendingAddIndexes()
	if idsErr != nil || len(ids) != 1 || ids[0] != pending.Reservation.ID {
		t.Fatalf("quarantined reservation = %v err=%v", ids, idsErr)
	}
	if _, err := os.Stat(AccountBackingDir(pending.Reservation.ID)); err != nil {
		t.Fatalf("quarantined removal deleted backing: %v", err)
	}
	assertLinkTarget(t, pending.ConfigDir, pending.PublicPath)
}

func TestAbandonAddRejectsForeignRetirementProof(t *testing.T) {
	manager := newAccountManager(t)
	pending := prepareRemovalTestAdd(t, manager)
	proof := pendingRetirementProof(pending)
	proof.AccountInstanceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	if err := manager.AbandonAdd(t.Context(), pending, proof); err == nil {
		t.Fatal("foreign retirement proof was accepted")
	}
	assertLinkTarget(t, pending.ConfigDir, pending.PublicPath)
	ids, err := manager.Store.PendingAddIndexes()
	if err != nil || len(ids) != 1 || ids[0] != pending.Reservation.ID {
		t.Fatalf("reservation changed after foreign proof: %v err=%v", ids, err)
	}
}

func prepareRemovalTestAdd(t *testing.T, manager *Manager) *PendingAdd {
	t.Helper()
	reservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	pending, err := manager.PrepareReservedAdd(
		t.Context(), reservation, filepath.Join(t.TempDir(), "CloudStorage", "account"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pending
}

func pendingRetirementProof(pending *PendingAdd) PendingAddRetirementProof {
	return PendingAddRetirementProof{
		AccountID: pending.Reservation.ID, AccountInstanceID: pending.Reservation.InstanceID,
		AccountGeneration: pending.Reservation.Generation, PublicPath: pending.PublicPath,
	}
}

func assertRemovalReceipt(t *testing.T, st *store.Store, pending *PendingAdd) {
	t.Helper()
	account := store.Account{
		ID: pending.Reservation.ID, InstanceID: pending.Reservation.InstanceID,
		Generation: pending.Reservation.Generation, ConfigDir: pending.ConfigDir,
		KeychainService: pending.KeychainService, KeychainAccount: creds.AccountLabel(),
	}
	intent, err := store.CredentialRemovalIntentDigest(
		account.ID, account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount,
	)
	if err != nil {
		t.Fatal(err)
	}
	active, receipt, err := st.CredentialOperationEvidence(store.CredentialOperationEvidenceQuery{
		AccountID: account.ID, AccountInstanceID: account.InstanceID,
		AccountGeneration: account.Generation, ConfigDir: account.ConfigDir,
		KeychainService: account.KeychainService, KeychainAccount: account.KeychainAccount,
		LocatorDigest: store.CredentialKeychainLocatorDigest(account.KeychainService, account.KeychainAccount),
		Kind:          store.CredentialOperationRemove, Target: store.CredentialTargetKeychain,
		IntentDigest: intent,
	})
	if err != nil || active != nil || receipt == nil {
		t.Fatalf("removal evidence = active=%+v receipt=%+v err=%v", active, receipt, err)
	}
	if receipt.TerminalStatus != store.CredentialTerminalSucceeded ||
		receipt.Result != store.CredentialResultDone || receipt.AcknowledgedAt.IsZero() {
		t.Fatalf("removal receipt = %+v", receipt)
	}
}

func TestAccountIdentityAndWriteUseBackingOnly(t *testing.T) {
	manager := newAccountManager(t)
	accountID := 7
	configDir := filepath.Join(t.TempDir(), "File Provider", "CCPool", "acct-07")
	backingDir := AccountBackingDir(accountID)
	if err := os.MkdirAll(backingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	seed := []byte(`{"keep":true,"oauthAccount":{"accountUuid":"old"}}`)
	if err := os.WriteFile(privateClaudeJSONPath(backingDir), seed, 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := json.RawMessage(`{"accountUuid":"new","emailAddress":"new@example.com"}`)
	if err := manager.WriteIdentity(t.Context(), accountID, configDir, replacement); err != nil {
		t.Fatal(err)
	}
	raw, identity, err := manager.AccountOAuth(t.Context(), accountID, configDir)
	if err != nil {
		t.Fatal(err)
	}
	if identity.AccountUUID != "new" || identity.EmailAddress != "new@example.com" || string(raw) != string(replacement) {
		t.Fatalf("identity = %s %+v", raw, identity)
	}
	content, err := os.ReadFile(privateClaudeJSONPath(backingDir))
	if err != nil || !json.Valid(content) {
		t.Fatalf("content = %q, %v", content, err)
	}
	var document map[string]json.RawMessage
	_ = json.Unmarshal(content, &document)
	if string(document["keep"]) != "true" {
		t.Fatalf("sibling lost: %s", content)
	}
	if _, err := os.Lstat(configDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("identity worker traversed presentation path %s: %v", configDir, err)
	}
}

func TestIdentityMissingIsExplicit(t *testing.T) {
	manager := newAccountManager(t)
	if _, err := manager.AccountIdentity(t.Context(), 1, testFileProviderConfigDir(1)); !errors.Is(err, ErrNoIdentity) {
		t.Fatalf("AccountIdentity error = %v", err)
	}
}
