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
	if first.Reservation.ID != 1 || first.ConfigDir != firstPath {
		t.Fatalf("first pending = %+v", first)
	}
	info, err := os.Lstat(AccountBackingDir(first.Reservation.ID))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("private backing = %v, %v", info, err)
	}
	if _, err := os.Lstat(first.ConfigDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pool mutated presentation path %s: %v", first.ConfigDir, err)
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
	if err := manager.AbandonAdd(t.Context(), first); err != nil {
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
}

func TestReleaseAddRetainsCompletedLogin(t *testing.T) {
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
	if err := manager.ReleaseAdd(pending); err != nil {
		t.Fatal(err)
	}
	retryReservation, err := manager.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	retry, err := manager.PrepareReservedAdd(t.Context(), retryReservation, configDir)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Reservation.ID != pending.Reservation.ID || retry.ClaudeJSONSeed != SeedKeptExisting {
		t.Fatalf("retry = %+v", retry)
	}
}

func TestAbandonAddJournalsExactCredentialRemoval(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "present"}[present], func(t *testing.T) {
			manager := newAccountManager(t)
			fake := manager.Creds.(*credstest.Fake)
			pending := prepareRemovalTestAdd(t, manager)
			if present {
				fake.Put(pending.KeychainService, creds.AccountLabel(), datedCred("retire", time.Hour))
			}

			if err := manager.AbandonAdd(t.Context(), pending); err != nil {
				t.Fatal(err)
			}
			assertRemovalReceipt(t, manager.Store, pending)
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

	if err := manager.AbandonAdd(t.Context(), pending); err == nil {
		t.Fatal("lost response unexpectedly reported success")
	}
	if _, ok := fake.Get(pending.KeychainService, creds.AccountLabel()); ok {
		t.Fatal("credential was not deleted before response loss")
	}
	if _, err := os.Stat(AccountBackingDir(pending.Reservation.ID)); err != nil {
		t.Fatalf("first attempt removed backing before durable replay: %v", err)
	}
	if err := manager.AbandonAdd(t.Context(), pending); err != nil {
		t.Fatalf("receipt replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("credential CAS executions = %d, want one", calls)
	}
	assertRemovalReceipt(t, manager.Store, pending)
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

	err := manager.AbandonAdd(t.Context(), pending)
	if !errors.Is(err, ErrCredentialOperationQuarantined) {
		t.Fatalf("replacement race = %v, want quarantine", err)
	}
	got, ok := fake.Get(pending.KeychainService, creds.AccountLabel())
	if !ok || got.ClaudeAiOauth.AccessToken != replacement.ClaudeAiOauth.AccessToken {
		t.Fatalf("replacement was not preserved: %+v", got)
	}
	if _, err := manager.Store.BeginAccountRemoval(pending.Reservation.ID, true); err != nil {
		t.Fatalf("durable removal missing after race: %v", err)
	}
	if _, err := os.Stat(AccountBackingDir(pending.Reservation.ID)); err != nil {
		t.Fatalf("quarantined removal deleted backing: %v", err)
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
