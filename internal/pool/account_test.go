package pool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

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
	if _, err := manager.AccountIdentity(t.Context(), 1, AccountDir(1)); !errors.Is(err, ErrNoIdentity) {
		t.Fatalf("AccountIdentity error = %v", err)
	}
}
