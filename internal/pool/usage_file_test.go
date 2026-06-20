package pool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestRefreshUsesFileBackendWhenKeychainEmpty pins the headless path: when an
// account's credential lives in claude's plaintext $CONFIG_DIR/.credentials.json
// (the Keychain holds nothing), a refresh reads and writes the file backend and
// never touches the Keychain.
func TestRefreshUsesFileBackendWhenKeychainEmpty(t *testing.T) {
	st := openTestStore(t)
	dir := t.TempDir()
	a := store.Account{ID: 1, ConfigDir: dir, KeychainService: keychain.ServiceName(dir), KeychainAccount: "user"}

	// claude wrote a near-expiry credential to the file backend (no Keychain item).
	seed := &keychain.Credential{}
	seed.ClaudeAiOauth.AccessToken = "at-0"
	seed.ClaudeAiOauth.RefreshToken = "rt-0"
	seed.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	if err := keychain.WriteFileCredential(dir, seed); err != nil {
		t.Fatal(err)
	}

	fk := newFakeKeychain() // empty: forces resolution to the file backend
	fo := &fakeOAuth{currentRT: "rt-0"}
	m := &Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()}

	cred, refreshed, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Fatal("near-expiry file-backed credential was not refreshed")
	}
	if cred.ClaudeAiOauth.AccessToken != "at-1" {
		t.Fatalf("returned access token = %q, want at-1", cred.ClaudeAiOauth.AccessToken)
	}

	// The rotated token landed in the file, with the non-token fields preserved.
	onDisk, err := keychain.ReadFileCredential(dir)
	if err != nil {
		t.Fatal(err)
	}
	if onDisk.ClaudeAiOauth.AccessToken != "at-1" || onDisk.ClaudeAiOauth.RefreshToken != "rt-1" {
		t.Fatalf("file backend not updated by refresh: %+v", onDisk.ClaudeAiOauth)
	}

	// The Keychain was never written — the account stays on the file backend.
	if _, err := fk.Read(a.KeychainService, a.KeychainAccount); !errors.Is(err, keychain.ErrNotFound) {
		t.Fatal("refresh wrote the credential to the Keychain instead of the file")
	}
}
