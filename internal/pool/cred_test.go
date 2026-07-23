package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

func moveCred() *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-1"
	c.ClaudeAiOauth.RefreshToken = "rt-1"
	c.ClaudeAiOauth.ExpiresAt = 1_700_000_000_000
	return c
}

func datedCred(token string, exp time.Duration) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = token
	c.ClaudeAiOauth.RefreshToken = "rt-" + token
	c.ClaudeAiOauth.ExpiresAt = time.Now().Add(exp).UnixMilli()
	return c
}

func TestReadCredentialUsesOnlyKeychain(t *testing.T) {
	dir := t.TempDir()
	account := store.Account{
		ID: 1, ConfigDir: dir, KeychainService: "svc-keychain-only", KeychainAccount: "user",
	}
	credentials := credstest.NewFake()
	if err := credstest.FileStore(dir).Write(t.Context(), datedCred("file", time.Hour)); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{Creds: credentials}

	if _, _, err := manager.ReadCredential(t.Context(), account); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("ReadCredential with file-only credential = %v, want ErrNotFound", err)
	}

	credentials.Put(account.KeychainService, account.KeychainAccount, datedCred("keychain", time.Hour))
	credential, source, err := manager.ReadCredential(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	if source != creds.SourceKeychain || credential.ClaudeAiOauth.AccessToken != "keychain" {
		t.Fatalf("ReadCredential = source %v credential %+v", source, credential.ClaudeAiOauth)
	}
}
