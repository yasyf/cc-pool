package pool

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func datedCred(token string, exp time.Duration) *creds.Credential {
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = token
	credential.ClaudeAiOauth.RefreshToken = "rt-" + token
	credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(exp).UnixMilli()
	return credential
}

func TestReadCredentialUsesKeychain(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	database := openTestStore(t)
	account := persistTestAccount(t, database, store.Account{ID: 1, KeychainAccount: "user"})
	credentials := credstest.NewFake()
	manager := &Manager{Store: database, Creds: credentials}
	if _, _, err := manager.ReadCredential(t.Context(), account); !errors.Is(err, creds.ErrNotFound) {
		t.Fatalf("ReadCredential without Keychain credential = %v, want ErrNotFound", err)
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
