package creds

import (
	"errors"
	"testing"
)

func mkCred(access, refresh string) *Credential {
	c := &Credential{}
	c.ClaudeAiOauth.AccessToken = access
	c.ClaudeAiOauth.RefreshToken = refresh
	return c
}

// TestWriteFuncRejectsEmptyAccessToken pins the Keychain write-side guard. The
// funnel rejects before any security(1) exec, so the test never touches the
// real Keychain.
func TestWriteFuncRejectsEmptyAccessToken(t *testing.T) {
	empty := mkCred("", "rt-live") // refresh present, access blank — the account-1 failure shape
	if err := Write(
		t.Context(),
		testTaskRunner{},
		"some-suffixed-service",
		"user",
		empty,
	); !errors.Is(err, ErrNoAccessToken) {
		t.Fatalf("Write err = %v, want ErrNoAccessToken", err)
	}
}
