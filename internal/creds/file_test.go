package creds

import (
	"errors"
	"os"
	"testing"
)

func TestFileCredentialRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if FileCredentialExists(dir) {
		t.Fatal("fresh dir reports a credential file")
	}
	if _, err := ReadFileCredential(dir); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadFileCredential on empty dir = %v, want ErrNotFound", err)
	}

	cred := &Credential{ClaudeAiOauth: OAuth{
		AccessToken:      "at-1",
		RefreshToken:     "rt-1",
		ExpiresAt:        1700000000000,
		SubscriptionType: "max",
	}}
	if err := WriteFileCredential(dir, cred); err != nil {
		t.Fatal(err)
	}
	if !FileCredentialExists(dir) {
		t.Fatal("FileCredentialExists false after write")
	}
	fi, err := os.Stat(FileCredentialPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", fi.Mode().Perm())
	}
	got, err := ReadFileCredential(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClaudeAiOauth.AccessToken != "at-1" || got.ClaudeAiOauth.RefreshToken != "rt-1" {
		t.Fatalf("round-trip mismatch: %+v", got.ClaudeAiOauth)
	}
	if got.ClaudeAiOauth.SubscriptionType != "max" {
		t.Fatalf("subscriptionType not preserved: %q", got.ClaudeAiOauth.SubscriptionType)
	}
}

// TestReadFileCredentialRejectsBlankToken pins that a blank-token file is
// rejected by the same parseCredential guard as Keychain blobs.
func TestReadFileCredentialRejectsBlankToken(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(FileCredentialPath(dir), []byte(`{"claudeAiOauth":{"accessToken":""}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFileCredential(dir); err == nil {
		t.Fatal("ReadFileCredential accepted a blank accessToken")
	}
}
