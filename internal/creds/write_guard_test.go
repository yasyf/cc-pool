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

// TestWriteFuncRejectsEmptyAccessToken pins the write-side guard: neither
// backend persists the refresh-only corruption shape (blank accessToken beside
// a live refreshToken), while a deliberate both-empty tombstone — the strip of
// a dead refresh-only chain — persists. The Keychain funnel is only checked to
// reject before any security(1) exec, so the test never touches the real Keychain.
func TestWriteFuncRejectsEmptyAccessToken(t *testing.T) {
	empty := mkCred("", "rt-live") // refresh present, access blank — the account-1 failure shape
	valid := mkCred("at-live", "rt-live")

	t.Run("file backend", func(t *testing.T) {
		cases := []struct {
			name       string
			cred       *Credential
			wantErr    error
			wantStored bool
		}{
			{name: "empty access token rejected", cred: empty, wantErr: ErrNoAccessToken, wantStored: false},
			{name: "populated access token persisted", cred: valid, wantErr: nil, wantStored: true},
			{name: "synced (access-only) blob persisted", cred: mkCred("at-synced", ""), wantErr: nil, wantStored: true},
			{name: "tombstone (both empty) persisted", cred: mkCred("", ""), wantErr: nil, wantStored: true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				err := writeFileCredentialForTest(dir, tc.cred)
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("credential file write err = %v, want %v", err, tc.wantErr)
				}
				if got := fileCredentialExistsForTest(dir); got != tc.wantStored {
					t.Fatalf("credential persisted = %v, want %v", got, tc.wantStored)
				}
			})
		}
	})

	t.Run("keychain funnel rejects before any security(1) exec", func(t *testing.T) {
		if err := Write(
			t.Context(),
			testTaskRunner{},
			"some-suffixed-service",
			"user",
			empty,
		); !errors.Is(err, ErrNoAccessToken) {
			t.Fatalf("Write err = %v, want ErrNoAccessToken", err)
		}
	})
}
