package creds

import (
	"encoding/json"
	"testing"
)

func hashCred(access, refresh string) *Credential {
	c := &Credential{}
	c.ClaudeAiOauth.AccessToken = access
	c.ClaudeAiOauth.RefreshToken = refresh
	c.ClaudeAiOauth.ExpiresAt = 1_700_000_000_000
	return c
}

// TestAccessHash pins AccessHash's identity semantics: stable across marshal
// round-trips and refresh-token/metadata changes, changed exactly when the
// access token changes, and equal between an owned blob and its stripped copy.
func TestAccessHash(t *testing.T) {
	base := hashCred("at", "rt")
	want := AccessHash(base)

	t.Run("stable across marshal round-trip", func(t *testing.T) {
		b, err := base.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		var rt Credential
		if err := json.Unmarshal(b, &rt); err != nil {
			t.Fatal(err)
		}
		if got := AccessHash(&rt); got != want {
			t.Fatalf("round-trip hash = %q, want %q", got, want)
		}
	})

	t.Run("ignores refresh token and metadata", func(t *testing.T) {
		c := hashCred("at", "rt-other")
		c.ClaudeAiOauth.ExpiresAt = 42
		c.ClaudeAiOauth.SubscriptionType = "max"
		if got := AccessHash(c); got != want {
			t.Fatalf("hash = %q, want %q (only the access token is hashed)", got, want)
		}
	})

	t.Run("stripped copy hashes identically", func(t *testing.T) {
		if got := AccessHash(base.Strip()); got != want {
			t.Fatalf("stripped hash = %q, want %q", got, want)
		}
	})

	t.Run("access-token change changes the hash", func(t *testing.T) {
		if got := AccessHash(hashCred("at-2", "rt")); got == want {
			t.Fatal("hash unchanged after an access-token change")
		}
	})

	t.Run("length prefix disambiguates the field boundary from CredentialHash", func(t *testing.T) {
		// CredentialHash("", "at") must not collide with AccessHash("at"): the
		// pair hash prefixes both fields.
		if got := CredentialHash(hashCred("at", "")); got == want {
			t.Fatal("CredentialHash of an RT-less pair collides with AccessHash")
		}
	})
}
