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
	c.ClaudeAiOauth.Scopes = []string{"user:inference", "user:profile"}
	c.ClaudeAiOauth.SubscriptionType = "max"
	c.ClaudeAiOauth.RateLimitTier = "raven"
	c.ClaudeAiOauth.ClientID = "client-1"
	return c
}

// TestAccessHash pins AccessHash's identity semantics: stable across marshal
// round-trips, invariant under Strip (the owned/stripped identity the sync
// design relies on), and changed by every field except the refresh token.
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

	t.Run("ignores the refresh token", func(t *testing.T) {
		if got := AccessHash(hashCred("at", "rt-other")); got != want {
			t.Fatalf("hash = %q, want %q (the refresh token must not be hashed)", got, want)
		}
	})

	t.Run("stripped copy hashes identically", func(t *testing.T) {
		if got := AccessHash(base.Strip()); got != want {
			t.Fatalf("stripped hash = %q, want %q", got, want)
		}
	})

	t.Run("every non-refresh field changes the hash", func(t *testing.T) {
		mutations := map[string]func(c *Credential){
			"accessToken":      func(c *Credential) { c.ClaudeAiOauth.AccessToken = "at-2" },
			"expiresAt":        func(c *Credential) { c.ClaudeAiOauth.ExpiresAt++ },
			"scopes":           func(c *Credential) { c.ClaudeAiOauth.Scopes = []string{"user:inference"} },
			"subscriptionType": func(c *Credential) { c.ClaudeAiOauth.SubscriptionType = "pro" },
			"rateLimitTier":    func(c *Credential) { c.ClaudeAiOauth.RateLimitTier = "default" },
			"clientId":         func(c *Credential) { c.ClaudeAiOauth.ClientID = "client-2" },
		}
		for name, mutate := range mutations {
			t.Run(name, func(t *testing.T) {
				c := hashCred("at", "rt")
				mutate(c)
				if got := AccessHash(c); got == want {
					t.Fatalf("hash unchanged after a %s change — the field is not authenticated", name)
				}
			})
		}
	})
}
