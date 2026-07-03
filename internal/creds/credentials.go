package creds

import (
	"encoding/json"
	"fmt"
	"time"
)

// OAuth is the inner object Claude stores under "claudeAiOauth". Field and
// wrapper-key names are reverse-engineered from the binary and MUST match
// byte-for-byte or Claude rejects the credential.
type OAuth struct {
	AccessToken      string   `json:"accessToken"`
	RefreshToken     string   `json:"refreshToken"`
	ExpiresAt        int64    `json:"expiresAt"` // Unix epoch MILLISECONDS
	Scopes           []string `json:"scopes,omitempty"`
	SubscriptionType string   `json:"subscriptionType,omitempty"`
	RateLimitTier    string   `json:"rateLimitTier,omitempty"`
	ClientID         string   `json:"clientId,omitempty"`
}

// Credential is the full JSON blob stored as the Keychain secret:
//
//	{"claudeAiOauth": { ...OAuth... }}
type Credential struct {
	ClaudeAiOauth OAuth `json:"claudeAiOauth"`
}

// Expiry returns the access-token expiry as a time.Time.
func (c *Credential) Expiry() time.Time {
	return time.UnixMilli(c.ClaudeAiOauth.ExpiresAt)
}

// ExpiresWithin reports whether the access token expires within d from now.
func (c *Credential) ExpiresWithin(d time.Duration) bool {
	return time.Until(c.Expiry()) <= d
}

// Expired reports whether the access token is already at or past its expiry.
func (c *Credential) Expired() bool {
	return c.ExpiresWithin(0)
}

// HasRefreshToken reports whether a usable refresh token is present. Claude
// blanks it on a dead token, so empty means re-login is required.
func (c *Credential) HasRefreshToken() bool {
	return c.ClaudeAiOauth.RefreshToken != ""
}

// Marshal renders the credential as the exact JSON bytes Claude expects.
func (c *Credential) Marshal() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("marshal credential: %w", err)
	}
	return b, nil
}

func parseCredential(b []byte) (*Credential, error) {
	var c Credential
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse credential blob: %w", err)
	}
	if c.ClaudeAiOauth.AccessToken == "" {
		return nil, fmt.Errorf("credential blob has no accessToken")
	}
	return &c, nil
}
