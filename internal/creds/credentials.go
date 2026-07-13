package creds

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrNoAccessToken rejects persisting a credential with an empty accessToken.
var ErrNoAccessToken = errors.New("refusing to persist a credential with no accessToken")

// ErrNoTokens rejects a credential blob that holds neither an access nor a refresh token — nothing to use and nothing to refresh, so re-login is required.
var ErrNoTokens = errors.New("credential blob has no access or refresh token")

// OAuth is the inner object Claude stores under "claudeAiOauth". Field and
// wrapper-key names are reverse-engineered from the binary and MUST match
// byte-for-byte or Claude rejects the credential.
type OAuth struct {
	AccessToken string `json:"accessToken"`
	// RefreshToken's omitempty is load-bearing: claude treats a PRESENT empty
	// refreshToken as a dead chain (tombstone) but an ABSENT one as a plain
	// access-token-only blob, so stripped blobs must omit the field entirely.
	RefreshToken     string   `json:"refreshToken,omitempty"`
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

// ExpiresWithin reports whether the access token expires within d from now; an empty access token always counts as expiring, forcing the refresh path.
func (c *Credential) ExpiresWithin(d time.Duration) bool {
	if c.ClaudeAiOauth.AccessToken == "" {
		return true
	}
	return time.Until(c.Expiry()) <= d
}

// Expired reports whether the access token is already at or past its expiry.
func (c *Credential) Expired() bool {
	return c.ExpiresWithin(0)
}

// HasRefreshToken reports whether a refresh token is present. Presence marks
// an OWNED chain — this host minted it and only it may refresh; absence marks
// a synced copy (or, with the access token also empty, a tombstone).
func (c *Credential) HasRefreshToken() bool {
	return c.ClaudeAiOauth.RefreshToken != ""
}

// Synced reports whether this is a synced (peer) copy: an access token to
// serve with but no refresh token — usable until expiry, never refreshable here.
func (c *Credential) Synced() bool {
	return c.ClaudeAiOauth.AccessToken != "" && c.ClaudeAiOauth.RefreshToken == ""
}

// Strip returns a copy with the refresh token cleared — the shape synced to
// peers, so the long-lived secret never leaves the origin host. Marshal omits
// the cleared field entirely (see the OAuth.RefreshToken tag).
func (c *Credential) Strip() *Credential {
	out := *c
	out.ClaudeAiOauth.RefreshToken = ""
	return &out
}

// Marshal renders the credential as the exact JSON bytes Claude expects.
func (c *Credential) Marshal() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("marshal credential: %w", err)
	}
	return b, nil
}

func (c *Credential) validateForWrite() error {
	if c.ClaudeAiOauth.AccessToken == "" {
		return ErrNoAccessToken
	}
	return nil
}

func parseCredential(b []byte) (*Credential, error) {
	var c Credential
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse credential blob: %w", err)
	}
	if c.ClaudeAiOauth.AccessToken == "" && c.ClaudeAiOauth.RefreshToken == "" {
		return nil, ErrNoTokens
	}
	return &c, nil
}
