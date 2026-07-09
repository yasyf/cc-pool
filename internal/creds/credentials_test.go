package creds

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestParseCredential pins the read-side guard: a blob is recoverable as long as
// it carries at least one token (the refresh token is the asset — an empty access
// token heals via refresh), only a wholly tokenless blob is ErrNoTokens, and
// malformed/wrong-shape JSON stays a parse-wrap error, never ErrNoTokens.
func TestParseCredential(t *testing.T) {
	cases := []struct {
		name        string
		blob        string
		wantErrIs   error  // non-nil: parse must fail with errors.Is(this)
		wantSubstr  string // non-empty: parse must fail with this substring (and NOT ErrNoTokens)
		wantAccess  string
		wantRefresh string
	}{
		{
			name:       "complete blob parses",
			blob:       `{"claudeAiOauth":{"accessToken":"at","refreshToken":"rt","expiresAt":1700000000000}}`,
			wantAccess: "at", wantRefresh: "rt",
		},
		{
			name:        "refresh-only blob parses",
			blob:        `{"claudeAiOauth":{"accessToken":"","refreshToken":"rt"}}`,
			wantRefresh: "rt",
		},
		{
			name:        "absent access-token key with a refresh token parses",
			blob:        `{"claudeAiOauth":{"refreshToken":"rt"}}`,
			wantRefresh: "rt",
		},
		{
			name:      "both tokens empty is ErrNoTokens",
			blob:      `{"claudeAiOauth":{"accessToken":"","refreshToken":""}}`,
			wantErrIs: ErrNoTokens,
		},
		{
			name:      "empty object is ErrNoTokens",
			blob:      `{}`,
			wantErrIs: ErrNoTokens,
		},
		{
			name:      "empty oauth object is ErrNoTokens",
			blob:      `{"claudeAiOauth":{}}`,
			wantErrIs: ErrNoTokens,
		},
		{
			name:       "json array is a parse error, not ErrNoTokens",
			blob:       `[]`,
			wantSubstr: "parse credential blob",
		},
		{
			name:       "malformed json is a parse error, not ErrNoTokens",
			blob:       `{not json`,
			wantSubstr: "parse credential blob",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := parseCredential([]byte(tc.blob))
			switch {
			case tc.wantErrIs != nil:
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("parseCredential err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			case tc.wantSubstr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("parseCredential err = %v, want substring %q", err, tc.wantSubstr)
				}
				if errors.Is(err, ErrNoTokens) {
					t.Fatalf("a parse-shape failure must not classify as ErrNoTokens: %v", err)
				}
			default:
				if err != nil {
					t.Fatalf("parseCredential = %v, want success", err)
				}
				if c.ClaudeAiOauth.AccessToken != tc.wantAccess || c.ClaudeAiOauth.RefreshToken != tc.wantRefresh {
					t.Fatalf("tokens = %q/%q, want %q/%q",
						c.ClaudeAiOauth.AccessToken, c.ClaudeAiOauth.RefreshToken, tc.wantAccess, tc.wantRefresh)
				}
			}
		})
	}
}

// TestExpiresWithinEmptyAccessTokenForcesRefresh pins that an empty access token
// always counts as expiring — even with a future expiry — so the refresh path
// runs, while Expiry() still returns the raw ExpiresAt the fresher-wins probe
// compares (the empty-token fold must never leak into Expiry()).
func TestExpiresWithinEmptyAccessTokenForcesRefresh(t *testing.T) {
	future := time.Now().Add(time.Hour)
	blob := fmt.Sprintf(`{"claudeAiOauth":{"refreshToken":"rt","expiresAt":%d}}`, future.UnixMilli())
	c, err := parseCredential([]byte(blob))
	if err != nil {
		t.Fatalf("parseCredential(refresh-only) = %v, want success", err)
	}
	if c.ClaudeAiOauth.AccessToken != "" {
		t.Fatalf("access token = %q, want empty", c.ClaudeAiOauth.AccessToken)
	}
	if !c.ExpiresWithin(0) {
		t.Error("ExpiresWithin(0) = false, want true (an empty access token always counts as expiring)")
	}
	if got := c.Expiry().UnixMilli(); got != future.UnixMilli() {
		t.Errorf("Expiry() = %d, want the raw future expiry %d (fresher-wins compares raw ExpiresAt)", got, future.UnixMilli())
	}
}
