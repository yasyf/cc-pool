// Package oauth talks to Anthropic's OAuth token-refresh and usage endpoints
// using the stored subscription credential (never an API key — API keys force
// per-request billing and disable subscription mode).
package oauth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// ClientID is Claude Code's public OAuth client id.
	ClientID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"

	tokenEndpoint = "https://platform.claude.com/v1/oauth/token" //nolint:gosec // G101: a public OAuth token endpoint URL, not a credential
	usageEndpoint = "https://api.anthropic.com/api/oauth/usage"

	// betaHeader is required for the usage endpoint.
	betaHeader = "oauth-2025-04-20"
)

// userAgentValue mimics the Claude Code CLI's User-Agent so the OAuth endpoints
// treat our polling like the official client; userAgentMu guards all access.
var (
	userAgentMu    sync.RWMutex
	userAgentValue = "claude-cli/2.1.166 (external)"
)

// SetUserAgentVersion sets the User-Agent to claude-cli/<version> (external);
// an empty version is a no-op.
func SetUserAgentVersion(version string) {
	if version == "" {
		return
	}
	userAgentMu.Lock()
	userAgentValue = "claude-cli/" + version + " (external)"
	userAgentMu.Unlock()
}

func userAgent() string {
	userAgentMu.RLock()
	defer userAgentMu.RUnlock()
	return userAgentValue
}

// Client is a thin OAuth client. The zero value is not usable; use New.
type Client struct {
	http      *http.Client
	tokenURL  string
	usageURL  string
	refreshSF singleflight.Group
}

// New returns a Client with sane timeouts.
func New() *Client {
	return &Client{
		http:     &http.Client{Timeout: 15 * time.Second},
		tokenURL: tokenEndpoint,
		usageURL: usageEndpoint,
	}
}

// TokenResponse is the refresh endpoint's reply.
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"` // may be empty if not rotated
	ExpiresIn    int64  `json:"expires_in"`    // seconds
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

// Expiry converts expires_in into an absolute time, from now.
func (t *TokenResponse) Expiry(now time.Time) time.Time {
	return now.Add(time.Duration(t.ExpiresIn) * time.Second)
}

// RefreshError carries the HTTP status so callers can distinguish a revoked
// token (4xx -> re-login needed) from a transient failure (5xx/network).
type RefreshError struct {
	Status int
	Body   string
}

func (e *RefreshError) Error() string {
	return fmt.Sprintf("oauth refresh failed: HTTP %d: %s", e.Status, e.Body)
}

// Revoked reports whether the error indicates the refresh token is no longer
// valid (invalid_grant / 400 / 401), meaning the account must be re-logged-in.
func (e *RefreshError) Revoked() bool {
	return e.Status == http.StatusBadRequest || e.Status == http.StatusUnauthorized
}

// Refresh exchanges a refresh token for a fresh access token. Calls sharing
// flightKey collapse to one in-flight request, so the daemon never races
// itself into a refresh-token rotation loop; pass a stable per-account key.
func (c *Client) Refresh(ctx context.Context, flightKey, refreshToken string) (*TokenResponse, error) {
	v, err, _ := c.refreshSF.Do(flightKey, func() (any, error) {
		return c.refresh(ctx, refreshToken)
	})
	if err != nil {
		return nil, err
	}
	return v.(*TokenResponse), nil
}

func (c *Client) refresh(ctx context.Context, refreshToken string) (*TokenResponse, error) {
	body, _ := json.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": refreshToken,
		"client_id":     ClientID,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent())

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oauth refresh request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return nil, &RefreshError{Status: resp.StatusCode, Body: truncate(string(raw), 300)}
	}
	var tr TokenResponse
	if err := json.Unmarshal(raw, &tr); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	if tr.AccessToken == "" {
		return nil, fmt.Errorf("token response missing access_token")
	}
	return &tr, nil
}

// Window is one usage window (5-hour or 7-day).
type Window struct {
	// Utilization is a percentage in [0,100] as the API reports it (e.g. 13.0 == 13%).
	Utilization float64
	// ResetsAt is when this window resets. Zero if absent.
	ResetsAt time.Time
}

// Used returns utilization as a 0..100 percentage for scoring/display.
func (w Window) Used() float64 { return w.Utilization }

// Remaining returns 100 - Used, clamped to [0,100].
func (w Window) Remaining() float64 {
	r := 100 - w.Used()
	if r < 0 {
		return 0
	}
	if r > 100 {
		return 100
	}
	return r
}

// Usage is the parsed /api/oauth/usage response. The API returns many windows;
// cc-pool consumes only the 5-hour, aggregate 7-day, and extra-usage (credit
// overage) blocks and ignores the rest.
type Usage struct {
	FiveHour   Window
	SevenDay   Window
	ExtraUsage ExtraUsage
}

// ExtraUsage is the pay-as-you-go overage block: when enabled, an exhausted
// account keeps serving on billed credits instead of 429s, so exhaustion is
// invisible to rate-limit checks. The zero value means absent or disabled.
type ExtraUsage struct {
	IsEnabled    bool
	MonthlyLimit float64 // credit cap for the month, in Currency cents
	UsedCredits  float64 // credits consumed this month, in Currency cents
	Utilization  float64 // percent of MonthlyLimit used, [0,100]
	Currency     string  // e.g. "USD"
}

type rawWindow struct {
	Utilization *float64  `json:"utilization"`
	ResetsAt    resetTime `json:"resets_at"`
}

func (rw *rawWindow) toWindow() Window {
	if rw == nil {
		return Window{}
	}
	var w Window
	if rw.Utilization != nil {
		w.Utilization = *rw.Utilization
	}
	if rw.ResetsAt.present {
		w.ResetsAt = rw.ResetsAt.t
	}
	return w
}

// resetTime decodes resets_at, which the API encodes inconsistently as a JSON
// number, numeric string (both epoch seconds), or RFC3339 string. Null/absent
// leaves present false; any other shape is a hard decode error, never swallowed.
type resetTime struct {
	t       time.Time
	present bool
}

func (r *resetTime) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == "" {
		return nil
	}
	if !strings.HasPrefix(s, `"`) {
		secs, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("resets_at %s: not a number or string", s)
		}
		r.t, r.present = time.Unix(int64(secs), 0), true
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return fmt.Errorf("resets_at %s: %w", s, err)
	}
	if str = strings.TrimSpace(str); str == "" {
		return nil
	}
	if secs, err := strconv.ParseFloat(str, 64); err == nil {
		r.t, r.present = time.Unix(int64(secs), 0), true
		return nil
	}
	ts, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return fmt.Errorf("resets_at %q: not epoch seconds or RFC3339: %w", str, err)
	}
	r.t, r.present = ts, true
	return nil
}

type rawUsage struct {
	FiveHour   *rawWindow     `json:"five_hour"`
	SevenDay   *rawWindow     `json:"seven_day"`
	ExtraUsage *rawExtraUsage `json:"extra_usage"`
}

type rawExtraUsage struct {
	IsEnabled    bool     `json:"is_enabled"`
	MonthlyLimit *float64 `json:"monthly_limit"`
	UsedCredits  *float64 `json:"used_credits"`
	Utilization  *float64 `json:"utilization"`
	Currency     string   `json:"currency"`
}

func (re *rawExtraUsage) toExtraUsage() ExtraUsage {
	if re == nil {
		return ExtraUsage{}
	}
	e := ExtraUsage{IsEnabled: re.IsEnabled, Currency: re.Currency}
	if re.MonthlyLimit != nil {
		e.MonthlyLimit = *re.MonthlyLimit
	}
	if re.UsedCredits != nil {
		e.UsedCredits = *re.UsedCredits
	}
	if re.Utilization != nil {
		e.Utilization = *re.Utilization
	}
	return e
}

// UsageError carries the HTTP status from a failed usage fetch.
type UsageError struct {
	Status int
	Body   string
}

func (e *UsageError) Error() string {
	return fmt.Sprintf("oauth usage failed: HTTP %d: %s", e.Status, e.Body)
}

// RateLimited reports whether the usage fetch itself was rate-limited (429).
func (e *UsageError) RateLimited() bool { return e.Status == http.StatusTooManyRequests }

// Unauthorized reports whether the access token was rejected (401) — the
// caller should refresh and retry.
func (e *UsageError) Unauthorized() bool { return e.Status == http.StatusUnauthorized }

// Usage fetches the current usage windows using a bearer access token.
func (c *Client) Usage(ctx context.Context, accessToken string) (*Usage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.usageURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("anthropic-beta", betaHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent())

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oauth usage request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return nil, &UsageError{Status: resp.StatusCode, Body: truncate(string(raw), 300)}
	}
	var ru rawUsage
	if err := json.Unmarshal(raw, &ru); err != nil {
		return nil, fmt.Errorf("decode usage response: %w", err)
	}
	return &Usage{
		FiveHour:   ru.FiveHour.toWindow(),
		SevenDay:   ru.SevenDay.toWindow(),
		ExtraUsage: ru.ExtraUsage.toExtraUsage(),
	}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
