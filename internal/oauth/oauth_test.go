package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRefreshRequestAndResponse(t *testing.T) {
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		//nolint:gosec // G117: a test fixture token response, not a real credential
		_ = json.NewEncoder(w).Encode(TokenResponse{
			AccessToken: "new-at", RefreshToken: "new-rt", ExpiresIn: 3600,
		})
	}))
	defer srv.Close()

	c := New()
	c.tokenURL = srv.URL
	tr, err := c.Refresh(context.Background(), "k", "old-rt")
	if err != nil {
		t.Fatal(err)
	}
	if gotBody["grant_type"] != "refresh_token" {
		t.Errorf("grant_type = %q", gotBody["grant_type"])
	}
	if gotBody["client_id"] != ClientID {
		t.Errorf("client_id = %q, want %q", gotBody["client_id"], ClientID)
	}
	if gotBody["refresh_token"] != "old-rt" {
		t.Errorf("refresh_token = %q", gotBody["refresh_token"])
	}
	if tr.AccessToken != "new-at" || tr.RefreshToken != "new-rt" {
		t.Errorf("token response = %+v", tr)
	}
	if exp := tr.Expiry(time.Unix(0, 0)); exp != time.Unix(3600, 0) {
		t.Errorf("expiry = %v", exp)
	}
}

func TestRefreshRevoked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := New()
	c.tokenURL = srv.URL
	_, err := c.Refresh(context.Background(), "k", "rt")
	var re *RefreshError
	if !errors.As(err, &re) || !re.Revoked() {
		t.Fatalf("expected revoked RefreshError, got %v", err)
	}
}

func TestUsageHeadersAndParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer abc" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("anthropic-beta"); got != betaHeader {
			t.Errorf("anthropic-beta = %q, want %q", got, betaHeader)
		}
		_, _ = io.WriteString(w, `{
			"five_hour":{"utilization":40.0,"resets_at":1700000000},
			"seven_day":{"utilization":10.0,"resets_at":1700600000}
		}`)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	u, err := c.Usage(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if u.FiveHour.Used() != 40 {
		t.Errorf("five_hour used = %.1f, want 40", u.FiveHour.Used())
	}
	if u.FiveHour.Remaining() != 60 {
		t.Errorf("five_hour remaining = %.1f, want 60", u.FiveHour.Remaining())
	}
	if u.SevenDay.Used() != 10 {
		t.Errorf("seven_day used = %.1f, want 10", u.SevenDay.Used())
	}
	if u.FiveHour.ResetsAt.Unix() != 1700000000 {
		t.Errorf("resets_at = %v", u.FiveHour.ResetsAt)
	}
}

func TestUsageIgnoresUnknownWindows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{
			"five_hour":{"utilization":31.0,"resets_at":"2026-06-08T12:10:01+00:00"},
			"seven_day":{"utilization":56.0,"resets_at":"2026-06-11T13:00:00+00:00"},
			"seven_day_oauth_apps":null,
			"seven_day_opus":null,
			"seven_day_sonnet":{"utilization":0.0,"resets_at":null},
			"seven_day_omelette":{"utilization":0.0,"resets_at":null},
			"tangelo":null,
			"extra_usage":{"is_enabled":true,"monthly_limit":5000,"used_credits":177.0,"utilization":3.54,"currency":"USD","disabled_reason":null}
		}`)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	u, err := c.Usage(context.Background(), "abc")
	if err != nil {
		t.Fatalf("Usage with current payload: %v", err)
	}
	if u.FiveHour.Used() != 31 {
		t.Errorf("five_hour used = %.2f, want 31", u.FiveHour.Used())
	}
	if u.SevenDay.Used() != 56 {
		t.Errorf("seven_day used = %.2f, want 56", u.SevenDay.Used())
	}
	if u.SevenDay.Remaining() != 44 {
		t.Errorf("seven_day remaining = %.2f, want 44", u.SevenDay.Remaining())
	}
	want := ExtraUsage{IsEnabled: true, MonthlyLimit: 5000, UsedCredits: 177.0, Utilization: 3.54, Currency: "USD"}
	if u.ExtraUsage != want {
		t.Errorf("extra_usage = %+v, want %+v", u.ExtraUsage, want)
	}
}

func TestUsageExtraUsageAbsent(t *testing.T) {
	for name, body := range map[string]string{
		"omitted": `{"five_hour":{"utilization":10.0,"resets_at":null},"seven_day":{"utilization":5.0,"resets_at":null}}`,
		"null":    `{"five_hour":{"utilization":10.0,"resets_at":null},"seven_day":{"utilization":5.0,"resets_at":null},"extra_usage":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer srv.Close()
			c := New()
			c.usageURL = srv.URL
			u, err := c.Usage(context.Background(), "abc")
			if err != nil {
				t.Fatalf("Usage: %v", err)
			}
			if u.ExtraUsage != (ExtraUsage{}) {
				t.Errorf("extra_usage = %+v, want zero value", u.ExtraUsage)
			}
		})
	}
}

// 1700000000 epoch seconds == 2023-11-14T22:13:20Z.
func TestResetTimeDecoding(t *testing.T) {
	const epoch int64 = 1700000000
	cases := []struct {
		name    string
		json    string
		present bool
		unix    int64
	}{
		{"number", `1700000000`, true, epoch},
		{"fractional number", `1700000000.5`, true, epoch},
		{"numeric string", `"1700000000"`, true, epoch},
		{"rfc3339 string", `"2023-11-14T22:13:20Z"`, true, epoch},
		{"null", `null`, false, 0},
		{"empty string", `""`, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var rt resetTime
			if err := json.Unmarshal([]byte(tc.json), &rt); err != nil {
				t.Fatalf("Unmarshal(%s): %v", tc.json, err)
			}
			if rt.present != tc.present {
				t.Fatalf("present = %v, want %v", rt.present, tc.present)
			}
			if tc.present && rt.t.Unix() != tc.unix {
				t.Errorf("unix = %d, want %d", rt.t.Unix(), tc.unix)
			}
		})
	}

	t.Run("unparseable string is a hard error", func(t *testing.T) {
		var rt resetTime
		if err := json.Unmarshal([]byte(`"not-a-time"`), &rt); err == nil {
			t.Fatal("want a decode error for an unparseable resets_at, got nil")
		}
	})
}

func TestUsageStringResetsAt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"five_hour":{"utilization":40.0,"resets_at":"1700000000"}}`)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	u, err := c.Usage(context.Background(), "abc")
	if err != nil {
		t.Fatalf("Usage with string resets_at: %v", err)
	}
	if u.FiveHour.ResetsAt.Unix() != 1700000000 {
		t.Errorf("resets_at = %v, want unix 1700000000", u.FiveHour.ResetsAt)
	}
}

func TestUsageUserAgentSent(t *testing.T) {
	var ua string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua = r.Header.Get("User-Agent")
		_, _ = io.WriteString(w, `{"five_hour":{"utilization":50.0,"resets_at":1}}`)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	if _, err := c.Usage(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ua, "claude-cli/") {
		t.Errorf("User-Agent = %q, want claude-cli/... form", ua)
	}
}

func TestUsageRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	_, err := c.Usage(context.Background(), "x")
	var ue *UsageError
	if !errors.As(err, &ue) || !ue.RateLimited() {
		t.Fatalf("expected rate-limited UsageError, got %v", err)
	}
}

func TestEndpointsAreClaudeDefaults(t *testing.T) {
	c := New()
	if !strings.Contains(c.tokenURL, "platform.claude.com/v1/oauth/token") {
		t.Errorf("token endpoint = %q", c.tokenURL)
	}
	if !strings.Contains(c.usageURL, "api.anthropic.com/api/oauth/usage") {
		t.Errorf("usage endpoint = %q", c.usageURL)
	}
}
