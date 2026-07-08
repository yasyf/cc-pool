package oauth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestParseRetryAfter pins both Retry-After encodings (delta-seconds, HTTP-date)
// and every malformed/absent shape that must fall back to 0.
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"delta-seconds", "120", 120 * time.Second},
		{"delta-seconds trimmed", "  90 ", 90 * time.Second},
		{"zero", "0", 0},
		{"negative", "-5", 0},
		{"absent", "", 0},
		{"garbage", "soon", 0},
		{"http-date future", now.Add(5 * time.Minute).UTC().Format(http.TimeFormat), 5 * time.Minute},
		{"http-date past", now.Add(-time.Hour).UTC().Format(http.TimeFormat), 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.in, now); got != tc.want {
				t.Fatalf("parseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestUsageRetryAfterHeader pins that a 429's Retry-After header is surfaced on
// UsageError.RetryAfter; a 429 without the header reports 0.
func TestUsageRetryAfterHeader(t *testing.T) {
	tests := []struct {
		name   string
		header string // "" means omit the header
		want   time.Duration
	}{
		{"present", "120", 120 * time.Second},
		{"absent", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("Retry-After", tc.header)
				}
				http.Error(w, "slow down", http.StatusTooManyRequests)
			}))
			defer srv.Close()
			c := New()
			c.usageURL = srv.URL

			_, err := c.Usage(context.Background(), "x")
			var ue *UsageError
			if !errors.As(err, &ue) || !ue.RateLimited() {
				t.Fatalf("want a 429 UsageError, got %v", err)
			}
			if ue.RetryAfter != tc.want {
				t.Fatalf("RetryAfter = %v, want %v", ue.RetryAfter, tc.want)
			}
		})
	}
}
