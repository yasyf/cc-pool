package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestTransportErrClassification pins the source-of-truth classifier: a generic
// transport error and a deadline are network-class; a deliberate cancellation is
// not; the underlying cause is always preserved.
func TestTransportErrClassification(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantNetwork bool
	}{
		{"connection refused", errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"deadline in url.Error", &url.Error{Op: "Get", URL: "http://x", Err: context.DeadlineExceeded}, true},
		{"context canceled", context.Canceled, false},
		{"canceled in url.Error", &url.Error{Op: "Get", URL: "http://x", Err: context.Canceled}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := transportErr("oauth op", tc.err)
			if errors.Is(got, ErrNetwork) != tc.wantNetwork {
				t.Fatalf("Is(ErrNetwork) = %v, want %v (err=%v)", errors.Is(got, ErrNetwork), tc.wantNetwork, got)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("underlying cause not preserved in %v", got)
			}
		})
	}
}

// TestUsageErrorClassification pins the wiring at the Usage call site: a 401/429
// HTTP response is never network-class, a real transport failure is, and a
// cancelled request is classed as the cancellation, not an outage.
func TestUsageErrorClassification(t *testing.T) {
	t.Run("401 is not network-class", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer srv.Close()
		c := New()
		c.usageURL = srv.URL
		_, err := c.Usage(context.Background(), "x")
		var ue *UsageError
		if !errors.As(err, &ue) || !ue.Unauthorized() {
			t.Fatalf("want a 401 UsageError, got %v", err)
		}
		if errors.Is(err, ErrNetwork) {
			t.Fatalf("a 401 must not classify as ErrNetwork: %v", err)
		}
	})

	t.Run("429 is not network-class", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "slow down", http.StatusTooManyRequests)
		}))
		defer srv.Close()
		c := New()
		c.usageURL = srv.URL
		_, err := c.Usage(context.Background(), "x")
		if errors.Is(err, ErrNetwork) {
			t.Fatalf("a 429 must not classify as ErrNetwork: %v", err)
		}
	})

	t.Run("transport failure is network-class", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		u := srv.URL
		srv.Close() // no listener → connection refused
		c := New()
		c.usageURL = u
		_, err := c.Usage(context.Background(), "x")
		if !errors.Is(err, ErrNetwork) {
			t.Fatalf("a transport failure must classify as ErrNetwork, got %v", err)
		}
	})

	t.Run("cancelled request is not network-class", func(t *testing.T) {
		block := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
		defer srv.Close()
		defer close(block)
		c := New()
		c.usageURL = srv.URL
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := c.Usage(ctx, "x")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
		if errors.Is(err, ErrNetwork) {
			t.Fatalf("a cancelled request must not classify as ErrNetwork: %v", err)
		}
	})
}

// TestUsageBodyReadClassification pins the response-body read sites: a transport
// failure mid-body (headers received, connection broken before the body
// completes) is network-class, so a partial 200 never masquerades as a proven
// API answer; a cancellation mid-body is not network-class.
func TestUsageBodyReadClassification(t *testing.T) {
	t.Run("mid-body transport failure is network-class", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("test server does not support hijack")
				return
			}
			conn, buf, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			// Promise 4096 body bytes, send one, then break the connection so the
			// client's body read fails only after a valid 200 status line.
			_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n{")
			_ = buf.Flush()
			_ = conn.Close()
		}))
		defer srv.Close()
		c := New()
		c.usageURL = srv.URL
		_, err := c.Usage(context.Background(), "x")
		if !errors.Is(err, ErrNetwork) {
			t.Fatalf("a mid-body transport failure must classify as ErrNetwork, got %v", err)
		}
	})

	t.Run("cancelled body read is not network-class", func(t *testing.T) {
		bodyStarted := make(chan struct{})
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "4096")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			close(bodyStarted)
			<-release // hang with the body unfinished
		}))
		defer srv.Close()
		defer close(release)
		c := New()
		c.usageURL = srv.URL
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-bodyStarted
			cancel()
		}()
		_, err := c.Usage(ctx, "x")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled from a cancelled body read, got %v", err)
		}
		if errors.Is(err, ErrNetwork) {
			t.Fatalf("a cancelled body read must not classify as ErrNetwork: %v", err)
		}
	})
}

// TestRefreshErrorClassification pins the same wiring at the Refresh call site.
func TestRefreshErrorClassification(t *testing.T) {
	t.Run("revoked 400 is not network-class", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		}))
		defer srv.Close()
		c := New()
		c.tokenURL = srv.URL
		_, err := c.Refresh(context.Background(), "k", "rt")
		if errors.Is(err, ErrNetwork) {
			t.Fatalf("a 400 revoked refresh must not classify as ErrNetwork: %v", err)
		}
	})

	t.Run("transport failure is network-class", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		u := srv.URL
		srv.Close()
		c := New()
		c.tokenURL = u
		_, err := c.Refresh(context.Background(), "k", "rt")
		if !errors.Is(err, ErrNetwork) {
			t.Fatalf("a transport failure must classify as ErrNetwork, got %v", err)
		}
	})
}

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

// TestRefreshRevoked pins the RefreshError classification per status and OAuth
// error-body code: Revoked is status-driven (400/401), InvalidGrant requires
// the server-confirmed invalid_grant code — a plain 401 or a codeless body
// must never read as a confirmed revocation.
func TestRefreshRevoked(t *testing.T) {
	cases := []struct {
		name             string
		status           int
		body             string
		wantRevoked      bool
		wantInvalidGrant bool
	}{
		{
			name:   "400 invalid_grant is revoked and confirmed",
			status: http.StatusBadRequest, body: `{"error":"invalid_grant"}`,
			wantRevoked: true, wantInvalidGrant: true,
		},
		{
			name:   "401 invalid_grant is revoked and confirmed",
			status: http.StatusUnauthorized, body: `{"error":"invalid_grant"}`,
			wantRevoked: true, wantInvalidGrant: true,
		},
		{
			name:   "401 with another code is revoked but unconfirmed",
			status: http.StatusUnauthorized, body: `{"error":"invalid_client"}`,
			wantRevoked: true, wantInvalidGrant: false,
		},
		{
			name:   "401 with a non-JSON body is revoked but unconfirmed",
			status: http.StatusUnauthorized, body: `unauthorized`,
			wantRevoked: true, wantInvalidGrant: false,
		},
		{
			name:   "401 with an empty body is revoked but unconfirmed",
			status: http.StatusUnauthorized, body: ``,
			wantRevoked: true, wantInvalidGrant: false,
		},
		{
			name:   "500 is neither revoked nor confirmed",
			status: http.StatusInternalServerError, body: `{"error":"server_error"}`,
			wantRevoked: false, wantInvalidGrant: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			c := New()
			c.tokenURL = srv.URL
			_, err := c.Refresh(context.Background(), "k", "rt")
			var re *RefreshError
			if !errors.As(err, &re) {
				t.Fatalf("expected a RefreshError, got %v", err)
			}
			if re.Revoked() != tc.wantRevoked {
				t.Errorf("Revoked() = %v, want %v", re.Revoked(), tc.wantRevoked)
			}
			if re.InvalidGrant() != tc.wantInvalidGrant {
				t.Errorf("InvalidGrant() = %v, want %v (Code=%q)", re.InvalidGrant(), tc.wantInvalidGrant, re.Code)
			}
		})
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
			"extra_usage":{"is_enabled":true,"monthly_limit":5000,"used_credits":177.0,"utilization":3.54,"currency":"USD","disabled_reason":null},
			"limits":[
				{"kind":"session","group":"session","percent":12,"severity":"normal","resets_at":"2026-07-03T11:49:59.564928+00:00","scope":null,"is_active":false},
				{"kind":"weekly_all","group":"weekly","percent":60,"severity":"normal","resets_at":"2026-07-08T16:59:59.564947+00:00","scope":null,"is_active":false}
			]
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

func TestUsageParsesScopedWeeklyLimits(t *testing.T) {
	// Live-captured 2026-07-03 payload: session/weekly_all carry no scope and
	// duplicate the five_hour/seven_day windows; only weekly_scoped is kept.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{
			"five_hour":{"utilization":40.0,"resets_at":1700000000},
			"seven_day":{"utilization":60.0,"resets_at":1700600000},
			"limits":[
				{"kind":"session","group":"session","percent":12,"severity":"normal","resets_at":"2026-07-03T11:49:59.564928+00:00","scope":null,"is_active":false},
				{"kind":"weekly_all","group":"weekly","percent":60,"severity":"normal","resets_at":"2026-07-08T16:59:59.564947+00:00","scope":null,"is_active":false},
				{"kind":"weekly_scoped","group":"weekly","percent":100,"severity":"critical","resets_at":"2026-07-08T16:59:59.565167+00:00","scope":{"model":{"id":null,"display_name":"Fable"},"surface":null},"is_active":true}
			]
		}`)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	u, err := c.Usage(context.Background(), "abc")
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if len(u.ScopedWeekly) != 1 {
		t.Fatalf("ScopedWeekly len = %d, want 1: %+v", len(u.ScopedWeekly), u.ScopedWeekly)
	}
	sw := u.ScopedWeekly[0]
	if sw.ModelName != "Fable" {
		t.Errorf("ModelName = %q, want %q", sw.ModelName, "Fable")
	}
	if sw.Used() != 100 {
		t.Errorf("Used = %.1f, want 100", sw.Used())
	}
	wantReset, err := time.Parse(time.RFC3339, "2026-07-08T16:59:59.565167+00:00")
	if err != nil {
		t.Fatal(err)
	}
	if !sw.ResetsAt.Equal(wantReset) {
		t.Errorf("ResetsAt = %v, want %v", sw.ResetsAt, wantReset)
	}
}

func TestUsageScopedWeeklyMalformedSkipped(t *testing.T) {
	// nil scope, nil model, empty/whitespace display_name, and a
	// weekly_all-with-model entry must all be dropped, leaving nothing.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{
			"five_hour":{"utilization":10.0,"resets_at":null},
			"seven_day":{"utilization":5.0,"resets_at":null},
			"limits":[
				{"kind":"weekly_scoped","percent":90,"resets_at":null,"scope":null},
				{"kind":"weekly_scoped","percent":90,"resets_at":null,"scope":{"model":null,"surface":null}},
				{"kind":"weekly_scoped","percent":90,"resets_at":null,"scope":{"model":{"id":null,"display_name":""}}},
				{"kind":"weekly_scoped","percent":90,"resets_at":null,"scope":{"model":{"id":null,"display_name":"   "}}},
				{"kind":"weekly_all","percent":80,"resets_at":null,"scope":{"model":{"id":null,"display_name":"Fable"}}}
			]
		}`)
	}))
	defer srv.Close()
	c := New()
	c.usageURL = srv.URL
	u, err := c.Usage(context.Background(), "abc")
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if len(u.ScopedWeekly) != 0 {
		t.Fatalf("ScopedWeekly = %+v, want empty", u.ScopedWeekly)
	}
	if _, ok := u.BindingScoped(); ok {
		t.Error("BindingScoped ok = true, want false for empty scoped weekly")
	}
}

func TestUsageBindingScoped(t *testing.T) {
	reset := time.Unix(1700600000, 0)
	cases := []struct {
		name      string
		scoped    []ScopedWindow
		wantOK    bool
		wantModel string
		wantUsed  float64
	}{
		{name: "empty", scoped: nil, wantOK: false},
		{
			name:      "single",
			scoped:    []ScopedWindow{{ModelName: "Fable", Window: Window{Utilization: 42, ResetsAt: reset}}},
			wantOK:    true,
			wantModel: "Fable",
			wantUsed:  42,
		},
		{
			name: "max utilization wins",
			scoped: []ScopedWindow{
				{ModelName: "Opus", Window: Window{Utilization: 30}},
				{ModelName: "Fable", Window: Window{Utilization: 100}},
				{ModelName: "Sonnet", Window: Window{Utilization: 70}},
			},
			wantOK:    true,
			wantModel: "Fable",
			wantUsed:  100,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &Usage{ScopedWeekly: tc.scoped}
			sw, ok := u.BindingScoped()
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if sw.ModelName != tc.wantModel {
				t.Errorf("ModelName = %q, want %q", sw.ModelName, tc.wantModel)
			}
			if sw.Used() != tc.wantUsed {
				t.Errorf("Used = %.1f, want %.1f", sw.Used(), tc.wantUsed)
			}
		})
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
