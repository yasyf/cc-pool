// Command fakeoauth is a hermetic stand-in for Anthropic's OAuth token and
// usage endpoints, one instance per simulated host in the two-host sync sim
// (scripts/sync-sim/run.sh). cc-pool's oauth client is pointed at it via the
// CLAUDE_POOL_TOKEN_URL / CLAUDE_POOL_USAGE_URL env seams, so the sim never
// reaches the real endpoints and every token POST a host makes is its own,
// exactly countable.
//
// The load-bearing invariant is SINGLE-USE ROTATION: each refresh token is
// redeemable exactly once. The first redemption mints a fresh access+refresh
// pair and marks the presented token spent; ANY later redemption of a spent
// token is a double-spend — it returns invalid_grant AND kills the whole chain
// family (every descendant access token goes invalid), exactly as Anthropic's
// rotation does. Every /token POST is appended to a JSONL log so per-host
// counts and any double-spend are inspectable; /admin/report exposes the tally.
//
// Endpoints:
//
//	POST /v1/oauth/token   refresh_token grant — single-use rotation
//	GET  /api/oauth/usage  canned windows if the bearer access token is live, else 401
//	POST /admin/seed       register a chain's initial access+refresh pair as live
//	GET  /admin/report     {host, tokenPosts, doubleSpends}
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// rtState tracks one refresh token: its chain family, generation, and whether
// it has already been redeemed (single-use).
type rtState struct {
	family string
	gen    int
	spent  bool
}

// atState tracks one access token: its chain family and absolute expiry.
type atState struct {
	family      string
	expiresAtMs int64
}

type server struct {
	mu          sync.Mutex
	host        string
	atLifetime  time.Duration
	families    map[string]bool // family -> alive
	rts         map[string]*rtState
	ats         map[string]*atState
	tokenPosts  int
	doubleSpend int
	log         *os.File
}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address (:0 picks a free port)")
	host := flag.String("host", "", "host label recorded in the request log")
	logPath := flag.String("log", "", "JSONL request-log path (required)")
	portFile := flag.String("portfile", "", "write the bound host:port here once listening")
	atLifetimeSec := flag.Int("at-lifetime-sec", 8*60*60, "minted access-token lifetime in seconds")
	flag.Parse()

	if *logPath == "" {
		fmt.Fprintln(os.Stderr, "fakeoauth: --log is required")
		os.Exit(2)
	}
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeoauth: open log %s: %v\n", *logPath, err)
		os.Exit(1)
	}
	defer func() { _ = lf.Close() }()

	s := &server{
		host:       *host,
		atLifetime: time.Duration(*atLifetimeSec) * time.Second,
		families:   map[string]bool{},
		rts:        map[string]*rtState{},
		ats:        map[string]*atState{},
		log:        lf,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/oauth/token", s.handleToken)
	mux.HandleFunc("/api/oauth/usage", s.handleUsage)
	mux.HandleFunc("/admin/seed", s.handleSeed)
	mux.HandleFunc("/admin/report", s.handleReport)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeoauth: listen %s: %v\n", *addr, err)
		os.Exit(1)
	}
	bound := ln.Addr().String()
	if *portFile != "" {
		if err := os.WriteFile(*portFile, []byte(bound), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "fakeoauth: write portfile: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "fakeoauth[%s]: listening on %s\n", *host, bound)
	if err := http.Serve(ln, mux); err != nil {
		fmt.Fprintf(os.Stderr, "fakeoauth: serve: %v\n", err)
		os.Exit(1)
	}
}

// writeLogLine appends one JSONL record; the caller holds s.mu.
func (s *server) writeLogLine(fields map[string]any) {
	fields["ts"] = time.Now().UnixNano()
	fields["host"] = s.host
	b, err := json.Marshal(fields)
	if err != nil {
		return
	}
	_, _ = s.log.Write(append(b, '\n'))
}

func (s *server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method_not_allowed"})
		return
	}
	var body struct {
		GrantType    string `json:"grant_type"`
		RefreshToken string `json:"refresh_token"`
		ClientID     string `json:"client_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_request"})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenPosts++
	rt := body.RefreshToken
	st, ok := s.rts[rt]
	switch {
	case body.GrantType != "refresh_token":
		s.writeLogLine(map[string]any{"rt": rt, "outcome": "bad_grant_type"})
		invalidGrant(w)
	case !ok:
		s.writeLogLine(map[string]any{"rt": rt, "outcome": "unknown_rt"})
		invalidGrant(w)
	case !s.families[st.family]:
		s.writeLogLine(map[string]any{"rt": rt, "family": st.family, "outcome": "dead_family"})
		invalidGrant(w)
	case st.spent:
		// Single-use violated: kill the whole chain family, matching rotation.
		s.families[st.family] = false
		s.doubleSpend++
		s.writeLogLine(map[string]any{"rt": rt, "family": st.family, "outcome": "double_spend"})
		invalidGrant(w)
	default:
		st.spent = true
		newGen := st.gen + 1
		newAT := fmt.Sprintf("ATOK-%s-g%d", st.family, newGen)
		newRT := fmt.Sprintf("RTSECRET-%s-g%d", st.family, newGen)
		s.rts[newRT] = &rtState{family: st.family, gen: newGen}
		s.ats[newAT] = &atState{family: st.family, expiresAtMs: time.Now().Add(s.atLifetime).UnixMilli()}
		s.writeLogLine(map[string]any{"rt": rt, "family": st.family, "outcome": "rotated", "newAt": newAT, "newRt": newRT})
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token":  newAT,
			"refresh_token": newRT,
			"expires_in":    int(s.atLifetime.Seconds()),
			"token_type":    "Bearer",
			"scope":         "user:inference user:profile",
		})
	}
}

func (s *server) handleUsage(w http.ResponseWriter, r *http.Request) {
	at := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	st, ok := s.ats[at]
	live := ok && s.families[st.family] && time.Now().UnixMilli() < st.expiresAtMs
	s.mu.Unlock()
	if !live {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid_token"})
		return
	}
	resetsAt := time.Now().Add(2 * time.Hour).Unix()
	writeJSON(w, http.StatusOK, map[string]any{
		"five_hour": map[string]any{"utilization": 13.0, "resets_at": resetsAt},
		"seven_day": map[string]any{"utilization": 22.0, "resets_at": time.Now().Add(72 * time.Hour).Unix()},
	})
}

func (s *server) handleSeed(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Family      string `json:"family"`
		Gen         int    `json:"gen"`
		Access      string `json:"access"`
		Refresh     string `json:"refresh"`
		ExpiresAtMs int64  `json:"expiresAtMs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Family == "" || body.Refresh == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_request"})
		return
	}
	gen := body.Gen
	if gen == 0 {
		gen = 1
	}
	s.mu.Lock()
	s.families[body.Family] = true
	s.rts[body.Refresh] = &rtState{family: body.Family, gen: gen}
	if body.Access != "" {
		s.ats[body.Access] = &atState{family: body.Family, expiresAtMs: body.ExpiresAtMs}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleReport(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"host":         s.host,
		"tokenPosts":   s.tokenPosts,
		"doubleSpends": s.doubleSpend,
	})
}

func invalidGrant(w http.ResponseWriter) {
	writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_grant"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
