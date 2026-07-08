package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// The daemon's Manager is fed the shared credstest seam fake; fakeOAuth is
// internally locked so -race points at code under test.
var _ pool.Credentials = (*credstest.Fake)(nil)

type fakeOAuth struct {
	mu         sync.Mutex
	currentRT  string
	refreshes  int
	usageCalls int
	usage401   bool
	refresh5xx bool

	// usageNet makes every Usage return a network-class (oauth.ErrNetwork) error.
	usageNet   bool
	usage429   bool            // every Usage returns a 429 UsageError
	rlByAT     map[string]bool // scope the 429 to specific access tokens
	retryAfter time.Duration   // Retry-After stamped onto the 429
	// netByAT overrides the response for a specific access token so a
	// multi-account sweep can diverge per account; usageByAT counts calls per
	// access token so a test can assert exactly which accounts were probed.
	netByAT   map[string]bool
	usageByAT map[string]int
}

// netError returns an error that classifies as an oauth network outage.
func netError() error { return fmt.Errorf("simulated transport failure: %w", oauth.ErrNetwork) }

func (f *fakeOAuth) Refresh(_ context.Context, _, refreshToken string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refresh5xx {
		// 503 is not Revoked(): a recoverable blip, not needs-login.
		return nil, &oauth.RefreshError{Status: 503, Body: "service unavailable"}
	}
	if refreshToken != f.currentRT {
		return nil, &oauth.RefreshError{Status: 400, Body: "invalid_grant"}
	}
	f.refreshes++
	f.currentRT = fmt.Sprintf("rt-%d", f.refreshes)
	return &oauth.TokenResponse{
		AccessToken:  fmt.Sprintf("at-%d", f.refreshes),
		RefreshToken: f.currentRT,
		ExpiresIn:    3600,
	}, nil
}

func (f *fakeOAuth) refreshCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshes
}

func (f *fakeOAuth) Usage(_ context.Context, at string) (*oauth.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageCalls++
	if f.usageByAT == nil {
		f.usageByAT = map[string]int{}
	}
	f.usageByAT[at]++
	if f.usageNet || f.netByAT[at] {
		return nil, netError()
	}
	if f.usage429 || f.rlByAT[at] {
		return nil, &oauth.UsageError{Status: 429, Body: "rate limited", RetryAfter: f.retryAfter}
	}
	if f.usage401 {
		return nil, &oauth.UsageError{Status: 401, Body: `{"type":"error"}`}
	}
	return &oauth.Usage{}, nil
}

func (f *fakeOAuth) usageCountFor(at string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usageByAT[at]
}

func (f *fakeOAuth) usageCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usageCalls
}

func (f *fakeOAuth) setUsage401(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usage401 = v
}

// TestPollOnceSkipsReservedAccountRefresh pins that a reserved account (just
// selected, claude not yet scannable) is never refreshed or adopted out from
// under the launching session; an expired reservation refreshes as usual.
func TestPollOnceSkipsReservedAccountRefresh(t *testing.T) {
	// Redirect ClaudeDir/StateDir off the real ~/.claude and ~/.cc-pool.
	t.Setenv("HOME", t.TempDir())

	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so an idle poll must refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	seedWrites := fk.WriteCount()
	fo := &fakeOAuth{currentRT: "rt-0"}

	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.cl.reserve(a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("reserved account was POST-refreshed %d time(s)", got)
	}
	if got := fk.WriteCount(); got != seedWrites {
		t.Fatalf("reserved account's credential was written %d time(s)", got-seedWrites)
	}

	s.cl.mu.Lock()
	s.cl.reservations[a.ID] = time.Now().Add(-reservationTTL - time.Second)
	s.cl.mu.Unlock()
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
	}
}

// TestPollOnceFailsClosedOnScanError pins that a failed scan makes pollOnce treat
// every account as busy (no idle refresh, no adopt); a clean scan still refreshes.
func TestPollOnceFailsClosedOnScanError(t *testing.T) {
	setup := func(t *testing.T, scan func(context.Context) ([]procscan.Session, error)) (*Server, *fakeOAuth, *credstest.Fake) {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		a := store.Account{
			ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
			KeychainService: "svc", KeychainAccount: "user",
		}
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		fk := credstest.NewFake()
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-0"
		cred.ClaudeAiOauth.RefreshToken = "rt-0"
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli() // near-expiry
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
		fo := &fakeOAuth{currentRT: "rt-0"}
		s := &Server{
			m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
			snapshot:     filepath.Join(t.TempDir(), "status.json"),
			log:          log.New(io.Discard, "", 0),
			scanSessions: scan,
			cl:           newClaims(),
			led:          newLedgers(),
		}
		return s, fo, fk
	}

	t.Run("scan error refreshes and adopts nothing", func(t *testing.T) {
		s, fo, fk := setup(t, func(context.Context) ([]procscan.Session, error) {
			return nil, fmt.Errorf("procscan: simulated EIO")
		})
		before := fk.WriteCount()
		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("scan-failed poll refreshed %d time(s), want 0", got)
		}
		if got := fk.WriteCount(); got != before {
			t.Fatalf("scan-failed poll wrote the credential %d time(s) (adopt must be skipped), want 0", got-before)
		}
	})

	t.Run("clean scan refreshes the idle near-expiry account", func(t *testing.T) {
		s, fo, _ := setup(t, func(context.Context) ([]procscan.Session, error) { return nil, nil })
		s.pollOnce(t.Context())
		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("clean idle poll refreshed %d time(s), want 1", got)
		}
	})
}

// TestPollOnceFlagsAndRecoversNeedsLogin pins that a definitive 401 flags
// needs-login and a recovered credential clears it on the next due poll.
func TestPollOnceFlagsAndRecoversNeedsLogin(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "" // no refresh token → a 401 is definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true}

	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("definitive 401 should flag needs-login")
	}
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("no refresh token to spend, but refreshed %d time(s)", got)
	}

	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo.setUsage401(false)
	// Move the throttle clock past the 15m window so the recovery poll is due.
	s.led.row(authStreakPolicy, a.ConfigDir).lastAt = time.Now().Add(-needsLoginPollInterval - time.Second)

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("a recovered account should clear needs-login")
	}
}

// TestPollOnceTransient401StaysSelectable pins that transient refresh failures
// (non-Revoked 5xx) never flag needs-login: the 401 streak only arms the poll
// backoff and the account stays selectable throughout.
func TestPollOnceTransient401StaysSelectable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0" // present → a 401 is recoverable, not definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true, refresh5xx: true}

	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	for i := 1; i <= needsLoginAfter; i++ {
		s.pollOnce(t.Context())
		// The 401 streak advances by one per transient failure and latches the
		// needs-login gate (faulted) on the needsLoginAfter-th, where the debounce
		// resets strikes to 0.
		l := s.led.peek(authStreakPolicy, a.ConfigDir)
		switch {
		case i < needsLoginAfter:
			if l == nil || l.strikes != i || l.faulted {
				t.Fatalf("after %d transient 401(s), auth streak = %+v, want strikes %d unfaulted", i, l, i)
			}
		case l == nil || !l.faulted:
			t.Fatalf("the %dth transient 401 must latch the needs-login gate (faulted): %+v", i, l)
		}
		h, _ := st.GetAuthHealth(1)
		if h.NeedsLogin {
			t.Fatalf("transient 401 #%d falsely flagged needs-login", i)
		}
		if !score.Score(score.Input{AccountID: 1, NeedsLogin: h.NeedsLogin}, time.Now()).Available {
			t.Fatalf("transient 401 #%d made the account unselectable", i)
		}
	}

	// The needsLoginAfter-th failure arms the backoff: the next poll skips sampling.
	before := fo.usageCallCount()
	s.pollOnce(t.Context())
	if got := fo.usageCallCount(); got != before {
		t.Fatalf("backed-off account was still sampled: Usage called %d extra time(s)", got-before)
	}
	if h, _ := st.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("poll backoff must not flag needs-login")
	}
}

// TestPollOnceFlagsConfirmedRevocation pins that a confirmed revocation (400
// invalid_grant, credential unchanged on re-read) flags needs-login on the
// first poll, independent of the transient-401 streak.
func TestPollOnceFlagsConfirmedRevocation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	cred := &creds.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-stale" // ≠ server's currentRT → invalid_grant
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, cred)
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true}

	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		cl:           newClaims(),
		led:          newLedgers(),
	}

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("a confirmed 400 invalid_grant revocation must flag needs-login")
	}
	if l := s.led.peek(authStreakPolicy, a.ConfigDir); l != nil && (l.strikes != 0 || l.faulted) {
		t.Fatalf("a confirmed revocation flags via ErrNeedsLogin and must not touch the 401 streak; strikes=%d faulted=%v", l.strikes, l.faulted)
	}
}

// newOutageServer builds a daemon Server with n idle accounts, each holding a
// distinct, unexpired credential (access token at-<i>), and a nil session scan
// so every account reads idle. The shared fakeOAuth drives per-account Usage
// behavior via its usageNet/netByAT fields.
func newOutageServer(t *testing.T, n int) (*Server, *fakeOAuth) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	fk := credstest.NewFake()
	for i := 1; i <= n; i++ {
		a := store.Account{
			ID: i, ConfigDir: filepath.Join(t.TempDir(), fmt.Sprintf("acct-%d", i)),
			KeychainService: fmt.Sprintf("svc-%d", i), KeychainAccount: "user",
		}
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = fmt.Sprintf("at-%d", i)
		cred.ClaudeAiOauth.RefreshToken = fmt.Sprintf("rt-%d", i)
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
	}
	fo := &fakeOAuth{currentRT: "rt-0"}
	s := &Server{
		m:            &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
		snapshot:     filepath.Join(t.TempDir(), "status.json"),
		log:          log.New(io.Discard, "", 0),
		scanSessions: func(context.Context) ([]procscan.Session, error) { return nil, nil },
		pollSpacing:  time.Millisecond, // keep multi-account sweeps out of real-time sleeps
		cl:           newClaims(),
		led:          newLedgers(),
	}
	return s, fo
}

// TestPollOnceEntersNetOutage pins that a full sweep whose every attempted
// account fails network-class enters outage mode, and that the network arm of
// handleAuthOutcome leaves every auth counter untouched.
func TestPollOnceEntersNetOutage(t *testing.T) {
	s, fo := newOutageServer(t, 1)
	fo.usageNet = true

	s.pollOnce(t.Context())

	if !s.netOutage {
		t.Fatal("an all-network-fail sweep must enter outage mode")
	}
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		t.Fatal(err)
	}
	dir := accts[0].ConfigDir
	if l := s.led.peek(authStreakPolicy, dir); l != nil {
		t.Fatalf("a network error must not touch the auth streak: %+v", l)
	}
	if l := s.led.peek(acctRateLimitPolicy, dir); l != nil {
		t.Fatalf("a network error must not arm the per-account 429 streak: %+v", l)
	}
	if h, _ := s.m.Store.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("a network error must not flag needs-login")
	}
}

// TestPollOnceMixedSweepStaysHealthy pins that a single reachable account keeps
// the fleet out of outage mode even when others fail network-class.
func TestPollOnceMixedSweepStaysHealthy(t *testing.T) {
	s, fo := newOutageServer(t, 2)
	fo.netByAT = map[string]bool{"at-1": true} // acct1 fails, acct2 succeeds

	s.pollOnce(t.Context())

	if s.netOutage {
		t.Fatal("a mixed sweep (one account reachable) must NOT enter outage mode")
	}
}

// TestPollOnceOutageProbesOnlyCanary pins that, while in outage mode, pollOnce
// samples only the first pollable account (the canary); a still-failing canary
// keeps the outage and never fans out to the rest.
func TestPollOnceOutageProbesOnlyCanary(t *testing.T) {
	s, fo := newOutageServer(t, 2)
	fo.usageNet = true // the network is still down
	s.netOutage = true

	s.pollOnce(t.Context())

	if !s.netOutage {
		t.Fatal("a still-failing canary must keep outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("canary (acct1) sampled %d time(s), want 1", got)
	}
	if got := fo.usageCountFor("at-2"); got != 0 {
		t.Fatalf("non-canary (acct2) sampled %d time(s), want 0 — outage polls only the canary", got)
	}
}

// TestPollOnceCanaryRecoveryRunsFullSweep pins that a canary answering anything
// non-network exits outage mode and re-samples the whole fleet in the same
// invocation.
func TestPollOnceCanaryRecoveryRunsFullSweep(t *testing.T) {
	s, fo := newOutageServer(t, 2) // both accounts answer now
	s.netOutage = true

	s.pollOnce(t.Context())

	if s.netOutage {
		t.Fatal("a recovered canary must exit outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("canary sampled %d time(s), want 1", got)
	}
	if got := fo.usageCountFor("at-2"); got != 1 {
		t.Fatalf("acct2 sampled %d time(s), want 1 — the recovery sweep must re-reach previously-skipped accounts", got)
	}
}

// TestPollOnceRecoverySweepReentersOutage pins that a recovery sweep whose
// post-canary accounts all fail network-class (connectivity dropped again after
// the canary reached the API) re-enters outage, and abandons the sweep early once
// recoveryAbandonThreshold consecutive failures accumulate so the trailing
// accounts aren't each charged a sample timeout.
func TestPollOnceRecoverySweepReentersOutage(t *testing.T) {
	s, fo := newOutageServer(t, 5)
	// The canary (acct1) reaches the API and flips recovery; every other account
	// then fails network-class.
	fo.netByAT = map[string]bool{"at-2": true, "at-3": true, "at-4": true, "at-5": true}
	s.netOutage = true

	s.pollOnce(t.Context())

	if !s.netOutage {
		t.Fatal("a recovery sweep that all-network-fails must re-enter outage mode")
	}
	if got := fo.usageCountFor("at-1"); got != 1 {
		t.Fatalf("canary sampled %d time(s), want 1", got)
	}
	for _, at := range []string{"at-2", "at-3", "at-4"} {
		if got := fo.usageCountFor(at); got != 1 {
			t.Fatalf("%s sampled %d time(s), want 1 (recovery samples before abandon)", at, got)
		}
	}
	if got := fo.usageCountFor("at-5"); got != 0 {
		t.Fatalf("acct5 sampled %d time(s), want 0 — the recovery sweep must abandon after %d consecutive network failures", got, recoveryAbandonThreshold)
	}
}

// TestPollOnceOutageLogThrottle pins that, while an outage persists, the
// per-canary "network unreachable" line logs only once per netProbeLogEvery
// probes so a multi-hour outage doesn't spam the log.
func TestPollOnceOutageLogThrottle(t *testing.T) {
	s, fo := newOutageServer(t, 1)
	fo.usageNet = true
	var buf bytes.Buffer
	s.log = log.New(&buf, "", 0)
	s.netOutage = true

	// Each outage poll runs exactly one canary probe; logs fire on probes
	// 1, netProbeLogEvery+1, 2*netProbeLogEvery+1, ...
	const polls = netProbeLogEvery*2 + 1
	for i := 0; i < polls; i++ {
		s.pollOnce(t.Context())
	}
	if !s.netOutage {
		t.Fatal("a still-down canary must keep outage mode")
	}
	if got, want := strings.Count(buf.String(), "network unreachable"), 3; got != want {
		t.Fatalf("network-unreachable lines = %d over %d probes, want %d (throttled 1-per-%d)", got, polls, want, netProbeLogEvery)
	}
}

// TestPollOnceRecoverySweepHealsBusy401 pins that the post-outage recovery sweep
// bypasses the busy-refresh streak gate (a busy account whose token expired
// during the outage refreshes same-pass), while a normal first-401 busy account
// still does not refresh (the existing heuristic gate is preserved).
func TestPollOnceRecoverySweepHealsBusy401(t *testing.T) {
	newBusy := func(t *testing.T) (*Server, *fakeOAuth) {
		t.Helper()
		t.Setenv("HOME", t.TempDir())
		st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		a := store.Account{
			ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
			KeychainService: "svc", KeychainAccount: "user",
		}
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		fk := credstest.NewFake()
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-0"
		cred.ClaudeAiOauth.RefreshToken = "rt-0"
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli() // expired
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
		fo := &fakeOAuth{currentRT: "rt-0", usage401: true}
		s := &Server{
			m:        &pool.Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()},
			snapshot: filepath.Join(t.TempDir(), "status.json"),
			log:      log.New(io.Discard, "", 0),
			scanSessions: func(context.Context) ([]procscan.Session, error) { // a live session → busy
				return []procscan.Session{{PID: 4242, ConfigDir: a.ConfigDir, StartedAt: time.Now()}}, nil
			},
			cl:  newClaims(),
			led: newLedgers(),
		}
		return s, fo
	}

	t.Run("recovery sweep refreshes a busy expired token", func(t *testing.T) {
		s, fo := newBusy(t)
		s.netOutage = true // the canary probe runs under recovery semantics

		s.pollOnce(t.Context())

		if got := fo.refreshCount(); got != 1 {
			t.Fatalf("recovery sweep refreshed %d time(s), want 1 (the busy-refresh streak gate must be bypassed)", got)
		}
		if s.netOutage {
			t.Fatal("the canary's 401 answer proves connectivity and must exit outage")
		}
	})

	t.Run("normal poll leaves a busy first-401 unrefreshed", func(t *testing.T) {
		s, fo := newBusy(t)

		s.pollOnce(t.Context())

		if got := fo.refreshCount(); got != 0 {
			t.Fatalf("a normal first-401 busy account refreshed %d time(s), want 0 (streak gate preserved)", got)
		}
		accts, err := s.m.Store.ListAccounts()
		if err != nil {
			t.Fatal(err)
		}
		if l := s.led.peek(authStreakPolicy, accts[0].ConfigDir); l == nil || l.strikes != 1 || l.faulted {
			t.Fatalf("auth streak = %+v, want strikes 1 (a busy 401 arms the streak)", l)
		}
	})
}
