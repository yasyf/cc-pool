package daemon

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// fakeKeychain / fakeOAuth mirror the fakes in internal/pool's tests (test
// helpers aren't importable across packages). Both are internally locked so
// any -race report points at code under test.

type fakeKeychain struct {
	mu     sync.Mutex
	items  map[string]*keychain.Credential
	writes int
}

func newFakeKeychain() *fakeKeychain {
	return &fakeKeychain{items: map[string]*keychain.Credential{}}
}

func (f *fakeKeychain) key(service, account string) string { return service + "\x00" + account }

func (f *fakeKeychain) Read(service, account string) (*keychain.Credential, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.items[f.key(service, account)]
	if !ok {
		return nil, keychain.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (f *fakeKeychain) Write(service, account string, cred *keychain.Credential) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *cred
	f.items[f.key(service, account)] = &cp
	f.writes++
	return nil
}

func (f *fakeKeychain) Delete(service, account string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, f.key(service, account))
	return nil
}

func (f *fakeKeychain) Discover(service string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := service + "\x00"
	for k := range f.items {
		if strings.HasPrefix(k, prefix) {
			return strings.TrimPrefix(k, prefix), nil
		}
	}
	return "", keychain.ErrNotFound
}

func (f *fakeKeychain) writeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

type fakeOAuth struct {
	mu         sync.Mutex
	currentRT  string
	refreshes  int
	usageCalls int  // total Usage() invocations (lets a test assert poll backoff)
	usage401   bool // when set, Usage returns a 401 UsageError
	refresh5xx bool // when set, Refresh fails transiently (503, non-Revoked)
}

func (f *fakeOAuth) Refresh(_ context.Context, _, refreshToken string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refresh5xx {
		// A 5xx is not Revoked(), so the sample surfaces the original 401 — the
		// recoverable blip Fix 2 must not escalate to needs-login.
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

func (f *fakeOAuth) Usage(context.Context, string) (*oauth.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageCalls++
	if f.usage401 {
		return nil, &oauth.UsageError{Status: 401, Body: `{"type":"error"}`}
	}
	return &oauth.Usage{}, nil
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

// TestPollOnceSkipsReservedAccountRefresh pins the reservation-aware idle
// decision: an account just handed out by handleSelect (reserved, claude not
// yet visible to procscan) must not have its near-expiry token POST-refreshed
// or adopted out from under the launching session; once the reservation
// expires, the scheduler refreshes as usual.
func TestPollOnceSkipsReservedAccountRefresh(t *testing.T) {
	// Redirect ClaudeDir/StateDir off the real ~/.claude and ~/.cc-pool.
	t.Setenv("HOME", t.TempDir())

	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so an idle poll must refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	seedWrites := fk.writeCount()
	fo := &fakeOAuth{currentRT: "rt-0"}

	s := &Server{
		m:               &pool.Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()},
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		reservations:    map[int]time.Time{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}

	// Reserved: the poll must neither refresh nor adopt (no credential writes).
	s.tryReserve(a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("reserved account was POST-refreshed %d time(s)", got)
	}
	if got := fk.writeCount(); got != seedWrites {
		t.Fatalf("reserved account's credential was written %d time(s)", got-seedWrites)
	}

	// Reservation expired: the account reads idle again and refreshes as usual.
	s.mu.Lock()
	s.reservations[a.ID] = time.Now().Add(-reservationTTL - time.Second)
	s.mu.Unlock()
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
	}
}

// TestPollOnceFlagsAndRecoversNeedsLogin pins the end-to-end auth-health flow: a
// definitive 401 (no refresh token) flags the account needs-login in the store,
// the flag backs sampling off, and a recovered credential clears it on the next
// due poll.
func TestPollOnceFlagsAndRecoversNeedsLogin(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "" // no refresh token → a 401 is definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true}

	s := &Server{
		m:               &pool.Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()},
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		reservations:    map[int]time.Time{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}

	// Poll 1: a 401 with no refresh token flags needs-login immediately.
	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("definitive 401 should flag needs-login")
	}
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("no refresh token to spend, but refreshed %d time(s)", got)
	}

	// Recover the credential and clear the API 401, then advance past the
	// needs-login backoff so the account is due for another sample.
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	fo.setUsage401(false)
	s.lastAuthAttempt[1] = time.Now().Add(-needsLoginPollInterval - time.Second)

	// Poll 2: a clean sample clears the flag.
	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("a recovered account should clear needs-login")
	}
}

// TestPollOnceTransient401StaysSelectable pins Fix 2: an account whose refresh
// keeps failing transiently (5xx → not Revoked, so the sample surfaces the
// original plain 401) is NEVER flagged needs-login. The 401 streak only backs
// the poll off to needsLoginPollInterval; AuthHealth.NeedsLogin stays false and
// the account stays selectable. After the streak arms the backoff, the next due
// poll skips sampling entirely so the 401 spam stops.
func TestPollOnceTransient401StaysSelectable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0" // present → a 401 is recoverable, not definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	// Usage 401s and every refresh fails transiently (503, not Revoked) with the
	// credential unchanged: the sample surfaces the original 401.
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true, refresh5xx: true}

	s := &Server{
		m:               &pool.Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()},
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		reservations:    map[int]time.Time{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}

	// needsLoginAfter consecutive transient 401s: the streak climbs but the
	// account is never flagged and stays selectable on every poll.
	for i := 1; i <= needsLoginAfter; i++ {
		s.pollOnce(t.Context())
		if got := s.authStreak[1]; got != i {
			t.Fatalf("after %d transient 401(s), authStreak = %d, want %d", i, got, i)
		}
		h, _ := st.GetAuthHealth(1)
		if h.NeedsLogin {
			t.Fatalf("transient 401 #%d falsely flagged needs-login", i)
		}
		if !score.Score(score.Input{AccountID: 1, NeedsLogin: h.NeedsLogin}, time.Now()).Available {
			t.Fatalf("transient 401 #%d made the account unselectable", i)
		}
	}

	// The needsLoginAfter-th failure armed the poll backoff: the next poll within
	// needsLoginPollInterval skips sampling — no new Usage call — so the 401 spam
	// stops while AuthHealth.NeedsLogin remains false.
	before := fo.usageCallCount()
	s.pollOnce(t.Context())
	if got := fo.usageCallCount(); got != before {
		t.Fatalf("backed-off account was still sampled: Usage called %d extra time(s)", got-before)
	}
	if h, _ := st.GetAuthHealth(1); h.NeedsLogin {
		t.Fatal("poll backoff must not flag needs-login")
	}
}

// TestPollOnceFlagsConfirmedRevocation pins that a CONFIRMED revocation — the
// refresh POST returns 400 invalid_grant (Revoked) with the on-disk credential
// unchanged — flags needs-login immediately on the first poll, before (and
// independent of) the transient-401 streak. This is the genuine "run ccp login"
// case Fix 2 keeps distinct from a recoverable blip.
func TestPollOnceFlagsConfirmedRevocation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-stale" // ≠ server's currentRT → invalid_grant
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	// Usage 401s; the stale refresh token yields 400 invalid_grant (Revoked) and
	// the credential never rotates (unchanged on re-read) → genuine revocation.
	fo := &fakeOAuth{currentRT: "rt-0", usage401: true}

	s := &Server{
		m:               &pool.Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()},
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		reservations:    map[int]time.Time{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}

	// One poll is enough: a definitive ErrNeedsLogin flags immediately, never
	// touching the 401 streak.
	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("a confirmed 400 invalid_grant revocation must flag needs-login")
	}
	if got := s.authStreak[1]; got != 0 {
		t.Fatalf("a confirmed revocation flags via ErrNeedsLogin and must not touch the 401 streak; authStreak = %d, want 0", got)
	}
}
