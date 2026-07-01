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

// fakeKeychain and fakeOAuth are internally locked so -race points at code
// under test.

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
	usageCalls int
	usage401   bool
	refresh5xx bool
}

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

	s.tryReserve(a.ID)
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("reserved account was POST-refreshed %d time(s)", got)
	}
	if got := fk.writeCount(); got != seedWrites {
		t.Fatalf("reserved account's credential was written %d time(s)", got-seedWrites)
	}

	s.mu.Lock()
	s.reservations[a.ID] = time.Now().Add(-reservationTTL - time.Second)
	s.mu.Unlock()
	s.pollOnce(t.Context())
	if got := fo.refreshCount(); got != 1 {
		t.Fatalf("idle near-expiry account refreshed %d time(s), want 1", got)
	}
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

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("definitive 401 should flag needs-login")
	}
	if got := fo.refreshCount(); got != 0 {
		t.Fatalf("no refresh token to spend, but refreshed %d time(s)", got)
	}

	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	fo.setUsage401(false)
	s.lastAuthAttempt[1] = time.Now().Add(-needsLoginPollInterval - time.Second)

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
	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0" // present → a 401 is recoverable, not definitive
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(-time.Hour).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
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
	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-stale" // ≠ server's currentRT → invalid_grant
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

	s.pollOnce(t.Context())
	if h, _ := st.GetAuthHealth(1); !h.NeedsLogin {
		t.Fatal("a confirmed 400 invalid_grant revocation must flag needs-login")
	}
	if got := s.authStreak[1]; got != 0 {
		t.Fatalf("a confirmed revocation flags via ErrNeedsLogin and must not touch the 401 streak; authStreak = %d, want 0", got)
	}
}
