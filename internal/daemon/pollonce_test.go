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
	mu        sync.Mutex
	currentRT string
	refreshes int
	usage401  bool // when set, Usage returns a 401 UsageError
}

func (f *fakeOAuth) Refresh(_ context.Context, _, refreshToken string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	if f.usage401 {
		return nil, &oauth.UsageError{Status: 401, Body: `{"type":"error"}`}
	}
	return &oauth.Usage{}, nil
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
