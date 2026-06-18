package pool

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// fakeOAuth401 accepts only access tokens in validAT (everything else 401s) and
// rotates the refresh token single-use like the real endpoint, marking each
// freshly minted access token valid.
type fakeOAuth401 struct {
	mu        sync.Mutex
	currentRT string
	validAT   map[string]bool
	refreshes int // successful refresh POSTs
}

func newFakeOAuth401(currentRT string, valid ...string) *fakeOAuth401 {
	m := map[string]bool{}
	for _, a := range valid {
		m[a] = true
	}
	return &fakeOAuth401{currentRT: currentRT, validAT: m}
}

func (f *fakeOAuth401) Usage(_ context.Context, at string) (*oauth.Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.validAT[at] {
		return &oauth.Usage{FiveHour: oauth.Window{Utilization: 31}, SevenDay: oauth.Window{Utilization: 7}}, nil
	}
	return nil, &oauth.UsageError{Status: 401, Body: `{"type":"error","error":{"type":"authentication_error"}}`}
}

func (f *fakeOAuth401) Refresh(_ context.Context, _, rt string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rt != f.currentRT {
		return nil, &oauth.RefreshError{Status: 400, Body: "invalid_grant"}
	}
	f.refreshes++
	at := fmt.Sprintf("at-r%d", f.refreshes)
	f.currentRT = fmt.Sprintf("rt-r%d", f.refreshes)
	f.validAT[at] = true
	return &oauth.TokenResponse{AccessToken: at, RefreshToken: f.currentRT, ExpiresIn: 3600}, nil
}

// rotatingKeychain returns `current` until rotateAfter reads have elapsed, then
// `rotated` — letting a test inject a live session rotating the chain between
// the daemon's successive reads. A Write makes the written credential
// authoritative (and cancels any pending rotation).
type rotatingKeychain struct {
	mu          sync.Mutex
	reads       int
	rotateAfter int // 0 = never rotate
	current     *keychain.Credential
	rotated     *keychain.Credential
	touched     []string
}

func (k *rotatingKeychain) Read(service, _ string) (*keychain.Credential, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.touched = append(k.touched, service)
	k.reads++
	c := k.current
	if k.rotated != nil && k.rotateAfter > 0 && k.reads > k.rotateAfter {
		c = k.rotated
	}
	if c == nil {
		return nil, keychain.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (k *rotatingKeychain) Write(service, _ string, cred *keychain.Credential) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.touched = append(k.touched, service)
	cp := *cred
	k.current = &cp
	k.rotated = nil
	return nil
}

func (k *rotatingKeychain) Delete(service, _ string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.touched = append(k.touched, service)
	return nil
}

func (k *rotatingKeychain) Discover(string) (string, error) { return "user", nil }

func (k *rotatingKeychain) touchedServices() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.touched...)
}

func cred401(at, rt string, expiresAt time.Time) *keychain.Credential {
	c := &keychain.Credential{}
	c.ClaudeAiOauth.AccessToken = at
	c.ClaudeAiOauth.RefreshToken = rt
	c.ClaudeAiOauth.ExpiresAt = expiresAt.UnixMilli()
	return c
}

func newManager401(t *testing.T, kc CredentialStore, fo *fakeOAuth401) (*Manager, store.Account) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "acct-1-suffixed", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	return &Manager{Store: st, OAuth: fo, Keychain: kc, LockDir: t.TempDir()}, a
}

// assertNeverCanonical pins the #1 safety rule across the new 401 arms: no op
// ever names plain claude's canonical unsuffixed Keychain item.
func assertNeverCanonical(t *testing.T, touched []string) {
	t.Helper()
	for i, s := range touched {
		if s == "Claude Code-credentials" {
			t.Fatalf("op %d named the canonical unsuffixed item", i)
		}
	}
}

// fakeOAuthRevoked reproduces the daemon mask: the pre-flight refresh confirms a
// revocation (400 invalid_grant) while the usage endpoint returns a 429, so a
// naive sampleUsage would surface rate-limited and swallow the needs-login.
type fakeOAuthRevoked struct{}

func (fakeOAuthRevoked) Refresh(_ context.Context, _, _ string) (*oauth.TokenResponse, error) {
	return nil, &oauth.RefreshError{Status: 400, Body: "invalid_grant"}
}

func (fakeOAuthRevoked) Usage(_ context.Context, _ string) (*oauth.Usage, error) {
	return nil, &oauth.UsageError{Status: 429}
}

// TestSampleUsageRevokedNotMaskedByRateLimit: a confirmed pre-flight revocation
// must not be masked by a usage-endpoint 429. SampleUsage surfaces ErrNeedsLogin
// (not rateLimited=true), and the error path records no usage sample.
func TestSampleUsageRevokedNotMaskedByRateLimit(t *testing.T) {
	kc := &rotatingKeychain{
		current: cred401("at-0", "rt-stale", time.Now().Add(-time.Hour)),
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "acct-1-suffixed", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: st, OAuth: fakeOAuthRevoked{}, Keychain: kc, LockDir: t.TempDir()}

	_, rateLimited, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin (revocation masked by 429)", err)
	}
	if rateLimited {
		t.Fatalf("rateLimited = true, want false (a revoked account is not rate-limited)")
	}
	if _, ok, serr := st.LatestUsageSample(a.ID); serr != nil {
		t.Fatalf("LatestUsageSample: %v", serr)
	} else if ok {
		t.Fatalf("a usage sample was recorded, want none (error path must skip recordSample)")
	}
	assertNeverCanonical(t, kc.touchedServices())
}

// TestFetchUsage401RereadRetriesRotatedToken: the pre-flight read gets the stale
// token, a session rotates the chain, and rung 1 (a pure re-read) retries with
// the rotated token — recovering WITHOUT spending a refresh token.
func TestFetchUsage401RereadRetriesRotatedToken(t *testing.T) {
	kc := &rotatingKeychain{
		rotateAfter: 1, // read#1 (pre-flight) = current; read#2 (rung 1) = rotated
		current:     cred401("at-0", "rt-0", time.Now().Add(-time.Hour)),
		rotated:     cred401("at-9", "rt-9", time.Now().Add(time.Hour)),
	}
	fo := newFakeOAuth401("rt-9", "at-9") // at-9 valid; at-0 401s
	m, a := newManager401(t, kc, fo)

	if _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: false}); err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0 (rung-1 re-read must not spend a refresh token)", fo.refreshes)
	}
	assertNeverCanonical(t, kc.touchedServices())
}

// TestSampleUsageClassifiesNeedsLogin: with no refresh token a 401 is definitive
// — the error wraps ErrNeedsLogin and no refresh is attempted.
func TestSampleUsageClassifiesNeedsLogin(t *testing.T) {
	kc := &rotatingKeychain{current: cred401("at-0", "", time.Now().Add(-time.Hour))}
	fo := newFakeOAuth401("") // nothing valid → at-0 401s
	m, a := newManager401(t, kc, fo)

	_, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0 (no refresh token to spend)", fo.refreshes)
	}
}

// TestSampleUsageBusyRefreshGuard pins the guarded busy-refresh ladder, with the
// load-bearing negatives: no refresh without the flag, none when the token is
// not expired, none when a session rotated the chain mid-fetch, and a revoked
// refresh with an unchanged credential is the only path to ErrNeedsLogin.
func TestSampleUsageBusyRefreshGuard(t *testing.T) {
	past := func() time.Time { return time.Now().Add(-time.Hour) }
	future := func() time.Time { return time.Now().Add(time.Hour) }

	cases := []struct {
		name           string
		opts           SampleOpts
		expiresAt      time.Time
		rt             string // refresh token in the stored credential
		fakeRT         string // the provider's current (valid) refresh token
		rotateAfter    int    // >0 rotates the chain to `rotated` mid-fetch
		wantRefreshes  int
		wantErr        bool
		wantNeedsLogin bool
	}{
		{
			name: "neither flag set: no refresh, 401 propagates",
			opts: SampleOpts{}, expiresAt: past(), rt: "rt-0", fakeRT: "rt-0",
			wantRefreshes: 0, wantErr: true,
		},
		{
			name: "busy but token not expired: guard blocks refresh",
			opts: SampleOpts{AllowBusyRefresh: true}, expiresAt: future(), rt: "rt-0", fakeRT: "rt-0",
			wantRefreshes: 0, wantErr: true,
		},
		{
			name: "guard satisfied: exactly one refresh, recovers",
			opts: SampleOpts{AllowBusyRefresh: true}, expiresAt: past(), rt: "rt-0", fakeRT: "rt-0",
			wantRefreshes: 1, wantErr: false,
		},
		{
			name: "session rotated mid-fetch: guard blocks refresh",
			opts: SampleOpts{AllowBusyRefresh: true}, expiresAt: past(), rt: "rt-0", fakeRT: "rt-0",
			rotateAfter: 2, wantRefreshes: 0, wantErr: true,
		},
		{
			name: "revoked + unchanged: needs-login",
			opts: SampleOpts{AllowBusyRefresh: true}, expiresAt: past(), rt: "rt-stale", fakeRT: "rt-current",
			wantRefreshes: 0, wantErr: true, wantNeedsLogin: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kc := &rotatingKeychain{
				current:     cred401("at-0", tc.rt, tc.expiresAt),
				rotateAfter: tc.rotateAfter,
				rotated:     cred401("at-9", "rt-9", time.Now().Add(time.Hour)),
			}
			fo := newFakeOAuth401(tc.fakeRT) // at-0 (and at-9) 401; only refreshed tokens become valid
			m, a := newManager401(t, kc, fo)

			_, _, err := m.SampleUsage(context.Background(), a, tc.opts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if fo.refreshes != tc.wantRefreshes {
				t.Fatalf("refreshes = %d, want %d", fo.refreshes, tc.wantRefreshes)
			}
			if got := errors.Is(err, ErrNeedsLogin); got != tc.wantNeedsLogin {
				t.Fatalf("errors.Is(err, ErrNeedsLogin) = %v, want %v (err=%v)", got, tc.wantNeedsLogin, err)
			}
			assertNeverCanonical(t, kc.touchedServices())
		})
	}
}
