package pool

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// fakeOAuth401 accepts only access tokens in validAT (else 401) and rotates the
// refresh token single-use like the real endpoint, marking each minted access
// token valid.
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
	return nil, &oauth.UsageError{Status: 401}
}

func (f *fakeOAuth401) Refresh(_ context.Context, _, rt string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rt != f.currentRT {
		return nil, &oauth.RefreshError{Status: 400, ConfirmedInvalidGrant: true}
	}
	f.refreshes++
	at := fmt.Sprintf("at-r%d", f.refreshes)
	f.currentRT = fmt.Sprintf("rt-r%d", f.refreshes)
	f.validAT[at] = true
	return &oauth.TokenResponse{AccessToken: at, RefreshToken: f.currentRT, ExpiresIn: 3600}, nil
}

// rotatingCreds is a Credentials seam whose Keychain store returns `current`
// until rotateAfter reads elapse, then `rotated` — injecting a live session
// that rotates the chain between successive reads. A Write makes the written
// credential authoritative and cancels the pending rotation.
type rotatingCreds struct {
	mu          sync.Mutex
	reads       int
	rotateAfter int // 0 = never rotate
	current     *creds.Credential
	rotated     *creds.Credential
	touched     []string
}

func (k *rotatingCreds) Store(a store.Account, src creds.Source) creds.Store {
	return rotatingItem{k: k, service: a.KeychainService}
}

func (k *rotatingCreds) Stores(a store.Account) []creds.Store {
	return []creds.Store{k.Store(a, creds.SourceKeychain)}
}

func (k *rotatingCreds) Discover(context.Context, string) (string, error) { return "user", nil }

func (k *rotatingCreds) touchedServices() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.touched...)
}

// rotatingItem is rotatingCreds' Keychain store bound to one service.
type rotatingItem struct {
	k       *rotatingCreds
	service string
}

func (i rotatingItem) Source() creds.Source { return creds.SourceKeychain }

func (i rotatingItem) Read(context.Context) (*creds.Credential, error) {
	i.k.mu.Lock()
	defer i.k.mu.Unlock()
	i.k.touched = append(i.k.touched, i.service)
	i.k.reads++
	c := i.k.current
	if i.k.rotated != nil && i.k.rotateAfter > 0 && i.k.reads > i.k.rotateAfter {
		c = i.k.rotated
	}
	if c == nil {
		return nil, creds.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (i rotatingItem) Write(_ context.Context, cred *creds.Credential) error {
	i.k.mu.Lock()
	defer i.k.mu.Unlock()
	i.k.touched = append(i.k.touched, i.service)
	cp := *cred
	i.k.current = &cp
	i.k.rotated = nil
	return nil
}

func (i rotatingItem) Delete(context.Context) error {
	i.k.mu.Lock()
	defer i.k.mu.Unlock()
	i.k.touched = append(i.k.touched, i.service)
	return nil
}

func (i rotatingItem) String() string { return fmt.Sprintf("rotating keychain item %q", i.service) }

func cred401(at, rt string, expiresAt time.Time) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = at
	c.ClaudeAiOauth.RefreshToken = rt
	c.ClaudeAiOauth.ExpiresAt = expiresAt.UnixMilli()
	return c
}

func newManager401(t *testing.T, kc Credentials, fo *fakeOAuth401) (*Manager, store.Account) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "acct-1-suffixed", KeychainAccount: "user"}
	a = persistTestAccount(t, st, a)
	manager := &Manager{Store: st, OAuth: fo, Creds: kc}
	bindTestWorkerAuthority(t, manager, "usage-401")
	return manager, a
}

// assertNeverCanonical pins the safety rule that no op ever names plain claude's
// canonical unsuffixed Keychain item.
func assertNeverCanonical(t *testing.T, touched []string) {
	t.Helper()
	for i, s := range touched {
		if s == "Claude Code-credentials" {
			t.Fatalf("op %d named the canonical unsuffixed item", i)
		}
	}
}

// fakeOAuthRevoked reproduces the daemon mask: pre-flight refresh confirms a
// revocation (400 invalid_grant) while usage returns 429, so a naive sampleUsage
// would surface rate-limited and swallow the needs-login.
type fakeOAuthRevoked struct{}

func (fakeOAuthRevoked) Refresh(_ context.Context, _, _ string) (*oauth.TokenResponse, error) {
	return nil, &oauth.RefreshError{Status: 400, ConfirmedInvalidGrant: true}
}

func (fakeOAuthRevoked) Usage(_ context.Context, _ string) (*oauth.Usage, error) {
	return nil, &oauth.UsageError{Status: 429}
}

// TestSampleUsageRevokedNotMaskedByRateLimit: a confirmed pre-flight revocation
// must surface ErrNeedsLogin, not a usage-endpoint 429's rateLimited, and record
// no usage sample.
func TestSampleUsageRevokedNotMaskedByRateLimit(t *testing.T) {
	kc := &rotatingCreds{
		current: cred401("at-0", "rt-stale", time.Now().Add(-time.Hour)),
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "acct-1-suffixed", KeychainAccount: "user"}
	a = persistTestAccount(t, st, a)
	m := &Manager{Store: st, OAuth: fakeOAuthRevoked{}, Creds: kc}
	bindTestWorkerAuthority(t, m, "usage-revoked")

	_, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
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

// fakeOAuthNet fails every request with a network-class error, standing in for a
// connectivity outage.
type fakeOAuthNet struct{}

func (fakeOAuthNet) Refresh(context.Context, string, string) (*oauth.TokenResponse, error) {
	return nil, fmt.Errorf("dial tcp: %w", oauth.ErrNetwork)
}

func (fakeOAuthNet) Usage(context.Context, string) (*oauth.Usage, error) {
	return nil, fmt.Errorf("dial tcp: %w", oauth.ErrNetwork)
}

// TestSampleUsageNetworkErrorPropagates: a transport failure surfaces as
// oauth.ErrNetwork (never rate-limited or needs-login) and records no sample, so
// the daemon's outage detector — not the auth or rate-limit ladders — owns it.
func TestSampleUsageNetworkErrorPropagates(t *testing.T) {
	// Unexpired so ensureFreshToken performs no preflight refresh: the network
	// error must come from the usage fetch itself.
	kc := &rotatingCreds{current: cred401("at-0", "rt-0", time.Now().Add(time.Hour))}
	m, a := newManager401(t, kc, newFakeOAuth401("rt-0"))
	m.OAuth = fakeOAuthNet{}

	_, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, oauth.ErrNetwork) {
		t.Fatalf("err = %v, want oauth.ErrNetwork", err)
	}
	if errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("a network outage must not classify as needs-login: %v", err)
	}
	if rateLimited {
		t.Fatal("a network outage must not classify as rate-limited")
	}
	if _, ok, serr := m.Store.LatestUsageSample(a.ID); serr != nil {
		t.Fatalf("LatestUsageSample: %v", serr)
	} else if ok {
		t.Fatal("a network-failed sample must record no usage sample")
	}
	assertNeverCanonical(t, kc.touchedServices())
}

// TestFetchUsage401RereadRetriesRotatedToken: after a session rotates the chain,
// rung 1 (a pure re-read) retries with the rotated token, recovering without
// spending a refresh token.
func TestFetchUsage401RereadRetriesRotatedToken(t *testing.T) {
	kc := &rotatingCreds{
		rotateAfter: 1, // read#1 (pre-flight) = current; read#2 (rung 1) = rotated
		current:     cred401("at-0", "rt-0", time.Now().Add(-time.Hour)),
		rotated:     cred401("at-9", "rt-9", time.Now().Add(time.Hour)),
	}
	fo := newFakeOAuth401("rt-9", "at-9") // at-9 valid; at-0 401s
	m, a := newManager401(t, kc, fo)

	if _, _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: false}); err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0 (rung-1 re-read must not spend a refresh token)", fo.refreshes)
	}
	assertNeverCanonical(t, kc.touchedServices())
}

// TestSampleUsageClassifiesUnrefreshable: an expired synced credential (no
// refresh token) is ErrUnrefreshable — this host cannot refresh it and must
// not classify it as needs-login (the origin's next rotation heals it) — and
// no refresh is attempted.
func TestSampleUsageClassifiesUnrefreshable(t *testing.T) {
	kc := &rotatingCreds{current: cred401("at-0", "", time.Now().Add(-time.Hour))}
	fo := newFakeOAuth401("") // nothing valid → at-0 401s
	m, a := newManager401(t, kc, fo)

	_, _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, ErrUnrefreshable) {
		t.Fatalf("err = %v, want ErrUnrefreshable", err)
	}
	if errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v; a synced credential must not classify as needs-login", err)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0 (no refresh token to spend)", fo.refreshes)
	}
}

// TestSampleUsagePropagatesUnrefreshableThroughGraceFetch pins the Phase-3
// propagation fix (Stage-1 finding #5): an expired synced token whose access
// token still grace-serves a 200 must STILL surface ErrUnrefreshable (the
// origin's rotation is the only real recovery) instead of the grace 200
// swallowing it. cred is the non-nil expired blob, so this exercises the guard
// gated on cred==nil, never a nil-deref (the v0.50.2 tombstone incident).
func TestSampleUsagePropagatesUnrefreshableThroughGraceFetch(t *testing.T) {
	kc := &rotatingCreds{current: cred401("at-0", "", time.Now().Add(-time.Hour))}
	fo := newFakeOAuth401("", "at-0") // at-0 grace-serves a 200
	m, a := newManager401(t, kc, fo)

	usage, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true})
	if !errors.Is(err, ErrUnrefreshable) {
		t.Fatalf("err = %v, want ErrUnrefreshable propagated despite the grace 200", err)
	}
	if errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v; a synced credential must not classify as needs-login", err)
	}
	if usage != nil || rateLimited {
		t.Fatalf("usage = %+v rateLimited = %v, want a suppressed sample", usage, rateLimited)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0", fo.refreshes)
	}
}

// TestFetchUsage401RereadWinsOverUnrefreshable: a fresher synced token pulled
// underfoot between the pre-flight read and the 401 must be retried and win —
// the ladder's re-read recovery runs before the unrefreshable classification.
func TestFetchUsage401RereadWinsOverUnrefreshable(t *testing.T) {
	kc := &rotatingCreds{
		rotateAfter: 1, // read#1 (pre-flight) = current; read#2 (rung 1) = the fresher pull
		current:     cred401("at-0", "", time.Now().Add(time.Hour)),
		rotated:     cred401("at-9", "", time.Now().Add(2*time.Hour)),
	}
	fo := newFakeOAuth401("", "at-9") // at-9 valid; at-0 401s
	m, a := newManager401(t, kc, fo)

	if _, _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true}); err != nil {
		t.Fatalf("SampleUsage = %v, want the re-read synced token to recover", err)
	}
	if fo.refreshes != 0 {
		t.Fatalf("refreshes = %d, want 0 (synced tokens never refresh)", fo.refreshes)
	}
	assertNeverCanonical(t, kc.touchedServices())
}

// TestSampleUsageBusyRefreshGuard pins the busy-refresh ladder's load-bearing
// negatives: no refresh without the flag, when unexpired, or after a mid-fetch
// rotation; only a revoked refresh with an unchanged credential reaches
// ErrNeedsLogin.
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
			kc := &rotatingCreds{
				current:     cred401("at-0", tc.rt, tc.expiresAt),
				rotateAfter: tc.rotateAfter,
				rotated:     cred401("at-9", "rt-9", time.Now().Add(time.Hour)),
			}
			fo := newFakeOAuth401(tc.fakeRT) // at-0 (and at-9) 401; only refreshed tokens become valid
			m, a := newManager401(t, kc, fo)

			_, _, _, err := m.SampleUsage(context.Background(), a, tc.opts)
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
