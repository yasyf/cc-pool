package pool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

// newHealManager builds a Manager over the in-memory fake seam with the given
// OAuth client and an upserted account, matching the other pool tests' shape.
func newHealManager(t *testing.T, fk *credstest.Fake, oa Refresher) (*Manager, store.Account) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc-heal", KeychainAccount: "user"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	return &Manager{Store: st, OAuth: oa, Creds: fk, LockDir: t.TempDir()}, a
}

// refreshOnly builds the account-1 corruption shape: a blob whose access token
// the `claude` binary blanked, leaving only the refresh token to heal from.
func refreshOnly(rt string, exp time.Time) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.RefreshToken = rt
	c.ClaudeAiOauth.ExpiresAt = exp.UnixMilli()
	return c
}

// TestEnsureFreshTokenHealsRefreshOnlyBlob pins the recovery: an idle account
// whose keychain holds a refresh-only blob (empty access token) refreshes on the
// next EnsureFreshToken and lands a complete blob — the future expiry proves the
// empty access token alone forced the refresh.
func TestEnsureFreshTokenHealsRefreshOnlyBlob(t *testing.T) {
	fk := credstest.NewFake()
	fo := &fakeOAuth{currentRT: "rt-0"}
	m, a := newHealManager(t, fk, fo)
	fk.Put(a.KeychainService, a.KeychainAccount, refreshOnly("rt-0", time.Now().Add(time.Hour)))

	cred, refreshed, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if err != nil {
		t.Fatalf("EnsureFreshToken: %v", err)
	}
	if !refreshed {
		t.Fatal("refreshed = false, want true (an empty access token forces the refresh despite a future expiry)")
	}
	if cred.ClaudeAiOauth.AccessToken != "at-1" {
		t.Errorf("returned access token = %q, want the freshly minted \"at-1\"", cred.ClaudeAiOauth.AccessToken)
	}
	fo.mu.Lock()
	refreshes := fo.refreshes
	fo.mu.Unlock()
	if refreshes != 1 {
		t.Errorf("refreshes = %d, want exactly 1", refreshes)
	}
	healed, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("keychain item missing after the heal")
	}
	if healed.ClaudeAiOauth.AccessToken != "at-1" {
		t.Errorf("keychain access token = %q, want the healed \"at-1\"", healed.ClaudeAiOauth.AccessToken)
	}
	if !healed.Expiry().After(time.Now()) {
		t.Error("healed credential has no future expiry")
	}
}

// TestEnsureFreshTokenRefreshOnlyRevoked pins that a refresh-only blob whose
// refresh token is revoked surfaces ErrNeedsLogin (there is nothing left to heal
// from), rather than a bare transient error.
func TestEnsureFreshTokenRefreshOnlyRevoked(t *testing.T) {
	fk := credstest.NewFake()
	m, a := newHealManager(t, fk, fakeOAuthRevoked{})
	fk.Put(a.KeychainService, a.KeychainAccount, refreshOnly("rt-dead", time.Now().Add(time.Hour)))

	_, _, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
	if !errors.Is(err, ErrNeedsLogin) {
		t.Fatalf("err = %v, want ErrNeedsLogin", err)
	}
}

// TestEnsureFreshTokenClassification pins the narrowness of the needs-login
// mapping: only a tokenless blob (ErrNoTokens) flags the account — a locked or
// opaque keychain and a fully-absent credential must never be classified as
// needs-login, so a reboot-time Keychain wedge never signs healthy accounts out.
func TestEnsureFreshTokenClassification(t *testing.T) {
	errBoom := errors.New("keychain read exploded")
	cases := []struct {
		name         string
		kcReadFault  error
		fileDeadBlob bool
		wantErrIs    []error
		wantNotErrIs []error
	}{
		{
			name:         "tokenless blob flags needs-login and names no-tokens",
			fileDeadBlob: true,
			wantErrIs:    []error{ErrNeedsLogin, creds.ErrNoTokens},
		},
		{
			name:         "unsearchable keychain is not needs-login",
			kcReadFault:  creds.ErrUnavailable,
			wantErrIs:    []error{creds.ErrUnavailable},
			wantNotErrIs: []error{ErrNeedsLogin},
		},
		{
			name:         "opaque keychain read error is not needs-login",
			kcReadFault:  errBoom,
			wantErrIs:    []error{errBoom},
			wantNotErrIs: []error{ErrNeedsLogin},
		},
		{
			name:         "absent everywhere is not needs-login",
			wantErrIs:    []error{creds.ErrNotFound},
			wantNotErrIs: []error{ErrNeedsLogin},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.kcReadFault}
			m, a := newHealManager(t, fk, &fakeOAuth{})
			if tc.fileDeadBlob {
				if err := os.WriteFile(creds.FileCredentialPath(a.ConfigDir), []byte(`{"claudeAiOauth":{}}`), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			_, _, err := m.EnsureFreshToken(context.Background(), a, RefreshLeadTime, true)
			if err == nil {
				t.Fatal("EnsureFreshToken err = nil, want an error")
			}
			for _, want := range tc.wantErrIs {
				if !errors.Is(err, want) {
					t.Errorf("err %v does not satisfy errors.Is(%v)", err, want)
				}
			}
			for _, notWant := range tc.wantNotErrIs {
				if errors.Is(err, notWant) {
					t.Errorf("err %v must NOT satisfy errors.Is(%v)", err, notWant)
				}
			}
		})
	}
}

// TestSampleUsageBusyHealsEmptyAccessToken pins the busy path's heal: a live
// account whose blob lost its access token (allowRefresh=false) recovers through
// the 401 ladder's busy-refresh guard — the empty access token makes Expired()
// true despite a future expiry, satisfying the guard and healing the blob.
func TestSampleUsageBusyHealsEmptyAccessToken(t *testing.T) {
	fk := credstest.NewFake()
	fo := newFakeOAuth401("rt-0")
	m, a := newManager401(t, fk, fo)
	fk.Put(a.KeychainService, a.KeychainAccount, refreshOnly("rt-0", time.Now().Add(time.Hour)))

	_, rateLimited, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowBusyRefresh: true})
	if err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	if rateLimited {
		t.Fatal("rateLimited = true, want false")
	}
	if fo.refreshes != 1 {
		t.Fatalf("refreshes = %d, want exactly 1 (the busy guard heals the empty access token)", fo.refreshes)
	}
	healed, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("keychain item missing after the busy heal")
	}
	if healed.ClaudeAiOauth.AccessToken == "" {
		t.Error("keychain still holds an empty access token after the busy heal")
	}
	assertNeverCanonical(t, fk.TouchedServices())
}
