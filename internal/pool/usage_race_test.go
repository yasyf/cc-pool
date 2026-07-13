package pool

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// The shared in-memory seam fake must satisfy the Manager's Credentials port.
var _ Credentials = (*credstest.Fake)(nil)

// fakeOAuth simulates the provider's single-use refresh-token rotation: only the
// current token refreshes; re-POSTing a consumed one is invalid_grant, like the
// real endpoint.
type fakeOAuth struct {
	mu            sync.Mutex
	currentRT     string
	refreshes     int // successful refresh POSTs
	invalidGrants int // double-spends of a consumed token
}

func (f *fakeOAuth) Refresh(_ context.Context, _, refreshToken string) (*oauth.TokenResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if refreshToken != f.currentRT {
		f.invalidGrants++
		return nil, &oauth.RefreshError{Status: 400, Body: `{"error":"invalid_grant"}`, Code: "invalid_grant"}
	}
	f.refreshes++
	f.currentRT = fmt.Sprintf("rt-%d", f.refreshes)
	return &oauth.TokenResponse{
		AccessToken:  fmt.Sprintf("at-%d", f.refreshes),
		RefreshToken: f.currentRT,
		ExpiresIn:    3600,
	}, nil
}

func (f *fakeOAuth) Usage(context.Context, string) (*oauth.Usage, error) {
	// Non-zero values so tests can pin the oauth→store join in recordSample.
	return &oauth.Usage{
		FiveHour:   oauth.Window{Utilization: 31},
		SevenDay:   oauth.Window{Utilization: 7},
		ExtraUsage: oauth.ExtraUsage{IsEnabled: true, MonthlyLimit: 5000, UsedCredits: 177, Utilization: 3.54, Currency: "USD"},
	}, nil
}

// TestPerAccountLockSerializesCredentialCycle hammers one account's credential
// with concurrent SampleUsage and AdoptRotatedToken cycles; the per-account lock
// must prevent both failure modes of the unsynchronized code:
//
//   - double-spend: two concurrent refreshes POST the same single-use token; the
//     loser gets invalid_grant → account flagged dead (invalidGrants > 0);
//   - lost update: adopt reads cred X, a refresh writes Y, adopt writes back X,
//     clobbering Y with a consumed token (final keychain RT != provider's).
//
// Run with -race; iteration count is the amplifier, no sleeps.
func TestPerAccountLockSerializesCredentialCycle(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
	fk := credstest.NewFake()
	seed := &creds.Credential{}
	seed.ClaudeAiOauth.AccessToken = "at-0"
	seed.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so the first SampleUsage must refresh.
	seed.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	fk.Put(a.KeychainService, a.KeychainAccount, seed)
	fo := &fakeOAuth{currentRT: "rt-0"}
	m := &Manager{Store: st, OAuth: fo, Creds: fk, LockDir: t.TempDir()}

	const goroutines = 16
	const iterations = 25
	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				if g%2 == 0 {
					if _, _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true}); err != nil {
						t.Errorf("SampleUsage: %v", err)
						return
					}
				} else {
					if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
						t.Errorf("AdoptRotatedToken: %v", err)
						return
					}
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()

	fo.mu.Lock()
	refreshes, invalidGrants, currentRT := fo.refreshes, fo.invalidGrants, fo.currentRT
	fo.mu.Unlock()
	if invalidGrants != 0 {
		t.Errorf("double-spend: %d refresh POST(s) re-used a consumed single-use token", invalidGrants)
	}
	if refreshes != 1 {
		t.Errorf("refreshes = %d, want exactly 1 (the first refresh yields a 1h-fresh token every serialized successor reuses)", refreshes)
	}
	final, ok := fk.Get(a.KeychainService, a.KeychainAccount)
	if !ok {
		t.Fatal("credential missing from the fake keychain after the hammer")
	}
	if got := final.ClaudeAiOauth.RefreshToken; got != currentRT {
		t.Errorf("stale clobber: keychain holds refresh token %q, provider's current is %q", got, currentRT)
	}
}
