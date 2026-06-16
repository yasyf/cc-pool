package pool

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/oauth"
	"github.com/yasyf/cc-pool/internal/store"
)

// fakeKeychain is an in-memory CredentialStore. It is internally locked so any
// race the detector reports is in the code under test, not the fake. Every
// operation's service is recorded in touched, so tests can pin which Keychain
// items the pool is willing to name.
type fakeKeychain struct {
	mu      sync.Mutex
	items   map[string]*keychain.Credential
	touched []string // service of every Read/Write/Delete, in order
	deleted []string // service of every Delete, in order
}

func newFakeKeychain() *fakeKeychain {
	return &fakeKeychain{items: map[string]*keychain.Credential{}}
}

func (f *fakeKeychain) key(service, account string) string { return service + "\x00" + account }

func (f *fakeKeychain) Read(service, account string) (*keychain.Credential, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touched = append(f.touched, service)
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
	f.touched = append(f.touched, service)
	cp := *cred
	f.items[f.key(service, account)] = &cp
	return nil
}

func (f *fakeKeychain) Delete(service, account string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touched = append(f.touched, service)
	f.deleted = append(f.deleted, service)
	delete(f.items, f.key(service, account))
	return nil
}

// Discover mirrors keychain.DiscoverAccount: the stored account label for a
// service, found by service alone.
func (f *fakeKeychain) Discover(service string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touched = append(f.touched, service)
	prefix := service + "\x00"
	for k := range f.items {
		if strings.HasPrefix(k, prefix) {
			return strings.TrimPrefix(k, prefix), nil
		}
	}
	return "", keychain.ErrNotFound
}

func (f *fakeKeychain) touchedServices() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.touched...)
}

func (f *fakeKeychain) deletedServices() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

// fakeOAuth simulates the provider's single-use refresh-token rotation: only
// the current token refreshes successfully; re-POSTing a consumed one is an
// invalid_grant, exactly like the real endpoint.
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

func (f *fakeOAuth) Usage(context.Context, string) (*oauth.Usage, error) {
	// Non-zero values so tests can pin the oauth→store join in recordSample.
	return &oauth.Usage{
		FiveHour:   oauth.Window{Utilization: 31},
		SevenDay:   oauth.Window{Utilization: 7},
		ExtraUsage: oauth.ExtraUsage{IsEnabled: true, MonthlyLimit: 5000, UsedCredits: 177, Utilization: 3.54, Currency: "USD"},
	}, nil
}

// TestPerAccountLockSerializesCredentialCycle hammers one account's credential
// from concurrent SampleUsage (read→refresh→write) and AdoptRotatedToken
// (read→write) cycles. The per-account lock must prevent both concrete
// failure modes of the unsynchronized code:
//
//   - double-spend: two concurrent refreshes POST the same single-use token;
//     the loser gets invalid_grant → Revoked() → ErrNeedsLogin → account
//     flagged dead (invalidGrants > 0);
//   - lost update: an adopt reads cred X, a refresh writes Y (new RT), the
//     adopt writes back X — clobbering Y with a consumed token (final
//     keychain RT != provider's current RT).
//
// Run with -race; the iteration count is the amplifier, no sleeps.
func TestPerAccountLockSerializesCredentialCycle(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	a := store.Account{ID: 1, KeychainService: "svc", KeychainAccount: "user"}
	fk := newFakeKeychain()
	seed := &keychain.Credential{}
	seed.ClaudeAiOauth.AccessToken = "at-0"
	seed.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry (< RefreshLeadTime) so the first SampleUsage must refresh.
	seed.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	if err := fk.Write(a.KeychainService, a.KeychainAccount, seed); err != nil {
		t.Fatal(err)
	}
	fo := &fakeOAuth{currentRT: "rt-0"}
	m := &Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()}

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
					if _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true}); err != nil {
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
	final, err := fk.Read(a.KeychainService, a.KeychainAccount)
	if err != nil {
		t.Fatal(err)
	}
	if got := final.ClaudeAiOauth.RefreshToken; got != currentRT {
		t.Errorf("stale clobber: keychain holds refresh token %q, provider's current is %q", got, currentRT)
	}
}
