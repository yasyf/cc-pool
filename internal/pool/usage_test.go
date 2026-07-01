package pool

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/store"
)

// TestPoolNeverTouchesDefaultKeychainItem pins the #1 safety invariant: no
// CredentialStore op ever names the canonical unsuffixed item plain `claude`
// owns ("Claude Code-credentials"). It drives the real ops — pre-flight refresh,
// rotated-token re-assert, remove — asserting each names only the account's own
// suffixed service.
func TestPoolNeverTouchesDefaultKeychainItem(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "user")
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := keychain.ServiceName("/tmp/ccp-test/acct-01")
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: svc, KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	// Near-expiry so SampleUsage's pre-flight must POST-refresh.
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Minute).UnixMilli()
	if err := fk.Write(svc, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	fo := &fakeOAuth{currentRT: "rt-0"}
	m := &Manager{Store: st, OAuth: fo, Keychain: fk, LockDir: t.TempDir()}

	if _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: true}); err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	if got := fo.refreshes; got != 1 {
		t.Fatalf("refreshes = %d, want 1 (near-expiry token must be refreshed)", got)
	}
	if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
		t.Fatalf("AdoptRotatedToken: %v", err)
	}
	if err := m.Remove(a.ID, true); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	touched := fk.touchedServices()
	if len(touched) == 0 {
		t.Fatal("no keychain ops recorded; the test exercised nothing")
	}
	for i, s := range touched {
		if s == "Claude Code-credentials" {
			t.Fatalf("op %d named the canonical unsuffixed item", i)
		}
		if s != svc {
			t.Errorf("op %d named service %q, want %q", i, s, svc)
		}
	}
	if del := fk.deletedServices(); len(del) != 1 || del[0] != svc {
		t.Errorf("deletes = %v, want exactly [%q]", del, svc)
	}
}

// TestSampleUsagePersistsExtraUsage pins the oauth→store join in recordSample:
// the usage windows and extra-usage block must land in the latest stored sample
// verbatim (status and the exhausted-fallback billing warning read them there) —
// a mapping each layer's isolated tests would miss if dropped.
func TestSampleUsagePersistsExtraUsage(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user", OverlayKind: "symlink"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	fk := newFakeKeychain()
	cred := &keychain.Credential{}
	cred.ClaudeAiOauth.AccessToken = "at-0"
	cred.ClaudeAiOauth.RefreshToken = "rt-0"
	cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli() // fresh: no refresh needed
	if err := fk.Write(a.KeychainService, a.KeychainAccount, cred); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: st, OAuth: &fakeOAuth{currentRT: "rt-0"}, Keychain: fk, LockDir: t.TempDir()}

	if _, _, err := m.SampleUsage(context.Background(), a, SampleOpts{AllowRefresh: false}); err != nil {
		t.Fatalf("SampleUsage: %v", err)
	}
	got, ok, err := st.LatestUsageSample(1)
	if err != nil || !ok {
		t.Fatalf("latest sample: ok=%v err=%v", ok, err)
	}
	if got.Util5h != 31 || got.Util7d != 7 {
		t.Fatalf("windows not persisted: %+v", got)
	}
	if !got.ExtraEnabled || got.ExtraUsed != 177 || got.ExtraLimit != 5000 {
		t.Fatalf("extra usage not persisted: %+v", got)
	}
}
