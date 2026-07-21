package pool

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

func casCred(access, refresh string) *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = access
	c.ClaudeAiOauth.RefreshToken = refresh
	return c
}

// TestAdoptRotatedTokenReassertsUnchanged pins the CAS happy path: with no
// concurrent writer the re-read matches and the credential is rewritten in place.
func TestAdoptRotatedTokenReassertsUnchanged(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
	a = persistTestAccount(t, st, a)
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, casCred("at-0", "rt-0"))
	m := &Manager{Store: st, Creds: fk}
	bindTestWorkerAuthority(t, m, "adopt-rotated")
	before := fk.WriteCount()

	if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
		t.Fatalf("AdoptRotatedToken: %v", err)
	}
	if got := fk.WriteCount(); got != before+1 {
		t.Fatalf("adopt performed %d write(s), want 1 (ACL re-assert)", got-before)
	}
	if got, _ := fk.Get(a.KeychainService, a.KeychainAccount); got.ClaudeAiOauth.AccessToken != "at-0" {
		t.Fatalf("stored access token = %q, want at-0", got.ClaudeAiOauth.AccessToken)
	}
}

// TestAdoptRotatedTokenAbortsOnLogoutUnderfoot pins the empty-re-read guard: a
// `claude` logout deleting the blob between the adopt's read and its CAS
// re-read must abort — writing the old owned blob back would undo the logout
// and resurrect a possibly-dead chain.
func TestAdoptRotatedTokenAbortsOnLogoutUnderfoot(t *testing.T) {
	f := newInstallFixture(t)
	owned := casCred("at-0", "rt-0")
	ks := &swapStore{Store: f.fk.Store(f.a, creds.SourceKeychain), old: owned, swappedErr: creds.ErrNotFound}
	f.m.Creds = swapCreds{Fake: f.fk, ks: ks}

	err := f.m.AdoptRotatedToken(context.Background(), f.a)
	if !errors.Is(err, ErrCredentialChangedUnderfoot) {
		t.Fatalf("err = %v, want ErrCredentialChangedUnderfoot", err)
	}
	if ks.reads < 2 {
		t.Fatalf("CAS re-read never happened (reads = %d)", ks.reads)
	}
	if f.fk.WriteCount() != 0 || f.hookCalls != 0 {
		t.Fatalf("aborted adopt acted (writes=%d hooks=%d), want none", f.fk.WriteCount(), f.hookCalls)
	}
	if got, ok := f.fk.Get(f.a.KeychainService, f.a.KeychainAccount); ok {
		t.Fatalf("backend holds %+v, want the logout's deletion left in place", got)
	}
}
