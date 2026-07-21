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

// TestWriteCredCAS pins the compare-and-swap guard: a snapshot matching on
// both tokens writes through, any divergence — access token, refresh token
// alone, a credential appearing over a slot decided empty, or one vanishing
// under a non-nil snapshot — aborts with ErrCredentialChangedUnderfoot
// without clobbering, only a nil prev writes through an empty slot, and a
// re-read that fails for any reason other than a proven-empty slot aborts
// with ErrCredentialUnverifiable.
func TestWriteCredCAS(t *testing.T) {
	errOpaque := errors.New("keychain read exploded")
	cases := []struct {
		name        string
		stored      *creds.Credential // seeded backend value; nil = absent
		readFault   error             // injected re-read failure
		prev        *creds.Credential // the snapshot next was derived from
		next        *creds.Credential
		wantErr     error
		wantStored  string // access token expected in the backend afterward
		wantWritten bool
	}{
		{
			name:        "matching snapshot writes through",
			stored:      casCred("at-0", "rt-0"),
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-1", "rt-1"),
			wantErr:     nil,
			wantStored:  "at-1",
			wantWritten: true,
		},
		{
			name:        "changed underfoot aborts and keeps the newer credential",
			stored:      casCred("at-claude", "rt-claude"),
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-1", "rt-1"),
			wantErr:     ErrCredentialChangedUnderfoot,
			wantStored:  "at-claude",
			wantWritten: false,
		},
		{
			name:        "same access token with a rotated refresh token aborts",
			stored:      casCred("at-0", "rt-rotated"),
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-0", ""), // the strip shape
			wantErr:     ErrCredentialChangedUnderfoot,
			wantStored:  "at-0",
			wantWritten: false,
		},
		{
			name:        "credential appearing over a slot decided empty aborts",
			stored:      casCred("at-login", "rt-login"),
			prev:        nil,
			next:        casCred("at-synced", ""),
			wantErr:     ErrCredentialChangedUnderfoot,
			wantStored:  "at-login",
			wantWritten: false,
		},
		{
			name:        "credential vanishing under a non-nil snapshot aborts (logout underfoot)",
			stored:      nil,
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-0", "rt-0"),
			wantErr:     ErrCredentialChangedUnderfoot,
			wantStored:  "",
			wantWritten: false,
		},
		{
			name:        "nil snapshot writes through an absent slot",
			stored:      nil,
			prev:        nil,
			next:        casCred("at-synced", ""),
			wantErr:     nil,
			wantStored:  "at-synced",
			wantWritten: true,
		},
		{
			name:        "unsearchable keychain aborts (unverifiable, not empty)",
			stored:      casCred("at-0", "rt-0"),
			readFault:   creds.ErrUnavailable,
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-1", "rt-1"),
			wantErr:     ErrCredentialUnverifiable,
			wantStored:  "at-0",
			wantWritten: false,
		},
		{
			name:        "opaque re-read error aborts",
			stored:      casCred("at-0", "rt-0"),
			readFault:   errOpaque,
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-1", "rt-1"),
			wantErr:     errOpaque,
			wantStored:  "at-0",
			wantWritten: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
			fk := credstest.NewFake()
			if tc.stored != nil {
				fk.Put(a.KeychainService, a.KeychainAccount, tc.stored)
			}
			fk.KeychainFaults = credstest.Faults{Read: tc.readFault}
			st := openTestStore(t)
			a = persistTestAccount(t, st, a)
			m := &Manager{Store: st, Creds: fk}
			before := fk.WriteCount()

			err := m.writeObservedCredential(t.Context(), a, creds.SourceKeychain, tc.prev, tc.next, nil)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeObservedCredential err = %v, want %v", err, tc.wantErr)
			}
			got, ok := fk.Get(a.KeychainService, a.KeychainAccount)
			if tc.wantStored == "" {
				if ok {
					t.Fatalf("backend holds %+v after writeObservedCredential, want it left absent", got)
				}
			} else {
				if !ok {
					t.Fatal("no credential in the fake backend after writeObservedCredential")
				}
				if got.ClaudeAiOauth.AccessToken != tc.wantStored {
					t.Fatalf("stored access token = %q, want %q", got.ClaudeAiOauth.AccessToken, tc.wantStored)
				}
			}
			if written := fk.WriteCount() > before; written != tc.wantWritten {
				t.Fatalf("write performed = %v, want %v", written, tc.wantWritten)
			}
		})
	}
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
