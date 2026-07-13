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
// alone, or a credential appearing over a slot decided empty — aborts with
// ErrCredentialChangedUnderfoot without clobbering, an absent backend
// proceeds, and a re-read that fails for any reason other than a proven-empty
// slot aborts with ErrCredentialUnverifiable.
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
			name:        "absent backend proceeds (no prior value to compare)",
			stored:      nil,
			prev:        casCred("at-0", "rt-0"),
			next:        casCred("at-1", "rt-1"),
			wantErr:     nil,
			wantStored:  "at-1",
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
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			m := &Manager{Store: st, Creds: fk, LockDir: t.TempDir()}
			before := fk.WriteCount()

			err := m.writeCredCAS(a, creds.SourceKeychain, tc.prev, tc.next)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("writeCredCAS err = %v, want %v", err, tc.wantErr)
			}
			got, ok := fk.Get(a.KeychainService, a.KeychainAccount)
			if !ok {
				t.Fatal("no credential in the fake backend after writeCredCAS")
			}
			if got.ClaudeAiOauth.AccessToken != tc.wantStored {
				t.Fatalf("stored access token = %q, want %q", got.ClaudeAiOauth.AccessToken, tc.wantStored)
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
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	fk.Put(a.KeychainService, a.KeychainAccount, casCred("at-0", "rt-0"))
	m := &Manager{Store: st, Creds: fk, LockDir: t.TempDir()}
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
