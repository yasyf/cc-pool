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

// TestWriteCredCAS pins the compare-and-swap guard: a matching snapshot writes
// through, a changed-underfoot re-read aborts with ErrCredentialChangedUnderfoot
// without clobbering, and an absent backend proceeds.
func TestWriteCredCAS(t *testing.T) {
	cases := []struct {
		name        string
		stored      *creds.Credential // seeded backend value; nil = absent
		prevAccess  string            // the snapshot next was derived from
		next        *creds.Credential
		wantErr     error
		wantStored  string // access token expected in the backend afterward
		wantWritten bool
	}{
		{
			name:        "matching snapshot writes through",
			stored:      casCred("at-0", "rt-0"),
			prevAccess:  "at-0",
			next:        casCred("at-1", "rt-1"),
			wantErr:     nil,
			wantStored:  "at-1",
			wantWritten: true,
		},
		{
			name:        "changed underfoot aborts and keeps the newer credential",
			stored:      casCred("at-claude", "rt-claude"),
			prevAccess:  "at-0",
			next:        casCred("at-1", "rt-1"),
			wantErr:     ErrCredentialChangedUnderfoot,
			wantStored:  "at-claude",
			wantWritten: false,
		},
		{
			name:        "absent backend proceeds (no prior value to compare)",
			stored:      nil,
			prevAccess:  "at-0",
			next:        casCred("at-1", "rt-1"),
			wantErr:     nil,
			wantStored:  "at-1",
			wantWritten: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
			fk := credstest.NewFake()
			if tc.stored != nil {
				fk.Put(a.KeychainService, a.KeychainAccount, tc.stored)
			}
			st := openTestStore(t)
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			m := &Manager{Store: st, Creds: fk, LockDir: t.TempDir()}
			before := fk.WriteCount()

			err := m.writeCredCAS(a, creds.SourceKeychain, tc.prevAccess, tc.next, "")
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

// TestAdoptRotatedTokenRecordsLineage pins the ""-parent resolution: adopting a
// session-rotated credential records the pre-rotation cred_hash as its parent,
// and adopting an unchanged credential leaves both columns intact.
func TestAdoptRotatedTokenRecordsLineage(t *testing.T) {
	st := openTestStore(t)
	a := store.Account{ID: 1, ConfigDir: t.TempDir(), KeychainService: "svc", KeychainAccount: "user"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	m := &Manager{Store: st, Creds: fk, LockDir: t.TempDir()}

	c1 := casCred("at-1", "rt-1")
	fk.Put(a.KeychainService, a.KeychainAccount, c1)
	if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	row, err := st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.CredHash != creds.CredentialHash(c1) || row.CredParentHash != "" {
		t.Fatalf("first adopt columns = (%q,%q), want (hash(c1),\"\")", row.CredHash, row.CredParentHash)
	}

	// A live session rotates the chain; the adopt records c1 as c2's parent.
	c2 := casCred("at-2", "rt-2")
	fk.Put(a.KeychainService, a.KeychainAccount, c2)
	if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	row, err = st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.CredHash != creds.CredentialHash(c2) || row.CredParentHash != creds.CredentialHash(c1) {
		t.Fatalf("rotation adopt columns = (%q,%q), want (hash(c2),hash(c1))", row.CredHash, row.CredParentHash)
	}

	// An adopt of the unchanged credential must not corrupt the lineage.
	if err := m.AdoptRotatedToken(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	row, err = st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if row.CredHash != creds.CredentialHash(c2) || row.CredParentHash != creds.CredentialHash(c1) {
		t.Fatalf("unchanged adopt columns = (%q,%q), want (hash(c2),hash(c1)) intact", row.CredHash, row.CredParentHash)
	}
}
