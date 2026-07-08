package pool

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

func hookCred() *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-hook"
	c.ClaudeAiOauth.RefreshToken = "rt-hook"
	c.ClaudeAiOauth.ExpiresAt = 1_800_000_000_000
	return c
}

// TestOnCredWriteHook pins the writeCred seam: the hook fires exactly once per
// successful store write with the written account, credential, and parent
// hash, a nil hook is a no-op, and a failed store write fails loud without
// ever reaching the hook. A successful write also persists the chain-hash
// columns.
func TestOnCredWriteHook(t *testing.T) {
	writeFault := errors.New("keychain write exploded")
	hookErr := errors.New("registry write failed")
	cases := map[string]struct {
		setHook    bool
		hookErr    error
		writeFault error
		wantErrIs  error
		wantHook   int
		wantWrites int
	}{
		"hook fires once, nil error, passthrough":       {setHook: true, wantHook: 1, wantWrites: 1},
		"hook error is swallowed":                       {setHook: true, hookErr: hookErr, wantHook: 1, wantWrites: 1},
		"store write failure skips hook and fails loud": {setHook: true, writeFault: writeFault, wantErrIs: writeFault, wantHook: 0, wantWrites: 0},
		"nil hook is a no-op":                           {setHook: false, wantHook: 0, wantWrites: 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Write: tc.writeFault}
			st := openTestStore(t)
			a := store.Account{ID: 3, ConfigDir: t.TempDir(), KeychainService: "svc-hook", KeychainAccount: "user"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			m := &Manager{Store: st, Creds: fk, LockDir: t.TempDir()}

			var (
				gotCalls  int
				gotAcct   store.Account
				gotCred   *creds.Credential
				gotParent string
			)
			if tc.setHook {
				m.OnCredWrite = func(acc store.Account, cr *creds.Credential, parentHash string) error {
					gotCalls++
					gotAcct = acc
					gotCred = cr
					gotParent = parentHash
					return tc.hookErr
				}
			}

			cred := hookCred()
			err := m.writeCred(a, creds.SourceKeychain, cred, "h-parent")
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("writeCred err = %v, want errors.Is %v", err, tc.wantErrIs)
				}
			} else if err != nil {
				t.Fatalf("writeCred err = %v, want nil", err)
			}
			if gotCalls != tc.wantHook {
				t.Fatalf("hook fired %d times, want %d", gotCalls, tc.wantHook)
			}
			if fk.WriteCount() != tc.wantWrites {
				t.Fatalf("store writes = %d, want %d", fk.WriteCount(), tc.wantWrites)
			}
			if tc.wantHook > 0 {
				if gotAcct.ID != a.ID {
					t.Fatalf("hook account ID = %d, want %d", gotAcct.ID, a.ID)
				}
				if gotCred != cred {
					t.Fatalf("hook received a different *Credential (%p) than was written (%p)", gotCred, cred)
				}
				if gotParent != "h-parent" {
					t.Fatalf("hook parentHash = %q, want h-parent", gotParent)
				}
			}
			if tc.wantWrites > 0 {
				row, err := st.GetAccount(a.ID)
				if err != nil {
					t.Fatal(err)
				}
				if row.CredHash != creds.CredentialHash(cred) || row.CredParentHash != "h-parent" {
					t.Fatalf("chain columns = (%q,%q), want (hash(cred),h-parent)", row.CredHash, row.CredParentHash)
				}
			}
		})
	}
}

// TestOnCredWriteErrorLoggedAndSwallowed proves the hook error is both logged
// (naming the account) and swallowed so a registry write never fails a refresh.
func TestOnCredWriteErrorLoggedAndSwallowed(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	fk := credstest.NewFake()
	st := openTestStore(t)
	a := store.Account{ID: 9, ConfigDir: t.TempDir(), KeychainService: "svc-hook", KeychainAccount: "user"}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	m := &Manager{Store: st, Creds: fk, LockDir: t.TempDir()}
	hookErr := errors.New("registry unreachable")
	m.OnCredWrite = func(store.Account, *creds.Credential, string) error { return hookErr }

	if err := m.writeCred(a, creds.SourceKeychain, hookCred(), ""); err != nil {
		t.Fatalf("writeCred err = %v, want nil (hook error must be swallowed)", err)
	}
	out := buf.String()
	if !strings.Contains(out, "OnCredWrite") || !strings.Contains(out, hookErr.Error()) {
		t.Fatalf("log = %q, want it to mention OnCredWrite and %q", out, hookErr)
	}
	if !strings.Contains(out, "acct-9") {
		t.Fatalf("log = %q, want it to name acct-9", out)
	}
}
