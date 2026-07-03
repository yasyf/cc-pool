package cli

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func cred(token, refresh string, expiresAtMillis int64) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken:  token,
		RefreshToken: refresh,
		ExpiresAt:    expiresAtMillis,
	}}
}

// TestNewReloginProbe: completion keys on the credential turning fresh and usable
// (refresh-token-bearing, unexpired), not on mere presence the account already
// carries before re-login.
func TestNewReloginProbe(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()
	past := time.Now().Add(-time.Hour).UnixMilli()
	brokenErr := errors.New("security: keychain locked")

	cases := map[string]struct {
		baseline string
		read     credReader
		want     bool
		wantErr  error
	}{
		// Claude clears the refresh token to "" on a dead token; a still-revoked
		// credential is not a completed login even though its access token is new.
		"revoked stays revoked": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return cred("tok-old", "", future), nil },
		},
		"revoked to fresh valid": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return cred("tok-new", "rt", future), nil },
			want:     true,
		},
		"same valid token no change": {
			baseline: "tok-A",
			read:     func() (*creds.Credential, error) { return cred("tok-A", "rt", future), nil },
		},
		"valid token changes": {
			baseline: "tok-A",
			read:     func() (*creds.Credential, error) { return cred("tok-B", "rt", future), nil },
			want:     true,
		},
		// A fresh-but-expired credential is not usable: re-login did not land yet.
		"new credential valid but expired": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return cred("tok-new", "rt", past), nil },
		},
		// ErrNotFound means "not yet": the wait continues without erroring.
		"no credential yet": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return nil, creds.ErrNotFound },
		},
		// Any read error means "not yet": a transient backend hiccup must not
		// abort the watch and force-close the live login.
		"transient read error keeps waiting": {
			baseline: "tok-old",
			read:     func() (*creds.Credential, error) { return nil, brokenErr },
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			probe := newReloginProbe(tc.read, tc.baseline)
			done, err := probe()
			if done != tc.want || !errors.Is(err, tc.wantErr) {
				t.Errorf("probe() = %v, %v; want %v, %v", done, err, tc.want, tc.wantErr)
			}
		})
	}
}

// TestFinishRelogin pins that the post-login credential gate resolves through
// m.ReadCredential — both backends in resolution order — so a headless session
// surfaces the Keychain's unknown state (creds.ErrUnavailable) instead of a
// bogus not-found, and only a usable credential clears the needs-login flag.
func TestFinishRelogin(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()
	past := time.Now().Add(-time.Hour).UnixMilli()

	cases := map[string]struct {
		keychain     *creds.Credential
		file         *creds.Credential
		keychainRead error // injected keychain Read fault
		wantErr      error // errors.Is target; nil with empty wantContains = success
		wantContains []string
		wantOmits    []string
		wantWrites   int // seam keychain writes (the ACL re-assertion)
	}{
		"keychain-backed login lands and re-asserts the ACL": {
			keychain:   cred("at-new", "rt", future),
			wantWrites: 1,
		},
		"file-backed login lands": {
			file: cred("at-new", "rt", future),
		},
		"headless unsearchable keychain surfaces unknown state, not absence": {
			keychainRead: creds.ErrUnavailable,
			wantErr:      creds.ErrUnavailable,
			wantContains: []string{"login keychain not in the security search list", "ccp login 3"},
			wantOmits:    []string{"not found"},
		},
		"no credential in either backend": {
			wantErr:      creds.ErrNotFound,
			wantContains: []string{"ccp login 3"},
		},
		"revoked credential (no refresh token) fails closed": {
			keychain:     cred("at-new", "", future),
			wantContains: []string{"no usable credential", "ccp login 3"},
		},
		"expired credential fails closed": {
			keychain:     cred("at-new", "rt", past),
			wantContains: []string{"no usable credential"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			st, err := store.Open(filepath.Join(home, "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			a := store.Account{ID: 3, ConfigDir: filepath.Join(home, "acct-03"), KeychainService: "svc-03", KeychainAccount: "user", Label: "bob@example.com"}
			if err := st.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
			if _, err := st.SetNeedsLogin(a.ID, time.Now(), "revoked"); err != nil {
				t.Fatal(err)
			}
			fk := credstest.NewFake()
			fk.KeychainFaults = credstest.Faults{Read: tc.keychainRead}
			if tc.keychain != nil {
				fk.Put(a.KeychainService, a.KeychainAccount, tc.keychain)
			}
			if tc.file != nil {
				if err := creds.WriteFileCredential(a.ConfigDir, tc.file); err != nil {
					t.Fatal(err)
				}
			}
			m := &pool.Manager{Store: st, Creds: fk, LockDir: t.TempDir()}

			ferr := finishRelogin(context.Background(), m, a)

			h, herr := st.GetAuthHealth(a.ID)
			if herr != nil {
				t.Fatal(herr)
			}
			if wantOK := tc.wantErr == nil && len(tc.wantContains) == 0; wantOK {
				if ferr != nil {
					t.Fatalf("finishRelogin: %v", ferr)
				}
				if h.NeedsLogin {
					t.Error("needs-login flag not cleared by a successful re-login")
				}
				if got := fk.WriteCount(); got != tc.wantWrites {
					t.Errorf("keychain writes = %d, want %d", got, tc.wantWrites)
				}
				return
			}
			if ferr == nil {
				t.Fatal("finishRelogin succeeded, want failure")
			}
			if tc.wantErr != nil && !errors.Is(ferr, tc.wantErr) {
				t.Errorf("err = %v, want errors.Is %v", ferr, tc.wantErr)
			}
			for _, frag := range tc.wantContains {
				if !strings.Contains(ferr.Error(), frag) {
					t.Errorf("err %q missing %q", ferr, frag)
				}
			}
			for _, frag := range tc.wantOmits {
				if strings.Contains(ferr.Error(), frag) {
					t.Errorf("err %q must not contain %q", ferr, frag)
				}
			}
			if !h.NeedsLogin {
				t.Error("needs-login flag cleared by a failed re-login")
			}
		})
	}
}

// TestWatchedLoginRun drives watchedLogin.Run() against real child processes
// (no claude): a fresh credential mid-flight closes claude, a manual exit
// returns without a force-kill. The injected read never touches the real credential backends.
func TestWatchedLoginRun(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("fresh credential closes claude", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		// Baseline read (Run's first call) is revoked; a later poll turns fresh — the close signal.
		var calls int
		read := func() (*creds.Credential, error) {
			calls++
			if calls <= 2 {
				return cred("tok-old", "", future), nil // revoked: no refresh token
			}
			return cred("tok-new", "rt", future), nil // fresh + usable
		}
		wl := &watchedLogin{ctx: context.Background(), cmd: c, read: read}

		done := make(chan error, 1)
		go func() { done <- wl.Run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(killGrace + 2*time.Second):
			t.Fatal("Run did not return after a fresh credential landed")
		}
		// A fresh credential must close claude (signaled or killed), not run to a clean exit.
		if c.ProcessState == nil || c.ProcessState.Exited() && c.ProcessState.Success() {
			t.Fatalf("process state = %v, want signaled/killed", c.ProcessState)
		}
	})

	t.Run("transient read errors then a fresh credential still closes claude", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		// A transient read error must not abort the watch — claude must still
		// close once the credential lands.
		brokenErr := errors.New("security: keychain locked")
		var calls int
		read := func() (*creds.Credential, error) {
			calls++
			switch {
			case calls == 1:
				return cred("tok-old", "", future), nil // baseline: revoked
			case calls <= 3:
				return nil, brokenErr // transient backend hiccup
			default:
				return cred("tok-new", "rt", future), nil // fresh + usable
			}
		}
		wl := &watchedLogin{ctx: context.Background(), cmd: c, read: read}

		done := make(chan error, 1)
		go func() { done <- wl.Run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(killGrace + 2*time.Second):
			t.Fatal("Run did not return after a fresh credential landed")
		}
		if c.ProcessState == nil || c.ProcessState.Exited() && c.ProcessState.Success() {
			t.Fatalf("process state = %v, want signaled/killed", c.ProcessState)
		}
	})

	t.Run("manual exit needs no kill", func(t *testing.T) {
		c := exec.Command("/usr/bin/true")
		// Always revoked: the probe never fires, so Run returns on the child's own exit (awaitExited), no force-kill.
		read := func() (*creds.Credential, error) { return cred("tok-old", "", future), nil }
		wl := &watchedLogin{ctx: context.Background(), cmd: c, read: read}

		done := make(chan error, 1)
		go func() { done <- wl.Run() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run hung on a self-exiting child")
		}
		if c.ProcessState == nil || !c.ProcessState.Success() {
			t.Fatalf("process state = %v, want clean self-exit (no kill)", c.ProcessState)
		}
	})
}
