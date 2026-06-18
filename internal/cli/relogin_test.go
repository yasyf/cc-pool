package cli

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/keychain"
)

// cred builds a credential with the given access/refresh tokens and an
// expiry in Unix milliseconds (ExpiresAt is millis, per keychain.OAuth).
func cred(token, refresh string, expiresAtMillis int64) *keychain.Credential {
	return &keychain.Credential{ClaudeAiOauth: keychain.OAuth{
		AccessToken:  token,
		RefreshToken: refresh,
		ExpiresAt:    expiresAtMillis,
	}}
}

// TestNewReloginProbe drives the credential-change probe against an injected
// read: completion is keyed on the credential changing to a fresh, usable
// (refresh-token-bearing, unexpired) one — not on mere identity/credential
// presence, which the account already carries before re-login.
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
			read:     func() (*keychain.Credential, error) { return cred("tok-old", "", future), nil },
		},
		"revoked to fresh valid": {
			baseline: "tok-old",
			read:     func() (*keychain.Credential, error) { return cred("tok-new", "rt", future), nil },
			want:     true,
		},
		"same valid token no change": {
			baseline: "tok-A",
			read:     func() (*keychain.Credential, error) { return cred("tok-A", "rt", future), nil },
		},
		"valid token changes": {
			baseline: "tok-A",
			read:     func() (*keychain.Credential, error) { return cred("tok-B", "rt", future), nil },
			want:     true,
		},
		// A fresh-but-expired credential is not usable: re-login did not land yet.
		"new credential valid but expired": {
			baseline: "tok-old",
			read:     func() (*keychain.Credential, error) { return cred("tok-new", "rt", past), nil },
		},
		// ErrNotFound means "not yet" — a startup adoption may not have written
		// anything; the wait continues without erroring.
		"no credential yet": {
			baseline: "tok-old",
			read:     func() (*keychain.Credential, error) { return nil, keychain.ErrNotFound },
		},
		// Any read error means "not yet" — a transient backend hiccup must not
		// abort the watch and force-close the live login. The interactive user
		// bounds the wait.
		"transient read error keeps waiting": {
			baseline: "tok-old",
			read:     func() (*keychain.Credential, error) { return nil, brokenErr },
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

// TestWatchedLoginRun drives watchedLogin.Run() against real child processes
// (no claude), mirroring TestTerminate: a fresh credential mid-flight must
// close a live claude, and a manual exit must return without a force-kill. The
// injected read never touches the real reloginCred/Keychain.
func TestWatchedLoginRun(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("fresh credential closes claude", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		// The baseline read (Run's first call) sees the revoked credential; a
		// later poll sees a fresh, usable one — that's the close signal.
		var calls int
		read := func() (*keychain.Credential, error) {
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
		// A fresh credential must have closed claude via terminate (signaled or
		// killed), not let it run to a clean success exit.
		if c.ProcessState == nil || c.ProcessState.Exited() && c.ProcessState.Success() {
			t.Fatalf("process state = %v, want signaled/killed", c.ProcessState)
		}
	})

	t.Run("transient read errors then a fresh credential still closes claude", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		// The baseline read succeeds (revoked); the next polls fail transiently;
		// then a fresh, usable credential lands. A transient error must not abort
		// the watch — claude must still be closed once the credential lands.
		brokenErr := errors.New("security: keychain locked")
		var calls int
		read := func() (*keychain.Credential, error) {
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
		// Always revoked: the probe never fires, so Run returns only when the
		// process exits on its own (the awaitExited path), no force-kill.
		read := func() (*keychain.Credential, error) { return cred("tok-old", "", future), nil }
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
