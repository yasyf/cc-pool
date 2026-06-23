package cli

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func TestAwaitLogin(t *testing.T) {
	sentinel := errors.New("exit status 1")
	probeErr := errors.New("security: keychain locked")

	t.Run("credential lands on a later tick", func(t *testing.T) {
		calls := 0
		probe := func() (bool, error) {
			calls++
			return calls >= 3, nil
		}
		outcome, err := awaitLogin(context.Background(), make(chan error), probe, time.Millisecond)
		if outcome != awaitCred || err != nil {
			t.Fatalf("outcome = %v err = %v, want awaitCred, nil", outcome, err)
		}
		if calls < 3 {
			t.Fatalf("probe called %d time(s), want ≥3", calls)
		}
	})

	t.Run("process exits first with an error", func(t *testing.T) {
		procExit := make(chan error, 1)
		procExit <- sentinel
		// Hour-long interval: the ticker must never decide this case.
		outcome, err := awaitLogin(context.Background(), procExit,
			func() (bool, error) { t.Fatal("probe must not run"); return false, nil }, time.Hour)
		if outcome != awaitExited || !errors.Is(err, sentinel) {
			t.Fatalf("outcome = %v err = %v, want awaitExited with the exit error", outcome, err)
		}
	})

	t.Run("process exits cleanly", func(t *testing.T) {
		procExit := make(chan error, 1)
		procExit <- nil
		outcome, err := awaitLogin(context.Background(), procExit,
			func() (bool, error) { return false, nil }, time.Hour)
		if outcome != awaitExited || err != nil {
			t.Fatalf("outcome = %v err = %v, want awaitExited, nil", outcome, err)
		}
	})

	t.Run("probe failure aborts instead of silently retrying", func(t *testing.T) {
		outcome, err := awaitLogin(context.Background(), make(chan error),
			func() (bool, error) { return false, probeErr }, time.Millisecond)
		if outcome != awaitCanceled || !errors.Is(err, probeErr) {
			t.Fatalf("outcome = %v err = %v, want awaitCanceled with the probe error", outcome, err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		outcome, err := awaitLogin(ctx, make(chan error),
			func() (bool, error) { return false, nil }, time.Hour)
		if outcome != awaitCanceled || !errors.Is(err, context.Canceled) {
			t.Fatalf("outcome = %v err = %v, want awaitCanceled, context.Canceled", outcome, err)
		}
	})
}

func TestNewIdentityProbe(t *testing.T) {
	brokenErr := errors.New("parse .claude.json: unexpected end of JSON input")
	cases := map[string]struct {
		read    func(fkoverlay.Backend, string) (*pool.Identity, error)
		want    bool
		wantErr error
	}{
		// A startup adoption copies the global credential but writes no identity.
		"no identity yet is not done": {
			read: func(fkoverlay.Backend, string) (*pool.Identity, error) { return nil, pool.ErrNoIdentity },
		},
		"a real login wrote the account identity is done": {
			read: func(fkoverlay.Backend, string) (*pool.Identity, error) {
				return &pool.Identity{AccountUUID: "u-1", EmailAddress: "a@example.com"}, nil
			},
			want: true,
		},
		"a broken identity read aborts instead of silently retrying": {
			read:    func(fkoverlay.Backend, string) (*pool.Identity, error) { return nil, brokenErr },
			wantErr: brokenErr,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			probe := newIdentityProbe(tc.read, fkoverlay.BackendSymlink, "/cfg")
			done, err := probe()
			if done != tc.want || !errors.Is(err, tc.wantErr) {
				t.Errorf("probe() = %v, %v; want %v, %v", done, err, tc.want, tc.wantErr)
			}
		})
	}
}

// TestTerminate drives terminate against real processes: a live child must be
// signaled down within the grace period, and a child that already exited (the
// awaitCred-after-exit race — select picks pseudo-randomly when both the tick
// and the exit are ready) must not hang on the already-drained channel.
func TestTerminate(t *testing.T) {
	t.Run("live process is terminated", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		procExit := make(chan error, 1)
		go func() { procExit <- c.Wait() }()

		done := make(chan struct{})
		go func() { terminate(c, procExit); close(done) }()
		select {
		case <-done:
		case <-time.After(killGrace + 2*time.Second):
			t.Fatal("terminate did not return within the kill grace")
		}
		if c.ProcessState == nil || c.ProcessState.Exited() && c.ProcessState.Success() {
			t.Fatalf("process state = %v, want signaled/killed", c.ProcessState)
		}
	})

	t.Run("already-exited process returns immediately", func(t *testing.T) {
		c := exec.Command("/usr/bin/true")
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		procExit := make(chan error, 1)
		procExit <- c.Wait() // child fully reaped before terminate runs

		done := make(chan struct{})
		go func() { terminate(c, procExit); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("terminate hung on an already-exited child")
		}
	})
}
