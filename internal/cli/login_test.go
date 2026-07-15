package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/ptyrelay"
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

func TestRunLoginAttachedNonTTY(t *testing.T) {
	dir := t.TempDir()
	stdin, err := os.CreateTemp(dir, "stdin")
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := os.CreateTemp(dir, "stdout")
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := os.CreateTemp(dir, "stderr")
	if err != nil {
		t.Fatal(err)
	}
	origStdin, origStdout, origStderr := os.Stdin, os.Stdout, os.Stderr
	os.Stdin, os.Stdout, os.Stderr = stdin, stdout, stderr
	t.Cleanup(func() {
		os.Stdin, os.Stdout, os.Stderr = origStdin, origStdout, origStderr
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
	})

	prev := watchAndClose
	t.Cleanup(func() { watchAndClose = prev })
	wantErr := errors.New("child exited")
	const wantOutput = "plain output"
	c := exec.Command("/usr/bin/true")
	watchAndClose = func(_ context.Context, p loginProc, fp bool, _ func() (bool, error)) (awaitOutcome, error) {
		ep, ok := p.(execProc)
		if !ok {
			t.Fatalf("process = %T, want execProc", p)
		}
		if ep.c != c {
			t.Fatalf("command = %p, want %p", ep.c, c)
		}
		if !fp {
			t.Fatal("fp = false, want true")
		}
		if c.Stdin != os.Stdin || c.Stdout != os.Stdout || c.Stderr != os.Stderr {
			t.Fatalf("stdio = (%p, %p, %p), want (%p, %p, %p)", c.Stdin, c.Stdout, c.Stderr, os.Stdin, os.Stdout, os.Stderr)
		}
		if _, err := c.Stdout.Write([]byte(wantOutput)); err != nil {
			t.Fatal(err)
		}
		return awaitExited, wantErr
	}

	outcome, err := runLoginAttached(context.Background(), c, true, func() (bool, error) { return false, nil })
	if outcome != awaitExited || !errors.Is(err, wantErr) {
		t.Fatalf("runLoginAttached = %v, %v; want awaitExited, %v", outcome, err, wantErr)
	}
	if _, err := stdout.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stdout)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != wantOutput {
		t.Fatalf("stdout = %q, want %q", got, wantOutput)
	}
}

func TestLoginURLAnnotation(t *testing.T) {
	const url = "https://example.com/oauth?code=abc"
	copyErr := errors.New("pbcopy unavailable")
	tests := map[string]struct {
		copy func(string) error
		want string
	}{
		"success": {
			copy: func(got string) error {
				if got != url {
					t.Errorf("copied URL = %q, want %q", got, url)
				}
				return nil
			},
			want: "Login URL copied",
		},
		"failure": {
			copy: func(got string) error {
				if got != url {
					t.Errorf("copied URL = %q, want %q", got, url)
				}
				return copyErr
			},
			want: "couldn't copy the login URL",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			prev := copyToClipboard
			copyToClipboard = tc.copy
			t.Cleanup(func() { copyToClipboard = prev })

			if got := loginURLAnnotation(url); !strings.Contains(got, tc.want) {
				t.Fatalf("annotation = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

func TestExecProcBeforeStart(t *testing.T) {
	tests := map[string]func(execProc) error{
		"signal": func(p execProc) error { return p.Signal(os.Interrupt) },
		"kill":   func(p execProc) error { return p.Kill() },
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			err := call(execProc{exec.Command("/usr/bin/true")})
			if !errors.Is(err, ptyrelay.ErrNotStarted) {
				t.Fatalf("error = %v, want %v", err, ptyrelay.ErrNotStarted)
			}
		})
	}
}

// TestTerminate's already-exited case pins the awaitCred-after-exit race: Go's select picks pseudo-randomly when tick and exit are both ready.
func TestTerminate(t *testing.T) {
	t.Run("live process is terminated", func(t *testing.T) {
		c := exec.Command("/bin/sleep", "60")
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		procExit := make(chan error, 1)
		go func() { procExit <- c.Wait() }()

		done := make(chan struct{})
		go func() { terminate(execProc{c}, procExit); close(done) }()
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
		procExit <- c.Wait()

		done := make(chan struct{})
		go func() { terminate(execProc{c}, procExit); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("terminate hung on an already-exited child")
		}
	})
}
