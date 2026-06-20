package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"golang.org/x/term"
)

const (
	// loginPollInterval is how often the account's login identity is probed
	// while a login is in flight.
	loginPollInterval = 500 * time.Millisecond
	// killGrace is how long a SIGTERMed claude gets before SIGKILL.
	killGrace = 3 * time.Second
)

// inputModeReset disables the terminal input modes claude may have enabled —
// bracketed paste, mouse (normal/button/any), SGR mouse, focus reporting, and
// the kitty keyboard protocol — so a force-killed claude can't leave them on.
const inputModeReset = "\x1b[?2004l\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1004l\x1b[?1006l\x1b[<u"

// awaitOutcome is how a watched login ended.
type awaitOutcome int

const (
	awaitCred     awaitOutcome = iota // the login identity landed; claude still running
	awaitExited                       // the process exited first (user quit claude)
	awaitCanceled                     // the wait was aborted: context canceled or probe failure
)

// awaitLogin polls probe every interval until it reports done, the process
// exits, or ctx is canceled. probe errors abort the wait — a broken probe must
// not become a silent infinite retry loop. For awaitExited the process's exit
// error (possibly nil) is passed through.
func awaitLogin(ctx context.Context, procExit <-chan error, probe func() (bool, error), interval time.Duration) (awaitOutcome, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-procExit:
			return awaitExited, err
		case <-ctx.Done():
			return awaitCanceled, ctx.Err()
		case <-ticker.C:
			done, err := probe()
			if err != nil {
				return awaitCanceled, err
			}
			if done {
				return awaitCred, nil
			}
		}
	}
}

// identityFunc matches pool.AccountIdentity; injectable for tests.
type identityFunc func(kind overlay.Kind, configDir string) (*pool.Identity, error)

// newIdentityProbe returns a probe reporting that the account's own .claude.json
// now carries a fresh oauthAccount identity — the signal that a real
// `claude /login` completed. The mere appearance of a credential is NOT the
// signal: with a fresh CLAUDE_CONFIG_DIR claude adopts the global session's
// secret into the account's Keychain item (or, headless over SSH, its plaintext
// .credentials.json) at startup, before any login — but it writes no identity,
// so this never fires on that adoption. ErrNoIdentity means "not yet"; any other
// error aborts the wait (a broken read must not become a silent retry loop).
func newIdentityProbe(read identityFunc, kind overlay.Kind, configDir string) func() (bool, error) {
	return func() (bool, error) {
		_, err := read(kind, configDir)
		if errors.Is(err, pool.ErrNoIdentity) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

// runWatchedLogin runs `claude /login` attached to the terminal and watches
// for the account's own login identity to land; when it does, it closes claude
// for the user. It delegates the spawn/poll/terminate to watchAndClose (the
// single watched-login primitive) and owns only terminal setup/teardown. The
// child stays in our foreground process group (a background pgrp touching the
// tty would be stopped with SIGTTIN/SIGTTOU), so termination signals target
// c.Process directly and the terminal is only restored after the child exited.
//
// On the kept-existing reuse path (the dir already holds a logged-in identity)
// completion cannot be detected — the identity is already present, so an
// identity probe would fire immediately — so the session runs with a never-fire
// probe and the user exits claude themselves, exactly the pre-watcher behavior.
func runWatchedLogin(ctx context.Context, cmd *cobra.Command, p *pool.PendingAdd) error {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	// Watch for a fresh login unless the dir already holds a logged-in identity
	// (SeedKeptExisting — the documented reuse path): there the identity is
	// already present, so the identity probe would fire immediately. PrepareAdd
	// already computed this, so no credential read is needed here.
	probe := func() (bool, error) { return false, nil }
	if p.ClaudeJSONSeed != pool.SeedKeptExisting {
		probe = newIdentityProbe(pool.AccountIdentity, p.OverlayKind, p.ConfigDir)
	} else {
		note(cmd.OutOrStdout(), "Found an existing login. Exit claude when done; it's reused unless you log in again.")
	}

	fd := int(os.Stdin.Fd())
	state, _ := term.GetState(fd) // nil on non-TTY; restore is nil-safe

	//nolint:gosec // G204: bin is the resolved claude executable path; "/login" is a fixed argument
	c := exec.Command(bin, "/login")
	c.Env = execEnv(os.Environ(), p.ConfigDir)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	outcome, werr := watchAndClose(ctx, c, probe)
	restoreTerminal(cmd.OutOrStdout(), fd, state)
	// awaitCred: a fresh account identity landed — a real login completed (a
	// startup adoption of the global credential writes none); claude was closed.
	// A cancellation while closing still stops the add here, not at finalize.
	if outcome == awaitCred {
		return ctx.Err()
	}
	// awaitExited (user quit claude) and awaitCanceled (ctx canceled or a probe
	// error) both surface the watch error directly.
	return werr
}

// watchAndClose starts c, polls probe every loginPollInterval, and closes c (via
// terminate) once the probe fires or ctx cancels; on a manual exit it leaves c
// alone. Returns the await outcome and its error. The caller owns terminal setup
// and teardown.
func watchAndClose(ctx context.Context, c *exec.Cmd, probe func() (bool, error)) (awaitOutcome, error) {
	if err := c.Start(); err != nil {
		return awaitCanceled, fmt.Errorf("start claude /login: %w", err)
	}
	procExit := make(chan error, 1)
	go func() { procExit <- c.Wait() }()
	outcome, err := awaitLogin(ctx, procExit, probe, loginPollInterval)
	if outcome != awaitExited {
		terminate(c, procExit)
	}
	return outcome, err
}

// terminate closes the child: SIGTERM, a grace period, then SIGKILL; always
// drains procExit so the Wait goroutine finishes.
func terminate(c *exec.Cmd, procExit <-chan error) {
	_ = c.Process.Signal(syscall.SIGTERM)
	select {
	case <-procExit:
		return
	case <-time.After(killGrace):
	}
	_ = c.Process.Kill()
	<-procExit
}

// restoreTerminal undoes whatever raw-mode state a (possibly SIGKILLed) claude
// left behind: termios via the saved state, then explicit escape sequences to
// leave the alternate screen, show the cursor, disable bracketed paste, mouse
// and focus-event reporting, pop the kitty keyboard protocol, and reset SGR.
// Escapes are only emitted on a real terminal.
func restoreTerminal(out io.Writer, fd int, state *term.State) {
	if state != nil {
		_ = term.Restore(fd, state)
	}
	if isTTY() {
		_, _ = fmt.Fprint(out, "\x1b[?1049l\x1b[?25h"+inputModeReset+"\x1b[0m")
	}
}

// waitForLogin polls until the account's own .claude.json shows a fresh
// oauthAccount identity — a real `claude /login` completed — showing a one-line
// spinner. Used by the manual login path; unbounded — the user may take
// arbitrarily long in another terminal, and ^C cancels. A startup adoption of
// the global credential writes no identity, so it never trips this.
func waitForLogin(ctx context.Context, out io.Writer, kind overlay.Kind, configDir string) error {
	probe := newIdentityProbe(pool.AccountIdentity, kind, configDir)
	ticker := time.NewTicker(loginPollInterval)
	defer ticker.Stop()
	for i := 0; ; i++ {
		_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render("waiting for login… press ctrl-c to abort"))
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return ctx.Err()
		case <-ticker.C:
			done, err := probe()
			if err != nil {
				_, _ = fmt.Fprint(out, "\r\x1b[K")
				return err
			}
			if !done {
				continue
			}
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			success(out, "Logged in.")
			return nil
		}
	}
}
