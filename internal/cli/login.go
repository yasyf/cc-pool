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
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"golang.org/x/term"
)

const (
	loginPollInterval = 500 * time.Millisecond
	killGrace         = 3 * time.Second
)

// inputModeReset disables the terminal input modes claude may have enabled —
// bracketed paste, mouse (normal/button/any), SGR mouse, focus reporting, and
// the kitty keyboard protocol — so a force-killed claude can't leave them on.
const inputModeReset = "\x1b[?2004l\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1004l\x1b[?1006l\x1b[<u"

type awaitOutcome int

const (
	awaitCred     awaitOutcome = iota // the login identity landed; claude still running
	awaitExited                       // the process exited first (user quit claude)
	awaitCanceled                     // the wait was aborted: context canceled or probe failure
)

// awaitLogin polls probe until it reports done, the process exits, or ctx is
// canceled; a probe error aborts the wait rather than retrying silently.
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
type identityFunc func(backend fkoverlay.Backend, configDir string) (*pool.Identity, error)

// newIdentityProbe returns a probe for a fresh oauthAccount identity in the
// account's own .claude.json — the only reliable login signal: at startup
// claude pre-seeds a fresh CLAUDE_CONFIG_DIR with the global credential but
// writes no identity.
func newIdentityProbe(read identityFunc, backend fkoverlay.Backend, configDir string) func() (bool, error) {
	return func() (bool, error) {
		_, err := read(backend, configDir)
		if errors.Is(err, pool.ErrNoIdentity) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

// runWatchedLogin runs `claude /login` attached to the terminal and closes
// claude once the account's own login identity lands. The child stays in our
// foreground process group (a background pgrp touching the tty would stop on
// SIGTTIN/SIGTTOU).
func runWatchedLogin(ctx context.Context, cmd *cobra.Command, p *pool.PendingAdd) error {
	bin, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("`claude` not found on PATH: %w", err)
	}
	// On SeedKeptExisting an identity is already present and a real probe would
	// fire immediately; watch with a never-fire probe and let the user exit.
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
	// On success return ctx.Err(): a cancellation while closing must stop the
	// add here, not at finalize.
	if outcome == awaitCred {
		return ctx.Err()
	}
	return werr
}

// watchAndClose starts c, waits via awaitLogin, and terminates c unless it
// exited on its own. The caller owns terminal setup and teardown.
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

// terminate closes c, always draining procExit so the Wait goroutine exits.
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

// restoreTerminal undoes terminal state a possibly-SIGKILLed claude left
// behind: saved termios, then escapes to leave the alt screen, show the
// cursor, and reset input modes and SGR.
func restoreTerminal(out io.Writer, fd int, state *term.State) {
	if state != nil {
		_ = term.Restore(fd, state)
	}
	if isTTY() {
		_, _ = fmt.Fprint(out, "\x1b[?1049l\x1b[?25h"+inputModeReset+"\x1b[0m")
	}
}

// waitForLogin polls until the account's own login identity lands.
// Deliberately unbounded: the user may take arbitrarily long in another
// terminal; ^C cancels.
func waitForLogin(ctx context.Context, out io.Writer, backend fkoverlay.Backend, configDir string) error {
	probe := newIdentityProbe(pool.AccountIdentity, backend, configDir)
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
