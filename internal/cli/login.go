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
	"github.com/yasyf/cc-pool/internal/execguard"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/ptyrelay"
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

// loginProc is the child surface watchAndClose drives: a bare exec.Cmd
// (execProc) or a pty relay wrapping one.
type loginProc interface {
	Start() error
	Wait() error
	Signal(os.Signal) error
	Kill() error
}

type execProc struct{ c *exec.Cmd }

func (p execProc) Start() error { return p.c.Start() }

func (p execProc) Wait() error { return p.c.Wait() }

func (p execProc) Signal(sig os.Signal) error {
	if p.c.Process == nil {
		return ptyrelay.ErrNotStarted
	}
	return p.c.Process.Signal(sig)
}

func (p execProc) Kill() error {
	if p.c.Process == nil {
		return ptyrelay.ErrNotStarted
	}
	return p.c.Process.Kill()
}

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

// runWatchedLogin runs `claude auth login` attached to this terminal — on a
// pty relay when interactive — and closes claude once the account's own login
// identity lands.
func runWatchedLogin(ctx context.Context, cmd *cobra.Command, p *pool.PendingAdd) error {
	// A fresh add has no known email yet, so no --email prefill.
	c, err := loginCommand(p.ConfigDir, "")
	if err != nil {
		return err
	}
	// On SeedKeptExisting an identity is already present and a real probe would
	// fire immediately; watch with a never-fire probe and let the user exit.
	probe := func() (bool, error) { return false, nil }
	if p.ClaudeJSONSeed != pool.SeedKeptExisting {
		probe = newIdentityProbe(pool.AccountIdentity, p.OverlayKind, p.ConfigDir)
		if isTTY() {
			note(cmd.OutOrStdout(), "Logging in with claude — the sign-in URL lands on your clipboard, and cc-pool closes claude once the login finishes.")
		}
	} else {
		note(cmd.OutOrStdout(), "Found an existing login. Exit claude when done; it's reused unless you log in again.")
	}

	outcome, werr := runLoginAttached(ctx, c, p.OverlayKind == fkoverlay.BackendFileProvider, probe)
	// On success return ctx.Err(): a cancellation while closing must stop the
	// add here, not at finalize.
	if outcome == awaitCred {
		return ctx.Err()
	}
	return werr
}

// loginURLAnnotation copies the OAuth sign-in URL and returns the note the
// relay echoes under it; copy failure degrades to a dim aside.
func loginURLAnnotation(url string) string {
	if err := copyToClipboard(url); err != nil {
		return dimStyle.Render(fmt.Sprintf("couldn't copy the login URL: %v", err))
	}
	return okStyle.Render("✓") + " " + dimStyle.Render("Login URL copied — paste it into the browser profile for this account.")
}

// runLoginAttached runs c attached to this terminal — a pty relay when
// interactive (URL copy + precise mode restore), direct stdio otherwise —
// and watches probe via watchAndClose.
func runLoginAttached(ctx context.Context, c *exec.Cmd, fp bool, probe func() (bool, error)) (awaitOutcome, error) {
	if isTTY() {
		p := ptyrelay.New(c, ptyrelay.Options{Stdin: os.Stdin, Stdout: os.Stdout, OnURL: loginURLAnnotation})
		return watchAndClose(ctx, p, fp, probe)
	}
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	// Stdin may still be a tty (stdout redirected): claude can raw it and die
	// without restoring, so snapshot and restore best-effort, no escapes.
	fd := int(os.Stdin.Fd())
	state, _ := term.GetState(fd) // nil off-terminal
	outcome, err := watchAndClose(ctx, execProc{c}, fp, probe)
	if state != nil {
		_ = term.Restore(fd, state)
	}
	return outcome, err
}

// watchAndClose starts p, waits via awaitLogin, and terminates p unless it
// exited on its own. The process owns terminal setup and teardown. With fp set
// (a File Provider account), dataless-file materialization is turned on before the
// spawn so the claude child inherits it, then restored — this long-lived parent (the
// status TUI) must never keep the process-wide policy on. A package var so the TUI
// relogin flow's launch-failure surfacing is testable without a real spawn.
var watchAndClose = func(ctx context.Context, p loginProc, fp bool, probe func() (bool, error)) (awaitOutcome, error) {
	restore := func() error { return nil }
	if fp {
		r, err := execguard.EnableForSpawn()
		if err != nil {
			return awaitCanceled, fmt.Errorf("enable dataless-file materialization: %w", err)
		}
		restore = r
	}
	startErr := p.Start()
	restoreErr := restore()
	switch {
	case startErr != nil:
		return awaitCanceled, fmt.Errorf("start claude auth login: %w", startErr)
	case restoreErr != nil:
		_ = p.Kill()
		_ = p.Wait()
		return awaitCanceled, fmt.Errorf("restore dataless-file materialization after spawn: %w", restoreErr)
	}
	procExit := make(chan error, 1)
	go func() { procExit <- p.Wait() }()
	outcome, err := awaitLogin(ctx, procExit, probe, loginPollInterval)
	if outcome != awaitExited {
		terminate(p, procExit)
	}
	return outcome, err
}

// terminate closes p, always draining procExit so the Wait goroutine exits.
func terminate(p loginProc, procExit <-chan error) {
	_ = p.Signal(syscall.SIGTERM)
	select {
	case <-procExit:
		return
	case <-time.After(killGrace):
	}
	_ = p.Kill()
	<-procExit
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
			return nil
		}
	}
}
