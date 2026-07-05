package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpElection is the three-way pluginkit reading of the File Provider
// extension: not registered, registered-but-unelected, or elected.
type fpElection int

const (
	fpNotRegistered fpElection = iota // pluginkit has never seen the appex
	fpNotElected                      // registered but not elected ('-', '?', '!')
	fpElected                         // '+': fpAvailable's true
)

// classifyFPElection parses `pluginkit -m -i <id>` output (one line per copy).
// Any elected copy counts — a stale duplicate in the Trash must not mask a live election.
func classifyFPElection(out string) fpElection {
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return fpNotRegistered
	}
	for _, line := range strings.Split(trimmed, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "+") {
			return fpElected
		}
	}
	return fpNotElected
}

// fpElectionState is the pluginkit seam behind the onboarding trichotomy:
// exit 1 with no stderr is a clean "not registered"; stderr means a real failure.
var fpElectionState = func(ctx context.Context) (fpElection, error) {
	out, err := exec.CommandContext(ctx, "pluginkit", "-m", "-i", pool.FPExtensionBundleID).Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && len(bytes.TrimSpace(exitErr.Stderr)) == 0 {
		return fpNotRegistered, nil
	}
	if err != nil {
		return fpNotRegistered, fmt.Errorf("pluginkit -m -i %s: %w", pool.FPExtensionBundleID, err)
	}
	return classifyFPElection(string(out)), nil
}

// fpElect elects the appex headlessly (`pluginkit -e use`). It cannot clear a
// user-disabled election — only the Settings toggle can — so callers treat a
// still-unelected reading afterwards as "send the user to Settings".
var fpElect = func(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "pluginkit", "-e", "use", "-i", pool.FPExtensionBundleID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pluginkit -e use -i %s: %w (%s)", pool.FPExtensionBundleID, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// fpOpenSettings deep-links the System Settings File Providers pane — a seam
// over the backend's Enablement URLs.
var fpOpenSettings = func(ctx context.Context) error {
	return fkoverlay.BackendFileProvider.OpenSettings(ctx)
}

// fpDaemonProbe reports the daemon's liveness and its consent-pending signal —
// the precise "bridge bind parked on the group-container TCC prompt" fact only
// the daemon can observe. A seam for tests.
var fpDaemonProbe = func() (alive, consentPending bool) {
	cl := daemon.NewClient()
	if h, err := cl.Health(); err != nil || !h.OK {
		return false, false
	}
	if st, err := cl.Status(); err == nil && st.OK {
		return true, st.FPConsentPending
	}
	return true, false
}

const (
	// fpOnboardPollInterval paces the election wait and the health-rung polls.
	fpOnboardPollInterval = 500 * time.Millisecond
	// fpRungAttempts bounds each health-rung poll: the app just launched and the
	// daemon binds with retry, so a healthy stack answers within seconds — a
	// rung still down after the window is stuck, not slow.
	fpRungAttempts = 20
)

func newFPCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fp",
		Short: "Manage the File Provider overlay",
	}
	cmd.AddCommand(newFPOnboardCmd())
	return cmd
}

func newFPOnboardCmd() *cobra.Command {
	pane := fkoverlay.BackendFileProvider.Enablement().Pane
	return &cobra.Command{
		Use:   "onboard",
		Short: "Install, enable, and adopt the File Provider overlay end to end",
		Long: `onboard walks the File Provider overlay from zero to serving: it installs
(or upgrades) the CCPoolStatus app — the extension's host — launches it,
elects the extension with pluginkit, and if macOS still holds it disabled,
opens System Settings ▸ ` + pane + `
(the one toggle no CLI can flip) and waits for you. Once enabled it verifies
the health ladder — extension → app control socket → daemon bridge socket —
naming the exact fix for whichever rung is stuck, then offers to migrate
your accounts onto File Provider.

Idempotent: steps already satisfied are skipped.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runFPOnboard(cmd) },
	}
}

func runFPOnboard(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if _, err := exec.LookPath("brew"); err != nil {
		return fmt.Errorf("the CCPoolStatus app installs via Homebrew, which isn't on PATH (to build from source instead, see widget/README.md): %w", err)
	}
	if err := ensureWidgetTap(cmd); err != nil {
		return err
	}
	step(out, "Installing the CCPoolStatus app (it hosts the File Provider extension)…")
	if err := brewInstallWidget(cmd); err != nil {
		return err
	}
	step(out, "Launching it so macOS registers the extension…")
	if err := launchWidgetApp(cmd); err != nil {
		return err
	}

	if err := awaitFPElection(cmd.Context(), out, fpOnboardPollInterval); err != nil {
		return err
	}
	success(out, "File Provider extension enabled.")

	if err := checkFPRungs(cmd.Context(), out, fpOnboardPollInterval); err != nil {
		return err
	}
	success(out, "File Provider stack healthy.")

	return offerFPMigration(cmd)
}

// awaitFPElection drives the pluginkit election to elected (a user-disabled
// election only the Settings toggle clears). Unbounded like waitForLogin; ^C cancels.
func awaitFPElection(ctx context.Context, out io.Writer, interval time.Duration) error {
	en := fkoverlay.BackendFileProvider.Enablement()
	triedElect, openedSettings := false, false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for i := 0; ; i++ {
		state, err := fpElectionState(ctx)
		if err != nil {
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return err
		}
		switch state {
		case fpElected:
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return nil
		case fpNotElected:
			if !triedElect {
				triedElect = true
				if err := fpElect(ctx); err != nil {
					_, _ = fmt.Fprint(out, "\r\x1b[K")
					warn(out, "headless election failed: %v", err)
				}
				continue // re-read at once: a working `pluginkit -e use` lands synchronously
			}
			if !openedSettings {
				openedSettings = true
				_, _ = fmt.Fprint(out, "\r\x1b[K")
				step(out, "macOS is holding the extension disabled; only the Settings toggle can enable it.")
				step(out, "Flip CCPoolStatus ON under System Settings ▸ %s.", en.Pane)
				if err := fpOpenSettings(ctx); err != nil {
					warn(out, "couldn't open System Settings (%v) — navigate there yourself", err)
				}
			}
		case fpNotRegistered:
			// The just-launched app registers within seconds; nothing to do but wait.
		}
		msg := "waiting for macOS to register the extension… press ctrl-c to abort"
		if state == fpNotElected {
			msg = "waiting for the File Providers toggle… press ctrl-c to abort"
		}
		_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render(msg))
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// checkFPRungs walks the remaining File Provider rungs (app control socket,
// then daemon bridge socket), naming the exact fix for whichever rung stays down.
func checkFPRungs(ctx context.Context, out io.Writer, interval time.Duration) error {
	ver, err := pollFPRung(ctx, out, interval, "waiting for the CCPoolStatus control socket…", func() (string, error) {
		return fpControlHealth(ctx)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("the CCPoolStatus control socket %s never answered (%w) — launch %s and re-run `ccp fp onboard`",
			abbreviateHome(pool.FPControlSocketPath()), err, pool.WidgetAppPath())
	}
	step(out, "CCPoolStatus app serving (%s).", ver)

	_, err = pollFPRung(ctx, out, interval, "waiting for the daemon's bridge socket…", func() (string, error) {
		if fpBridgeReachable() {
			return "", nil
		}
		return "", errors.New("not accepting")
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		alive, pending := fpDaemonProbe()
		switch {
		case !alive:
			return fmt.Errorf("the daemon isn't running, so its bridge socket %s can't come up — start it with `ccp service install`, then re-run `ccp fp onboard`",
				abbreviateHome(pool.FPBridgeSocketPath()))
		case pending:
			return errors.New("the daemon is up but its bridge bind is parked on the one-time app group container consent prompt (macOS re-asks after every upgrade, and launchd never surfaces it) — approve the prompt, then restart the daemon (`brew services restart cc-pool`) and re-run `ccp fp onboard`")
		default:
			return errors.New("the daemon is up but its bridge socket " + abbreviateHome(pool.FPBridgeSocketPath()) +
				" isn't accepting — approve the app group container consent prompt if one is pending, restart the daemon (`brew services restart cc-pool`), and re-run `ccp fp onboard`; check " + abbreviateHome(pool.LogPath()))
		}
	}
	step(out, "Daemon bridge socket up.")
	return nil
}

// pollFPRung polls probe up to fpRungAttempts under a spinner, returning the
// probe's value on the first success or its last error once the window closes.
func pollFPRung(ctx context.Context, out io.Writer, interval time.Duration, msg string, probe func() (string, error)) (string, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastErr error
	for i := 0; i < fpRungAttempts; i++ {
		v, err := probe()
		if err == nil {
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return v, nil
		}
		lastErr = err
		_, _ = fmt.Fprintf(out, "\r%s %s", spinnerFrames[i%len(spinnerFrames)], dimStyle.Render(msg))
		select {
		case <-ctx.Done():
			_, _ = fmt.Fprint(out, "\r\x1b[K")
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
	_, _ = fmt.Fprint(out, "\r\x1b[K")
	return "", lastErr
}

// offerFPMigration drives `ccp migrate --to fileprovider` (the requestMigration
// shape) once the stack is green; non-TTY prints the command instead of
// prompting.
func offerFPMigration(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if !isTTY() {
		note(out, "Migrate accounts with: ccp migrate --to fileprovider")
		return nil
	}
	migrate := true
	switch err := huh.NewConfirm().
		Title("Migrate your accounts to the File Provider overlay now?").
		Value(&migrate).
		WithTheme(ccpTheme()).
		Run(); {
	case errors.Is(err, huh.ErrUserAborted):
		note(out, "Later: ccp migrate --to fileprovider")
		return nil
	case err != nil:
		return fmt.Errorf("confirm migration: %w", err)
	case !migrate:
		note(out, "Later: ccp migrate --to fileprovider")
		return nil
	}
	return withManager(func(m *pool.Manager) error {
		if ok, err := m.Initialized(); err != nil {
			return err
		} else if !ok {
			note(out, "Pool not set up yet — run `ccp add`, then `ccp migrate --to fileprovider`.")
			return nil
		}
		resp, err := requestMigration(m, "fileprovider", 0, false)
		if err != nil {
			return err
		}
		if len(resp.Migrations) == 0 {
			if resp.Error != "" {
				return errors.New(resp.Error)
			}
			note(out, "No accounts to migrate; fileprovider is now the default for new accounts.")
			return nil
		}
		return renderMigrations(cmd, resp, "fileprovider", false)
	})
}
