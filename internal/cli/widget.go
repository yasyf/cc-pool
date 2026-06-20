package cli

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

// Widget app cask coordinates. The cask lives in the shared external tap
// yasyf/homebrew-tap, so `brew install --cask yasyf/tap/cc-pool-status` would
// auto-tap; we still tap explicitly (with the URL) up front so the install is
// a single non-tapping step and works regardless of how cc-pool was installed.
const (
	widgetCask    = "cc-pool-status"
	widgetTap     = "yasyf/homebrew-tap"
	widgetTapURL  = "https://github.com/yasyf/homebrew-tap"
	widgetAppName = "CCPoolStatus"
	widgetAppPath = "/Applications/CCPoolStatus.app" // the cask's default appdir
)

func newWidgetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "widget",
		Short: "Install the Notification Center status widget",
		Long: `Installs the CCPoolStatus app via Homebrew (cask ` + widgetTap + `/` + widgetCask + `),
launches it so macOS discovers the widget, and prints how to enable it.
Safe to re-run: an existing install is upgraded in place.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runWidget(cmd) },
	}
}

func runWidget(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if _, err := exec.LookPath("brew"); err != nil {
		return fmt.Errorf("the widget installs via Homebrew, which isn't on PATH (to build from source instead, see widget/README.md): %w", err)
	}
	if err := ensureWidgetTap(cmd); err != nil {
		return err
	}
	step(out, "Installing the widget app…")
	if err := brewInstallWidget(cmd); err != nil {
		return err
	}
	// Launching once registers the embedded widget extension with macOS so it
	// shows up in the widget gallery. -g keeps the agent app in the background.
	// By path first: LaunchServices hasn't indexed a freshly installed app yet,
	// so the by-name lookup fails on exactly the first (registering) launch.
	step(out, "Launching it so macOS discovers the widget…")
	if err := runStreamed(cmd, "open", "-g", widgetAppPath); err != nil {
		if err := runStreamed(cmd, "open", "-g", "-a", widgetAppName); err != nil {
			return fmt.Errorf("launch %s (is the cask appdir customized?): %w", widgetAppName, err)
		}
	}
	success(out, "Widget installed.")
	_, _ = fmt.Fprint(out, widgetInstructions())
	return nil
}

// ensureWidgetTap taps the cc-pool tap if it isn't already present, so the
// cask resolves even when cc-pool itself was installed some other way.
func ensureWidgetTap(cmd *cobra.Command) error {
	outBytes, err := exec.Command("brew", "tap").Output()
	if err != nil {
		return fmt.Errorf("list brew taps: %w", err)
	}
	for _, line := range strings.Split(string(outBytes), "\n") {
		if strings.TrimSpace(line) == widgetTap {
			return nil
		}
	}
	step(cmd.OutOrStdout(), "Adding the %s tap…", widgetTap)
	if err := runStreamed(cmd, "brew", "tap", widgetTap, widgetTapURL); err != nil {
		return fmt.Errorf("brew tap %s: %w", widgetTap, err)
	}
	return nil
}

// brewInstallWidget installs the cask, or upgrades it when already present.
func brewInstallWidget(cmd *cobra.Command) error {
	installed := exec.Command("brew", "list", "--cask", widgetCask).Run() == nil
	if installed {
		if err := brewCask(cmd, "upgrade", "--cask", widgetCask); err != nil {
			return fmt.Errorf("brew upgrade --cask %s: %w", widgetCask, err)
		}
		return nil
	}
	if err := brewCask(cmd, "install", "--cask", widgetTap+"/"+widgetCask); err != nil {
		return fmt.Errorf("brew install --cask %s: %w", widgetCask, err)
	}
	return nil
}

// brewCask runs a brew cask install/upgrade unattended. The widget app is
// Developer ID signed, notarized, and stapled, so a normal quarantined install
// validates — no quarantine handling needed.
func brewCask(cmd *cobra.Command, args ...string) error {
	// -y / --no-ask disables Homebrew's default ask-mode confirmation so the
	// install runs unattended. It follows the subcommand: `brew install -y --cask …`.
	//nolint:gosec // G204: args are this CLI's own fixed `brew install --cask` argv, not user input
	c := exec.Command("brew", append([]string{args[0], "-y"}, args[1:]...)...)
	c.Stdout, c.Stderr = cmd.OutOrStdout(), cmd.ErrOrStderr()
	return c.Run()
}

// runStreamed runs a command with its output streamed to the user.
func runStreamed(cmd *cobra.Command, name string, args ...string) error {
	//nolint:gosec // G204: name/args are this CLI's own fixed subprocess invocation, not user input
	c := exec.Command(name, args...)
	c.Stdout, c.Stderr = cmd.OutOrStdout(), cmd.ErrOrStderr()
	return c.Run()
}

// widgetInstructions is the post-install walkthrough for enabling the widget.
func widgetInstructions() string {
	return `
To add the widget:
  1. Open Notification Center — click the clock in the menu bar.
  2. Scroll to the bottom and click "Edit Widgets".
  3. Search "cc-pool" and add the small, medium, or large widget.
     (Desktop widgets work too: right-click the desktop → Edit Widgets.)

The widget refreshes every ~3 minutes while CCPoolStatus is running. To keep
that across logins: System Settings → General → Login Items → add CCPoolStatus.

Not showing up in the gallery? Run:
  killall NotificationCenter && open -ga ` + widgetAppName + `
`
}
