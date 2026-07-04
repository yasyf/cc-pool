package cli

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
)

const (
	widgetCask    = "cc-pool-status"
	widgetTap     = "yasyf/homebrew-tap"
	widgetTapURL  = "https://github.com/yasyf/homebrew-tap"
	widgetAppName = "CCPoolStatus"
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
	// By path first: LaunchServices hasn't indexed a fresh install, so by-name fails on first launch.
	step(out, "Launching it so macOS discovers the widget…")
	if err := runStreamed(cmd, "open", "-g", pool.WidgetAppPath()); err != nil {
		if err := runStreamed(cmd, "open", "-g", "-a", widgetAppName); err != nil {
			return fmt.Errorf("launch %s (is the cask appdir customized?): %w", widgetAppName, err)
		}
	}
	success(out, "Widget installed.")
	_, _ = fmt.Fprint(out, widgetInstructions())
	return nil
}

// ensureWidgetTap taps widgetTap so the cask resolves however cc-pool was installed.
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

func brewCask(cmd *cobra.Command, args ...string) error {
	// -y (--no-ask) skips Homebrew's confirmation; must follow the subcommand.
	//nolint:gosec // G204: args are this CLI's own fixed `brew install --cask` argv, not user input
	c := exec.Command("brew", append([]string{args[0], "-y"}, args[1:]...)...)
	c.Stdout, c.Stderr = cmd.OutOrStdout(), cmd.ErrOrStderr()
	return c.Run()
}

func runStreamed(cmd *cobra.Command, name string, args ...string) error {
	//nolint:gosec // G204: name/args are this CLI's own fixed subprocess invocation, not user input
	c := exec.Command(name, args...)
	c.Stdout, c.Stderr = cmd.OutOrStdout(), cmd.ErrOrStderr()
	return c.Run()
}

func widgetInstructions() string {
	return `
To add the widget:
  1. Open Notification Center — click the clock in the menu bar.
  2. Scroll to the bottom and click "Edit Widgets".
  3. Search "cc-pool" and add the small, medium, or large widget.
     (Desktop widgets work too: right-click the desktop → Edit Widgets.)

The widget refreshes every ~3 minutes while CCPoolStatus is running. On first
launch the app adds itself to Login Items, so that survives reboots; manage or
disable it under System Settings → General → Login Items (turning it off sticks).

Not showing up in the gallery? Run:
  killall NotificationCenter && open -ga ` + widgetAppName + `
`
}
