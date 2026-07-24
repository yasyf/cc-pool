package cli

import (
	"fmt"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
)

const widgetAppName = "CCPoolStatus"

var installWidgetStack = installDaemonService

func newWidgetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "widget",
		Short: "Install the Notification Center status widget",
		Long: `Requires the exact CCPoolStatus app from this cc-pool release to be installed
with ` + "`ccp package install`" + `, reconciles its services, launches it so macOS discovers
the widget, and prints how to enable it.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runWidget(cmd) },
	}
}

func runWidget(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	step(out, "Reconciling the signed widget app and services…")
	if err := installWidgetStack(cmd.Context()); err != nil {
		return fmt.Errorf("reconcile CCPoolStatus and services: %w", err)
	}
	step(out, "Launching it so macOS discovers the widget…")
	if err := launchWidgetApp(cmd); err != nil {
		return err
	}
	success(out, "Widget installed.")
	_, _ = fmt.Fprint(out, widgetInstructions())
	return nil
}

// launchWidgetApp opens the CCPoolStatus app in the background. By path first:
// LaunchServices has not indexed a fresh per-user install, so by-name may fail.
func launchWidgetApp(cmd *cobra.Command) error {
	if err := runStreamed(cmd, "open", "-g", pool.WidgetAppPath()); err != nil {
		if err := runStreamed(cmd, "open", "-g", "-a", widgetAppName); err != nil {
			return fmt.Errorf("launch %s: %w", widgetAppName, err)
		}
	}
	return nil
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
