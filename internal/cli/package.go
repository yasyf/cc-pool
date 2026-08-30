package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newPackageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "package",
		Short: "Install or remove the delivered signed application",
		Args:  cobra.NoArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "install",
			Short: "Verify, install, and activate the delivered signed application",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				if err := installPackage(cmd.Context()); err != nil {
					return err
				}
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "installed: CCPoolStatus package")
				return err
			},
		},
		&cobra.Command{
			Use:   "uninstall",
			Short: "Deactivate and remove the installed signed application",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				if err := uninstallPackage(cmd.Context()); err != nil {
					return err
				}
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "uninstalled: CCPoolStatus package")
				return err
			},
		},
		&cobra.Command{
			Use:   "reset",
			Short: "Retire the installed application's agents and deployment records, keeping its installed bytes and the FuseKit catalog",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				if err := resetPackage(cmd.Context()); err != nil {
					return err
				}
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "reset: CCPoolStatus package")
				return err
			},
		},
	)
	return cmd
}
