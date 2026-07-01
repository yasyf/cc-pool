package cli

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
)

func newDaemonCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "daemon",
		Short:  "Run the background daemon used by the LaunchAgent",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return daemon.Run(cmd.Context())
		},
	}
}
