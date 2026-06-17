package cli

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/mountd"
)

// newMountHolderCmd is the hidden entry point for the detached mount-holder
// process spawned by pool.SpawnHolder.
func newMountHolderCmd() *cobra.Command {
	var socket string
	cmd := &cobra.Command{
		Use:    "mount-holder",
		Short:  "Run the detached fuse mount holder",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// host is nil in a non-fuse build; Server.Run refuses loudly.
			host, _ := overlay.InProcessFuse()
			// Version is cc-pool's APP version (version.String()), never
			// fusekit's: the daemon compares the holder's wire Version to its own
			// version.String() and would replace-loop the holder forever if
			// fusekit's module version leaked onto the wire.
			s := &mountd.Server{Socket: socket, Host: host, Probe: overlay.HostProbe, Version: version.String()}
			return s.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&socket, "socket", pool.MountsSocketPath(), "unix socket path to serve")
	return cmd
}
