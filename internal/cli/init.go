package cli

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func newInitCmd() *cobra.Command {
	var noService bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Set up the pool and start the daemon",
		Long: `init prepares ~/.cc-pool with its state db and account dirs, records the
overlay provider, and starts the background daemon. It never touches ~/.claude
or any credential. Accounts, including your main subscription, join via ` + "`ccp add`" + `,
each with its own ` + "`claude auth login`" + `. Running init is optional; ` + "`ccp add`" + ` does the
same setup automatically.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				out := cmd.OutOrStdout()
				res, err := m.Init()
				if err != nil {
					return err
				}
				if res.Already {
					success(out, "cc-pool is already set up.")
				} else {
					success(out, "Set up cc-pool.")
				}

				reportOverlayChoice(cmd, res)

				if !noService {
					ensureDaemon(cmd, false)
				}

				step(out, "\nNext, run `ccp add` to pool your subscriptions, including your main one.")
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&noService, "no-service", false, "do not start the daemon now; `ccp add` will start it")
	return cmd
}

// reportOverlayChoice warns on a symlink fallback only where fuse was expected; where fuse can't be hosted, symlinks are the default, so it only notes.
func reportOverlayChoice(cmd *cobra.Command, res *pool.InitResult) {
	switch {
	case res.OverlayFallbackReason != "" && pool.CanHostFuse():
		warn(cmd.ErrOrStderr(), "fuse overlay unavailable (%s); using symlinks", res.OverlayFallbackReason)
	case res.OverlayKind == fkoverlay.BackendSymlink && !pool.CanHostFuse():
		note(cmd.OutOrStdout(), "For a live-mirror overlay, run `ccp fuse enable`.")
	}
}
