package cli

import (
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
)

func newInitCmd() *cobra.Command {
	var noService bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Set up the pool and start the daemon",
		Long: `init prepares ~/.cc-pool with its state db and private account dirs and
starts the background daemon. It never touches ~/.claude
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

				if !noService {
					ensureDaemon(cmd)
				}

				step(out, "\nNext, run `ccp add` to pool your subscriptions, including your main one.")
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&noService, "no-service", false, "do not start the daemon now; `ccp add` will start it")
	return cmd
}
