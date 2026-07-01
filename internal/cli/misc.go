package cli

import (
	"fmt"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List accounts with their ids, paths, and Keychain items",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				accts, err := m.Store.ListAccounts()
				if err != nil {
					return err
				}
				if len(accts) == 0 {
					step(cmd.ErrOrStderr(), "No accounts yet. Run `ccp add` to add one.")
					return nil
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "ACCT\tLABEL\tOVERLAY\tCONFIG DIR\tKEYCHAIN SERVICE")
				for _, a := range accts {
					_, _ = fmt.Fprintf(tw, "acct-%02d\t%s\t%s\t%s\t%s\n",
						a.ID, accountName(a.Label), a.OverlayKind, a.ConfigDir, a.KeychainService)
				}
				return tw.Flush()
			})
		},
	}
}

func newRemoveCmd() *cobra.Command {
	var keepCred bool
	cmd := &cobra.Command{
		Use:   "remove <account-id>",
		Short: "Remove an account from the pool",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseAccountRef(args[0])
			if err != nil {
				return err
			}
			return withManager(func(m *pool.Manager) error {
				if err := m.Remove(id, !keepCred); err != nil {
					return err
				}
				success(cmd.OutOrStdout(), "Removed acct-%02d.", id)
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&keepCred, "keep-credential", false, "do not delete the account's Keychain item")
	return cmd
}

func newEnvCmd() *cobra.Command {
	var account int
	cmd := &cobra.Command{
		Use:   "env",
		Short: "Print shell export lines to launch an account",
		Long: `env prints the environment needed to launch a specific account:

    eval "$(ccp env --account 1)"; claude`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				var a store.Account
				var err error
				if cmd.Flags().Changed("account") {
					a, err = m.Store.GetAccount(account)
				} else {
					var sr *pool.SelectResult
					sr, err = m.Select(cmd.Context(), pool.SelectOptions{Live: true})
					if err == nil {
						a = sr.Best
						if sr.ExhaustedFallback {
							// stderr, so an eval'd stdout capture is unaffected.
							warnExhaustedFallback(cmd, accountName(a.Label), sr.ExtraEnabled, sr.Result.ExhaustedUntil)
						}
					}
				}
				if err != nil {
					return err
				}
				mergeLaunchSettings(cmd, m, a)
				out := cmd.OutOrStdout()
				_, _ = fmt.Fprintf(out, "export CLAUDE_CONFIG_DIR=%s\n", shellQuote(a.ConfigDir))
				_, _ = fmt.Fprintf(out, "export CLAUDE_CODE_PLUGIN_CACHE_DIR=%s\n", shellQuote(filepath.Join(pool.ClaudeDir(), "plugins")))
				return nil
			})
		},
	}
	cmd.Flags().IntVar(&account, "account", 0, "account id (defaults to the best account)")
	return cmd
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
