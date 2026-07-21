package cli

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
)

func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List accounts with their ids, paths, and Keychain items",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(cmd.Context(), func(m *pool.Manager) error {
				accts, err := m.Store.ListAccounts()
				if err != nil {
					return err
				}
				if len(accts) == 0 {
					step(cmd.ErrOrStderr(), "No accounts yet. Run `ccp add` to add one.")
					return nil
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "ACCT\tLABEL\tCONFIG DIR\tKEYCHAIN SERVICE")
				for _, a := range accts {
					_, _ = fmt.Fprintf(tw, "acct-%02d\t%s\t%s\t%s\n",
						a.ID, accountName(a.Label), a.ConfigDir, a.KeychainService)
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
		Long: `remove tears down an account: its overlay, config dir, Keychain item, and rows.

With host sync enabled the removal is pool-wide: the account is tombstoned in
the shared registry before any local teardown, and peer hosts tear down their
copies on their next converge. --keep-credential preserves only this host's
Keychain item — the account is still tombstoned and removed everywhere.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseAccountRef(args[0])
			if err != nil {
				return err
			}
			if err := withManager(cmd.Context(), func(m *pool.Manager) error {
				client, err := requireDaemon(m, "account and credential removal runs inside the daemon")
				if err != nil {
					return err
				}
				defer func() { _ = client.Close() }()
				return client.RemoveAccount(cmd.Context(), id, !keepCred)
			}); err != nil {
				return err
			}
			success(cmd.OutOrStdout(), "Removed acct-%02d.", id)
			return nil
		},
	}
	cmd.Flags().BoolVar(&keepCred, "keep-credential", false, "keep this host's Keychain item (the account is still removed pool-wide when sync is enabled)")
	return cmd
}

func newEnvCmd() *cobra.Command {
	var account int
	cmd := &cobra.Command{
		Use:   "env",
		Short: "Print shell export lines to launch an account",
		Long: `env prints the environment needed to inspect a specific account:

    ccp env --account 1

Use ` + "`ccp run`" + ` to launch. env does not create a session.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(cmd.Context(), func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				if !cmd.Flags().Changed("account") {
					return errors.New("env requires --account; use `ccp run` for selection and launch")
				}
				a, err := m.Store.GetAccount(account)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				_, _ = fmt.Fprintf(out, "export CLAUDE_CONFIG_DIR=%s\n", shellQuote(pool.AccountPresentationDir(a.ID)))
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
