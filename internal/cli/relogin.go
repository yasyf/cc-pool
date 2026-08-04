package cli

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
)

func newLoginCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "login <account>",
		Short: "Re-authenticate an account",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withManager(func(m *pool.Manager) error {
				return runRelogin(cmd, m, args[0])
			})
		},
	}
}

func runRelogin(cmd *cobra.Command, m *pool.Manager, ref string) error {
	id, err := parseAccountRef(ref)
	if err != nil {
		return err
	}
	account, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	cl, err := requireDaemon(m, "credential mutation runs inside the daemon")
	if err != nil {
		return err
	}
	defer func() { _ = cl.Close() }()
	out := cmd.OutOrStdout()
	note(out, "Logging in %s through the daemon-owned terminal session.", accountName(account.Label))
	loginURL := &terminalURLAction{}
	result, err := cl.AccountMutationTerminal(cmd.Context(), daemon.AccountMutationRequest{
		Kind: daemon.AccountMutationRelogin, Action: daemon.AccountMutationStartOrAttach,
		AccountID: account.ID,
	}, os.Stdin, out, loginURL.observe)
	loginURL.annotate(out)
	if err != nil {
		return err
	}
	if !result.Completed || result.State != daemon.AccountMutationCompleted {
		return errors.New("daemon did not commit the credential mutation")
	}
	success(cmd.OutOrStdout(), "%s re-logged in.", accountName(account.Label))
	return nil
}
