package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
)

func newCredCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cred",
		Short: "Manage account credential storage",
	}
	cmd.AddCommand(newCredMoveCmd())
	return cmd
}

func newCredMoveCmd() *cobra.Command {
	var account int
	var to string
	cmd := &cobra.Command{
		Use:   "move",
		Short: "Move account credentials between the macOS Keychain and file storage",
		Long: `move relocates account credentials between the macOS Keychain and claude's
plaintext .credentials.json inside each account's config dir. It is a MOVE,
never a copy: Claude refresh tokens are single-use, so two live copies diverge
at the next refresh until one signs itself out.

The move runs inside the daemon, where it cannot race a token refresh or a
launching session — and because the daemon runs in your GUI login session, it
can reach the Keychain even when this shell (over SSH, for example) cannot.
Accounts with live sessions are skipped and reported — re-run after they end.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if to != creds.SourceKeychain.String() && to != creds.SourceFile.String() {
					return fmt.Errorf("unknown credential backend %q (want keychain or file)", to)
				}
				resp, err := requestCredMove(m, to, account)
				if err != nil {
					return err
				}
				if len(resp.Migrations) == 0 {
					if resp.Error != "" {
						return errors.New(resp.Error)
					}
					return errors.New("daemon returned no credential move results")
				}
				return renderCredMoves(cmd, resp, account > 0)
			})
		},
	}
	cmd.Flags().IntVar(&account, "account", 0, "move only this account id")
	cmd.Flags().StringVar(&to, "to", "", "target credential backend: keychain or file")
	_ = cmd.MarkFlagRequired("to") // errors only if the flag above is missing
	return cmd
}

// requestCredMove asks the daemon (which gates moves against its select
// reservations, poll claims, and live sessions) to move credentials;
// account==0 means every account.
func requestCredMove(m *pool.Manager, to string, account int) (*daemon.Response, error) {
	cl, err := requireDaemon(m, "credential moves run inside the daemon (it gates them against token refreshes and launching sessions)")
	if err != nil {
		return nil, err
	}
	var acct *int
	if account > 0 {
		acct = &account
	}
	resp, err := cl.CredMove(acct, to)
	if err != nil {
		return nil, fmt.Errorf("cred move: %w", err)
	}
	return resp, nil
}

func renderCredMoves(cmd *cobra.Command, resp *daemon.Response, explicit bool) error {
	out := cmd.OutOrStdout()
	var done, already, busy, failed int
	for _, r := range resp.Migrations {
		name := fmt.Sprintf("acct-%02d (%s)", r.ID, accountName(r.Label))
		// Done/already may carry a detail ("cleaned a stray file copy").
		suffix := ""
		if r.Detail != "" {
			suffix = " — " + r.Detail
		}
		switch r.Outcome {
		case daemon.MigrationDone:
			done++
			success(out, "%s %s → %s%s", name, r.From, r.To, suffix)
		case daemon.MigrationAlready:
			already++
			note(out, "%s already %s%s", name, r.To, suffix)
		case daemon.MigrationBusy:
			busy++
			step(out, "%s skipped: %s", name, r.Detail)
		case daemon.MigrationFailed:
			failed++
			step(out, "%s %s: %s", badStyle.Render("✗"), name, r.Detail)
		}
	}
	if busy > 0 {
		step(out, "Moved %d of %d; %d busy — re-run `ccp cred move` when their sessions end.", done, len(resp.Migrations), busy)
	} else if done > 0 {
		step(out, "Moved %d credential(s).", done)
	}
	if resp.Error != "" {
		// Op-level failure; the outcomes above stand.
		return errors.New(resp.Error)
	}
	if failed > 0 {
		return fmt.Errorf("%d account(s) failed to move credentials", failed)
	}
	if explicit && done == 0 && already == 0 {
		return errors.New("the requested account's credential did not move (busy); re-run when its sessions end")
	}
	return nil
}
