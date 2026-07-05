package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpAvailable is a test seam over fkoverlay.FileProviderAvailable, whose
// pluginkit query cannot be scripted in tests.
var fpAvailable = fkoverlay.FileProviderAvailable

func newMigrateCmd() *cobra.Command {
	var account int
	var to string
	var force bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Convert accounts to a different overlay provider (symlink ⇄ fuse ⇄ fileprovider)",
		Long: `migrate converts existing pool accounts to a different overlay provider —
by default fuse, the live mirror preferred when fuse-t is installed. Accounts
created before fuse-t was set up stay on symlinks until migrated. The
fileprovider target hosts an account as an OS-managed File Provider domain
instead; it needs the CCPoolStatus companion app installed with its File
Provider extension enabled.

The conversion runs inside the daemon, which owns the gates it needs (select
reservations, poll claims); the mounts themselves live in the shared,
launchd-managed fusekit-holder that cc-pool drives over RPC, so daemon restarts
and upgrades never disturb them. An
account's private files (.claude.json identity, backups, …) move into its
private backing dir, the old overlay comes down, the mirror mounts over the
account dir, and the row records the new provider only once the identity is
verified through the mount. Accounts with live sessions or in-flight selects
are skipped and reported — re-run the command as they free up. A failed mount
rolls back to a working symlink overlay; nothing is left half-converted.

The mount holder's first fuse mount may pop a one-time macOS permission
prompt for cc-pool; if it is blocked, ccp status and ccp doctor name the
exact Settings pane to grant — then re-run. New accounts follow the last
migrated-to provider.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if to != "fuse" && to != "symlink" && to != "fileprovider" {
					return fmt.Errorf("unknown overlay kind %q (want fuse, symlink, or fileprovider)", to)
				}
				if to == "fuse" && !pool.CanHostFuse() {
					return errors.New("fuse is not available on this machine; run `ccp fuse enable` to install the fusekit-holder cask")
				}
				if to == "fileprovider" && !fpAvailable(m.OverlaySpec()) {
					return fmt.Errorf("fileprovider is not available: the %s extension is not installed and enabled — run `ccp fp onboard` to install %s, enable the extension, and migrate in one step", pool.FPExtensionBundleID, pool.WidgetAppPath())
				}
				resp, err := requestMigration(m, to, account, force)
				if err != nil {
					return err
				}
				if len(resp.Migrations) == 0 {
					if resp.Error != "" {
						return errors.New(resp.Error)
					}
					return errors.New("daemon returned no migration results")
				}
				return renderMigrations(cmd, resp, to, account > 0)
			})
		},
	}
	cmd.Flags().IntVar(&account, "account", 0, "convert only this account id")
	cmd.Flags().StringVar(&to, "to", "fuse", "target overlay kind: fuse, symlink, or fileprovider")
	cmd.Flags().BoolVar(&force, "force", false, "migrate despite live sessions (idle ones may briefly error mid-flip; launching ones still refuse)")
	return cmd
}

// requestMigration asks the daemon (which owns the conversion gates) to migrate;
// account==0 means every account. No local fuse-capability check: the daemon
// hosts the mounts, so a still-pure, just-reinstalled CLI can drive a fuse daemon.
func requestMigration(m *pool.Manager, to string, account int, force bool) (*daemon.Response, error) {
	cl, err := requireDaemon(m, "migration runs inside the daemon (it owns the conversion gates)")
	if err != nil {
		return nil, err
	}
	var acct *int
	if account > 0 {
		acct = &account
	}
	resp, err := cl.Migrate(acct, to, force)
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return resp, nil
}

func renderMigrations(cmd *cobra.Command, resp *daemon.Response, toWord string, explicit bool) error {
	out := cmd.OutOrStdout()
	var done, already, busy, failed int
	for _, r := range resp.Migrations {
		name := fmt.Sprintf("acct-%02d (%s)", r.ID, accountName(r.Label))
		switch r.Outcome {
		case daemon.MigrationDone:
			done++
			success(out, "%s %s → %s", name, r.From, r.To)
		case daemon.MigrationAlready:
			already++
			note(out, "%s already %s", name, r.To)
		case daemon.MigrationBusy:
			busy++
			step(out, "%s skipped: %s", name, r.Detail)
		case daemon.MigrationFailed:
			failed++
			step(out, "%s %s: %s", badStyle.Render("✗"), name, r.Detail)
		}
	}
	if busy > 0 {
		step(out, "Migrated %d of %d; %d busy — re-run `ccp migrate` when their sessions end.", done, len(resp.Migrations), busy)
	} else if done > 0 {
		step(out, "Migrated %d account(s).", done)
	}
	if done > 0 {
		note(out, "New accounts will use the %s overlay.", toWord)
	}
	if resp.Error != "" {
		// Op-level failure (e.g. recording the new-account default); the outcomes above stand.
		return errors.New(resp.Error)
	}
	if failed > 0 {
		return fmt.Errorf("%d account(s) failed to migrate", failed)
	}
	if explicit && done == 0 && already == 0 {
		return errors.New("the requested account did not migrate (busy); re-run when its sessions end")
	}
	return nil
}
