package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/version"
)

func newMigrateCmd() *cobra.Command {
	var account int
	var to string
	var force bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Convert accounts to a different overlay provider (symlink ⇄ fuse)",
		Long: `migrate converts existing pool accounts to a different overlay provider —
by default fuse, the live mirror preferred when fuse-t is installed. Accounts
created before fuse-t was set up stay on symlinks until migrated.

The conversion runs inside the daemon, which owns the gates it needs (select
reservations, poll claims); the mounts themselves live in a detached cc-pool
mount-holder process, so daemon restarts and upgrades never disturb them. An
account's private files (.claude.json identity, backups, …) move into its
private backing dir, the old overlay comes down, the mirror mounts over the
account dir, and the row records the new provider only once the identity is
verified through the mount. Accounts with live sessions or in-flight selects
are skipped and reported — re-run the command as they free up. A failed mount
rolls back to a working symlink overlay; nothing is left half-converted.

The mount holder's first fuse mount may pop macOS's one-time "Network
Volumes" prompt for cc-pool; grant it (System Settings ▸ Privacy & Security)
and re-run. New accounts follow the last migrated-to provider.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				kind := overlay.Kind(to)
				if kind != overlay.KindFuse && kind != overlay.KindSymlink {
					return fmt.Errorf("unknown overlay kind %q (want fuse or symlink)", to)
				}
				if kind == overlay.KindFuse && !overlay.FuseBuilt() {
					return errors.New("this cc-pool build has no fuse support; install fuse-t (`brew install macos-fuse-t/cask/fuse-t`), then `brew reinstall cc-pool`")
				}

				// The daemon performs the conversion; require one at exactly
				// this version so the op and its gates exist on the other
				// end — a stale-version daemon cannot be trusted to drive
				// them. NOT auto-restarted, purely to keep this command
				// read-only on the service; the restart itself is mount-safe
				// (the detached holder keeps serving the mirrors across it)
				// and the skew error below recommends it plainly.
				cl := daemon.NewClient()
				health, err := cl.Health()
				switch {
				case errors.Is(err, daemon.ErrDaemonUnavailable):
					return fmt.Errorf("migration runs inside the daemon (it owns the conversion gates), which is not running; start it with `ccp service install` and re-run: %w", err)
				case err != nil:
					// A daemon that accepted the dial but failed the probe is
					// hung, not absent. Surface that as-is — a restart would
					// be mount-safe, but prescribing one for a hang would
					// mask the real failure.
					return fmt.Errorf("daemon health check: %w", err)
				}
				if health.Version != version.String() {
					return fmt.Errorf("the daemon is %s but this ccp is %s; restart it (`brew services restart cc-pool` or `ccp service install`) and re-run — mounts and live sessions are unaffected", health.Version, version.String())
				}

				var acct *int
				if account > 0 {
					acct = &account
				}
				resp, err := cl.Migrate(acct, string(kind), force)
				if err != nil {
					return fmt.Errorf("migrate: %w", err)
				}
				if len(resp.Migrations) == 0 {
					if resp.Error != "" {
						return errors.New(resp.Error)
					}
					return errors.New("daemon returned no migration results")
				}
				return renderMigrations(cmd, resp, kind, account > 0)
			})
		},
	}
	cmd.Flags().IntVar(&account, "account", 0, "convert only this account id")
	cmd.Flags().StringVar(&to, "to", string(overlay.KindFuse), "target overlay kind: fuse or symlink")
	cmd.Flags().BoolVar(&force, "force", false, "migrate despite live sessions (idle ones may briefly error mid-flip; launching ones still refuse)")
	return cmd
}

// renderMigrations prints per-account outcomes and the summary, returning an
// error (nonzero exit) when anything failed — or, for an explicit --account,
// when that account did not convert.
func renderMigrations(cmd *cobra.Command, resp *daemon.Response, kind overlay.Kind, explicit bool) error {
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
		note(out, "New accounts will use the %s overlay.", kind)
	}
	if resp.Error != "" {
		// Per-account outcomes above are truthful; this is the op-level
		// failure (e.g. recording the new-account default).
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
