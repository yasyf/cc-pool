package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit"
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

The mount holder's first fuse mount may pop a one-time macOS permission
prompt for cc-pool; if it is blocked, ccp status and ccp doctor name the
exact Settings pane to grant — then re-run. New accounts follow the last
migrated-to provider.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if to != "fuse" && to != "symlink" {
					return fmt.Errorf("unknown overlay kind %q (want fuse or symlink)", to)
				}
				if to == "fuse" && !fusekit.Built() {
					return errors.New("this cc-pool build has no fuse support; run `ccp fuse enable` to install fuse-t and switch to the live-mirror build")
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
	cmd.Flags().StringVar(&to, "to", "fuse", "target overlay kind: fuse or symlink")
	cmd.Flags().BoolVar(&force, "force", false, "migrate despite live sessions (idle ones may briefly error mid-flip; launching ones still refuse)")
	return cmd
}

// requestMigration runs the client side of an overlay migration, shared by
// `ccp migrate` and `ccp fuse enable`: require an initialized pool and a healthy
// daemon at exactly this version (the daemon owns the conversion gates, so a
// stale-version one cannot be trusted to drive them), then ask it to migrate.
// account==0 means every account. It deliberately does NOT check this process's
// own fuse capability: the daemon — not this CLI — hosts the mounts, so
// `ccp fuse enable` can drive a fuse daemon from a still-pure CLI whose binary
// was just reinstalled. The daemon is NOT auto-restarted here; the version-skew
// error recommends a restart plainly, and `ccp fuse enable` forces its own.
func requestMigration(m *pool.Manager, to string, account int, force bool) (*daemon.Response, error) {
	if err := requireInit(m); err != nil {
		return nil, err
	}
	cl := daemon.NewClient()
	health, err := cl.Health()
	switch {
	case errors.Is(err, daemon.ErrDaemonUnavailable):
		return nil, fmt.Errorf("migration runs inside the daemon (it owns the conversion gates), which is not running; start it with `ccp service install` and re-run: %w", err)
	case err != nil:
		// A daemon that accepted the dial but failed the probe is hung, not
		// absent. Surface that as-is — a restart would be mount-safe, but
		// prescribing one for a hang would mask the real failure.
		return nil, fmt.Errorf("daemon health check: %w", err)
	}
	if health.Version != version.String() {
		return nil, fmt.Errorf("the daemon is %s but this ccp is %s; restart it (`brew services restart cc-pool` or `ccp service install`) and re-run — mounts and live sessions are unaffected", health.Version, version.String())
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

// renderMigrations prints per-account outcomes and the summary, returning an
// error (nonzero exit) when anything failed — or, for an explicit --account,
// when that account did not convert.
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
