package cli

import (
	"errors"
	"fmt"
	"io"

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
				resp, err := requestMigration(cmd, m, to, account, force)
				if err != nil {
					return err
				}
				if len(resp.Migrations) == 0 {
					if resp.Error != "" {
						return errors.New(resp.Error)
					}
					return errors.New("daemon returned no migration results")
				}
				return renderMigrationSummary(cmd, resp, to, account > 0)
			})
		},
	}
	cmd.Flags().IntVar(&account, "account", 0, "convert only this account id")
	cmd.Flags().StringVar(&to, "to", "fuse", "target overlay kind: fuse, symlink, or fileprovider")
	cmd.Flags().BoolVar(&force, "force", false, "migrate despite live sessions (idle ones may briefly error mid-flip; launching ones still refuse)")
	return cmd
}

// requestMigration drives a migration through the daemon (which owns the
// conversion gates), streaming each account's result as its RPC lands and
// returning the aggregate response for the caller's summary and empty-pool
// handling. A fleet request (account==0) fans out one Migrate RPC per account so
// each gets the full per-account budget — a slow domain materialization cannot
// starve later accounts of their window; account>0 issues a single RPC. No local
// fuse-capability check: the daemon hosts the mounts, so a still-pure,
// just-reinstalled CLI can drive a fuse daemon.
func requestMigration(cmd *cobra.Command, m *pool.Manager, to string, account int, force bool) (*daemon.Response, error) {
	cl, err := requireDaemon(m, "migration runs inside the daemon (it owns the conversion gates)")
	if err != nil {
		return nil, err
	}
	defer func() { _ = cl.Close() }()
	if account > 0 {
		resp, err := cl.Migrate(&account, to, force)
		if err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
		out := cmd.OutOrStdout()
		for _, r := range resp.Migrations {
			renderMigrationResult(out, r)
		}
		return resp, nil
	}
	return fleetMigrate(cmd, m, cl, to, force)
}

// fleetMigrate issues one Migrate RPC per account so each gets the full
// per-account budget — the readiness gate inside each conversion is the settle
// and the daemon loop stays sequential — streaming each result as it lands and
// folding them into one aggregate response. An empty pool falls back to a single
// all-accounts RPC so a passing gate still records the new-account default. A
// machine-wide gate failure (an op-level error with no results) fails identically
// for every account, so it surfaces once and stops the fan-out.
func fleetMigrate(cmd *cobra.Command, m *pool.Manager, cl *daemon.Client, to string, force bool) (*daemon.Response, error) {
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	if len(accts) == 0 {
		return cl.Migrate(nil, to, force)
	}
	out := cmd.OutOrStdout()
	agg := &daemon.Response{OK: true}
	for _, a := range accts {
		id := a.ID
		resp, err := cl.Migrate(&id, to, force)
		if err != nil {
			return nil, fmt.Errorf("migrate acct-%02d: %w", id, err)
		}
		if !resp.OK && len(resp.Migrations) == 0 {
			// Machine-wide gate failure — identical for every account, so surface
			// it once and stop, keeping already-collected outcomes for the summary.
			agg.OK = false
			agg.Error = resp.Error
			return agg, nil
		}
		for _, r := range resp.Migrations {
			renderMigrationResult(out, r)
			agg.Migrations = append(agg.Migrations, r)
		}
		if resp.Error != "" {
			agg.OK = false
			agg.Error = resp.Error
		}
	}
	return agg, nil
}

// renderMigrationResult prints one account's outcome. Split from the summary so
// the fleet fan-out can stream each result the moment its RPC lands.
func renderMigrationResult(out io.Writer, r daemon.MigrationResult) {
	name := fmt.Sprintf("acct-%02d (%s)", r.ID, accountName(r.Label))
	switch r.Outcome {
	case daemon.MigrationDone:
		success(out, "%s %s → %s", name, r.From, r.To)
	case daemon.MigrationAlready:
		note(out, "%s already %s", name, r.To)
	case daemon.MigrationBusy:
		step(out, "%s skipped: %s", name, r.Detail)
	case daemon.MigrationFailed:
		step(out, "%s %s: %s", badStyle.Render("✗"), name, r.Detail)
	}
}

// renderMigrationSummary tallies the (already-printed) results into the closing
// lines and the exit-code error. resp.Error and per-account failures propagate;
// an explicit single-account request that neither converted nor was already at
// the target exits nonzero.
func renderMigrationSummary(cmd *cobra.Command, resp *daemon.Response, toWord string, explicit bool) error {
	out := cmd.OutOrStdout()
	var done, already, busy, failed int
	for _, r := range resp.Migrations {
		switch r.Outcome {
		case daemon.MigrationDone:
			done++
		case daemon.MigrationAlready:
			already++
		case daemon.MigrationBusy:
			busy++
		case daemon.MigrationFailed:
			failed++
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
