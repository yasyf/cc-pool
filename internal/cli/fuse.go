package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/fuset"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/service"
)

// newFuseCmd groups the fuse-t live-mirror subcommands. cc-pool is pure-Go and
// drives the shared fusekit-holder cask over RPC, so there is no build tag here —
// `enable` installs the casks the holder needs and the daemon does the rest.
func newFuseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fuse",
		Short: "Manage the fuse-t live-mirror overlay",
	}
	cmd.AddCommand(newFuseEnableCmd())
	return cmd
}

func newFuseEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Install the fuse-t and fusekit-holder casks and switch the pool to the live-mirror overlay",
		Long: `enable sets up the fuse-t live-mirror overlay end to end: it installs the
fuse-t cask (the libfuse-t the holder mounts with) and the signed fusekit-holder
cask (the shared, multi-tenant mount host cc-pool drives over RPC), ensures the
daemon is running, then migrates your accounts onto the live mirror and records
fuse as the default for new accounts.

A Homebrew formula cannot depend on a cask, and both ship only as casks, so this
is the one-step setup the formula itself cannot express. It is idempotent: steps
already satisfied are skipped.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runFuseEnable(cmd) },
	}
}

// runFuseEnable is the turnkey live-mirror setup. cc-pool is pure-Go and never
// hosts mounts itself, so this installs the casks the shared holder needs (fuse-t
// + fusekit-holder), ensures the daemon is up, and asks it — the real
// fuse-capability authority — to migrate. Each step fails loud; nothing is left
// half-done that a re-run can't converge.
func runFuseEnable(cmd *cobra.Command) error {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()

	if fuset.Installed() {
		step(out, "fuse-t already installed.")
	} else {
		step(out, "Installing fuse-t…")
		if err := fuset.Install(out, errOut); err != nil {
			return fmt.Errorf("install fuse-t: %w", err)
		}
		if !fuset.Installed() {
			return fmt.Errorf("installed the fuse-t cask but %s is still missing; check the brew output above", fuset.Dylib)
		}
	}

	// The signed, multi-tenant fusekit-holder cask hosts every consumer's mounts;
	// cc-pool drives it over RPC and needs no fuse build of its own.
	if _, err := os.Stat(mountd.HolderExe); err == nil {
		step(out, "fusekit-holder already installed.")
	} else {
		step(out, "Installing the fusekit-holder cask…")
		if err := service.InstallCask(mountd.HolderCask, out, errOut); err != nil {
			return fmt.Errorf("install fusekit-holder: %w", err)
		}
		if _, err := os.Stat(mountd.HolderExe); err != nil {
			return fmt.Errorf("installed the fusekit-holder cask but %s is still missing; check the brew output above", mountd.HolderExe)
		}
	}

	// Ensure the daemon is running; it binds the content bridge and drives the
	// cask holder over RPC, migrating accounts onto the live mirror on request.
	ensureDaemon(cmd, false)

	// Flip the default to fuse and migrate existing accounts. The conversion runs
	// in the daemon, which drives the cask holder over RPC; this CLI only kicks it off.
	if err := withManager(func(m *pool.Manager) error {
		if ok, err := m.Initialized(); err != nil {
			return err
		} else if !ok {
			note(out, "Pool not set up yet — run `ccp add`; new accounts will use the live mirror.")
			return nil
		}
		resp, err := requestMigration(m, "fuse", 0, false)
		if err != nil {
			return err
		}
		if len(resp.Migrations) == 0 {
			if resp.Error != "" {
				return errors.New(resp.Error)
			}
			note(out, "No accounts to migrate; fuse is now the default for new accounts.")
			return nil
		}
		return renderMigrations(cmd, resp, "fuse", false)
	}); err != nil {
		return err
	}

	success(out, "Live-mirror overlay enabled.")
	return nil
}
