package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/fuset"
)

// newFuseCmd groups the fuse-t live-mirror subcommands. It is registered in
// every build variant (a pure build must be able to upgrade itself), so it
// uses no fuse build tag and relies on runtime detection instead.
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
		Short: "Install fuse-t and switch the pool to the live-mirror overlay",
		Long: `enable sets up the fuse-t live-mirror overlay end to end: it installs the
fuse-t cask (Homebrew), reinstalls cc-pool on its ` + "`-tags fuse`" + ` build when this is
the pure build, restarts the daemon on that build, then migrates your accounts
onto the live mirror and records fuse as the default for new accounts.

A Homebrew formula cannot depend on a cask, and fuse-t ships only as a cask, so
this is the one-step setup the formula itself cannot express. It is idempotent:
steps already satisfied are skipped.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runFuseEnable(cmd) },
	}
}

// runFuseEnable is the turnkey live-mirror setup. The running process is the
// pure CLI even after the reinstall below (a process cannot swap its own image),
// so it drives Homebrew, restarts the daemon onto the freshly-installed fuse
// binary, and then asks that daemon — the real fuse-capability authority — to
// migrate. Each step fails loud; nothing is left half-done that a re-run can't
// converge.
func runFuseEnable(cmd *cobra.Command) error {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()

	// A pure build can only swap itself to the fuse build through Homebrew. If it
	// is not a brew install, there is no automatic path — fail before touching
	// anything rather than installing fuse-t and leaving the wrong binary behind.
	reinstallNeeded := !fusekit.Built()
	if reinstallNeeded && !ccpAgent().IsBrewManaged() {
		exe, _ := os.Executable()
		return fmt.Errorf("cc-pool is not a Homebrew install (running %s), so it cannot swap itself to the fuse build; install fuse-t with `brew install --cask %s` and rebuild with `-tags fuse`, or reinstall via `brew install yasyf/tap/cc-pool`", exe, fuset.Cask)
	}

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

	// Swap to the fuse build if this is the pure one. With fuse-t now present,
	// the formula's install logic selects the -tags fuse variant.
	if reinstallNeeded {
		step(out, "Reinstalling cc-pool on the fuse build…")
		if err := ccpAgent().BrewReinstall(out, errOut); err != nil {
			return fmt.Errorf("brew reinstall cc-pool: %w", err)
		}
	} else {
		step(out, "cc-pool already on the fuse build.")
	}

	// Get the daemon running the fuse build. After a reinstall the on-disk binary
	// changed but the live daemon's version string did not, so force the restart —
	// ensureDaemon's same-version short-circuit would otherwise leave the stale
	// pure image running. Without a reinstall a plain ensure suffices (the holder
	// reprobes fuse-t when it spawns).
	ensureDaemon(cmd, reinstallNeeded)

	// Flip the default to fuse and migrate existing accounts. The conversion runs
	// in the daemon (now the fuse build); this still-pure CLI only drives it.
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
