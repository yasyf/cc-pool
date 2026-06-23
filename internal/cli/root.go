// Package cli wires up the cobra command tree for cc-pool. The binary is
// installed as `cc-pool` with a `ccp` symlink; both dispatch here.
package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/version"
)

// NewRootCmd builds the root command and attaches all subcommands.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "cc-pool",
		Short: "Predictive multi-account load-balancing for Claude Code",
		Long: `cc-pool (ccp) pools several Claude subscriptions and launches each session
on the emptiest account:

    ccp run

Run bare ` + "`ccp`" + ` to get started. On an empty pool it walks you through adding
your subscriptions; once accounts exist it shows the status table.

Plain ` + "`claude`" + ` keeps working untouched on ~/.claude and is never part of the
pool.`,
		Version:       version.String(),
		SilenceUsage:  true,
		SilenceErrors: true,
		// Args deliberately left nil: cobra's legacyArgs already rejects
		// unknown subcommands on a root that has children (with suggestions),
		// so RunE only ever runs for bare `ccp`.
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				initialized, err := m.Initialized()
				if err != nil {
					return err
				}
				accounts := 0
				if initialized {
					accts, err := m.Store.ListAccounts()
					if err != nil {
						return err
					}
					accounts = len(accts)
				}
				switch bareAction(initialized, accounts, isTTY()) {
				case actionStatus:
					return runStatus(cmd, m, false, false, false)
				case actionAdd:
					return runAdd(cmd, m, addOptions{})
				default:
					return fmt.Errorf("no accounts yet; run `ccp add` to pool your first subscription")
				}
			})
		},
	}
	root.SetVersionTemplate("{{.Version}}\n")

	root.AddCommand(
		newInitCmd(),
		newAddCmd(),
		newSelectCmd(),
		newLoginCmd(),
		newStatusCmd(),
		newListCmd(),
		newRunCmd(),
		newEnvCmd(),
		newDoctorCmd(),
		newMigrateCmd(),
		newFuseCmd(),
		newRemoveCmd(),
		newRenameCmd(),
		newServiceCmd(),
		newWidgetCmd(),
		newDaemonCmd(),
		newMountHolderCmd(),
	)
	return root
}

// rootAction is what bare `ccp` does for a given pool state.
type rootAction int

const (
	actionStatus rootAction = iota // pool has accounts → show status
	actionAdd                      // empty pool on a TTY → interactive onboarding
	actionErr                      // empty pool, no TTY → fail loud
)

// bareAction routes bare `ccp`: a populated pool shows status; an empty or
// uninitialized one onboards interactively, or errors when no TTY is attached.
func bareAction(initialized bool, accounts int, tty bool) rootAction {
	switch {
	case initialized && accounts > 0:
		return actionStatus
	case tty:
		return actionAdd
	default:
		return actionErr
	}
}

// withManager opens a Manager, runs fn, and closes it.
func withManager(fn func(*pool.Manager) error) error {
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	return fn(m)
}

// requireInit returns an error if the pool has not been initialized.
func requireInit(m *pool.Manager) error {
	ok, err := m.Initialized()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("pool not initialized; run `ccp add` to set it up and add your first account")
	}
	return nil
}
