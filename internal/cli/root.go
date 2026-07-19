// Package cli wires up the cobra command tree for cc-pool. The binary is
// installed as `cc-pool` with a `ccp` symlink; both dispatch here.
package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
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
		// Args deliberately nil: cobra's legacyArgs already rejects unknown
		// subcommands on a root with children, so RunE only runs for bare `ccp`.
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
		newFPCmd(),
		newCredCmd(),
		newRemoveCmd(),
		newRenameCmd(),
		newServiceCmd(),
		newWidgetCmd(),
		newDaemonCmd(),
		newSyncCmd(),
	)
	return root
}

type rootAction int

const (
	actionStatus rootAction = iota
	actionAdd
	actionErr
)

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

func withManager(fn func(*pool.Manager) error) error {
	m, err := pool.Open()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	return fn(m)
}

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

// requireDaemon vets an op that must run inside the daemon: pool initialized,
// daemon reachable, and an exact version match — a skewed daemon is refused,
// never auto-restarted here. purpose opens the not-running error, e.g.
// "migration runs inside the daemon (it owns the conversion gates)".
func requireDaemon(m *pool.Manager, purpose string) (*daemon.Client, error) {
	if err := requireInit(m); err != nil {
		return nil, err
	}
	cl := daemon.NewClient()
	keepClient := false
	defer func() {
		if !keepClient {
			_ = cl.Close()
		}
	}()
	health, err := cl.Health()
	switch {
	case errors.Is(err, daemon.ErrDaemonUnavailable):
		return nil, fmt.Errorf("%s, which is not running; start it with `ccp service install` and re-run: %w", purpose, err)
	case err != nil:
		// Dial ok, probe failed: hung not absent — don't prescribe a restart (masks the real failure).
		return nil, fmt.Errorf("daemon health check: %w", err)
	}
	if health.Version != version.String() {
		return nil, fmt.Errorf("the daemon is %s but this ccp is %s; restart it (`brew services restart cc-pool` or `ccp service install`) and re-run — mounts and live sessions are unaffected", health.Version, version.String())
	}
	keepClient = true
	return cl, nil
}
