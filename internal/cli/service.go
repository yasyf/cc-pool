package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
)

// Test seams: tests must never touch real processes, mounts, or launchctl/brew.
var (
	scanSessions        = procscan.Scan
	stopDaemon          = stopDaemonService
	ensureHolder        = func(ctx context.Context) (holderServiceInstall, error) { return daemon.InstallHolderService(ctx) }
	stopHolder          = daemon.StopAndUninstallHolderService
	ensureDaemonService = func(ctx context.Context) (daemonkit.Ensured, error) {
		spec, err := daemon.ProductionSpec()
		if err != nil {
			return daemonkit.Ensured{}, err
		}
		client, err := daemonkit.Open(spec)
		if err != nil {
			return daemonkit.Ensured{}, err
		}
		return client.Ensure(ctx)
	}
	removeLegacyDaemon = daemon.RemoveLegacyDaemon
	// daemonHealthContext is the Control-routed health observation, seamed
	// because daemonkit's control lane refuses an in-process self-attach
	// (daemonkit control.go:136): a test daemon serves the business lane
	// in-process but can never answer this verb.
	daemonHealthContext = func(ctx context.Context, cl *daemon.Client) (*daemon.HealthResponse, error) {
		return cl.HealthContext(ctx)
	}
	stopDaemonRuntime = func(ctx context.Context) error {
		if err := removeLegacyDaemon(ctx); err != nil {
			return err
		}
		client, err := daemonkit.Open(daemon.Spec(daemonkit.Program{}, nil))
		if err != nil {
			return err
		}
		return client.Stop(ctx)
	}
)

// The whole budgets each daemonkit lifecycle verb is worth: a caller that
// stated its own deadline keeps it, per the fleet deadline-budget convention.
const (
	daemonServiceEnsureTimeout = 90 * time.Second
	daemonServiceStopTimeout   = 65 * time.Second
	daemonServiceCloseTimeout  = 30 * time.Second
)

// budgeted states budget as ctx's deadline when ctx carries none. A caller
// that stated its own keeps it: the budget is this package's default, never an
// override of a deadline the caller chose.
func budgeted(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if _, stated := ctx.Deadline(); stated {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
}

type holderServiceInstall interface {
	Commit()
	Rollback(context.Context) error
}

func newServiceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "service",
		Short: "Manage the background daemon",
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "install",
			Short: "Install and start the user LaunchAgent",
			Args:  cobra.NoArgs,
			RunE:  func(cmd *cobra.Command, _ []string) error { return runServiceInstall(cmd) },
		},
		&cobra.Command{
			Use:    "runtime-stop-uninstall",
			Hidden: true,
			Args:   cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return stopHolder(cmd.Context())
			},
		},
		newServiceUninstallCmd(),
		&cobra.Command{
			Use:   "status",
			Short: "Show daemon and LaunchAgent status",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				out := cmd.OutOrStdout()
				cl := daemon.NewClient()
				resp, healthErr := daemonHealthContext(context.Background(), cl)
				_ = cl.Close()
				if healthErr == nil {
					_, _ = fmt.Fprintf(out, "Daemon: running (%s)\n", resp.RuntimeBuild)
				} else {
					_, _ = fmt.Fprintln(out, "Daemon: not responding")
				}
				_, _ = fmt.Fprintf(out, "Socket: %s\n", pool.SocketPath())
				return nil
			},
		},
	)
	return cmd
}

func newServiceUninstallCmd() *cobra.Command {
	var purge bool
	var force bool
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Stop the daemon; --purge also deprovisions accounts and removes pool state",
		Long: `uninstall stops only the background daemon and preserves the signed runtime and
File Provider domains. --purge additionally deprovisions all accounts, removes their
Keychain items and ~/.cc-pool, and refuses while live claude sessions use those private
account directories unless --force vouches for them. ~/.claude is never touched.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServiceUninstall(cmd, purge, force)
		},
	}
	cmd.Flags().BoolVar(&purge, "purge", false, "also remove all pool accounts and state; never touches ~/.claude")
	cmd.Flags().BoolVar(&force, "force", false, "skip the live-session gate when purging")
	return cmd
}

// runServiceUninstall is strictly gate-before-destruction.
func runServiceUninstall(cmd *cobra.Command, purge, force bool) error {
	out := cmd.OutOrStdout()

	accts, err := poolAccounts(cmd.Context())
	if err != nil {
		return err
	}

	if purge && !force {
		if err := gateUninstallSessions(accts); err != nil {
			return err
		}
	}
	if purge {
		if err := deprovisionAll(cmd); err != nil {
			return err
		}
	}

	if err := stopDaemon(cmd); err != nil {
		return err
	}

	if !purge {
		note(out, "Your accounts and state are preserved. Run `ccp service install` to resume.")
		return nil
	}
	if err := stopHolder(cmd.Context()); err != nil {
		return err
	}
	return purgeAll(cmd)
}

// gateUninstallSessions refuses while live sessions use account presentations.
// A failed scan aborts rather than removing a tenant under an unseen session.
func gateUninstallSessions(accts []store.Account) error {
	sessions, err := scanSessions(context.Background())
	if err != nil {
		return fmt.Errorf("cannot verify no live sessions: %w; re-run with --force to skip this check", err)
	}
	var busy []string
	for _, a := range accts {
		var pids []string
		for _, s := range sessions {
			if a.ConfigDir != "" && s.ConfigDir == a.ConfigDir {
				pids = append(pids, strconv.Itoa(s.PID))
			}
		}
		if len(pids) > 0 {
			busy = append(busy, fmt.Sprintf("acct-%02d (pid %s)", a.ID, strings.Join(pids, ", ")))
		}
	}
	if len(busy) > 0 {
		return fmt.Errorf("live claude sessions are using pool accounts: %s — close them or pass --force", strings.Join(busy, "; "))
	}
	return nil
}

// stopDaemonService leads with the direct legacy bootout — Client.Stop cannot
// reach a live pre-daemonkit incumbent, whose listener refuses the attach —
// then routes the marked world through Stop's own observe-drain-remove ladder.
func stopDaemonService(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	stopCtx, cancel := budgeted(cmd.Context(), daemonServiceStopTimeout)
	defer cancel()
	if err := stopDaemonRuntime(stopCtx); err != nil {
		return err
	}
	success(out, "Removed the daemon LaunchAgent. The signed FuseKit runtime and File Provider domains are preserved.")
	return nil
}

func poolAccounts(ctx context.Context) ([]store.Account, error) {
	var accts []store.Account
	err := withManager(ctx, func(m *pool.Manager) error {
		var e error
		accts, e = m.Store.ListAccounts()
		return e
	})
	return accts, err
}

// purgeAll never touches ~/.claude or plain claude's canonical credential.
func purgeAll(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if err := withManager(cmd.Context(), func(m *pool.Manager) error {
		accts, err := m.Store.ListAccounts()
		if err != nil {
			return err
		}
		if len(accts) != 0 {
			return fmt.Errorf("refusing to purge: %d accounts remain provisioned", len(accts))
		}
		return nil
	}); err != nil {
		return err
	}
	if err := os.RemoveAll(pool.StateDir()); err != nil {
		return fmt.Errorf("remove %s: %w", pool.StateDir(), err)
	}
	success(out, "Purged all pool state. ~/.claude is untouched.")
	return nil
}

func deprovisionAll(cmd *cobra.Command) error {
	accounts, err := poolAccounts(cmd.Context())
	if err != nil {
		return err
	}
	if len(accounts) == 0 {
		return nil
	}
	ensureDaemon(cmd)
	client := daemon.NewClient()
	defer func() { _ = client.Close() }()
	for _, account := range accounts {
		if err := client.RemoveAccount(cmd.Context(), account.ID, true); err != nil {
			return fmt.Errorf("deprovision acct-%02d before purge: %w", account.ID, err)
		}
	}
	return nil
}

func runServiceInstall(cmd *cobra.Command) (err error) {
	if err := installDaemonService(cmd.Context()); err != nil {
		return err
	}
	success(cmd.OutOrStdout(), "Installed and started the daemon.")
	return nil
}

// installDaemonService converges the daemon through Ensure, which places the
// stable executable, applies the LaunchAgent, evicts a stale incumbent, and
// subscribes on readiness — the retired controller's whole ladder in one verb.
func installDaemonService(ctx context.Context) (err error) {
	holderInstall, err := ensureHolder(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), daemonServiceCloseTimeout)
		defer cancel()
		err = errors.Join(err, holderInstall.Rollback(rollbackCtx))
	}()
	if err := removeLegacyDaemon(ctx); err != nil {
		return err
	}
	ensureCtx, cancel := budgeted(ctx, daemonServiceEnsureTimeout)
	defer cancel()
	ensured, err := ensureDaemonService(ensureCtx)
	if err != nil {
		return fmt.Errorf("ensure cc-pool daemon: %w", err)
	}
	if ensured.After.Build != version.String() {
		return fmt.Errorf(
			"ensured daemon build %q is not this build %q", ensured.After.Build, version.String(),
		)
	}
	holderInstall.Commit()
	return nil
}

// ensureDaemon is best-effort: failures warn, callers fall back to direct
// sampling. Ensure owns any exact-build takeover.
func ensureDaemon(cmd *cobra.Command) {
	want := version.String()
	if daemonAt(want) {
		return
	}
	step(cmd.OutOrStdout(), "Starting the cc-pool daemon…")
	if err := runServiceInstall(cmd); err != nil {
		// A concurrent start or an already-bootstrapped agent can fail the
		// install while leaving a healthy current daemon behind.
		if daemonAt(want) {
			return
		}
		warn(cmd.ErrOrStderr(),
			"couldn't start the daemon: %v; run `ccp service install` from a GUI session to enable background polling", err)
		return
	}
	ready := true
	_ = withSpinner(cmd.OutOrStdout(), "waiting for the daemon…", func() error {
		ready = waitDaemon(want, 10*time.Second)
		return nil
	})
	if !ready {
		warn(cmd.ErrOrStderr(), "the daemon isn't responding yet; check `ccp service status`")
	}
}

// daemonAt is exact-version: a stale pre-upgrade daemon never counts.
func daemonAt(wantVersion string) bool {
	return daemonHealth(context.Background(), wantVersion) == nil
}

func daemonHealth(ctx context.Context, wantVersion string) error {
	cl := daemon.NewClient()
	defer func() { _ = cl.Close() }()
	resp, err := daemonHealthContext(ctx, cl)
	if err != nil {
		return err
	}
	if resp.RuntimeBuild != wantVersion {
		return fmt.Errorf("daemon identity is not exact: build=%q, want build=%q", resp.RuntimeBuild, wantVersion)
	}
	return nil
}

func waitForDaemonService(ctx context.Context, wantVersion string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		lastErr = daemonHealth(ctx, wantVersion)
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func waitDaemon(wantVersion string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return waitForDaemonService(ctx, wantVersion) == nil
}
