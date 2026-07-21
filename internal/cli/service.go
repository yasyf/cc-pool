package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/service"
)

// Test seams: tests must never touch real processes, mounts, or launchctl/brew.
var (
	scanSessions                = procscan.Scan
	stopDaemon                  = stopDaemonService
	ensureHolder                = daemon.EnsureHolderService
	stopHolder                  = daemon.StopAndUninstallHolderService
	serviceExecutable           = resolveDaemonServiceExecutable
	openDaemonServiceController = func(ctx context.Context) (daemonServiceController, error) {
		return service.NewController(ctx, daemonServiceControllerConfig())
	}
)

const (
	daemonServiceWorkerLimit  = 1
	daemonServiceCloseTimeout = 30 * time.Second
	daemonServicePATH         = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
)

type daemonServiceController interface {
	Converge(context.Context, []service.Agent) error
	Close(context.Context) error
}

func ccpAgent(executable string) (service.Agent, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return service.Agent{}, fmt.Errorf("daemon executable %q is not exact and absolute", executable)
	}
	return service.Agent{
		Label:         daemon.ServiceRoleID,
		Program:       executable,
		Args:          []string{"daemon"},
		LogPath:       pool.LogPath(),
		RestartPolicy: service.RestartOnFailure,
		Env: map[string]string{
			"PATH": daemonServicePATH,
		},
	}, nil
}

func resolveDaemonServiceExecutable() (string, error) {
	rolePath, err := daemon.ServiceRolePath()
	if err != nil {
		return "", err
	}
	executable, err := filepath.EvalSymlinks(rolePath)
	if err != nil {
		return "", fmt.Errorf("resolve daemon service target: %w", err)
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return "", fmt.Errorf("daemon service target %q is not exact and absolute", executable)
	}
	return executable, nil
}

func daemonServiceControllerConfig() service.ControllerConfig {
	return service.ControllerConfig{
		StatePath:   filepath.Join(pool.StateDir(), "daemon-services.db"),
		ProcessPath: filepath.Join(pool.StateDir(), "daemon-service-processes.db"),
		WorkerLimit: daemonServiceWorkerLimit,
	}
}

func withDaemonServiceController(
	ctx context.Context,
	run func(daemonServiceController) error,
) (err error) {
	controller, err := openDaemonServiceController(ctx)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), daemonServiceCloseTimeout)
		defer cancel()
		err = errors.Join(err, controller.Close(closeCtx))
	}()
	return run(controller)
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
			Use:    "holder-stop-uninstall",
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
				resp, healthErr := cl.Health()
				_ = cl.Close()
				if healthErr == nil && resp.OK {
					_, _ = fmt.Fprintf(out, "Daemon: running (%s)\n", resp.Version)
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
		Short: "Stop the daemon; --purge also removes pool state",
		Long: `uninstall stops the background daemon. --purge additionally removes all pool
accounts, their Keychain items, and ~/.cc-pool after refusing while live claude sessions
use those private account directories unless --force vouches for them. ~/.claude is never
touched.`,
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

func stopDaemonService(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if err := withDaemonServiceController(cmd.Context(), func(controller daemonServiceController) error {
		return controller.Converge(cmd.Context(), nil)
	}); err != nil {
		return err
	}
	if err := stopHolder(cmd.Context()); err != nil {
		return err
	}
	success(out, "Removed the daemon and holder LaunchAgents.")
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

func runServiceInstall(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if err := ensureHolder(cmd.Context()); err != nil {
		return err
	}
	executable, err := serviceExecutable()
	if err != nil {
		return err
	}
	agent, err := ccpAgent(executable)
	if err != nil {
		return err
	}
	if err := withDaemonServiceController(cmd.Context(), func(controller daemonServiceController) error {
		return controller.Converge(cmd.Context(), []service.Agent{agent})
	}); err != nil {
		return err
	}
	success(out, "Installed and started the daemon.")
	return nil
}

// ensureDaemon is best-effort: failures warn, callers fall back to direct
// sampling. daemonkit Runtime owns any exact-build takeover.
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
	cl := daemon.NewClient()
	defer func() { _ = cl.Close() }()
	resp, err := cl.Health()
	return err == nil && resp.OK && resp.Version == wantVersion
}

func waitDaemon(wantVersion string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if daemonAt(wantVersion) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
