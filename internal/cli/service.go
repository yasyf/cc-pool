package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/service"
	"github.com/yasyf/fusekit/version"
)

// Test seams. CLI tests must never scan real processes, stat real
// mountpoints, signal real pids, or drive launchctl/brew; each var is the
// real implementation in production and a fake in tests.
var (
	scanSessions = procscan.Scan
	// dirMounted is a non-blocking Getfsstat read of the kernel mount table
	// (overlay.Mounted): it answers membership without touching the mirror, so
	// it can never hang doctor's carcass check or the uninstall sweeps on a
	// partially wedged mount. mountAliveAt stays the bounded liveness probe
	// (overlay's 2s stat-probe harness): it reads base THROUGH the mount, where
	// a parked mirror is read as dead and flagged.
	dirMounted   = overlay.Mounted
	mountAliveAt = overlay.MountAliveWithin
	// deepProbeAt is doctor's bounded wedge probe (a 2 MiB read with a 5s
	// bound) — like the stat seams above, it can never hang doctor on a
	// wedged mirror.
	deepProbeAt    = overlay.DeepProbeWithin
	killHolderPeer = func(socket string) (int, error) { return mountd.NewClient(socket).Kill() }
	stopDaemon     = stopDaemonService
	brewManaged    = func() bool { return ccpAgent().IsBrewManaged() }
	brewStop       = func() error { return ccpAgent().BrewStop() }
	// holderGoneWait bounds each wait for the mount holder to release its
	// socket after Shutdown (and again after the kill escalation). The
	// holder's sweep runs under a 60s op deadline and the client's Shutdown
	// timeout is 65s, so this sits just above both — the same rationale as
	// the daemon's defaultHolderGoneWait.
	holderGoneWait = 70 * time.Second
)

// ccpAgent is cc-pool's daemon LaunchAgent / brew-services descriptor: the
// generic launchctl + Homebrew lifecycle (fusekit/service) configured with
// cc-pool's label, formula, daemon args, log path, and fuse-t library env. The
// program defaults to the running binary (os.Executable), so a Homebrew symlink
// stays a stable launchd program path across upgrades.
func ccpAgent() service.Agent {
	return service.Agent{
		Label:   "com.yasyf.cc-pool",
		Formula: "cc-pool",
		Args:    []string{"daemon"},
		LogPath: pool.LogPath(),
		Env: map[string]string{
			"PATH":                 os.Getenv("PATH"),
			"CGOFUSE_LIBFUSE_PATH": "/usr/local/lib/libfuse-t.dylib",
		},
	}
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
		newServiceUninstallCmd(),
		&cobra.Command{
			Use:   "status",
			Short: "Show daemon/LaunchAgent and mount-holder status",
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				out := cmd.OutOrStdout()
				if ccpAgent().IsBrewManaged() {
					_, _ = fmt.Fprintln(out, "Management: Homebrew (brew services)")
					if info, err := ccpAgent().BrewInfo(); err == nil {
						_, _ = fmt.Fprintln(out, info)
					}
				} else {
					_, _ = fmt.Fprintf(out, "Management: self-managed LaunchAgent (loaded: %v)\n", ccpAgent().Loaded())
				}
				if resp, err := daemon.NewClient().Health(); err == nil && resp.OK {
					_, _ = fmt.Fprintf(out, "Daemon: running (%s)\n", resp.Version)
				} else {
					_, _ = fmt.Fprintln(out, "Daemon: not responding")
				}
				_, _ = fmt.Fprintf(out, "Socket: %s\n", pool.SocketPath())
				fuseRows, err := fuseAccountRows()
				if err != nil {
					return err
				}
				if line := holderStatusLine(mountd.NewClient(pool.MountsSocketPath()), fuseRows); line != "" {
					_, _ = fmt.Fprintln(out, line)
				}
				return nil
			},
		},
	)
	return cmd
}

// holderStatusLine renders the mount-holder line for `ccp service status`, or
// "" when there is nothing truthful to say (no holder running and no fuse
// rows that would need one). It dials the holder directly rather than asking
// the daemon's cache: the holder outlives daemons by design, and service
// status is exactly where the user looks when the two disagree. Same trust as
// the daemon dial above it — a local 0600 socket.
func holderStatusLine(cl *mountd.Client, fuseRows int) string {
	if !cl.Available() {
		if fuseRows > 0 {
			return "Mount holder: not running"
		}
		return ""
	}
	ver, err := cl.Health()
	if err != nil {
		return "Mount holder: not responding"
	}
	mounts, err := cl.List()
	if err != nil {
		return fmt.Sprintf("Mount holder: running (%s, mounts unknown: %v)", ver, err)
	}
	line := fmt.Sprintf("Mount holder: running (%s, %s)", ver, plural(len(mounts), "mount"))
	if ver != version.String() {
		line += ", version skew — will be replaced when idle"
	}
	return line
}

// fuseAccountRows counts the fuse-kind account rows; `ccp service status`
// needs it to decide whether an absent holder is worth a line.
func fuseAccountRows() (int, error) {
	n := 0
	err := withManager(func(m *pool.Manager) error {
		accts, err := m.Store.ListAccounts()
		if err != nil {
			return err
		}
		n = countFuse(accts)
		return nil
	})
	return n, err
}

// countFuse counts the fuse-kind rows in an account list.
func countFuse(accts []store.Account) int {
	n := 0
	for _, a := range accts {
		if overlay.Kind(a.OverlayKind) == overlay.KindFuse {
			n++
		}
	}
	return n
}

func newServiceUninstallCmd() *cobra.Command {
	var purge bool
	var force bool
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Stop the daemon and mount holder; --purge also removes pool state",
		Long: `uninstall stops the background daemon and shuts down the detached mount
holder, unmounting every fuse account. It refuses while live claude sessions
sit on dirs it would yank — fuse accounts for a plain uninstall, every
account with --purge — unless --force vouches for them. --purge additionally
removes all pool accounts, their Keychain items, and ~/.cc-pool; ~/.claude is
never touched.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServiceUninstall(cmd, purge, force)
		},
	}
	cmd.Flags().BoolVar(&purge, "purge", false, "also remove all pool accounts and state; never touches ~/.claude")
	cmd.Flags().BoolVar(&force, "force", false, "skip the live-session gate (sessions on unmounted dirs will break)")
	return cmd
}

// runServiceUninstall is the uninstall flow, strictly gate-before-destruction:
// (a) refuse while live sessions depend on dirs this run would destroy,
// (b) stop the daemon (mount-safe: it never touches the holder's mounts),
// (c) retire the mount holder, (d) re-verify against kernel truth that no
// account dir is still a mountpoint, and only then (e) purge if asked.
func runServiceUninstall(cmd *cobra.Command, purge, force bool) error {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()

	accts, err := poolAccounts()
	if err != nil {
		return err
	}

	if !force {
		if err := gateUninstallSessions(accts, purge); err != nil {
			return err
		}
	}

	if err := stopDaemon(cmd); err != nil {
		return err
	}

	shutdownHolder(cmd)

	// Kernel-truth sweep verification. A purge must hard-abort on any
	// survivor: RemoveAll through a live mirror deletes inside ~/.claude.
	if survivors := mountedAccounts(accts); len(survivors) > 0 {
		names := make([]string, len(survivors))
		for i, a := range survivors {
			names[i] = fmt.Sprintf("acct-%02d", a.ID)
			warn(errOut, "acct-%02d (%s) is still a live mountpoint", a.ID, a.ConfigDir)
		}
		if purge {
			return fmt.Errorf("refusing to purge: %s is still a live mountpoint; unmount it first (purging through a live mirror would delete inside ~/.claude)", strings.Join(names, ", "))
		}
		return fmt.Errorf("%s still mounted after the holder shutdown; check %s", plural(len(survivors), "account dir"), abbreviateHome(pool.MountHolderLogPath()))
	}

	if !purge {
		note(out, "Your accounts and state are preserved. Run `ccp service install` to resume.")
		return nil
	}
	return purgeAll(cmd)
}

// gateUninstallSessions refuses the uninstall while live claude sessions sit
// on dirs it is about to destroy: a plain uninstall sweeps only the fuse
// mounts (symlink dirs survive untouched), while --purge removes every
// account dir. A failed scan aborts — proceeding blind could yank a dir out
// from under a session we could not see.
func gateUninstallSessions(accts []store.Account, purge bool) error {
	sessions, err := scanSessions(context.Background())
	if err != nil {
		return fmt.Errorf("cannot verify no live sessions: %w; re-run with --force to skip this check", err)
	}
	var busy []string
	for _, a := range accts {
		if !purge && overlay.Kind(a.OverlayKind) != overlay.KindFuse {
			continue
		}
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

// stopDaemonService stops the daemon: via `brew services` when
// Homebrew-managed (also clearing any stale self-rolled agent), else the
// self-rolled LaunchAgent. Mount-safe by design — the daemon never touches
// the holder's mounts on shutdown. A failed stop is fatal, never a warning:
// everything after this step (the holder sweep, the purge) is only safe once
// the daemon is actually down — a still-live daemon respawns the holder and
// remounts fuse rows on its next supervision tick.
func stopDaemonService(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if brewManaged() {
		if err := brewStop(); err != nil {
			return fmt.Errorf("brew services stop: %w — a still-running daemon would respawn the mount holder mid-uninstall", err)
		}
		// Remove any stale self-rolled agent from before the brew switch.
		_ = ccpAgent().Uninstall()
		success(out, "Stopped the daemon.")
		return nil
	}
	if err := ccpAgent().Uninstall(); err != nil {
		return err
	}
	success(out, "Removed the LaunchAgent.")
	return nil
}

// shutdownHolder retires the detached mount holder: ask it to sweep its
// mounts and exit, wait out its socket, and escalate to a socket-peer kill —
// loudly — only when it wedges. A holder that was never reachable is skipped
// silently (no holder means no process is serving mounts for us), but the
// caller still re-verifies kernel truth afterwards: a dead holder can leave
// mount carcasses.
func shutdownHolder(cmd *cobra.Command) {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()
	cl := mountd.NewClient(pool.MountsSocketPath())
	if !cl.Available() {
		return
	}
	failed, err := cl.Shutdown()
	if err != nil {
		warn(errOut, "mount holder shutdown: %v", err)
	}
	for _, mi := range failed {
		warn(errOut, "the mount holder couldn't unmount %s", mi.Dir)
	}
	if cl.WaitGone(holderGoneWait) {
		success(out, "Stopped the mount holder.")
		return
	}
	warn(errOut, "the mount holder won't release %s; killing the process holding it", pool.MountsSocketPath())
	if _, kerr := killHolderPeer(pool.MountsSocketPath()); kerr != nil {
		warn(errOut, "couldn't kill the mount holder: %v", kerr)
	}
	if cl.WaitGone(holderGoneWait) {
		success(out, "Stopped the mount holder.")
		return
	}
	warn(errOut, "the mount holder still holds %s; check `ccp service status`", pool.MountsSocketPath())
}

// poolAccounts lists every account row (empty when the pool has none).
func poolAccounts() ([]store.Account, error) {
	var accts []store.Account
	err := withManager(func(m *pool.Manager) error {
		var e error
		accts, e = m.Store.ListAccounts()
		return e
	})
	return accts, err
}

// mountedAccounts returns the accounts whose config dirs are still
// mountpoints — kernel truth, independent of what the holder reported.
func mountedAccounts(accts []store.Account) []store.Account {
	var still []store.Account
	for _, a := range accts {
		if dirMounted(a.ConfigDir) {
			still = append(still, a)
		}
	}
	return still
}

// mountedStateDirs returns any dir under ~/.cc-pool/accounts that is still a
// mountpoint, independent of the (possibly already deleted) account rows. A
// missing accounts dir means nothing can be mounted under it.
func mountedStateDirs() []string {
	entries, err := os.ReadDir(pool.AccountsDir())
	if err != nil {
		return nil
	}
	var still []string
	for _, e := range entries {
		dir := filepath.Join(pool.AccountsDir(), e.Name())
		if dirMounted(dir) {
			still = append(still, dir)
		}
	}
	return still
}

// purgeAll removes every pool account (overlay + Keychain items) and the
// state dir (which contains the account dirs). ~/.claude and plain claude's
// canonical credential are never touched. Callers must already have verified
// no account dir is a live mountpoint; the re-check immediately before the
// recursive delete is belt and braces for the catastrophic path — RemoveAll
// through a live mirror deletes inside ~/.claude.
func purgeAll(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	err := withManager(func(m *pool.Manager) error {
		accts, err := m.Store.ListAccounts()
		if err != nil {
			return err
		}
		for _, a := range accts {
			if err := m.Remove(a.ID, true); err != nil {
				warn(cmd.ErrOrStderr(), "couldn't remove acct-%02d: %v", a.ID, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if still := mountedStateDirs(); len(still) > 0 {
		return fmt.Errorf("refusing to purge: %s is still a live mountpoint; unmount it first (purging through a live mirror would delete inside ~/.claude)", strings.Join(still, ", "))
	}
	if err := os.RemoveAll(pool.StateDir()); err != nil {
		return fmt.Errorf("remove %s: %w", pool.StateDir(), err)
	}
	success(out, "Purged all pool state. ~/.claude is untouched.")
	return nil
}

// runServiceInstall starts the daemon: via `brew services` when Homebrew-managed
// (booting out any stale self-rolled agent first), else the self-rolled
// LaunchAgent.
func runServiceInstall(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if ccpAgent().IsBrewManaged() {
		// A source-build `ccp service install` leaves a self-rolled
		// com.yasyf.cc-pool agent that would run alongside the brew one.
		// Boot it out before delegating.
		_ = ccpAgent().Uninstall()
		if err := ccpAgent().BrewStart(); err != nil {
			return fmt.Errorf("brew services start: %w", err)
		}
		// brew services start only loads the job; a bootout race can leave it
		// loaded-but-not-running, so force the daemon to actually exec.
		if err := ccpAgent().BrewKickstart(); err != nil {
			warn(cmd.ErrOrStderr(), "couldn't kickstart the brew service: %v", err)
		}
		success(out, "Started the daemon.")
		return nil
	}
	if err := ccpAgent().Install(); err != nil {
		return err
	}
	success(out, "Installed and started the daemon.")
	return nil
}

// evictTimeout bounds the wait for a version-skewed daemon to release the
// socket after being asked to step down.
const evictTimeout = 5 * time.Second

// ensureDaemon makes sure a daemon at the current version is responding,
// silently starting (or restarting, after an upgrade) it as needed.
// Best-effort: failures are warnings, never fatal — no calling flow requires
// the daemon (selection and add validation fall back to direct sampling).
func ensureDaemon(cmd *cobra.Command) {
	want := version.String()
	if daemonAt(want) {
		return
	}
	cl := daemon.NewClient()
	if resp, err := cl.Health(); err == nil && resp.OK {
		// A version-skewed daemon answers here: launchd's KeepAlive holding a
		// pre-upgrade image, or a detached EnsureRunning spawn launchd never
		// tracked.
		step(cmd.OutOrStdout(), "Restarting the cc-pool daemon to pick up the new version…")
		// Bootout first: this terminates a launchd-tracked daemon — mount-safe,
		// since the detached holder keeps serving any fuse mirrors across it —
		// and disables KeepAlive so launchd can't respawn the pre-upgrade image
		// under us. A no-op for an orphan launchd never tracked.
		if ccpAgent().IsBrewManaged() {
			_ = ccpAgent().BrewStop()
		} else {
			_ = ccpAgent().Uninstall()
		}
		// An orphan survives bootout. Ask it to step down over the socket (its one
		// clean-teardown path); if it still won't let go, kill the exact process on
		// the other end of the socket — what the old `pkill` hint did by hand.
		if !cl.WaitGone(evictTimeout) {
			_, _ = cl.Shutdown()
			if !cl.WaitGone(evictTimeout) {
				if _, err := cl.KillSocketPeer(); err != nil {
					warn(cmd.ErrOrStderr(), "couldn't evict the old daemon (%s): %v", resp.Version, err)
				}
				if !cl.WaitGone(evictTimeout) {
					warn(cmd.ErrOrStderr(),
						"the old daemon (%s) still won't release the socket; check `ccp service status`", resp.Version)
				}
			}
		}
	} else {
		step(cmd.OutOrStdout(), "Starting the cc-pool daemon…")
	}
	if err := runServiceInstall(cmd); err != nil {
		// Re-check at the wanted version: a concurrent start or an
		// already-bootstrapped agent can fail the install while leaving a
		// healthy current daemon behind.
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

// daemonAt reports whether a healthy daemon at exactly wantVersion responds.
// Version-checked so a stale pre-upgrade daemon never counts as success.
func daemonAt(wantVersion string) bool {
	resp, err := daemon.NewClient().Health()
	return err == nil && resp.OK && resp.Version == wantVersion
}

// waitDaemon polls until a daemon at wantVersion responds or timeout elapses.
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
