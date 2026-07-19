package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// Test seams: tests must never touch real processes, mounts, or launchctl/brew.
var (
	scanSessions = procscan.Scan
	// dirMounted reads the kernel mount table (never hangs on a wedged mount);
	// mountAliveAt probes THROUGH the mount (2s bound), so a parked mirror reads dead.
	dirMounted   = overlay.Mounted
	mountAliveAt = overlay.MountAliveWithin
	// deepProbeAt is doctor's wedge probe: a 2 MiB read with a 5s bound.
	deepProbeAt = overlay.DeepProbeWithin
	// fpDomainProbeAt is doctor's File Provider data-plane wedge probe (daemon-down
	// path): it asks the companion app over its shallow control op to enumerate the
	// domain root — never a through-domain filesystem read
	// (which would mint a per-account TCC prompt). A domain whose bridge is down
	// (daemon down) answers domain-not-serving → ErrFPProbeWedged; an app that is
	// also down answers ErrFPProbeNoVerdict (unprobeable, reported as no wedge). A
	// seam so tests never dial a real app.
	fpDomainProbeAt = func(dir string) error {
		prov, err := fpOverlayProvider(fkoverlay.BackendFileProvider)
		if err != nil {
			return fmt.Errorf("%w: resolve file provider: %w", overlay.ErrFPProbeNoVerdict, err)
		}
		prober, ok := prov.(overlay.FPDomainProber)
		if !ok {
			return fmt.Errorf("%w: provider %T lacks the app control-op probe", overlay.ErrFPProbeNoVerdict, prov)
		}
		ctx, cancel := context.WithTimeout(context.Background(), fpDomainProbeTimeout)
		defer cancel()
		return overlay.FPDomainProbeShallow(ctx, prober, dir)
	}
	// fpOverlayProvider resolves the File Provider overlay provider for orphan
	// reconciliation; a seam so tests never remove a real domain.
	fpOverlayProvider = pool.OverlayProviderFor
	// fpRawProbeAt is doctor's OPT-IN raw File Provider probe (`--fp-raw-probe`):
	// it reads the account dir's .claude.json THROUGH the domain with a bound —
	// the materializing read the control-op probe exists to avoid, which can trip a
	// per-account macOS permission prompt, hence the flag gate. The result maps onto
	// the FP probe sentinels so reportFPWedges classifies it like the control op: a
	// bounded read that never answers (a genuinely wedged domain) is a wedge; ENOENT
	// is the benign no-identity Missing. A seam so tests never read a real domain.
	fpRawProbeAt = func(dir string) error {
		path := filepath.Join(dir, ".claude.json")
		done := make(chan error, 1)
		go func() {
			_, err := os.ReadFile(path) //nolint:gosec // G304: dir is a pool account dir, not external input
			done <- err
		}()
		select {
		case err := <-done:
			switch {
			case err == nil:
				return nil
			case errors.Is(err, os.ErrNotExist):
				return fmt.Errorf("%w: %s", overlay.ErrFPProbeMissing, path)
			default:
				return fmt.Errorf("%w: raw read of %s: %w", overlay.ErrFPProbeWedged, path, err)
			}
		case <-time.After(fpRawProbeTimeout):
			return fmt.Errorf("%w: raw read of %s did not answer within %s", overlay.ErrFPProbeWedged, path, fpRawProbeTimeout)
		}
	}
	stopDaemon      = stopDaemonService
	brewManaged     = func() bool { return ccpAgent().IsBrewManaged() }
	brewStop        = func(ctx context.Context) error { return ccpAgent().BrewStop(ctx) }
	runCCPLaunchctl = func(ctx context.Context, args ...string) (string, error) {
		//nolint:gosec // G204: launchctl is fixed; args are daemonkit's service-management argv, not external input.
		out, err := exec.CommandContext(ctx, "launchctl", args...).CombinedOutput()
		return string(out), err
	}
)

// fpRawProbeTimeout bounds the opt-in raw File Provider read so `ccp doctor`
// never parks on a wedged domain (the reader goroutine may leak on a genuinely
// wedged descriptor — unavoidable with os.ReadFile — but doctor is short-lived).
const fpRawProbeTimeout = 3 * time.Second

// fpDomainProbeTimeout bounds the one-shot File Provider domain verifications
// (`ccp doctor` daemon-down; post-migrate renderFPServeVerdicts) where a real
// verdict is the whole point. Sized above the app's ~13s worst-case reply and
// fusekit's 16s per-op client timeout so a slow-but-serving domain yields a
// verdict, not a false "cannot verify" — unlike the daemon's cheap 3s heal tick
// (daemon.fpControlProbeTimeout), which is NoVerdict-tolerant by design.
const fpDomainProbeTimeout = 20 * time.Second

// holderClient is Owner-scoped: List/Reclaim see only cc-pool's own mounts,
// never another tenant's.
func holderClient() *mountd.Client {
	return &mountd.Client{Socket: mountd.DefaultHolderSocket(), Owner: pool.HolderOwner}
}

// daemonBundleBin resolves the daemon .app bundle's main executable path from the
// running binary; a seam so tests exercise both the bundle-present and
// source-build branches.
var daemonBundleBin = func() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	return pool.DaemonBinaryPath(exe)
}

// daemonBundleExecutable returns the daemon .app bundle's main executable and
// whether it exists on disk. A Homebrew release ships CCPoolDaemon.app (app-group
// entitlement + embedded Developer ID profile) so launchd execs the bundle for
// prompt-free group-container access; source and HEAD builds have no bundle.
func daemonBundleExecutable() (string, bool) {
	p, err := daemonBundleBin()
	if err != nil {
		return "", false
	}
	fi, err := os.Stat(p)
	return p, err == nil && fi.Mode().IsRegular()
}

type ccpAgentLauncher struct{}

func (ccpAgentLauncher) Run(ctx context.Context, args ...string) (string, error) {
	return runCCPLaunchctl(ctx, args...)
}

// ccpAgent's Program is the daemon .app bundle when it is installed (launchd execs
// the entitled+profiled bundle), else empty so launchd falls back to os.Executable
// — a Homebrew symlink that stays a stable program path across upgrades.
func ccpAgent() service.Agent {
	program := ""
	if p, ok := daemonBundleExecutable(); ok {
		program = p
	}
	return service.Agent{
		Label:         "com.yasyf.cc-pool",
		Formula:       "cc-pool",
		Program:       program,
		Args:          []string{"daemon"},
		LogPath:       pool.LogPath(),
		RestartPolicy: service.RestartOnFailure,
		Launcher:      ccpAgentLauncher{},
		Env: map[string]string{
			"PATH": os.Getenv("PATH"),
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
				for _, line := range ccpAgent().StatusLines(cmd.Context()) {
					_, _ = fmt.Fprintln(out, line)
				}
				cl := daemon.NewClient()
				resp, healthErr := cl.Health()
				_ = cl.Close()
				if healthErr == nil && resp.OK {
					_, _ = fmt.Fprintf(out, "Daemon: running (%s)\n", resp.Version)
				} else {
					_, _ = fmt.Fprintln(out, "Daemon: not responding")
				}
				_, _ = fmt.Fprintf(out, "Socket: %s\n", pool.SocketPath())
				fuseRows, err := fuseAccountRows()
				if err != nil {
					return err
				}
				if line := holderStatusLine(holderClient(), fuseRows); line != "" {
					_, _ = fmt.Fprintln(out, line)
				}
				return nil
			},
		},
	)
	return cmd
}

// holderStatusLine dials the holder directly, not the daemon's cache: the
// holder outlives daemons, and status must reflect them when they disagree.
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
	// Holder version is unrelated to cc-pool's, so no skew verdict.
	return fmt.Sprintf("Mount holder: running (%s, %s)", ver, plural(len(mounts), "mount"))
}

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

// fuseBackedRow treats an unparseable overlay_kind as non-fuse, failing safe
// to the symlink path.
func fuseBackedRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	if err != nil {
		return false
	}
	return b.IsFuse()
}

func countFuse(accts []store.Account) int {
	n := 0
	for _, a := range accts {
		if fuseBackedRow(a.OverlayKind) {
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
		Short: "Stop the daemon and release cc-pool's mounts; --purge also removes pool state",
		Long: `uninstall stops the background daemon and releases cc-pool's mounts from the
shared fuse holder (it never stops the holder itself — that is multi-tenant and
launchd-managed, and other tools may be using it). It refuses while live claude sessions
sit on dirs it would yank — fuse accounts for a plain uninstall, every account
with --purge — unless --force vouches for them. --purge additionally removes all
pool accounts, their Keychain items, and ~/.cc-pool; ~/.claude is
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

// runServiceUninstall is strictly gate-before-destruction.
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

	reclaimHolderMounts(cmd)

	survivors := mountedAccounts(accts)
	// Account dirs are bridge symlinks into the shared mux mount, so a live
	// ~/.cc-pool/mnt is invisible to mountedAccounts — check it explicitly. It is
	// the load-bearing survivor now: purgeAll's RemoveAll(~/.cc-pool) would walk a
	// still-mounted mux root straight into the merged mirror and delete inside
	// ~/.claude.
	muxMounted := dirMounted(pool.MuxRootDir())
	if len(survivors) > 0 || muxMounted {
		names := make([]string, 0, len(survivors)+1)
		for _, a := range survivors {
			names = append(names, fmt.Sprintf("acct-%02d", a.ID))
			warn(errOut, "acct-%02d (%s) is still a live mountpoint", a.ID, a.ConfigDir)
		}
		if muxMounted {
			names = append(names, pool.MuxRootDir())
			warn(errOut, "the shared mount %s is still a live mountpoint", pool.MuxRootDir())
		}
		if purge {
			return fmt.Errorf("refusing to purge: %s is still a live mountpoint; unmount it first (purging through a live mirror would delete inside ~/.claude)", strings.Join(names, ", "))
		}
		return fmt.Errorf("%s still mounted after the holder shutdown; check %s", plural(len(names), "pool mount"), abbreviateHome(pool.MountHolderLogPath()))
	}

	if !purge {
		note(out, "Your accounts and state are preserved. Run `ccp service install` to resume.")
		return nil
	}
	return purgeAll(cmd)
}

// gateUninstallSessions refuses while live sessions sit on dirs the uninstall
// would destroy: fuse rows for a plain uninstall, every row for --purge. A
// failed scan aborts rather than yank a dir from under an unseen session.
func gateUninstallSessions(accts []store.Account, purge bool) error {
	sessions, err := scanSessions(context.Background())
	if err != nil {
		return fmt.Errorf("cannot verify no live sessions: %w; re-run with --force to skip this check", err)
	}
	var busy []string
	for _, a := range accts {
		if !purge && !fuseBackedRow(a.OverlayKind) {
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

// stopDaemonService is mount-safe (daemon shutdown never touches the holder's
// mounts). A failed stop is fatal: a still-live daemon respawns the holder
// and remounts fuse rows on its next heal tick.
func stopDaemonService(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if brewManaged() {
		if err := brewStop(cmd.Context()); err != nil {
			return fmt.Errorf("brew services stop: %w — a still-running daemon would respawn the mount holder mid-uninstall", err)
		}
		// Remove any stale self-rolled agent from before the brew switch.
		_ = ccpAgent().Uninstall(cmd.Context())
		success(out, "Stopped the daemon.")
		return nil
	}
	if err := ccpAgent().Uninstall(cmd.Context()); err != nil {
		return err
	}
	success(out, "Removed the LaunchAgent.")
	return nil
}

// reclaimHolderMounts unmounts only cc-pool-owned mounts and NEVER stops the
// shared multi-tenant holder. An unreachable holder is skipped silently; the
// caller re-verifies kernel truth, since a wedged reclaim can leave carcasses.
func reclaimHolderMounts(cmd *cobra.Command) {
	out, errOut := cmd.OutOrStdout(), cmd.ErrOrStderr()
	cl := holderClient()
	if !cl.Available() {
		return
	}
	failed, warning, err := cl.Reclaim()
	if err != nil {
		warn(errOut, "release mount-holder mounts: %v", err)
		return
	}
	if warning != "" {
		warn(errOut, "mount-holder reclaim persist-warning: %s", warning)
	}
	for _, mi := range failed {
		warn(errOut, "the mount holder couldn't unmount %s", mi.Dir)
	}
	success(out, "Released cc-pool's mounts from the shared holder.")
}

func poolAccounts() ([]store.Account, error) {
	var accts []store.Account
	err := withManager(func(m *pool.Manager) error {
		var e error
		accts, e = m.Store.ListAccounts()
		return e
	})
	return accts, err
}

func mountedAccounts(accts []store.Account) []store.Account {
	var still []store.Account
	for _, a := range accts {
		if dirMounted(a.ConfigDir) {
			still = append(still, a)
		}
	}
	return still
}

// mountedStateDirs scans on-disk dirs, not the (possibly already-deleted)
// account rows.
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

// purgeAll never touches ~/.claude or plain claude's canonical credential.
// The mountpoint re-check before RemoveAll is load-bearing: deleting through
// a live mirror deletes inside ~/.claude.
func purgeAll(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	err := withManager(func(m *pool.Manager) error {
		accts, err := m.Store.ListAccounts()
		if err != nil {
			return err
		}
		for _, a := range accts {
			if err := m.Remove(cmd.Context(), a.ID, true); err != nil {
				warn(cmd.ErrOrStderr(), "couldn't remove acct-%02d: %v", a.ID, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	still := mountedStateDirs()
	// The shared mux mount lives at ~/.cc-pool/mnt, outside accounts/, so
	// mountedStateDirs never sees it — but RemoveAll walks straight through it into
	// the merged mirror. Refuse while it is a kernel mountpoint.
	if dirMounted(pool.MuxRootDir()) {
		still = append(still, pool.MuxRootDir())
	}
	if len(still) > 0 {
		return fmt.Errorf("refusing to purge: %s is still a live mountpoint; unmount it first (purging through a live mirror would delete inside ~/.claude)", strings.Join(still, ", "))
	}
	if err := os.RemoveAll(pool.StateDir()); err != nil {
		return fmt.Errorf("remove %s: %w", pool.StateDir(), err)
	}
	success(out, "Purged all pool state. ~/.claude is untouched.")
	return nil
}

func runServiceInstall(cmd *cobra.Command) error {
	out := cmd.OutOrStdout()
	if _, ok := daemonBundleExecutable(); !ok {
		warn(cmd.ErrOrStderr(), "no CCPoolDaemon.app bundle found; running the daemon from the current binary — File Provider needs the signed release bundle for prompt-free group-container access (expected on source/HEAD builds)")
	}
	if ccpAgent().IsBrewManaged() {
		// A source build leaves a self-rolled agent that would run alongside
		// the brew one; boot it out first.
		_ = ccpAgent().Uninstall(cmd.Context())
		if err := ccpAgent().BrewStart(cmd.Context()); err != nil {
			return fmt.Errorf("brew services start: %w", err)
		}
		// brew services start only loads the job; a bootout race can leave it
		// loaded-but-not-running, so force the daemon to actually exec.
		if err := ccpAgent().BrewKickstart(cmd.Context()); err != nil {
			warn(cmd.ErrOrStderr(), "couldn't kickstart the brew service: %v", err)
		}
		success(out, "Started the daemon.")
		return nil
	}
	if err := ccpAgent().Install(cmd.Context()); err != nil {
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
