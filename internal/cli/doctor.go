package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/fileproviderd"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func newDoctorCmd() *cobra.Command {
	var fix bool
	var openSettings bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check accounts' Keychain items and overlays; --fix repairs drift",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				out := cmd.OutOrStdout()
				ok := true
				report := func(label string, healthy bool, detail string) {
					mark := okStyle.Render("✓")
					if !healthy {
						mark = badStyle.Render("✗")
						ok = false
					}
					if detail != "" {
						_, _ = fmt.Fprintf(out, "%s %s: %s\n", mark, label, detail)
					} else {
						_, _ = fmt.Fprintf(out, "%s %s\n", mark, label)
					}
				}

				// Auto-update can move claude off PATH.
				if _, err := exec.LookPath("claude"); err != nil {
					report("claude on PATH", false, err.Error())
				} else {
					report("claude on PATH", true, "")
				}

				var cachedHolder *daemon.HolderStatus
				var contentHealth string
				var fpConsentPending bool
				var fpWedged []daemon.FPDomainState
				var daemonAlive bool
				if resp, err := daemon.NewClient().Health(); err == nil && resp.OK {
					daemonAlive = true
					report("daemon", true, resp.Version)
					// Pending-TCC guidance, content-source health, and the File
					// Provider wedge verdicts live only in the daemon's cache.
					if sresp, serr := daemon.NewClient().Status(); serr == nil && sresp.OK {
						cachedHolder = sresp.Holder
						contentHealth = sresp.ContentHealth
						fpConsentPending = sresp.FPConsentPending
						fpWedged = sresp.FPWedged
					}
				} else {
					report("daemon", false, "not running; run `ccp service install`")
				}

				accts, err := m.Store.ListAccounts()
				if err != nil {
					return err
				}

				reachable, holderVer := probeHolder()
				facts := holderFacts{
					reachable: reachable,
					version:   holderVer,
					cached:    cachedHolder,
				}
				reportHolder(facts, countFuse(accts), report)
				reportHolderMitigations(facts, report)
				reportCarcasses(accts, report)

				// Mount truth lives in the holder, which outlives daemons — not the daemon cache.
				var holderMounts []mountd.MountInfo
				if reachable {
					holderMounts = listHolderMounts()
				}
				var sessions []procscan.Session
				if len(holderMounts) > 0 {
					// Advisory checks: a failed scan degrades silently.
					sessions, _ = scanSessions(cmd.Context())
				}
				reportWedges(accts, holderMounts, sessions, report)
				reportStaleSessions(accts, holderMounts, sessions, report)
				reportOrphanedHolder(cmd.Context(), reachable, accts, report)

				reportFileProvider(cmd.Context(), m, accts, fpConsentPending, report)
				reportFPWedges(accts, fpWedged, daemonAlive, report)
				reportContentHealth(contentHealth, report)

				for _, a := range accts {
					checkAccount(cmd, m, a, fix, report)
				}

				if !ok {
					if fix {
						_, _ = fmt.Fprintln(out, "\nApplied fixes where possible; re-run `ccp doctor` to confirm.")
					} else {
						_, _ = fmt.Fprintln(out, "\nIssues found. Re-run with --fix to repair.")
					}
				} else {
					_, _ = fmt.Fprintln(out, "\nAll checks passed.")
				}

				// The report detail above already carries the deep link.
				if openSettings && cachedHolder != nil && cachedHolder.TCCError != "" {
					backend := cachedHolder.TCCBlockedBackend
					if err := backend.OpenSettings(cmd.Context()); err != nil {
						warn(cmd.ErrOrStderr(), "couldn't open the %s settings pane: %v", backend.Enablement().Pane, err)
					}
				}
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&fix, "fix", false, "attempt to repair detected drift")
	cmd.Flags().BoolVar(&openSettings, "open-settings", false, "if the mount holder needs a macOS grant, open its System Settings pane")
	return cmd
}

// fuseGrantHint renders the hint from the backend's Enablement — no cc-pool pane literal.
func fuseGrantHint(backend fkoverlay.Backend) string {
	en := backend.Enablement()
	if !en.Needed {
		return "grant the required macOS permission"
	}
	if len(en.URLs) > 0 {
		return fmt.Sprintf("open Settings (%s): open %q", en.Pane, en.URLs[0])
	}
	return "open Settings: " + en.Pane
}

type holderFacts struct {
	reachable bool
	version   string
	cached    *daemon.HolderStatus
}

func probeHolder() (reachable bool, ver string) {
	cl := holderClient()
	if !cl.Available() {
		return false, ""
	}
	v, err := cl.Health()
	if err != nil {
		return false, ""
	}
	return true, v
}

// The holder is a separate multi-tenant product: cc-pool never compares the
// holder's version against its own or replaces the holder over skew (the
// cask's launchd owns upgrades; a version-replacement would tear down other
// tenants' mounts), and serving no cc-pool mounts is not an orphan. The one
// version constraint that does exist — the MinHolderVersion mitigation floor,
// a refusal, never a replacement — is reportHolderMitigations' job.
func reportHolder(f holderFacts, fuseRows int, report func(string, bool, string)) {
	switch {
	case !f.reachable && fuseRows > 0:
		report("mount holder", false, fmt.Sprintf("not running with %s; install the fusekit-holder cask (`ccp fuse enable`) — check %s",
			plural(fuseRows, "fuse account"), abbreviateHome(pool.MountHolderLogPath())))
	case f.reachable:
		report("mount holder", true, f.version)
	}
	if f.cached == nil {
		return
	}
	if f.cached.TCCError != "" {
		report("mount holder grant", false, f.cached.TCCError+" — "+fuseGrantHint(f.cached.TCCBlockedBackend)+" (cc-pool falls back to symlink automatically if the grant never lands)")
	}
}

// reportHolderMitigations flags a reachable holder that predates the NFS
// kernel-panic mitigations (pool.MinHolderVersion). An unreachable holder is
// silent — reportHolder already covers that.
func reportHolderMitigations(f holderFacts, report func(string, bool, string)) {
	if !f.reachable {
		return
	}
	if pool.HolderVersionMitigated(f.version) {
		report("holder panic mitigations", true, "")
		return
	}
	report("holder panic mitigations", false, fmt.Sprintf("holder %s predates the NFS kernel-panic mitigations (need %s or newer) — run `brew upgrade --cask fusekit-holder`",
		f.version, pool.MinHolderVersion))
}

// listHolderMounts returns nil on failure; reportHolder already covers an unreachable holder.
func listHolderMounts() []mountd.MountInfo {
	mounts, err := holderClient().List()
	if err != nil {
		return nil
	}
	return mounts
}

// reportWedges deep-probes Live fuse rows itself (works with the daemon down).
// ErrProbeMissing is no verdict, not a wedge. Nothing run against a possibly
// wedged mirror may block unboundedly.
func reportWedges(accts []store.Account, mounts []mountd.MountInfo, sessions []procscan.Session, report func(string, bool, string)) {
	byDir := make(map[string]mountd.MountInfo, len(mounts))
	for _, mi := range mounts {
		byDir[mi.Dir] = mi
	}
	for _, a := range accts {
		if !fuseBackedRow(a.OverlayKind) {
			continue
		}
		mi, held := byDir[a.ConfigDir]
		if !held || !mi.Live {
			continue
		}
		if err := deepProbeAt(a.ConfigDir); err != nil && !errors.Is(err, overlay.ErrProbeMissing) {
			detail := "wedged (serves metadata but hangs reads) — the daemon will remount it"
			if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
				detail = fmt.Sprintf("wedged (serves metadata but hangs reads); left mounted under %d live session(s); relaunch them — the daemon will NOT force-unmount a busy mirror (would panic the kernel)", n)
			}
			report(fmt.Sprintf("acct-%02d mirror", a.ID), false, detail)
		}
	}
}

// staleSessionSlack absorbs second-granularity rounding on both sides (ps
// etime, the holder's MountedAt) plus scan-vs-List clock jitter.
const staleSessionSlack = 5 * time.Second

// reportStaleSessions flags sessions started before the holder's current mount
// of their dir. A zero MountedAt or StartedAt is no fact and is skipped.
func reportStaleSessions(accts []store.Account, mounts []mountd.MountInfo, sessions []procscan.Session, report func(string, bool, string)) {
	mountedAt := make(map[string]time.Time, len(mounts))
	for _, mi := range mounts {
		if mi.MountedAt != 0 {
			mountedAt[mi.Dir] = time.Unix(mi.MountedAt, 0)
		}
	}
	for _, a := range accts {
		if !fuseBackedRow(a.OverlayKind) {
			continue
		}
		at, ok := mountedAt[a.ConfigDir]
		if !ok {
			continue
		}
		for _, s := range sessions {
			if s.ConfigDir != a.ConfigDir || s.StartedAt.IsZero() {
				continue
			}
			if at.Sub(s.StartedAt) > staleSessionSlack {
				report(fmt.Sprintf("acct-%02d session", a.ID), false,
					fmt.Sprintf("pid %d predates the current mirror (remounted %s) — it is bound to a yanked mount; relaunch it",
						s.PID, at.Format("15:04:05")))
			}
		}
	}
}

// fpControlHealth probes the CCPoolStatus companion app's control socket for
// its version — a seam so doctor tests fake the socket.
var fpControlHealth = func(ctx context.Context) (string, error) {
	return fileproviderd.NewAppClient(pool.FPControlSocketPath()).Health(ctx)
}

// fpBridgeReachable reports whether the daemon's File Provider data socket
// accepts a connection — a seam so doctor tests fake the socket.
var fpBridgeReachable = func() bool {
	return content.NewBridgeClient(pool.FPBridgeSocketPath()).Available()
}

// fileProviderRow treats an unparseable overlay_kind as non-fileprovider,
// failing safe to the symlink path (fuseBackedRow's sibling — File Provider
// is a third backend category, never folded into fuse).
func fileProviderRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	if err != nil {
		return false
	}
	return b == fkoverlay.BackendFileProvider
}

func countFileProvider(accts []store.Account) int {
	n := 0
	for _, a := range accts {
		if fileProviderRow(a.OverlayKind) {
			n++
		}
	}
	return n
}

// reportFileProvider covers the File Provider rungs: extension enablement, app
// control socket, daemon bridge socket. Silent when the opt-in stack is unused;
// with rows, an absent extension is the root fault and skips the socket probes.
func reportFileProvider(ctx context.Context, m *pool.Manager, accts []store.Account, consentPending bool, report func(string, bool, string)) {
	fpRows := countFileProvider(accts)
	if !fpAvailable(m.OverlaySpec()) {
		if fpRows == 0 {
			return
		}
		en := fkoverlay.BackendFileProvider.Enablement()
		report("file provider extension", false, fmt.Sprintf(
			"not enabled with %s — run `ccp fp onboard` for the guided setup (install %s if missing, then: %s)",
			plural(fpRows, "fileprovider account"), pool.WidgetAppPath(), en.Guidance,
		))
		return
	}
	report("file provider extension", true,
		fmt.Sprintf("%s; %s", pool.FPExtensionBundleID, plural(fpRows, "fileprovider account")))
	if ver, err := fpControlHealth(ctx); err != nil {
		report("file provider app", false, fmt.Sprintf(
			"control socket %s not answering: %v — launch %s so domains can be registered and signalled",
			abbreviateHome(pool.FPControlSocketPath()), err, pool.WidgetAppPath(),
		))
	} else {
		report("file provider app", true, ver)
	}
	switch {
	case fpBridgeReachable():
		report("file provider bridge", true, "")
	case consentPending:
		report("file provider bridge", false,
			"data socket "+abbreviateHome(pool.FPBridgeSocketPath())+" not accepting — the daemon reports its bind parked on the one-time app group container consent prompt (macOS re-asks after every upgrade, and launchd never surfaces it): approve it, then restart the daemon (`brew services restart cc-pool`) — `ccp fp onboard` walks this end to end")
	default:
		report("file provider bridge", false,
			"data socket "+abbreviateHome(pool.FPBridgeSocketPath())+" not accepting — the daemon binds it at startup and retries every few seconds (is the daemon running? check `ccp service status`); on first run macOS gates the app group container behind a one-time consent prompt: approve it, then restart the daemon; domains cannot fetch computed content until the socket is up — run `ccp fp onboard` for the guided setup")
	}
}

// reportFPWedges surfaces wedged File Provider domains — control ops answer but
// reads hang, the wedge the control-plane health check misses. When the daemon is
// up it renders the daemon's authoritative verdicts (it is actively recovering
// them); with the daemon down it probes each File Provider domain itself through
// the fpDomainProbeAt seam so the wedge is still visible. Advisory: it points at
// `ccp fp repair` (the re-register breaks open fds, so it is never run inline from
// the doctor loop). A missing or empty-by-design .claude.json is no verdict.
func reportFPWedges(accts []store.Account, cached []daemon.FPDomainState, daemonAlive bool, report func(string, bool, string)) {
	if daemonAlive {
		for _, w := range cached {
			detail := "domain wedged (serves control ops but hangs reads); the daemon is recovering it — run `ccp fp repair` to re-register it now, then relaunch any sessions on it"
			if w.BreakerTripped {
				detail = "domain wedged; the daemon's automated recovery is exhausted — run `ccp fp repair` (it re-registers the domain), then relaunch any sessions on it; a stuck fileproviderd needs a manual restart (see " + abbreviateHome(pool.LogPath()) + ")"
			}
			report(fmt.Sprintf("acct-%02d file provider", w.ID), false, detail)
		}
		return
	}
	// Daemon down: probe each File Provider domain ourselves (its content bridge
	// is down with the daemon, so a domain with an identity reads not-serving).
	for _, a := range accts {
		if !fileProviderRow(a.OverlayKind) {
			continue
		}
		err := fpDomainProbeAt(a.ConfigDir)
		if err == nil || errors.Is(err, overlay.ErrFPProbeMissing) || errors.Is(err, overlay.ErrFPProbeEmpty) {
			continue
		}
		report(fmt.Sprintf("acct-%02d file provider", a.ID), false,
			"domain not serving reads (probed directly; the daemon is down, so its content bridge is too) — start the daemon (`ccp service install`); if it stays wedged after that, run `ccp fp repair`")
	}
}

// reportContentHealth surfaces the daemon content source's recorded read and
// write-through failures for the computed files (merged .claude.json,
// injected settings.json) served over the holder and File Provider bridges.
// Healthy — or no daemon to ask — is silent: reads degrade to the raw copies
// rather than EIO, so this line only ever adds a failure.
func reportContentHealth(health string, report func(string, bool, string)) {
	if health == "" {
		return
	}
	report("content source", false,
		health+" — reads of the computed files fall back to raw copies; check "+abbreviateHome(pool.LogPath()))
}

// reportCarcasses flags fuse rows whose dir is a mountpoint no longer showing
// ~/.claude. Both seams are bounded (non-blocking cached Getfsstat; an
// unanswered stat folds to NOT-alive) — doctor must never park in the D-state
// it checks for.
func reportCarcasses(accts []store.Account, report func(string, bool, string)) {
	for _, a := range accts {
		if !fuseBackedRow(a.OverlayKind) {
			continue
		}
		if dirMounted(a.ConfigDir) && !mountAliveAt(pool.ClaudeDir(), a.ConfigDir) {
			report(fmt.Sprintf("acct-%02d mount", a.ID), false,
				"dead mount (carcass): ~/.claude is not visible through it; the daemon remounts it automatically — check "+abbreviateHome(pool.MountHolderLogPath()))
		}
	}
}

// reportOrphanedHolder flags a dead holder whose mounts are still in the kernel
// mount table (see ccn doc 1668381), naming the live sessions the operator must
// relaunch before the daemon can reap. A reachable holder is reportHolder's beat.
func reportOrphanedHolder(ctx context.Context, reachable bool, accts []store.Account, report func(string, bool, string)) {
	if reachable {
		return
	}
	orphans := orphanedFuseDirs(accts)
	if len(orphans) == 0 {
		return
	}
	// Advisory: a failed scan just omits the blocker list.
	sessions, _ := scanSessions(ctx)
	detail := fmt.Sprintf("holder dead, %s", plural(len(orphans), "orphaned mount"))
	if blockers := sessionsOnDirs(orphans, sessions); blockers != "" {
		detail += ", waiting on sessions " + blockers + " — relaunch them so the daemon can reap the orphaned go-nfsv4 and remount"
	} else {
		detail += " — the daemon reaps the orphaned go-nfsv4 and remounts automatically once idle"
	}
	report("mount holder orphans", false, detail)
}

// orphanedFuseDirs returns every fuse account dir orphaned by an unreachable
// holder: a still-mounted shared mux root orphans EVERY fuse account (each is a
// bridge symlink into it), and a still-mounted legacy per-dir mount orphans just
// its own. dirMounted is a non-blocking Getfsstat read, so a wedged carcass cannot
// park it.
func orphanedFuseDirs(accts []store.Account) []string {
	muxHeld := dirMounted(pool.MuxRootDir())
	var dirs []string
	for _, a := range accts {
		if !fuseBackedRow(a.OverlayKind) {
			continue
		}
		if muxHeld || dirMounted(a.ConfigDir) {
			dirs = append(dirs, a.ConfigDir)
		}
	}
	return dirs
}

// sessionsOnDirs formats the live claude sessions bound to any of dirs as
// "pid N, pid M", or "" if none.
func sessionsOnDirs(dirs []string, sessions []procscan.Session) string {
	want := make(map[string]bool, len(dirs))
	for _, d := range dirs {
		want[d] = true
	}
	var pids []string
	for _, se := range sessions {
		if want[se.ConfigDir] {
			pids = append(pids, fmt.Sprintf("pid %d", se.PID))
		}
	}
	return strings.Join(pids, ", ")
}

func checkAccount(cmd *cobra.Command, m *pool.Manager, a store.Account, fix bool, report func(string, bool, string)) {
	prefix := fmt.Sprintf("acct-%02d", a.ID)

	checkCredential(m, a, fix, report)

	// NeedsLogin: the Keychain item can be readable yet useless.
	if h, herr := m.Store.GetAuthHealth(a.ID); herr == nil && h.NeedsLogin {
		report(prefix+" auth", false, fmt.Sprintf("needs re-login — run `ccp login %d`", a.ID))
	}

	backend, perr := fkoverlay.Parse(a.OverlayKind)
	if perr != nil {
		report(prefix+" overlay", false, perr.Error())
		return
	}
	prov, perr := pool.OverlayProviderFor(backend)
	if perr != nil {
		report(prefix+" overlay", false, perr.Error())
		return
	}
	if err := prov.Health(pool.ClaudeDir(), a.ConfigDir); err != nil {
		if fix {
			if serr := prov.Sync(pool.ClaudeDir(), a.ConfigDir); serr == nil {
				report(prefix+" overlay", true, "re-asserted")
			} else {
				report(prefix+" overlay", false, serr.Error())
			}
		} else {
			report(prefix+" overlay", false, err.Error())
		}
	} else {
		report(prefix+" overlay", true, string(prov.Backend()))
	}

	checkFuseFallback(m, a, report)
	checkFileProviderFallback(m, a, report)
	checkStrandedPrivate(m, a, fix, cmd.OutOrStdout(), report)
}

// checkCredential reports one account's credential state, each backend probed
// through the Manager seam in runtime resolution order (Keychain first, then
// the plaintext file).
func checkCredential(m *pool.Manager, a store.Account, fix bool, report func(string, bool, string)) {
	prefix := fmt.Sprintf("acct-%02d", a.ID)
	keychain := m.Creds.Store(a, creds.SourceKeychain)
	file := m.Creds.Store(a, creds.SourceFile)
	_, kerr := keychain.Read()
	_, ferr := file.Read()

	switch {
	case kerr == nil:
		if errors.Is(ferr, creds.ErrNotFound) {
			report(prefix+" credential", true, "keychain")
			return
		}
		// Drift: both backends hold a credential (refresh tokens are single-use,
		// so copies diverge). Advisory even under --fix — only `ccp cred move`
		// gates against live sessions and keeps the fresher copy.
		report(prefix+" credential", false, "credential in BOTH the Keychain and .credentials.json — copies diverge (Claude refresh tokens are single-use); consolidate with `ccp cred move --to keychain` (moves the fresher copy; the daemon gates it against live sessions)")
	case !errors.Is(kerr, creds.ErrNotFound) && !errors.Is(kerr, creds.ErrUnavailable):
		if !fix {
			report(prefix+" keychain", false, kerr.Error())
			return
		}
		// Item exists but our ACL can't read it (-w denied): read then write back
		// through our security(1) to re-assert ownership, as FinalizeAdd does.
		cred, rerr := keychain.Read()
		if rerr == nil {
			rerr = keychain.Write(cred)
		}
		if rerr != nil {
			report(prefix+" keychain", false, rerr.Error())
			return
		}
		report(prefix+" keychain", true, "re-asserted")
	case ferr == nil:
		report(prefix+" credential", true, "file")
	case !errors.Is(ferr, creds.ErrNotFound):
		report(prefix+" credential", false, ferr.Error())
	case errors.Is(kerr, creds.ErrUnavailable):
		detail := "keychain unreachable in this session (login keychain not in the security search list) — credential state unknown; run doctor from a GUI session, or move the account to the file backend: `ccp cred move --to file`"
		if fix {
			detail += "; nothing for --fix to do locally"
		}
		report(prefix+" credential", false, detail)
	default:
		report(prefix+" credential", false, fmt.Sprintf("no credential in either backend — run `ccp login %d`", a.ID))
	}
}

// checkFuseFallback flags a symlink account under a fuse pool default — the
// automatic fuse→symlink fallback is permanent until `ccp migrate` re-promotes.
func checkFuseFallback(m *pool.Manager, a store.Account, report func(string, bool, string)) {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil || backend != fkoverlay.BackendSymlink {
		return
	}
	canHostFuse := pool.CanHostFuse()
	if m.CanHostFuse != nil {
		canHostFuse = m.CanHostFuse()
	}
	if !canHostFuse {
		return
	}
	def, ok, err := m.ConfiguredOverlayKind()
	if err != nil || !ok || !def.IsFuse() {
		return
	}
	report(fmt.Sprintf("acct-%02d fuse fallback", a.ID), false,
		"on symlink but the pool default is fuse — likely an automatic fallback after the mount holder failed; re-run `ccp migrate` once fuse-t is healthy")
}

// checkFileProviderFallback is checkFuseFallback's sibling for the permanent
// fileprovider→symlink retreat.
func checkFileProviderFallback(m *pool.Manager, a store.Account, report func(string, bool, string)) {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil || backend == fkoverlay.BackendFileProvider {
		return
	}
	if !fpAvailable(m.OverlaySpec()) {
		return
	}
	def, ok, derr := m.ConfiguredOverlayKind()
	if derr != nil || !ok || def != fkoverlay.BackendFileProvider {
		return
	}
	report(fmt.Sprintf("acct-%02d fileprovider fallback", a.ID), false,
		fmt.Sprintf("on %s but the pool default is fileprovider — re-run `ccp migrate --to fileprovider` once the account is idle", backend))
}

// checkStrandedPrivate reports (and under --fix, heals) private files stranded
// by an interrupted migration. Only symlink rows can strand (HealStrandedPrivate's
// fence, mirrored here); the heal is synchronous, so the ResolvedConflictLogf swap is safe.
func checkStrandedPrivate(m *pool.Manager, a store.Account, fix bool, out io.Writer, report func(string, bool, string)) {
	if fuseBackedRow(a.OverlayKind) || fileProviderRow(a.OverlayKind) {
		return
	}
	prefix := fmt.Sprintf("acct-%02d", a.ID)
	priv := fkoverlay.FusePrivateRoot(a.ConfigDir)
	has, herr := fkoverlay.HasPrivateEntries(priv, m.OverlaySpec())
	switch {
	case herr != nil:
		report(prefix+" private files", false, herr.Error())
		return
	case !has:
		return
	case !fix:
		report(prefix+" private files", false, "stranded in "+priv+" by an interrupted migration")
		return
	}
	// A CLI-side heal cannot see the daemon's converting claim; racing an
	// in-flight conversion would move files under its teardown.
	if daemon.NewClient().Available() {
		report(prefix+" private files", false, "stranded in "+priv+"; the daemon is running — re-run `ccp migrate`, or stop the daemon and re-run doctor --fix")
		return
	}
	prev := fkoverlay.ResolvedConflictLogf
	fkoverlay.ResolvedConflictLogf = func(format string, args ...any) { warn(out, format, args...) }
	healed, ferr := m.HealStrandedPrivate(a)
	fkoverlay.ResolvedConflictLogf = prev
	switch {
	case ferr != nil:
		report(prefix+" private files", false, ferr.Error())
	case healed:
		report(prefix+" private files", true, "restored from "+priv)
	}
}
