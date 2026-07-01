package cli

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
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
				if resp, err := daemon.NewClient().Health(); err == nil && resp.OK {
					report("daemon", true, resp.Version)
					// Pending-TCC guidance lives only in the daemon's cache.
					if sresp, serr := daemon.NewClient().Status(); serr == nil && sresp.OK {
						cachedHolder = sresp.Holder
					}
				} else {
					report("daemon", false, "not running; run `ccp service install`")
				}

				accts, err := m.Store.ListAccounts()
				if err != nil {
					return err
				}

				reachable, holderVer := probeHolder()
				reportHolder(holderFacts{
					reachable: reachable,
					version:   holderVer,
					cached:    cachedHolder,
				}, countFuse(accts), report)
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

// The holder is a separate multi-tenant product: no version-skew check, and
// serving no cc-pool mounts is not an orphan.
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

func checkAccount(cmd *cobra.Command, m *pool.Manager, a store.Account, fix bool, report func(string, bool, string)) {
	prefix := fmt.Sprintf("acct-%02d", a.ID)

	_, kerr := keychain.Read(a.KeychainService, a.KeychainAccount)
	switch {
	case kerr == nil:
		report(prefix+" keychain", true, "")
	case errors.Is(kerr, keychain.ErrNotFound):
		// Keychain unavailable (e.g. headless): claude writes plaintext .credentials.json.
		if _, ferr := keychain.ReadFileCredential(a.ConfigDir); ferr == nil {
			report(prefix+" credential", true, "file")
		} else {
			report(prefix+" credential", false, kerr.Error())
		}
	case fix:
		// Item exists but our ACL can't read it (-w denied).
		if _, rerr := keychain.Reassert(a.KeychainService, a.KeychainAccount); rerr == nil {
			report(prefix+" keychain", true, "re-asserted")
		} else {
			report(prefix+" keychain", false, rerr.Error())
		}
	default:
		report(prefix+" keychain", false, kerr.Error())
	}

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
	checkStrandedPrivate(m, a, fix, cmd.OutOrStdout(), report)
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

// checkStrandedPrivate reports (and under --fix, heals) private files stranded
// by an interrupted migration. The heal is a single synchronous call, so the
// ResolvedConflictLogf global swap is safe.
func checkStrandedPrivate(m *pool.Manager, a store.Account, fix bool, out io.Writer, report func(string, bool, string)) {
	if fuseBackedRow(a.OverlayKind) {
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
