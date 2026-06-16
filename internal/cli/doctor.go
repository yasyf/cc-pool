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
	"github.com/yasyf/cc-pool/internal/mountd"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
)

func newDoctorCmd() *cobra.Command {
	var fix bool
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
						fmt.Fprintf(out, "%s %s: %s\n", mark, label, detail)
					} else {
						fmt.Fprintf(out, "%s %s\n", mark, label)
					}
				}

				// claude binary present (auto-update can move it).
				if _, err := exec.LookPath("claude"); err != nil {
					report("claude on PATH", false, err.Error())
				} else {
					report("claude on PATH", true, "")
				}

				// Daemon.
				daemonUp := false
				var cachedHolder *daemon.HolderStatus
				if resp, err := daemon.NewClient().Health(); err == nil && resp.OK {
					report("daemon", true, resp.Version)
					daemonUp = true
					// The holder's TCC/spawn failures live only in the
					// daemon's cache (the daemon owns spawning); fetch them
					// while it is up.
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

				// Mount holder: reachability vs. the fuse rows that need one,
				// orphan/skew notes, cached TCC/spawn failures, and a
				// kernel-truth carcass check per fuse row.
				reachable, holderVer := probeHolder()
				reportHolder(holderFacts{
					reachable: reachable,
					version:   holderVer,
					daemonUp:  daemonUp,
					cached:    cachedHolder,
				}, countFuse(accts), report)
				reportCarcasses(accts, report)

				// The wedge and stale-session checks read the holder's List
				// rows directly over its socket — the same direct dial
				// probeHolder uses, because these are holder facts and the
				// holder outlives daemons; the daemon cache above carries
				// only daemon-owned state (TCC/spawn failures). An
				// unreachable holder (or a failed List) yields nil rows and
				// both checks stay quiet, matching the other holder checks'
				// degrade.
				var holderMounts []mountd.MountInfo
				if reachable {
					holderMounts = listHolderMounts()
				}
				reportWedges(accts, holderMounts, report)
				var sessions []procscan.Session
				if len(holderMounts) > 0 {
					// A failed scan skips the stale check silently: it is
					// advisory, and doctor degrades rather than aborts.
					sessions, _ = scanSessions(cmd.Context())
				}
				reportStaleSessions(accts, holderMounts, sessions, report)

				for _, a := range accts {
					checkAccount(cmd, m, a, fix, report)
				}

				if !ok {
					if fix {
						fmt.Fprintln(out, "\nApplied fixes where possible; re-run `ccp doctor` to confirm.")
					} else {
						fmt.Fprintln(out, "\nIssues found. Re-run with --fix to repair.")
					}
				} else {
					fmt.Fprintln(out, "\nAll checks passed.")
				}
				return nil
			})
		},
	}
	cmd.Flags().BoolVar(&fix, "fix", false, "attempt to repair detected drift")
	return cmd
}

// holderFacts is reportHolder's gathered input: holder reachability and
// version from a direct socket dial, plus the daemon's cached holder view
// (nil when the daemon is down or predates the field).
type holderFacts struct {
	reachable bool
	version   string
	daemonUp  bool
	cached    *daemon.HolderStatus
}

// probeHolder dials the mount-holder socket directly for reachability and
// version. A socket that accepts but fails the health probe counts as
// unreachable — a held-but-unresponsive socket serves nothing.
func probeHolder() (reachable bool, ver string) {
	cl := mountd.NewClient(pool.MountsSocketPath())
	if !cl.Available() {
		return false, ""
	}
	v, err := cl.Health()
	if err != nil {
		return false, ""
	}
	return true, v
}

// reportHolder renders the mount-holder doctor checks from pre-gathered
// facts. An unreachable holder is only a failure when fuse rows need one (the
// daemon respawns it; the check catches respawn not happening). A holder with
// nothing to serve and no daemon to retire it is flagged as an orphan; a
// version-skewed holder is a note, not a failure — the daemon replaces it
// once the pool is idle.
func reportHolder(f holderFacts, fuseRows int, report func(string, bool, string)) {
	switch {
	case !f.reachable && fuseRows > 0:
		report("mount holder", false, fmt.Sprintf("not running with %s; the daemon respawns it automatically — check %s",
			plural(fuseRows, "fuse account"), abbreviateHome(pool.MountHolderLogPath())))
	case f.reachable && fuseRows == 0 && !f.daemonUp:
		report("mount holder", true, fmt.Sprintf("orphan (%s) running with no fuse accounts; `ccp service uninstall` stops it", f.version))
	case f.reachable && f.version != version.String():
		report("mount holder", true, fmt.Sprintf("%s (version skew; the daemon replaces it when the pool is idle)", f.version))
	case f.reachable:
		report("mount holder", true, f.version)
	}
	if f.cached == nil {
		return
	}
	if f.cached.TCCError != "" {
		report("mount holder TCC", false, f.cached.TCCError)
	}
	if f.cached.SpawnError != "" {
		report("mount holder spawn", false, f.cached.SpawnError)
	}
}

// listHolderMounts fetches the holder's mount registry over its socket. nil
// on any failure: the wedge and stale-session checks read missing rows as "no
// facts" and report nothing — reportHolder already covers an unreachable
// holder.
func listHolderMounts() []mountd.MountInfo {
	mounts, err := mountd.NewClient(pool.MountsSocketPath()).List()
	if err != nil {
		return nil
	}
	return mounts
}

// reportWedges flags partial-wedge mirrors on fuse rows: mounts that answer
// shallow metadata stats but hang bulk reads. The holder ships no deep
// verdict (the daemon owns the probe), so doctor runs its OWN bounded local
// deep probe of every fuse row the holder still reports Live — independent of
// the daemon, so it surfaces a wedge even when the daemon is down (a key
// doctor case the daemon's cached count cannot cover). ErrProbeMissing is no
// verdict (a pre-upgrade holder's mount predating the probe file), never a
// wedge. nil mounts (holder unreachable) reports nothing. deepProbeAt is
// bounded by design, like reportCarcasses' stat seams: nothing doctor runs
// against a possibly wedged mirror may block unboundedly.
func reportWedges(accts []store.Account, mounts []mountd.MountInfo, report func(string, bool, string)) {
	byDir := make(map[string]mountd.MountInfo, len(mounts))
	for _, mi := range mounts {
		byDir[mi.Dir] = mi
	}
	for _, a := range accts {
		if overlay.Kind(a.OverlayKind) != overlay.KindFuse {
			continue
		}
		mi, held := byDir[a.ConfigDir]
		if !held || !mi.Live {
			continue
		}
		if err := deepProbeAt(a.ConfigDir); err != nil && !errors.Is(err, overlay.ErrProbeMissing) {
			report(fmt.Sprintf("acct-%02d mirror", a.ID), false,
				"wedged (serves metadata but hangs reads) — daemon will remount; relaunch its sessions")
		}
	}
}

// staleSessionSlack absorbs the second-granularity rounding on both sides of
// the stale-session comparison (ps etime and the holder's MountedAt stamp)
// plus scan-vs-List clock jitter. Strictly MORE than the slack flags; exactly
// the slack does not.
const staleSessionSlack = 5 * time.Second

// reportStaleSessions flags live sessions bound to a mount that no longer
// exists: a session on a fuse row whose start predates the holder's current
// mount of that dir (by more than staleSessionSlack) was launched against a
// previous mount epoch — its open fds point at the yanked mirror and every
// read hangs or fails until relaunch. A zero MountedAt (a holder predating
// the field) or a zero StartedAt (unparseable ps etime) is silently skipped:
// no fact, no flag.
func reportStaleSessions(accts []store.Account, mounts []mountd.MountInfo, sessions []procscan.Session, report func(string, bool, string)) {
	mountedAt := make(map[string]time.Time, len(mounts))
	for _, mi := range mounts {
		if mi.MountedAt != 0 {
			mountedAt[mi.Dir] = time.Unix(mi.MountedAt, 0)
		}
	}
	for _, a := range accts {
		if overlay.Kind(a.OverlayKind) != overlay.KindFuse {
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

// reportCarcasses flags dead mounts on fuse rows: the dir is a mountpoint but
// ~/.claude is not visible through it — a carcass left by a holder that died.
// The daemon's supervision remounts these within its tick; doctor seeing one
// means that has not happened yet (or the daemon is down). Both seams are
// bounded (overlay's stat-probe harness): on a wedged mirror an unanswered
// mountpoint stat reads still-mounted and an unanswered aliveness stat reads
// dead — a carcass-suspect that cannot answer a 2s stat is exactly what this
// check exists to flag — so the row is reported within the bound instead of
// parking doctor in the very uninterruptible sleep it is checking for.
func reportCarcasses(accts []store.Account, report func(string, bool, string)) {
	for _, a := range accts {
		if overlay.Kind(a.OverlayKind) != overlay.KindFuse {
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

	// Credential usable: the Keychain item (readable under our ACL) or, when the
	// Keychain is unavailable (e.g. headless), claude's plaintext
	// .credentials.json fallback.
	_, kerr := keychain.Read(a.KeychainService, a.KeychainAccount)
	switch {
	case kerr == nil:
		report(prefix+" keychain", true, "")
	case errors.Is(kerr, keychain.ErrNotFound):
		// No Keychain item; accept the plaintext file backend if claude wrote one.
		if _, ferr := keychain.ReadFileCredential(a.ConfigDir); ferr == nil {
			report(prefix+" credential", true, "file")
		} else {
			report(prefix+" credential", false, kerr.Error())
		}
	case fix:
		// Item exists but our ACL can't read it (-w denied): re-assert ownership.
		if _, rerr := keychain.Reassert(a.KeychainService, a.KeychainAccount); rerr == nil {
			report(prefix+" keychain", true, "re-asserted")
		} else {
			report(prefix+" keychain", false, rerr.Error())
		}
	default:
		report(prefix+" keychain", false, kerr.Error())
	}

	// Auth health: the daemon flags an account when its refresh token is gone or
	// revoked, so its Keychain item may be present and readable yet useless.
	if h, herr := m.Store.GetAuthHealth(a.ID); herr == nil && h.NeedsLogin {
		report(prefix+" auth", false, fmt.Sprintf("needs re-login — run `ccp login %d`", a.ID))
	}

	// Overlay health.
	prov := pool.OverlayProviderFor(overlay.Kind(a.OverlayKind))
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
		report(prefix+" overlay", true, string(prov.Kind()))
	}

	checkStrandedPrivate(m, a, fix, cmd.OutOrStdout(), report)
}

// checkStrandedPrivate reports — and, under --fix with no running daemon, heals
// — private files stranded in a fuse backing dir by an interrupted migration: a
// non-fuse account must hold its .claude.json (and friends) in the account dir
// itself. When this CLI process drives the heal it wires
// overlay.ResolvedConflictLogf at out for the duration of the move, so a
// last-write-wins discard of a duplicate stranded copy is reported rather than
// lost silently. The daemon sets the same seam at startup, but a daemon-less
// `doctor --fix` is its own actor and must surface the recovery itself; the
// wiring is scoped to the single synchronous heal call (this process runs no
// concurrent move) and restored after, so it never leaks across commands.
func checkStrandedPrivate(m *pool.Manager, a store.Account, fix bool, out io.Writer, report func(string, bool, string)) {
	if overlay.Kind(a.OverlayKind) == overlay.KindFuse {
		return
	}
	prefix := fmt.Sprintf("acct-%02d", a.ID)
	priv := overlay.FusePrivateRoot(a.ConfigDir)
	has, herr := overlay.HasPrivateEntries(priv)
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
	// Only heal when no daemon holds the pool: a CLI-side heal cannot see the
	// daemon's converting claim, and racing an in-flight conversion would move
	// files under its teardown sequence. With a daemon up, the same recovery
	// runs under the claim via `ccp migrate` (or the daemon's own startup
	// reconcile).
	if daemon.NewClient().Available() {
		report(prefix+" private files", false, "stranded in "+priv+"; the daemon is running — re-run `ccp migrate`, or stop the daemon and re-run doctor --fix")
		return
	}
	prev := overlay.ResolvedConflictLogf
	overlay.ResolvedConflictLogf = func(format string, args ...any) { warn(out, format, args...) }
	healed, ferr := m.HealStrandedPrivate(a)
	overlay.ResolvedConflictLogf = prev
	switch {
	case ferr != nil:
		report(prefix+" private files", false, ferr.Error())
	case healed:
		report(prefix+" private files", true, "restored from "+priv)
	}
}
