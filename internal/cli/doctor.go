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

				// claude binary present (auto-update can move it).
				if _, err := exec.LookPath("claude"); err != nil {
					report("claude on PATH", false, err.Error())
				} else {
					report("claude on PATH", true, "")
				}

				// Daemon.
				var cachedHolder *daemon.HolderStatus
				if resp, err := daemon.NewClient().Health(); err == nil && resp.OK {
					report("daemon", true, resp.Version)
					// The holder's pending-TCC guidance lives in the daemon's
					// cache (the heal loop records it on a blocked mount); fetch
					// it while the daemon is up.
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

				// Mount holder: reachability vs. the fuse rows that need one, the
				// cached pending-TCC guidance, and a kernel-truth carcass check
				// per fuse row.
				reachable, holderVer := probeHolder()
				reportHolder(holderFacts{
					reachable: reachable,
					version:   holderVer,
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
				var sessions []procscan.Session
				if len(holderMounts) > 0 {
					// A failed scan skips the session-aware checks silently: they
					// are advisory, and doctor degrades rather than aborts. Shared
					// by both the wedge and stale-session checks.
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

				// --open-settings jumps straight to the fuse backend's enablement
				// pane when the holder is TCC-blocked. The pane and the deep links
				// come from fusekit (Backend.OpenSettings), never a cc-pool literal.
				// Best-effort: a failure to launch it is a warning, never a command
				// failure — the detail line above already carries the copy-pasteable
				// deep link.
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

// fuseGrantHint renders the open-Settings hint for a fuse backend's one-time
// macOS grant, sourced entirely from fusekit (backend.Enablement) — cc-pool
// holds no pane literal of its own. It names the pane and offers the first deep
// link as a copy-pasteable `open` command. Production callers pass the daemon's
// TCCBlockedBackend; taking the backend as a parameter keeps cc-pool a blind
// consumer (a test can hand it any backend and watch the pane flip). A backend
// that needs no grant yields a bare "grant the required macOS permission".
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

// holderFacts is reportHolder's gathered input: holder reachability and
// version from a direct socket dial, plus the daemon's cached holder view
// (nil when the daemon is down or predates the field).
type holderFacts struct {
	reachable bool
	version   string
	cached    *daemon.HolderStatus
}

// probeHolder dials the mount-holder socket directly for reachability and
// version. A socket that accepts but fails the health probe counts as
// unreachable — a held-but-unresponsive socket serves nothing.
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

// reportHolder renders the shared-holder doctor checks from pre-gathered facts.
// An unreachable holder is only a failure when fuse rows need one (the daemon's
// heal loop lazily respawns the cask holder; the check catches that not
// happening). The holder is a separate, multi-tenant product, so its version is
// just reported (no skew comparison against cc-pool's own version), and a holder
// serving no cc-pool mounts is not an "orphan" — another tenant may use it.
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

// listHolderMounts fetches the holder's mount registry over its socket. nil
// on any failure: the wedge and stale-session checks read missing rows as "no
// facts" and report nothing — reportHolder already covers an unreachable
// holder.
func listHolderMounts() []mountd.MountInfo {
	mounts, err := holderClient().List()
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
			// A wedged mirror backing live sessions is LEFT mounted — the daemon
			// never force-unmounts a busy mirror (it would panic the kernel), so
			// the only fix is to relaunch those sessions. An idle wedge the daemon
			// remounts on its own next tick.
			detail := "wedged (serves metadata but hangs reads) — the daemon will remount it"
			if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
				detail = fmt.Sprintf("wedged (serves metadata but hangs reads); left mounted under %d live session(s); relaunch them — the daemon will NOT force-unmount a busy mirror (would panic the kernel)", n)
			}
			report(fmt.Sprintf("acct-%02d mirror", a.ID), false, detail)
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

// reportCarcasses flags dead mounts on fuse rows: the dir is a mountpoint but
// ~/.claude is not visible through it — a carcass left by a holder that died.
// The daemon's heal loop remounts these within its tick; doctor seeing one
// means that has not happened yet (or the daemon is down). The mountpoint seam
// (dirMounted) is now overlay.Mounted — a non-blocking cached Getfsstat read
// that cannot park, with no still-mounted fold; only the aliveness seam
// (mountAliveAt) is a bounded stat-probe whose unanswered read folds to
// NOT-alive. A carcass-suspect that cannot answer a 2s aliveness stat is
// exactly what this check exists to flag, so the row is reported within that
// single bound instead of parking doctor in the very uninterruptible sleep it
// is checking for.
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

// checkFuseFallback surfaces an account on the symlink overlay while the pool's
// recorded default is fuse and this machine can host fuse — almost always the
// aftermath of an automatic fuse→symlink fallback (a mount never recovered or
// fuse-t went unavailable). That fallback is permanent until re-promoted, so
// doctor is where the user learns to undo it: `ccp migrate` converts symlink
// accounts back to fuse once fuse-t is healthy. The fuse-hosting check honors
// the Manager's CanHostFuse seam so it is exercisable without a fuse build.
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
	// Only heal when no daemon holds the pool: a CLI-side heal cannot see the
	// daemon's converting claim, and racing an in-flight conversion would move
	// files under its teardown sequence. With a daemon up, the same recovery
	// runs under the claim via `ccp migrate` (or the daemon's own startup
	// reconcile).
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
