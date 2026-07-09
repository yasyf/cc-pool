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
	"github.com/yasyf/fusekit/fileproviderd"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

func newDoctorCmd() *cobra.Command {
	var fix bool
	var openSettings bool
	var fpRawProbe bool
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
				// Warnings render in the report column but never flip the verdict.
				reportWarn := func(label, detail string) {
					_, _ = fmt.Fprintf(out, "%s %s: %s\n", warnStyle.Render("!"), label, detail)
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
				var fpBridgeUp *bool
				var ledgers []daemon.LedgerState
				var daemonAlive bool
				if resp, err := daemon.NewClient().Health(); err == nil && resp.OK {
					daemonAlive = true
					if resp.Version == version.String() {
						report("daemon", true, resp.Version)
					} else {
						report("daemon", false, fmt.Sprintf("running %s but the cli is %s — the upgraded daemon hasn't taken over: `brew services restart cc-pool`", resp.Version, version.String()))
					}
					// Pending-TCC guidance, bridge liveness, content-source health,
					// and the self-heal ledger block live only in the daemon's cache.
					if sresp, serr := daemon.NewClient().Status(); serr == nil && sresp.OK {
						cachedHolder = sresp.Holder
						contentHealth = sresp.ContentHealth
						fpConsentPending = sresp.FPConsentPending
						fpBridgeUp = sresp.FPBridgeUp
						ledgers = sresp.Ledgers
					}
				} else {
					report("daemon", false, "not running; run `ccp service install`")
					down := false
					fpBridgeUp = &down // a dead daemon's bridge is definitively down, not unreported
				}

				accts, err := m.Store.ListAccounts()
				if err != nil {
					return err
				}

				reachable, holderVer := probeHolder()
				facts := holderFacts{
					reachable: reachable,
					version:   holderVer,
				}
				reportHolder(facts, countFuse(accts), report)
				reportHolderMitigations(facts, report)
				reportCarcasses(accts, report)
				reportLedgers(accts, ledgers, cachedHolder, time.Now(), report)

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
				reportWedges(accts, holderMounts, sessions, daemonAlive, report)
				reportStaleSessions(accts, holderMounts, sessions, report)
				reportStaleWidgetAppex(cmd.Context(), report)
				reportOrphanedHolder(cmd.Context(), reachable, accts, report)

				reportFileProvider(cmd.Context(), m, accts, fpConsentPending, fpBridgeUp, report)
				reportFPWedges(accts, daemonAlive, fpRawProbe, report)
				reportOrphanFPDomains(cmd.Context(), m, accts, fix, report)
				reportContentHealth(contentHealth, report)

				if err := reportSync(cmd.Context(), m, accts, report, reportWarn); err != nil {
					return err
				}

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
	cmd.Flags().BoolVar(&fpRawProbe, "fp-raw-probe", false, "probe File Provider domains with a direct filesystem read instead of the app control op (may trigger a macOS permission prompt)")
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

// reportLedgers renders the daemon's composed self-heal ledger block — the one
// daemon-up observability surface over both ledger stores (with the daemon down
// the block is empty and the live-probe readers reportWedges/reportFPWedges
// carry the diagnosis instead). Healthy rows are silent; only latched faults,
// tripped breakers, and active backoffs report. Two sources deliberately stay
// outside it: the store-persisted needs-login verdict (checkAccount — the
// auth.streak row here is the daemon's transient-401 streak, a different
// source) and the holder-version mitigation floor (reportHolderMitigations —
// holder version, not ledger state). The TCC grant walkthrough is holder-cache
// guidance the ledger's alt lane only counts waits for, so it renders from
// holder, verbatim, ahead of the per-row lines.
func reportLedgers(accts []store.Account, ledgers []daemon.LedgerState, holder *daemon.HolderStatus, now time.Time, report func(string, bool, string)) {
	if holder != nil && holder.TCCError != "" {
		report("mount holder grant", false, holder.TCCError+" — "+fuseGrantHint(holder.TCCBlockedBackend)+" (cc-pool falls back to symlink automatically if the grant never lands)")
	}
	byDir := make(map[string]store.Account, len(accts))
	for _, a := range accts {
		byDir[a.ConfigDir] = a
	}
	label := func(l daemon.LedgerState, kind string) string {
		if a, ok := byDir[l.Resource]; ok {
			return fmt.Sprintf("acct-%02d %s", a.ID, kind)
		}
		if l.Resource == "pool" {
			return "pool " + kind
		}
		return abbreviateHome(l.Resource) + " " + kind
	}
	for _, l := range ledgers {
		switch l.Policy {
		case "fuse.deepwedge":
			if !l.Faulted {
				continue
			}
			report(label(l, "mirror"), false, "wedged (serves metadata but hangs reads) — the daemon will remount it; relaunch any sessions on it")
		case "fuse.shallowdead":
			if !l.Faulted {
				continue
			}
			report(label(l, "mirror"), false, "dead mirror (fails reads outright; unmounted out of band or its fuse worker died?) — the daemon will remount it")
		case "fuse.remount":
			// A parked row means the breaker tripped but the retreat was DEFERRED
			// (live sessions / a held claim) — the escalation clears the row when
			// the retreat actually fires, so a visible parked row is always pending.
			switch {
			case l.Parked && l.AltHits > 0:
				report(label(l, "remount"), false, fmt.Sprintf("TCC breaker tripped after %d blocked mounts; retreat to symlink pending (deferred by live sessions or a held claim) — the daemon re-fires it on the next elapsed backoff window; granting the permission first keeps the account on fuse", l.AltHits))
			case l.Parked:
				report(label(l, "remount"), false, fmt.Sprintf("remount breaker tripped after %s; retreat to symlink pending (deferred by live sessions or a held claim) — the daemon re-fires it on the next elapsed backoff window", plural(l.Strikes, "failed attempt")))
			case l.AltHits > 0:
				report(label(l, "remount"), false, fmt.Sprintf("mount blocked pending the macOS grant (%s so far) — see the mount holder grant line", plural(l.AltHits, "wait")))
			case l.Strikes > 0:
				detail := fmt.Sprintf("remount failing (%s so far); falls back to symlink if the breaker trips", plural(l.Strikes, "attempt"))
				if l.LastErr != "" {
					detail += " — last error: " + l.LastErr
				}
				report(label(l, "remount"), false, detail)
			}
			// Both lanes clear: a benign deferral (busy mirror, or a holder
			// awaiting its cask upgrade — reportHolderMitigations owns that
			// guidance), so the row stays silent here.
		case "fp.domain":
			switch {
			case l.Faulted:
				detail := "domain wedged (serves control ops but hangs reads); the daemon is recovering it — run `ccp fp repair` to re-register it now, then relaunch any sessions on it"
				if l.Parked {
					id := byDir[l.Resource].ID
					detail = fmt.Sprintf("domain parked (wedged): the daemon's automated recovery is exhausted — run `ccp fp repair --account %d` to re-register it (or `ccp fp repair --retreat --account %d` to fall back to symlink), then relaunch any sessions on it; a stuck fileproviderd needs a manual restart (see %s)", id, id, abbreviateHome(pool.LogPath()))
				}
				report(label(l, "file provider"), false, detail)
			case l.Parked:
				// Parked without ever faulting: the Missing control-plane heal (a domain
				// deregistered externally or gone uncontrollable) exhausts its recovery
				// attempts without the data plane ever wedging. ledgerFooter counts it
				// parked, so it needs a doctor explanation too — every banner state does.
				id := byDir[l.Resource].ID
				report(label(l, "file provider"), false, fmt.Sprintf("domain parked: the daemon's automated recovery is exhausted (the domain was deregistered or its extension is uncontrollable) — run `ccp fp repair --account %d` to re-register it (or `ccp fp repair --retreat --account %d` to fall back to symlink), then relaunch any sessions on it; a stuck fileproviderd needs a manual restart (see %s)", id, id, abbreviateHome(pool.LogPath())))
			}
		case "auth.streak":
			if !l.Faulted {
				continue
			}
			detail := "polling keeps hitting auth failures — needs re-login"
			if a, ok := byDir[l.Resource]; ok {
				detail += fmt.Sprintf(": run `ccp login %d`", a.ID)
			}
			if l.LastErr != "" {
				detail += " (last error: " + l.LastErr + ")"
			}
			report(label(l, "auth"), false, detail)
		case "ratelimit.acct", "ratelimit.pool":
			if !l.NextDue.After(now) {
				continue
			}
			report(label(l, "rate limit"), false, fmt.Sprintf("429 backoff active: polling paused for another %s (%s so far)", l.NextDue.Sub(now).Round(time.Second), plural(l.Attempts, "hit")))
		}
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

// reportWedges deep-probes Live fuse rows itself — the daemon-DOWN diagnosis
// (with the daemon up, reportLedgers renders its cached wedge verdicts instead;
// probing here too would double-report the same mirror). ErrProbeMissing is no
// verdict, not a wedge. Nothing run against a possibly wedged mirror may block
// unboundedly.
func reportWedges(accts []store.Account, mounts []mountd.MountInfo, sessions []procscan.Session, daemonAlive bool, report func(string, bool, string)) {
	if daemonAlive {
		return
	}
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

// staleWidgetAppexes is a seam over the shared detector so doctor tests feed
// canned appexes without live processes.
var staleWidgetAppexes = daemon.StaleWidgetAppexes

// reportStaleWidgetAppex flags a Notification Center widget appex left running
// a binary a cask upgrade replaced — its render is frozen until the process
// dies. Advisory: the daemon reaps it on its next poll, and a failed scan
// degrades silently like the other proc-scan checks.
func reportStaleWidgetAppex(ctx context.Context, report func(string, bool, string)) {
	stale, err := staleWidgetAppexes(ctx, pool.WidgetAppexBinaryPath())
	if err != nil {
		return
	}
	for _, p := range stale {
		report("widget appex", false, fmt.Sprintf(
			"pid %d (started %s) is running a widget binary an upgrade replaced; its render is frozen — the daemon kills it on its next poll, or now: kill -9 %d",
			p.PID, p.StartedAt.Format("15:04:05"), p.PID))
	}
}

// fpControlHealth probes the CCPoolStatus companion app's control socket for
// its version — a seam so doctor tests fake the socket.
var fpControlHealth = func(ctx context.Context) (string, error) {
	return fileproviderd.NewAppClient(pool.FPControlSocketPath()).Health(ctx)
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
func reportFileProvider(ctx context.Context, m *pool.Manager, accts []store.Account, consentPending bool, bridgeUp *bool, report func(string, bool, string)) {
	for _, f := range fpDiagnose(ctx, m.OverlaySpec(), countFileProvider(accts), consentPending, bridgeUp) {
		report(f.label, f.healthy, f.detail)
	}
}

// reportFPWedges surfaces wedged File Provider domains — control ops answer but
// reads hang, the wedge the control-plane health check misses. The daemon-DOWN
// diagnosis: it probes each File Provider domain itself through the
// fpDomainProbeAt seam so the wedge is still visible (with the daemon up,
// reportLedgers renders the daemon's authoritative fp.domain verdicts instead —
// it is actively recovering them). Advisory: it points at `ccp fp repair` (the
// re-register breaks open fds, so it is never run inline from the doctor loop).
// A missing or empty-by-design .claude.json is no verdict.
func reportFPWedges(accts []store.Account, daemonAlive, rawProbe bool, report func(string, bool, string)) {
	if daemonAlive {
		return
	}
	// Probe each File Provider domain ourselves. By default this is the app
	// control op (never a materializing read); --fp-raw-probe swaps in a direct
	// filesystem read that can trip a per-account macOS permission prompt.
	probe := fpDomainProbeAt
	if rawProbe {
		probe = fpRawProbeAt
	}
	for _, a := range accts {
		if !fileProviderRow(a.OverlayKind) {
			continue
		}
		switch err := probe(a.ConfigDir); {
		case err == nil, errors.Is(err, overlay.ErrFPProbeMissing), errors.Is(err, overlay.ErrFPProbeEmpty):
			// Serving, or a benign no-identity/empty-by-design .claude.json.
		case errors.Is(err, overlay.ErrFPProbeNoVerdict):
			// The companion app is down too, or too old to answer the control op:
			// unprobeable, not a confirmed wedge. Say so rather than flag a false
			// wedge or silently pass — `ccp doctor` must not vouch for what it can't see.
			report(fmt.Sprintf("acct-%02d file provider", a.ID), false,
				"cannot verify whether the domain serves — the CCPoolStatus companion app isn't answering its control socket; launch "+abbreviateHome(pool.WidgetAppPath())+" (or start the daemon with `ccp service install`), then re-run `ccp doctor` (or add --fp-raw-probe to read through the domain directly)")
		default:
			report(fmt.Sprintf("acct-%02d file provider", a.ID), false,
				"domain not serving reads (probed directly; the daemon is down, so its content bridge is too) — start the daemon (`ccp service install`); if it stays wedged after that, run `ccp fp repair`")
		}
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
	kcred, kerr := keychain.Read()
	_, ferr := file.Read()

	switch {
	case kerr == nil:
		if errors.Is(ferr, creds.ErrNotFound) {
			if kcred.ClaudeAiOauth.AccessToken == "" {
				report(prefix+" credential", true, "keychain (access token empty — the daemon refreshes it on its next poll)")
				return
			}
			report(prefix+" credential", true, "keychain")
			return
		}
		// Drift: both backends hold a credential (refresh tokens are single-use,
		// so copies diverge). Advisory even under --fix — only `ccp cred move`
		// gates against live sessions and keeps the fresher copy.
		report(prefix+" credential", false, "credential in BOTH the Keychain and .credentials.json — copies diverge (Claude refresh tokens are single-use); consolidate with `ccp cred move --to keychain` (moves the fresher copy; the daemon gates it against live sessions)")
	case errors.Is(kerr, creds.ErrNoTokens):
		report(prefix+" credential", false, fmt.Sprintf("credential holds no tokens — re-login required: run `ccp login %d`", a.ID))
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
	case errors.Is(ferr, creds.ErrNoTokens):
		report(prefix+" credential", false, fmt.Sprintf("credential holds no tokens — re-login required: run `ccp login %d`", a.ID))
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
