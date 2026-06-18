package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/version"
)

func newStatusCmd() *cobra.Command {
	var watch bool
	var live bool
	var plain bool
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show per-account usage, score, and sessions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withManager(func(m *pool.Manager) error {
				if err := requireInit(m); err != nil {
					return err
				}
				if jsonOut {
					return runStatusJSON(cmd, m, live)
				}
				return runStatus(cmd, m, watch, live, plain)
			})
		},
	}
	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "refresh continuously (plain mode)")
	cmd.Flags().BoolVar(&live, "live", false, "force live sampling even if the daemon is running")
	cmd.Flags().BoolVar(&plain, "plain", false, "print the plain table instead of the interactive TUI")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "print the status snapshot JSON (same schema as ~/.cc-pool/status.json)")
	cmd.MarkFlagsMutuallyExclusive("json", "watch")
	cmd.MarkFlagsMutuallyExclusive("json", "plain")
	return cmd
}

// runStatusJSON prints the StatusSnapshot the widget reads — the daemon's
// cached view when usable, else live sampling.
func runStatusJSON(cmd *cobra.Command, m *pool.Manager, forceLive bool) error {
	snap, err := statusSnapshotJSON(cmd.Context(), m, forceLive)
	if err != nil {
		return err
	}
	out, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode status snapshot: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}

// statusSnapshotJSON builds the StatusSnapshot for --json. It bypasses
// gatherStatus/fromDaemon deliberately: that round-trip drops SampleAge, and a
// re-conversion through ToStatuses would fabricate "sample_age":"0s".
func statusSnapshotJSON(ctx context.Context, m *pool.Manager, forceLive bool) (daemon.StatusSnapshot, error) {
	if !forceLive {
		resp, err := daemon.NewClient().Status()
		if daemonStatusUsable(resp, err) {
			return daemon.NewStatusSnapshot(resp.Accounts, time.Now()), nil
		}
	}
	snaps, err := m.Snapshots(ctx, true, pool.DefaultFreshFor)
	if err != nil {
		return daemon.StatusSnapshot{}, err
	}
	return daemon.NewStatusSnapshot(daemon.ToStatuses(snaps), time.Now()), nil
}

// runStatus shows account status. On an interactive terminal it launches the
// TUI (which refreshes itself); piped or with --plain it prints the plain
// table, once or continuously under --watch. Both `ccp status` and bare `ccp`
// dispatch here.
func runStatus(cmd *cobra.Command, m *pool.Manager, watch, live, plain bool) error {
	if isTTY() && !plain {
		return runStatusTUI(cmd, m, live)
	}
	cwd, _ := os.Getwd() // best-effort: an unreadable cwd just hides pin state
	render := func() error {
		snaps, holder, err := gatherStatus(cmd.Context(), m, live)
		if err != nil {
			return err
		}
		// Unlike the TUI's keep-last-view loop, the one-shot path fails fast.
		pin, err := readDirPin(m, cwd)
		if err != nil {
			return err
		}
		out := renderTable(snaps, pin)
		if watch {
			fmt.Fprint(cmd.OutOrStdout(), "\033[H\033[2J") // clear
		}
		fmt.Fprintln(cmd.OutOrStdout(), out)
		if line := holderFooter(holder); line != "" {
			fmt.Fprintln(cmd.OutOrStdout(), line)
		}
		return nil
	}
	if !watch {
		return render()
	}
	for {
		if err := render(); err != nil {
			return err
		}
		select {
		case <-cmd.Context().Done():
			return nil
		case <-time.After(5 * time.Second):
		}
	}
}

// gatherStatus prefers the daemon's cached view, falling back to live
// sampling. The second return is the daemon's mount-holder cache — nil on the
// live path, where no process supervises the holder.
func gatherStatus(ctx context.Context, m *pool.Manager, forceLive bool) ([]pool.Snapshot, *daemon.HolderStatus, error) {
	if !forceLive {
		resp, err := daemon.NewClient().Status()
		if daemonStatusUsable(resp, err) {
			return fromDaemon(resp.Accounts), resp.Holder, nil
		}
	}
	snaps, err := m.Snapshots(ctx, true, pool.DefaultFreshFor)
	return snaps, nil, err
}

// holderFooter renders the one-line mount-holder alert under the plain status
// table — only when something needs attention. A healthy holder at the
// current version prints nothing, so status stays clean. Plain path only: the
// TUI's gather closure plumbs statusData (snapshots + pin) and deliberately
// drops the holder — `ccp doctor` and `ccp service status` carry the same
// facts for interactive users.
func holderFooter(h *daemon.HolderStatus) string {
	if h == nil {
		return ""
	}
	// Priority: TCC and a failing respawn block the holder entirely and need
	// the user; a wedge is narrower (N mirrors, supervision remounts them)
	// and only asks for a doctor run; skew is cosmetic.
	switch {
	case h.TCCError != "":
		return warnStyle.Render("mount holder: TCC blocked — " + h.TCCError)
	case h.SpawnError != "":
		return warnStyle.Render("mount holder: respawn failing — " + h.SpawnError)
	case h.WedgedMounts > 0:
		return warnStyle.Render(fmt.Sprintf("mount holder: %s — run `ccp doctor`", plural(h.WedgedMounts, "wedged mirror")))
	case h.Skewed:
		return warnStyle.Render(fmt.Sprintf("mount holder %s skewed; will be replaced when idle", h.Version))
	default:
		return ""
	}
}

// dirPin is the launch directory's pin as render input (ok=false: no pin).
type dirPin struct {
	cwd  string
	view pool.PinView
	ok   bool
}

// readDirPin reads cwd's pin straight from the store — a single-row local read
// that needs neither the daemon's status cache nor a wire change.
func readDirPin(m *pool.Manager, cwd string) (dirPin, error) {
	if cwd == "" {
		return dirPin{}, nil
	}
	view, ok, err := m.PinView(cwd, time.Now())
	if err != nil {
		return dirPin{}, err
	}
	return dirPin{cwd: cwd, view: view, ok: ok}, nil
}

// pinToken is the row badge on the pinned account's line.
func pinToken(manual bool) string {
	if manual {
		return pinStyle.Render("pinned")
	}
	return pinStyle.Render("pinned (auto)")
}

// renderPinLine renders the launch directory's pin summary ("" when none):
// who it is pinned to, manual vs auto, and what the pin is doing — counting
// down to expiry, held open by live sessions, or waiting for one to end.
// Shared by the TUI and the plain table.
func renderPinLine(pin dirPin, snaps []pool.Snapshot) string {
	if !pin.ok {
		return ""
	}
	name := fmt.Sprintf("acct-%02d", pin.view.AccountID)
	unusable := false
	for _, s := range snaps {
		if s.Account.ID != pin.view.AccountID {
			continue
		}
		name = accountName(s.Account.Label)
		// Mirrors score.UsableForSticky off the snapshot's own breakdown.
		unusable = s.RateLimited || s.Exhausted ||
			(s.HasUsage && s.Components.RawRemaining5h < score.StickyMinRemaining5h)
		break
	}
	kind := "auto"
	if pin.view.Manual {
		kind = "manual"
	}
	var state string
	switch {
	case unusable:
		// Never promise a bind the selector would hold or abandon.
		state = "account can't serve — selects rank freely"
	case pin.view.Live && pin.view.Binding:
		state = "while sessions live"
	case pin.view.Live:
		state = "waiting for session end"
	default:
		state = "until " + humanizeReset(pin.view.ExpiresAt)
	}
	return pinStyle.Render("pinned "+name) +
		dimStyle.Render(" · "+kind+" · "+state+" · "+abbreviateHome(pin.cwd))
}

// abbreviateHome renders path with the user's home directory shortened to ~.
func abbreviateHome(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" || !strings.HasPrefix(path, home) {
		return path
	}
	return "~" + strings.TrimPrefix(path, home)
}

// daemonStatusUsable reports whether a status response can be rendered directly.
// A transport error, a not-OK reply, or a version-skewed (pre-upgrade) daemon —
// which omits newer wire fields like Components — must fall back to live
// sampling so the render is never partial.
func daemonStatusUsable(resp *daemon.Response, err error) bool {
	return err == nil && resp != nil && resp.OK && resp.Version == version.String()
}

// fromDaemon converts daemon account statuses into Snapshots for rendering.
func fromDaemon(accs []daemon.AccountStatus) []pool.Snapshot {
	out := make([]pool.Snapshot, 0, len(accs))
	for _, a := range accs {
		s := pool.Snapshot{
			Score:          a.Score,
			HasUsage:       a.HasUsage,
			Remaining5h:    a.Remaining5h,
			Remaining7d:    a.Remaining7d,
			Util5h:         100 - a.Remaining5h,
			Util7d:         100 - a.Remaining7d,
			ActiveSessions: a.ActiveSessions,
			RateLimited:    a.RateLimited,
			Exhausted:      a.Exhausted,
			NeedsLogin:     a.NeedsLogin,
			Stale:          a.Stale,
			Resets5h:       a.Resets5h,
			Resets7d:       a.Resets7d,
			ExtraEnabled:   a.ExtraEnabled,
			ExtraUsed:      a.ExtraUsed,
			ExtraLimit:     a.ExtraLimit,
			Components:     a.Components,
		}
		s.Account.ID = a.ID
		s.Account.ConfigDir = a.ConfigDir
		s.Account.Label = a.Label
		s.Account.OverlayKind = a.OverlayKind
		out = append(out, s)
	}
	return out
}

// snapshotTier mirrors selection preference: Pick serves available accounts,
// PickFallback falls back to exhausted-but-not-rate-limited, and a rate-limited
// account cannot serve at all. Status must never show an unusable account above
// a usable one, however high its forward-looking score.
func snapshotTier(s pool.Snapshot) int {
	switch {
	case s.NeedsLogin:
		return 3 // needs a human (`ccp login`); below even rate-limited
	case s.RateLimited:
		return 2
	case s.Exhausted:
		return 1
	default:
		return 0
	}
}

// sortSnapshots orders snapshots for display: usability tier first, then score
// desc — the same order select consults, so the ▸ next-pick marker stays honest.
func sortSnapshots(snaps []pool.Snapshot) {
	sort.SliceStable(snaps, func(i, j int) bool {
		if ti, tj := snapshotTier(snaps[i]), snapshotTier(snaps[j]); ti != tj {
			return ti < tj
		}
		return snaps[i].Score > snaps[j].Score
	})
}

// renderTable produces a styled fixed-width table, best account highlighted,
// with the launch directory's pin badged on its account and summarized below.
func renderTable(snaps []pool.Snapshot, pin dirPin) string {
	if len(snaps) == 0 {
		return "No accounts yet. Run `ccp add` to add one.\n"
	}
	sortSnapshots(snaps)

	var b strings.Builder
	// Two leading spaces align the header with the rows' marker gutter ("▸ "/"  ").
	header := fmt.Sprintf("  %-24s %8s %8s %8s %5s %-17s",
		"ACCOUNT", "SCORE", "5h used", "7d used", "LIVE", "RESETS")
	b.WriteString(hdrStyle.Render(header))
	b.WriteByte('\n')

	for i, s := range snaps {
		label := truncate(accountName(s.Account.Label), 24)
		used5 := fmt.Sprintf("%.0f%%", s.Util5h)
		used7 := fmt.Sprintf("%.0f%%", s.Util7d)
		reset := humanizeReset(s.Resets5h)
		row := fmt.Sprintf("%-24s %8.1f %8s %8s %5d %-17s",
			label, s.Score, used5, used7, s.ActiveSessions, reset)
		if flags := snapshotFlags(s); flags != "" {
			row += " " + flags
		}
		if pin.ok && s.Account.ID == pin.view.AccountID {
			row += " " + pinToken(pin.view.Manual)
		}
		if i == 0 {
			row = bestStyle.Render("▸ ") + row
		} else {
			row = "  " + row
		}
		b.WriteString(row)
		b.WriteByte('\n')
	}
	b.WriteString(dimStyle.Render("▸ = next pick · score higher = emptier · 5h/7d = % used"))
	b.WriteByte('\n')
	if line := renderPinLine(pin, snaps); line != "" {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString(dimStyle.Render(fmt.Sprintf("updated %s", time.Now().Format(clockLayout))))
	return b.String()
}

// snapshotFlags renders the colored status tokens (stale / rate-limited /
// exhausted / overage / no-data) for one account, or "" when the account is
// healthy. Shared by the plain table and the TUI.
func snapshotFlags(s pool.Snapshot) string {
	var flags []string
	if s.NeedsLogin {
		// Most actionable state — lead with it.
		flags = append(flags, badStyle.Render("needs login"))
	}
	if s.Stale {
		flags = append(flags, warnStyle.Render("stale"))
	}
	if s.RateLimited {
		flags = append(flags, badStyle.Render("rate-limited"))
	}
	if s.Exhausted {
		flags = append(flags, badStyle.Render("exhausted"))
	}
	if s.ExtraEnabled && s.ExtraUsed > 0 {
		// Only actual spend earns the badge — overage merely being enabled is
		// not an alert, and a permanent flag would train the eye to ignore it.
		// API amounts are currency cents (e.g. 177 of 5000 == $1.77 of $50).
		flags = append(flags, warnStyle.Render(fmt.Sprintf("overage $%.2f/$%.2f", s.ExtraUsed/100, s.ExtraLimit/100)))
	}
	if !s.HasUsage {
		flags = append(flags, dimStyle.Render("no-data"))
	}
	return strings.Join(flags, " ")
}

// accountName returns an account's display label, or a placeholder when it is
// unnamed. The internal acct-NN id is shown only by `ccp list` and `ccp doctor`.
func accountName(label string) string {
	if label == "" {
		return "(unnamed)"
	}
	return label
}

// usageSuffix renders raw 5h/7d percent-used as " · 5h X% used · 7d Y% used"
// for the `select`/`run` diagnostic lines, each figure health-tinted. It returns
// "" when usage is unknown — a never-sampled pick, or a daemon predating the wire
// fields — so we never print a fabricated 0%.
func usageSuffix(hasUsage bool, used5, used7 float64) string {
	if !hasUsage {
		return ""
	}
	pct5 := usageStyle(used5).Render(fmt.Sprintf("%.0f%%", used5))
	pct7 := usageStyle(used7).Render(fmt.Sprintf("%.0f%%", used7))
	return dimStyle.Render(" · 5h ") + pct5 + dimStyle.Render(" used · 7d ") + pct7 + dimStyle.Render(" used")
}

// daemonAccountName resolves a daemon SelectedID to a display name, degrading to
// "account" when the id is nil or unknown.
func daemonAccountName(m *pool.Manager, id *int) string {
	if id != nil {
		if a, err := m.Store.GetAccount(*id); err == nil {
			return accountName(a.Label)
		}
	}
	return "account"
}

// clockLayout is the shared spelling for every human-facing wall-clock time in
// the status UI: 12-hour with AM/PM, e.g. "3:58 PM".
const clockLayout = "3:04 PM"

// humanizeReset renders an absolute rate-limit reset time in the user's local
// zone with smart day context — "3:58 PM" today, "tomorrow 3:58 PM", "Tue 3:58
// PM" within the week, "Jun 10, 3:58 PM" beyond. The zero time (no active
// window) renders "-".
func humanizeReset(t time.Time) string {
	return humanizeResetAt(t, time.Now())
}

// humanizeResetAt is humanizeReset with an injectable now for deterministic
// tests. Both ends are normalized to Local so the clock and day arithmetic match
// the user's wall clock regardless of the zone the reset arrived in (the /usage
// RFC3339 path and the daemon's JSON can carry a non-local offset).
func humanizeResetAt(t, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	t, now = t.Local(), now.Local()
	switch days := calendarDaysFrom(now, t); {
	case days <= 0: // today, or a past reset from stale data
		return t.Format(clockLayout)
	case days == 1:
		return "tomorrow " + t.Format(clockLayout)
	case days < 7:
		return t.Format("Mon " + clockLayout)
	default:
		return t.Format("Jan 2, " + clockLayout)
	}
}

// calendarDaysFrom returns the count of whole local calendar days from now to t
// (0 = same day, 1 = next day, negative = past). Anchoring both ends at local
// midnight and rounding keeps it correct across DST (a 23h or 25h day).
func calendarDaysFrom(now, t time.Time) int {
	y0, m0, d0 := now.Date()
	y1, m1, d1 := t.Date()
	start := time.Date(y0, m0, d0, 0, 0, 0, 0, now.Location())
	end := time.Date(y1, m1, d1, 0, 0, 0, 0, now.Location())
	return int(math.Round(end.Sub(start).Hours() / 24))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}
