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

// runStatusJSON prints the StatusSnapshot the widget reads.
func runStatusJSON(cmd *cobra.Command, m *pool.Manager, forceLive bool) error {
	snap, err := statusSnapshotJSON(cmd.Context(), m, forceLive)
	if err != nil {
		return err
	}
	out, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode status snapshot: %w", err)
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return nil
}

// statusSnapshotJSON bypasses gatherStatus/fromDaemon: that round-trip
// drops SampleAge and fabricates "sample_age":"0s".
func statusSnapshotJSON(ctx context.Context, m *pool.Manager, forceLive bool) (daemon.StatusSnapshot, error) {
	if !forceLive {
		cl := daemon.NewClient()
		resp, err := cl.StatusContext(ctx)
		_ = cl.Close()
		if daemonStatusUsable(resp, err) {
			snap := daemon.NewStatusSnapshot(resp.Accounts, time.Now())
			snap.Ledgers = resp.Ledgers
			return snap, nil
		}
	}
	snaps, err := m.Snapshots(ctx, true, pool.DefaultFreshFor)
	if err != nil {
		return daemon.StatusSnapshot{}, err
	}
	return daemon.NewStatusSnapshot(daemon.ToStatuses(snaps), time.Now()), nil
}

func runStatus(cmd *cobra.Command, m *pool.Manager, watch, live, plain bool) error {
	if isTTY() && !plain {
		return runStatusTUI(cmd, m, live)
	}
	cwd, _ := os.Getwd() // best-effort: an unreadable cwd just hides pin state
	render := func() error {
		snaps, ledgers, fpConsentPending, err := gatherStatus(cmd.Context(), m, live)
		if err != nil {
			return err
		}
		pin, err := readDirPin(m, cwd)
		if err != nil {
			return err
		}
		out := renderTable(snaps, pin)
		if watch {
			_, _ = fmt.Fprint(cmd.OutOrStdout(), "\033[H\033[2J") // clear
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), out)
		if line := fpConsentFooter(fpConsentPending); line != "" {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
		}
		if line := ledgerFooter(ledgers); line != "" {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
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

func gatherStatus(ctx context.Context, m *pool.Manager, forceLive bool) ([]pool.Snapshot, []daemon.LedgerState, bool, error) {
	if !forceLive {
		cl := daemon.NewClient()
		resp, err := cl.StatusContext(ctx)
		_ = cl.Close()
		if daemonStatusUsable(resp, err) {
			return fromDaemon(resp.Accounts), resp.Ledgers, resp.FPConsentPending, nil
		}
	}
	snaps, err := m.Snapshots(ctx, true, pool.DefaultFreshFor)
	return snaps, nil, false, err
}

// ledgerFooter is the compact self-heal rollup — plain-path only, like the
// other footer. It renders only when a ledger row is faulted or parked; a
// healthy pool prints nothing new. `ccp doctor` carries the per-row detail.
func ledgerFooter(ledgers []daemon.LedgerState) string {
	faulted, parked := 0, 0
	for _, l := range ledgers {
		if l.Parked {
			parked++
			continue
		}
		if l.Faulted {
			faulted++
		}
	}
	if faulted == 0 && parked == 0 {
		return ""
	}
	var parts []string
	if faulted > 0 {
		parts = append(parts, fmt.Sprintf("%d faulted", faulted))
	}
	if parked > 0 {
		parts = append(parts, fmt.Sprintf("%d parked", parked))
	}
	return warnStyle.Render("self-heal: " + strings.Join(parts, ", ") + " — run `ccp doctor` for detail")
}

// fpConsentFooter warns that the daemon's File Provider data bridge bind is
// pending its app-group-container grant — plain-path only, like the other footers
// (the TUI drops daemon-cache alerts on purpose). "" when not pending.
func fpConsentFooter(pending bool) string {
	if !pending {
		return ""
	}
	return warnStyle.Render("file provider: the daemon's data bridge bind is pending its macOS app-group-container grant — a release daemon binds prompt-free from the signed CCPoolDaemon.app bundle, so reinstall with `ccp service install`; run `ccp doctor` for detail")
}

// dirPin is the launch directory's pin as render input (ok=false: no pin).
type dirPin struct {
	cwd  string
	view pool.PinView
	ok   bool
}

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

func pinToken(manual bool) string {
	if manual {
		return pinStyle.Render("pinned")
	}
	return pinStyle.Render("pinned (auto)")
}

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

func abbreviateHome(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" || !strings.HasPrefix(path, home) {
		return path
	}
	return "~" + strings.TrimPrefix(path, home)
}

// daemonStatusUsable rejects version skew: an older daemon omits newer wire
// fields, so rendering its reply would be partial.
func daemonStatusUsable(resp *daemon.Response, err error) bool {
	return err == nil && resp != nil && resp.OK && resp.Version == version.String()
}

func fromDaemon(accs []daemon.AccountStatus) []pool.Snapshot {
	out := make([]pool.Snapshot, 0, len(accs))
	for _, a := range accs {
		s := pool.Snapshot{
			Score:           a.Score,
			HasUsage:        a.HasUsage,
			Remaining5h:     a.Remaining5h,
			Remaining7d:     a.Remaining7d,
			Util5h:          100 - a.Remaining5h,
			Util7d:          100 - a.Remaining7d,
			ActiveSessions:  a.ActiveSessions,
			RateLimited:     a.RateLimited,
			Exhausted:       a.Exhausted,
			NeedsLogin:      a.NeedsLogin,
			AwaitingOrigin:  a.AwaitingOrigin,
			Stale:           a.Stale,
			Resets5h:        a.Resets5h,
			Resets7d:        a.Resets7d,
			ExtraEnabled:    a.ExtraEnabled,
			ExtraUsed:       a.ExtraUsed,
			ExtraLimit:      a.ExtraLimit,
			Scoped7dUtil:    a.Scoped7dUtil,
			Scoped7dResets:  a.Scoped7dResets,
			Scoped7dModel:   a.Scoped7dModel,
			WeeklyExhausted: a.WeeklyExhausted,
			Components:      a.Components,
		}
		// Display fields only: the wire carries no keychain fields, so credential
		// operations must re-load the account from the store (statusTUI.resolveAccount).
		s.Account.ID = a.ID
		s.Account.ConfigDir = a.ConfigDir
		s.Account.Label = a.Label
		s.Account.OverlayKind = a.OverlayKind
		out = append(out, s)
	}
	return out
}

// snapshotTier mirrors Pick/PickFallback selection preference: an unusable
// account must never sort above a usable one, however high its score.
func snapshotTier(s pool.Snapshot) int {
	switch {
	case s.NeedsLogin:
		return 3
	case s.RateLimited:
		return 2
	case s.Exhausted:
		return 1
	default:
		return 0
	}
}

// sortSnapshots matches the order select consults, so the ▸ next-pick marker stays honest.
func sortSnapshots(snaps []pool.Snapshot) {
	sort.SliceStable(snaps, func(i, j int) bool {
		if ti, tj := snapshotTier(snaps[i]), snapshotTier(snaps[j]); ti != tj {
			return ti < tj
		}
		return snaps[i].Score > snaps[j].Score
	})
}

// usedCell renders a "% used" column, or "-" when the account has no known-good
// sample (never sampled, or only 429 placeholders) — never a fabricated 0%.
func usedCell(hasUsage bool, util float64) string {
	if !hasUsage {
		return "-"
	}
	return fmt.Sprintf("%.0f%%", util)
}

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
		used5 := usedCell(s.HasUsage, s.Util5h)
		used7 := usedCell(s.HasUsage, s.Util7d)
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

func snapshotFlags(s pool.Snapshot) string {
	var flags []string
	switch {
	case s.AwaitingOrigin:
		// A synced peer copy whose token expired: recovers on the origin's rotation
		// or a local `ccp login` — a warning, not a dead account.
		flags = append(flags, warnStyle.Render("origin stale"))
	case s.NeedsLogin:
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
		// Badge only on actual spend, not a permanent enabled-flag. API amounts
		// are cents (177 == $1.77).
		flags = append(flags, warnStyle.Render(fmt.Sprintf("overage $%.2f/$%.2f", s.ExtraUsed/100, s.ExtraLimit/100)))
	}
	if s.Scoped7dModel != "" {
		// The plain table has no per-account column, so the binding model-scoped
		// weekly bucket (e.g. Fable 5) rides here as a heat-colored badge; label
		// comes from the API, never hardcoded.
		flags = append(flags, usageStyle(s.Scoped7dUtil).Render(fmt.Sprintf("%s %.0f%%", s.Scoped7dModel, s.Scoped7dUtil)))
	}
	if !s.HasUsage {
		flags = append(flags, dimStyle.Render("no-data"))
	}
	return strings.Join(flags, " ")
}

// accountName never shows the acct-NN id; only `ccp list`/`ccp doctor` do.
func accountName(label string) string {
	if label == "" {
		return "(unnamed)"
	}
	return label
}

// usageSuffix returns "" for unknown usage (never-sampled or old daemon)
// rather than a fabricated 0%. When the pick carries a binding model-scoped
// weekly bucket (scopedModel != ""), its usage is appended too.
func usageSuffix(hasUsage bool, used5, used7 float64, scopedModel string, scopedUsed float64) string {
	if !hasUsage {
		return ""
	}
	pct5 := usageStyle(used5).Render(fmt.Sprintf("%.0f%%", used5))
	pct7 := usageStyle(used7).Render(fmt.Sprintf("%.0f%%", used7))
	out := dimStyle.Render(" · 5h ") + pct5 + dimStyle.Render(" used · 7d ") + pct7 + dimStyle.Render(" used")
	if scopedModel != "" {
		pctScoped := usageStyle(scopedUsed).Render(fmt.Sprintf("%.0f%%", scopedUsed))
		out += dimStyle.Render(" · "+scopedModel+" ") + pctScoped + dimStyle.Render(" used")
	}
	return out
}

func daemonAccountName(m *pool.Manager, id *int) string {
	if id != nil {
		if a, err := m.Store.GetAccount(*id); err == nil {
			return accountName(a.Label)
		}
	}
	return "account"
}

// clockLayout is the one layout for every human-facing clock in the status UI.
const clockLayout = "3:04 PM"

// humanizeReset renders a reset time; the zero time (no active window) is "-".
func humanizeReset(t time.Time) string {
	return humanizeResetAt(t, time.Now())
}

// humanizeResetAt is humanizeReset with an injectable now. Both ends normalize
// to Local: the /usage RFC3339 path and daemon JSON can carry non-local offsets.
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

// calendarDaysFrom counts whole local calendar days from now to t (0 = same
// day, negative = past). Midnight anchors + rounding stay correct across DST.
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
