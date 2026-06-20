package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// statusRefreshInterval is how often the TUI re-polls the daemon (or live
// samples) for fresh account state.
const statusRefreshInterval = 5 * time.Second

var (
	// selectedStyle marks the row under the cursor and the detail it drives.
	selectedStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	panelStyle    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
	panelTitle    = lipgloss.NewStyle().Bold(true)
)

// runStatusTUI runs the interactive status dashboard until the user quits or
// the context is cancelled. It is only reached on an interactive terminal; the
// piped/`--plain` path stays on renderTable.
func runStatusTUI(cmd *cobra.Command, m *pool.Manager, live bool) error {
	ctx := cmd.Context()
	// Restart a pre-upgrade daemon onto the current binary so the cached view it
	// serves carries the newer wire fields the detail pane renders. Best-effort;
	// gatherStatus version-gates and falls back to live regardless. Skipped under
	// --live (the user already opted out of the daemon). The alt-screen clears any
	// "restarting…" line on entry.
	if !live {
		ensureDaemon(cmd)
	}
	cwd, _ := os.Getwd() // best-effort: an unreadable cwd just hides pin controls
	model := statusTUI{
		ctx: ctx,
		cwd: cwd,
		gather: func(c context.Context) (statusData, error) {
			// The holder alert is dropped here on purpose: statusData carries
			// only snapshots + pin, and the TUI footer already multiplexes
			// refresh/pin errors — holderFooter stays a plain-path concern.
			snaps, _, err := gatherStatus(c, m, live)
			if err != nil {
				return statusData{}, err
			}
			pin, err := readDirPin(m, cwd)
			if err != nil {
				return statusData{}, fmt.Errorf("read pin: %w", err)
			}
			return statusData{snaps: snaps, pin: pin}, nil
		},
		toggle: func(accountID int) (bool, error) {
			return m.TogglePin(cwd, accountID, time.Now())
		},
		buildLogin: func(a store.Account) (*exec.Cmd, error) {
			return loginCommand(a.ConfigDir)
		},
		finishLogin: func(a store.Account) error {
			return finishRelogin(ctx, m, a)
		},
	}
	p := tea.NewProgram(model,
		tea.WithContext(ctx),
		tea.WithAltScreen(),
		tea.WithOutput(cmd.OutOrStdout()),
	)
	if _, err := p.Run(); err != nil && !errors.Is(err, tea.ErrProgramKilled) {
		return err
	}
	return nil
}

// statusData is one gather's worth of TUI state: the account snapshots plus
// the launch directory's pin.
type statusData struct {
	snaps []pool.Snapshot
	pin   dirPin
}

// statusTUI is the Bubble Tea model for `ccp status`. It owns a sorted snapshot
// list, a cursor tracked by account id (so a re-sort on refresh never moves the
// selection), a gather closure that re-fetches state off the UI goroutine, and
// a toggle closure that pins/unpins the launch directory to the cursor's
// account.
type statusTUI struct {
	ctx         context.Context
	cwd         string // launch directory; "" hides pin controls
	gather      func(context.Context) (statusData, error)
	toggle      func(accountID int) (bool, error)
	buildLogin  func(a store.Account) (*exec.Cmd, error) // build the interactive `claude /login`
	finishLogin func(a store.Account) error              // verify + adopt + clear, off the UI goroutine
	snaps       []pool.Snapshot
	pin         dirPin
	cursorID    int
	width       int
	height      int
	err         error
	pinErr      error
	pinBusy     bool // a toggle is in flight; drop repeat presses
	reloginErr  error
	reloginBusy bool // a re-login is in flight; drop repeat presses
	lastUpdate  time.Time
	quitting    bool
}

// Bubble Tea messages.
type (
	snapsMsg struct {
		data statusData
		at   time.Time
	}
	errMsg     struct{ err error }
	pinDoneMsg struct{ err error }
	// reloginExitedMsg fires once claude /login exited — whether cc-pool
	// auto-closed it on a fresh credential or the user quit it — and the watched
	// tea.Exec spawn handed the terminal back; the verify+adopt+clear still has to
	// run, and finishLogin stays the sole credential gate.
	reloginExitedMsg struct{ account store.Account }
	reloginDoneMsg   struct{ err error }
	tickMsg          time.Time
)

// watchedLogin runs `claude /login` for the status TUI while polling for a fresh
// credential; when one lands it closes claude, so re-login auto-closes exactly
// like `ccp login`. Implements tea.ExecCommand: Bubble Tea calls Run with the
// renderer paused and the terminal released, so the poll+terminate run off the UI
// goroutine without fighting claude for the tty.
type watchedLogin struct {
	ctx  context.Context
	cmd  *exec.Cmd
	read credReader
	out  io.Writer // claude's (and the TUI's) output; where the input-mode reset is emitted
}

func (w *watchedLogin) SetStdin(r io.Reader)  { w.cmd.Stdin = r }
func (w *watchedLogin) SetStdout(o io.Writer) { w.cmd.Stdout = o; w.out = o }
func (w *watchedLogin) SetStderr(o io.Writer) { w.cmd.Stderr = o }

func (w *watchedLogin) Run() error {
	baseline := ""
	if cred, err := w.read(); err == nil {
		baseline = cred.ClaudeAiOauth.AccessToken
	}
	outcome, _ := watchAndClose(w.ctx, w.cmd, newReloginProbe(w.read, baseline))
	// Bubble Tea's post-Exec restore re-enters the alt screen, shows the cursor,
	// and restores raw mode, but does not disable the input modes claude turned on
	// that the TUI doesn't use. When we force-killed claude (anything but a clean
	// self-exit), reset only those input modes — Bubble Tea owns alt-screen/cursor
	// and will re-enter and repaint them itself.
	if outcome != awaitExited && isTTY() {
		_, _ = fmt.Fprint(w.out, inputModeReset)
	}
	return nil // finishRelogin (post-exit, via reloginExitedMsg) stays the sole credential gate
}

func (t statusTUI) Init() tea.Cmd {
	return tea.Batch(t.refreshCmd(), tickCmd())
}

// refreshCmd fetches fresh status off the UI goroutine; live sampling never
// blocks key input.
func (t statusTUI) refreshCmd() tea.Cmd {
	return func() tea.Msg {
		data, err := t.gather(t.ctx)
		if err != nil {
			return errMsg{err}
		}
		return snapsMsg{data: data, at: time.Now()}
	}
}

// togglePinCmd flips the launch directory's pin against the given account off
// the UI goroutine. The decision intentionally uses the displayed (≤5s stale)
// pin state: the action always matches what the user saw, and the post-toggle
// refresh reconciles the view.
func (t statusTUI) togglePinCmd(accountID int) tea.Cmd {
	return func() tea.Msg {
		_, err := t.toggle(accountID)
		return pinDoneMsg{err: err}
	}
}

// finishReloginCmd runs the post-login verify+adopt+clear off the UI goroutine
// after claude /login exited, keeping the Keychain/network I/O off the render
// loop.
func (t statusTUI) finishReloginCmd(a store.Account) tea.Cmd {
	return func() tea.Msg {
		return reloginDoneMsg{err: t.finishLogin(a)}
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(statusRefreshInterval, func(tm time.Time) tea.Msg { return tickMsg(tm) })
}

// reloginable reports whether the status TUI should offer its 'a' re-login action:
// any non-serving state a manual `claude /login` can clear. Mirrors `ccp login`,
// which has no health gate.
func reloginable(s pool.Snapshot) bool {
	return s.NeedsLogin || s.Stale || s.RateLimited || s.Exhausted
}

func (t statusTUI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		t.width, t.height = msg.Width, msg.Height
		return t, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			t.quitting = true
			return t, tea.Quit
		case "up", "k":
			t.moveCursor(-1)
			return t, nil
		case "down", "j":
			t.moveCursor(1)
			return t, nil
		case "r":
			return t, t.refreshCmd()
		case "p":
			if t.cwd == "" || len(t.snaps) == 0 || t.pinBusy || t.toggle == nil {
				return t, nil
			}
			t.pinBusy = true
			t.pinErr = nil
			return t, t.togglePinCmd(t.current().Account.ID)
		case "a":
			// Re-login any account a login can recover — the remedy the badge
			// advertises. claude /login takes over the terminal via a watched
			// tea.Exec spawn that auto-closes claude once a fresh credential lands
			// (the user may also exit it manually).
			s := t.current()
			if len(t.snaps) == 0 || t.reloginBusy || t.buildLogin == nil || !reloginable(s) {
				return t, nil
			}
			c, err := t.buildLogin(s.Account)
			if err != nil {
				t.reloginErr = err
				return t, nil
			}
			t.reloginBusy = true
			t.reloginErr = nil
			a := s.Account
			wl := &watchedLogin{ctx: t.ctx, cmd: c, read: func() (*keychain.Credential, error) {
				return reloginCred(a)
			}}
			// The exit status is not the signal: a real login is confirmed only by
			// the usable credential it leaves behind, exactly as `ccp login`
			// checks. finishLogin is the sole gate.
			return t, tea.Exec(wl, func(error) tea.Msg { return reloginExitedMsg{account: a} })
		}
		return t, nil
	case snapsMsg:
		t.snaps = msg.data.snaps
		sortSnapshots(t.snaps)
		t.pin = msg.data.pin
		t.lastUpdate = msg.at
		t.err = nil
		t.ensureCursor()
		return t, nil
	case errMsg:
		t.err = msg.err
		return t, nil
	case pinDoneMsg:
		t.pinBusy = false
		t.pinErr = msg.err
		if msg.err != nil {
			return t, nil
		}
		return t, t.refreshCmd()
	case reloginExitedMsg:
		// claude exited and the terminal is back under our control; finish the
		// verify+adopt+clear off the UI goroutine.
		return t, t.finishReloginCmd(msg.account)
	case reloginDoneMsg:
		t.reloginBusy = false
		t.reloginErr = msg.err
		if msg.err != nil {
			return t, nil
		}
		return t, t.refreshCmd() // pick up the cleared flag / fresh sample
	case tickMsg:
		return t, tea.Batch(t.refreshCmd(), tickCmd())
	}
	return t, nil
}

// sortedIndex returns the display row of the cursor's account, or 0.
func (t statusTUI) sortedIndex() int {
	for i, s := range t.snaps {
		if s.Account.ID == t.cursorID {
			return i
		}
	}
	return 0
}

// ensureCursor keeps the cursor on a real account after a refresh, defaulting
// to the best (top) account when its previous target is gone.
func (t *statusTUI) ensureCursor() {
	if len(t.snaps) == 0 {
		t.cursorID = 0
		return
	}
	for _, s := range t.snaps {
		if s.Account.ID == t.cursorID {
			return
		}
	}
	t.cursorID = t.snaps[0].Account.ID
}

func (t *statusTUI) moveCursor(d int) {
	if len(t.snaps) == 0 {
		return
	}
	i := t.sortedIndex() + d
	if i < 0 {
		i = 0
	}
	if i >= len(t.snaps) {
		i = len(t.snaps) - 1
	}
	t.cursorID = t.snaps[i].Account.ID
}

func (t statusTUI) current() pool.Snapshot {
	i := t.sortedIndex()
	if i < 0 || i >= len(t.snaps) {
		return pool.Snapshot{}
	}
	return t.snaps[i]
}

func (t statusTUI) View() string {
	if t.quitting {
		return ""
	}
	if len(t.snaps) == 0 {
		if t.err != nil {
			return fmt.Sprintf("status error: %v\n", t.err)
		}
		return "Loading account status…\n"
	}
	w := t.width
	if w <= 0 {
		w = 80
	}
	contentW := w - 4
	if contentW < 40 {
		contentW = 40
	}
	listBox := panelStyle.Width(contentW).Render(t.renderList())
	detailBox := panelStyle.Width(contentW).Render(t.renderDetail())
	// Each binding is advertised only where it actually works on the cursor's
	// account, and names the effect the press will have.
	helpParts := []string{"↑/↓ navigate"}
	if reloginable(t.current()) {
		helpParts = append(helpParts, "a re-login")
	}
	if t.cwd != "" {
		pinKey := "p pin"
		if t.pin.ok && t.current().Account.ID == t.pin.view.AccountID {
			pinKey = "p unpin"
		}
		helpParts = append(helpParts, pinKey)
	}
	helpParts = append(helpParts, "r refresh", "q quit")
	footer := dimStyle.Render(strings.Join(helpParts, " · "))
	if t.reloginErr != nil {
		footer = badStyle.Render(fmt.Sprintf("re-login failed: %v", t.reloginErr)) + "  " + footer
	}
	if t.pinErr != nil {
		footer = badStyle.Render(fmt.Sprintf("pin failed: %v", t.pinErr)) + "  " + footer
	}
	if t.err != nil {
		footer = warnStyle.Render(fmt.Sprintf("refresh failed: %v", t.err)) + "  " + footer
	}
	parts := []string{listBox, detailBox}
	if line := renderPinLine(t.pin, t.snaps); line != "" {
		parts = append(parts, line)
	}
	parts = append(parts, footer)
	return lipgloss.JoinVertical(lipgloss.Left, parts...) + "\n"
}

// renderList draws the account table. A 3-cell gutter carries two independent
// markers: ▸ (green) = the account `select` would pick next, ❯ = the cursor.
func (t statusTUI) renderList() string {
	hdr := hdrStyle.Render(fmt.Sprintf("   %-22s %8s %8s %8s %5s",
		"ACCOUNT", "SCORE", "5h used", "7d used", "LIVE"))
	lines := make([]string, 0, 1+len(t.snaps))
	lines = append(lines, hdr)
	cursor := t.sortedIndex()
	for i, s := range t.snaps {
		bestMark := " "
		if i == 0 {
			bestMark = bestStyle.Render("▸")
		}
		curMark := " "
		if i == cursor {
			curMark = selectedStyle.Render("❯")
		}
		cells := fmt.Sprintf("%-22s %8.1f %8s %8s %5d",
			truncate(accountName(s.Account.Label), 22), s.Score,
			fmt.Sprintf("%.0f%%", s.Util5h), fmt.Sprintf("%.0f%%", s.Util7d), s.ActiveSessions)
		if i == cursor {
			cells = selectedStyle.Render(cells)
		}
		row := bestMark + curMark + " " + cells
		if fl := snapshotFlags(s); fl != "" {
			row += " " + fl
		}
		if t.pin.ok && s.Account.ID == t.pin.view.AccountID {
			row += " " + pinToken(t.pin.view.Manual)
		}
		lines = append(lines, row)
	}
	return strings.Join(lines, "\n")
}

// renderDetail explains the selected account's score factor-by-factor, sourced
// from the score Components so it reconciles exactly with the SCORE column.
func (t statusTUI) renderDetail() string {
	s := t.current()
	c := s.Components
	var b strings.Builder

	pick := "no"
	if len(t.snaps) > 0 && t.snaps[0].Account.ID == s.Account.ID {
		pick = "yes"
	}
	b.WriteString(panelTitle.Render(fmt.Sprintf("%s · next pick: %s", accountName(s.Account.Label), pick)))
	b.WriteByte('\n')
	_, _ = fmt.Fprintf(&b, "score %.1f\n", s.Score)

	// Positive contributions: reset-aware effective headroom × its weight. Labeled
	// "effective" (not "free") because the reset credit can lift it above the raw
	// remaining shown by the usage bars below; tinted by how depleted it is.
	eff5Str := usageStyle(100 - c.Eff5).Render(fmt.Sprintf("%3.0f%%", c.Eff5))
	eff7Str := usageStyle(100 - c.Eff7).Render(fmt.Sprintf("%3.0f%%", c.Eff7))
	_, _ = fmt.Fprintf(&b, "  5h  %s effective  ×%.2f  = %+5.1f\n", eff5Str, score.W5h, c.Remaining5h)
	_, _ = fmt.Fprintf(&b, "  7d  %s effective  ×%.2f  = %+5.1f\n", eff7Str, score.W7d, c.Remaining7d)

	// Penalties, only the ones actually engaged.
	var pen []string
	if c.SessionPenalty > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", fmt.Sprintf("sessions %d", s.ActiveSessions), -c.SessionPenalty))
	}
	if c.RateLimitPenalty > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", "rate-limited", -c.RateLimitPenalty))
	}
	if c.NeedsLoginPenalty > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", "needs-login", -c.NeedsLoginPenalty))
	}
	if c.StalePenalty > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", "stale data", -c.StalePenalty))
	}
	if c.Barrier5h > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", "low 5h headroom", -c.Barrier5h))
	}
	if c.Barrier7d > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", "low 7d headroom", -c.Barrier7d))
	}
	if c.RunwayPenalty > 0 {
		pen = append(pen, fmt.Sprintf("  %-18s %+5.1f", "burn rate", -c.RunwayPenalty))
	}
	if len(pen) == 0 {
		b.WriteString("  penalties          none\n")
	} else {
		b.WriteString(strings.Join(pen, "\n"))
		b.WriteByte('\n')
	}

	// Raw usage bars (what's consumed) with reset timing.
	b.WriteString(usageRow("5h", s.Util5h, s.Resets5h))
	b.WriteByte('\n')
	b.WriteString(usageRow("7d", s.Util7d, s.Resets7d))
	b.WriteByte('\n')

	overlay := s.Account.OverlayKind
	if overlay == "" {
		overlay = "symlink"
	}
	meta := "overlay " + overlay
	if t.pin.ok && s.Account.ID == t.pin.view.AccountID {
		if t.pin.view.Manual {
			meta += " · pinned to this directory (manual)"
		} else {
			meta += " · pinned to this directory (auto)"
		}
	}
	if !t.lastUpdate.IsZero() {
		meta += " · updated " + t.lastUpdate.Format("15:04:05")
	}
	b.WriteString(dimStyle.Render(meta))
	return b.String()
}

// usageRow renders one "5h ▕████░░░▏ 58% used · resets 2h03m" line.
func usageRow(label string, usedPct float64, resets time.Time) string {
	when := "no active window"
	if !resets.IsZero() {
		when = "resets " + humanizeReset(resets)
	}
	return fmt.Sprintf("%-2s %s %3.0f%% used · %s", label, usageBar(usedPct, 16), usedPct, when)
}

// usageBar renders a fixed-width filled bar for a 0..100 used percentage, tinted
// green/yellow/red as it fills.
func usageBar(usedPct float64, width int) string {
	if usedPct < 0 {
		usedPct = 0
	}
	if usedPct > 100 {
		usedPct = 100
	}
	filled := int(math.Round(usedPct / 100 * float64(width)))
	if filled > width {
		filled = width
	}
	bar := usageStyle(usedPct).Render(strings.Repeat("█", filled)) + dimStyle.Render(strings.Repeat("░", width-filled))
	return "▕" + bar + "▏"
}
