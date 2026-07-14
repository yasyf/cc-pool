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
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
)

const statusRefreshInterval = 5 * time.Second

var (
	selectedStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("12"))
	panelStyle    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
	panelTitle    = lipgloss.NewStyle().Bold(true)
)

// runStatusTUI runs only on an interactive terminal; the piped/`--plain` path uses renderTable.
func runStatusTUI(cmd *cobra.Command, m *pool.Manager, live bool) error {
	ctx := cmd.Context()
	// Restart a version-skewed daemon so its cached view carries the detail pane's wire fields; gatherStatus falls back to live.
	if !live {
		ensureDaemon(cmd, false)
	}
	cwd, _ := os.Getwd() // unreadable cwd just hides pin controls
	model := statusTUI{
		ctx: ctx,
		cwd: cwd,
		gather: func(c context.Context) (statusData, error) {
			// Ledger and FP-consent alerts stay a plain-path concern.
			snaps, _, _, err := gatherStatus(c, m, live)
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
			return loginCommand(a.ConfigDir, accountLoginEmail(a))
		},
		acquireLease: acquireAndProbeSessionLease,
		finishLogin: func(a store.Account, baseline string) error {
			return tuiFinishRelogin(ctx, m, a, baseline)
		},
		readCred: func(a store.Account) (*creds.Credential, error) {
			cred, _, err := m.ReadCredential(a)
			return cred, err
		},
		resolveAccount: m.Store.GetAccount,
		checkFresh: func(a store.Account) (bool, error) {
			return tuiCheckFresh(ctx, m, a)
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

type statusData struct {
	snaps []pool.Snapshot
	pin   dirPin
}

// statusTUI is the Bubble Tea model for `ccp status`; the cursor tracks account id so a refresh re-sort never moves the selection.
type statusTUI struct {
	ctx         context.Context
	cwd         string // launch directory; "" hides pin controls
	gather      func(context.Context) (statusData, error)
	toggle      func(accountID int) (bool, error)
	buildLogin  func(a store.Account) (*exec.Cmd, error)
	finishLogin func(a store.Account, baseline string) error
	// acquireLease holds the session lease across the interactive relogin so the
	// holder never tears down the mount mid-login; nil uses acquireAndProbeSessionLease.
	acquireLease func(a store.Account) (*lease.Handle, error)
	readCred     func(a store.Account) (*creds.Credential, error) // credential resolution for the re-login probe (both backends)
	// resolveAccount re-loads the account from the store: wire snapshots carry
	// no keychain fields, so credential operations must never use s.Account.
	resolveAccount func(id int) (store.Account, error)
	checkFresh     func(a store.Account) (bool, error) // needs-login short-circuit: a login that already landed
	snaps          []pool.Snapshot
	pin            dirPin
	cursorID       int
	width          int
	height         int
	err            error
	pinErr         error
	pinBusy        bool
	reloginErr     error
	reloginBusy    bool
	reloginLease   *lease.Handle // held for the interactive relogin flow; closed on reloginDoneMsg
	lastUpdate     time.Time
	quitting       bool
}

type (
	snapsMsg struct {
		data statusData
		at   time.Time
	}
	errMsg     struct{ err error }
	pinDoneMsg struct{ err error }
	// reloginStartMsg starts the interactive login after the short-circuit check passed
	// on it; lease is the session lease already acquired (before any credential
	// read-modify-write) and held across the login, released on reloginDoneMsg.
	reloginStartMsg struct {
		account store.Account
		lease   *lease.Handle
	}
	// reloginExitedMsg fires after claude auth login exits and the terminal is back
	// under the TUI; baseline is the pre-login access token finishRelogin
	// compares against, so a quit-without-login never reads as success.
	reloginExitedMsg struct {
		account  store.Account
		baseline string
	}
	reloginDoneMsg struct{ err error }
	tickMsg        time.Time
)

// watchedLogin runs `claude auth login` as a tea.ExecCommand and auto-closes claude
// once a fresh credential lands; Bubble Tea releases the tty for Run, so poll+terminate never fight claude for it.
type watchedLogin struct {
	ctx      context.Context
	cmd      *exec.Cmd
	fp       bool // File Provider account: turn on dataless-file materialization around the spawn
	read     credReader
	out      io.Writer // where the input-mode reset is emitted
	baseline string    // pre-login access token, set by Run for the finish gate
}

func (w *watchedLogin) SetStdin(r io.Reader)  { w.cmd.Stdin = r }
func (w *watchedLogin) SetStdout(o io.Writer) { w.cmd.Stdout = o; w.out = o }
func (w *watchedLogin) SetStderr(o io.Writer) { w.cmd.Stderr = o }

func (w *watchedLogin) Run() error {
	if cred, err := w.read(); err == nil {
		w.baseline = cred.ClaudeAiOauth.AccessToken
	}
	outcome, err := watchAndClose(w.ctx, w.cmd, w.fp, newReloginProbe(w.read, w.baseline))
	// Bubble Tea's post-Exec restore leaves claude's input modes on, so after a
	// force-kill (not a clean self-exit) reset only those; Bubble Tea owns alt-screen/cursor.
	if outcome != awaitExited && isTTY() {
		_, _ = fmt.Fprint(w.out, inputModeReset)
	}
	// A launch/execguard failure (awaitCanceled) fails loud; a clean exit or landed
	// identity defers to finishRelogin's credential gate.
	if outcome == awaitCanceled {
		return err
	}
	return nil
}

func (t statusTUI) Init() tea.Cmd {
	return tea.Batch(t.refreshCmd(), tickCmd())
}

func (t statusTUI) refreshCmd() tea.Cmd {
	return func() tea.Msg {
		data, err := t.gather(t.ctx)
		if err != nil {
			return errMsg{err}
		}
		return snapsMsg{data: data, at: time.Now()}
	}
}

// togglePinCmd toggles from the displayed (≤5s stale) pin state so the action matches what the user saw; the refresh reconciles.
func (t statusTUI) togglePinCmd(accountID int) tea.Cmd {
	return func() tea.Msg {
		_, err := t.toggle(accountID)
		return pinDoneMsg{err: err}
	}
}

func (t statusTUI) finishReloginCmd(a store.Account, baseline string) tea.Cmd {
	return func() tea.Msg {
		return reloginDoneMsg{err: t.finishLogin(a, baseline)}
	}
}

// startReloginCmd resolves the store account off the event loop, acquires+probes the
// session lease BEFORE any credential read-modify-write (the needs-login
// short-circuit persists credentials, so the holder must not tear the mount down
// under it), then lets a login that already landed clear the needs-login flag
// without spawning claude; otherwise the interactive login starts holding the lease.
func (t statusTUI) startReloginCmd(id int) tea.Cmd {
	return func() tea.Msg {
		a, err := t.resolveAccount(id)
		if err != nil {
			return reloginDoneMsg{err: err}
		}
		acquire := t.acquireLease
		if acquire == nil {
			acquire = acquireAndProbeSessionLease
		}
		h, err := acquire(a)
		if err != nil {
			return reloginDoneMsg{err: err}
		}
		if t.checkFresh != nil {
			cleared, err := t.checkFresh(a)
			if err != nil {
				closeLease(h)
				return reloginDoneMsg{err: err}
			}
			if cleared {
				closeLease(h)
				return reloginDoneMsg{}
			}
		}
		return reloginStartMsg{account: a, lease: h}
	}
}

// reloginExited maps a watchedLogin.Run result into the next relogin message: a
// launch/execguard failure (non-nil err) surfaces immediately via reloginDoneMsg,
// releasing the lease and skipping finishRelogin's misleading unchanged-credential
// report; a clean run proceeds to that credential gate via reloginExitedMsg.
func reloginExited(a store.Account, baseline string, err error) tea.Msg {
	if err != nil {
		return reloginDoneMsg{err: err}
	}
	return reloginExitedMsg{account: a, baseline: baseline}
}

// closeLease closes a session-lease handle when non-nil (a test seam may return nil).
func closeLease(h *lease.Handle) {
	if h != nil {
		_ = h.Close()
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(statusRefreshInterval, func(tm time.Time) tea.Msg { return tickMsg(tm) })
}

// tuiFinishRelogin is the TUI's finishLogin seam: the CLI relogin tail with
// warnings discarded — the alt-screen owns the terminal.
func tuiFinishRelogin(ctx context.Context, m *pool.Manager, a store.Account, baseline string) error {
	if err := finishRelogin(ctx, m, a, baseline); err != nil {
		return err
	}
	afterReloginIO(ctx, io.Discard, io.Discard, m, a)
	return nil
}

// tuiCheckFresh is the TUI's short-circuit seam; a cleared flag runs the same
// publish tail as `ccp login`.
func tuiCheckFresh(ctx context.Context, m *pool.Manager, a store.Account) (bool, error) {
	cleared, err := shortCircuitRelogin(ctx, m, a)
	if cleared {
		afterReloginIO(ctx, io.Discard, io.Discard, m, a)
	}
	return cleared, err
}

// reloginable reports whether a manual `claude auth login` could clear s's state (no health gate).
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
			s := t.current()
			if len(t.snaps) == 0 || t.reloginBusy || t.buildLogin == nil || t.resolveAccount == nil || !reloginable(s) {
				return t, nil
			}
			t.reloginBusy = true
			t.reloginErr = nil
			return t, t.startReloginCmd(s.Account.ID)
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
	case reloginStartMsg:
		// The session lease was acquired+probed in startReloginCmd (before the
		// credential-persisting short-circuit) and is held across the interactive login
		// so the holder never tears down the account's mount while claude writes its new
		// identity. Released on reloginDoneMsg. This is the leased equivalent of
		// runRelogin, which the TUI cannot call directly (it owns the terminal via
		// Bubble Tea).
		t.reloginLease = msg.lease
		c, err := t.buildLogin(msg.account)
		if err != nil {
			closeLease(msg.lease)
			t.reloginLease = nil
			t.reloginBusy = false
			t.reloginErr = err
			return t, nil
		}
		a := msg.account
		wl := &watchedLogin{ctx: t.ctx, cmd: c, fp: isFPRow(a.OverlayKind), read: func() (*creds.Credential, error) {
			return t.readCred(a)
		}}
		// The callback runs after wl.Run, so wl.baseline is set by then; wl.Run's
		// error (a launch/execguard failure) routes to reloginExited.
		return t, tea.Exec(wl, func(err error) tea.Msg { return reloginExited(a, wl.baseline, err) })
	case reloginExitedMsg:
		return t, t.finishReloginCmd(msg.account, msg.baseline)
	case reloginDoneMsg:
		if t.reloginLease != nil {
			_ = t.reloginLease.Close()
			t.reloginLease = nil
		}
		t.reloginBusy = false
		t.reloginErr = msg.err
		if msg.err != nil {
			return t, nil
		}
		return t, t.refreshCmd()
	case tickMsg:
		return t, tea.Batch(t.refreshCmd(), tickCmd())
	}
	return t, nil
}

func (t statusTUI) sortedIndex() int {
	for i, s := range t.snaps {
		if s.Account.ID == t.cursorID {
			return i
		}
	}
	return 0
}

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

// renderList draws the account table; ▸ marks the account `select` picks next, ❯ the cursor.
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
			usedCell(s.HasUsage, s.Util5h), usedCell(s.HasUsage, s.Util7d), s.ActiveSessions)
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

// renderDetail breaks down the selected account's score from its Components, so it reconciles exactly with the SCORE column.
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

	// "effective", not "free": reset credit can lift headroom above the raw remaining the bars below show.
	eff5Str := usageStyle(100 - c.Eff5).Render(fmt.Sprintf("%3.0f%%", c.Eff5))
	eff7Str := usageStyle(100 - c.Eff7).Render(fmt.Sprintf("%3.0f%%", c.Eff7))
	_, _ = fmt.Fprintf(&b, "  5h  %s effective  ×%.2f  = %+5.1f\n", eff5Str, score.W5h, c.Remaining5h)
	_, _ = fmt.Fprintf(&b, "  7d  %s effective  ×%.2f  = %+5.1f\n", eff7Str, score.W7d, c.Remaining7d)

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

	if !s.HasUsage {
		// No known-good sample (never sampled, or only 429 placeholders): the score
		// breakdown above still explains the score, but there is no measured
		// utilization to draw — an empty bar reading "0% used" would be a lie.
		b.WriteString(dimStyle.Render("no usage data yet"))
		b.WriteByte('\n')
	} else {
		labelWidth := len("7d")
		if n := utf8.RuneCountInString(s.Scoped7dModel); n > labelWidth {
			labelWidth = n
		}
		b.WriteString(usageRow("5h", labelWidth, s.Util5h, s.Resets5h))
		b.WriteByte('\n')
		b.WriteString(usageRow("7d", labelWidth, s.Util7d, s.Resets7d))
		b.WriteByte('\n')
		if s.Scoped7dModel != "" {
			// The binding model-scoped weekly bucket (e.g. Fable 5); label from the API.
			b.WriteString(usageRow(s.Scoped7dModel, labelWidth, s.Scoped7dUtil, s.Scoped7dResets))
			b.WriteByte('\n')
		}
	}

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

// usageRow renders one aligned usage line. labelWidth is the pane-wide label
// column (the widest of 5h/7d/model) so the bars line up; the label — for the
// scoped row, the API-provided model name — is never truncated.
func usageRow(label string, labelWidth int, usedPct float64, resets time.Time) string {
	when := "no active window"
	if !resets.IsZero() {
		when = "resets " + humanizeReset(resets)
	}
	return fmt.Sprintf("%-*s %s %3.0f%% used · %s", labelWidth, label, usageBar(usedPct, 16), usedPct, when)
}

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
