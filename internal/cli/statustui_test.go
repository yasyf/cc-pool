package cli

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

type fakeToggle struct {
	calls []int
	err   error
}

func (f *fakeToggle) fn(accountID int) (bool, error) {
	f.calls = append(f.calls, accountID)
	return f.err == nil, f.err
}

func pinTUI(cwd string, pin dirPin, ft *fakeToggle) statusTUI {
	best := pool.Snapshot{Account: store.Account{ID: 1, Label: "alice@example.com"}, Score: 90, HasUsage: true}
	busy := pool.Snapshot{Account: store.Account{ID: 2, Label: "bob@example.com"}, Score: 50, HasUsage: true}
	return statusTUI{
		cwd:      cwd,
		snaps:    []pool.Snapshot{best, busy},
		cursorID: 2,
		pin:      pin,
		toggle:   ft.fn,
	}
}

func pressP(t *testing.T, tui statusTUI) (statusTUI, tea.Cmd) {
	t.Helper()
	model, cmd := tui.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("p")})
	out, ok := model.(statusTUI)
	if !ok {
		t.Fatalf("Update returned %T, want statusTUI", model)
	}
	return out, cmd
}

// TestStatusTUIPinToggle drives the 'p' pin-toggle key end to end.
func TestStatusTUIPinToggle(t *testing.T) {
	t.Run("p toggles the cursor's account", func(t *testing.T) {
		ft := &fakeToggle{}
		tui, cmd := pressP(t, pinTUI("/proj", dirPin{}, ft))
		if !tui.pinBusy || cmd == nil {
			t.Fatalf("p must mark busy and return a Cmd, busy=%v cmd=%v", tui.pinBusy, cmd)
		}
		msg := cmd()
		if len(ft.calls) != 1 || ft.calls[0] != 2 {
			t.Fatalf("toggle calls = %v, want [2]", ft.calls)
		}
		done, ok := msg.(pinDoneMsg)
		if !ok || done.err != nil {
			t.Fatalf("msg = %#v, want clean pinDoneMsg", msg)
		}
		model, refresh := tui.Update(done)
		tui = model.(statusTUI)
		if tui.pinBusy || tui.pinErr != nil {
			t.Fatalf("done must clear busy and error: busy=%v err=%v", tui.pinBusy, tui.pinErr)
		}
		if refresh == nil {
			t.Fatal("a successful toggle must trigger a refresh")
		}
	})

	t.Run("busy debounce drops repeats", func(t *testing.T) {
		ft := &fakeToggle{}
		tui := pinTUI("/proj", dirPin{}, ft)
		tui.pinBusy = true
		if _, cmd := pressP(t, tui); cmd != nil {
			t.Fatal("p while busy must be inert")
		}
	})

	t.Run("inert without a cwd", func(t *testing.T) {
		ft := &fakeToggle{}
		tui, cmd := pressP(t, pinTUI("", dirPin{}, ft))
		if cmd != nil || tui.pinBusy || len(ft.calls) != 0 {
			t.Fatalf("p without cwd must be inert: cmd=%v busy=%v calls=%v", cmd, tui.pinBusy, ft.calls)
		}
	})

	t.Run("inert without accounts", func(t *testing.T) {
		ft := &fakeToggle{}
		tui := pinTUI("/proj", dirPin{}, ft)
		tui.snaps = nil
		if _, cmd := pressP(t, tui); cmd != nil {
			t.Fatal("p with no accounts must be inert")
		}
	})
}

// TestStatusTUIPinErrorSurfaced: a failed toggle surfaces; success clears it.
func TestStatusTUIPinErrorSurfaced(t *testing.T) {
	ft := &fakeToggle{err: errors.New("database is locked")}
	tui, cmd := pressP(t, pinTUI("/proj", dirPin{}, ft))
	model, refresh := tui.Update(cmd())
	tui = model.(statusTUI)
	if tui.pinBusy || tui.pinErr == nil {
		t.Fatalf("failed toggle: busy=%v err=%v", tui.pinBusy, tui.pinErr)
	}
	if refresh != nil {
		t.Fatal("a failed toggle must not refresh (the view did not change)")
	}
	tui.width = 100
	if view := stripANSI(tui.View()); !strings.Contains(view, "pin failed: database is locked") {
		t.Fatalf("error not surfaced:\n%s", view)
	}

	ft.err = nil
	tui, cmd = pressP(t, tui)
	model, _ = tui.Update(cmd())
	tui = model.(statusTUI)
	if tui.pinErr != nil {
		t.Fatalf("recovered toggle must clear the error, got %v", tui.pinErr)
	}
}

// TestStatusTUIDetailNeedsLoginPenalty: the detail pane shows the needs-login
// row only when the penalty is engaged.
func TestStatusTUIDetailNeedsLoginPenalty(t *testing.T) {
	want := fmt.Sprintf("  %-18s %+5.1f", "needs-login", -score.PenNeedsLogin)

	t.Run("rendered when the penalty is engaged", func(t *testing.T) {
		tui := pinTUI("/proj", dirPin{}, &fakeToggle{})
		tui.snaps[1].Components.NeedsLoginPenalty = score.PenNeedsLogin
		detail := stripANSI(tui.renderDetail())
		if !strings.Contains(detail, want) {
			t.Fatalf("detail must show the needs-login penalty row %q:\n%s", want, detail)
		}
	})

	t.Run("absent when the penalty is zero", func(t *testing.T) {
		tui := pinTUI("/proj", dirPin{}, &fakeToggle{})
		detail := stripANSI(tui.renderDetail())
		if strings.Contains(detail, "needs-login") {
			t.Fatalf("detail must omit the needs-login row when the penalty is zero:\n%s", detail)
		}
	})
}

// TestStatusTUIDetailScopedRow: the detail pane renders the model-scoped weekly
// usage row (labeled by the API model name) only when the account carries a
// scoped bucket.
func TestStatusTUIDetailScopedRow(t *testing.T) {
	t.Run("rendered when a scoped model is set", func(t *testing.T) {
		tui := pinTUI("/proj", dirPin{}, &fakeToggle{})
		tui.snaps[1].Scoped7dModel = "Fable"
		tui.snaps[1].Scoped7dUtil = 100
		tui.snaps[1].Scoped7dResets = time.Now().Add(48 * time.Hour)
		detail := stripANSI(tui.renderDetail())
		if !strings.Contains(detail, "Fable") || !strings.Contains(detail, "100% used") {
			t.Fatalf("detail must show the scoped model row when a model is set:\n%s", detail)
		}
	})

	t.Run("absent when no scoped model", func(t *testing.T) {
		tui := pinTUI("/proj", dirPin{}, &fakeToggle{})
		detail := stripANSI(tui.renderDetail())
		if strings.Contains(detail, "Fable") {
			t.Fatalf("detail must omit the scoped row when no model is set:\n%s", detail)
		}
	})

	t.Run("long model label renders verbatim", func(t *testing.T) {
		// The label is API-provided; a name wider than the static 5h/7d labels
		// must never be truncated to fit the column.
		tui := pinTUI("/proj", dirPin{}, &fakeToggle{})
		tui.snaps[1].Scoped7dModel = "Opus 4.8"
		tui.snaps[1].Scoped7dUtil = 50
		tui.snaps[1].Scoped7dResets = time.Now().Add(48 * time.Hour)
		detail := stripANSI(tui.renderDetail())
		if !strings.Contains(detail, "Opus 4.8") {
			t.Fatalf("detail must show the full API model name:\n%s", detail)
		}
		if strings.Contains(detail, "…") {
			t.Fatalf("scoped model label must not be truncated:\n%s", detail)
		}
	})
}

// TestStatusTUIViewShowsPin covers pin badges, summary, detail, and footer hints.
func TestStatusTUIViewShowsPin(t *testing.T) {
	pin := dirPin{cwd: "/proj", ok: true, view: pool.PinView{
		AccountID: 2, Manual: true, Binding: true, ExpiresAt: time.Now().Add(30 * time.Minute),
	}}
	tui := pinTUI("/proj", pin, &fakeToggle{})
	tui.width = 120
	view := stripANSI(tui.View())

	if !strings.Contains(view, "p unpin") {
		t.Fatalf("footer must advertise unpinning the pinned account:\n%s", view)
	}
	other := tui
	other.cursorID = 1
	if v := stripANSI(other.View()); !strings.Contains(v, "p pin ") || strings.Contains(v, "p unpin") {
		t.Fatalf("footer must advertise pinning on an unpinned account:\n%s", v)
	}
	if !strings.Contains(view, "pinned bob@example.com") || !strings.Contains(view, "manual") {
		t.Fatalf("pin summary line missing:\n%s", view)
	}
	var bobRow string
	for _, line := range strings.Split(view, "\n") {
		if strings.Contains(line, "bob@example.com") && strings.Contains(line, "50.0") {
			bobRow = line
		}
		if strings.Contains(line, "alice@example.com") && strings.Contains(line, "pinned") {
			t.Fatalf("unpinned row must not be badged: %q", line)
		}
	}
	if !strings.Contains(bobRow, "pinned") {
		t.Fatalf("pinned row must be badged: %q", bobRow)
	}
	if !strings.Contains(view, "pinned to this directory (manual)") {
		t.Fatalf("detail pane must name the pin:\n%s", view)
	}

	bare := pinTUI("", dirPin{}, &fakeToggle{})
	bare.width = 120
	view = stripANSI(bare.View())
	if strings.Contains(view, "p pin") || strings.Contains(view, "p unpin") || strings.Contains(view, "pinned ") {
		t.Fatalf("no-cwd view must hide pin UI:\n%s", view)
	}
}

type fakeLogin struct {
	built      []store.Account
	finished   []int
	resolved   []int
	fresh      []store.Account
	buildErr   error
	finishErr  error
	resolveErr error
	freshDone  bool
	freshErr   error
}

func (f *fakeLogin) build(a store.Account) (*exec.Cmd, error) {
	f.built = append(f.built, a)
	if f.buildErr != nil {
		return nil, f.buildErr
	}
	// Never started: tests drive the ExecProcess messages directly.
	return exec.Command("true"), nil
}

func (f *fakeLogin) finish(a store.Account) error {
	f.finished = append(f.finished, a.ID)
	return f.finishErr
}

// resolve returns the store-shaped account — keychain fields populated —
// unlike the wire-derived snapshot accounts the TUI renders.
func (f *fakeLogin) resolve(id int) (store.Account, error) {
	f.resolved = append(f.resolved, id)
	if f.resolveErr != nil {
		return store.Account{}, f.resolveErr
	}
	return store.Account{ID: id, KeychainService: fmt.Sprintf("svc-%02d", id), KeychainAccount: "user"}, nil
}

func (f *fakeLogin) checkFresh(a store.Account) (bool, error) {
	f.fresh = append(f.fresh, a)
	return f.freshDone, f.freshErr
}

func reloginTUI(fl *fakeLogin) statusTUI {
	healthy := pool.Snapshot{Account: store.Account{ID: 1, Label: "alice@example.com"}, Score: 90, HasUsage: true}
	stale := pool.Snapshot{Account: store.Account{ID: 2, Label: "bob@example.com"}, Score: 50, HasUsage: true, NeedsLogin: true}
	return statusTUI{
		snaps:          []pool.Snapshot{healthy, stale},
		cursorID:       2,
		buildLogin:     fl.build,
		finishLogin:    fl.finish,
		resolveAccount: fl.resolve,
	}
}

func pressA(t *testing.T, tui statusTUI) (statusTUI, tea.Cmd) {
	t.Helper()
	model, cmd := tui.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")})
	out, ok := model.(statusTUI)
	if !ok {
		t.Fatalf("Update returned %T, want statusTUI", model)
	}
	return out, cmd
}

// startLogin runs the async start Cmd pressA returned and feeds its
// reloginStartMsg back through Update, returning the tea.Exec Cmd.
func startLogin(t *testing.T, tui statusTUI, cmd tea.Cmd) (statusTUI, tea.Cmd) {
	t.Helper()
	msg := cmd()
	start, ok := msg.(reloginStartMsg)
	if !ok {
		t.Fatalf("start msg = %#v, want reloginStartMsg", msg)
	}
	model, execCmd := tui.Update(start)
	return model.(statusTUI), execCmd
}

// TestStatusTUIReloginAction drives the 'a' re-login key end to end.
func TestStatusTUIReloginAction(t *testing.T) {
	bob := store.Account{ID: 2, Label: "bob@example.com"}

	t.Run("a re-logs in the needs-login account", func(t *testing.T) {
		fl := &fakeLogin{}
		tui, cmd := pressA(t, reloginTUI(fl))
		if !tui.reloginBusy || cmd == nil {
			t.Fatalf("a must mark busy and return a Cmd: busy=%v cmd=%v", tui.reloginBusy, cmd)
		}
		tui, execCmd := startLogin(t, tui, cmd)
		if execCmd == nil {
			t.Fatal("reloginStartMsg must return the exec Cmd")
		}
		if len(fl.resolved) != 1 || fl.resolved[0] != 2 {
			t.Fatalf("resolveAccount calls = %v, want [2]", fl.resolved)
		}
		if len(fl.built) != 1 || fl.built[0].ID != 2 {
			t.Fatalf("buildLogin calls = %v, want account 2", fl.built)
		}
		// The regression this flow pins: the login must run on the store-resolved
		// account, never the wire snapshot (whose keychain fields are empty).
		if fl.built[0].KeychainService != "svc-02" {
			t.Fatalf("buildLogin got keychain service %q, want the store-resolved svc-02", fl.built[0].KeychainService)
		}
		model, finish := tui.Update(reloginExitedMsg{account: bob})
		tui = model.(statusTUI)
		if finish == nil {
			t.Fatal("reloginExitedMsg must return the finish Cmd")
		}
		msg := finish()
		done, ok := msg.(reloginDoneMsg)
		if !ok || done.err != nil {
			t.Fatalf("finish msg = %#v, want clean reloginDoneMsg", msg)
		}
		if len(fl.finished) != 1 || fl.finished[0] != 2 {
			t.Fatalf("finishLogin calls = %v, want [2]", fl.finished)
		}
		model, refresh := tui.Update(done)
		tui = model.(statusTUI)
		if tui.reloginBusy || tui.reloginErr != nil {
			t.Fatalf("done must clear busy and error: busy=%v err=%v", tui.reloginBusy, tui.reloginErr)
		}
		if refresh == nil {
			t.Fatal("a successful re-login must trigger a refresh")
		}
	})

	t.Run("busy debounce drops repeats", func(t *testing.T) {
		fl := &fakeLogin{}
		tui := reloginTUI(fl)
		tui.reloginBusy = true
		if _, cmd := pressA(t, tui); cmd != nil {
			t.Fatal("a while busy must be inert")
		}
		if len(fl.built) != 0 {
			t.Fatalf("a while busy must not build a login: %v", fl.built)
		}
	})

	t.Run("inert on a healthy account", func(t *testing.T) {
		fl := &fakeLogin{}
		tui := reloginTUI(fl)
		tui.cursorID = 1
		got, cmd := pressA(t, tui)
		if cmd != nil || got.reloginBusy || len(fl.built) != 0 {
			t.Fatalf("a on a healthy account must be inert: cmd=%v busy=%v built=%v", cmd, got.reloginBusy, fl.built)
		}
	})

	t.Run("a re-logs in a stale rate-limited account", func(t *testing.T) {
		fl := &fakeLogin{}
		tui := statusTUI{
			snaps: []pool.Snapshot{{
				Account:     store.Account{ID: 7, Label: "carol@example.com"},
				Score:       10,
				HasUsage:    true,
				Stale:       true,
				RateLimited: true,
			}},
			cursorID:       7,
			buildLogin:     fl.build,
			finishLogin:    fl.finish,
			resolveAccount: fl.resolve,
		}
		got, cmd := pressA(t, tui)
		if !got.reloginBusy || cmd == nil {
			t.Fatalf("a on a stale rate-limited account must not be inert: busy=%v cmd=%v", got.reloginBusy, cmd)
		}
		_, execCmd := startLogin(t, got, cmd)
		if execCmd == nil {
			t.Fatal("reloginStartMsg must return the exec Cmd")
		}
		if len(fl.built) != 1 || fl.built[0].ID != 7 {
			t.Fatalf("buildLogin calls = %v, want account 7", fl.built)
		}
	})

	t.Run("build error surfaces without starting a login", func(t *testing.T) {
		fl := &fakeLogin{buildErr: errors.New("`claude` not found on PATH")}
		tui, cmd := pressA(t, reloginTUI(fl))
		if cmd == nil {
			t.Fatal("a must return the start Cmd")
		}
		msg := cmd()
		model, execCmd := tui.Update(msg)
		tui = model.(statusTUI)
		if execCmd != nil {
			t.Fatal("a build error must not start a login")
		}
		if tui.reloginBusy || tui.reloginErr == nil {
			t.Fatalf("build error must stay un-busy and record the error: busy=%v err=%v", tui.reloginBusy, tui.reloginErr)
		}
	})

	t.Run("resolver error surfaces without starting a login", func(t *testing.T) {
		fl := &fakeLogin{resolveErr: errors.New("database is locked")}
		tui, cmd := pressA(t, reloginTUI(fl))
		if !tui.reloginBusy || cmd == nil {
			t.Fatalf("a must mark busy and return a Cmd: busy=%v cmd=%v", tui.reloginBusy, cmd)
		}
		msg := cmd()
		done, ok := msg.(reloginDoneMsg)
		if !ok || done.err == nil {
			t.Fatalf("msg = %#v, want failed reloginDoneMsg", msg)
		}
		model, _ := tui.Update(done)
		tui = model.(statusTUI)
		if tui.reloginBusy || tui.reloginErr == nil {
			t.Fatalf("resolver error must clear busy and record the error: busy=%v err=%v", tui.reloginBusy, tui.reloginErr)
		}
		if len(fl.built) != 0 {
			t.Fatalf("resolver error must not build a login: %v", fl.built)
		}
	})

	t.Run("nil resolver is inert", func(t *testing.T) {
		fl := &fakeLogin{}
		tui := reloginTUI(fl)
		tui.resolveAccount = nil
		if _, cmd := pressA(t, tui); cmd != nil {
			t.Fatal("a without a resolver must be inert")
		}
	})
}

// TestStatusTUIReloginShortCircuit: a needs-login account whose credential
// already landed clears without spawning claude; anything else logs in.
func TestStatusTUIReloginShortCircuit(t *testing.T) {
	t.Run("already-fresh credential skips the login", func(t *testing.T) {
		fl := &fakeLogin{freshDone: true}
		tui := reloginTUI(fl)
		tui.checkFresh = fl.checkFresh
		tui, cmd := pressA(t, tui)
		msg := cmd()
		done, ok := msg.(reloginDoneMsg)
		if !ok || done.err != nil {
			t.Fatalf("msg = %#v, want clean reloginDoneMsg", msg)
		}
		if len(fl.fresh) != 1 || fl.fresh[0].KeychainService != "svc-02" {
			t.Fatalf("checkFresh calls = %v, want the store-resolved account", fl.fresh)
		}
		if len(fl.built) != 0 {
			t.Fatalf("short-circuit must not build a login: %v", fl.built)
		}
		model, refresh := tui.Update(done)
		tui = model.(statusTUI)
		if tui.reloginBusy || tui.reloginErr != nil || refresh == nil {
			t.Fatalf("short-circuit must clear busy and refresh: busy=%v err=%v refresh=%v", tui.reloginBusy, tui.reloginErr, refresh)
		}
	})

	t.Run("not-fresh proceeds to the login", func(t *testing.T) {
		fl := &fakeLogin{freshDone: false}
		tui := reloginTUI(fl)
		tui.checkFresh = fl.checkFresh
		tui, cmd := pressA(t, tui)
		_, execCmd := startLogin(t, tui, cmd)
		if execCmd == nil || len(fl.built) != 1 {
			t.Fatalf("not-fresh must start the login: exec=%v built=%v", execCmd, fl.built)
		}
	})

	t.Run("check error surfaces without a login", func(t *testing.T) {
		fl := &fakeLogin{freshErr: errors.New("database is locked")}
		tui := reloginTUI(fl)
		tui.checkFresh = fl.checkFresh
		tui, cmd := pressA(t, tui)
		msg := cmd()
		done, ok := msg.(reloginDoneMsg)
		if !ok || done.err == nil {
			t.Fatalf("msg = %#v, want failed reloginDoneMsg", msg)
		}
		model, refresh := tui.Update(done)
		tui = model.(statusTUI)
		if tui.reloginBusy || tui.reloginErr == nil || refresh != nil {
			t.Fatalf("check error must surface and not refresh: busy=%v err=%v refresh=%v", tui.reloginBusy, tui.reloginErr, refresh)
		}
		if len(fl.built) != 0 {
			t.Fatalf("check error must not build a login: %v", fl.built)
		}
	})
}

// TestStatusTUIReloginErrorSurfaced: a failed finish surfaces; success clears it.
func TestStatusTUIReloginErrorSurfaced(t *testing.T) {
	bob := store.Account{ID: 2, Label: "bob@example.com"}

	fl := &fakeLogin{finishErr: errors.New("login left no usable credential")}
	tui, cmd := pressA(t, reloginTUI(fl))
	if !tui.reloginBusy || cmd == nil {
		t.Fatalf("a must mark busy and return a Cmd: busy=%v cmd=%v", tui.reloginBusy, cmd)
	}
	model, finish := tui.Update(reloginExitedMsg{account: bob})
	tui = model.(statusTUI)
	model, refresh := tui.Update(finish())
	tui = model.(statusTUI)
	if tui.reloginBusy || tui.reloginErr == nil {
		t.Fatalf("failed re-login: busy=%v err=%v", tui.reloginBusy, tui.reloginErr)
	}
	if refresh != nil {
		t.Fatal("a failed re-login must not refresh (nothing changed)")
	}
	tui.width = 120
	if view := stripANSI(tui.View()); !strings.Contains(view, "re-login failed: login left no usable credential") {
		t.Fatalf("error not surfaced:\n%s", view)
	}

	fl.finishErr = nil
	tui, _ = pressA(t, tui)
	model, finish = tui.Update(reloginExitedMsg{account: bob})
	tui = model.(statusTUI)
	model, refresh = tui.Update(finish())
	tui = model.(statusTUI)
	if tui.reloginErr != nil {
		t.Fatalf("recovered re-login must clear the error, got %v", tui.reloginErr)
	}
	if refresh == nil {
		t.Fatal("recovered re-login must refresh")
	}
}

// TestStatusTUIListNoDataDash pins that renderList prints "-" (never a
// fabricated 0%) in the used columns for an account with no known-good sample,
// while a genuinely-sampled account still shows real percentages.
func TestStatusTUIListNoDataDash(t *testing.T) {
	sampled := pool.Snapshot{
		Account: store.Account{ID: 1, Label: "sampled@example.com"},
		Score:   90, HasUsage: true, Util5h: 58, Util7d: 61,
	}
	nodata := pool.Snapshot{
		Account: store.Account{ID: 2, Label: "nodata@example.com"},
		Score:   50, HasUsage: false, RateLimited: true,
	}
	tui := statusTUI{snaps: []pool.Snapshot{sampled, nodata}, cursorID: 1}
	out := stripANSI(tui.renderList())

	row := func(label string) string {
		t.Helper()
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, label) {
				return l
			}
		}
		t.Fatalf("row %q not found in\n%s", label, out)
		return ""
	}

	if sampledRow := row("sampled@example.com"); !strings.Contains(sampledRow, "58%") || !strings.Contains(sampledRow, "61%") {
		t.Errorf("genuinely-sampled account must show real percentages (58/61)\n%q", sampledRow)
	}

	nodataRow := row("nodata@example.com")
	if strings.Contains(nodataRow, "0%") {
		t.Errorf("no-data account must not show a fabricated 0%% used\n%q", nodataRow)
	}
	if strings.Contains(nodataRow, "%") {
		t.Errorf("no-data account must not show any fabricated %% used\n%q", nodataRow)
	}
	// Both used columns render the right-aligned "-" cell (reusing the code's own
	// %8s width so the token can't be confused with the hyphen in the flags).
	if want := fmt.Sprintf("%8s %8s", "-", "-"); !strings.Contains(nodataRow, want) {
		t.Errorf("no-data account must render the dash cells %q\n%q", want, nodataRow)
	}
	if !strings.Contains(nodataRow, "no-data") {
		t.Errorf("no-data account must carry the no-data flag\n%q", nodataRow)
	}
}

// TestStatusTUIDetailNoUsageData pins that renderDetail prints "no usage data
// yet" (never a fabricated usage bar) for an account with no known-good sample,
// while a genuinely-sampled account renders real usage rows.
func TestStatusTUIDetailNoUsageData(t *testing.T) {
	sampled := pool.Snapshot{
		Account: store.Account{ID: 1, Label: "sampled@example.com"},
		Score:   90, HasUsage: true, Util5h: 58, Util7d: 61,
	}
	nodata := pool.Snapshot{
		Account: store.Account{ID: 2, Label: "nodata@example.com"},
		Score:   50, HasUsage: false, RateLimited: true,
	}

	t.Run("no-data account shows the no-usage note, not a fabricated bar", func(t *testing.T) {
		tui := statusTUI{snaps: []pool.Snapshot{sampled, nodata}, cursorID: 2}
		detail := stripANSI(tui.renderDetail())
		if !strings.Contains(detail, "no usage data yet") {
			t.Fatalf("detail must show the no-usage note:\n%s", detail)
		}
		// The eff5/eff7 breakdown lines above legitimately carry "% effective";
		// a fabricated usage row is the "% used" phrasing, which must be absent.
		if strings.Contains(detail, "% used") {
			t.Fatalf("no-data detail must not fabricate a usage row:\n%s", detail)
		}
	})

	t.Run("genuinely-sampled account renders real usage rows", func(t *testing.T) {
		tui := statusTUI{snaps: []pool.Snapshot{sampled, nodata}, cursorID: 1}
		detail := stripANSI(tui.renderDetail())
		if strings.Contains(detail, "no usage data yet") {
			t.Fatalf("sampled detail must not show the no-usage note:\n%s", detail)
		}
		if !strings.Contains(detail, "58% used") || !strings.Contains(detail, "61% used") {
			t.Fatalf("sampled detail must render real usage rows (58/61):\n%s", detail)
		}
	})
}

// TestStatusTUIReloginFooter: 'a re-login' shows only on a needs-login account.
func TestStatusTUIReloginFooter(t *testing.T) {
	tui := reloginTUI(&fakeLogin{})
	tui.width = 120
	if v := stripANSI(tui.View()); !strings.Contains(v, "a re-login") {
		t.Fatalf("footer must advertise re-login on a needs-login account:\n%s", v)
	}
	tui.cursorID = 1
	if v := stripANSI(tui.View()); strings.Contains(v, "a re-login") {
		t.Fatalf("footer must hide re-login on a healthy account:\n%s", v)
	}
}
