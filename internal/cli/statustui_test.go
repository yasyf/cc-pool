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

// fakeToggle records TogglePin calls and returns an injectable error.
type fakeToggle struct {
	calls []int // account ids, in order
	err   error
}

func (f *fakeToggle) fn(accountID int) (bool, error) {
	f.calls = append(f.calls, accountID)
	return f.err == nil, f.err
}

// pinTUI builds a model with two accounts (cursor on acct-2, the worse one),
// an optional current pin, and a fake toggle.
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

// TestStatusTUIPinToggle drives the 'p' key end to end: the async toggle Cmd
// fires against the cursor's account, the busy debounce drops repeats, and the
// inert configurations issue nothing.
func TestStatusTUIPinToggle(t *testing.T) {
	t.Run("p toggles the cursor's account", func(t *testing.T) {
		ft := &fakeToggle{}
		tui, cmd := pressP(t, pinTUI("/proj", dirPin{}, ft))
		if !tui.pinBusy || cmd == nil {
			t.Fatalf("p must mark busy and return a Cmd, busy=%v cmd=%v", tui.pinBusy, cmd)
		}
		msg := cmd() // run the toggle synchronously
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

// TestStatusTUIPinErrorSurfaced: a failed toggle surfaces in the footer, keeps
// the model usable, and a subsequent success clears it.
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

	// Recovery: the next successful toggle clears the error.
	ft.err = nil
	tui, cmd = pressP(t, tui)
	model, _ = tui.Update(cmd())
	tui = model.(statusTUI)
	if tui.pinErr != nil {
		t.Fatalf("recovered toggle must clear the error, got %v", tui.pinErr)
	}
}

// TestStatusTUIDetailNeedsLoginPenalty: the detail pane renders the needs-login
// penalty row exactly when the scorer engaged it, matching the format of the
// adjacent rate-limited row, and omits it when the penalty is zero.
func TestStatusTUIDetailNeedsLoginPenalty(t *testing.T) {
	// The cursor sits on acct-2 (snaps[1]), so its Components drive renderDetail.
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

// TestStatusTUIViewShowsPin: the pinned account's row is badged, the detail
// pane names the pin when the cursor sits on it, the summary line renders, and
// the footer advertises 'p' only when a cwd exists.
func TestStatusTUIViewShowsPin(t *testing.T) {
	pin := dirPin{cwd: "/proj", ok: true, view: pool.PinView{
		AccountID: 2, Manual: true, Binding: true, ExpiresAt: time.Now().Add(30 * time.Minute),
	}}
	tui := pinTUI("/proj", pin, &fakeToggle{})
	tui.width = 120
	view := stripANSI(tui.View())

	// Cursor sits on bob, the pinned account: the footer names the release.
	if !strings.Contains(view, "p unpin") {
		t.Fatalf("footer must advertise unpinning the pinned account:\n%s", view)
	}
	// On an unpinned account the same key reads as a pin.
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
	// Cursor sits on bob (the pinned account): the detail pane names the pin.
	if !strings.Contains(view, "pinned to this directory (manual)") {
		t.Fatalf("detail pane must name the pin:\n%s", view)
	}

	// Without a cwd the key is hidden and no pin line renders.
	bare := pinTUI("", dirPin{}, &fakeToggle{})
	bare.width = 120
	view = stripANSI(bare.View())
	if strings.Contains(view, "p pin") || strings.Contains(view, "p unpin") || strings.Contains(view, "pinned ") {
		t.Fatalf("no-cwd view must hide pin UI:\n%s", view)
	}
}

// fakeLogin records buildLogin/finishLogin calls and returns injectable results.
type fakeLogin struct {
	built     []int // account ids passed to buildLogin
	finished  []int // account ids passed to finishLogin
	buildErr  error
	finishErr error
}

func (f *fakeLogin) build(a store.Account) (*exec.Cmd, error) {
	f.built = append(f.built, a.ID)
	if f.buildErr != nil {
		return nil, f.buildErr
	}
	// A valid *exec.Cmd that is never started — tests drive the messages the
	// ExecProcess callback would emit rather than running the process.
	return exec.Command("true"), nil
}

func (f *fakeLogin) finish(a store.Account) error {
	f.finished = append(f.finished, a.ID)
	return f.finishErr
}

// reloginTUI builds a model with a healthy acct-1 and a needs-login acct-2, the
// cursor on acct-2, and a fake login.
func reloginTUI(fl *fakeLogin) statusTUI {
	healthy := pool.Snapshot{Account: store.Account{ID: 1, Label: "alice@example.com"}, Score: 90, HasUsage: true}
	stale := pool.Snapshot{Account: store.Account{ID: 2, Label: "bob@example.com"}, Score: 50, HasUsage: true, NeedsLogin: true}
	return statusTUI{
		snaps:       []pool.Snapshot{healthy, stale},
		cursorID:    2,
		buildLogin:  fl.build,
		finishLogin: fl.finish,
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

// TestStatusTUIReloginAction drives the 'a' key: it builds a login for the
// needs-login account, the post-exit finish runs off the UI goroutine, the busy
// debounce drops repeats, and inert configurations issue nothing.
func TestStatusTUIReloginAction(t *testing.T) {
	bob := store.Account{ID: 2, Label: "bob@example.com"}

	t.Run("a re-logs in the needs-login account", func(t *testing.T) {
		fl := &fakeLogin{}
		tui, cmd := pressA(t, reloginTUI(fl))
		if !tui.reloginBusy || cmd == nil {
			t.Fatalf("a must mark busy and return a Cmd: busy=%v cmd=%v", tui.reloginBusy, cmd)
		}
		if len(fl.built) != 1 || fl.built[0] != 2 {
			t.Fatalf("buildLogin calls = %v, want [2]", fl.built)
		}
		// Simulate claude exiting cleanly; driving the message directly avoids
		// running the real ExecProcess child.
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
		tui.cursorID = 1 // alice, not flagged
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
			cursorID:    7,
			buildLogin:  fl.build,
			finishLogin: fl.finish,
		}
		got, cmd := pressA(t, tui)
		if !got.reloginBusy || cmd == nil {
			t.Fatalf("a on a stale rate-limited account must not be inert: busy=%v cmd=%v", got.reloginBusy, cmd)
		}
		if len(fl.built) != 1 || fl.built[0] != 7 {
			t.Fatalf("buildLogin calls = %v, want [7]", fl.built)
		}
	})

	t.Run("build error surfaces without starting a login", func(t *testing.T) {
		fl := &fakeLogin{buildErr: errors.New("`claude` not found on PATH")}
		tui, cmd := pressA(t, reloginTUI(fl))
		if cmd != nil {
			t.Fatal("a build error must not start a login")
		}
		if tui.reloginBusy || tui.reloginErr == nil {
			t.Fatalf("build error must stay un-busy and record the error: busy=%v err=%v", tui.reloginBusy, tui.reloginErr)
		}
	})
}

// TestStatusTUIReloginErrorSurfaced: a failed finish surfaces in the footer and
// keeps the model usable, and a process error short-circuits the finish step.
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

	// Recovery: the next successful re-login clears the error and refreshes.
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

// TestStatusTUIReloginFooter: the 'a re-login' hint shows only when the cursor
// sits on a needs-login account.
func TestStatusTUIReloginFooter(t *testing.T) {
	tui := reloginTUI(&fakeLogin{})
	tui.width = 120
	if v := stripANSI(tui.View()); !strings.Contains(v, "a re-login") {
		t.Fatalf("footer must advertise re-login on a needs-login account:\n%s", v)
	}
	tui.cursorID = 1 // alice, healthy
	if v := stripANSI(tui.View()); strings.Contains(v, "a re-login") {
		t.Fatalf("footer must hide re-login on a healthy account:\n%s", v)
	}
}
