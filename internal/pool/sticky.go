package pool

import (
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
)

// StickyTTL is how long a pin outlives its last activity. One hour tracks
// Claude's prompt-cache lifetime. Var so tests can tune it.
var StickyTTL = time.Hour

// StickyOutcome is how a select must treat the cwd's pin.
type StickyOutcome int

const (
	// StickyMiss means no pin worth honoring — rank freely and record the winner.
	StickyMiss StickyOutcome = iota
	// StickyBind means launch on the pinned account.
	StickyBind
	// StickyHold means the pin is alive but only a live session carries it; a new
	// session ranks freely (no repoint), and it binds for a TTL after the session ends.
	StickyHold
	// StickyHoldManual is a manual pin whose account is temporarily unusable —
	// rank freely, keep the pin, and surface the bypass to the user.
	StickyHoldManual
)

// Held reports whether the pin survives this select without binding it. Held
// selects must not record the winner over the pin.
func (o StickyOutcome) Held() bool { return o == StickyHold || o == StickyHoldManual }

// pinState classifies a pin's activity-based lifecycle, independent of account
// usability. All signals are scoped to the pinned account in the pinned cwd —
// the prompt cache belongs to that account alone.
type pinState struct {
	live         bool      // a tracked session is running on the pinned account in the cwd
	warm         bool      // such a session ended within StickyTTL
	lastActivity time.Time // max(selected_at, latest tracked session end)
}

func (p pinState) alive(now time.Time) bool {
	return p.live || now.Sub(p.lastActivity) <= StickyTTL
}

// binding reports whether the activity rules bind a new session to the pin:
// manual pins bind whenever alive; auto pins bind on a warm ended session
// (something resumable exists) or when no tracked session carries the directory
// at all — the no-data fallback for pid-0 `ccp select` flows.
func (p pinState) binding(manual bool, now time.Time) bool {
	switch {
	case !p.alive(now):
		return false
	case manual || p.warm:
		return true
	default:
		return !p.live
	}
}

// classify reads the pin's account-scoped cwd activity. Best-effort: a store
// error degrades to zero activity (selected_at-only freshness), never a failed
// select.
func (m *Manager) classify(st store.Sticky, now time.Time) pinState {
	act, err := m.Store.GetCwdActivity(st.Cwd, st.AccountID)
	if err != nil {
		act = store.CwdActivity{}
	}
	last := st.SelectedAt
	if act.LastEnded.After(last) {
		last = act.LastEnded
	}
	return pinState{
		live:         act.Live > 0,
		warm:         !act.LastEnded.IsZero() && now.Sub(act.LastEnded) <= StickyTTL,
		lastActivity: last,
	}
}

// StickyPick decides how cwd's pin applies to this select. The returned
// Result is the pin's entry in ranked, meaningful for every outcome but Miss.
// Best-effort by design: store errors read as a miss, never a failed select.
func (m *Manager) StickyPick(cwd string, ranked []score.Result, now time.Time) (score.Result, StickyOutcome) {
	if cwd == "" {
		return score.Result{}, StickyMiss
	}
	st, ok, err := m.Store.GetSticky(cwd)
	if err != nil || !ok {
		return score.Result{}, StickyMiss
	}
	ps := m.classify(st, now)
	if !ps.alive(now) {
		// Expired but unpruned — the daemonless path has no pruner, and an expired
		// manual row would block UpsertSticky forever. Version-guarded so a newer
		// concurrent pin is never erased on this stale read. Best-effort.
		_ = m.Store.DeleteStickyVersion(cwd, st.SelectedAt, st.Manual)
		return score.Result{}, StickyMiss
	}
	for _, r := range ranked {
		if r.AccountID != st.AccountID {
			continue
		}
		switch {
		case !score.UsableForSticky(r):
			if st.Manual {
				return r, StickyHoldManual
			}
			return score.Result{}, StickyMiss
		case ps.binding(st.Manual, now):
			return r, StickyBind
		default:
			return r, StickyHold
		}
	}
	// The sticky account was removed from the pool.
	return score.Result{}, StickyMiss
}

// RecordSticky upserts the select-path sticky record for cwd, refreshing the
// sliding activity clock. It can never repoint or downgrade a manual pin (see
// store.UpsertSticky). An empty cwd is a no-op.
func (m *Manager) RecordSticky(cwd string, accountID int, now time.Time) error {
	if cwd == "" {
		return nil
	}
	return m.Store.UpsertSticky(cwd, accountID, now)
}

// PinManual pins cwd to accountID so every select there binds to it (while the
// account can serve) until one StickyTTL past its last activity. Unlike the
// best-effort select path, explicit pin edits fail loudly.
func (m *Manager) PinManual(cwd string, accountID int, now time.Time) error {
	if cwd == "" {
		return errors.New("pin: empty working directory")
	}
	if _, err := m.Store.GetAccount(accountID); err != nil {
		return fmt.Errorf("pin %s: %w", cwd, err)
	}
	return m.Store.PinManual(cwd, accountID, now)
}

// Unpin removes cwd's pin (manual or auto). Idempotent on a missing pin.
func (m *Manager) Unpin(cwd string) error {
	if cwd == "" {
		return errors.New("unpin: empty working directory")
	}
	return m.Store.DeleteSticky(cwd)
}

// TogglePin pins cwd to accountID, or unpins when already pinned to that account
// (manual or auto). An expired-but-unpruned pin counts as absent, mirroring
// StickyPick: pressing pin on a dead pin must pin, not silently unpin. Returns
// the resulting pinned state.
func (m *Manager) TogglePin(cwd string, accountID int, now time.Time) (bool, error) {
	if cwd == "" {
		return false, errors.New("pin: empty working directory")
	}
	st, ok, err := m.Store.GetSticky(cwd)
	if err != nil {
		return false, fmt.Errorf("pin %s: %w", cwd, err)
	}
	if ok && st.AccountID == accountID && m.classify(st, now).alive(now) {
		return false, m.Unpin(cwd)
	}
	return true, m.PinManual(cwd, accountID, now)
}

// PinView is cwd's pin as display input.
type PinView struct {
	AccountID int
	Manual    bool
	PinnedAt  time.Time // last pin activity (the record's selected_at)
	// ExpiresAt is when the pin dies absent further activity; zero while a
	// live tracked session holds it open.
	ExpiresAt time.Time
	Live      bool // a tracked session is running in the pinned directory
	// Binding reports whether the activity rules would bind a new session to the
	// pin now. Activity-only: usability is judged at select time, so an exhausted
	// account can read Binding here yet still be held or abandoned by the select.
	Binding bool
}

// PinView returns cwd's pin for display, ok=false when the directory has no
// pin or only an expired one — the TUI must never show a pin the selector
// would miss.
func (m *Manager) PinView(cwd string, now time.Time) (PinView, bool, error) {
	if cwd == "" {
		return PinView{}, false, nil
	}
	st, ok, err := m.Store.GetSticky(cwd)
	if err != nil {
		return PinView{}, false, fmt.Errorf("read pin for %s: %w", cwd, err)
	}
	if !ok {
		return PinView{}, false, nil
	}
	ps := m.classify(st, now)
	if !ps.alive(now) {
		return PinView{}, false, nil
	}
	pv := PinView{
		AccountID: st.AccountID,
		Manual:    st.Manual,
		PinnedAt:  st.SelectedAt,
		Live:      ps.live,
		Binding:   ps.binding(st.Manual, now),
	}
	if !ps.live {
		pv.ExpiresAt = ps.lastActivity.Add(StickyTTL)
	}
	return pv, true, nil
}
