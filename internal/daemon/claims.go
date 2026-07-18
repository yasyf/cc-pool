package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

var errAccountConverting = errors.New("account is converting overlays")

// claims is the daemon's account-claim discipline in one type: the short-lived
// select reservation plus the poll, convert, and pool-wide claims that keep a
// conversion's move/teardown/mount from interleaving with a select, a poll, or
// another conversion. It is the vocabulary every destructive-convergence site
// shares:
//
//	reserve      — a select's short-lived hold (expires after reservationTTL)
//	hold/disownHold        — a scheduler/reconcile poll claim on the dir
//	own/disownConvert      — a standalone overlay-conversion claim
//	ownHeld                — a conversion claim taken under a held poll claim
//	held / reservedCount   — the claim queries the select/poll paths read
//
// ownFresh (a Server method — it also touches the store) is the sanctioned
// claim-then-re-read for destructive convergence.
//
// Locking: claims owns its mutex; each operation is a self-contained critical
// section, and the claim flag — never the mutex — owns the account across the I/O
// that follows (the mutex is never held across a move, mount, or teardown). This
// preserves the exact convention the state used as Server fields under s.mu.
type claims struct {
	mu         sync.Mutex
	selections map[string]*selectionReservation
	converting map[int]bool // accountID -> overlay conversion in flight
	polling    map[int]bool // accountID -> scheduler/reconcile owns the dir this iteration
	changed    chan struct{}
	now        func() time.Time
}

type reservation struct {
	accountID         int
	accountInstanceID string
	accountGeneration uint64
	createdAt         time.Time
}

type selectionLaunch struct {
	pid              int
	processStartedAt time.Time
	cwd              string
	recordSticky     bool
}

type selectionState uint8

const (
	selectionPending selectionState = iota
	selectionCommitting
	selectionTerminal
)

type selectionReservation struct {
	reservation
	launch     selectionLaunch
	expiresAt  time.Time
	state      selectionState
	done       chan struct{}
	response   Response
	terminalAt time.Time
}

// newClaims builds an empty claims store.
func newClaims() *claims {
	return &claims{
		selections: map[string]*selectionReservation{},
		converting: map[int]bool{},
		polling:    map[int]bool{},
		changed:    make(chan struct{}),
		now:        time.Now,
	}
}

// reserve records a short-lived reservation for an account, refusing while an
// overlay conversion holds it (it is about to remake the dir a launching claude
// would land in).
func (c *claims) reserve(id int) bool {
	_, err := c.beginSelection(
		store.Account{ID: id, InstanceID: fmt.Sprintf("test-%d", id), Generation: 1},
		selectionLaunch{}, reservationTTL,
	)
	return err == nil
}

func (c *claims) beginSelection(account store.Account, launch selectionLaunch, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		return "", errors.New("reserve account: positive ttl is required")
	}
	if launch.pid > 0 && launch.processStartedAt.IsZero() {
		return "", errors.New("reserve account: process start time is required")
	}
	if launch.pid <= 0 && !launch.processStartedAt.IsZero() {
		return "", errors.New("reserve account: process start time requires a pid")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.pruneSelectionsLocked(now)
	if c.converting[account.ID] {
		return "", errAccountConverting
	}
	if account.ID <= 0 || account.InstanceID == "" || account.Generation == 0 {
		return "", errors.New("reserve account: complete account identity is required")
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate reservation token: %w", err)
	}
	token := hex.EncodeToString(raw[:])
	c.selections[token] = &selectionReservation{
		reservation: reservation{
			accountID: account.ID, accountInstanceID: account.InstanceID,
			accountGeneration: account.Generation, createdAt: now,
		},
		launch: launch, expiresAt: now.Add(ttl), state: selectionPending,
	}
	c.notifyChangedLocked()
	return token, nil
}

func (c *claims) commitSelection(ctx context.Context, token string, activate func(string, reservation, selectionLaunch) Response) Response {
	for {
		c.mu.Lock()
		c.pruneSelectionsLocked(c.now())
		selection, ok := c.selections[token]
		if !ok {
			c.mu.Unlock()
			return Response{OK: false, Error: "selection reservation is unknown or expired"}
		}
		switch selection.state {
		case selectionTerminal:
			resp := selection.response
			c.mu.Unlock()
			return resp
		case selectionCommitting:
			done := selection.done
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return Response{OK: false, Error: ctx.Err().Error()}
			case <-done:
			}
			continue
		case selectionPending:
			selection.state = selectionCommitting
			selection.done = make(chan struct{})
			reserved := selection.reservation
			launch := selection.launch
			c.mu.Unlock()

			resp := activate(token, reserved, launch)
			c.mu.Lock()
			selection.state = selectionTerminal
			selection.response = resp
			selection.terminalAt = c.now()
			close(selection.done)
			c.notifyChangedLocked()
			c.mu.Unlock()
			return resp
		default:
			c.mu.Unlock()
			panic("unknown selection reservation state")
		}
	}
}

func (c *claims) abortSelection(ctx context.Context, token string) Response {
	for {
		c.mu.Lock()
		c.pruneSelectionsLocked(c.now())
		selection, ok := c.selections[token]
		if !ok {
			c.mu.Unlock()
			return Response{OK: true}
		}
		if selection.state == selectionTerminal {
			c.mu.Unlock()
			return Response{OK: true}
		}
		if selection.state == selectionCommitting {
			done := selection.done
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return Response{OK: false, Error: ctx.Err().Error()}
			case <-done:
			}
			continue
		}
		delete(c.selections, token)
		c.notifyChangedLocked()
		c.mu.Unlock()
		return Response{OK: true}
	}
}

func (c *claims) abortReservation(token string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(c.now())
	selection, ok := c.selections[token]
	if !ok || selection.state != selectionPending {
		return false
	}
	delete(c.selections, token)
	c.notifyChangedLocked()
	return true
}

func (c *claims) pruneSelections(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(now)
}

func (c *claims) knowsSelection(token string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(c.now())
	_, ok := c.selections[token]
	return ok
}

func (c *claims) pruneSelectionsLocked(now time.Time) {
	changed := false
	for token, selection := range c.selections {
		switch selection.state {
		case selectionPending:
			if now.After(selection.expiresAt) {
				delete(c.selections, token)
				changed = true
			}
		case selectionTerminal:
			if now.Sub(selection.terminalAt) > provisionalSelectionTTL {
				delete(c.selections, token)
			}
		}
	}
	if changed {
		c.notifyChangedLocked()
	}
}

func (c *claims) beginReservation(account store.Account) (string, error) {
	return c.beginSelection(account, selectionLaunch{}, provisionalSelectionTTL)
}

// reservedCount returns the number of live reservations for an account.
func (c *claims) reservedCount(id int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	count := 0
	c.pruneSelectionsLocked(now)
	for _, selection := range c.selections {
		if selection.state != selectionTerminal && selection.accountID == id {
			count++
		}
	}
	return count
}

// hold claims an account for one scheduler/reconcile iteration — the
// Reconcile/fallback/refresh work that must never interleave with a conversion's
// move/teardown/mount. Unlike a convert claim, a poll claim does not hide the
// account from select (sessions can land on a dir being health-checked); it only
// excludes conversions, two-sidedly with own. The claim — not the mutex — owns
// the account across the iteration's I/O.
func (c *claims) hold(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.holdLocked(id)
}

func (c *claims) holdContext(ctx context.Context, id int) bool {
	for {
		c.mu.Lock()
		if c.holdLocked(id) {
			c.mu.Unlock()
			return true
		}
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

func (c *claims) holdLocked(id int) bool {
	if c.converting[id] || c.polling[id] {
		return false
	}
	if c.polling == nil {
		c.polling = map[int]bool{}
	}
	c.polling[id] = true
	return true
}

// disownHold releases a poll claim.
func (c *claims) disownHold(id int) {
	c.mu.Lock()
	delete(c.polling, id)
	c.notifyChangedLocked()
	c.mu.Unlock()
}

// own claims an account for overlay conversion iff it has no live reservation, no
// conversion in flight, and no poll mid-iteration on its dir. The check-and-claim
// is one critical section (closing the race against reserve and hold); the
// converting flag — not the mutex — then owns the account across the conversion's
// I/O.
func (c *claims) own(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reservedLocked(id, c.now()) {
		return false
	}
	if c.converting[id] || c.polling[id] {
		return false
	}
	if c.converting == nil {
		c.converting = map[int]bool{}
	}
	c.converting[id] = true
	return true
}

// ownHeld claims an account for a conversion run from inside a poll iteration (the
// fuse→symlink fallback) iff it has no live reservation and no conversion in
// flight. Unlike own it tolerates the caller's own poll claim (healFuse runs under
// one); once converting is set, reserve refuses for the whole ConvertOverlay,
// closing the claim→convert window against select. Callers must hold the account's
// poll claim so two conversions never interleave.
func (c *claims) ownHeld(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reservedLocked(id, c.now()) {
		return false
	}
	if c.converting[id] {
		return false
	}
	if c.converting == nil {
		c.converting = map[int]bool{}
	}
	c.converting[id] = true
	return true
}

func (c *claims) reservedLocked(id int, now time.Time) bool {
	c.pruneSelectionsLocked(now)
	for _, selection := range c.selections {
		if selection.state != selectionTerminal && selection.accountID == id {
			return true
		}
	}
	return false
}

// disownConvert releases a conversion claim (own or ownHeld).
func (c *claims) disownConvert(id int) {
	c.mu.Lock()
	delete(c.converting, id)
	c.notifyChangedLocked()
	c.mu.Unlock()
}

func (c *claims) notifyChangedLocked() {
	if c.changed == nil {
		c.changed = make(chan struct{})
		return
	}
	close(c.changed)
	c.changed = make(chan struct{})
}

// held reports whether an overlay conversion holds the account.
func (c *claims) held(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.converting[id]
}

// ownFresh takes the standalone convert claim on id, then re-reads the row under
// it — the sanctioned claim-then-re-read for destructive convergence, since a
// caller's listed row is a stale snapshot the claim now stabilizes. ok=false means
// the claim was refused (a pending select, a poll, or another conversion) and is
// NOT held. On ok=true the claim IS held (caller must disownConvert); a non-nil
// err is a row re-read failure (caller disowns and reports).
func (s *Server) ownFresh(id int) (fresh store.Account, ok bool, err error) {
	if !s.cl.own(id) {
		return store.Account{}, false, nil
	}
	fresh, err = s.m.Store.GetAccount(id)
	return fresh, true, err
}
