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
	mu           sync.Mutex
	reservations map[string]reservation
	pending      map[string]reservation
	converting   map[int]bool // accountID -> overlay conversion in flight
	polling      map[int]bool // accountID -> scheduler/reconcile owns the dir this iteration
	changed      chan struct{}
}

type reservation struct {
	accountID int
	createdAt time.Time
}

// newClaims builds an empty claims store.
func newClaims() *claims {
	return &claims{
		reservations: map[string]reservation{},
		pending:      map[string]reservation{},
		converting:   map[int]bool{},
		polling:      map[int]bool{},
		changed:      make(chan struct{}),
	}
}

// reserve records a short-lived reservation for an account, refusing while an
// overlay conversion holds it (it is about to remake the dir a launching claude
// would land in).
func (c *claims) reserve(id int) bool {
	token, err := c.beginReservation(id)
	if err != nil {
		return false
	}
	_, ok := c.commitReservation(token)
	return ok
}

func (c *claims) beginReservation(id int) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.converting[id] {
		return "", errAccountConverting
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate reservation token: %w", err)
	}
	token := hex.EncodeToString(raw[:])
	c.pending[token] = reservation{accountID: id, createdAt: time.Now()}
	return token, nil
}

func (c *claims) commitReservation(token string) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	r, ok := c.liveReservation(token, now)
	if !ok {
		return 0, false
	}
	delete(c.pending, token)
	r.createdAt = now
	c.reservations[token] = r
	return r.accountID, true
}

func (c *claims) releaseCommittedReservation(token string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.reservations[token]
	delete(c.reservations, token)
	return ok
}

func (c *claims) abortReservation(token string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.liveReservation(token, time.Now())
	delete(c.pending, token)
	return ok
}

func (c *claims) liveReservation(token string, now time.Time) (reservation, bool) {
	r, ok := c.pending[token]
	if !ok || now.Sub(r.createdAt) > provisionalSelectionTTL {
		delete(c.pending, token)
		return reservation{}, false
	}
	return r, true
}

// reservedCount returns the number of live reservations for an account.
func (c *claims) reservedCount(id int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	count := 0
	for token, r := range c.pending {
		if now.Sub(r.createdAt) > provisionalSelectionTTL {
			delete(c.pending, token)
			continue
		}
		if r.accountID == id {
			count++
		}
	}
	for token, r := range c.reservations {
		if now.Sub(r.createdAt) > reservationTTL {
			delete(c.reservations, token)
			continue
		}
		if r.accountID == id {
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
	if c.reservedLocked(id, time.Now()) {
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
	if c.reservedLocked(id, time.Now()) {
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
	for token, r := range c.pending {
		if now.Sub(r.createdAt) > provisionalSelectionTTL {
			delete(c.pending, token)
			continue
		}
		if r.accountID == id {
			return true
		}
	}
	for token, r := range c.reservations {
		if now.Sub(r.createdAt) <= reservationTTL {
			if r.accountID == id {
				return true
			}
			continue
		}
		delete(c.reservations, token)
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
