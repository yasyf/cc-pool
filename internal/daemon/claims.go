package daemon

import (
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

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
//	ownPool/disownPool     — the pool-wide native-mux-root force-unmount claim
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
	reservations map[int]time.Time // accountID -> select reserved-at
	converting   map[int]bool      // accountID -> overlay conversion in flight
	polling      map[int]bool      // accountID -> scheduler/reconcile owns the dir this iteration
	// nativeRecovering is a pool-wide claim: a force-unmount of the shared native
	// mux root is in flight, so reserve refuses EVERY account for its span (the
	// unmount drops every subtree). Set under mu across the scan→unmount→cache-
	// invalidate span, the same window-close as a per-account convert claim.
	nativeRecovering bool
}

// newClaims builds an empty claims store.
func newClaims() *claims {
	return &claims{
		reservations: map[int]time.Time{},
		converting:   map[int]bool{},
		polling:      map[int]bool{},
	}
}

// reserve records a short-lived reservation for an account, refusing while an
// overlay conversion or a pool-wide recovery holds it (both are about to remake
// the dir a launching claude would land in).
func (c *claims) reserve(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.converting[id] || c.nativeRecovering {
		return false
	}
	c.reservations[id] = time.Now()
	return true
}

// reservedCount returns the number of live reservations for an account.
func (c *claims) reservedCount(id int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.reservations[id]
	if !ok {
		return 0
	}
	if time.Since(t) > reservationTTL {
		delete(c.reservations, id)
		return 0
	}
	return 1
}

// hold claims an account for one scheduler/reconcile iteration — the
// Sync/Setup/fallback/refresh work that must never interleave with a conversion's
// move/teardown/mount. Unlike a convert claim, a poll claim does not hide the
// account from select (sessions can land on a dir being health-checked); it only
// excludes conversions, two-sidedly with own. The claim — not the mutex — owns
// the account across the iteration's I/O.
func (c *claims) hold(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	if t, ok := c.reservations[id]; ok && time.Since(t) <= reservationTTL {
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
	if t, ok := c.reservations[id]; ok && time.Since(t) <= reservationTTL {
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

// disownConvert releases a conversion claim (own or ownHeld).
func (c *claims) disownConvert(id int) {
	c.mu.Lock()
	delete(c.converting, id)
	c.mu.Unlock()
}

// held reports whether an overlay conversion holds the account.
func (c *claims) held(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.converting[id]
}

// ownPool claims the pool for a shared native mux-root force-unmount: it refuses
// if another recovery already holds the claim or any listed fuse account holds a
// live reservation (a select is launching onto a subtree of the root right now),
// else sets nativeRecovering so reserve refuses new reservations for the whole
// force-unmount span. The reservation scan and the flag set are one critical
// section — the same window-close own uses against a racing select. Callers pair
// it with disownPool.
func (c *claims) ownPool(fuse []store.Account) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nativeRecovering {
		// An overlapping sweep (holder-loss vs startup/periodic) coalesces: the
		// in-flight one owns the span, and its disownPool must not be undercut by a
		// second claimant releasing early.
		return false
	}
	for _, a := range fuse {
		if t, ok := c.reservations[a.ID]; ok && time.Since(t) <= reservationTTL {
			return false
		}
	}
	c.nativeRecovering = true
	return true
}

// disownPool releases the pool-wide native-recovery claim.
func (c *claims) disownPool() {
	c.mu.Lock()
	c.nativeRecovering = false
	c.mu.Unlock()
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
