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
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/fusekit/catalogproto"
)

var errAccountExclusive = errors.New("account is under exclusive maintenance")

// claims owns short-lived selection reservations and the bookkeeping-only
// exclusion used to install durable account-removal intent:
//
//	reserve      — a select's short-lived hold (expires after reservationTTL)
//	ownExclusive/releaseExclusive — removal-intent bookkeeping exclusion
//	held / reservedCount   — reservation and removal queries
//
// ownFresh (a Server method — it also touches the store) is the sanctioned
// claim-then-re-read for destructive convergence.
//
// Locking: claims owns its mutex; each operation is a self-contained critical
// section. Reservations are bookkeeping records, not held locks. The separate
// credential-write exclusion may span Keychain I/O only after an exact pending
// reservation proves that selection owns the mutation.
type claims struct {
	mu         sync.Mutex
	selections map[string]*selectionReservation
	exclusive  map[int]bool // accountID -> removal intent being installed
	now        func() time.Time
}

type reservation struct {
	accountID         int
	accountInstanceID string
	accountGeneration uint64
	createdAt         time.Time
	expiresAt         time.Time
	preparation       *catalogproto.TenantPreparationProof
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

type liveClaimCounts struct {
	reservations int
	exclusive    int
}

// newClaims builds an empty claims store.
func newClaims() *claims {
	return &claims{
		selections: map[string]*selectionReservation{},
		exclusive:  map[int]bool{},
		now:        time.Now,
	}
}

// reserve records a short-lived reservation for an account, refusing while an
// exclusive credential mutation owns it.
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
	if c.exclusive[account.ID] {
		return "", errAccountExclusive
	}
	if account.ID <= 0 || account.InstanceID == "" || account.Generation == 0 {
		return "", errors.New("reserve account: complete account identity is required")
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate reservation token: %w", err)
	}
	token := hex.EncodeToString(raw[:])
	expiresAt := now.Add(ttl)
	c.selections[token] = &selectionReservation{
		reservation: reservation{
			accountID: account.ID, accountInstanceID: account.InstanceID,
			accountGeneration: account.Generation, createdAt: now, expiresAt: expiresAt,
		},
		launch: launch, expiresAt: expiresAt, state: selectionPending,
	}
	return token, nil
}

func (c *claims) ownSelectionExclusive(token string, accountID int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(c.now())
	selection, ok := c.selections[token]
	if !ok || selection.state != selectionPending || selection.accountID != accountID || c.exclusive[accountID] {
		return false
	}
	c.exclusive[accountID] = true
	return true
}

func (c *claims) commitSelection(ctx context.Context, token string, activate func(context.Context, string, reservation, selectionLaunch) Response) Response {
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

			resp := activate(ctx, token, reserved, launch)
			c.mu.Lock()
			selection.state = selectionTerminal
			selection.response = resp
			selection.terminalAt = c.now()
			close(selection.done)
			c.mu.Unlock()
			return resp
		default:
			c.mu.Unlock()
			panic("unknown selection reservation state")
		}
	}
}

func (c *claims) bindPreparation(token string, proof catalogproto.TenantPreparationProof) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(c.now())
	selection, ok := c.selections[token]
	if !ok || selection.state != selectionPending {
		return false
	}
	selection.preparation = &proof
	return true
}

func (c *claims) preparationLease(token string) (tenantfs.PreparationLease, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(c.now())
	selection, ok := c.selections[token]
	if !ok || selection.state != selectionPending {
		return tenantfs.PreparationLease{}, false
	}
	return tenantfs.PreparationLease{ID: token, ExpiresAt: selection.expiresAt}, true
}

func (c *claims) abortSelection(
	ctx context.Context,
	token string,
	release func(context.Context, reservation) error,
) Response {
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
		selection.state = selectionCommitting
		selection.done = make(chan struct{})
		reserved := selection.reservation
		done := selection.done
		c.mu.Unlock()
		err := release(ctx, reserved)
		c.mu.Lock()
		delete(c.selections, token)
		close(done)
		c.mu.Unlock()
		if err != nil {
			return Response{OK: false, Error: err.Error()}
		}
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
	for token, selection := range c.selections {
		switch selection.state {
		case selectionPending:
			if now.After(selection.expiresAt) {
				delete(c.selections, token)
			}
		case selectionTerminal:
			if now.Sub(selection.terminalAt) > provisionalSelectionTTL {
				delete(c.selections, token)
			}
		}
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

func (c *claims) liveCounts() liveClaimCounts {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneSelectionsLocked(c.now())
	counts := liveClaimCounts{exclusive: len(c.exclusive)}
	for _, selection := range c.selections {
		if selection.state != selectionTerminal {
			counts.reservations++
		}
	}
	return counts
}

// ownExclusive claims an account while durable removal intent is installed.
func (c *claims) ownExclusive(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reservedLocked(id, c.now()) {
		return false
	}
	if c.exclusive[id] {
		return false
	}
	if c.exclusive == nil {
		c.exclusive = map[int]bool{}
	}
	c.exclusive[id] = true
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

// releaseExclusive releases an exclusive account claim.
func (c *claims) releaseExclusive(id int) {
	c.mu.Lock()
	delete(c.exclusive, id)
	c.mu.Unlock()
}

// held reports whether exclusive maintenance holds the account.
func (c *claims) held(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exclusive[id]
}

// ownFresh takes the standalone exclusive claim on id, then re-reads the row under
// it — the sanctioned claim-then-re-read for destructive convergence, since a
// caller's listed row is a stale snapshot the claim now stabilizes. ok=false means
// the claim was refused (a pending select or another removal intent) and is
// NOT held. On ok=true the claim IS held (caller must releaseExclusive); a non-nil
// err is a row re-read failure (caller disowns and reports). Destructive lifecycle
// callers may install durable intent while held, but must release before external I/O.
func (s *Server) ownFresh(id int) (fresh store.Account, ok bool, err error) {
	if !s.cl.ownExclusive(id) {
		return store.Account{}, false, nil
	}
	fresh, err = s.m.Store.GetAccount(id)
	return fresh, true, err
}
