package daemon

import "time"

// ledger is one (policy, resource) self-heal record — the shape every self-heal
// family folds onto. It runs a policy's two phases:
//
//   - Phase 1 (debounce): strike accumulates the primary lane toward the
//     policy's debounce; the strike that reaches it latches faulted (or
//     forceFault latches it immediately). The lane resets on the latch so the
//     recovery ladder counts from zero.
//   - Phase 2 (recovery ladder): attempt advances the shared backoff clock
//     (attempts) and charges one of two mutually-resetting breaker lanes —
//     strikes (primary) or altHits (alt). A lane reaching its threshold parks
//     the ledger (onTrip). attempts alone spaces the next attempt (nextDue).
//
// The two-lane, shared-clock shape is fuse.remount's incident contract: hazard
// and TCC outcomes each reset the other's lane, so an alternating row trips
// neither breaker while the shared attempts clock still spaces every remount.
type ledger struct {
	// strikes is the primary lane: the debounce count before the fault latches,
	// then the primary breaker lane during recovery.
	strikes int
	// faulted latches the debounce verdict (wedged / needs-login / forced). It
	// persists across recovery attempts — a parked resource stays faulted.
	faulted bool
	// attempts is the shared backoff clock: every recovery attempt, whatever its
	// lane, so the backoff spacing never resets on alternating outcome kinds.
	attempts int
	// altHits is the alt breaker lane (e.g. TCC), mutually-resetting with strikes.
	altHits int
	// nextDue is the earliest time the next attempt may run (attempts × backoff).
	nextDue time.Time
	lastErr error
	lastAt  time.Time
}

// attemptKind selects which breaker lane a recovery attempt charges. The primary
// and alt lanes mutually reset, so alternating kinds trip neither.
type attemptKind int

const (
	// attemptPrimary charges the strikes lane and resets altHits — the hazard /
	// remount-failure outcome.
	attemptPrimary attemptKind = iota
	// attemptAlt charges the altHits lane and resets strikes — the TCC-block
	// outcome.
	attemptAlt
	// attemptNeutral advances only the shared clock and resets both lanes — a
	// benign deferral (busy / unmitigated) that must reach no breaker.
	attemptNeutral
)

// strike records one debounce failure. Until the ledger faults, strikes
// accumulate toward the policy's debounce; the strike that reaches it latches
// the fault and resets the lane for the recovery ladder. A strike after the
// fault only refreshes lastErr/lastAt — the verdict already stands, so continued
// failures never advance the breaker lane.
func (l *ledger) strike(p policy, now time.Time, err error) {
	l.lastErr, l.lastAt = err, now
	if l.faulted {
		return
	}
	l.strikes++
	if p.debounce > 0 && l.strikes >= p.debounce {
		l.faulted = true
		l.strikes = 0
	}
}

// forceFault latches the fault immediately, bypassing the debounce — the
// select-time forceWedge shape, where a launching session has no live reads a
// false positive could orphan. It resets the primary lane so recovery starts
// clean.
func (l *ledger) forceFault(now time.Time, err error) {
	l.faulted = true
	l.strikes = 0
	l.lastErr, l.lastAt = err, now
}

// attempt books one recovery attempt: it advances the shared backoff clock (and
// spaces the next attempt), charges the selected breaker lane while resetting the
// other, and stamps lastAt. It returns whether the attempt parked a breaker.
func (l *ledger) attempt(p policy, kind attemptKind, now time.Time) (parked bool) {
	l.attempts++
	switch kind {
	case attemptPrimary:
		l.strikes++
		l.altHits = 0
	case attemptAlt:
		l.altHits++
		l.strikes = 0
	case attemptNeutral:
		l.strikes, l.altHits = 0, 0
	}
	l.nextDue = now.Add(p.backoff.After(l.attempts))
	l.lastAt = now
	return l.parked(p)
}

// clear resets the ledger to healthy — the resource recovered. Both the
// debounce verdict and the recovery ladder are dropped.
func (l *ledger) clear() { *l = ledger{} }

// parked reports whether a breaker has tripped: the primary lane reached the
// policy's breaker, or the alt lane reached alt. onTrip names what the consumer
// then does (gate / park / retreat).
func (l *ledger) parked(p policy) bool {
	return (p.breaker > 0 && l.strikes >= p.breaker) || (p.alt > 0 && l.altHits >= p.alt)
}

// due reports whether another recovery attempt is warranted now: no breaker has
// tripped and the backoff since the last attempt has elapsed.
func (l *ledger) due(p policy, now time.Time) bool {
	return !l.parked(p) && !now.Before(l.nextDue)
}

// gateOpen reports whether a gated operation may proceed: the ledger is neither
// faulted nor parked and its backoff window has elapsed. Rate-limit streaks gate
// on the backoff window; auth streaks gate on the fault.
func (l *ledger) gateOpen(p policy, now time.Time) bool {
	return !l.faulted && !l.parked(p) && !now.Before(l.nextDue)
}

// ledgerKey identifies one ledger: a policy name plus a resource (an account
// dir, an account id, or "pool").
type ledgerKey struct {
	policy   string
	resource string
}

// ledgerSnapshot is one ledger's state for the status wire and diagnostics.
type ledgerSnapshot struct {
	Policy   string
	Resource string
	Strikes  int
	Faulted  bool
	Attempts int
	AltHits  int
	Parked   bool
	NextDue  time.Time
	LastErr  string
	LastAt   time.Time
}

// ledgers is the Server-held store of self-heal ledgers, keyed by (policy,
// resource). Every self-heal family shares it, replacing the separately-invented
// strike/backoff/breaker maps (rowRetry, fpState, the auth/rate-limit streaks).
//
// Locking: ledgers carries NO internal mutex — the convention of the rowRetry
// map it subsumes. The owning Server serializes access: heal- or
// scheduler-goroutine exclusivity per the family, and an enclosing lock once a
// row is shared across goroutines (the wiring the ports add, at the Server, not
// here). Standalone tests drive it single-threaded.
type ledgers struct {
	m map[ledgerKey]*ledger
}

// newLedgers builds an empty ledgers store.
func newLedgers() *ledgers { return &ledgers{m: map[ledgerKey]*ledger{}} }

// row returns dir's ledger for p, creating it on first touch.
func (ls *ledgers) row(p policy, resource string) *ledger {
	k := ledgerKey{p.name, resource}
	l := ls.m[k]
	if l == nil {
		l = &ledger{}
		ls.m[k] = l
	}
	return l
}

// peek returns the existing ledger for (p, resource) or nil — the read path that
// never allocates, so absent resources answer the healthy default.
func (ls *ledgers) peek(p policy, resource string) *ledger {
	return ls.m[ledgerKey{p.name, resource}]
}

// strike records one debounce failure for (p, resource); see ledger.strike.
func (ls *ledgers) strike(p policy, resource string, now time.Time, err error) {
	ls.row(p, resource).strike(p, now, err)
}

// forceFault latches (p, resource) faulted immediately; see ledger.forceFault.
func (ls *ledgers) forceFault(p policy, resource string, now time.Time, err error) {
	ls.row(p, resource).forceFault(now, err)
}

// attempt books one recovery attempt for (p, resource), returning whether it
// parked a breaker; see ledger.attempt.
func (ls *ledgers) attempt(p policy, resource string, kind attemptKind, now time.Time) bool {
	return ls.row(p, resource).attempt(p, kind, now)
}

// clear drops (p, resource): the resource recovered.
func (ls *ledgers) clear(p policy, resource string) {
	delete(ls.m, ledgerKey{p.name, resource})
}

// faulted reports whether (p, resource) has latched its fault verdict. An absent
// ledger is not faulted.
func (ls *ledgers) faulted(p policy, resource string) bool {
	l := ls.peek(p, resource)
	return l != nil && l.faulted
}

// due reports whether (p, resource) is due for a recovery attempt. An absent
// ledger (never attempted) is immediately due.
func (ls *ledgers) due(p policy, resource string, now time.Time) bool {
	l := ls.peek(p, resource)
	return l == nil || l.due(p, now)
}

// parked reports whether (p, resource) has tripped a breaker. An absent ledger
// is not parked.
func (ls *ledgers) parked(p policy, resource string) bool {
	l := ls.peek(p, resource)
	return l != nil && l.parked(p)
}

// gateOpen reports whether a gated operation on (p, resource) may proceed. An
// absent ledger is open.
func (ls *ledgers) gateOpen(p policy, resource string, now time.Time) bool {
	l := ls.peek(p, resource)
	return l == nil || l.gateOpen(p, now)
}

// snapshot lists every live ledger for the status wire, taken by the caller
// under the Server's mu so the wire sees a consistent view.
func (ls *ledgers) snapshot() []ledgerSnapshot {
	out := make([]ledgerSnapshot, 0, len(ls.m))
	for k, l := range ls.m {
		s := ledgerSnapshot{
			Policy: k.policy, Resource: k.resource,
			Strikes: l.strikes, Faulted: l.faulted, Attempts: l.attempts, AltHits: l.altHits,
			Parked: l.parked(policies[k.policy]), NextDue: l.nextDue, LastAt: l.lastAt,
		}
		if l.lastErr != nil {
			s.LastErr = l.lastErr.Error()
		}
		out = append(out, s)
	}
	return out
}

// prune drops p's ledgers whose resource keep rejects — the per-pass hygiene the
// heal loop runs when a resource leaves the live set.
func (ls *ledgers) prune(p policy, keep func(resource string) bool) {
	for k := range ls.m {
		if k.policy == p.name && !keep(k.resource) {
			delete(ls.m, k)
		}
	}
}
