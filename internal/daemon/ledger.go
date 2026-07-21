package daemon

import "time"

// ledger is one product auth or rate-limit gate record.
type ledger struct {
	strikes  int
	faulted  bool
	attempts int
	// nextDue is the earliest time the next attempt may run (attempts × backoff).
	nextDue time.Time
	lastErr error
	lastAt  time.Time
}

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

// stamp records the attempt clock and error without advancing the debounce or
// the recovery ladder — a definitive needs-login flags the persisted store
// verdict, not the transient-401 streak, yet its 15-minute poll cadence still
// engages off this clock.
func (l *ledger) stamp(now time.Time, err error) {
	l.lastErr, l.lastAt = err, now
}

// attempt books one rate-limit observation and advances its backoff clock.
func (l *ledger) attempt(p policy, now time.Time) {
	l.attempts++
	l.nextDue = now.Add(p.backoff.After(l.attempts))
	l.lastAt = now
}

// setNextDue overrides the backoff clock so the gate holds until t — the 429
// Retry-After hint replacing the computed exponential window.
func (l *ledger) setNextDue(t time.Time) { l.nextDue = t }

// due reports whether the product gate's backoff has elapsed.
func (l *ledger) due(now time.Time) bool {
	return !now.Before(l.nextDue)
}

// backoffElapsed reports whether the backoff window since the last attempt has
// elapsed, ignoring breakers — a breaker whose escalation was deferred (a claim
// refusal) stays armed to re-fire on its next elapsed window, unlike due.
func (l *ledger) backoffElapsed(now time.Time) bool {
	return !now.Before(l.nextDue)
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

// stamp records (p, resource)'s attempt clock without advancing it; see
// ledger.stamp.
func (ls *ledgers) stamp(p policy, resource string, now time.Time, err error) {
	ls.row(p, resource).stamp(now, err)
}

// attempt books one rate-limit observation for (p, resource).
func (ls *ledgers) attempt(p policy, resource string, now time.Time) {
	ls.row(p, resource).attempt(p, now)
}

// setNextDue overrides (p, resource)'s backoff clock; see ledger.setNextDue.
func (ls *ledgers) setNextDue(p policy, resource string, t time.Time) {
	ls.row(p, resource).setNextDue(t)
}

// clear drops (p, resource): the resource recovered.
func (ls *ledgers) clear(p policy, resource string) {
	delete(ls.m, ledgerKey{p.name, resource})
}

// due reports whether (p, resource) is due for a recovery attempt. An absent
// ledger (never attempted) is immediately due.
func (ls *ledgers) due(p policy, resource string, now time.Time) bool {
	l := ls.peek(p, resource)
	return l == nil || l.due(now)
}

// backoffElapsed reports whether (p, resource)'s backoff window has elapsed,
// ignoring breakers; see ledger.backoffElapsed. An absent ledger is immediately
// elapsed.
func (ls *ledgers) backoffElapsed(p policy, resource string, now time.Time) bool {
	l := ls.peek(p, resource)
	return l == nil || l.backoffElapsed(now)
}

// snapshot lists every live ledger for the status wire, taken by the caller
// under the Server's mu so the wire sees a consistent view.
func (ls *ledgers) snapshot() []ledgerSnapshot {
	out := make([]ledgerSnapshot, 0, len(ls.m))
	for k, l := range ls.m {
		s := ledgerSnapshot{
			Policy: k.policy, Resource: k.resource,
			Strikes: l.strikes, Faulted: l.faulted, Attempts: l.attempts,
			NextDue: l.nextDue, LastAt: l.lastAt,
		}
		if l.lastErr != nil {
			s.LastErr = l.lastErr.Error()
		}
		out = append(out, s)
	}
	return out
}
