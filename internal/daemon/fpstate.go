package daemon

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/fusekit/proc"
)

// fpWedgeStrikes, fpRecoveryBreaker, and fpRecoveryBackoff (the FP wedge
// debounce, recovery breaker, and recovery backoff) live in policies.go — the
// self-heal policy substrate.

// fpVerdict is one domain's debounced wedge verdict (serves control ops, hangs
// reads); guarded by fpState.mu.
type fpVerdict struct {
	strikes int
	wedged  bool
}

// fpRecovery tracks a wedged domain's recovery ladder: the attempt count
// (feeding the backoff and the breaker) and when the next attempt is due.
type fpRecovery struct {
	attempts int
	nextDue  time.Time
}

// fpState tracks per-domain File Provider health: a 2-strike debounced wedge
// verdict plus backoff-spaced, breaker-capped recovery bookkeeping. Keyed by
// account ConfigDir; concurrency-safe. It mirrors the holder deepVerdict model
// but is standalone — a parked FP read must never entangle holder state.
type fpState struct {
	mu       sync.Mutex
	verdicts map[string]*fpVerdict
	recovery map[string]*fpRecovery
	backoff  proc.Backoff
	// synthNonEmpty reports whether the content source's synthetic .claude.json
	// for dir is non-empty. Injected (not imported) so ErrFPProbeEmpty strikes
	// only when a domain serves 0 bytes for a synth that genuinely has content.
	synthNonEmpty func(dir string) bool
}

// newFPState builds an fpState. synthNonEmpty must be non-nil: it decides
// whether a zero-byte served .claude.json is a wedge or an empty-by-design
// account.
func newFPState(synthNonEmpty func(dir string) bool) *fpState {
	return &fpState{
		verdicts:      map[string]*fpVerdict{},
		recovery:      map[string]*fpRecovery{},
		backoff:       fpRecoveryBackoff,
		synthNonEmpty: synthNonEmpty,
	}
}

// wedged reports whether dir's domain is currently marked wedged (2 corroborated
// strikes). A tripped breaker does not clear the verdict — a parked domain stays
// wedged so the select path keeps it out of new sessions.
func (f *fpState) wedged(dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.verdicts[dir]
	return v != nil && v.wedged
}

// recordProbe folds one FP probe outcome for dir into its debounced verdict,
// returning a one-shot log line on a wedge or recovery transition.
// overlay.ErrFPProbeMissing never strikes (no identity file yet).
// overlay.ErrFPProbeEmpty strikes only when synthNonEmpty(dir) is true. A nil
// outcome clears the verdict and the recovery ladder.
func (f *fpState) recordProbe(dir string, err error) (logMsg string) {
	// No verdict, evaluated off the lock (the synth seam may read a local file):
	// a missing identity file, a 0-byte read that matches an empty synth, or a
	// no-verdict probe (app busy/unreachable/too old). None strikes; none clears —
	// a transient control blip must neither un-vouch nor re-vouch a domain.
	switch {
	case errors.Is(err, overlay.ErrFPProbeNoVerdict):
		return ""
	case errors.Is(err, overlay.ErrFPProbeMissing):
		return ""
	case errors.Is(err, overlay.ErrFPProbeEmpty) && !f.synthNonEmpty(dir):
		return ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		return f.clearLocked(dir)
	}
	v := f.verdicts[dir]
	if v == nil {
		v = &fpVerdict{}
		f.verdicts[dir] = v
	}
	v.strikes++
	if v.strikes == fpWedgeStrikes {
		v.wedged = true
		logMsg = fmt.Sprintf("file provider domain %s: %d consecutive probe failures; marking wedged (serves control ops but hangs reads): %v", dir, v.strikes, err)
	}
	return logMsg
}

// clearLocked drops dir's verdict and recovery ladder; caller holds f.mu.
func (f *fpState) clearLocked(dir string) (logMsg string) {
	if v := f.verdicts[dir]; v != nil && v.wedged {
		logMsg = fmt.Sprintf("file provider domain %s: recovered; the domain serves reads again", dir)
	}
	delete(f.verdicts, dir)
	delete(f.recovery, dir)
	return logMsg
}

// due reports whether a wedged domain is due for a recovery attempt: its breaker
// has not tripped and its backoff has elapsed. A wedged-but-never-attempted
// domain is immediately due; a non-wedged domain is never due.
func (f *fpState) due(dir string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.verdicts[dir]
	if v == nil || !v.wedged {
		return false
	}
	return f.recoveryDueLocked(dir, now)
}

// recoveryDue reports whether dir's recovery schedule permits another attempt —
// the breaker has not tripped and any prior attempt's backoff has elapsed —
// independent of a wedge verdict. The control-plane heal (a Missing probe masking
// a domain deregistered externally, which never strikes the data-plane wedge
// ladder) rides the same backoff/breaker schedule without the wedge gate due adds.
func (f *fpState) recoveryDue(dir string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recoveryDueLocked(dir, now)
}

// recoveryDueLocked is the schedule check shared by due and recoveryDue: a dir
// with no recovery record is immediately due; a tripped breaker is never due;
// otherwise the backoff since the last attempt must have elapsed. Caller holds
// f.mu.
func (f *fpState) recoveryDueLocked(dir string, now time.Time) bool {
	r := f.recovery[dir]
	if r == nil {
		return true
	}
	if r.attempts >= fpRecoveryBreaker {
		return false
	}
	return !now.Before(r.nextDue)
}

// attemptsSoFar reports how many recovery attempts dir has consumed; 0 if the
// domain is healthy or has never been attempted. The heal ladder reads it to pick
// the next step (Sync vs re-register vs breaker).
func (f *fpState) attemptsSoFar(dir string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.recovery[dir]; r != nil {
		return r.attempts
	}
	return 0
}

// fpWedge is a snapshot of one wedged domain's recovery bookkeeping for the
// status wire: its dir, the recovery attempts spent, and whether the breaker
// has tripped.
type fpWedge struct {
	Dir      string
	Attempts int
	Tripped  bool
}

// wedgedSnapshot lists every currently-wedged domain with its recovery attempt
// count and breaker state, taken under one lock so the status wire sees a
// consistent view.
func (f *fpState) wedgedSnapshot() []fpWedge {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fpWedge, 0, len(f.verdicts))
	for dir, v := range f.verdicts {
		if v == nil || !v.wedged {
			continue
		}
		w := fpWedge{Dir: dir}
		if r := f.recovery[dir]; r != nil {
			w.Attempts = r.attempts
			w.Tripped = r.attempts >= fpRecoveryBreaker
		}
		out = append(out, w)
	}
	return out
}

// forceWedge marks dir wedged immediately, bypassing the strike debounce. The
// select path uses it: a hard data-plane probe failure at select time must exclude
// the dir now — a launching session has no live reads a false positive could
// orphan, so there is nothing to protect with a debounce.
func (f *fpState) forceWedge(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := f.verdicts[dir]
	if v == nil {
		v = &fpVerdict{}
		f.verdicts[dir] = v
	}
	v.strikes = fpWedgeStrikes
	v.wedged = true
}

// recordAttempt books a recovery attempt against dir: increments the attempt
// count and schedules the next attempt after the backoff, reporting the new
// count and whether the breaker has now tripped.
func (f *fpState) recordAttempt(dir string, now time.Time) (attempt int, tripped bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.recovery[dir]
	if r == nil {
		r = &fpRecovery{}
		f.recovery[dir] = r
	}
	r.attempts++
	r.nextDue = now.Add(f.backoff.After(r.attempts))
	return r.attempts, r.attempts >= fpRecoveryBreaker
}

// reset clears all wedge and recovery state for dir: the domain recovered, was
// converted off File Provider, or was manually repaired.
func (f *fpState) reset(dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.verdicts, dir)
	delete(f.recovery, dir)
}
