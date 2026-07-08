package daemon

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpOrphanPolicy backs the fp.orphan.reap row: a debounce of fpOrphanReapStrikes
// consecutive confirmed sweeps before a rowless registered domain is
// deregistered, then a fixed backoff spacing RemoveDomain retries. Keyed by the
// domain's account dir (like fp.domain), so each candidate folds onto one row.
var fpOrphanPolicy = policies["fp.orphan"]

// errFPDomainOrphaned is the strike reason recorded for a confirmed orphan: a
// registered File Provider domain that no account row, pending add, or on-disk
// account dir owns.
var errFPDomainOrphaned = errors.New("registered file provider domain has no owning account")

// sweepOrphanFPDomains deregisters File Provider domains that materialize a
// ~/Library/CloudStorage root but that no pool account, pending add, or on-disk
// account dir owns — the incident shape a failed add's rollback (or a manual
// mishap) leaves behind. It is the artifact-driven complement to the row-driven
// sweepLeakedFPDomain: a domain whose account dir still exists on disk (a live
// row, a symlink row's leaked domain, a kept-add resume) is guarded here and
// handled there, so the two never touch the same domain.
//
// A confirmed orphan must survive fpOrphanReapStrikes consecutive sweeps — each
// re-confirmed by the zero-spawn DomainRoot registration query, which never
// spawns the app — before the reap fires; any guard hit, no-verdict, or vanished
// candidate resets its streak. Immediately before RemoveDomain the guards and the
// registration are re-confirmed against fresh state, closing the sub-tick window
// where a concurrent `ccp add` reserves the freed index. The daemon deregisters
// the domain only; it never deletes the CloudStorage contents (that dir is the
// OS's). It fails closed on any listing error — an empty-by-error guard set is
// the catastrophic direction — skipping the whole pass and resetting nothing.
func (s *Server) sweepOrphanFPDomains(ctx context.Context) {
	if !s.fpEnabled() {
		return
	}
	if !s.fpBridgeReady() {
		// A down bridge is a pool-wide no-verdict: a DomainRoot probe through it is
		// meaningless, so no candidate can be CONFIRMED this tick. Reset every streak
		// (contract: bridge down resets the row) so only consecutive bridge-up
		// confirmations reap — an intervening bridge-down tick can never bridge two
		// confirmations into a false consecutive run.
		s.resetAllFPOrphanStrikes()
		return
	}
	prov := s.overlayFor(fkoverlay.BackendFileProvider)
	if prov == nil {
		return
	}
	registry, okReg := prov.(overlay.FPDomainRegistry)
	remover, okRem := prov.(overlay.FPDomainRemover)
	if !okReg || !okRem {
		return
	}
	ids, err := fpCloudStorageDomains()
	if err != nil {
		s.log.Printf("file provider orphan reap: list cloud storage domains: %v; skipping this pass", err)
		return
	}
	guard, ok := s.fpOrphanGuardSet()
	if !ok {
		return // a listing error already logged; fail closed, reset nothing
	}
	s.pruneFPOrphanLedger(ids)

	now := time.Now()
	for _, id := range ids {
		s.reapOrphanFPDomain(ctx, id, guard, registry, remover, now)
	}
}

// reapOrphanFPDomain runs one candidate through the guard set, the confirmation
// strike, and — on the fpOrphanReapStrikes-th confirmed sweep, reconfirmed — the
// deregistration. Any guard hit or non-registered verdict resets the candidate's
// streak.
func (s *Server) reapOrphanFPDomain(ctx context.Context, id int, guard fpOwnerSet, registry overlay.FPDomainRegistry, remover overlay.FPDomainRemover, now time.Time) {
	dir := pool.AccountDir(id)
	name := pool.AccountDirName(id)
	if guard.owns(id, dir) {
		s.fpOrphanReset(dir)
		return
	}
	root, err := s.fpDomainRegistered(ctx, registry, dir)
	if err != nil {
		// ErrNoDomain (CloudStorage residue only — never deleted here) or any
		// no-verdict (ErrAppUnavailable, bridge blip, timeout): strikes count only
		// consecutive CONFIRMED registrations, so reset.
		s.fpOrphanReset(dir)
		return
	}
	faulted, first := s.fpOrphanStrike(dir, now)
	if first {
		s.log.Printf("file provider domain %s is registered (root %s) but no account row, pending add, or account dir owns it; deregistering after %d consecutive confirmations", name, root, fpOrphanReapStrikes)
	}
	if !faulted || !s.fpOrphanReapDue(dir, now) {
		return
	}
	// Reconfirm-before-kill: a concurrent `ccp add` can reserve the freed index or
	// seed the dir between the strike above and this remove. Re-read the guards and
	// the registration once more; any change spares the domain and resets it.
	if !s.fpOrphanReconfirm(ctx, id, dir, registry) {
		s.fpOrphanReset(dir)
		return
	}
	s.log.Printf("deregistering orphaned file provider domain %s (root %s): no account owns it after %d confirmations", name, root, fpOrphanReapStrikes)
	if err := remover.RemoveDomain(dir); err != nil {
		s.log.Printf("deregister orphaned file provider domain %s: %v; retrying after %s", name, err, fpOrphanReapBackoff.Base)
		s.fpOrphanBookRetry(dir, now)
		return
	}
	s.log.Printf("deregistered orphaned file provider domain %s (%s)", name, root)
	s.fpOrphanReset(dir)
}

// fpOwnerSet is the reap's ownership guard: the account indices held by a row or
// a live add reservation, taken once per sweep.
type fpOwnerSet struct {
	rows     map[int]bool
	reserved map[int]bool
}

// owns reports whether id (or its on-disk backing) is owned — a live row, a live
// add reservation, or an existing account dir / private root. Any hit means the
// domain is not an orphan.
func (o fpOwnerSet) owns(id int, dir string) bool {
	return o.rows[id] || o.reserved[id] || fpCandidateBacked(dir)
}

// fpOrphanGuardSet loads the account-row and pending-add index guards. ok is
// false (already logged) on any listing error, so the caller fails closed rather
// than reap against a partial guard set.
func (s *Server) fpOrphanGuardSet() (fpOwnerSet, bool) {
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		s.log.Printf("file provider orphan reap: list accounts: %v; skipping this pass", err)
		return fpOwnerSet{}, false
	}
	reserved, err := s.m.Store.PendingAddIndexes()
	if err != nil {
		s.log.Printf("file provider orphan reap: list pending adds: %v; skipping this pass", err)
		return fpOwnerSet{}, false
	}
	set := fpOwnerSet{rows: make(map[int]bool, len(accts)), reserved: make(map[int]bool, len(reserved))}
	for _, a := range accts {
		set.rows[a.ID] = true
	}
	for _, id := range reserved {
		set.reserved[id] = true
	}
	return set, true
}

// fpOrphanReconfirm re-runs the full orphan predicate against fresh state,
// immediately before the remove: one more zero-spawn registration probe, then a
// fresh guard set. The ownership read runs LAST, adjacent to RemoveDomain, so a
// concurrent `ccp add` reserving the freed index has the narrowest possible
// window to slip past — the residual TOCTOU reconfirm-before-kill accepts (a
// lost race is self-healing: fusekit Setup re-registers an absent domain). It
// returns true only when the candidate is still a registered, unowned domain;
// any non-registered verdict, listing error, or ownership returns false so the
// caller leaves the domain untouched.
func (s *Server) fpOrphanReconfirm(ctx context.Context, id int, dir string, registry overlay.FPDomainRegistry) bool {
	if _, err := s.fpDomainRegistered(ctx, registry, dir); err != nil {
		return false // deregistered or no verdict since the strike
	}
	guard, ok := s.fpOrphanGuardSet()
	return ok && !guard.owns(id, dir)
}

// fpDomainRegistered runs the zero-spawn DomainRoot registration query for dir
// under the leak-sweep timeout, returning the domain root or the registry's
// error (ErrNoDomain when unregistered, ErrAppUnavailable when the app is down).
func (s *Server) fpDomainRegistered(ctx context.Context, registry overlay.FPDomainRegistry, dir string) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, fpLeakSweepTimeout)
	defer cancel()
	return registry.DomainRoot(probeCtx, dir)
}

// fpCandidateBacked reports whether a candidate domain's account dir or its
// private backing root exists on disk — a live row's dir, a symlink row's leaked
// domain, a mid-add PrepareAdd seed, or a ReleaseAdd keep-dir resume, none of
// which is an orphan. An unreadable (non-ENOENT) path is treated as present: an
// ambiguous path is never confirmed absent, so it is never reaped.
func fpCandidateBacked(dir string) bool {
	for _, p := range []string{dir, fkoverlay.FusePrivateRoot(dir)} {
		if _, err := os.Lstat(p); err == nil || !os.IsNotExist(err) {
			return true
		}
	}
	return false
}

// fpOrphanStrike records one confirmed-orphan observation for dir, returning
// whether the debounce has now latched (fpOrphanReapStrikes reached) and whether
// this was the first strike of a fresh streak (for the one-shot loud log).
func (s *Server) fpOrphanStrike(dir string, now time.Time) (faulted, first bool) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	if l := s.led.peek(fpOrphanPolicy, dir); l == nil || (l.strikes == 0 && !l.faulted) {
		first = true
	}
	s.led.strike(fpOrphanPolicy, dir, now, errFPDomainOrphaned)
	return s.led.faulted(fpOrphanPolicy, dir), first
}

// fpOrphanReapDue reports whether dir's reap is due — the debounced fault stands
// and any failed-remove backoff has elapsed.
func (s *Server) fpOrphanReapDue(dir string, now time.Time) bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.due(fpOrphanPolicy, dir, now)
}

// fpOrphanBookRetry books a failed reap, advancing dir's backoff clock so the
// next remove waits fpOrphanReapBackoff; the strike verdict is kept.
func (s *Server) fpOrphanBookRetry(dir string, now time.Time) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.attempt(fpOrphanPolicy, dir, attemptPrimary, now)
}

// fpOrphanReset drops dir's orphan bookkeeping: it is owned again, gone, or
// deregistered.
func (s *Server) fpOrphanReset(dir string) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.clear(fpOrphanPolicy, dir)
}

// resetAllFPOrphanStrikes drops every fp.orphan row — the pool-wide reset a
// bridge-down (no-verdict) tick applies.
func (s *Server) resetAllFPOrphanStrikes() {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.prune(fpOrphanPolicy, func(string) bool { return false })
}

// pruneFPOrphanLedger drops fp.orphan rows whose candidate vanished from the
// CloudStorage listing — a disappeared candidate resets.
func (s *Server) pruneFPOrphanLedger(ids []int) {
	keep := make(map[string]bool, len(ids))
	for _, id := range ids {
		keep[pool.AccountDir(id)] = true
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.prune(fpOrphanPolicy, func(resource string) bool { return keep[resource] })
}
