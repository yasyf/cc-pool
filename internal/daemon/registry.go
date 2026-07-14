package daemon

import (
	"context"

	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
)

// This file declares the daemon's maintenance passes as data: three ordered
// maintainer tables (poll, heal, startup) run by runTable, over a shared per-pass
// tick that carries one session scan. The passes' bodies live in their own files
// (scheduler.go, holder.go, fpheal.go, strandheal.go, server.go); the rows here
// only name them and fix their order — a reorder is a diff a reviewer must
// confront (registry_test.go pins each table's order by name). Cadence (both
// tickers), the holder-loss edge trigger, and the request-scoped RPC handlers are
// deliberately NOT rows — see their notes below.

// tick is one maintenance pass's shared context. It carries a single, lazily
// memoized procscan session scan so every consumer in the pass reads one snapshot
// and the scan-fail-means-all-busy rule lives in exactly one place (idle). The
// scan is lazy: a pass whose rows never ask about session liveness never scans, so
// the startup table adds no scan and the heal tick scans at most once even though
// two families (fuse.remount, strand.heal) consult it.
type tick struct {
	s       *Server
	ctx     context.Context
	scanned bool
	sess    []procscan.Session
	ok      bool
}

// newTick builds a fresh per-pass tick; the scan runs on first access.
func (s *Server) newTick(ctx context.Context) *tick { return &tick{s: s, ctx: ctx} }

// scan runs (once) the tick's session scan, logging a failure a single time — the
// one home of the "a failed scan proves nothing idle" message.
func (t *tick) scan() {
	if t.scanned {
		return
	}
	t.scanned = true
	sess, err := t.s.scan(t.ctx)
	t.sess = sess
	if err != nil {
		t.s.log.Printf("procscan failed; treating all accounts as busy this tick: %v", err)
		return
	}
	t.ok = true
}

// scanOK reports whether this tick's session scan succeeded.
func (t *tick) scanOK() bool { t.scan(); return t.ok }

// sessions returns this tick's memoized session scan (nil on a failed scan).
func (t *tick) sessions() []procscan.Session { t.scan(); return t.sess }

// idle reports whether dir backs no live claude session in this tick's scan. A
// failed scan makes idle false for EVERY dir: this is the single home of the
// daemon's scan-fail-means-all-busy fail-closed rule, and every ticker-pass
// consumer inherits it here rather than re-deriving it.
func (t *tick) idle(dir string) bool {
	t.scan()
	return t.ok && procscan.CountByConfigDir(t.sess, dir) == 0
}

// sessionCount returns dir's live-session count in this tick's scan (0 on a
// failed scan).
func (t *tick) sessionCount(dir string) int {
	t.scan()
	return procscan.CountByConfigDir(t.sess, dir)
}

// claimScope documents a maintainer row's per-account claim discipline. It is
// declarative — the claim itself is taken inside each pass body (or forEach); the
// field states the contract a reader relies on, and registry_test.go pins it.
type claimScope int

const (
	// claimNone: the row takes no per-account claim (pool-wide or process work).
	claimNone claimScope = iota
	// claimPerAccount: the row claims each account it mutates with the poll claim.
	claimPerAccount
)

// maintainer is one declared maintenance pass: a named, claim-scoped unit the tick
// runners execute in table order. gate (nil ⇒ always) decides whether the row runs
// this tick; run reports whether the runner should continue to the next row —
// false stops the table (a poll that entered outage skips the status snapshot,
// mirroring pollOnce's early returns exactly).
type maintainer struct {
	name       string
	claimScope claimScope
	gate       func(s *Server) bool
	run        func(s *Server, ctx context.Context, t *tick) bool
}

// runTable executes table in order over one shared tick: skip a gated-out row and
// stop when a row's run returns false. It does not itself poll ctx between rows —
// each pass handles cancellation internally exactly as it did before the cutover
// (the old poll/heal/startup sequences had no per-step ctx check), and a poll that
// aborts on ctx signals it by returning false.
func (s *Server) runTable(ctx context.Context, t *tick, table []maintainer) {
	for _, m := range table {
		if m.gate != nil && !m.gate(s) {
			continue
		}
		if !m.run(s, ctx, t) {
			return
		}
	}
}

// pollTable is the scheduler poll pass. Deliberately no fp row: File Provider
// recovery is owned exclusively by the backoff-gated heal ticker (probe + recovery
// ladder), so a Health+Setup on every poll would be the reconcile storm that
// removing the inline FP path fixed — with the registry this exclusion is now
// structural (registry_test.go asserts it). account.poll's body stays whole
// (outage canary, inline healFuse fallback, AdoptRotatedToken, SampleUsage, auth
// outcome) and returns false on every skip-the-snapshot condition, exactly as
// pollOnce returned.
var pollTable = []maintainer{
	{"holder.refresh", claimNone, nil, func(s *Server, _ context.Context, _ *tick) bool {
		s.holder.refresh(s.holderClient())
		return true
	}},
	{"session.reconcile", claimNone, nil, func(s *Server, _ context.Context, t *tick) bool {
		s.reconcileDeadSessions(t)
		return true
	}},
	{"widget.stale", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.reconcileStaleWidget(ctx)
		return true
	}},
	{"sticky.prune", claimNone, nil, func(s *Server, _ context.Context, _ *tick) bool {
		s.pruneStickyRows()
		return true
	}},
	{"account.poll", claimPerAccount, nil, func(s *Server, ctx context.Context, t *tick) bool {
		return s.pollAccounts(ctx, t)
	}},
	{"status.snapshot", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		if err := s.writeStatusSnapshot(ctx); err != nil {
			s.log.Printf("status snapshot: %v", err)
		}
		return true
	}},
}

// healTable is the per-account mount-health net (the heal ticker body). holder
// cache first (the ticker outpaces the poll's refresh); then fp.app.ensure
// relaunches the companion app whose death would otherwise park every FP probe
// on NoVerdict forever, at tick start so probe coverage resumes the tick after a
// respawn; then the fuse/FP self-heal families (fp.bridge.health explains a stalled
// bridge before fp.heal probes through it); then fp.orphan.reap deregisters
// rowless leaked domains (after fp.heal so a legit domain's own recovery runs
// first, before strand.heal's row-driven leak sweep); then content-source health
// logging (gated on a configured source).
var healTable = []maintainer{
	{"holder.refresh", claimNone, nil, func(s *Server, _ context.Context, _ *tick) bool {
		s.holder.refresh(s.holderClient())
		return true
	}},
	{"fp.app.ensure", claimNone, (*Server).shouldEnsureFPApp, func(s *Server, ctx context.Context, _ *tick) bool {
		s.ensureFPAppAsync(ctx)
		return true
	}},
	{"fuse.remount", claimPerAccount, nil, func(s *Server, ctx context.Context, t *tick) bool {
		s.retryUnvouchedFuseRows(ctx, t)
		return true
	}},
	{"fp.bridge.health", claimNone, func(s *Server) bool { return s.contentSource != nil }, func(s *Server, ctx context.Context, _ *tick) bool {
		s.recordFPBridgeHealth(ctx)
		return true
	}},
	{"fp.heal", claimPerAccount, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.healFPRows(ctx)
		return true
	}},
	{"fp.orphan.reap", claimNone, (*Server).fpEnabled, func(s *Server, ctx context.Context, _ *tick) bool {
		s.sweepOrphanFPDomains(ctx)
		return true
	}},
	{"strand.heal", claimPerAccount, nil, func(s *Server, ctx context.Context, t *tick) bool {
		s.healStrandedRows(ctx, t)
		return true
	}},
	{"content.health", claimNone, func(s *Server) bool { return s.contentSource != nil }, func(s *Server, _ context.Context, _ *tick) bool {
		s.logContentHealth()
		return true
	}},
}

// startupTable is the ordered one-shot the serve goroutine runs before the heal
// loop and scheduler start. bridge.content and bridge.fp bind before any mount or
// FP enumeration registers; holder.refresh primes the mount cache before selects
// key on it; ua.detect only stamps the OAuth UA; fp.app.ensure (non-blocking,
// after bridge.fp settles consent) warms the companion app in parallel so the
// first FP account's reconcile finds it up rather than eating the cold ~30s
// spawn serially; overlays.reconcile must finish
// first (it and the loops both touch fuse Setup). Carcass clearing is the shared
// holder's job now, so the startup reconcile sweeps nothing. bridge.fp wraps
// today's in-daemon FP-bridge bind unchanged.
var startupTable = []maintainer{
	{"bridge.content", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.startContentBridge(ctx)
		return true
	}},
	{"bridge.fp", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.startFPBridge(ctx)
		return true
	}},
	{"holder.refresh", claimNone, nil, func(s *Server, _ context.Context, _ *tick) bool {
		s.holder.refresh(s.holderClient())
		return true
	}},
	{"ua.detect", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.detectAndSetUserAgent(ctx)
		return true
	}},
	{"fp.app.ensure", claimNone, (*Server).shouldEnsureFPApp, func(s *Server, ctx context.Context, _ *tick) bool {
		s.ensureFPAppAsync(ctx)
		return true
	}},
	{"overlays.reconcile", claimPerAccount, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.reconcileOverlays(ctx)
		return true
	}},
}

// rowKind selects which overlay-backed accounts a heal-family pass iterates.
type rowKind int

const (
	fuseRows rowKind = iota
	fpRows
	symlinkRows
)

// String names the kind for the list-accounts error log.
func (k rowKind) String() string {
	switch k {
	case fuseRows:
		return "fuse"
	case fpRows:
		return "file provider"
	default:
		return "stranded-row"
	}
}

// accountsOf lists the accounts whose overlay backend matches kind.
func (s *Server) accountsOf(kind rowKind) ([]store.Account, error) {
	switch kind {
	case fuseRows:
		return s.fuseAccounts()
	case fpRows:
		return s.fpAccounts()
	default:
		return s.symlinkAccounts()
	}
}

// symlinkAccounts lists the accounts on the symlink overlay (neither fuse nor File
// Provider) — the only rows that can strand crash-window wreckage.
func (s *Server) symlinkAccounts() ([]store.Account, error) {
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	out := make([]store.Account, 0, len(accts))
	for _, a := range accts {
		if !fpBackedRow(a.OverlayKind) && !fuseBackedRow(a.OverlayKind) {
			out = append(out, a)
		}
	}
	return out, nil
}

// claimed runs fn under a poll claim on a (hold → skip-don't-race if refused → run
// → disownHold). The shared claim wrapper the heal-family loops apply to their
// mutating section: same claim kind, same skip-don't-race behavior across all three.
func (s *Server) claimed(a store.Account, fn func()) {
	if !s.cl.hold(a.ID) {
		return // skip-don't-race; the owner leaves it consistent
	}
	defer s.cl.disownHold(a.ID)
	fn()
}

// forEach lists every account of kind and runs fn per account, stopping if ctx is
// cancelled between accounts — the iterator the three heal-family row loops share.
// It reports whether the full list was iterated (false on a list-accounts error or
// a mid-iteration ctx cancellation) so a caller with an end-of-pass step gated on
// completion — fuse.remount's remountPrune — runs it only then, exactly as the old
// hand-written loop returned before pruning on those two conditions.
//
// claim=true wraps fn in the poll claim via claimed (strand.heal, whose only
// pre-claim work is the kind filter forEach already applies). fuse.remount and
// fp.heal pass claim=false and take the claim themselves inside fn via claimed,
// because they must probe UNCLAIMED before claiming: a claim-first loop would skip
// probing an account the scheduler is concurrently polling (its poll claim is
// held), silently dropping that account's probe-strike bookkeeping.
func (s *Server) forEach(ctx context.Context, kind rowKind, claim bool, fn func(a store.Account)) bool {
	accts, err := s.accountsOf(kind)
	if err != nil {
		s.log.Printf("%s heal: list accounts: %v", kind, err)
		return false
	}
	for _, a := range accts {
		if ctx.Err() != nil {
			return false
		}
		if claim {
			s.claimed(a, func() { fn(a) })
			continue
		}
		fn(a)
	}
	return true
}
