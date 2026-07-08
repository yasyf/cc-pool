package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpControlProbeTimeout bounds one control-op FP domain probe. The op is a
// socket round-trip to the companion app (never a through-domain read that mints
// TCC prompts): a serving or unregistered domain answers fast; only a
// materializing appex parks, and that must read NoVerdict, not a false wedge.
const fpControlProbeTimeout = 3 * time.Second

// fpDomainPolicy is the File Provider self-heal policy row: a 2-strike wedge
// debounce, then the backoff-spaced, breaker-capped recovery ladder that parks a
// domain. Each domain folds onto one ledger row keyed by its account ConfigDir.
var fpDomainPolicy = policies["fp.domain"]

// fpEnabled reports whether FP self-heal is wired (the synth seam is injected).
// It replaces the fp-state nil guard: bare test servers leave it off, so every FP
// reader below is a no-op / healthy default there.
func (s *Server) fpEnabled() bool { return s.fpSynth != nil }

// fpWedge is a snapshot of one wedged domain's recovery bookkeeping for the
// status wire: its dir, the recovery attempts spent, and whether the breaker has
// tripped.
type fpWedge struct {
	Dir      string
	Attempts int
	Tripped  bool
}

// recordFPProbe folds one FP data-plane probe outcome for dir into its debounced
// wedge verdict on the fp.domain ledger, returning a one-shot log line on a wedge
// or recovery transition. ErrFPProbeNoVerdict and ErrFPProbeMissing never strike
// (a transient control blip / an identity-less account); ErrFPProbeEmpty strikes
// only when the synth genuinely has content. A nil outcome clears the verdict and
// the recovery ladder.
func (s *Server) recordFPProbe(dir string, err error) (logMsg string) {
	if !s.fpEnabled() {
		return ""
	}
	// Classify off the ledger lock (the synth seam may read a local file): a
	// no-verdict, a missing identity file, or a 0-byte read matching an empty synth
	// neither strikes nor clears — a transient control blip must not un-vouch or
	// re-vouch a domain.
	switch {
	case errors.Is(err, overlay.ErrFPProbeNoVerdict), errors.Is(err, overlay.ErrFPProbeMissing):
		return ""
	case errors.Is(err, overlay.ErrFPProbeEmpty) && !s.fpSynth(dir):
		return ""
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	if err == nil {
		if s.led.faulted(fpDomainPolicy, dir) {
			logMsg = fmt.Sprintf("file provider domain %s: recovered; the domain serves reads again", dir)
		}
		s.led.clear(fpDomainPolicy, dir)
		return logMsg
	}
	before := s.led.faulted(fpDomainPolicy, dir)
	s.led.strike(fpDomainPolicy, dir, time.Now(), err)
	if !before && s.led.faulted(fpDomainPolicy, dir) {
		logMsg = fmt.Sprintf("file provider domain %s: %d consecutive probe failures; marking wedged (serves control ops but hangs reads): %v", dir, fpWedgeStrikes, err)
	}
	return logMsg
}

// fpForceWedge latches dir's wedge verdict immediately, bypassing the strike
// debounce — the select path: a hard data-plane probe failure at select time must
// exclude the domain now (a launching session has no live reads a false positive
// could orphan).
func (s *Server) fpForceWedge(dir string, err error) {
	if !s.fpEnabled() {
		return
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.forceFault(fpDomainPolicy, dir, time.Now(), err)
}

// fpRecordAttempt books one recovery attempt against dir's ladder, returning the
// new attempt count and whether the park breaker (attempts ≥ fpRecoveryBreaker)
// has now tripped. fp.domain is single-lane, so the attempt kind is ignored and
// pre-fault attempts (the Missing control-plane heal) never touch the wedge
// debounce.
func (s *Server) fpRecordAttempt(dir string, now time.Time) (attempt int, tripped bool) {
	if !s.fpEnabled() {
		return 0, false
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	tripped = s.led.attempt(fpDomainPolicy, dir, attemptPrimary, now)
	return s.led.peek(fpDomainPolicy, dir).attempts, tripped
}

// fpAttemptsSoFar reports how many recovery attempts dir has consumed; 0 if the
// domain is healthy or never attempted. The heal ladder reads it to pick the next
// step (Sync vs re-register vs breaker).
func (s *Server) fpAttemptsSoFar(dir string) int {
	if !s.fpEnabled() {
		return 0
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	if l := s.led.peek(fpDomainPolicy, dir); l != nil {
		return l.attempts
	}
	return 0
}

// fpRecoveryDue reports whether dir's recovery schedule permits another attempt —
// the breaker has not parked it and any prior attempt's backoff has elapsed —
// independent of the wedge verdict (the Missing control-plane heal rides the same
// schedule without the wedge gate). A never-attempted domain is immediately due.
func (s *Server) fpRecoveryDue(dir string, now time.Time) bool {
	if !s.fpEnabled() {
		return false
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.due(fpDomainPolicy, dir, now)
}

// fpReset drops dir's wedge and recovery state: the domain recovered, was
// converted off File Provider, or was manually repaired.
func (s *Server) fpReset(dir string) {
	if !s.fpEnabled() {
		return
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.clear(fpDomainPolicy, dir)
}

// fpWedgedSnapshot lists every currently-wedged domain with its recovery attempt
// count and breaker state, taken under one lock so the status wire sees a
// consistent view.
func (s *Server) fpWedgedSnapshot() []fpWedge {
	if !s.fpEnabled() {
		return nil
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	var out []fpWedge
	for _, snap := range s.led.snapshot() {
		if snap.Policy != fpDomainPolicy.name || !snap.Faulted {
			continue
		}
		out = append(out, fpWedge{Dir: snap.Resource, Attempts: snap.Attempts, Tripped: snap.Parked})
	}
	return out
}

// fpDomainProbe classifies an account's File Provider domain data-plane verdict
// through the signed companion app's control op (never a through-domain
// filesystem read), returning nil (healthy) or one of
// overlay.ErrFPProbe{Missing,Empty,Wedged,NoVerdict}. A package var so heal and
// select tests drive the recovery ladder without a live domain; the default
// resolves the FP provider and probes over the app socket under ctx.
var fpDomainProbe = func(ctx context.Context, dir string) error {
	prov, err := pool.OverlayProviderFor(fkoverlay.BackendFileProvider)
	if err != nil {
		return fmt.Errorf("%w: resolve file provider: %w", overlay.ErrFPProbeNoVerdict, err)
	}
	prober, ok := prov.(overlay.FPDomainProber)
	if !ok {
		return fmt.Errorf("%w: provider %T lacks the app control-op probe", overlay.ErrFPProbeNoVerdict, prov)
	}
	return overlay.FPDomainProbe(ctx, prober, dir)
}

// fpDirLinked reports whether an FP account dir is currently its live domain
// bridge symlink (Setup makes accountDir a symlink INTO the domain root). A
// mid-conversion real dir or an absent path is not probeable — reading it would
// misclassify a benign transient as a wedge. A package var so heal tests drive the
// ladder without laying a real symlink.
var fpDirLinked = func(dir string) bool {
	fi, err := os.Lstat(dir)
	return err == nil && fi.Mode()&os.ModeSymlink != 0
}

// fpAppexBounce SIGKILLs the File Provider extension process (CCPoolFileProvider)
// so fileproviderd respawns it against a fresh replica DB — the breaker's last
// automated lever before it parks the domain. It NEVER touches fileproviderd itself: a global
// File Provider daemon restart is a user action, never automated. pkill exits 1
// when nothing matched, which is not an error here. Test seam.
var fpAppexBounce = func(ctx context.Context) error {
	err := exec.CommandContext(ctx, "pkill", "-x", "CCPoolFileProvider").Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.ExitCode() == 1 {
		return nil
	}
	return err
}

// fpAccounts lists every File Provider row.
func (s *Server) fpAccounts() ([]store.Account, error) {
	accts, err := s.m.Store.ListAccounts()
	if err != nil {
		return nil, err
	}
	fp := make([]store.Account, 0, len(accts))
	for _, a := range accts {
		if fpBackedRow(a.OverlayKind) {
			fp = append(fp, a)
		}
	}
	return fp, nil
}

// healFPRows probes every File Provider domain's data plane and, on a debounced
// wedge verdict that is due for a recovery attempt, escalates it up the recovery
// ladder. It runs on the heal ticker beside retryUnvouchedFuseRows — never through
// it: File Provider is a third backend, not a fuse mount. The probe is a bounded
// read (no poll claim); the ladder step runs under one. Probing is skipped while
// the FP bridge is down or awaiting consent (a probe through a down bridge reads
// every domain as wedged), and per-row while a conversion owns the dir or the dir
// is not its live bridge symlink.
func (s *Server) healFPRows(ctx context.Context) {
	if !s.fpEnabled() || !s.fpBridgeReady() {
		return
	}
	fp, err := s.fpAccounts()
	if err != nil {
		s.log.Printf("file provider heal loop: list accounts: %v", err)
		return
	}
	now := time.Now()
	for _, a := range fp {
		if ctx.Err() != nil {
			return
		}
		if s.cl.held(a.ID) || !fpDirLinked(a.ConfigDir) {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, fpControlProbeTimeout)
		probeErr := fpDomainProbe(probeCtx, a.ConfigDir)
		cancel()
		if msg := s.recordFPProbe(a.ConfigDir, probeErr); msg != "" {
			s.log.Printf("%s", msg)
		}
		// A no-verdict tick (app busy/unreachable/too old, or an app restart) is
		// neither a strike nor a clear: skip it entirely so a transient control
		// blip never escalates a previously-wedged domain's ladder.
		if errors.Is(probeErr, overlay.ErrFPProbeNoVerdict) {
			continue
		}
		// ENOENT never strikes the wedge ladder (an identity-less account is
		// benign), so a domain deregistered out from under the daemon — its bridge
		// symlink now pointing at a dead root — would go unhealed forever. The
		// control-plane heal tells the two apart and repairs the broken one.
		if errors.Is(probeErr, overlay.ErrFPProbeMissing) {
			s.healFPMissing(ctx, a, now)
			continue
		}
		if !s.fpWedged(a.ConfigDir) || !s.fpRecoveryDue(a.ConfigDir, now) {
			continue
		}
		if !s.cl.hold(a.ID) {
			continue // scheduler/reconcile owns the dir this iteration
		}
		s.healFP(ctx, a, now)
		s.cl.disownHold(a.ID)
	}
}

// healFPMissing handles an FP row whose data-plane probe returned ENOENT
// (ErrFPProbeMissing — the domain serves no .claude.json). A registered,
// controllable domain with no identity yet is benign, so Missing never strikes
// the wedge ladder; but the same ENOENT masks a control-plane failure — a domain
// deregistered externally, or a companion app gone uncontrollable, whose bridge
// symlink now points at a dead root. The cheap Health check tells them apart:
// Health OK is the benign case (nothing to do); Health failing is the broken one,
// repaired through reconcileFileProvider (Health→Setup, or the ErrCannotControl
// retreat to symlink) under the SAME backoff/attempt schedule that spaces the
// wedge ladder — one attempt per due window, success resets the ladder. The poll
// claim is held for the whole run so the repair never interleaves with the
// scheduler; reconcile's own wedged-and-backing-off gate never blocks here because
// a Missing probe leaves the domain un-wedged.
func (s *Server) healFPMissing(ctx context.Context, a store.Account, now time.Time) {
	if !s.cl.hold(a.ID) {
		return // scheduler/reconcile owns the dir this iteration
	}
	defer s.cl.disownHold(a.ID)
	prov := s.fpProvider(a)
	if prov == nil {
		return
	}
	if prov.Health(pool.ClaudeDir(), a.ConfigDir) == nil {
		return // benign: the control plane is healthy, the account just has no identity yet
	}
	if !s.fpRecoveryDue(a.ConfigDir, now) {
		return // a prior control-plane repair is still backing off
	}
	s.log.Printf("acct-%02d file provider domain serves no .claude.json and its control plane is unhealthy (deregistered externally?); reconciling", a.ID)
	switch s.reconcileFileProvider(ctx, a) {
	case fpHealthy, fpRepaired, fpRetreated:
		s.fpReset(a.ConfigDir) // control plane repaired (or retreated); clear the ladder
	default: // fpRetry, fpDeferred: keep the attempt booked so the backoff spaces the retry
		s.fpRecordAttempt(a.ConfigDir, now)
	}
}

// healFP runs one recovery-ladder step against a wedged File Provider domain,
// escalating with the attempt count: attempt 1 re-asserts the overlay (Sync,
// non-destructive); attempts 2–4 re-register the domain (Teardown+Setup, which
// discards fileproviderd's poisoned replica state); attempt 5 trips the breaker
// (appex bounce, one final re-register, then park — retreat only if the widget is
// genuinely gone). Each step is
// idempotent and verified by the next tick's probe — no inline sleeps. The caller
// holds the account's poll claim. Re-registration proceeds even under live sessions
// (a wedged domain already fails their reads); a pending select reservation defers
// it, without consuming the attempt.
func (s *Server) healFP(ctx context.Context, a store.Account, now time.Time) {
	dir := a.ConfigDir
	// Attempt 1 is a non-destructive re-assert (safe under a live reservation), so
	// it runs directly under the held poll claim.
	if s.fpAttemptsSoFar(dir) == 0 {
		prov := s.fpProvider(a)
		if prov == nil {
			return
		}
		s.fpRecordAttempt(dir, now)
		s.log.Printf("acct-%02d file provider domain wedged; recovery attempt 1: re-asserting the overlay (non-destructive) — relaunch any sessions on it", a.ID)
		if err := prov.Sync(pool.ClaudeDir(), dir); err != nil {
			s.log.Printf("acct-%02d file provider recovery attempt 1 (sync): %v", a.ID, err)
		}
		return
	}
	// Attempts 2+ remake the domain registration: take the convert claim so no
	// select hands the dir to a new session mid-re-register. A live reservation
	// defers (attempt NOT consumed); a live session does not.
	if !s.cl.ownHeld(a.ID) {
		s.log.Printf("acct-%02d file provider recovery deferred: reserved by a pending select", a.ID)
		return
	}
	defer s.cl.disownConvert(a.ID)
	fresh, err := s.m.Store.GetAccount(a.ID)
	if err != nil {
		s.log.Printf("acct-%02d file provider recovery: re-read row: %v", a.ID, err)
		return
	}
	if !fpBackedRow(fresh.OverlayKind) {
		s.fpReset(dir) // converted off File Provider in the claim gap
		return
	}
	if !s.fpWedged(dir) {
		return // recovered between the probe and the claim
	}
	prov := s.fpProvider(fresh)
	if prov == nil {
		return
	}
	attempt, tripped := s.fpRecordAttempt(dir, now)
	if tripped {
		s.breakerFP(ctx, fresh, prov)
		return
	}
	s.log.Printf("acct-%02d file provider domain still wedged; recovery attempt %d: re-registering the domain (discards its replica state) — relaunch any sessions on it", fresh.ID, attempt)
	if s.reRegisterFP(fresh, prov) {
		// Setup reported the domain cannot serve here at all; retreat now (we hold
		// the convert claim), live-session-gated.
		if s.convertFPToSymlinkHeld(ctx, fresh) {
			s.log.Printf("acct-%02d fell back to symlink: File Provider cannot serve on this machine", fresh.ID)
		}
	}
}

// fpProvider resolves the account's File Provider overlay provider with the
// backend fence, returning nil (logged) on an unresolvable or wrong-backend
// provider so the caller skips recovery rather than acting through it.
func (s *Server) fpProvider(a store.Account) fkoverlay.Provider {
	prov := s.overlayForRow(a)
	if prov == nil || prov.Backend() != fkoverlay.BackendFileProvider {
		s.log.Printf("no file provider resolved for acct-%02d; skipping recovery through it", a.ID)
		return nil
	}
	return prov
}

// reRegisterFP tears the domain down and re-adds it (Teardown+Setup) — the reset
// that discards fileproviderd's poisoned replica state and forces a clean
// re-enumeration. It returns whether Setup reported ErrCannotControl (File Provider
// cannot serve on this machine) so the caller retreats to symlink. Caller holds the
// convert claim.
func (s *Server) reRegisterFP(a store.Account, prov fkoverlay.Provider) (cannotControl bool) {
	base, dir := pool.ClaudeDir(), a.ConfigDir
	if err := prov.Teardown(base, dir); err != nil {
		// A wedged domain may refuse a clean Teardown; the idempotent Setup below
		// re-adds regardless, so log and press on rather than stalling the ladder.
		s.log.Printf("acct-%02d file provider re-register: teardown: %v", a.ID, err)
	}
	switch err := prov.Setup(base, dir); {
	case err == nil:
		s.log.Printf("acct-%02d file provider domain re-registered; the next probe verifies it", a.ID)
		return false
	case errors.Is(err, fileproviderd.ErrCannotControl):
		s.log.Printf("acct-%02d file provider cannot serve on this machine (no entitlement or extension disabled): %v", a.ID, err)
		return true
	default:
		s.log.Printf("acct-%02d file provider re-register setup failed; the next tick re-probes and re-attempts: %v", a.ID, err)
		return false
	}
}

// breakerFP is the recovery ladder's terminal step once fpRecoveryBreaker attempts
// have not cleared the wedge: bounce the extension process (never fileproviderd),
// one final re-register, then PARK. It NEVER retreats a wedged-but-controllable
// domain to symlink behind the operator's back — a false wedge (an app restart, a
// materializing appex) would silently strand the account on the symlink floor, the
// exact regression this breaker rewrite closes. Automatic retreat survives ONLY
// ErrCannotControl (reRegisterFP reports the widget is genuinely gone: no
// entitlement or extension disabled); otherwise the domain parks wedged and the log
// names the two operator levers plus the fileproviderd kickstart. Caller holds the
// convert claim.
func (s *Server) breakerFP(ctx context.Context, a store.Account, prov fkoverlay.Provider) {
	s.log.Printf("acct-%02d file provider domain wedged past %d recovery attempts; breaker: bouncing the extension, one final re-register, then parking — relaunch any sessions on it", a.ID, fpRecoveryBreaker)
	if err := fpAppexBounce(ctx); err != nil {
		s.log.Printf("acct-%02d file provider extension bounce: %v", a.ID, err)
	}
	if s.reRegisterFP(a, prov) {
		// ErrCannotControl: the widget is genuinely gone, so the domain can never
		// serve here — the one condition an automatic symlink retreat survives.
		if s.convertFPToSymlinkHeld(ctx, a) {
			s.log.Printf("acct-%02d fell back to symlink: File Provider cannot serve on this machine", a.ID)
			return
		}
	}
	s.log.Printf("acct-%02d file provider domain parked wedged: automated recovery is exhausted. Re-register it now with `ccp fp repair --account %d`, or force it back to the symlink floor with `ccp fp repair --retreat --account %d`. A stuck fileproviderd needs a manual restart — run `launchctl kickstart -k gui/$(id -u)/com.apple.fileproviderd` (or reboot), then relaunch sessions on it", a.ID, a.ID, a.ID)
}
