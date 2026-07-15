package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/content"
)

// FPBridgeVerdict classifies the File Provider content bridge's data-plane health,
// the distinction a raw socket dial (FPBridgeUp) cannot make.
type FPBridgeVerdict string

const (
	// FPBridgeServing: the bridge answers a Manifest+Read self-test.
	FPBridgeServing FPBridgeVerdict = "serving"
	// FPBridgeConsentParked: the bind is parked on the one-time app-group-container
	// TCC consent; the daemon binds automatically once it is granted.
	FPBridgeConsentParked FPBridgeVerdict = "consent-parked"
	// FPBridgeBoundDead: the socket is bound but a content round-trip fails —
	// bound-but-dead (the daemon is up but its bridge is not serving).
	FPBridgeBoundDead FPBridgeVerdict = "bound-dead"
	// FPBridgeDown: the bridge socket is not bound (dial refused).
	FPBridgeDown FPBridgeVerdict = "down"
)

// FPBridgeStatus is one fpBridgeCheck verdict plus its operator lever (Detail is
// empty when serving — there is nothing to fix).
type FPBridgeStatus struct {
	Verdict FPBridgeVerdict `json:"verdict"`
	Detail  string          `json:"detail,omitempty"`
}

// fpBridgeProbeDomain is the self-test's synthetic domain — never a registered
// account. The probe is safe pre-registration: PoolContentSource.Manifest does no
// through-domain I/O and ReadSynth(domain, "settings.json") is domain-independent.
const fpBridgeProbeDomain = "ccp-bridge-selftest"

// fpBridgeSelfTestBudget bounds the on-demand and periodic self-test so a
// bound-but-dead bridge (a hung Manifest+Read, up to two 5s fusekit op timeouts)
// resolves to a BoundDead verdict well within the server's connection deadline
// and the client's fpBridgeCheckTimeout — never abandoned as a transport error
// that would fall back to the dial-only signal.
const fpBridgeSelfTestBudget = 9 * time.Second

// The three levers a not-serving verdict carries at record time. Consent-parked
// clears itself once granted (the watchdog re-binds with no restart); the others
// name the concrete daemon action.
const (
	fpBridgeConsentLever   = "run `ccp fp consent` in a local terminal; the daemon binds automatically once granted (no restart)"
	fpBridgeBoundDeadLever = "the daemon's bridge is bound but not serving; restart the daemon (brew services restart cc-pool)"
	fpBridgeDownLever      = "bridge socket not bound; check `ccp service status`"
)

// fpBridgePolicy backs the fp.bridge ledger row: a 2-strike debounce over the
// bridge data-plane verdict. Verdict-only — serveFPBridge's own retry loop is the
// recovery, this row only explains why FP heal is stalled. Keyed by poolResource.
var fpBridgePolicy = policies["fp.bridge"]

// fpBridgeCheck classifies the File Provider content bridge's data plane without a
// through-domain read: a consent-pending bind is parked WITHOUT dialing; otherwise
// a SelfTest over the bridge socket separates serving (nil), down (dial refused),
// and bound-dead (any other error, including a served manifest miss). fpBridgeCheckFn
// is a test seam.
func (s *Server) fpBridgeCheck(ctx context.Context) FPBridgeStatus {
	if s.fpBridgeCheckFn != nil {
		return s.fpBridgeCheckFn(ctx)
	}
	if s.fpConsentPending.Load() {
		return FPBridgeStatus{Verdict: FPBridgeConsentParked, Detail: fpBridgeConsentLever}
	}
	ctx, cancel := context.WithTimeout(ctx, fpBridgeSelfTestBudget)
	defer cancel()
	cl := content.NewBridgeClient(pool.FPBridgeSocketPath())
	switch err := cl.SelfTest(ctx, fpBridgeProbeDomain, settingsProbeEntry); {
	case err == nil:
		return FPBridgeStatus{Verdict: FPBridgeServing}
	case errors.Is(err, content.ErrBridgeDialRefused):
		return FPBridgeStatus{Verdict: FPBridgeDown, Detail: fpBridgeDownLever}
	default:
		return FPBridgeStatus{Verdict: FPBridgeBoundDead, Detail: fpBridgeBoundDeadLever}
	}
}

// settingsProbeEntry is the domain-independent synth entry the self-test reads
// (base ~/.claude/settings.json), so a missing per-account private file never
// misreads a serving bridge as dead.
const settingsProbeEntry = "settings.json"

// recordFPBridgeHealth probes the bridge and folds the verdict onto the fp.bridge
// ledger row, but only when at least one account is File-Provider-backed — a parked
// bridge on a non-FP machine must never alert. The heal maintainer's own gate holds
// contentSource != nil; this owns the FP-row half.
func (s *Server) recordFPBridgeHealth(ctx context.Context) {
	fp, err := s.fpAccounts()
	if err != nil {
		s.log.Printf("fp.bridge health: list accounts: %v", err)
		return
	}
	if len(fp) == 0 {
		s.ledMu.Lock()
		s.led.clear(fpBridgePolicy, poolResource)
		s.ledMu.Unlock()
		return
	}
	s.recordFPBridgeVerdict(s.fpBridgeCheck(ctx))
}

// recordFPBridgeVerdict folds one bridge verdict onto the fp.bridge row: serving
// clears it; any not-serving verdict strikes the 2-strike debounce, carrying the
// lever as the row's lastErr, and logs the one-shot wedge transition.
func (s *Server) recordFPBridgeVerdict(st FPBridgeStatus) {
	s.ledMu.Lock()
	if st.Verdict == FPBridgeServing {
		s.led.clear(fpBridgePolicy, poolResource)
		s.ledMu.Unlock()
		return
	}
	before := s.led.faulted(fpBridgePolicy, poolResource)
	s.led.strike(fpBridgePolicy, poolResource, time.Now(), fmt.Errorf("%s: %s", st.Verdict, st.Detail))
	wedged := !before && s.led.faulted(fpBridgePolicy, poolResource)
	s.ledMu.Unlock()
	// Log the one-shot wedge transition off the lock — a stalled log sink must
	// not block every bridge-ledger, readiness, and status reader on ledMu.
	if wedged {
		s.log.Printf("file provider bridge: %s — %s", st.Verdict, st.Detail)
	}
}

// fpBridgeFaulted reports whether the fp.bridge row has latched its not-serving
// verdict — the signal fpBridgeReady consults so healFPRows stops treating a
// bound-but-dead bridge as ready to probe through.
func (s *Server) fpBridgeFaulted() bool {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.faulted(fpBridgePolicy, poolResource)
}

// handleFPBridgeCheck runs the on-demand self-test, refreshes the fp.bridge ledger
// row, and returns the verdict as an op payload so a `ccp` bridge query and the
// heal ledger agree.
func (s *Server) handleFPBridgeCheck(ctx context.Context) Response {
	st := s.fpBridgeCheck(ctx)
	s.recordFPBridgeVerdict(st)
	return Response{OK: true, FPBridge: &st}
}
