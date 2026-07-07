package overlay

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/fileproviderd"
)

// Deliberately untagged, like probe.go: every build variant must compile these
// so the daemon and CLI can errors.Is File Provider probe verdicts across
// process boundaries.

var (
	// ErrFPProbeWedged means the domain answers control ops but does not serve an
	// enumeration/read of its .claude.json — the data-plane wedge cc-pool's
	// control-plane Health cannot see (ProbeDomain reported ErrDomainNotServing).
	// Treat as dead.
	ErrFPProbeWedged = errors.New("file provider domain wedged")

	// ErrFPProbeMissing means the domain serves but has no .claude.json (an account
	// with no identity yet), or the app has no registration for it (ErrNoDomain, a
	// control-plane repair healFPMissing drives, never a data-plane wedge). Like
	// ErrProbeMissing it is no verdict, never a wedge — such a domain survives until
	// it has content to probe.
	ErrFPProbeMissing = errors.New("file provider .claude.json missing")

	// ErrFPProbeEmpty means the domain served a zero-byte .claude.json. FPFS skips
	// fetchContents at size 0, so a 0-byte read proves nothing about the data plane;
	// the caller strikes on it only when the content source's synth read is genuinely
	// non-empty (empty-when-nonempty-expected).
	ErrFPProbeEmpty = errors.New("file provider .claude.json empty")

	// ErrFPProbeNoVerdict means the probe reached NO data-plane conclusion: the
	// companion app was busy, unreachable, or too old to answer the control op
	// (ErrBusy/ErrAppUnavailable/ErrOpUnsupported, or any unrecognized error). It is
	// neither a strike nor a clear — a transient app restart must not un-vouch a
	// domain nor fleet-wedge a select. Callers skip the tick / read ready.
	ErrFPProbeNoVerdict = errors.New("file provider domain: no probe verdict")
)

// FPDomainProber probes a File Provider domain's data plane through the signed
// companion app's control op: the app enumerates the domain and reports its
// .claude.json byte count WITHOUT a materializing (TCC-tripping) filesystem read.
// Satisfied by *fusekit/overlay.FileProviderProvider.
type FPDomainProber interface {
	ProbeDomain(ctx context.Context, accountDir string) (*int64, error)
}

// FPDomainRemover deregisters a File Provider domain WITHOUT retracting the
// account-dir bridge symlink — the deregistration a failed convert or a symlink
// retreat must perform to avoid leaking a domain registration. Satisfied by
// *fusekit/overlay.FileProviderProvider.
type FPDomainRemover interface {
	RemoveDomain(accountDir string) error
}

// FPDomainRegistry reports whether a File Provider domain is currently registered
// for an account dir, via the host's zero-spawn State query (never a spawn, never
// a through-domain read). A registered domain returns its root; an unregistered
// one surfaces fileproviderd.ErrNoDomain and a down app fileproviderd.ErrAppUnavailable
// as non-nil errors. Used to detect a domain leaked onto a symlink row so it can be
// deregistered. Satisfied by *fusekit/overlay.FileProviderProvider.
type FPDomainRegistry interface {
	DomainRoot(ctx context.Context, accountDir string) (string, error)
}

// FPDomainProbe classifies accountDir's File Provider domain verdict from the
// app-side control op, mapping fileproviderd's error classes and its byte-count
// verdict onto the cc-pool sentinels the fpstate/heal ladder keys on. It performs
// ZERO through-domain filesystem I/O — the raw read that mints per-account macOS
// TCC prompts, gets misclassified as a wedge, and drives the breaker's silent
// symlink retreat.
//
// Byte-count verdict (no error): >0 bytes read -> nil (healthy); a nil pointer
// (.claude.json absent) -> ErrFPProbeMissing; a pointer to 0 (present but empty) ->
// ErrFPProbeEmpty.
//
// Error classes: ErrDomainNotServing -> ErrFPProbeWedged; ErrNoDomain ->
// ErrFPProbeMissing (a control-plane repair, not a data-plane wedge);
// ErrBusy/ErrAppUnavailable/ErrOpUnsupported, and any unrecognized error ->
// ErrFPProbeNoVerdict.
func FPDomainProbe(ctx context.Context, prober FPDomainProber, accountDir string) error {
	n, err := prober.ProbeDomain(ctx, accountDir)
	if err != nil {
		switch {
		case errors.Is(err, fileproviderd.ErrDomainNotServing):
			return fmt.Errorf("%w: %w", ErrFPProbeWedged, err)
		case errors.Is(err, fileproviderd.ErrNoDomain):
			return fmt.Errorf("%w: %w", ErrFPProbeMissing, err)
		default:
			// Busy, app-unavailable, op-unsupported, or any unrecognized class: no
			// data-plane conclusion — never strike, never clear.
			return fmt.Errorf("%w: %w", ErrFPProbeNoVerdict, err)
		}
	}
	switch {
	case n == nil:
		return fmt.Errorf("%w: %s serves no .claude.json", ErrFPProbeMissing, accountDir)
	case *n == 0:
		return fmt.Errorf("%w: %s served 0 bytes", ErrFPProbeEmpty, accountDir)
	default:
		return nil
	}
}
