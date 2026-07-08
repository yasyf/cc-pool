package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpFinding is one File Provider health rung: a label, its verdict, and the
// detail line both doctor and the add-failure diagnosis render.
type fpFinding struct {
	label   string
	healthy bool
	detail  string
}

// fpDiagnose produces the File Provider health rungs (extension enablement, app
// control socket, daemon data bridge) as renderer-agnostic findings. fpRows is
// the number of fileprovider account rows; consentPending is the daemon's
// group-container consent signal. With no rows and the extension unavailable it
// returns nil — the opt-in stack is unused. An absent extension is the root
// fault and skips the socket probes.
func fpDiagnose(ctx context.Context, spec fkoverlay.Spec, fpRows int, consentPending bool) []fpFinding {
	if !fpAvailable(spec) {
		if fpRows == 0 {
			return nil
		}
		en := fkoverlay.BackendFileProvider.Enablement()
		return []fpFinding{{"file provider extension", false, fmt.Sprintf(
			"not enabled with %s — run `ccp fp onboard` for the guided setup (install %s if missing, then: %s)",
			plural(fpRows, "fileprovider account"), pool.WidgetAppPath(), en.Guidance,
		)}}
	}
	findings := []fpFinding{{"file provider extension", true,
		fmt.Sprintf("%s; %s", pool.FPExtensionBundleID, plural(fpRows, "fileprovider account"))}}
	if ver, err := fpControlHealth(ctx); err != nil {
		findings = append(findings, fpFinding{"file provider app", false, fmt.Sprintf(
			"control socket %s not answering: %v — launch %s so domains can be registered and signalled",
			abbreviateHome(pool.FPControlSocketPath()), err, pool.WidgetAppPath(),
		)})
	} else {
		findings = append(findings, fpFinding{"file provider app", true, ver})
	}
	switch {
	case fpBridgeReachable():
		findings = append(findings, fpFinding{"file provider bridge", true, ""})
	case consentPending:
		findings = append(findings, fpFinding{"file provider bridge", false,
			"data socket " + abbreviateHome(pool.FPBridgeSocketPath()) + " not accepting — the daemon reports its bind parked on the one-time app group container consent prompt (macOS re-asks only if the binary's signing identity changes — e.g. an unsigned local build — and launchd never surfaces it): approve it, then restart the daemon (`brew services restart cc-pool`) — `ccp fp onboard` walks this end to end"})
	default:
		findings = append(findings, fpFinding{"file provider bridge", false,
			"data socket " + abbreviateHome(pool.FPBridgeSocketPath()) + " not accepting — the daemon binds it at startup and retries every few seconds (is the daemon running? check `ccp service status`); on first run macOS gates the app group container behind a one-time consent prompt: approve it, then restart the daemon; domains cannot fetch computed content until the socket is up — run `ccp fp onboard` for the guided setup"})
	}
	return findings
}

// fpSetupFailureSentinels are the fileproviderd sentinels a File Provider Setup
// surfaces when the OS or companion app cannot stand up a domain — every class the
// EnsureReport/Register and waitDomainServes chain can return (ErrBusy and
// ErrRegisterFailed come from a reached, entitled app's register). Slice form so a
// new sentinel is a one-line append.
var fpSetupFailureSentinels = []error{
	fileproviderd.ErrDomainNotServing,
	fileproviderd.ErrCannotControl,
	fileproviderd.ErrOpUnsupported,
	fileproviderd.ErrAppUnavailable,
	fileproviderd.ErrBusy,
	fileproviderd.ErrRegisterFailed,
	fileproviderd.ErrDomainRemovalUnconfirmed,
}

// isFPSetupFailure reports whether err is a File Provider Setup failure worth diagnosing.
func isFPSetupFailure(err error) bool {
	for _, s := range fpSetupFailureSentinels {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// diagnoseFPAddFailure prints a read-only File Provider diagnosis after a `ccp
// add` whose PrepareAdd failed on a File Provider Setup sentinel: first the data
// bridge's liveness, then the unhealthy health rungs, then the onboard pointer.
// A no-op for any other failure — it never probes, mutates, or waits beyond the
// probes' own bounds.
func diagnoseFPAddFailure(cmd *cobra.Command, m *pool.Manager, err error) {
	if !isFPSetupFailure(err) {
		return
	}
	errOut := cmd.ErrOrStderr()
	alive, consentPending := fpDaemonProbe()
	if !alive {
		warn(errOut, "the cc-pool daemon isn't running, so the File Provider content bridge is down — start it with `ccp service install`, then retry `ccp add`")
	}
	// fpRows=1 forces the extension rung to evaluate even before any fileprovider
	// row exists (the failed add would have been the first).
	first := true
	for _, f := range fpDiagnose(cmd.Context(), m.OverlaySpec(), 1, consentPending) {
		if f.healthy {
			continue
		}
		if first {
			fail(errOut, "%s: %s", f.label, f.detail)
			first = false
			continue
		}
		warn(errOut, "%s: %s", f.label, f.detail)
	}
	if errors.Is(err, fileproviderd.ErrDomainNotServing) || errors.Is(err, fileproviderd.ErrDomainRemovalUnconfirmed) {
		note(errOut, "the OS may still materialize this account's File Provider domain later as an orphan — `ccp doctor` flags it and `ccp doctor --fix` removes it.")
	}
	note(errOut, "run `ccp fp onboard` to walk the File Provider setup end to end, then re-run `ccp add`.")
}
