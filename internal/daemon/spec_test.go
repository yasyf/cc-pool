package daemon

import (
	"testing"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/daemonkit"
)

func testSpec(t *testing.T) daemonkit.Daemon {
	t.Helper()
	return Spec(daemonkit.Program{}, nil)
}

// TestSpecMaxDetailCarriesTheLargestPollPage is trap 1 of the migration guide:
// MaxDetail subtracts the frame envelope and the base64 reserve from MaxFrame,
// so a MaxFrame chosen without it can silently sit below a full poll page.
func TestSpecMaxDetailCarriesTheLargestPollPage(t *testing.T) {
	if got := daemonkit.MaxDetail(testSpec(t).MaxFrame); got < maxPayload {
		t.Fatalf("MaxDetail(MaxFrame) = %d, want >= the %d-byte maximum payload", got, maxPayload)
	}
}

func TestSpecConcurrencyCarriesEveryParkedPollPlusForegroundWork(t *testing.T) {
	parked := accountTerminalLimit * accountterminal.TerminalAttachmentLimit
	if parked != 128 {
		t.Fatalf("worst-case parked polls = %d, want 4 terminals x 32 attachments = 128", parked)
	}
	want := parked + foregroundHeadroom
	if want != 136 {
		t.Fatalf("concurrency arithmetic = %d, want 128 parked + 8 foreground = 136", want)
	}
	if got := testSpec(t).Concurrency; got != want {
		t.Fatalf("Concurrency = %d, want %d", got, want)
	}
}

func TestSpecStatesTheSameUserFloorAndRestartAlways(t *testing.T) {
	spec := testSpec(t)
	if spec.Trust.Control != nil {
		t.Errorf("production Trust.Control = %+v, want nil — the same-user floor alone", spec.Trust.Control)
	}
	if spec.Trust.Business != nil {
		t.Errorf("production Trust.Business = %+v, want nil — the same-user floor alone", spec.Trust.Business)
	}
	if spec.Trust.Serving != daemonkit.ServingSameUser() {
		t.Errorf("Trust.Serving = %+v, want the same-user posture", spec.Trust.Serving)
	}
	// The zero Restart is RestartNever, which would leave a drained daemon
	// dead until the next CLI call: state it.
	if spec.Restart != daemonkit.RestartAlways {
		t.Errorf("Restart = %v, want RestartAlways stated explicitly", spec.Restart)
	}
}

func TestSpecCarriesExactlyTheCurrentSchema(t *testing.T) {
	spec := testSpec(t)
	if len(spec.Schemas) != 1 || spec.Schemas[0] != RuntimeSchema {
		t.Fatalf("Schemas = %v, want exactly [%s]", spec.Schemas, RuntimeSchema)
	}
	if spec.Label != ServiceRoleID {
		t.Fatalf("Label = %q, want %q", spec.Label, ServiceRoleID)
	}
}

func TestSpecAcceptsAnInjectedControlRequirement(t *testing.T) {
	want := daemonkit.Requirement{TeamID: ServiceTeamID, SigningIdentifier: ServiceRoleID}
	spec := Spec(daemonkit.Program{}, &want)
	if spec.Trust.Control == nil || spec.Trust.Control.TeamID != want.TeamID ||
		spec.Trust.Control.SigningIdentifier != want.SigningIdentifier {
		t.Fatalf("Trust.Control = %+v, want the injected requirement", spec.Trust.Control)
	}
}
