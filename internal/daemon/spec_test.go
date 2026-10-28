package daemon

import (
	"testing"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/daemonkit"
)

func testSpec(t *testing.T) daemonkit.Daemon {
	t.Helper()
	return Spec(daemonkit.Program{})
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

func TestSpecStatesRestartAlways(t *testing.T) {
	spec := testSpec(t)
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
