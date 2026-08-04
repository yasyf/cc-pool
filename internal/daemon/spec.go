package daemon

import (
	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/daemonkit"
)

// daemonHandshakeTimeout is the admission budget, inherited from the retired
// ladder's widest client connect ceiling.
const daemonHandshakeTimeout = selectConnTimeout

// pollPageBytes is the largest poll page: every chunk a page may carry, at the
// largest chunk a terminal retains.
const pollPageBytes = PollPageChunks * accountterminal.TerminalChunkSize

// statusSnapshotBytes is the headroom a full status response needs beside a
// poll page. Status projects every account with its score components, and it
// is the only response that grows with the pool.
const statusSnapshotBytes = 1 << 20

// maxPayload is the largest response body the daemon ever serializes.
const maxPayload = pollPageBytes + statusSnapshotBytes

// daemonMaxFrame carries maxPayload with room for the frame envelope and the
// base64 reserve. spec_test.go pins MaxDetail(daemonMaxFrame) >= maxPayload so
// the ceiling cannot silently shrink below what a poll page needs.
const daemonMaxFrame daemonkit.Bytes = 4 << 20

// daemonConcurrency is every request that can be in flight at once. A parked
// poll holds a business slot for as long as it parks, and daemonkit runs one
// global in-flight pool, so the bound is the real worst case: one poll per
// attachment, at most TerminalAttachmentLimit attachments per terminal, across
// accountTerminalLimit terminals — 4 x 32 = 128 parked polls — plus 8 for the
// foreground verbs that must still dispatch through them (StartOrAttach,
// ProvideInput, Cancel, Ack, select, status, stop).
const daemonConcurrency = accountTerminalLimit*accountterminal.TerminalAttachmentLimit + foregroundHeadroom

const foregroundHeadroom = 8

// Spec is the cc-pool daemon's whole identity, read by the process that serves
// it and by every client that reaches it.
//
// control is a test seam. Every production call passes nil, which is the
// same-user floor: cc-pool's control callers are the Homebrew CLI and the
// status widget, both unsigned or same-user, and Trust.Control gates Health,
// WaitReady, Ensure and Drain alike — so a stated requirement would lock out
// the CLI lane it is meant to protect.
func Spec(program daemonkit.Program, control *daemonkit.Requirement) daemonkit.Daemon {
	return daemonkit.Daemon{
		Label:   ServiceRoleID,
		Program: program,
		Args:    []string{"daemon"},
		Schemas: []daemonkit.Schema{RuntimeSchema},
		Trust: daemonkit.Trust{
			Serving:  daemonkit.ServingSameUser(),
			Control:  control,
			Business: nil,
		},
		Restart:     daemonkit.RestartAlways,
		Shutdown:    daemonkit.Grace(daemonShutdownTimeout),
		Handshake:   daemonkit.Grace(daemonHandshakeTimeout),
		MaxFrame:    daemonMaxFrame,
		Concurrency: daemonConcurrency,
	}
}

// ProductionSpec is the daemon identity every real entry point uses: the
// stable executable path launchd runs, and the same-user control floor.
func ProductionSpec() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable()
	if err != nil {
		return daemonkit.Daemon{}, err
	}
	return Spec(program, nil), nil
}
