package overlay

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Deliberately untagged, like probe.go: every build variant must compile these
// so the daemon and CLI can errors.Is File Provider probe verdicts across
// process boundaries.

var (
	// ErrFPProbeWedged means the served .claude.json could not be read through
	// the File Provider domain in time: the read parked past the timeout or the
	// open/read itself failed (EDEADLK, EPERM, EIO, …). The domain answers
	// control ops but hangs the data plane — the wedge cc-pool's control-plane
	// Health cannot see. Treat as dead.
	ErrFPProbeWedged = errors.New("file provider domain wedged")

	// ErrFPProbeMissing means the open returned ENOENT: the domain serves no
	// .claude.json (an account with no identity yet). Like ErrProbeMissing it is
	// no verdict, never a wedge — such a domain survives until it has content to
	// probe.
	ErrFPProbeMissing = errors.New("file provider .claude.json missing")

	// ErrFPProbeEmpty means the domain served a zero-byte .claude.json. FPFS
	// skips fetchContents at size 0, so a 0-byte read proves nothing about the
	// data plane; the caller strikes on it only when the content source's synth
	// read is genuinely non-empty (empty-when-nonempty-expected).
	ErrFPProbeEmpty = errors.New("file provider .claude.json empty")
)

// fpProbeTimeout bounds one FP domain read; a var so tests can shrink it.
var fpProbeTimeout = 5 * time.Second

// fpProbes joins concurrent FP domain probes per dir. Its OWN StatProbes,
// never shared with the deep or alive probes: a parked FP read must never block
// a probe of another class behind its join.
var fpProbes StatProbes[error]

// FPDomainProbeWithin reads configDir's served .claude.json in full through the
// File Provider domain within fpProbeTimeout, returning a verdict: nil (a
// non-empty read completed), ErrFPProbeMissing (ENOENT), ErrFPProbeEmpty (zero
// bytes), or ErrFPProbeWedged (timeout or any other read failure). configDir is
// a fail-closed symlink into the domain root, so the read traverses the domain's
// data plane. Concurrent callers join one read; a wedged probe parks one
// goroutine and fd, joined by later callers.
func FPDomainProbeWithin(configDir string) error {
	path := filepath.Join(configDir, claudeJSONName)
	err, ok := fpProbes.Do(configDir, fpProbeTimeout, func() error { return readFPClaudeJSON(path) })
	if !ok {
		return fmt.Errorf("%w: read of %s did not answer within %s", ErrFPProbeWedged, path, fpProbeTimeout)
	}
	return err
}

// readFPClaudeJSON is FPDomainProbeWithin's unbounded body; on a wedged domain
// it parks in open or read until the kernel answers.
func readFPClaudeJSON(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path is an account's overlay ConfigDir under cc-pool state
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", ErrFPProbeMissing, path)
		}
		// Only ENOENT is no-verdict; EDEADLK/EPERM/EIO are data-plane wedges.
		return fmt.Errorf("%w: open %s: %v", ErrFPProbeWedged, path, err)
	}
	defer func() { _ = f.Close() }()
	n, err := io.Copy(io.Discard, f)
	if err != nil {
		return fmt.Errorf("%w: read %s: %v", ErrFPProbeWedged, path, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s served 0 bytes", ErrFPProbeEmpty, path)
	}
	return nil
}
