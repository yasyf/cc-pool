package daemon

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/launchd"
)

// crossEraGateTimeout bounds the whole flag-day gate: the legacy bootout plus
// the wait for the evicted incumbent to release the legacy listener lock.
// launchd's ExitTimeOut SIGKILLs a wedged legacy drain, so the wait is bounded
// by the legacy daemon's own 30s grace plus scheduling headroom.
const crossEraGateTimeout = 90 * time.Second

// launchctlRunner is cc-pool's exec-over-/bin/launchctl launchd.Runner;
// daemonkit binds its own runner at Open and does not export it. Only a
// positive code reports a launchctl exit status; an error means the command
// never ran.
func launchctlRunner(ctx context.Context, path string, args ...string) (string, int, error) {
	out, err := exec.CommandContext(ctx, path, args...).CombinedOutput() //nolint:gosec // G204 flags args: daemonkit hands this runner the /bin/launchctl constant and its own ValidateLabel-checked verbs, and argv form reaches no shell.
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out), exitErr.ExitCode(), nil
	}
	if err != nil {
		return string(out), 0, err
	}
	return string(out), 0, nil
}

// crossEraGate closes the one two-owner window Serve's flock cannot see: a
// live v0.20.9 incumbent, which holds a different lock and refuses a
// protocol-2 attach outright, so Client.Stop's markerless fallback is
// unreachable against it. The order is load-bearing: the direct
// launchd.RemoveUnmarked bootout first — acquired first, the lock would park
// forever behind a healthy incumbent nothing is killing — then the blocking
// acquire of the legacy listener lock, which v0.20.9 released only after full
// product settlement or by dying, so a successful acquire proves no legacy
// saga goroutine exists. The caller holds the lock for the process lifetime:
// the belt against a lingering legacy binary relaunching mid-cycle.
//
// One transition cycle only — ships in v0.21.x, deleted in v0.22, after which
// the stale lock file is doctor/uninstall cleanup.
func crossEraGate(ctx context.Context, run launchd.Runner, label string) (*durable.Lock, error) {
	gateCtx, cancel := context.WithTimeout(ctx, crossEraGateTimeout)
	defer cancel()
	if err := launchd.RemoveUnmarked(gateCtx, run, label); err != nil && !errors.Is(err, launchd.ErrMarked) {
		return nil, fmt.Errorf("boot out the pre-daemonkit daemon: %w", err)
	}
	lock, err := durable.AcquireLock(gateCtx, legacyListenerLockPath())
	if err != nil {
		return nil, fmt.Errorf("acquire the legacy daemon listener lock: %w", err)
	}
	return lock, nil
}

// legacyListenerLockPath is exactly the lock v0.20.9's listen() derived:
// Socket + ".lock" (daemonkit@v0.20.9 daemon/runtime.go:1086).
func legacyListenerLockPath() string {
	return pool.SocketPath() + ".lock"
}

// RemoveLegacyDaemon boots out a live pre-daemonkit LaunchAgent directly
// through launchd. Client.Stop cannot reach one — a pre-cut listener refuses
// the protocol-2 attach before Stop's markerless fallback runs — so install
// and uninstall lead with this call, then route the marked world through
// Client.Stop. A marked plist or no plist at all is success: no legacy
// incumbent exists.
func RemoveLegacyDaemon(ctx context.Context) error {
	removeCtx, cancel := context.WithTimeout(ctx, crossEraGateTimeout)
	defer cancel()
	if err := launchd.RemoveUnmarked(removeCtx, launchctlRunner, ServiceRoleID); err != nil && !errors.Is(err, launchd.ErrMarked) {
		return fmt.Errorf("boot out the pre-daemonkit daemon: %w", err)
	}
	return nil
}
