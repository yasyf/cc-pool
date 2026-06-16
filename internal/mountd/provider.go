package mountd

import (
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
)

// RemoteProvider implements overlay.Provider by driving the detached
// mount-holder over its socket, so the mounts outlive the daemon and CLI
// processes that ask for them. It compiles in every build variant: a running
// holder is usable by any build, and only the spawn path inside EnsureRunning
// requires the fuse build.
type RemoteProvider struct {
	// Socket is the mount-holder's unix socket path.
	Socket string
	// LogPath receives a spawned holder's stdout and stderr.
	LogPath string
	// SpawnTimeout bounds waiting for a freshly spawned holder's socket.
	// Zero means DefaultSpawnTimeout.
	SpawnTimeout time.Duration
}

var _ overlay.Provider = (*RemoteProvider)(nil)

// NewRemoteProvider returns a RemoteProvider for the given holder socket and
// log path.
func NewRemoteProvider(socket, logPath string) *RemoteProvider {
	return &RemoteProvider{Socket: socket, LogPath: logPath}
}

func (p *RemoteProvider) spawnTimeout() time.Duration {
	if p.SpawnTimeout > 0 {
		return p.SpawnTimeout
	}
	return DefaultSpawnTimeout
}

// Kind always reports KindFuse: the provider stands in for the fuse overlay
// regardless of whether this build could host the mounts itself, so every
// Kind() fence keyed on KindFuse stays honest.
func (p *RemoteProvider) Kind() overlay.Kind { return overlay.KindFuse }

// overlayClass dual-wraps a wire sentinel with its overlay equivalent
// (multi-%w), honoring overlay/errors.go's contract: a caller holding an
// overlay.Provider classifies with the overlay sentinels no matter which
// process detected the condition — the in-process FuseProvider and this
// remote one must be errors.Is-identical. The wire identity stays in the
// chain for mountd-aware callers.
func overlayClass(err error) error {
	switch {
	case errors.Is(err, ErrTCCDenied):
		return fmt.Errorf("%w: %w", overlay.ErrMountNotLive, err)
	case errors.Is(err, ErrMountTimeout):
		return fmt.Errorf("%w: %w", overlay.ErrMountTimeout, err)
	case errors.Is(err, ErrUnmountWedged):
		return fmt.Errorf("%w: %w", overlay.ErrUnmountWedged, err)
	default:
		return err
	}
}

// Setup ensures a live mirror of base at accountDir. A mirror that is already
// mounted and shallow-live is adopted with zero RPC — the holder kept serving
// it across a daemon restart; the adoption stat is bounded (probeMount), so a
// wedged mirror reads not-adoptable and routes to the holder instead of
// hanging the caller. A partial wedge (shallow-alive, bulk reads hang) is NOT
// distinguished here — detecting and healing it is the daemon's job: it
// deep-probes the live mirror and, on a wedge, tears it down (mountFuse) so
// the dir reads not-mounted by the time this Setup runs and falls through to a
// fresh Mount. Otherwise the holder is spawned if needed and asked to mount
// (ensure-mounted holder-side: a mirror the holder still holds but that died
// is remounted). A dead HOLDER's carcass — accountDir still a mountpoint but
// absent from the fresh holder's registry — fails with ErrForeignMount by
// design (the holder never stacks mounts): callers must Teardown(base,
// accountDir) to clear it, then retry Setup.
func (p *RemoteProvider) Setup(base, accountDir string) error {
	if st, ok := probeMount(base, accountDir); ok && st.mounted && st.alive {
		return nil
	}
	if err := EnsureRunning(p.Socket, p.LogPath, p.spawnTimeout()); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, err)
	}
	if err := NewClient(p.Socket).Mount(base, accountDir); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, overlayClass(err))
	}
	return nil
}

// Teardown unmounts the mirror at accountDir. Nothing mounted is an immediate
// no-op with no holder contact (this absorbs the pure build's "retreat from a
// fuse account" semantics). Otherwise the holder is spawned if needed — a
// fresh holder clears a dead holder's carcass via its registry-miss path —
// and asked to unmount; an OK reply is then re-verified against the local
// kernel state, because honesty across the RPC boundary requires it (a lost
// response or skewed holder must not read as a clean teardown). Both stats
// are bounded and fail closed: a probe that does not answer reads
// still-mounted — the pre-check proceeds to the holder rather than skipping
// the teardown, and the re-verify reports the wedge rather than vouching for
// a teardown it cannot see, so callers never RemoveAll through a live mount.
func (p *RemoteProvider) Teardown(base, accountDir string) error {
	if st, ok := probeMount(base, accountDir); ok && !st.mounted {
		return nil
	}
	if err := EnsureRunning(p.Socket, p.LogPath, p.spawnTimeout()); err != nil {
		return fmt.Errorf("unmount %s: %w", accountDir, err)
	}
	if err := NewClient(p.Socket).Unmount(base, accountDir); err != nil {
		return fmt.Errorf("unmount %s: %w", accountDir, overlayClass(err))
	}
	switch st, ok := probeMount(base, accountDir); {
	case !ok:
		return fmt.Errorf("unmount %s: holder reported success but the mountpoint stat did not answer within %s (wedged mirror?): %w", accountDir, liveProbeTimeout, overlay.ErrUnmountWedged)
	case st.mounted:
		return fmt.Errorf("unmount %s: holder reported success but it is still a mountpoint: %w", accountDir, overlay.ErrUnmountWedged)
	}
	return nil
}

// Sync re-asserts the overlay. The fuse mirror is live by construction, so
// there is nothing to repair — Sync is Health: report the mirror's state.
func (p *RemoteProvider) Sync(base, accountDir string) error {
	return p.Health(base, accountDir)
}

// Health reports whether accountDir is a live mirror of base. It is local
// kernel truth only — zero RPC — because it sits on the daemon's poll hot
// path, and it is bounded (probeMount): a wedged mirror's stats never return,
// and an unanswered probe reads dead so the caller routes the dir into the
// bounded teardown→remount recovery instead of blocking the scheduler with
// the account's poll claim held.
func (p *RemoteProvider) Health(base, accountDir string) error {
	st, ok := probeMount(base, accountDir)
	switch {
	case !ok:
		return fmt.Errorf("mount at %s did not answer a liveness stat within %s; treating it as dead (wedged mirror?)", accountDir, liveProbeTimeout)
	case !st.mounted:
		return fmt.Errorf("%s is not a mountpoint", accountDir)
	case !st.alive:
		return fmt.Errorf("mount at %s is dead: %s's contents are not visible through it", accountDir, base)
	}
	return nil
}

// PrivateRoot returns the fuse provider's per-account private backing dir.
func (p *RemoteProvider) PrivateRoot(accountDir string) string {
	return overlay.FusePrivateRoot(accountDir)
}
