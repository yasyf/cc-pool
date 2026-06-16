//go:build fuse && cgo && darwin

// Package overlay's fuse provider: an in-process passthrough MIRROR of
// ~/.claude mounted at an account dir via fuse-t (kext-less, NFS-over-loopback,
// mounted as the user without root). A single backing dir means writes pass
// straight through to ~/.claude and are shared live — no copy-up.
//
// cgofuse drives fuse-t natively (it dlopens /usr/local/lib/libfuse-t.dylib).
// Build with: CGO_ENABLED=1 go build -tags fuse ./...
//
// Mounts are hosted in-process and block while serving, so the daemon owns
// their lifecycle (it calls Setup at startup and Teardown at shutdown). A
// short-lived CLI invocation cannot host a mount; for those, detection falls
// back to symlink until the daemon establishes the mount.
package overlay

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/unix"
)

const fuseBuilt = true

// InProcessFuse returns the in-process fuse provider, available only in fuse
// builds. Mounts it creates live and die with the calling process; once the
// mount-holder lands it must only be consumed by the holder process.
func InProcessFuse() (Provider, bool) { return &FuseProvider{}, true }

func init() {
	// This machine (and many dev Macs) may have BOTH macFUSE's libfuse.2.dylib
	// and fuse-t's libfuse-t.dylib. Without the override cgofuse dlopens
	// macFUSE's kext-backed lib first, so pin fuse-t explicitly unless the user
	// already set the override. CGOFUSE_LIBFUSE_PATH is honored (and tried
	// FIRST) only by cgofuse newer than v1.6.0 — go.mod pins a post-v1.6.0
	// commit for exactly this; v1.6.0 ignored the variable entirely. The
	// dlopen is lazy (first fuse call), so setting it here is in time, and
	// os.Setenv updates the C environment under cgo.
	if os.Getenv("CGOFUSE_LIBFUSE_PATH") == "" {
		_ = os.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	}
}

// mountRegistry tracks live mounts so Teardown can unmount the right host.
// everMountedLive is the sticky per-process "Network Volumes" TCC deduction:
// once any mount registers live the grant is proven for this process, and
// later mount-up timeouts are transient (ErrMountTimeout), not TCC
// (ErrMountNotLive). It is set beside the mounts map insert in Setup, which
// covers OpProbe's throwaway probe mount automatically: probeFuse drives
// FuseProvider.Setup, the only path that registers a mount. Guarded by
// mountMu like the map it shadows.
var (
	mountMu         sync.Mutex
	mounts          = map[string]*mountHandle{}
	everMountedLive bool
)

// mountProven reports whether this process has ever hosted a live mount —
// the proof that the "Network Volumes" TCC grant is held.
func mountProven() bool {
	mountMu.Lock()
	defer mountMu.Unlock()
	return everMountedLive
}

// Mount-up wait bounds, selected by mountProven. The first mount in a process
// gets longer: a genuine TCC denial fails fast regardless, while a slow but
// granted fuse-t deserves the extra patience before we surface the scary TCC
// walkthrough. 14s, not 15: the holder-side OpMount budget is liveWithin
// probe (<=2s) + wait + 3s done-drain, and 2+14+3 = 19 must stay under the
// 20s mount op deadline in internal/mountd. OpProbe shares that 20s deadline
// and its throwaway mount (HostProbe) is the process's first, so it waits the
// full 14s: the failure path fits (14s wait + 3s drain = 17), but a mount
// coming live near the 14s bound whose Teardown then wedges (3s unmountGrace
// + 2s forceGrace + <=2s bounded MountedWithin) totals ~21s — an accepted
// tail. That conjunction (a near-deadline first mount AND a wedged teardown
// of the just-live probe mount) is vanishingly narrow, and the blown deadline
// fails loud as one transient "mount holder probe failed" fuse-unavailable
// verdict (pool.DetectOverlayKind), re-proven on the next probe.
var (
	mountWait      = 8 * time.Second
	firstMountWait = 14 * time.Second
)

type mountHandle struct {
	host *fuse.FileSystemHost
	fs   *mirrorFS
	done chan struct{}
}

// FuseProvider mounts a passthrough mirror of base at the account dir.
type FuseProvider struct{}

func (p *FuseProvider) Kind() Kind { return KindFuse }

// PrivateRoot is the per-account backing dir beside the mountpoint. Private
// files written there are visible through the mount (mirrorFS redirects
// PrivateEntry names) and survive whether or not the mount is currently up.
func (p *FuseProvider) PrivateRoot(accountDir string) string {
	return FusePrivateRoot(accountDir)
}

// Setup mounts a passthrough mirror of base at accountDir. It blocks only until
// the mount is live (or a timeout). The serving loop runs in a goroutine.
// Like Teardown, it refuses to operate on base itself — mounting over
// ~/.claude would shadow the user's real config dir.
func (p *FuseProvider) Setup(base, accountDir string) error {
	if accountDir == base || accountDir == "" {
		return fmt.Errorf("refusing to mount over base dir %q", accountDir)
	}
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		return err
	}
	mountMu.Lock()
	if _, ok := mounts[accountDir]; ok {
		mountMu.Unlock()
		return nil // already mounted
	}
	mountMu.Unlock()

	sweepAppleDoubleLitter(base)

	// Private backing for excluded (instance-local) entries.
	priv := FusePrivateRoot(accountDir)
	for name := range ExcludedEntries {
		_ = os.MkdirAll(filepath.Join(priv, name), 0o700)
	}

	// The shared ~/.claude.json is a SIBLING of base (~/.claude), not inside
	// it — the third path the mirror needs, for the merged /.claude.json read
	// view and its shareable-key write-through.
	baseClaudeJSON := filepath.Join(filepath.Dir(base), ".claude.json")
	fs := newMirrorFS(base, priv, baseClaudeJSON)
	host := fuse.NewFileSystemHost(fs)
	host.SetCapReaddirPlus(true)
	done := make(chan struct{})

	// fuse-t mount options (its NFS backend has NO soft/timeout/retrans knobs;
	// the coherence lever is noattrcache). The backing ~/.claude is written
	// directly by plain `claude` while a pooled session reads through this
	// mirror, so disable attribute caching to avoid stale reads. nobrowse
	// keeps the mount out of Finder sidebars.
	// namedattr: macOS's NFS client defaults to nonamedattr, under which every
	// xattr op on the mount fails ENOTSUP — and ENOTSUP from setxattr is
	// exactly what trips xnu's AppleDouble fallback (bsd/vfs/vfs_xattr.c),
	// littering ._<name> sidecar files into the backing dir. namedattr routes
	// xattrs to the mirror's xattr ops instead (fuse-t supports NFSv4 named
	// attributes but ships with them disabled by default since v1.0.46).
	// rwsize=1048576: fuse-t's NFS read/write RPC size (default 32768). A wedged
	// mirror's signature is a large sequential read hanging while small ops stay
	// instant — multi-READ-RPC readahead (see probe.go). Raising rwsize to 1 MiB
	// cuts the RPCs a bulk transfer fans out (a 1.5 MiB read drops from ~47 RPCs
	// to 2), shrinking that readahead surface. 1 MiB mounts and moves 32 MiB
	// cleanly here; the intermittent wedge could not be reproduced on demand to
	// A/B it, so this is a mechanism-driven mitigation, not a measured one —
	// deep-probe detection (probe.go) stays the safety net.
	opts := []string{
		"-o", "volname=cc-pool-" + filepath.Base(accountDir),
		"-o", "noattrcache",
		"-o", "nobrowse",
		"-o", "namedattr",
		"-o", "rwsize=1048576",
	}
	go func() {
		defer close(done)
		// Mount blocks until unmounted. ok=false means the mount failed.
		_ = host.Mount(accountDir, opts)
	}()

	wait := mountWait
	if !mountProven() {
		wait = firstMountWait
	}
	if !waitMounted(base, accountDir, wait) {
		host.Unmount()
		// Bounded wait: a mount stuck on the one-time "Network Volumes" TCC
		// grant must not hang the daemon; the holder retries it.
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		// Re-read mountProven at failure time: a sibling mount coming live
		// mid-wait proves the grant and turns this from TCC into a transient
		// timeout — exactly the kill-9 revive-storm incident shape.
		return mountWaitErr(accountDir, wait, mountProven())
	}
	mountMu.Lock()
	mounts[accountDir] = &mountHandle{host: host, fs: fs, done: done}
	everMountedLive = true
	mountMu.Unlock()
	return nil
}

// Sync is a no-op for fuse: the mirror reflects base live, including new
// entries, so there is nothing to re-assert beyond confirming health.
func (p *FuseProvider) Sync(base, accountDir string) error {
	return p.Health(base, accountDir)
}

// Health verifies the mount is live by stat-ing a known entry through it, and
// joins in the mirror's sticky /.claude.json merged-read and base
// write-through errors so the daemon logs propagation failures every poll.
// The scheduler's reaction to a Health error is benign for a live mount: it
// retries Setup, which early-returns on the registered mount.
func (p *FuseProvider) Health(base, accountDir string) error {
	var liveness error
	if !MountAlive(base, accountDir) {
		liveness = fmt.Errorf("fuse mount at %s is not live", accountDir)
	}
	mountMu.Lock()
	h, ok := mounts[accountDir]
	mountMu.Unlock()
	if !ok {
		return liveness
	}
	return errors.Join(liveness, h.fs.healthErr())
}

const (
	// unmountGrace lets cgofuse's graceful Unmount complete before we escalate
	// to a forced kernel unmount.
	unmountGrace = 3 * time.Second
	// forceGrace bounds the wait for the serving goroutine to exit after a
	// forced unmount, so a wedged fuse-t fault can't hold shutdown open.
	forceGrace = 2 * time.Second
)

// Teardown unmounts the account dir's mirror. It is bounded: cgofuse's
// host.Unmount is a blocking cgo call that can wedge on a fuse-t fault, so it
// runs in a goroutine behind a grace timer and escalates to a forced kernel
// unmount — a synchronous unmount here would hang the daemon's shutdown and
// orphan it (which is exactly how the socket-holding orphan this guards against
// is born).
func (p *FuseProvider) Teardown(base, accountDir string) error {
	if accountDir == base || accountDir == "" {
		return fmt.Errorf("refusing to tear down base dir %q", accountDir)
	}
	mountMu.Lock()
	h, ok := mounts[accountDir]
	delete(mounts, accountDir)
	mountMu.Unlock()
	if !ok {
		// Not ours (e.g. left over from a prior run): forced best-effort unmount.
		_ = unix.Unmount(accountDir, unix.MNT_FORCE)
	} else {
		// host.Unmount returns once the mount is gone (graceful or forced); the
		// forced kernel unmount below guarantees that, so this goroutine exits.
		go h.host.Unmount()
		select {
		case <-h.done:
		case <-time.After(unmountGrace):
			_ = unix.Unmount(accountDir, unix.MNT_FORCE)
			select {
			case <-h.done:
			case <-time.After(forceGrace):
			}
		}
		// The mirror is gone; drain a write-through the last commit scheduled so
		// its shareable keys still reach the (real, non-mount) base file before
		// the view is discarded. Bounded like the unmount: a stuck local write
		// must not hang teardown — it stays in writeErr/Health.
		h.fs.cj.flushWithin(unmountGrace)
	}
	// Honest teardown: confirm the path is no longer a mountpoint. If the
	// unmount wedged (e.g. fuse-t issue-45), report it so callers do NOT
	// RemoveAll through a live mount into the backing ~/.claude. Bounded: the
	// stat itself can wedge with the mirror a forced unmount failed to clear,
	// and a probe that does not answer reads still-mounted — never torn down.
	if m, ok := MountedWithin(accountDir); !ok || m {
		return fmt.Errorf("%w: %s; refusing to treat it as torn down", ErrUnmountWedged, accountDir)
	}
	return nil
}

// sweepAppleDoubleLitter unlinks top-level AppleDouble sidecars of private
// entries from base. Pre-namedattr mounts misrouted them there: xnu's ENOTSUP
// fallback wrote "._<name>" beside where it believed "<name>" lived, and the
// mirror routed the sidecar of a private file into the shared base, where
// claude's tmp→rename commit orphaned it. The litter is provably mount-origin:
// plain claude's state file lives at base's SIBLING ~/.claude.json, so any
// ._.claude.json* (or other private-entry sidecar) inside base can only have
// come from the mirror's old misrouting. Janitorial and best-effort by design
// — a sweep failure must never block a mount. No other base mutations.
func sweepAppleDoubleLitter(base string) {
	entries, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "._") || !PrivateEntry(strings.TrimPrefix(name, "._")) {
			continue
		}
		_ = os.Remove(filepath.Join(base, name))
	}
}

// HostProbe attempts a throwaway in-process probe mount; it must run in the
// process that will host mounts (capability + TCC grant are per-process).
func HostProbe() bool { return probeFuse() }

// probeFuse attempts a throwaway mount to confirm fuse-t works on this machine
// (and trips the one-time "Network Volumes" privacy grant). Used by Detect.
func probeFuse() bool {
	tmp, err := os.MkdirTemp("", "ccp-fuse-probe-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(tmp)
	src := filepath.Join(tmp, "src")
	mnt := filepath.Join(tmp, "mnt")
	_ = os.MkdirAll(src, 0o700)
	_ = os.MkdirAll(mnt, 0o700)
	_ = os.WriteFile(filepath.Join(src, "probe"), []byte("ok"), 0o600)

	p := &FuseProvider{}
	if err := p.Setup(src, mnt); err != nil {
		return false
	}
	defer p.Teardown(src, mnt)
	_, err = os.Stat(filepath.Join(mnt, "probe"))
	return err == nil
}
