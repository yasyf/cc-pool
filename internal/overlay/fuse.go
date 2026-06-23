//go:build fuse && cgo && darwin

// Package overlay here defines the fuse provider: an in-process passthrough MIRROR of
// ~/.claude mounted at an account dir via fuse-t (kext-less, NFS-over-loopback,
// mounted as the user without root). A single backing dir means writes pass
// straight through to ~/.claude and are shared live — no copy-up.
//
// The mount/wait/bounded-teardown lifecycle is fusekit's: FuseProvider keeps the
// cc-pool mirror-specific code (the mirrorFS, the merged /.claude.json view with
// its shareable-key write-through, the live-symlink carve-out, the /.ccp-probe
// view, the byte-identical mount options, the libfuse-t pin) and delegates the
// actual mount/serve/teardown to a process-global fusekit.MountSet. From the
// extraction, cc-pool gains cgofuse-load panic recovery (a missing libfuse-t
// surfaces as ErrFuseUnavailable instead of crashing the holder) and pre-mount
// carcass cleanup (ClearCarcass).
//
// cgofuse drives fuse-t natively (it dlopens fuset.Dylib).
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

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/fuset"
	"github.com/yasyf/fusekit/mountd"
)

const fuseBuilt = true

// InProcessFuse returns the in-process fuse host that satisfies the mount
// holder's narrow seam (mountd.Host: Setup/Teardown/State), available only in
// fuse builds. Mounts it creates live and die with the calling process, so it
// must only be consumed by the holder process. FuseProvider is that host: it
// keeps the full overlay.Provider surface (the in-process mirror tests and the
// pool conversion tests drive it directly) and adds State for the holder. Every
// FuseProvider delegates to the same process-global registry (fuseMounts), so a
// mount one instance establishes is the same mount a later, separately
// constructed instance tears down.
func InProcessFuse() (mountd.Host, bool) { return &FuseProvider{}, true }

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
		_ = os.Setenv("CGOFUSE_LIBFUSE_PATH", fuset.Dylib)
	}
}

// Mount-up wait bounds, handed to fusekit via Config.Wait/FirstWait. The first
// mount in a process gets longer: a genuine "Network Volumes" TCC denial fails
// fast regardless, while a slow but granted fuse-t deserves the extra patience
// before fusekit surfaces the scary TCC walkthrough. 14s, not 15: the
// holder-side OpMount budget is the liveWithin probe (<=2s) + this wait + a 3s
// done-drain, and 2+14+3 = 19 must stay under the 20s mount op deadline in
// fusekit/mountd. fusekit owns the proven/unproven TCC deduction (markMountProven)
// and the bounded teardown now; these only feed its mount-up wait.
var (
	mountWait      = 8 * time.Second
	firstMountWait = 14 * time.Second
)

// unmountGrace bounds the post-unmount drain of the mirror's /.claude.json
// write-through (see Teardown). It matches fusekit's own graceful-unmount grace:
// a stuck local write must not hang teardown — it stays in writeErr/Health.
const unmountGrace = 3 * time.Second

// fuseMounts is the process-global registry of in-process ~/.claude mirrors,
// backed by fusekit's MountSet (the mount/wait/bounded-teardown lifecycle).
// Every FuseProvider delegates to it, exactly as the old package-global mounts
// map behaved: a mount Setup registers and a later — separately constructed —
// Teardown finds it, the pattern the daemon and pool/convert_fuse_test.go rely
// on. Build constructs the mirror Config for a (base, dir); StateFn reports the
// kernel-truth liveness pair.
var fuseMounts = &fusekit.MountSet{
	Build:   buildMirrorConfig,
	StateFn: func(base, dir string) (mounted, alive bool) { return Mounted(dir), MountAlive(base, dir) },
}

// mirrors tracks each live mirror's mirrorFS by mountpoint, beside fuseMounts'
// own Handle registry. fusekit's Handle carries only the cgofuse host; the
// mirrorFS (and its claudeJSON view) is cc-pool's, so Teardown's write-through
// drain and Health's error surfacing need it kept here. Guarded by mirrorMu,
// the same way the old mounts[dir].fs.cj reach worked.
var (
	mirrorMu sync.Mutex
	mirrors  = map[string]*mirrorFS{}
)

// buildMirrorConfig constructs the fusekit Config for a passthrough mirror of
// base at dir: the mirrorFS (which serves the merged /.claude.json and the
// virtual /.ccp-probe internally), the byte-identical fuse-t mount options, the
// mount-up wait bounds, and ClearCarcass. It is fuseMounts.Build, called once
// per first Setup of a dir, before fusekit serves it; the mirrorFS is recorded
// in mirrors so Teardown/Health can reach its claudeJSON view.
//
// fuse-t mount options (its NFS backend has NO soft/timeout/retrans knobs; the
// coherence lever is noattrcache). The backing ~/.claude is written directly by
// plain `claude` while a pooled session reads through this mirror, so disable
// attribute caching to avoid stale reads. noattrcache is LOAD-BEARING for
// /.claude.json specifically: with attr caching on, the NFS client clamps a
// merged-view read to a stale cached size and serves a TORN (truncated)
// /.claude.json after an external base edit — close-to-open does not save it
// (pinned by TestFuseAttrCacheNoTornRead). MountOptions.Build forces noattrcache
// on darwin regardless of AttrCache for exactly this reason. Removing it to kill
// the GETATTR storm needs /.claude.json moved off this mount first (see the perf
// notes / Phase 3b). nobrowse keeps the mount out of Finder sidebars. namedattr:
// macOS's NFS client defaults to nonamedattr, under which every xattr op on the
// mount fails ENOTSUP — and ENOTSUP from setxattr is exactly what trips xnu's
// AppleDouble fallback (bsd/vfs/vfs_xattr.c), littering ._<name> sidecar files
// into the backing dir. namedattr routes xattrs to the mirror's xattr ops
// instead (fuse-t supports NFSv4 named attributes but ships with them disabled
// by default since v1.0.46). rwsize=1048576: fuse-t's NFS read/write RPC size
// (default 32768). A wedged mirror's signature is a large sequential read
// hanging while small ops stay instant — multi-READ-RPC readahead (see
// probe.go). Raising rwsize to 1 MiB cuts the RPCs a bulk transfer fans out (a
// 1.5 MiB read drops from ~47 RPCs to 2), shrinking that readahead surface. 1
// MiB mounts and moves 32 MiB cleanly here; the intermittent wedge could not be
// reproduced on demand to A/B it, so this is a mechanism-driven mitigation —
// deep-probe detection (probe.go) stays the safety net.
func buildMirrorConfig(base, dir string) fusekit.Config {
	// The shared ~/.claude.json is a SIBLING of base (~/.claude), not inside it
	// — the third path the mirror needs, for the merged /.claude.json read view
	// and its shareable-key write-through.
	priv := FusePrivateRoot(dir)
	baseClaudeJSON := filepath.Join(filepath.Dir(base), ".claude.json")
	fs := newMirrorFS(base, priv, baseClaudeJSON)
	// Snapshot base's current top-level names before serving: those present as
	// live symlinks (sharedLink), carving claude's bulk I/O out of the NFS path.
	fs.snapshotShared()

	mirrorMu.Lock()
	mirrors[dir] = fs
	mirrorMu.Unlock()

	return fusekit.Config{
		Base: base,
		Dir:  dir,
		FS:   fs,
		Options: fusekit.MountOptions{
			Volname:   "cc-pool-" + filepath.Base(dir),
			NoBrowse:  true,
			NamedAttr: true,
			Extra:     []string{"rwsize=1048576"},
		}.Build(),
		Wait:      mountWait,
		FirstWait: firstMountWait,
		// ClearCarcass: force-unmount a dead-mount carcass a killed holder left at
		// dir before mounting over it (the extraction's gain).
		ClearCarcass: true,
		// CacheDefeat nil + Ready nil (fusekit defaults Ready to MountAlive(base,
		// dir), exactly cc-pool's readiness check) → cc-pool runtime byte-identical
		// apart from panic-recovery and ClearCarcass.
	}
}

// FuseProvider mounts a passthrough mirror of base at the account dir.
type FuseProvider struct{}

// Kind reports the provider kind (KindFuse).
func (p *FuseProvider) Kind() Kind { return KindFuse }

// PrivateRoot is the per-account backing dir beside the mountpoint. Private
// files written there are visible through the mount (mirrorFS redirects
// PrivateEntry names) and survive whether or not the mount is currently up.
func (p *FuseProvider) PrivateRoot(accountDir string) string {
	return FusePrivateRoot(accountDir)
}

// State reports the two kernel-truth halves of mirror liveness as a PAIR for
// the mount holder's seam (mountd.Host.State): mounted (accountDir is a
// mountpoint at all, the non-blocking Getfsstat read) and alive (base's
// contents are visible through it). The halves are returned independently and
// never collapsed — the holder keys foreign-mount refusal on mounted alone, the
// unmount no-op on !mounted, and idempotent mount/list on both; probeMount
// bounds the pair holder-side. It delegates to fuseMounts.State (the registry's
// StateFn).
func (p *FuseProvider) State(base, accountDir string) (mounted, alive bool) {
	return fuseMounts.State(base, accountDir)
}

// Setup mounts a passthrough mirror of base at accountDir. It refuses to operate
// on base itself — mounting over ~/.claude would shadow the user's real config
// dir — then does the cc-pool-specific pre-mount prep (mountpoint mkdir,
// AppleDouble litter sweep, private backing dirs) and hands the actual mount to
// fuseMounts, which blocks only until the mount is live (or a bounded timeout),
// serving in a goroutine. An already-mounted dir is an idempotent no-op inside
// fuseMounts.Setup.
func (p *FuseProvider) Setup(base, accountDir string) error {
	if accountDir == base || accountDir == "" {
		return fmt.Errorf("refusing to mount over base dir %q", accountDir)
	}
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		return err
	}

	sweepAppleDoubleLitter(base)

	// Private backing for excluded (instance-local) entries.
	priv := FusePrivateRoot(accountDir)
	for name := range ExcludedEntries {
		_ = os.MkdirAll(filepath.Join(priv, name), 0o700)
	}

	return fuseMounts.Setup(base, accountDir)
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
	mirrorMu.Lock()
	fs, ok := mirrors[accountDir]
	mirrorMu.Unlock()
	if !ok {
		return liveness
	}
	return errors.Join(liveness, fs.healthErr())
}

// Teardown unmounts the account dir's mirror via fuseMounts (cgofuse graceful
// unmount → MNT_FORCE → verify, all bounded by fusekit so a wedged fuse-t fault
// can't hang the daemon's shutdown), then drains both the /.claude.json and
// /settings.json write-throughs. It refuses to tear down base itself. The drain
// runs AFTER the unmount: once the mount is gone no further commit can schedule a
// write-through, so this captures the LAST one each view scheduled — .claude.json's
// shareable keys must still reach the real base sibling (~/.claude.json) and
// settings.json's last strip must still reach ~/.claude/settings.json (both plain
// files OUTSIDE the mount, which the write-through reads/writes directly), and the
// views are then discarded. Bounded like the unmount: a stuck local write must not
// hang teardown — it stays in writeErr/Health. Draining before the unmount would
// race an in-flight commit landing during the unmount window.
func (p *FuseProvider) Teardown(base, accountDir string) error {
	if accountDir == base || accountDir == "" {
		return fmt.Errorf("refusing to tear down base dir %q", accountDir)
	}
	mirrorMu.Lock()
	fs, ok := mirrors[accountDir]
	delete(mirrors, accountDir)
	mirrorMu.Unlock()

	err := fuseMounts.Teardown(base, accountDir)
	if ok {
		fs.cj.flushWithin(unmountGrace)
		fs.settings.flushWithin(unmountGrace)
	}
	return err
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
// process that will host mounts (capability + TCC grant are per-process). It
// delegates to fusekit, whose probe proves fuse-t works on this machine, trips
// the one-time "Network Volumes" privacy grant, and sets fusekit's process-
// global mount-proven deduction the real mounts then read.
func HostProbe() (bool, error) { return fusekit.HostProbe() }
