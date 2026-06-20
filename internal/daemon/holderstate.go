package daemon

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/mountd"
)

// holderRefreshFloor rate-limits select-path cache refreshes: a fuse row the
// cache cannot vouch for triggers at most one holder round-trip per floor, so
// a pool with a genuinely down mount cannot turn every select into holder
// RPCs.
const holderRefreshFloor = 5 * time.Second

// deepProbe is the daemon's seam over the bounded deep bulk-read wedge probe
// (overlay.DeepProbeWithin, 5s); tests inject verdicts without a real fuse
// mount. Concurrent probes of the same dir — the periodic supervisor probe and
// any number of select-time probes — join one in-flight read inside overlay
// (StatProbes), so the daemon needs no claim/inflight machinery of its own.
var deepProbe = overlay.DeepProbeWithin

// deepProbeInterval throttles the periodic in-use probe: a dir is re-probed at
// most once per interval even though the supervisor ticks faster (so an in-use
// mount is not probed several times per interval). A var so tests can shrink
// it. The select-time probe is deliberately NOT throttled — it is a
// correctness gate on an idle mirror whose verdict may be cold or stale.
var deepProbeInterval = 30 * time.Second

// deepWedgeStrikes is how many consecutive periodic deep-probe failures flip a
// dir's verdict to wedged: two, so one transient slow read under load never
// un-vouches a healthy mirror serving live sessions. The select-time probe
// bypasses this debounce (markDeepWedged): an idle mirror about to serve a new
// session has no live session to spuriously orphan, so a single observed wedge
// is actionable.
const deepWedgeStrikes = 2

// deepVerdict is one dir's daemon-side deep-probe state. wedged flips at
// deepWedgeStrikes consecutive failures (recordDeep) or immediately at select
// time (markDeepWedged), and stays until a probe succeeds or the dir is
// remounted (noteMounted clears it). Guarded by holderState.mu.
type deepVerdict struct {
	strikes int
	wedged  bool
}

// holderState is the daemon's cache of mount-holder truth: reachability, the
// holder's version, and per-dir liveness of every mount the holder owns. The
// select path keys fuse readiness on it instead of stat-ing through
// mountpoints — an lstat through a dead fuse-t NFS mount can hang — so it is
// primed at serve start, refreshed by the startup reconcile, once per
// scheduler poll, and once per supervision tick, updated in place after a
// successful mount, and lazily refreshed by mountReady when it cannot vouch
// for a fuse dir (see refreshIfStale). Respawn/backoff policy lives in
// superviseHolder; this is only the cache it keys on.
type holderState struct {
	mu      sync.Mutex
	healthy bool
	// degraded is the holder-alive-but-List-failed verdict: Health answered (so
	// the holder is responsive at a known version) but List did not, so its
	// live-mount set is unknowable this poll. Distinct from !healthy (Health
	// itself failed → the holder is gone or socket-wedged): a degraded holder is
	// alive, version is KEPT, and mounts is fail-closed nil. degraded ⟹ !healthy.
	degraded bool
	version  string
	mounts   map[string]bool // dir -> Live (shallow), per the holder's last List
	// epochs and mountedAt mirror the holder's per-dir mount epoch and mount
	// time from the last List, keyed like mounts (one entry per registered
	// mount). A zero Epoch/MountedAt on the wire means the holder predates the
	// fields and stores as 0/zero-time here. Like mounts they describe a
	// reachable holder's registry, so they are replaced wholesale by refresh
	// and cleared by markUnhealthy.
	epochs    map[string]uint64
	mountedAt map[string]time.Time
	// deep is the daemon's OWN per-dir deep-probe verdict — NOT sourced from
	// the holder (which ships none). It is maintained by recordDeep/
	// markDeepWedged (the periodic supervisor probe and the select-time probe)
	// and persists across refresh (a poll does not re-probe); noteMounted/
	// noteUnmounted clear a dir's entry and markUnhealthy drops them all (an
	// unreachable holder's verdict is meaningless). lastProbed throttles the
	// periodic probe (per dir, once per deepProbeInterval).
	deep        map[string]*deepVerdict
	lastProbed  map[string]time.Time
	refreshedAt time.Time
	// bases mirrors the holder's dir -> base registry from the last
	// successful List. Unlike mounts it SURVIVES markUnhealthy: it exists so
	// reviveHolder can remount a dead holder's pre-row mounts (`ccp add`'s
	// login window — no account row names the dir yet), and by the time the
	// revive runs the cache has already been marked unhealthy. Replaced
	// wholesale by the next successful refresh; a deliberate dismount
	// (noteUnmounted) drops its entry so a later revive cannot resurrect it.
	bases map[string]string
	// everMounted records that a holder served at least one mount at some
	// point in this daemon's lifetime. It survives markUnhealthy: a dead
	// holder may still be worth respawning for its orphaned mirrors even when
	// no fuse row remains in the store.
	everMounted bool
	// spawnErr is the daemon's latest failed holder-spawn attempt, surfaced
	// via HolderStatus; "" when the last spawn succeeded or none was needed.
	spawnErr string
	// tccErr is the latest mount-blocked-pending-TCC guidance (the holder's
	// "Network Volumes" grant walkthrough), kept for status/doctor rendering;
	// "" when no mount is TCC-blocked. Cleared by the next successful mount,
	// which proves the grant landed (the grant is per holder process, so one
	// live mount clears it for all).
	tccErr string

	// gen counts in-place cache mutations (noteMounted, noteUnmounted,
	// markUnhealthy). refresh snapshots it before its RPCs and discards the
	// polled snapshot if it changed by install time: an in-place update is
	// event truth newer than a List computed holder-side before the event, so
	// installing the snapshot over it would be a lost update — un-vouching a
	// live fresh mount (and rate-limit-suppressing mountReady's backstop for
	// the next floor), or re-vouching mirrors a replace just swept.
	gen uint64
}

// refresh polls the holder once (Client.Poll = Health then List) and replaces
// the cache. The verdict splits three ways: a Health failure is unreachable
// (markUnhealthy — version cleared, the holder is gone or socket-wedged); a
// Health success with a List failure is DEGRADED (markDegraded — the holder is
// alive at a known version, but its mounts are unreadable this poll, so they
// fail closed); only a full success installs a mounts snapshot. The RPCs run
// outside the lock; a snapshot raced by an in-place update is discarded (see
// gen). A cache that cannot vouch for a dir must not let selection trust it.
func (h *holderState) refresh(c *mountd.Client) {
	h.mu.Lock()
	startGen := h.gen
	h.mu.Unlock()
	res, _ := c.Poll() // Reachable/Degraded encode the outcome; the raw error is unused on this hot poll path
	if !res.Reachable {
		h.markUnhealthy()
		return
	}
	if res.Degraded {
		h.markDegraded(res.Version)
		return
	}
	m := make(map[string]bool, len(res.Mounts))
	b := make(map[string]string, len(res.Mounts))
	e := make(map[string]uint64, len(res.Mounts))
	at := make(map[string]time.Time, len(res.Mounts))
	for _, mi := range res.Mounts {
		m[mi.Dir] = mi.Live
		b[mi.Dir] = mi.Base
		e[mi.Dir] = mi.Epoch
		if mi.MountedAt != 0 {
			at[mi.Dir] = time.Unix(mi.MountedAt, 0)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.gen != startGen {
		// An in-place update landed while this snapshot was in flight; the
		// snapshot may predate it. Drop it — refreshedAt deliberately stays
		// put, so the next refreshIfStale re-polls promptly.
		return
	}
	h.healthy, h.degraded, h.version, h.mounts, h.bases, h.refreshedAt = true, false, res.Version, m, b, time.Now()
	h.epochs, h.mountedAt = e, at
	// deep and lastProbed are the daemon's own probe state, NOT holder truth —
	// a List does not re-probe, so they persist across refresh untouched.
	if len(m) > 0 {
		h.everMounted = true
	}
}

// view snapshots holder reachability and the version it reported. A degraded
// holder reads healthy=false here (Health answered but the cache cannot vouch
// for any mount); routing that must distinguish degraded from unreachable uses
// viewState.
func (h *holderState) view() (healthy bool, version string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy, h.version
}

// viewState snapshots the three-way reachability verdict for superviseTick's
// routing: fully healthy, degraded (alive at version, mounts unreadable), or
// unreachable (both false). degraded ⟹ !healthy, so a caller routes degraded
// before the !healthy revive.
func (h *holderState) viewState() (healthy, degraded bool, version string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy, h.degraded, h.version
}

// hadMounts reports whether a holder ever served a mount in this daemon's
// lifetime (survives markUnhealthy — see everMounted).
func (h *holderState) hadMounts() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.everMounted
}

// mountDirs returns every dir in the holder's last List, live or dead —
// kernel-truth coverage for the skew-replace idle gate, including mounts
// whose account rows no longer exist.
func (h *holderState) mountDirs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	dirs := make([]string, 0, len(h.mounts))
	for dir := range h.mounts {
		dirs = append(dirs, dir)
	}
	return dirs
}

// carriedBases snapshots the holder's dir -> base registry from its last
// successful List. It survives markUnhealthy by design (see bases): a revive
// reads it to remount the dead holder's pre-row mounts.
func (h *holderState) carriedBases() map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	carry := make(map[string]string, len(h.bases))
	for dir, base := range h.bases {
		carry[dir] = base
	}
	return carry
}

// recordSpawnError keeps the latest holder-spawn failure for status
// rendering; "" clears it.
func (h *holderState) recordSpawnError(msg string) {
	h.mu.Lock()
	h.spawnErr = msg
	h.mu.Unlock()
}

// refreshIfStale runs one refresh iff the cache has never been refreshed or
// its last refresh is older than holderRefreshFloor. It is the select path's
// backstop for truth the poll cadence misses: a select racing the startup
// prime (the daemon socket binds before the startup goroutine runs) and a
// mount established outside the daemon (`ccp add` mounts from the CLI
// process). Bounded socket RPC only — never a filesystem touch, per
// mountReady's contract. The zero refreshedAt reads as maximally stale.
func (h *holderState) refreshIfStale(c *mountd.Client) {
	h.mu.Lock()
	fresh := time.Since(h.refreshedAt) < holderRefreshFloor
	h.mu.Unlock()
	if fresh {
		return
	}
	h.refresh(c)
}

// markUnhealthy records an unreachable holder: every mount entry is dropped
// and the version cleared — Version "" is the wire signal for unreachable.
// bases deliberately survives (see its doc): it is the only record of a dead
// holder's pre-row mounts, read by the revive that follows this very call.
func (h *holderState) markUnhealthy() {
	h.mu.Lock()
	h.gen++
	h.healthy, h.degraded, h.version, h.mounts, h.refreshedAt = false, false, "", nil, time.Now()
	h.epochs, h.mountedAt = nil, nil
	// An unreachable holder serves nothing, so its dirs' deep verdicts are
	// meaningless — drop them (and the probe clock) so a respawned holder's
	// fresh mounts start with a clean slate.
	h.deep, h.lastProbed = nil, nil
	h.mu.Unlock()
}

// markDegraded records a holder that answered Health at ver but whose List
// failed: it is alive at a known version, but its live-mount set is unreadable
// this poll, so mounts fail closed (nil → ready/shallowLive read not-live) and
// the version is KEPT so superviseTick can tell a skewed degraded holder (force
// a converge) from an our-version one (heal its rows). Like markUnhealthy it
// bumps gen so a racing snapshot is discarded, drops the now-unvouchable deep
// verdicts, and lets bases survive — but unlike it, keeps the version and sets
// the degraded flag.
func (h *holderState) markDegraded(ver string) {
	h.mu.Lock()
	h.gen++
	h.healthy, h.degraded, h.version, h.mounts, h.refreshedAt = false, true, ver, nil, time.Now()
	h.epochs, h.mountedAt = nil, nil
	h.deep, h.lastProbed = nil, nil
	h.mu.Unlock()
}

// wedgedLocked reports dir's cached deep-probe verdict. Caller holds h.mu.
func (h *holderState) wedgedLocked(dir string) bool {
	v := h.deep[dir]
	return v != nil && v.wedged
}

// ready reports whether the cache vouches for a live mirror at dir: a reachable
// holder, Live (shallow) in its last List, AND the daemon's own deep probe has
// not marked the mirror wedged. A deep-wedged mirror keeps shallow Live=true,
// so folding the verdict in is what excludes it from selection (and, being
// not-ready, sends it to the supervisor's heal).
func (h *holderState) ready(dir string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy && h.mounts[dir] && !h.wedgedLocked(dir)
}

// shallowLive reports whether a reachable holder vouches for dir's shallow
// liveness (mountpoint present, base visible) — ready() WITHOUT the deep-probe
// fold. The periodic probe gates on it: a mount the holder no longer serves is
// not worth deep-probing, and a deep-wedged mount (shallow-live) must stay
// probe-eligible so a recovery probe can clear the verdict.
func (h *holderState) shallowLive(dir string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy && h.mounts[dir]
}

// heldDead reports the held-dead signature for dir: a healthy holder NAMES the
// dir in its last List (its registry owns a mount there) yet the dir is not
// servable — either the holder reports it not Live (a plain-dead mirror: an
// out-of-band `umount -f` or a dead fuse-t worker, fails reads outright) or the
// daemon's own deep probe marked it wedged (shallow-alive, bulk reads hang).
// Present is the precise discriminator: refresh stores exactly one mounts entry
// per List row, and the holder registers a mount only after a successful Setup,
// so a TCC-blocked or never-mounted dir is ABSENT and reads false here —
// heldDead can never hot-loop a TCC-blocked row. wedged splits the two dead
// shapes for the caller's log copy.
func (h *holderState) heldDead(dir string) (dead, wedged bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	live, present := h.mounts[dir]
	w := h.wedgedLocked(dir)
	dead = h.healthy && present && (!live || w)
	return dead, dead && w
}

// deepWedged reports dir's cached deep-probe verdict (false when unknown).
func (h *holderState) deepWedged(dir string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wedgedLocked(dir)
}

// dueForDeepProbe reports whether dir has not been deep-probed within interval
// (a never-probed dir is always due). The periodic supervisor probe gates on
// it so an in-use mount is probed at most once per interval.
func (h *holderState) dueForDeepProbe(dir string, now time.Time, interval time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	last, ok := h.lastProbed[dir]
	return !ok || now.Sub(last) >= interval
}

// recordDeep folds one deep-probe outcome into dir's debounced verdict and
// stamps the probe time. A success resets the strike count and clears any
// wedge (returning a recovery log line); overlay.ErrProbeMissing is no verdict
// (a pre-probe holder's mirror across an upgrade — never a strike); any other
// failure is a strike, and deepWedgeStrikes consecutive strikes flip the
// verdict to wedged (returning the wedge log line). The returned string is ""
// when nothing log-worthy happened.
func (h *holderState) recordDeep(dir string, err error) (logMsg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stampProbedLocked(dir)
	if errors.Is(err, overlay.ErrProbeMissing) {
		return "" // no verdict
	}
	v := h.deep[dir]
	if v == nil {
		v = &deepVerdict{}
		if h.deep == nil {
			h.deep = map[string]*deepVerdict{}
		}
		h.deep[dir] = v
	}
	switch err {
	case nil:
		if v.wedged {
			logMsg = fmt.Sprintf("deep probe %s: recovered; the mirror reads live again", dir)
		}
		v.strikes, v.wedged = 0, false
	default:
		v.strikes++
		if v.strikes == deepWedgeStrikes {
			v.wedged = true
			logMsg = fmt.Sprintf("deep probe %s: %d consecutive failures; marking the mirror wedged (serves metadata but hangs bulk reads): %v", dir, v.strikes, err)
		}
	}
	return logMsg
}

// markDeepWedged forces dir's verdict to wedged immediately, bypassing the
// strike debounce, and stamps the probe time. The select-time probe uses it:
// an idle mirror about to serve a NEW session has no live session a false
// positive could orphan, so a single observed wedge is actionable — and the
// forced verdict both refuses the select and sends the row to the supervisor's
// heal, which clears it on a successful remount (noteMounted).
func (h *holderState) markDeepWedged(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stampProbedLocked(dir)
	v := h.deep[dir]
	if v == nil {
		v = &deepVerdict{}
		if h.deep == nil {
			h.deep = map[string]*deepVerdict{}
		}
		h.deep[dir] = v
	}
	v.strikes, v.wedged = deepWedgeStrikes, true
}

// stampProbedLocked records that dir was just deep-probed. Caller holds h.mu.
func (h *holderState) stampProbedLocked(dir string) {
	if h.lastProbed == nil {
		h.lastProbed = map[string]time.Time{}
	}
	h.lastProbed[dir] = time.Now()
}

// noteMounted records a mirror the daemon just established or adopted without
// waiting for the next refresh, so a select landing in between trusts it. It
// vouches for holder health too — a successful Setup proves a live mirror
// serves the dir — and clears any recorded TCC guidance; the next refresh
// restores polled truth.
func (h *holderState) noteMounted(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen++
	// A successful Setup proves the holder is fully serving this dir, so it
	// supersedes a prior degraded verdict (degraded ⟹ !healthy): clear both.
	h.healthy, h.degraded = true, false
	if h.mounts == nil {
		h.mounts = map[string]bool{}
	}
	h.mounts[dir] = true
	// A fresh mount supersedes any prior deep-probe verdict and probe clock for
	// the dir: the corpse is gone, so clear the wedge and let the dir be
	// re-probed promptly. Epoch and mount time are NOT fabricated — the next
	// refresh installs the holder's polled truth.
	delete(h.deep, dir)
	delete(h.lastProbed, dir)
	h.everMounted = true
	h.tccErr = ""
}

// noteUnmounted drops a dir the daemon just dismounted (a fuse→symlink
// conversion or fallback) without waiting for the next refresh, so neither
// selection nor HolderStatus.Mounts keeps vouching for a mirror that no
// longer exists; the next refresh restores polled truth.
func (h *holderState) noteUnmounted(dir string) {
	h.mu.Lock()
	h.gen++
	delete(h.mounts, dir)
	delete(h.bases, dir)
	delete(h.deep, dir)
	delete(h.lastProbed, dir)
	delete(h.epochs, dir)
	delete(h.mountedAt, dir)
	h.mu.Unlock()
}

// recordTCC keeps the latest TCC-blocked mount guidance for status rendering.
func (h *holderState) recordTCC(msg string) {
	h.mu.Lock()
	h.tccErr = msg
	h.mu.Unlock()
}

// wireStatus snapshots the cache as the status op's HolderStatus. Version ""
// means the holder was unreachable at the last refresh (or a fresh mount was
// trusted via noteMounted before any refresh succeeded); Skewed is asserted
// only against a version actually reported by a healthy holder.
func (h *holderState) wireStatus() *HolderStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	live := 0
	for dir, ok := range h.mounts {
		// A deep-wedged mirror is shallow-Live but not servable — count it only
		// under WedgedMounts, never as a healthy live mount (matches ready).
		if ok && !h.wedgedLocked(dir) {
			live++
		}
	}
	wedged := 0
	for _, v := range h.deep {
		if v.wedged {
			wedged++
		}
	}
	return &HolderStatus{
		Version:      h.version,
		Mounts:       live,
		WedgedMounts: wedged,
		Skewed:       h.healthy && h.version != "" && h.version != version.String(),
		TCCError:     h.tccErr,
		SpawnError:   h.spawnErr,
	}
}
