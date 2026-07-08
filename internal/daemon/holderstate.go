package daemon

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// holderRefreshFloor bounds select-path refreshes: a down mount must not turn
// every select into holder RPCs.
const holderRefreshFloor = 5 * time.Second

// deepProbe is a test seam; overlay (StatProbes) singleflights concurrent
// probes of one dir, so no inflight machinery here.
var deepProbe = overlay.DeepProbeWithin

// deepProbeInterval throttles the periodic probe (var for tests); the
// select-time probe is deliberately unthrottled — it is a correctness gate.
var deepProbeInterval = 30 * time.Second

// deepWedgeStrikes and shallowDeadStrikes (the deep-probe wedge and List-liveness
// remount debounces) live in policies.go — the self-heal policy substrate.

// deepVerdict is one dir's deep-probe state (wedged: serves metadata, hangs
// bulk reads); guarded by holderState.mu.
type deepVerdict struct {
	strikes int
	wedged  bool
}

// holderState caches holder truth: reachability, version, per-dir mount
// liveness. The select path keys fuse readiness on it — an lstat through a
// dead fuse-t NFS mount can hang.
type holderState struct {
	mu      sync.Mutex
	healthy bool
	version string
	mounts  map[string]bool // Live (shallow), per the holder's last List
	// servedMounts remembers the holder was last observed healthy with mounts;
	// it survives the degraded step (reachable, List failing mid-crash) so the
	// crash still reads lost-with-mounts once the holder goes unreachable.
	servedMounts bool
	// deep is daemon-local, not holder-sourced; it survives refresh (a poll
	// does not re-probe).
	deep        map[string]*deepVerdict
	lastProbed  map[string]time.Time
	shallow     map[string]int
	refreshedAt time.Time
	// tccErr is the latest TCC-blocked mount guidance for status/doctor; any
	// successful mount clears it — the TCC grant is per holder process.
	tccErr string
	// tccBackend is the backend needing the grant; set and cleared with tccErr.
	tccBackend fkoverlay.Backend

	// gen counts in-place cache mutations; refresh drops a polled snapshot when
	// gen moved mid-flight — an in-place update is newer truth than the List.
	gen uint64

	// onLostWithMounts, when set, fires once when a holder last seen serving
	// mounts becomes unreachable — the dead-holder-with-orphans signature (see
	// ccn doc 1668381). Nil in tests that don't care.
	onLostWithMounts func()
}

func (h *holderState) refresh(c *mountd.Client) {
	h.mu.Lock()
	startGen := h.gen
	h.mu.Unlock()
	res, _ := c.Poll() // error unused: Reachable/Degraded encode the outcome
	if !res.Reachable {
		h.markUnhealthy()
		return
	}
	if res.Degraded {
		h.markDegraded(res.Version)
		return
	}
	m := make(map[string]bool, len(res.Mounts))
	for _, mi := range res.Mounts {
		// The holder reports the served path (a mux subtree ~/.cc-pool/mnt/acct-NN);
		// the daemon keys the cache on the account ConfigDir. This is the ONE place
		// the wire dir is translated — everything downstream stays ConfigDir-keyed.
		m[pool.ConfigDirForMount(mi.Dir)] = mi.Live
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.gen != startGen {
		// Raced by an in-place update (see gen); refreshedAt stays stale so the
		// next refreshIfStale re-polls promptly.
		return
	}
	h.healthy, h.version, h.mounts, h.refreshedAt = true, res.Version, m, time.Now()
	// A clean reachable observation is truth either way: with mounts it arms
	// the lost-with-mounts memory, empty it disarms it.
	h.servedMounts = len(m) > 0
	h.pruneAbsentVerdictsLocked(m)
}

func (h *holderState) pruneAbsentVerdictsLocked(listed map[string]bool) {
	for dir := range h.deep {
		if _, ok := listed[dir]; !ok {
			delete(h.deep, dir)
		}
	}
	for dir := range h.lastProbed {
		if _, ok := listed[dir]; !ok {
			delete(h.lastProbed, dir)
		}
	}
	for dir := range h.shallow {
		if _, ok := listed[dir]; !ok {
			delete(h.shallow, dir)
		}
	}
}

func (h *holderState) view() (healthy bool, version string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy, h.version
}

// refreshIfStale backstops a select racing the startup prime or a CLI-made
// mount; socket RPC only, never a filesystem touch (mountReady's contract).
func (h *holderState) refreshIfStale(c *mountd.Client) {
	h.mu.Lock()
	fresh := time.Since(h.refreshedAt) < holderRefreshFloor
	h.mu.Unlock()
	if fresh {
		return
	}
	h.refresh(c)
}

// markUnhealthy records an unreachable holder (version "" is the wire signal),
// firing onLostWithMounts once per death, off the lock.
func (h *holderState) markUnhealthy() {
	h.mu.Lock()
	lost := h.servedMounts || (h.healthy && len(h.mounts) > 0)
	h.gen++
	h.healthy, h.version, h.mounts, h.refreshedAt = false, "", nil, time.Now()
	h.servedMounts = false // the sweep is scheduled; one fire per death
	// A respawned holder starts clean.
	h.deep, h.lastProbed, h.shallow = nil, nil, nil
	hook := h.onLostWithMounts
	h.mu.Unlock()
	if lost && hook != nil {
		hook()
	}
}

// markDegraded records Health-ok/List-failed: mounts fail closed, version
// kept for status. The holder is still REACHABLE, so no loss hook fires here —
// but a crash often tears down through this state, so latch servedMounts for
// markUnhealthy instead of losing the memory with the mounts map.
func (h *holderState) markDegraded(ver string) {
	h.mu.Lock()
	if h.healthy && len(h.mounts) > 0 {
		h.servedMounts = true
	}
	h.gen++
	h.healthy, h.version, h.mounts, h.refreshedAt = false, ver, nil, time.Now()
	h.deep, h.lastProbed, h.shallow = nil, nil, nil
	h.mu.Unlock()
}

func (h *holderState) wedgedLocked(dir string) bool {
	v := h.deep[dir]
	return v != nil && v.wedged
}

// ready reports a servable mirror at dir; a deep-wedged mirror stays
// shallow-Live, so the verdict fold is what excludes it from selection.
func (h *holderState) ready(dir string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy && h.mounts[dir] && !h.wedgedLocked(dir)
}

// shallowLive is ready without the deep-probe fold: a deep-wedged mount must
// stay probe-eligible so a recovery probe can clear the verdict.
func (h *holderState) shallowLive(dir string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.healthy && h.mounts[dir]
}

// heldDead reports a healthy holder listing dir as unservable: not Live or
// deep-wedged. Presence gates it — the holder lists a mount only after a
// successful Setup, so a TCC-blocked or never-mounted dir can never hot-loop.
func (h *holderState) heldDead(dir string) (dead, wedged bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	live, present := h.mounts[dir]
	w := h.wedgedLocked(dir)
	dead = h.healthy && present && (!live || w)
	return dead, dead && w
}

func (h *holderState) deepWedged(dir string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wedgedLocked(dir)
}

func (h *holderState) dueForDeepProbe(dir string, now time.Time, interval time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	last, ok := h.lastProbed[dir]
	return !ok || now.Sub(last) >= interval
}

// recordDeep folds one probe outcome into dir's debounced verdict.
// overlay.ErrProbeMissing is never a strike (a pre-probe holder's mirror
// across an upgrade).
func (h *holderState) recordDeep(dir string, err error) (logMsg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stampProbedLocked(dir)
	if errors.Is(err, overlay.ErrProbeMissing) {
		return ""
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

// markDeepWedged wedges dir immediately, bypassing the strike debounce: an
// idle mirror has no live session a false positive could orphan.
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

// recordShallowDead bumps dir's strike count; callers count only corroborated
// unresponsive-yet-peer-alive readings (see shallowDeadStrikes).
func (h *holderState) recordShallowDead(dir string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shallow == nil {
		h.shallow = map[string]int{}
	}
	h.shallow[dir]++
	return h.shallow[dir]
}

func (h *holderState) resetShallowDead(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.shallow, dir)
}

func (h *holderState) stampProbedLocked(dir string) {
	if h.lastProbed == nil {
		h.lastProbed = map[string]time.Time{}
	}
	h.lastProbed[dir] = time.Now()
}

// noteMounted trusts a just-established mirror ahead of the next refresh; a
// successful Setup also proves the holder healthy.
func (h *holderState) noteMounted(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen++
	h.healthy = true
	if h.mounts == nil {
		h.mounts = map[string]bool{}
	}
	h.mounts[dir] = true
	h.servedMounts = true
	delete(h.deep, dir)
	delete(h.lastProbed, dir)
	delete(h.shallow, dir)
	h.tccErr = ""
	h.tccBackend = ""
}

// noteUnmounted drops a just-dismounted dir ahead of the next refresh; a
// deliberate drain of the last mount while the holder is HEALTHY disarms the
// lost-with-mounts memory. The healthy gate matters: after markDegraded wipes
// h.mounts to nil, an empty map is a stale cache, not a drain — disarming then
// would lose the crash memory the degraded→unreachable path depends on.
func (h *holderState) noteUnmounted(dir string) {
	h.mu.Lock()
	h.gen++
	delete(h.mounts, dir)
	if h.healthy && len(h.mounts) == 0 {
		h.servedMounts = false
	}
	delete(h.deep, dir)
	delete(h.lastProbed, dir)
	delete(h.shallow, dir)
	h.mu.Unlock()
}

func (h *holderState) recordTCC(msg string, backend fkoverlay.Backend) {
	h.mu.Lock()
	h.tccErr = msg
	h.tccBackend = backend
	h.mu.Unlock()
}

// wireStatus snapshots the cache. Version "" means unreachable — or a mount
// trusted via noteMounted before any refresh.
func (h *holderState) wireStatus() *HolderStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	live := 0
	for dir, ok := range h.mounts {
		// Wedged never counts live (matches ready).
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
		Version:           h.version,
		Mounts:            live,
		WedgedMounts:      wedged,
		TCCError:          h.tccErr,
		TCCBlockedBackend: h.tccBackend,
	}
}
