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

// fuseDeepWedgePolicy debounces the deep-probe wedged verdict (wedged: serves
// metadata, hangs bulk reads); fuseShallowDeadPolicy debounces the holder
// List-liveness remount. Their rows live in holderState.led, keyed by ConfigDir.
var (
	fuseDeepWedgePolicy   = policies["fuse.deepwedge"]
	fuseShallowDeadPolicy = policies["fuse.shallowdead"]
)

// holderState caches holder truth: reachability, version, per-dir mount
// liveness. The select path keys fuse readiness on it — an lstat through a
// dead fuse-t NFS mount can hang.
type holderState struct {
	mu      sync.Mutex
	healthy bool
	version string
	mounts  map[string]bool // Live (shallow), per the holder's last List
	// led holds the fuse.deepwedge / fuse.shallowdead ledger rows — daemon-local,
	// not holder-sourced; it survives refresh (a poll does not re-probe). Guarded
	// by h.mu, never the Server's ledMu: the verdicts reset in atomic lockstep
	// with the mount cache (refresh, noteMounted/noteUnmounted, holder death) and
	// fold into the select path's liveness reads.
	led         *ledgers
	lastProbed  map[string]time.Time
	refreshedAt time.Time
	// tccErr is the latest TCC-blocked mount guidance for status/doctor; any
	// successful mount clears it — the TCC grant is per holder process.
	tccErr string
	// tccBackend is the backend needing the grant; set and cleared with tccErr.
	tccBackend fkoverlay.Backend

	// gen counts in-place cache mutations; refresh drops a polled snapshot when
	// gen moved mid-flight — an in-place update is newer truth than the List.
	gen uint64
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
	h.pruneAbsentVerdictsLocked(m)
}

func (h *holderState) pruneAbsentVerdictsLocked(listed map[string]bool) {
	keep := func(dir string) bool { _, ok := listed[dir]; return ok }
	if h.led != nil {
		h.led.prune(fuseDeepWedgePolicy, keep)
		h.led.prune(fuseShallowDeadPolicy, keep)
	}
	for dir := range h.lastProbed {
		if _, ok := listed[dir]; !ok {
			delete(h.lastProbed, dir)
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

// markUnhealthy records an unreachable holder (version "" is the wire signal).
// Carcass recovery is the shared holder's job now (proven-dead pre-mount clear),
// so a holder death fires no consumer-side sweep.
func (h *holderState) markUnhealthy() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen++
	h.healthy, h.version, h.mounts, h.refreshedAt = false, "", nil, time.Now()
	// A respawned holder starts clean.
	h.led, h.lastProbed = nil, nil
}

// markDegraded records Health-ok/List-failed: mounts fail closed, version
// kept for status. The holder is still REACHABLE.
func (h *holderState) markDegraded(ver string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gen++
	h.healthy, h.version, h.mounts, h.refreshedAt = false, ver, nil, time.Now()
	h.led, h.lastProbed = nil, nil
}

// ledLocked returns the ledger store, allocating on first touch (holderState
// is used as a zero value); callers hold h.mu.
func (h *holderState) ledLocked() *ledgers {
	if h.led == nil {
		h.led = newLedgers()
	}
	return h.led
}

func (h *holderState) wedgedLocked(dir string) bool {
	return h.led != nil && h.led.faulted(fuseDeepWedgePolicy, dir)
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
	led := h.ledLocked()
	if err == nil {
		if led.faulted(fuseDeepWedgePolicy, dir) {
			logMsg = fmt.Sprintf("deep probe %s: recovered; the mirror reads live again", dir)
		}
		led.clear(fuseDeepWedgePolicy, dir)
		return logMsg
	}
	before := led.faulted(fuseDeepWedgePolicy, dir)
	led.strike(fuseDeepWedgePolicy, dir, time.Now(), err)
	if !before && led.faulted(fuseDeepWedgePolicy, dir) {
		logMsg = fmt.Sprintf("deep probe %s: %d consecutive failures; marking the mirror wedged (serves metadata but hangs bulk reads): %v", dir, deepWedgeStrikes, err)
	}
	return logMsg
}

// markDeepWedged wedges dir immediately, bypassing the strike debounce: an
// idle mirror has no live session a false positive could orphan.
func (h *holderState) markDeepWedged(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stampProbedLocked(dir)
	h.ledLocked().forceFault(fuseDeepWedgePolicy, dir, time.Now(), nil)
}

// recordShallowDead bumps dir's strike count; callers count only corroborated
// unresponsive-yet-peer-alive readings (see shallowDeadStrikes).
func (h *holderState) recordShallowDead(dir string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	led := h.ledLocked()
	led.strike(fuseShallowDeadPolicy, dir, time.Now(), nil)
	if led.faulted(fuseShallowDeadPolicy, dir) {
		return shallowDeadStrikes
	}
	return led.peek(fuseShallowDeadPolicy, dir).strikes
}

func (h *holderState) resetShallowDead(dir string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.led != nil {
		h.led.clear(fuseShallowDeadPolicy, dir)
	}
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
	h.clearVerdictsLocked(dir)
	h.tccErr = ""
	h.tccBackend = ""
}

// noteUnmounted drops a just-dismounted dir ahead of the next refresh.
func (h *holderState) noteUnmounted(dir string) {
	h.mu.Lock()
	h.gen++
	delete(h.mounts, dir)
	h.clearVerdictsLocked(dir)
	h.mu.Unlock()
}

// clearVerdictsLocked drops dir's wedge/shallow-dead verdicts and probe clock —
// a fresh (un)mount resets its self-heal history; callers hold h.mu.
func (h *holderState) clearVerdictsLocked(dir string) {
	if h.led != nil {
		h.led.clear(fuseDeepWedgePolicy, dir)
		h.led.clear(fuseShallowDeadPolicy, dir)
	}
	delete(h.lastProbed, dir)
}

func (h *holderState) recordTCC(msg string, backend fkoverlay.Backend) {
	h.mu.Lock()
	h.tccErr = msg
	h.tccBackend = backend
	h.mu.Unlock()
}

// ledgersSnapshot lists the holder cache's ledger rows (fuse.deepwedge /
// fuse.shallowdead) for the composed status wire — bookkeeping only, under
// h.mu.
func (h *holderState) ledgersSnapshot() []ledgerSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.led == nil {
		return nil
	}
	return h.led.snapshot()
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
	if h.led != nil {
		for _, snap := range h.led.snapshot() {
			if snap.Policy == fuseDeepWedgePolicy.name && snap.Faulted {
				wedged++
			}
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
