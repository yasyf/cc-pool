//go:build fuse && cgo && darwin

package overlay

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sync/singleflight"
)

// settingsJSONFusePath is the fuse path of claude's settings.json — the one
// other name in the mirror (beside claudeJSONFusePath) with a synthesized read
// view and base write-through. Unlike .claude.json it is a fully SHARED file:
// the view reads and writes the base settings.json directly, injecting
// plansDirectory only into the served bytes (see settingsView).
const settingsJSONFusePath = "/settings.json"

// settingsFhBase is the first handle ID handed out for settings synthetic
// read handles. It sits ABOVE the claudeJSON range start (syntheticFhBase =
// 1<<62) so settings handles and cj handles never collide: cj allocates upward
// from syntheticFhBase and settings allocates upward from settingsFhBase = 3<<61,
// each with a 2^61 runway, so neither allocator can reach the other's base. Both
// match syntheticFh (they live in the merged-view range [1<<62, ^uint64(0))), so
// every fuse handler must dispatch settingsFh BEFORE syntheticFh — a handle in
// the settings sub-range belongs to settings, everything else in the synthetic
// range to cj.
const settingsFhBase = uint64(3) << 61

// settingsFh reports whether fh is a settings synthetic read handle (the top
// sub-range of the merged-view space; see settingsFhBase). ^uint64(0) ("no
// handle") is excluded, exactly as syntheticFh excludes it.
func settingsFh(fh uint64) bool {
	return fh >= settingsFhBase && fh != ^uint64(0)
}

// settingsView serves /settings.json as a synthesized document: read opens get
// the base settings.json with plansDirectory injected (when base lacks it), and
// committed writes split the injected key back out so the base file stays
// pristine. Unlike claudeJSONView there is no private backing file and no
// two-file merge — base IS the only file — and no per-handle injected flag: the
// write-through strips plansDirectory by value-equality (only when its value
// equals the value we inject), so a stateless strip suffices.
type settingsView struct {
	basePath string // shared ~/.claude/settings.json
	plansDir string // the absolute value injected as plansDirectory (<root>/plans)

	// mergeSF collapses concurrent cache-miss recomputes of the served view to a
	// single recompute, the same thundering-herd guard claudeJSONView uses: under
	// many concurrent sessions a base write bumps every mount's cache key at once.
	// Keyed on the settingsKey, so misses for the same base snapshot share one
	// result. Has its own internal locking — never held under mu.
	mergeSF singleflight.Group

	mu        sync.Mutex          // guards every field below — the one synchronization story for this view
	nextFh    uint64              // next synthetic handle ID
	snapshots map[uint64][]byte   // synthetic fh → per-handle served snapshot
	dirtyFds  map[uint64]struct{} // real /settings.json fds that wrote or truncated; Release write-throughs only these
	cacheKey  settingsKey
	cacheBuf  []byte
	cacheOK   bool
	readErr   error // last served-read failure; cleared by the next successful recompute
	writeErr  error // last write-through failure; cleared only by a successful write-through

	// Write-through worker coordination (guarded by mu). The base
	// read→strip→write runs OFF the fuse handler path: Release/Rename call
	// scheduleWriteThrough and return immediately, so a contended writeThroughMu
	// or a slow base rewrite can never park a fuse-t worker thread and stall the
	// mount's NFS server (the "not responding" wedge). wtRunning marks a cycle
	// goroutine in flight; wtPending coalesces a commit arriving mid-cycle into
	// exactly one more cycle (which re-reads base, so the latest committed state
	// always wins); wtIdle, when non-nil, is closed when the worker goes idle, so
	// flushWithin can wait the last cycle out.
	wtRunning bool
	wtPending bool
	wtIdle    chan struct{}
}

// settingsKey identifies one base settings.json input for the served-view cache.
// Caching keyed on the base file's (mtime, size) is load-bearing, not an
// optimization: noattrcache — the only coherence lever fuse-t offers — makes the
// kernel getattr constantly. An absent base is encoded as -1s, distinct from any
// real stat.
type settingsKey struct {
	baseMtimeNS, baseSize int64
}

// sfKey renders the settings key as a singleflight key so concurrent recomputes
// for the same base snapshot coalesce.
func (k settingsKey) sfKey() string {
	return fmt.Sprintf("%d:%d", k.baseMtimeNS, k.baseSize)
}

func newSettingsView(basePath, plansDir string) *settingsView {
	return &settingsView{
		basePath:  basePath,
		plansDir:  plansDir,
		nextFh:    settingsFhBase,
		snapshots: map[uint64][]byte{},
		dirtyFds:  map[uint64]struct{}{},
	}
}

// statKey builds the served-view cache key. ok is false when the base file
// cannot be statted — nothing cacheable, and the read path is about to serve
// -ENOENT (an absent base is never synthesized; see merged).
func (v *settingsView) statKey() (settingsKey, bool) {
	bfi, err := os.Lstat(v.basePath)
	if err != nil {
		return settingsKey{}, false
	}
	return settingsKey{baseMtimeNS: bfi.ModTime().UnixNano(), baseSize: bfi.Size()}, true
}

// merged materializes the current served /settings.json: the base file with
// plansDirectory injected when absent. An ABSENT base is plain -ENOENT — the
// file is never minted (matching the .claude.json onboarding policy; the user's
// settings.json exists, so this is the non-exercised fallback). An unparseable
// or non-object base falls back to the raw base bytes and records the read error
// for healthErr; the session never sees EIO. Every recompute outcome assigns
// readErr: a success clears it, so a user fixing a corrupt file stops the health
// noise on the next read.
func (v *settingsView) merged() ([]byte, int) {
	key, cacheable := v.statKey()
	if cacheable {
		v.mu.Lock()
		if v.cacheOK && v.cacheKey == key {
			buf := v.cacheBuf
			v.readErr = nil
			v.mu.Unlock()
			return buf, 0
		}
		v.mu.Unlock()
	}
	// A non-cacheable key (base unstattable) can't be coalesced — recompute
	// directly. Otherwise single-flight on the key so the concurrent post-commit
	// miss herd shares one recompute (see mergeSF).
	if !cacheable {
		return v.recompute(key, false)
	}
	r, _, _ := v.mergeSF.Do(key.sfKey(), func() (any, error) {
		buf, st := v.recompute(key, true)
		return mergeResult{buf: buf, st: st}, nil
	})
	res := r.(mergeResult)
	return res.buf, res.st
}

// recompute is merged()'s cache-miss body: read the base file, inject
// plansDirectory, and (when cacheable) store the result. Split out so mergeSF
// can wrap it. The read-error fallback contract is merged()'s: an absent base is
// -ENOENT; an unparseable base records readErr and falls back to the raw base
// bytes so the session never sees EIO; a success clears readErr.
func (v *settingsView) recompute(key settingsKey, cacheable bool) ([]byte, int) {
	base, err := os.ReadFile(v.basePath)
	if err != nil {
		return nil, errno(err)
	}
	served, err := injectPlansDirectory(base, v.plansDir)
	if err != nil {
		v.setReadErr(fmt.Errorf("served view of %s: %w", v.basePath, err))
		return base, 0
	}
	v.mu.Lock()
	v.readErr = nil
	if cacheable {
		v.cacheKey, v.cacheBuf, v.cacheOK = key, served, true
	}
	v.mu.Unlock()
	return served, 0
}

// setReadErr stores a served-read failure for healthErr. Write-through failures
// live in the separate writeErr slot, so a read outcome can never mask (or be
// masked by) a write-through outcome.
func (v *settingsView) setReadErr(err error) {
	v.mu.Lock()
	v.readErr = err
	v.mu.Unlock()
}

// openSnapshot materializes a served snapshot for one read handle. Per-handle
// snapshots are load-bearing: NFS reads arrive chunked, and re-injecting per
// read could tear the JSON mid-handle if base changed between chunks.
func (v *settingsView) openSnapshot() (int, uint64) {
	buf, st := v.merged()
	if st != 0 {
		return st, ^uint64(0)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	fh := v.nextFh
	v.nextFh++
	v.snapshots[fh] = buf
	return 0, fh
}

// snapshot returns the snapshot behind a synthetic handle.
func (v *settingsView) snapshot(fh uint64) ([]byte, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	buf, ok := v.snapshots[fh]
	return buf, ok
}

// closeSnapshot drops a synthetic handle.
func (v *settingsView) closeSnapshot(fh uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.snapshots, fh)
}

// markDirty records that a real /settings.json fd mutated the base file (Write
// or fd Truncate), so its Release must run the base write-through. A
// write-capable fd that never writes stays clean — its Release must not push a
// strip over a base another writer just changed.
func (v *settingsView) markDirty(fh uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.dirtyFds[fh] = struct{}{}
}

// takeDirty reports whether fh was marked dirty and clears the flag — kernel fd
// numbers are reused, so the flag must not outlive the Release.
func (v *settingsView) takeDirty(fh uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, dirty := v.dirtyFds[fh]
	delete(v.dirtyFds, fh)
	return dirty
}

// getattrSnapshot answers Getattr for a synthetic handle: the base file's mode
// and ownership with the snapshot's size — Getattr.Size must equal what Read
// returns on this handle or the NFS client serves truncated reads.
func (v *settingsView) getattrSnapshot(fh uint64, stat *fuse.Stat_t) int {
	buf, ok := v.snapshot(fh)
	if !ok {
		return -int(syscall.EBADF)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(v.basePath, &st); err != nil {
		return errno(err)
	}
	copyStat(stat, &st)
	stat.Size = int64(len(buf))
	return 0
}

// readSnapshot serves a chunked read from a synthetic handle's snapshot; reads
// at or past the end are EOF.
func (v *settingsView) readSnapshot(fh uint64, buff []byte, ofst int64) int {
	buf, ok := v.snapshot(fh)
	if !ok {
		return -int(syscall.EBADF)
	}
	if ofst < 0 {
		return -int(syscall.EINVAL)
	}
	if ofst >= int64(len(buf)) {
		return 0
	}
	return copy(buff, buf[ofst:])
}

// overrideMergedAttr rewrites a path-based Getattr of /settings.json (stat
// already holds the base file's attributes) to the served view: Size becomes the
// served length. The served Mtim is base's own (already in stat from the Lstat),
// so unlike the two-file merge there is no second file to max against — the only
// driver of served changes is the base file, whose mtime stat already carries.
// Size must track the served bytes or the NFS client keeps serving stale data
// pages — noattrcache disables attribute caching, not data caching.
func (v *settingsView) overrideMergedAttr(stat *fuse.Stat_t) int {
	buf, st := v.merged()
	if st != 0 {
		return st
	}
	stat.Size = int64(len(buf))
	return 0
}

// scheduleWriteThrough requests a base write-through and returns IMMEDIATELY —
// it never runs the read→strip→write itself. That I/O is the work that would
// otherwise run inline in the fuse Release/Rename handler under the
// process-global writeThroughMu; holding a fuse-t worker there serializes every
// account's commit handler and starves the mount's NFS server ("nfs server …
// not responding"). Here a handler only flips a flag and (at most) spawns the
// worker, so it returns at fuse-op speed. A commit arriving while a cycle runs
// is coalesced into one more cycle; each cycle re-reads base, so the latest
// committed bytes always win.
func (v *settingsView) scheduleWriteThrough() {
	v.mu.Lock()
	if v.wtRunning {
		v.wtPending = true
		v.mu.Unlock()
		return
	}
	v.wtRunning = true
	v.mu.Unlock()
	go v.writeThroughLoop()
}

// writeThroughLoop drains write-through requests until none remain, then exits
// (no idle goroutine lingers per mount). Each cycle takes writeThroughMu for
// cross-account base atomicity OFF the fuse handler path. The cycle must never
// fail a fuse op: failures land in writeErr and surface through
// FuseProvider.Health (the daemon logs them every poll; ccp doctor shows them),
// cleared only by the next successful write-through. A missing base file skips
// the write-through entirely — the same policy as .claude.json: cc-pool must not
// mint base settings.json.
func (v *settingsView) writeThroughLoop() {
	for {
		writeThroughMu.Lock()
		err := v.writeThroughBase()
		writeThroughMu.Unlock()

		v.mu.Lock()
		v.writeErr = err
		if v.wtPending {
			v.wtPending = false
			v.mu.Unlock()
			continue
		}
		v.wtRunning = false
		if v.wtIdle != nil {
			close(v.wtIdle)
			v.wtIdle = nil
		}
		v.mu.Unlock()
		return
	}
}

// flushWithin waits up to d for any in-flight write-through to drain, so a
// teardown (or a test) sees the last committed strip reach base before the
// mirror goes away. Returns true once the worker is idle. The base file is a real
// local file (never a fuse mount), so a cycle cannot hang on a wedged mirror; the
// bound only guards a genuinely stuck local write. d <= 0 waits indefinitely.
func (v *settingsView) flushWithin(d time.Duration) bool {
	v.mu.Lock()
	if !v.wtRunning {
		v.mu.Unlock()
		return true
	}
	if v.wtIdle == nil {
		v.wtIdle = make(chan struct{})
	}
	ch := v.wtIdle
	v.mu.Unlock()

	if d <= 0 {
		<-ch
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// writeThroughBase runs one base read→strip→write cycle. Caller holds
// writeThroughMu. The strip removes plansDirectory only when its value equals
// the value we inject (stripInjectedPlansDirectory), so a user-set value of any
// other path survives untouched.
func (v *settingsView) writeThroughBase() error {
	base, err := os.ReadFile(v.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("write-through to %s: read base: %w", v.basePath, err)
	}
	newBase, err := stripInjectedPlansDirectory(base, v.plansDir)
	if err != nil {
		return fmt.Errorf("write-through to %s: %w", v.basePath, err)
	}
	if bytes.Equal(newBase, base) {
		// Nothing to strip (no injected key, or a user value we leave alone):
		// skip the rewrite. Writing identical bytes would still bump base's mtime
		// — invalidating every mount's served cache — for nothing. The skip IS a
		// successful cycle, so returning nil (clearing writeErr) is deliberate.
		return nil
	}
	if err := WriteAtomic0600(v.basePath, newBase); err != nil {
		return fmt.Errorf("write-through to %s: %w", v.basePath, err)
	}
	return nil
}
