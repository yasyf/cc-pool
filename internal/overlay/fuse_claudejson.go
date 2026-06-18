//go:build fuse && cgo && darwin

package overlay

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sync/singleflight"
)

// claudeJSONFusePath is the fuse path of claude's primary state file — the one
// name in the mirror with a merged read view and base write-through.
const claudeJSONFusePath = "/.claude.json"

// syntheticFhBase is the first handle ID handed out for synthetic merged-view
// read handles. Real handles are raw kernel fds (small ints), so IDs from
// 1<<62 cannot collide with them; ^uint64(0) means "no handle" and is excluded
// by syntheticFh.
const syntheticFhBase = uint64(1) << 62

// writeThroughMu serializes every base ~/.claude.json read→split→write cycle
// across ALL mounts: each account's mirror writes through to the same base
// file, and the read-modify-write must be atomic against other in-process
// write-throughs. It is held across the cycle's I/O (a documented exception to
// the lock-scope rule), but ONLY inside the background write-through worker
// (writeThroughLoop) — never on a fuse handler goroutine. A fuse Release/Rename
// schedules the cycle and returns at once, so a contended lock or a slow base
// rewrite can never park a fuse-t worker thread and stall the mount's NFS
// server (the "not responding" wedge). All mounts live in the single holder
// process, so this lock is the complete story. Against a concurrently-running
// vanilla claude we accept last-writer-wins within the window — blacklisted
// keys are structurally protected because base is re-read each cycle and
// blacklisted keys are never copied.
var writeThroughMu sync.Mutex

// claudeJSONView serves /.claude.json as a live merged document: read opens
// get base's shareable keys (everything outside ClaudeJSONPrivateKeys)
// overlaid on the account's committed private file, and committed writes split
// the shareable keys back through to base. The private file keeps the FULL
// committed payload — load-bearing for ccp migrate, which relocates it
// verbatim on every conversion/rollback/heal and re-reads identity through it;
// do not "optimize" shareable keys out of it.
type claudeJSONView struct {
	privatePath string // committed per-account file under the private backing root
	basePath    string // shared ~/.claude.json, sibling of the mirrored root

	// mergeSF collapses concurrent cache-miss recomputes of the merged view to a
	// single merge. Under ~20 concurrent sessions a base write-through bumps every
	// mount's cache key at once, so without it each concurrent getattr/open would
	// independently re-run the MB-scale ReadFile+Unmarshal+merge (a thundering
	// herd). Keyed on the mergeKey, so misses for the same (priv,base) snapshot
	// share one result. Has its own internal locking — never held under mu.
	mergeSF singleflight.Group

	mu        sync.Mutex          // guards every field below — the one synchronization story for this view
	nextFh    uint64              // next synthetic handle ID
	snapshots map[uint64][]byte   // synthetic fh → per-handle merged snapshot
	dirtyFds  map[uint64]struct{} // real /.claude.json fds that wrote or truncated; Release write-throughs only these
	cacheKey  mergeKey
	cacheBuf  []byte
	cacheOK   bool
	readErr   error // last merged-read failure; cleared by the next successful merge
	writeErr  error // last write-through failure; cleared only by a successful write-through

	// Write-through worker coordination (guarded by mu). The base
	// read→split→write runs OFF the fuse handler path: Release/Rename call
	// scheduleWriteThrough and return immediately, so a contended writeThroughMu
	// or a slow base rewrite can never park a fuse-t worker thread and stall the
	// mount's NFS server (the "not responding" wedge). wtRunning marks a cycle
	// goroutine in flight; wtPending coalesces a commit arriving mid-cycle into
	// exactly one more cycle (which re-reads the private file, so the latest
	// committed state always wins); wtIdle, when non-nil, is closed when the
	// worker goes idle, so flushWithin can wait the last cycle out.
	wtRunning bool
	wtPending bool
	wtIdle    chan struct{}
}

// mergeKey identifies one (private, base) input pair for the merge cache.
// Caching the merge keyed on both files' (mtime, size) is load-bearing, not an
// optimization: noattrcache — the only coherence lever fuse-t offers — makes
// the kernel getattr constantly, and the file can be MBs.
type mergeKey struct {
	privMtimeNS, privSize int64
	baseMtimeNS, baseSize int64
}

// sfKey renders the merge key as a singleflight key so concurrent recomputes for
// the same (priv,base) snapshot coalesce.
func (k mergeKey) sfKey() string {
	return fmt.Sprintf("%d:%d:%d:%d", k.privMtimeNS, k.privSize, k.baseMtimeNS, k.baseSize)
}

func newClaudeJSONView(privatePath, basePath string) *claudeJSONView {
	return &claudeJSONView{
		privatePath: privatePath,
		basePath:    basePath,
		nextFh:      syntheticFhBase,
		snapshots:   map[uint64][]byte{},
		dirtyFds:    map[uint64]struct{}{},
	}
}

// syntheticFh reports whether fh is a synthetic merged-view read handle (as
// opposed to a raw kernel fd or ^uint64(0), "no handle").
func syntheticFh(fh uint64) bool {
	return fh >= syntheticFhBase && fh != ^uint64(0)
}

// statKey builds the merge-cache key. ok is false when the private file cannot
// be statted — nothing cacheable, the read path is about to fail or race. An
// absent base is encoded as -1s, distinct from any real stat.
func (v *claudeJSONView) statKey() (mergeKey, bool) {
	pfi, err := os.Lstat(v.privatePath)
	if err != nil {
		return mergeKey{}, false
	}
	k := mergeKey{
		privMtimeNS: pfi.ModTime().UnixNano(), privSize: pfi.Size(),
		baseMtimeNS: -1, baseSize: -1,
	}
	if bfi, err := os.Lstat(v.basePath); err == nil {
		k.baseMtimeNS, k.baseSize = bfi.ModTime().UnixNano(), bfi.Size()
	}
	return k, true
}

// merged materializes the current merged /.claude.json: base's shareable keys
// overlaid on the committed private file. A missing private file is plain
// -ENOENT — onboarding semantics are preserved, and a view is never fabricated
// from base alone. An unparseable private or base falls back to the raw
// private bytes and records the read error for healthErr (claude's own
// backups/ recovery must still be able to read its state file); the session
// never sees EIO. Every merge outcome assigns readErr: a success clears it, so
// a user fixing a corrupt file stops the health noise on the next read.
func (v *claudeJSONView) merged() ([]byte, int) {
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
	// A non-cacheable key (private file unstattable) can't be coalesced — recompute
	// directly. Otherwise single-flight on the key so the concurrent post-commit
	// miss herd shares one merge of the MB-scale file (see mergeSF).
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

// mergeResult carries recompute's (bytes, errno) result through singleflight.
type mergeResult struct {
	buf []byte
	st  int
}

// recompute is merged()'s cache-miss body: read the private and base files,
// merge them, and (when cacheable) store the result. Split out so mergeSF can
// wrap it. The read-error fallback contract is merged()'s: a missing private
// file is -ENOENT; an unparseable private/base records readErr and falls back to
// the raw private bytes so the session never sees EIO; a success clears readErr.
func (v *claudeJSONView) recompute(key mergeKey, cacheable bool) ([]byte, int) {
	priv, err := os.ReadFile(v.privatePath)
	if err != nil {
		return nil, errno(err)
	}
	base, err := os.ReadFile(v.basePath)
	if err != nil {
		if !os.IsNotExist(err) {
			v.setReadErr(fmt.Errorf("merged view of %s: read base: %w", v.privatePath, err))
			return priv, 0
		}
		base = nil
	}
	merged, _, err := MergeClaudeJSON(priv, base)
	if err != nil {
		v.setReadErr(fmt.Errorf("merged view of %s: %w", v.privatePath, err))
		return priv, 0
	}
	v.mu.Lock()
	v.readErr = nil
	if cacheable {
		v.cacheKey, v.cacheBuf, v.cacheOK = key, merged, true
	}
	v.mu.Unlock()
	return merged, 0
}

// setReadErr stores a merged-read failure for healthErr. Write-through
// failures live in the separate writeErr slot, so a read outcome can never
// mask (or be masked by) a write-through outcome.
func (v *claudeJSONView) setReadErr(err error) {
	v.mu.Lock()
	v.readErr = err
	v.mu.Unlock()
}

// openSnapshot materializes a merged snapshot for one read handle. Per-handle
// snapshots are load-bearing: NFS reads arrive chunked, and re-merging per
// read could tear the JSON mid-handle if private or base changed between
// chunks.
func (v *claudeJSONView) openSnapshot() (int, uint64) {
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
func (v *claudeJSONView) snapshot(fh uint64) ([]byte, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	buf, ok := v.snapshots[fh]
	return buf, ok
}

// closeSnapshot drops a synthetic handle.
func (v *claudeJSONView) closeSnapshot(fh uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.snapshots, fh)
}

// markDirty records that a real /.claude.json fd mutated the private file
// (Write or fd Truncate), so its Release must run the base write-through. A
// write-capable fd that never writes stays clean — its Release must not push
// possibly-stale private shareable keys over a newer base.
func (v *claudeJSONView) markDirty(fh uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.dirtyFds[fh] = struct{}{}
}

// takeDirty reports whether fh was marked dirty and clears the flag — kernel
// fd numbers are reused, so the flag must not outlive the Release.
func (v *claudeJSONView) takeDirty(fh uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, dirty := v.dirtyFds[fh]
	delete(v.dirtyFds, fh)
	return dirty
}

// getattrSnapshot answers Getattr for a synthetic handle: the private file's
// mode and ownership with the snapshot's size — Getattr.Size must equal what
// Read returns on this handle or the NFS client serves truncated reads.
func (v *claudeJSONView) getattrSnapshot(fh uint64, stat *fuse.Stat_t) int {
	buf, ok := v.snapshot(fh)
	if !ok {
		return -int(syscall.EBADF)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(v.privatePath, &st); err != nil {
		return errno(err)
	}
	copyStat(stat, &st)
	stat.Size = int64(len(buf))
	return 0
}

// readSnapshot serves a chunked read from a synthetic handle's snapshot;
// reads at or past the end are EOF.
func (v *claudeJSONView) readSnapshot(fh uint64, buff []byte, ofst int64) int {
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

// overrideMergedAttr rewrites a path-based Getattr of /.claude.json (stat
// already holds the private file's attributes) to the merged view: Size
// becomes the merged length and Mtim the max of private and base. Base-driven
// changes must bump size/mtime or the NFS client keeps serving stale data
// pages — noattrcache disables attribute caching, not data caching.
func (v *claudeJSONView) overrideMergedAttr(stat *fuse.Stat_t) int {
	buf, st := v.merged()
	if st != 0 {
		return st
	}
	stat.Size = int64(len(buf))
	var bst syscall.Stat_t
	if err := syscall.Lstat(v.basePath, &bst); err == nil {
		base := fuse.Timespec{Sec: bst.Mtimespec.Sec, Nsec: bst.Mtimespec.Nsec}
		if base.Sec > stat.Mtim.Sec || (base.Sec == stat.Mtim.Sec && base.Nsec > stat.Mtim.Nsec) {
			stat.Mtim = base
		}
	}
	return 0
}

// scheduleWriteThrough requests a base write-through and returns IMMEDIATELY —
// it never runs the read→split→write itself. That I/O is the work that used to
// run inline in the fuse Release/Rename handler under the process-global
// writeThroughMu; holding a fuse-t worker there serialized every account's
// commit handler and starved the mount's NFS server ("nfs server … not
// responding"). Here a handler only flips a flag and (at most) spawns the
// worker, so it returns at fuse-op speed. A commit arriving while a cycle runs
// is coalesced into one more cycle; each cycle re-reads the private file, so
// the latest committed shareable keys always win.
func (v *claudeJSONView) scheduleWriteThrough() {
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
// the write-through entirely — deliberate policy: cc-pool must not pre-empt
// vanilla claude's own onboarding by minting ~/.claude.json.
func (v *claudeJSONView) writeThroughLoop() {
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
// teardown (or a test) sees the last committed shareable keys reach base before
// the mirror goes away. Returns true once the worker is idle. The base and
// private files are real local files (never a fuse mount), so a cycle cannot
// hang on a wedged mirror; the bound only guards a genuinely stuck local write.
// d <= 0 waits indefinitely.
func (v *claudeJSONView) flushWithin(d time.Duration) bool {
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

// writeThroughBase runs one base read→split→write cycle. Caller holds
// writeThroughMu.
func (v *claudeJSONView) writeThroughBase() error {
	base, err := os.ReadFile(v.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("write-through to %s: read base: %w", v.basePath, err)
	}
	payload, err := os.ReadFile(v.privatePath)
	if err != nil {
		return fmt.Errorf("write-through to %s: read committed private file: %w", v.basePath, err)
	}
	newBase, err := SplitClaudeJSON(payload, base)
	if err != nil {
		return fmt.Errorf("write-through to %s: %w", v.basePath, err)
	}
	if bytes.Equal(newBase, base) {
		// No shareable key changed: skip the rewrite. Writing identical bytes
		// would still bump base's mtime — invalidating every mount's merge
		// cache — and widen the vanilla-claude last-writer window for nothing.
		// The skip IS a successful cycle, so returning nil (clearing writeErr)
		// is deliberate.
		return nil
	}
	if err := WriteAtomic0600(v.basePath, newBase); err != nil {
		return fmt.Errorf("write-through to %s: %w", v.basePath, err)
	}
	return nil
}

// healthErr joins both views' independent failure domains for
// FuseProvider.Health: each view's last merged/served-read failure (cleared by
// the next successful recompute) and last base write-through failure (cleared
// only by a successful write-through). The daemon logs the joined error every
// poll and ccp doctor shows it. The two views' locks are taken in turn (never
// nested) — there is no lock-ordering hazard.
func (fs *mirrorFS) healthErr() error {
	fs.cj.mu.Lock()
	cjRead, cjWrite := fs.cj.readErr, fs.cj.writeErr
	fs.cj.mu.Unlock()
	fs.settings.mu.Lock()
	setRead, setWrite := fs.settings.readErr, fs.settings.writeErr
	fs.settings.mu.Unlock()
	return errors.Join(cjRead, cjWrite, setRead, setWrite)
}
