//go:build fuse && cgo && darwin

package overlay

import (
	"crypto/rand"
	"encoding/binary"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// probeFusePath is the fuse path of the synthetic deep-probe file. Like
// claudeJSONFusePath it is intercepted by name in the mirror's ops — but
// BEFORE the real() mapping, so the file is purely virtual: a real base entry
// of the same name is shadowed, and nothing is ever created, written, or
// modified in the backing ~/.claude for probing.
const probeFusePath = "/" + ProbeFileName

// probeFhBase is the first handle ID handed out for probe read handles. The
// probe range is [1<<61, 1<<62): disjoint from real kernel fds (small ints)
// below and from the merged-view synthetic range [syntheticFhBase, ^uint64(0))
// above. Both allocators only increment, so crossing into the other range
// would take 2^61 concurrent opens — the ranges cannot collide.
const probeFhBase = uint64(1) << 61

// probeFh reports whether fh is a probe read handle.
func probeFh(fh uint64) bool {
	return fh >= probeFhBase && fh < syntheticFhBase
}

// probeView serves the virtual read-only probe file at /.ccp-probe. Every
// open mints a random nonce and serves FillProbe bytes for it, so two opens
// observe different content — a page cache replaying a previous open's data
// is caught by the 8-byte nonce header. Getattr advances the reported mtime
// on every call for the same reason: the NFS client invalidates cached data
// pages on mtime change, and a frozen mtime would let it satisfy a re-probe
// without issuing a single READ.
type probeView struct {
	mu       sync.Mutex
	nextFh   uint64            // next probe handle ID
	nonces   map[uint64]uint64 // open probe fh → its per-open nonce
	lastAttr time.Time         // last mtime reported; the next is strictly later
}

func newProbeView() *probeView {
	return &probeView{nextFh: probeFhBase, nonces: map[uint64]uint64{}}
}

// getattr answers Getattr for the probe path: a fixed-size read-only regular
// file whose mtime strictly advances on every call (see probeView).
func (v *probeView) getattr(stat *fuse.Stat_t) int {
	v.mu.Lock()
	now := time.Now()
	if !now.After(v.lastAttr) {
		now = v.lastAttr.Add(time.Nanosecond)
	}
	v.lastAttr = now
	v.mu.Unlock()
	ts := fuse.Timespec{Sec: now.Unix(), Nsec: int64(now.Nanosecond())}
	*stat = fuse.Stat_t{
		Mode:     fuse.S_IFREG | 0o444,
		Nlink:    1,
		Uid:      uint32(os.Getuid()), //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
		Gid:      uint32(os.Getgid()), //nolint:gosec // G115: FUSE/syscall ABI conversion of a kernel-supplied stat/offset field; the value is bounded by the OS
		Size:     ProbeFileSize,
		Blksize:  4096,
		Blocks:   (ProbeFileSize + 511) / 512,
		Atim:     ts,
		Mtim:     ts,
		Ctim:     ts,
		Birthtim: ts,
	}
	return 0
}

// open answers Open for the probe path: read-only opens mint a per-open
// nonce; any write access is refused — the file has no writable backing.
func (v *probeView) open(flags int) (int, uint64) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return -int(syscall.EACCES), ^uint64(0)
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return -int(syscall.EIO), ^uint64(0)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	fh := v.nextFh
	v.nextFh++
	v.nonces[fh] = binary.BigEndian.Uint64(b[:])
	return 0, fh
}

// read serves a chunked read from a probe handle: FillProbe bytes for the
// handle's nonce, clamped at ProbeFileSize; reads at or past the end are EOF.
func (v *probeView) read(fh uint64, buff []byte, ofst int64) int {
	v.mu.Lock()
	nonce, ok := v.nonces[fh]
	v.mu.Unlock()
	if !ok {
		return -int(syscall.EBADF)
	}
	if ofst < 0 {
		return -int(syscall.EINVAL)
	}
	if ofst >= ProbeFileSize {
		return 0
	}
	n := len(buff)
	if rem := int64(ProbeFileSize) - ofst; int64(n) > rem {
		n = int(rem)
	}
	FillProbe(nonce, ofst, buff[:n])
	return n
}

// release drops a probe handle's nonce.
func (v *probeView) release(fh uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.nonces, fh)
}
