package overlay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// This file holds the untagged deep-probe contract: the synthetic probe file's
// name, size, and byte pattern (served by the fuse mirror, fuse_probe.go) plus
// DeepProbeWithin, the bounded full read the mount-holder runs through the
// kernel mount to detect a partially wedged mirror. It compiles in every build
// variant so the daemon and CLI can errors.Is against probe verdicts that
// crossed a process boundary.

// ProbeFileName is the synthetic read-only file every fuse mirror serves at
// its root for deep wedge probing. It is purely virtual: it never exists in
// the backing ~/.claude, is hidden from Readdir, and rejects all writes.
const ProbeFileName = ".ccp-probe"

// ProbeFileSize is the probe file's fixed size: 2 MiB. The observed wedge
// mechanism is multi-READ-RPC readahead — a wedged fuse-t mirror served small
// stats and reads instantly while a 1.5 MB sequential read (the only
// confirmed-bad data point) hung forever — so the probe must be comfortably
// above that and span many NFS READ RPCs; small reads provably succeed on a
// wedged mirror and would report it healthy.
const ProbeFileSize = 2 << 20

var (
	// ErrProbeMissing means dir does not serve a readable probe file to an
	// external opener: the open returned ENOENT, or a permission-class refusal
	// (EPERM/EACCES). Both are the signature of a mirror mounted by a holder
	// that predates the probe-in-daemon contract — the deep probe moved from the
	// holder into the daemon, so a pre-move holder's mirror refuses the daemon's
	// external open. Callers treat it as "no verdict" — never as a wedge — so
	// old-holder mounts keep working across an upgrade until they are naturally
	// remounted.
	ErrProbeMissing = errors.New("probe file missing")

	// ErrProbeWedged means the deep probe could not pull the full probe file
	// through the kernel mount in time: the read (or its open) parked past the
	// probe timeout, or EOF arrived short of ProbeFileSize. Either way the
	// mirror is serving metadata but not bulk data — the partial-wedge
	// signature — and callers must treat the mount as dead.
	ErrProbeWedged = errors.New("deep probe wedged")
)

// deepProbeTimeout bounds one DeepProbeWithin read. A var, not a const, so
// tests can shrink it.
var deepProbeTimeout = 5 * time.Second

// deepProbes joins concurrent deep probes per dir. Deliberately its OWN
// StatProbes instance, never shared with the shallow stat probes (ownProbes
// here, liveProbes in mountd): a parked 2 MiB read against a wedged mirror
// must never block a shallow liveness stat behind its join.
var deepProbes StatProbes[error]

// DeepProbeWithin reads dir's probe file (dir/ProbeFileName) in full through
// the kernel mount, bounded by the package's deep-probe timeout. nil means
// the mirror moved ProbeFileSize bytes of FRESH bulk data: the full byte
// count arrived AND the 8-byte header nonce differs from this process's
// previous probe of the same file — the mirror mints a new nonce per open, so
// a repeat means the read was served by a cache, not the mirror, and counts
// as wedged. Sentinels: ErrProbeMissing (no verdict: pre-probe holder),
// ErrProbeWedged (timed out, short read, or replayed nonce). Concurrent
// callers for the same dir join one in-flight read; a wedged probe parks
// exactly one goroutine and one fd until the kernel finally answers (see
// StatProbes).
func DeepProbeWithin(dir string) error {
	path := filepath.Join(dir, ProbeFileName)
	err, ok := deepProbes.Do(dir, deepProbeTimeout, func() error { return readProbeFile(path) })
	if !ok {
		return fmt.Errorf("%w: read of %s did not answer within %s", ErrProbeWedged, path, deepProbeTimeout)
	}
	return err
}

// lastProbeNonce remembers, per probe-file path, the header nonce the last
// successful full read observed, making the anti-cache defense self-verifying:
// the mirror mints a fresh random nonce per open, so observing the same nonce
// twice proves the bytes came from a cache replay, not the mirror. Guarded by
// probeNonceMu; bounded by the number of probed account dirs.
var (
	probeNonceMu   sync.Mutex
	lastProbeNonce = map[string]uint64{}
)

// readProbeFile is DeepProbeWithin's unbounded body: open, defeat the page
// cache, read to EOF, verify the byte count, and verify the header nonce is
// fresh (see lastProbeNonce). It runs inside a deepProbes goroutine; on a
// wedged mirror it parks in open or read until the kernel answers.
func readProbeFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		// ENOENT and a permission-class refusal (EPERM/EACCES → os.ErrPermission)
		// both mean this mirror does not serve a readable probe to an external
		// opener — a holder predating the probe-in-daemon move. No verdict (see
		// ErrProbeMissing): never a wedge. A genuine partial wedge is the timeout
		// or short/replayed read below, never an instant open refusal.
		if os.IsNotExist(err) || errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("%w: %s", ErrProbeMissing, path)
		}
		return fmt.Errorf("deep probe open %s: %w", path, err)
	}
	defer f.Close()
	// F_NOCACHE: without it the NFS client's page cache can satisfy the whole
	// read after the first probe, proving nothing about the mirror. The
	// mirror's advancing-mtime Getattr is the second half of the same defense.
	if _, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1); err != nil {
		return fmt.Errorf("deep probe %s: set F_NOCACHE: %w", path, err)
	}
	var header [8]byte
	hn, err := io.ReadFull(f, header[:])
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("deep probe read %s: %w", path, err)
	}
	rest, err := io.Copy(io.Discard, f)
	if err != nil {
		return fmt.Errorf("deep probe read %s: %w", path, err)
	}
	if n := int64(hn) + rest; n != ProbeFileSize {
		return fmt.Errorf("%w: %s served %d bytes, want %d", ErrProbeWedged, path, n, ProbeFileSize)
	}
	nonce := binary.BigEndian.Uint64(header[:])
	probeNonceMu.Lock()
	last, seen := lastProbeNonce[path]
	lastProbeNonce[path] = nonce
	probeNonceMu.Unlock()
	if seen && last == nonce {
		return fmt.Errorf("%w: %s replayed nonce %#x — the read was served from a cache, not the mirror", ErrProbeWedged, path, nonce)
	}
	return nil
}

// FillProbe writes the probe file's deterministic pattern into buff, which
// holds the file's bytes starting at offset ofst (ofst must be >= 0; callers
// clamp at ProbeFileSize — the pattern itself is defined for every offset).
// Bytes 0-7 of the file are the big-endian nonce, so a reader can recover
// which open it is observing from the header alone; every later byte is a
// cheap mix of (nonce, offset). The per-open nonce makes consecutive probes
// byte-distinguishable: a page cache replaying a previous open's data is
// caught by the header, not just by luck.
func FillProbe(nonce uint64, ofst int64, buff []byte) {
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], nonce)
	for i := range buff {
		off := ofst + int64(i)
		if off < 8 {
			buff[i] = header[off]
			continue
		}
		buff[i] = probeByte(nonce, off)
	}
}

// probeByte is one body byte of the probe pattern: a splitmix64-style mix of
// the nonce and the byte's file offset. Cheap on purpose — the mirror
// regenerates it for every NFS READ of the 2 MiB file.
func probeByte(nonce uint64, off int64) byte {
	x := nonce + uint64(off)*0x9E3779B97F4A7C15
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	return byte(x)
}
