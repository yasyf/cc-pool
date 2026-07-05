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

	"github.com/yasyf/fusekit"
)

// Deliberately untagged: every build variant must compile this so daemon and
// CLI can errors.Is probe verdicts across process boundaries.

// ProbeFileName is the synthetic read-only probe file the holder serves at
// the mount root; it never exists in the backing ~/.claude and is hidden from
// Readdir.
const ProbeFileName = ".ccp-probe"

// ProbeFileSize is the probe file's fixed size, 2 MiB. A wedged fuse-t mirror
// answers small stats and reads instantly but hangs a multi-RPC sequential read
// (1.5 MB was the confirmed-bad case), so the probe must span many NFS READ
// RPCs to catch the wedge. WIRE CONTRACT: equals holderfs's probeSize —
// readProbeFile rejects any other byte count as wedged.
const ProbeFileSize = 2 << 20

var (
	// ErrProbeMissing means the open returned ENOENT: the mirror serves no probe
	// file, the signature of an old holder predating the probe. Callers treat it
	// as no verdict, never a wedge, so such mounts survive upgrades until
	// remounted. A permission refusal (EPERM/EACCES) is NOT this class — it is the
	// orphaned-dead-server signature and reads as ErrProbeWedged.
	ErrProbeMissing = errors.New("probe file missing")

	// ErrProbeWedged means the deep probe could not pull the full probe file
	// through the kernel mount in time — parked past the timeout, or EOF short
	// of ProbeFileSize. The mirror serves metadata but not bulk data; treat as
	// dead.
	ErrProbeWedged = errors.New("deep probe wedged")
)

// deepProbeTimeout is a var, not a const, so tests can shrink it.
var deepProbeTimeout = 5 * time.Second

// deepProbes joins concurrent deep probes per dir. Its OWN StatProbes, never
// shared with the shallow stat probes: a parked 2 MiB read must never block a
// shallow liveness stat behind its join.
var deepProbes StatProbes[error]

// DeepProbeWithin reads dir's probe file in full through the kernel mount,
// bounded by the deep-probe timeout. nil means the mirror moved ProbeFileSize
// FRESH bytes: the full count arrived AND the header nonce differs from this
// process's last probe of the file (a repeat is a cache replay, counted
// wedged). Returns ErrProbeMissing (no verdict) or ErrProbeWedged (timeout,
// short read, or replayed nonce). Concurrent callers for a dir join one
// in-flight read; a wedged probe parks one goroutine and one fd.
func DeepProbeWithin(dir string) error {
	path := filepath.Join(dir, ProbeFileName)
	err, ok := deepProbes.Do(dir, deepProbeTimeout, func() error { return readProbeFile(path) })
	if !ok {
		return fmt.Errorf("%w: read of %s did not answer within %s", ErrProbeWedged, path, deepProbeTimeout)
	}
	return err
}

// lastProbeNonce holds, per probe-file path, the nonce the last full read saw;
// a repeat proves a cache replay. Guarded by probeNonceMu.
var (
	probeNonceMu   sync.Mutex
	lastProbeNonce = map[string]uint64{}
)

// readProbeFile is DeepProbeWithin's unbounded body; on a wedged mirror it
// parks in open or read until the kernel answers.
func readProbeFile(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path is a cc-pool-managed overlay mirror file under the state dir
	if err != nil {
		// Only genuine ENOENT is "no verdict". EPERM/EACCES is a dead-holder
		// orphan answering every op with a permission error, so it reads as a
		// wedged verdict onto the remount path (the 2026-07 incident's misread).
		switch verdict := fusekit.ProbeOpenVerdict(err); {
		case errors.Is(verdict, fusekit.ErrProbeMissing):
			return fmt.Errorf("%w: %s", ErrProbeMissing, path)
		case errors.Is(verdict, fusekit.ErrProbeDenied):
			return fmt.Errorf("%w: %s: mount denied the probe (dead-holder orphan): %v", ErrProbeWedged, path, err)
		}
		return fmt.Errorf("deep probe open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	// Without this the NFS page cache can satisfy the whole read after the first
	// probe, proving nothing; no-op off Darwin (holder is macOS/fuse-t only).
	if err := disableReadCache(f); err != nil {
		return fmt.Errorf("deep probe %s: disable read cache: %w", path, err)
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

// FillProbe writes the probe's deterministic pattern into buff, holding the
// file's bytes from offset ofst (>= 0). Bytes 0-7 are the big-endian nonce;
// every later byte is a cheap mix of (nonce, offset). The per-open nonce makes
// consecutive probes byte-distinguishable so a cache replay is caught.
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

// probeByte is one body byte: a splitmix64-style mix of nonce and file offset,
// cheap because the mirror regenerates it for every NFS READ.
func probeByte(nonce uint64, off int64) byte {
	x := nonce + uint64(off)*0x9E3779B97F4A7C15 //nolint:gosec // G115: off is a non-negative file offset; the wraparound is intended for the probe hash
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	return byte(x) //nolint:gosec // G115: deliberately truncating the splitmix64 hash to one byte for the probe pattern
}
