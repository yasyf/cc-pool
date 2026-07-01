package overlay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// swapDeepProbeTimeout overrides deepProbeTimeout for one test; callers must not
// run in parallel (it mutates a package global).
func swapDeepProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := deepProbeTimeout
	deepProbeTimeout = d
	t.Cleanup(func() { deepProbeTimeout = prev })
}

func TestProbePatternDeterministic(t *testing.T) {
	const nonce = uint64(0xDEADBEEFCAFEF00D)
	const otherNonce = nonce + 1
	ref := make([]byte, 64<<10)
	FillProbe(nonce, 0, ref)

	t.Run("header is the big-endian nonce", func(t *testing.T) {
		if got := binary.BigEndian.Uint64(ref[:8]); got != nonce {
			t.Fatalf("header = %#x, want the nonce %#x", got, nonce)
		}
	})

	t.Run("same nonce and offset reproduce the same bytes", func(t *testing.T) {
		again := make([]byte, len(ref))
		FillProbe(nonce, 0, again)
		if !bytes.Equal(ref, again) {
			t.Fatal("two fills with the same nonce and offset diverged")
		}
	})

	t.Run("different nonce diverges past the header", func(t *testing.T) {
		other := make([]byte, 4096)
		FillProbe(otherNonce, 8, other)
		if bytes.Equal(ref[8:8+4096], other) {
			t.Fatal("body bytes identical across different nonces — the page-cache defense is void")
		}
		otherHeader := make([]byte, 8)
		FillProbe(otherNonce, 0, otherHeader)
		if bytes.Equal(ref[:8], otherHeader) {
			t.Fatal("headers identical across different nonces")
		}
	})

	t.Run("sliced fills reassemble the contiguous pattern", func(t *testing.T) {
		cases := []struct {
			name string
			ofst int64
			n    int
		}{
			{"single byte at zero", 0, 1},
			{"odd offset inside header", 3, 4},
			{"odd slice spanning the header boundary", 5, 9},
			{"single byte at the header boundary", 8, 1},
			{"odd offset odd length body", 8191, 513},
			{"unaligned tail chunk", int64(len(ref)) - 7, 7},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				chunk := make([]byte, tc.n)
				FillProbe(nonce, tc.ofst, chunk)
				if want := ref[tc.ofst : tc.ofst+int64(tc.n)]; !bytes.Equal(chunk, want) {
					t.Fatalf("FillProbe(%d, %d bytes) = %x, want the contiguous slice %x", tc.ofst, tc.n, chunk, want)
				}
			})
		}
	})
}

func TestDeepProbeWithinMissingFile(t *testing.T) {
	err := DeepProbeWithin(t.TempDir())
	if err == nil {
		t.Fatal("DeepProbeWithin = nil for a dir without the probe file, want ErrProbeMissing")
	}
	if !errors.Is(err, ErrProbeMissing) {
		t.Errorf("errors.Is(err, ErrProbeMissing) = false; err = %v", err)
	}
	if errors.Is(err, ErrProbeWedged) {
		t.Errorf("a missing probe must be \"no verdict\", never a wedge; err = %v", err)
	}
}

// TestDeepProbeWithinPermissionDeniedIsMissing pins that a permission-class open
// refusal (EPERM/EACCES → os.ErrPermission) reads as ErrProbeMissing, never a
// wedge: an old holder can refuse the daemon's external open, and a false wedge
// would make the daemon refuse every account across an upgrade.
func TestDeepProbeWithinPermissionDeniedIsMissing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits, so open(2) would not return EACCES")
	}
	dir := t.TempDir()
	// mode-0000 makes os.Open refuse with EACCES, an old holder's refusal shape.
	if err := os.WriteFile(filepath.Join(dir, ProbeFileName), []byte("x"), 0o000); err != nil {
		t.Fatal(err)
	}
	err := DeepProbeWithin(dir)
	if !errors.Is(err, ErrProbeMissing) {
		t.Errorf("errors.Is(err, ErrProbeMissing) = false for a permission-denied open; err = %v", err)
	}
	if errors.Is(err, ErrProbeWedged) {
		t.Errorf("a permission-denied open must be \"no verdict\", never a wedge; err = %v", err)
	}
}

func TestDeepProbeWithinFullRead(t *testing.T) {
	dir := t.TempDir()
	buf := make([]byte, ProbeFileSize)
	FillProbe(42, 0, buf)
	if err := os.WriteFile(filepath.Join(dir, ProbeFileName), buf, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := DeepProbeWithin(dir); err != nil {
		t.Fatalf("DeepProbeWithin = %v, want nil for a full %d-byte probe file", err, ProbeFileSize)
	}
}

// TestDeepProbeWithinReplayedNonceWedged: a second full read observing the
// SAME header nonce is a cache replay — the mirror mints a fresh nonce per
// open — and must read as wedged, not healthy. A fresh nonce recovers.
func TestDeepProbeWithinReplayedNonceWedged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ProbeFileName)
	buf := make([]byte, ProbeFileSize)
	FillProbe(7, 0, buf)
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := DeepProbeWithin(dir); err != nil {
		t.Fatalf("first probe = %v, want nil", err)
	}

	err := DeepProbeWithin(dir)
	if err == nil {
		t.Fatal("second probe of an identical-nonce file = nil, want ErrProbeWedged — a cache replay must be detected, not reported healthy")
	}
	if !errors.Is(err, ErrProbeWedged) {
		t.Errorf("errors.Is(err, ErrProbeWedged) = false; err = %v", err)
	}
	if errors.Is(err, ErrProbeMissing) {
		t.Errorf("a replayed nonce must not read as missing; err = %v", err)
	}

	FillProbe(8, 0, buf)
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := DeepProbeWithin(dir); err != nil {
		t.Fatalf("fresh-nonce probe after a replay = %v, want nil", err)
	}
}

func TestDeepProbeWithinShortRead(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ProbeFileName), make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	err := DeepProbeWithin(dir)
	if err == nil {
		t.Fatal("DeepProbeWithin = nil for a truncated probe file, want ErrProbeWedged")
	}
	if !errors.Is(err, ErrProbeWedged) {
		t.Errorf("errors.Is(err, ErrProbeWedged) = false; err = %v", err)
	}
	if errors.Is(err, ErrProbeMissing) {
		t.Errorf("a short read must not read as missing; err = %v", err)
	}
}

func TestDeepProbeWithinTimeoutWedged(t *testing.T) {
	swapDeepProbeTimeout(t, 50*time.Millisecond)
	dir := t.TempDir()
	fifo := filepath.Join(dir, ProbeFileName)
	// A FIFO with no writer parks the probe's open(2) indefinitely — the same
	// shape as a wedged mirror's uninterruptible openat.
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	before := deepProbes.Inflight()
	err := DeepProbeWithin(dir)
	if err == nil {
		t.Fatal("DeepProbeWithin = nil against a parked open, want ErrProbeWedged")
	}
	if !errors.Is(err, ErrProbeWedged) {
		t.Errorf("errors.Is(err, ErrProbeWedged) = false; err = %v", err)
	}
	if errors.Is(err, ErrProbeMissing) {
		t.Errorf("a timed-out probe must not read as missing; err = %v", err)
	}
	if got := deepProbes.Inflight(); got != before+1 {
		t.Errorf("Inflight = %d, want %d (one new parked probe goroutine)", got, before+1)
	}

	// Best-effort unwedge for a tidy exit; we deliberately do NOT assert it
	// drains. A real wedge leaks the goroutine by design (DeepProbeWithin bounds
	// the caller, not the goroutine), and a macOS FIFO's blocking read can miss
	// the writer's EOF under load (~1 in 3000) and strand forever, so a drain
	// assertion is inherently flaky. The Inflight check above is delta-based, so
	// a straggler never fails this run or a later one.
	if w, werr := os.OpenFile(fifo, os.O_WRONLY, 0); werr == nil { //nolint:gosec // G304: fifo is under the test's own t.TempDir()
		go func() {
			_, _ = w.Write(make([]byte, 128<<10))
			_ = w.Close()
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for deepProbes.Inflight() > before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}
