package overlay

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// swapFPProbeTimeout overrides fpProbeTimeout for one test; callers must not run
// in parallel (it mutates a package global).
func swapFPProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := fpProbeTimeout
	fpProbeTimeout = d
	t.Cleanup(func() { fpProbeTimeout = prev })
}

func TestFPDomainProbeWithinClassifies(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, dir string)
		want  error // nil means a healthy (non-empty) read
		// forbidden verdicts a case must never be confused with.
		notWant []error
	}{
		{
			name: "non-empty served file is healthy",
			setup: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, claudeJSONName), []byte(`{"x":1}`), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: nil,
		},
		{
			name:    "absent file is missing, never wedged",
			setup:   func(t *testing.T, dir string) {}, // no .claude.json
			want:    ErrFPProbeMissing,
			notWant: []error{ErrFPProbeWedged, ErrFPProbeEmpty},
		},
		{
			name: "zero-byte served file is empty, never missing or wedged",
			setup: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, claudeJSONName), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want:    ErrFPProbeEmpty,
			notWant: []error{ErrFPProbeMissing, ErrFPProbeWedged},
		},
		{
			name: "permission-denied open is wedged, never missing",
			setup: func(t *testing.T, dir string) {
				if os.Geteuid() == 0 {
					t.Skip("root bypasses file permission bits, so open(2) would not return EACCES")
				}
				// mode-0000 makes os.Open refuse with EACCES, a data-plane wedge shape.
				if err := os.WriteFile(filepath.Join(dir, claudeJSONName), []byte("x"), 0o000); err != nil {
					t.Fatal(err)
				}
			},
			want:    ErrFPProbeWedged,
			notWant: []error{ErrFPProbeMissing},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.setup(t, dir)
			err := FPDomainProbeWithin(dir)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("FPDomainProbeWithin = %v, want nil (healthy)", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("errors.Is(err, %v) = false; err = %v", tc.want, err)
			}
			for _, bad := range tc.notWant {
				if errors.Is(err, bad) {
					t.Errorf("err must not be %v; err = %v", bad, err)
				}
			}
		})
	}
}

// TestFPDomainProbeWithinTimeoutWedged: a read that never answers is classified
// wedged, and its probe goroutine is deliberately left parked (fuse-t/FP have no
// timeout mount option), joined by later callers.
func TestFPDomainProbeWithinTimeoutWedged(t *testing.T) {
	swapFPProbeTimeout(t, 50*time.Millisecond)
	dir := t.TempDir()
	fifo := filepath.Join(dir, claudeJSONName)
	// A FIFO with no writer parks open(2) indefinitely — a wedged domain's shape.
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	before := fpProbes.Inflight()
	err := FPDomainProbeWithin(dir)
	if !errors.Is(err, ErrFPProbeWedged) {
		t.Fatalf("errors.Is(err, ErrFPProbeWedged) = false against a parked open; err = %v", err)
	}
	if errors.Is(err, ErrFPProbeMissing) || errors.Is(err, ErrFPProbeEmpty) {
		t.Errorf("a timed-out probe must not read as missing or empty; err = %v", err)
	}
	if got := fpProbes.Inflight(); got != before+1 {
		t.Errorf("Inflight = %d, want %d (one new parked probe goroutine)", got, before+1)
	}

	// Best-effort unwedge; no drain assertion — a real wedge leaks the goroutine
	// by design, and a macOS FIFO read can miss the writer's EOF under load.
	if w, werr := os.OpenFile(fifo, os.O_WRONLY, 0); werr == nil { //nolint:gosec // G304: fifo is under the test's own t.TempDir()
		go func() {
			_, _ = w.Write([]byte("x"))
			_ = w.Close()
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for fpProbes.Inflight() > before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}
