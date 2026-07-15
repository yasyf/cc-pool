package ptyrelay

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

// harness wires a Relay to os.Pipe pairs so IsTerminal is false (raw mode and
// the WINCH watcher are skipped) and the mirrored output is capturable.
type harness struct {
	r    *Relay
	inR  *os.File // relay stdin (child input source)
	inW  *os.File // test writes child input here
	outR *os.File // test reads mirrored output here
	outW *os.File // relay mirror target
}

func newHarness(t *testing.T, c *exec.Cmd, onURL func(string) string) *harness {
	t.Helper()
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	h := &harness{
		r:   New(c, Options{Stdin: inR, Stdout: outW, OnURL: onURL}),
		inR: inR, inW: inW, outR: outR, outW: outW,
	}
	t.Cleanup(func() {
		_ = inR.Close()
		_ = inW.Close()
		_ = outR.Close()
		_ = outW.Close()
	})
	return h
}

// run starts the relay, invokes afterStart (may write to inW or signal), waits
// for the child with a timeout, then returns the fully mirrored output.
func (h *harness) run(t *testing.T, afterStart func()) ([]byte, error) {
	t.Helper()
	outCh := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(h.outR)
		outCh <- b
	}()

	if err := h.r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if afterStart != nil {
		afterStart()
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- h.r.Wait() }()

	var werr error
	select {
	case werr = <-waitCh:
	case <-time.After(testTimeout):
		t.Fatal("Wait timed out")
	}

	_ = h.outW.Close() // EOF the reader
	select {
	case out := <-outCh:
		return out, werr
	case <-time.After(testTimeout):
		t.Fatal("output read timed out")
		return nil, werr
	}
}

func TestRelayMirrorFidelity(t *testing.T) {
	h := newHarness(t, exec.Command("/bin/echo", "hello world"), nil)
	out, err := h.run(t, nil)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !strings.Contains(string(out), "hello world") {
		t.Errorf("output = %q, want it to contain %q", out, "hello world")
	}
}

func TestRelayStdinRelay(t *testing.T) {
	// The child reads one line and echoes it back through the mirror, then exits.
	c := exec.Command("/bin/sh", "-c", `read line; printf 'got:%s\n' "$line"`)
	h := newHarness(t, c, nil)
	out, err := h.run(t, func() {
		if _, werr := h.inW.WriteString("ping\n"); werr != nil {
			t.Errorf("write stdin: %v", werr)
		}
	})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !strings.Contains(string(out), "got:ping") {
		t.Errorf("output = %q, want it to contain %q", out, "got:ping")
	}
}

func TestRelayOnURL(t *testing.T) {
	var calls atomic.Int32
	var gotURL atomic.Value
	onURL := func(u string) string {
		calls.Add(1)
		gotURL.Store(u)
		return "<<COPIED:" + u + ">>"
	}
	c := exec.Command("/bin/sh", "-c", `printf 'Visit https://example.com/auth\n'`)
	h := newHarness(t, c, onURL)
	out, err := h.run(t, nil)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("OnURL called %d times, want 1", n)
	}
	if u, _ := gotURL.Load().(string); u != "https://example.com/auth" {
		t.Errorf("OnURL url = %q, want https://example.com/auth", u)
	}
	s := string(out)
	if !strings.Contains(s, "https://example.com/auth") {
		t.Fatalf("output = %q, want the mirrored URL", s)
	}
	// The note prints once, after teardown — never inside the live stream.
	if !strings.HasSuffix(s, "<<COPIED:https://example.com/auth>>\r\n") {
		t.Errorf("output = %q, want the annotation as the trailing line", s)
	}
}

func TestRelayAbortsDanglingSequence(t *testing.T) {
	// A child that dies mid-OSC must not leave the terminal parsing everything
	// that follows as OSC payload: teardown emits ST before any other output.
	c := exec.Command("/bin/sh", "-c", `printf '\033]0;title'`)
	h := newHarness(t, c, nil)
	out, err := h.run(t, nil)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !strings.HasSuffix(string(out), "\x1b\\"+"\r\n") {
		t.Errorf("output = %q, want ST then CRLF as the teardown suffix", out)
	}
}

func TestRelayModeResetOnExit(t *testing.T) {
	c := exec.Command("/bin/sh", "-c", `printf '\033[?2004h'`)
	h := newHarness(t, c, nil)
	out, err := h.run(t, nil)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !strings.HasSuffix(string(out), "\x1b[?2004l") {
		t.Errorf("output = %q, want suffix %q", out, "\x1b[?2004l")
	}
}

func TestRelayNoResetWhenBalanced(t *testing.T) {
	c := exec.Command("/bin/sh", "-c", `printf '\033[?2004h\033[?2004l'`)
	h := newHarness(t, c, nil)
	out, err := h.run(t, nil)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if n := strings.Count(string(out), "\x1b[?2004l"); n != 1 {
		t.Errorf("output = %q, has %d copies of ?2004l, want 1 (child's only)", out, n)
	}
}

func TestRelayAppendsNewlineWhenNoTrailing(t *testing.T) {
	c := exec.Command("/bin/sh", "-c", `printf abc123`)
	h := newHarness(t, c, nil)
	out, err := h.run(t, nil)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if string(out) != "abc123\r\n" {
		t.Errorf("output = %q, want %q", out, "abc123\r\n")
	}
}

func TestRelaySignalUnblocksWait(t *testing.T) {
	h := newHarness(t, exec.Command("/bin/sleep", "30"), nil)
	_, err := h.run(t, func() {
		if serr := h.r.Signal(syscall.SIGTERM); serr != nil {
			t.Errorf("Signal: %v", serr)
		}
	})
	if err == nil {
		t.Fatal("Wait returned nil, want the child's signal exit error")
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Errorf("Wait error = %v (%T), want *exec.ExitError", err, err)
	}
}

func TestRelaySignalBeforeStart(t *testing.T) {
	r := New(exec.Command("/bin/echo"), Options{Stdin: os.Stdin, Stdout: os.Stdout})
	if err := r.Signal(syscall.SIGTERM); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Signal before Start = %v, want ErrNotStarted", err)
	}
	if err := r.Kill(); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Kill before Start = %v, want ErrNotStarted", err)
	}
}
