package ptyrelay

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/creack/pty"
	"github.com/muesli/cancelreader"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

const (
	defaultRows uint16 = 24
	defaultCols uint16 = 80
	pumpBuf            = 32 * 1024
)

// ErrNotStarted is returned by Signal and Kill before Start (or after a failed
// Start) — there is no process to touch.
var ErrNotStarted = errors.New("ptyrelay: process not started")

// Options configures a Relay.
type Options struct {
	// Stdin is the real terminal input, put into raw mode iff it is a terminal.
	Stdin *os.File
	// Stdout is the mirror target; its size seeds the pty iff it is a terminal.
	Stdout *os.File
	// OnURL is called once, on its own goroutine, with the first URL the child
	// prints; a non-empty return is echoed to Stdout after teardown. Wait joins
	// the call, so OnURL must be internally bounded.
	OnURL func(url string) string
}

// Relay runs a child process on a PTY, mirroring its output to the real
// terminal while observing the byte stream. Its lifecycle mirrors exec.Cmd:
// Start, then exactly one Wait; Signal and Kill may be called concurrently with
// Wait. There is no ctx — the caller owns cancellation via Signal/Kill.
type Relay struct {
	cmd     *exec.Cmd
	opts    Options
	master  *os.File
	scanner *Scanner

	rawStdin  bool
	prevState *term.State

	input     cancelreader.CancelReader
	inputDone chan struct{}
	pumpDone  chan struct{}

	winchCh   chan os.Signal
	winchStop chan struct{}
	winchDone chan struct{}

	// Output-goroutine-only state, read by Wait only after joining the pump.
	urlSeen bool
	noteCh  chan string // nil until the first URL; carries OnURL's note
}

// New returns a Relay that will run c on a PTY. c must not have its Stdin,
// Stdout, or Stderr set — the PTY slave becomes all three.
func New(c *exec.Cmd, opts Options) *Relay {
	return &Relay{cmd: c, opts: opts, scanner: NewScanner()}
}

// Start opens the PTY, spawns the child on it, raws Stdin if it is a terminal,
// and launches the output/input pumps and the SIGWINCH watcher.
func (r *Relay) Start() error {
	m, err := pty.StartWithSize(r.cmd, r.winsize())
	if err != nil {
		return fmt.Errorf("start pty child: %w", err)
	}
	r.master = m

	if term.IsTerminal(int(r.opts.Stdin.Fd())) {
		st, err := term.MakeRaw(int(r.opts.Stdin.Fd()))
		if err != nil {
			r.abortStart()
			return fmt.Errorf("set raw mode: %w", err)
		}
		r.rawStdin = true
		r.prevState = st
		// ^C must interrupt ccp (which then closes the child), not vanish into raw input.
		if err := enableIntr(int(r.opts.Stdin.Fd())); err != nil {
			r.restoreRaw()
			r.abortStart()
			return fmt.Errorf("re-enable INTR: %w", err)
		}
	}

	cr, err := cancelreader.NewReader(r.opts.Stdin)
	if err != nil {
		r.restoreRaw()
		r.abortStart()
		return fmt.Errorf("wrap stdin: %w", err)
	}
	r.input = cr

	r.pumpDone = make(chan struct{})
	r.inputDone = make(chan struct{})
	go r.outputPump()
	go r.inputPump()

	if term.IsTerminal(int(r.opts.Stdout.Fd())) {
		r.startWinch()
	}
	return nil
}

// Wait reaps the child, joins the pumps, restores the terminal, and returns the
// child's exit error verbatim (so callers' errors.As on *exec.ExitError works).
func (r *Relay) Wait() error {
	werr := r.cmd.Wait()

	// The darwin pty master fd is not poller-registered: no deadline or Close
	// unblocks the output pump, so its defined exit is master EOF after reap.
	// Join it before reading scanner state.
	r.input.Cancel()
	<-r.pumpDone
	<-r.inputDone

	r.stopWinch()
	r.restoreRaw()

	// Cleanup writes are best-effort, like the rest of the CLI's escape output.
	// A kill mid-sequence leaves the real terminal mid-parse: abort that first.
	if seq := r.scanner.AbortSeq(); seq != "" {
		_, _ = r.opts.Stdout.WriteString(seq)
	}
	if !r.scanner.LineFresh() {
		_, _ = r.opts.Stdout.WriteString("\r\n")
	}
	if seq := r.scanner.ResetSeq(); seq != "" {
		_, _ = r.opts.Stdout.WriteString(seq)
	}
	if r.noteCh != nil {
		if note := <-r.noteCh; note != "" {
			_, _ = r.opts.Stdout.WriteString(note + "\r\n")
		}
	}

	_ = r.input.Close()
	_ = r.master.Close()
	return werr
}

// Signal forwards sig to the child.
func (r *Relay) Signal(sig os.Signal) error {
	if r.cmd.Process == nil {
		return ErrNotStarted
	}
	return r.cmd.Process.Signal(sig)
}

// Kill force-terminates the child.
func (r *Relay) Kill() error {
	if r.cmd.Process == nil {
		return ErrNotStarted
	}
	return r.cmd.Process.Kill()
}

func (r *Relay) winsize() *pty.Winsize {
	if term.IsTerminal(int(r.opts.Stdout.Fd())) {
		if ws, err := pty.GetsizeFull(r.opts.Stdout); err == nil {
			return ws
		}
	}
	return &pty.Winsize{Rows: defaultRows, Cols: defaultCols}
}

// abortStart tears down a child that started before a later setup step failed.
func (r *Relay) abortStart() {
	_ = r.master.Close()
	_ = r.cmd.Process.Kill()
	_, _ = r.cmd.Process.Wait()
}

func (r *Relay) outputPump() {
	defer close(r.pumpDone)
	buf := make([]byte, pumpBuf)
	for {
		n, err := r.master.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			_, _ = r.scanner.Write(chunk)
			_, _ = r.opts.Stdout.Write(chunk)
			r.maybeCallURL()
		}
		if err != nil {
			break
		}
	}
	r.scanner.Flush()
	r.maybeCallURL()
}

func (r *Relay) inputPump() {
	defer close(r.inputDone)
	buf := make([]byte, pumpBuf)
	for {
		n, err := r.input.Read(buf)
		if n > 0 {
			if _, werr := r.master.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// maybeCallURL hands the first URL to OnURL on its own goroutine — a slow
// clipboard must not stall the output pump. Wait joins via noteCh.
func (r *Relay) maybeCallURL() {
	if r.opts.OnURL == nil || r.urlSeen {
		return
	}
	u := r.scanner.URL()
	if u == "" {
		return
	}
	r.urlSeen = true
	ch := make(chan string, 1)
	r.noteCh = ch
	go func() { ch <- r.opts.OnURL(u) }()
}

// enableIntr turns ISIG back on after MakeRaw but disables every special
// character except INTR (0xff = _POSIX_VDISABLE).
func enableIntr(fd int) error {
	t, err := unix.IoctlGetTermios(fd, unix.TIOCGETA)
	if err != nil {
		return err
	}
	t.Lflag |= unix.ISIG
	t.Cc[unix.VQUIT] = 0xff
	t.Cc[unix.VSUSP] = 0xff
	t.Cc[unix.VDSUSP] = 0xff
	return unix.IoctlSetTermios(fd, unix.TIOCSETA, t)
}

func (r *Relay) startWinch() {
	r.winchCh = make(chan os.Signal, 1)
	r.winchStop = make(chan struct{})
	r.winchDone = make(chan struct{})
	signal.Notify(r.winchCh, syscall.SIGWINCH)
	go func() {
		defer close(r.winchDone)
		for {
			select {
			case <-r.winchStop:
				return
			case <-r.winchCh:
				_ = pty.InheritSize(r.opts.Stdout, r.master)
			}
		}
	}()
}

func (r *Relay) stopWinch() {
	if r.winchStop == nil {
		return
	}
	signal.Stop(r.winchCh)
	close(r.winchStop)
	<-r.winchDone
}

func (r *Relay) restoreRaw() {
	if r.rawStdin && r.prevState != nil {
		_ = term.Restore(int(r.opts.Stdin.Fd()), r.prevState)
	}
}
