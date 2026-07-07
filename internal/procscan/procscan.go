// Package procscan discovers live `claude` sessions and the pool account each is
// bound to. It enumerates this user's processes via sysctl (kern.proc.uid) and
// reads each one's argv and environment via KERN_PROCARGS2, keeping the ones
// whose argv[0] basename is "claude" and recording their CLAUDE_CONFIG_DIR.
//
// A claude process with no CLAUDE_CONFIG_DIR is plain `claude` on ~/.claude,
// mapped to no pool account.
package procscan

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// Session is a discovered live claude process.
type Session struct {
	PID       int
	ConfigDir string // CLAUDE_CONFIG_DIR value, or "" for plain claude
	// StartedAt is the process start time, read from the kernel's KinfoProc
	// (P_starttime). Always populated: staleness checks lean on it.
	StartedAt time.Time
}

// proc is one live process: its pid and absolute start time.
type proc struct {
	pid       int
	startedAt time.Time
}

var (
	// listProcs enumerates this user's live processes and their start times. It
	// is the process-table seam, swapped for canned data in tests.
	listProcs = func(context.Context) ([]proc, error) {
		kps, err := unix.SysctlKinfoProcSlice("kern.proc.uid", os.Getuid())
		if err != nil {
			return nil, fmt.Errorf("kern.proc.uid: %w", err)
		}
		procs := make([]proc, 0, len(kps))
		for i := range kps {
			p := &kps[i].Proc
			if p.P_pid <= 0 {
				continue
			}
			st := p.P_starttime
			procs = append(procs, proc{
				pid:       int(p.P_pid),
				startedAt: time.Unix(st.Sec, int64(st.Usec)*1000),
			})
		}
		return procs, nil
	}

	// procArgs returns the raw KERN_PROCARGS2 buffer for pid. It is the per-PID
	// argv/env seam, swapped for canned data in tests.
	procArgs = func(_ context.Context, pid int) ([]byte, error) {
		return unix.SysctlRaw("kern.procargs2", pid)
	}
)

var scanTimeout = 3 * time.Second

// ccdPrefix is the environment key whose value binds a session to a pool account.
var ccdPrefix = []byte("CLAUDE_CONFIG_DIR=")

// Scan returns all live claude sessions, bounding the walk with scanTimeout: a
// wedged fuse-t mount can pin a process in uninterruptible D-state where a
// KERN_PROCARGS2 copyin from its address space never returns and no ctx frees
// the caller, so Scan runs the walk in a goroutine and returns on the deadline.
// DeadlineExceeded surfaces as an error callers treat as "no sessions discovered".
func Scan(ctx context.Context) ([]Session, error) {
	// Capture the seams before spawning: the goroutine may outlive Scan and race
	// a test swapping them.
	list, args := listProcs, procArgs
	return runBounded(ctx, func(cctx context.Context) ([]Session, error) {
		return scan(cctx, list, args)
	})
}

// Proc is a live process's identity: pid and absolute start time. StartedAt
// doubles as the PID-generation fingerprint kill-time reconfirms key on.
type Proc struct {
	PID       int
	StartedAt time.Time
}

// ProcsByExecutable returns this user's live processes whose executable path
// equals execPath. The match is against the exec(2) path the kernel captured
// in KERN_PROCARGS2, so a process whose on-disk binary was since replaced or
// unlinked still matches. An empty execPath matches nothing. The walk is
// bounded like Scan's.
func ProcsByExecutable(ctx context.Context, execPath string) ([]Proc, error) {
	if execPath == "" {
		return nil, nil
	}
	list, args := listProcs, procArgs
	return runBounded(ctx, func(cctx context.Context) ([]Proc, error) {
		return byExecutable(cctx, execPath, list, args)
	})
}

// runBounded runs walk in its own goroutine and returns on scanTimeout; the
// buffered channel lets the abandoned goroutine finish without leaking.
func runBounded[T any](ctx context.Context, walk func(context.Context) ([]T, error)) ([]T, error) {
	cctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()
	type result struct {
		items []T
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		items, err := walk(cctx)
		ch <- result{items, err}
	}()
	select {
	case <-cctx.Done():
		return nil, fmt.Errorf("procscan: %w", cctx.Err())
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("procscan: %w", r.err)
		}
		return r.items, nil
	}
}

// scan lists this user's processes and attributes the claude ones. A failed list
// fails closed (callers read that as "cannot prove idle"); a per-PID read that
// says the process is gone (ESRCH) or has no readable args (EINVAL: zombie or
// kernel proc) skips just that process, while any other per-PID error fails the
// whole scan closed.
func scan(ctx context.Context, list func(context.Context) ([]proc, error), args func(context.Context, int) ([]byte, error)) ([]Session, error) {
	procs, err := list(ctx)
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
	}
	var sessions []Session
	for _, p := range procs {
		buf, err := args(ctx, p.pid)
		if err != nil {
			if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.EINVAL) {
				continue
			}
			return nil, fmt.Errorf("read args for pid %d: %w", p.pid, err)
		}
		argv0, configDir := parseProcArgs(buf)
		if !isClaudeProcess(argv0) {
			continue
		}
		sessions = append(sessions, Session{PID: p.pid, ConfigDir: configDir, StartedAt: p.startedAt})
	}
	return sessions, nil
}

// byExecutable walks this user's processes keeping those whose exec path
// equals execPath. Error semantics match scan: a failed list fails closed,
// ESRCH/EINVAL skips that process, any other per-PID error fails the walk.
func byExecutable(ctx context.Context, execPath string, list func(context.Context) ([]proc, error), args func(context.Context, int) ([]byte, error)) ([]Proc, error) {
	procs, err := list(ctx)
	if err != nil {
		return nil, fmt.Errorf("list processes: %w", err)
	}
	var out []Proc
	for _, p := range procs {
		buf, err := args(ctx, p.pid)
		if err != nil {
			if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.EINVAL) {
				continue
			}
			return nil, fmt.Errorf("read args for pid %d: %w", p.pid, err)
		}
		if parseExecPath(buf) != execPath {
			continue
		}
		out = append(out, Proc{PID: p.pid, StartedAt: p.startedAt})
	}
	return out, nil
}

// parseExecPath returns the executable path from a KERN_PROCARGS2 buffer — the
// first NUL-terminated string after argc — or "" for a buffer too short to
// hold one.
func parseExecPath(buf []byte) string {
	if len(buf) < 4 {
		return ""
	}
	rest := buf[4:]
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return ""
	}
	return string(rest[:i])
}

// parseProcArgs extracts argv[0] and the CLAUDE_CONFIG_DIR value from a
// KERN_PROCARGS2 buffer. Layout: uint32 argc; the NUL-terminated executable path;
// NUL alignment padding; argc NUL-terminated argv strings; then NUL-terminated
// environment strings ended by an empty string. A buffer too short to hold argc
// or the executable path yields an empty argv0, so its process is treated as
// non-claude and skipped.
func parseProcArgs(buf []byte) (argv0, configDir string) {
	if len(buf) < 4 {
		return "", ""
	}
	// argc is a non-negative count, decoded and iterated as uint32 (the kernel's
	// C int is never negative); the argv loop below is bounded by the buffer's
	// actual NUL runs, so a corrupt argc self-limits.
	argc := binary.NativeEndian.Uint32(buf[:4])
	rest := buf[4:]
	// Skip the executable path.
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return "", ""
	}
	rest = rest[i+1:]
	// Skip NUL alignment padding before argv[0].
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}
	// argv: argc NUL-terminated strings.
	for n := uint32(0); n < argc; n++ {
		j := bytes.IndexByte(rest, 0)
		if j < 0 {
			break // truncated argv
		}
		if n == 0 {
			argv0 = string(rest[:j])
		}
		rest = rest[j+1:]
	}
	// env: NUL-terminated KEY=VALUE strings until an empty string ends the block.
	for len(rest) > 0 {
		j := bytes.IndexByte(rest, 0)
		if j < 0 {
			break // truncated env
		}
		if j == 0 {
			break // empty string: end of environment
		}
		if v, ok := bytes.CutPrefix(rest[:j], ccdPrefix); ok {
			configDir = string(v)
		}
		rest = rest[j+1:]
	}
	return argv0, configDir
}

// isClaudeProcess reports whether argv0's basename is "claude" (the CLI, not
// ccp/cc-pool or a node child of a claude session).
func isClaudeProcess(argv0 string) bool {
	return filepath.Base(argv0) == "claude"
}

// CountByConfigDir counts sessions whose ConfigDir exactly matches configDir.
// Empty configDir matches nothing: plain-claude sessions belong to no pool
// account.
func CountByConfigDir(sessions []Session, configDir string) int {
	if configDir == "" {
		return 0
	}
	n := 0
	for _, s := range sessions {
		if s.ConfigDir == configDir {
			n++
		}
	}
	return n
}

// AlivePIDs returns the set of pids currently present, for session reconciliation.
func AlivePIDs(sessions []Session) map[int]bool {
	m := make(map[int]bool, len(sessions))
	for _, s := range sessions {
		m[s.PID] = true
	}
	return m
}
