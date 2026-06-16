// Package procscan discovers live `claude` sessions and the account each is
// bound to, by reading process environments. On macOS 26 `ps -Eww` prints the
// environment of same-user processes (verified on the target machine), which
// exposes CLAUDE_CONFIG_DIR.
//
// A claude process with no CLAUDE_CONFIG_DIR in its environment is plain
// `claude` on ~/.claude — not a pool session; it maps to no pool account.
package procscan

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Session is a discovered live claude process.
type Session struct {
	PID       int
	ConfigDir string // value of CLAUDE_CONFIG_DIR, or "" for plain claude
	// StartedAt is the process start time, derived from ps's etime column
	// (scan time minus elapsed). The zero time means etime was unparseable —
	// the one tolerated soft-fail in this package: staleness flagging is
	// advisory, while session detection is load-bearing (uninstall gates and
	// remount logs key on it), so a cosmetic parse failure must never drop a
	// live session.
	StartedAt time.Time
}

// psBin and its args are overridable in tests. etime sits between pid and
// command because its rendering ([[dd-]hh:]mm:ss) never contains spaces, so
// parse's space-delimited field splits stay unambiguous; it is locale-proof,
// unlike lstart.
var (
	psBin  = "/bin/ps"
	psArgs = []string{"-Eww", "-ax", "-o", "pid=,etime=,command="}
	// psOutput is the process-table seam: the real ps in production, canned
	// output in tests (which must never scan real processes).
	psOutput = func(ctx context.Context) ([]byte, error) {
		cmd := exec.CommandContext(ctx, psBin, psArgs...)
		// A ps blocked reading a D-state process (one wedged on a hung fuse-t
		// mount) cannot be reaped by SIGKILL, so cmd.Wait stays parked in
		// waitpid regardless of ctx — WaitDelay does NOT change that. What
		// frees the CALLER is Scan's goroutine decoupling (see Scan). WaitDelay
		// is still worth setting: once ps finally exits (the mount unwedged) it
		// closes the orphaned stdout pipe and lets the abandoned goroutine
		// drain instead of blocking forever on the output copy.
		cmd.WaitDelay = 1 * time.Second
		return cmd.Output()
	}
)

// scanTimeout bounds one Scan. A var so tests can shrink it.
var scanTimeout = 3 * time.Second

var configDirRE = regexp.MustCompile(`(?:^|\s)CLAUDE_CONFIG_DIR=(\S+)`)

// Scan returns all live claude sessions. It bounds the underlying `ps` with
// scanTimeout: a wedged fuse-t mount can drive a process into uninterruptible
// D-state, and a `ps` reading that process blocks in-kernel forever, which
// would hang every launch and daemon poll.
//
// Releasing the CALLER takes more than exec.CommandContext + WaitDelay: cmd.Wait
// blocks in waitpid first, and neither ctx cancellation nor WaitDelay makes
// waitpid return for an unkillable (D-state) ps. So Scan runs psOutput in a
// goroutine and returns on the deadline regardless of whether ps ever dies. The
// abandoned goroutine stays parked in waitpid until the mount unwedges and ps
// finally exits; the buffered channel keeps it from leaking on send. A
// DeadlineExceeded surfaces as a normal error, which callers treat as "no
// sessions discovered".
func Scan(ctx context.Context) ([]Session, error) {
	cctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()
	// Capture the seam before spawning so the goroutine never reads the package
	// var: a goroutine left parked on an unkillable ps would otherwise race a
	// test swapping psOutput. Production never mutates it.
	run := psOutput
	type result struct {
		out []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := run(cctx)
		ch <- result{out, err}
	}()
	select {
	case <-cctx.Done():
		return nil, fmt.Errorf("ps scan: %w", cctx.Err())
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("ps scan: %w", r.err)
		}
		return parse(string(r.out), time.Now()), nil
	}
}

// parse extracts sessions from `ps -Eww -o pid=,etime=,command=` output. now
// anchors StartedAt: etime is elapsed wall time, so start = now - elapsed.
func parse(out string, now time.Time) []Session {
	var sessions []Session
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimLeft(line, " ")
		if line == "" {
			continue
		}
		// Fields: "pid etime command args ENV...". etime never contains
		// spaces, so two space splits recover all three.
		sp := strings.IndexByte(line, ' ')
		if sp < 0 {
			continue
		}
		pid, err := strconv.Atoi(line[:sp])
		if err != nil {
			continue
		}
		rest := strings.TrimLeft(line[sp+1:], " ")
		sp = strings.IndexByte(rest, ' ')
		if sp < 0 {
			continue
		}
		etime := rest[:sp]
		rest = strings.TrimLeft(rest[sp+1:], " ")
		if !isClaudeProcess(rest) {
			continue
		}
		cd := ""
		if m := configDirRE.FindStringSubmatch(rest); m != nil {
			cd = m[1]
		}
		var startedAt time.Time
		// Soft-fail by design (see Session.StartedAt): a malformed etime
		// zeroes StartedAt but keeps the session.
		if d, perr := parseEtime(etime); perr == nil {
			startedAt = now.Add(-d)
		}
		sessions = append(sessions, Session{PID: pid, ConfigDir: cd, StartedAt: startedAt})
	}
	return sessions
}

// parseEtime parses ps's etime column — elapsed wall time since process
// start, rendered [[dd-]hh:]mm:ss — into a duration. The minimum form is
// mm:ss (ps never emits bare seconds).
func parseEtime(s string) (time.Duration, error) {
	rest := s
	var days uint64
	hasDays := false
	if i := strings.IndexByte(s, '-'); i >= 0 {
		d, err := etimeField(s[:i])
		if err != nil {
			return 0, fmt.Errorf("etime %q: days: %w", s, err)
		}
		days, hasDays, rest = d, true, s[i+1:]
	}
	parts := strings.Split(rest, ":")
	var hh, mm, ss uint64
	var err error
	switch {
	case len(parts) == 3:
		if hh, err = etimeField(parts[0]); err != nil {
			return 0, fmt.Errorf("etime %q: hours: %w", s, err)
		}
		if mm, err = etimeField(parts[1]); err != nil {
			return 0, fmt.Errorf("etime %q: minutes: %w", s, err)
		}
		if ss, err = etimeField(parts[2]); err != nil {
			return 0, fmt.Errorf("etime %q: seconds: %w", s, err)
		}
	case len(parts) == 2 && !hasDays:
		if mm, err = etimeField(parts[0]); err != nil {
			return 0, fmt.Errorf("etime %q: minutes: %w", s, err)
		}
		if ss, err = etimeField(parts[1]); err != nil {
			return 0, fmt.Errorf("etime %q: seconds: %w", s, err)
		}
	default:
		return 0, fmt.Errorf("etime %q: want [[dd-]hh:]mm:ss", s)
	}
	if ss > 59 || mm > 59 || (hasDays && hh > 23) {
		return 0, fmt.Errorf("etime %q: field out of range", s)
	}
	return time.Duration(days)*24*time.Hour +
		time.Duration(hh)*time.Hour +
		time.Duration(mm)*time.Minute +
		time.Duration(ss)*time.Second, nil
}

// etimeField parses one etime component: non-empty, digits only (ParseUint
// rejects signs, so a stray '-' inside a field reads as garbage, not a
// negative count).
func etimeField(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty field")
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// isClaudeProcess reports whether a command line belongs to the claude CLI
// itself (argv[0] basename == "claude"), excluding our own ccp/cc-pool.
func isClaudeProcess(cmd string) bool {
	tok := cmd
	if i := strings.IndexByte(cmd, ' '); i >= 0 {
		tok = cmd[:i]
	}
	base := filepath.Base(tok)
	return base == "claude"
}

// CountByConfigDir counts sessions whose ConfigDir exactly matches configDir.
// An empty configDir matches nothing: no pool account has an empty dir, and
// plain-claude sessions (empty ConfigDir) belong to no pool account.
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
