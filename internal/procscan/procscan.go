// Package procscan discovers live `claude` sessions and the account each is
// bound to by reading process environments. On macOS 26 `ps -Eww` prints
// same-user process environments, exposing CLAUDE_CONFIG_DIR.
//
// A claude process with no CLAUDE_CONFIG_DIR is plain `claude` on ~/.claude,
// mapped to no pool account.
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
	ConfigDir string // CLAUDE_CONFIG_DIR value, or "" for plain claude
	// StartedAt is the process start time (scan time minus ps's etime). Zero
	// means etime was unparseable — a soft-fail that keeps the session, since
	// staleness is advisory but session detection is load-bearing.
	StartedAt time.Time
}

// etime sits between pid and command because its [[dd-]hh:]mm:ss rendering never
// contains spaces (keeping parse's space splits unambiguous) and is locale-proof,
// unlike lstart.
var (
	psBin  = "/bin/ps"
	psArgs = []string{"-Eww", "-ax", "-o", "pid=,etime=,command="}
	// psOutput is the process-table seam, swapped for canned output in tests.
	psOutput = func(ctx context.Context) ([]byte, error) {
		cmd := exec.CommandContext(ctx, psBin, psArgs...)
		// WaitDelay can't free a caller blocked on a D-state ps (Scan's goroutine
		// decoupling does); it only lets the orphaned copy goroutine drain once ps
		// finally exits and closes stdout, instead of blocking forever.
		cmd.WaitDelay = 1 * time.Second
		return cmd.Output()
	}
)

var scanTimeout = 3 * time.Second

var configDirRE = regexp.MustCompile(`(?:^|\s)CLAUDE_CONFIG_DIR=(\S+)`)

// Scan returns all live claude sessions, bounding ps with scanTimeout: a wedged
// fuse-t mount can pin ps in uninterruptible D-state where neither ctx nor
// WaitDelay frees the caller (cmd.Wait parks in waitpid), so Scan runs ps in a
// goroutine and returns on the deadline. The buffered channel lets the abandoned
// goroutine finish without leaking; DeadlineExceeded surfaces as an error callers
// treat as "no sessions discovered".
func Scan(ctx context.Context) ([]Session, error) {
	cctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()
	// Capture the seam before spawning: the goroutine may outlive Scan and race a
	// test swapping psOutput.
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

// parse extracts sessions from ps output, anchoring StartedAt at now-etime.
func parse(out string, now time.Time) []Session {
	var sessions []Session
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimLeft(line, " ")
		if line == "" {
			continue
		}
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
		// Soft-fail: malformed etime keeps the session (see Session.StartedAt).
		if d, perr := parseEtime(etime); perr == nil {
			startedAt = now.Add(-d)
		}
		sessions = append(sessions, Session{PID: pid, ConfigDir: cd, StartedAt: startedAt})
	}
	return sessions
}

// parseEtime parses ps's etime ([[dd-]hh:]mm:ss elapsed since start) into a
// duration. Minimum form is mm:ss; ps never emits bare seconds.
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

// etimeField parses one etime component: non-empty, digits only. ParseUint
// rejects signs, so a stray '-' reads as garbage, not a negative count.
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

// isClaudeProcess reports whether argv[0]'s basename is "claude" (the CLI, not
// ccp/cc-pool).
func isClaudeProcess(cmd string) bool {
	tok := cmd
	if i := strings.IndexByte(cmd, ' '); i >= 0 {
		tok = cmd[:i]
	}
	base := filepath.Base(tok)
	return base == "claude"
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
