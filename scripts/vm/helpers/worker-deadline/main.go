// Package main verifies disposable-worker deadline and process-group semantics.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit"
)

type result struct {
	LeaderPID     int   `json:"leader_pid"`
	DescendantPID int   `json:"descendant_pid"`
	ElapsedMS     int64 `json:"elapsed_ms"`
	TermObserved  bool  `json:"term_observed"`
	GroupGone     bool  `json:"group_gone"`
	LaneReused    bool  `json:"lane_reused"`
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "worker-deadline acceptance: %v\n", err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	root, err := os.MkdirTemp("", "ccpool-worker-deadline-")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(root)) }()
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open worker deadline root: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rootFS.Close()) }()

	scopeCtx, cancelScope := context.WithTimeout(context.Background(), time.Minute)
	defer cancelScope()
	owned, err := daemonkit.OwnProcesses(scopeCtx, filepath.Join(root, "processes.db"))
	if err != nil {
		return fmt.Errorf("own worker processes: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, owned.Close(closeCtx))
	}()
	scope := owned.Ctx(scopeCtx)

	const leaderName = "leader.pid"
	const descendantName = "descendant.pid"
	const termName = "term-observed"
	leaderPath := filepath.Join(root, leaderName)
	descendantPath := filepath.Join(root, descendantName)
	termPath := filepath.Join(root, termName)
	script := `
trap 'printf term > "$3"' TERM
printf '%s\n' "$$" > "$1"
/bin/sh -c 'trap "" TERM; printf "%s\n" "$$" > "$1"; while :; do sleep 10; done' descendant "$2" &
while [ ! -s "$2" ]; do sleep 0.01; done
while :; do sleep 10; done
`
	const requestTimeout = 2 * time.Second
	started := time.Now()
	runCtx, cancelRun := context.WithTimeout(scopeCtx, requestTimeout)
	_, err = scope.Run(runCtx, daemonkit.Cmd{
		Path: "/bin/sh", Dir: root,
		Args:      []string{"-c", script, "worker", leaderPath, descendantPath, termPath},
		MaxOutput: 1024,
		Exec:      daemonkit.ServingSameUser(),
	})
	cancelRun()
	elapsed := time.Since(started)
	if err == nil {
		return errors.New("worker completed without deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("worker error, want deadline exceeded: %w", err)
	}
	if elapsed < 500*time.Millisecond {
		return fmt.Errorf("worker settled after %s without an observable TERM grace", elapsed)
	}
	content, err := rootFS.ReadFile(termName)
	if err != nil {
		return fmt.Errorf("read TERM observation: %w", err)
	}
	if string(content) != "term" {
		return fmt.Errorf("TERM observation = %q, want term", content)
	}
	leader, err := readPID(rootFS, leaderName)
	if err != nil {
		return err
	}
	descendant, err := readPID(rootFS, descendantName)
	if err != nil {
		return err
	}
	if err := awaitGone(leader); err != nil {
		return fmt.Errorf("leader: %w", err)
	}
	if err := awaitGone(descendant); err != nil {
		return fmt.Errorf("descendant: %w", err)
	}
	if err := awaitGone(-leader); err != nil {
		return fmt.Errorf("process group: %w", err)
	}
	reuseCtx, cancelReuse := context.WithTimeout(scopeCtx, requestTimeout)
	defer cancelReuse()
	if _, err := scope.Run(reuseCtx, daemonkit.Cmd{
		Path: "/usr/bin/true", Dir: root, MaxOutput: 1024, Exec: daemonkit.ServingSameUser(),
	}); err != nil {
		return fmt.Errorf("reuse worker lane: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(result{
		LeaderPID: leader, DescendantPID: descendant, ElapsedMS: elapsed.Milliseconds(),
		TermObserved: true, GroupGone: true, LaneReused: true,
	})
}

func readPID(root *os.Root, name string) (int, error) {
	content, err := root.ReadFile(name)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", name, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil {
		return 0, fmt.Errorf("parse pid in %s: %w", name, err)
	}
	if pid <= 1 {
		return 0, fmt.Errorf("invalid pid in %s: %q", name, content)
	}
	return pid, nil
}

func awaitGone(pid int) error {
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pid %d remains signalable", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
