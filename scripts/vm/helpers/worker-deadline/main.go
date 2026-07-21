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

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

type result struct {
	LeaderPID     int   `json:"leader_pid"`
	DescendantPID int   `json:"descendant_pid"`
	ElapsedMS     int64 `json:"elapsed_ms"`
	RecordsAfter  int   `json:"records_after"`
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

func run() error {
	root, err := os.MkdirTemp("", "ccpool-worker-deadline-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	store := &proc.FileStore{Path: filepath.Join(root, "processes.json")}
	reaper := &proc.Reaper{Store: store, Generation: "ccpool-vm-worker-deadline"}
	pool, err := supervise.NewPool(1, reaper)
	if err != nil {
		return err
	}
	leaderPath := filepath.Join(root, "leader.pid")
	descendantPath := filepath.Join(root, "descendant.pid")
	termPath := filepath.Join(root, "term-observed")
	script := `
trap 'printf term > "$3"' TERM
printf '%s\n' "$$" > "$1"
/bin/sh -c 'trap "" TERM; printf "%s\n" "$$" > "$1"; while :; do sleep 10; done' descendant "$2" &
while [ ! -s "$2" ]; do sleep 0.01; done
while :; do sleep 10; done
`
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	started := time.Now()
	err = pool.Run(ctx, supervise.Task{
		Path: "/bin/sh", Args: []string{"-c", script, "worker", leaderPath, descendantPath, termPath},
	})
	cancel()
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("worker error = %v, want deadline exceeded", err)
	}
	if elapsed < supervise.TerminationGrace {
		return fmt.Errorf("worker settled after %s, before TERM grace %s", elapsed, supervise.TerminationGrace)
	}
	if content, readErr := os.ReadFile(termPath); readErr != nil || string(content) != "term" {
		return fmt.Errorf("TERM observation = %q, %v", content, readErr)
	}
	leader, err := readPID(leaderPath)
	if err != nil {
		return err
	}
	descendant, err := readPID(descendantPath)
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
	records, err := store.Load(context.Background())
	if err != nil {
		return fmt.Errorf("load durable records: %w", err)
	}
	if len(records) != 0 {
		return fmt.Errorf("durable records after settlement = %d, want 0", len(records))
	}
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()
	if err := pool.Run(retryCtx, supervise.Task{Path: "/usr/bin/true"}); err != nil {
		return fmt.Errorf("reuse worker lane: %w", err)
	}
	pool.Close()
	if err := pool.Wait(context.Background()); err != nil {
		return fmt.Errorf("wait for worker pool: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(result{
		LeaderPID: leader, DescendantPID: descendant, ElapsedMS: elapsed.Milliseconds(),
		RecordsAfter: len(records), TermObserved: true, GroupGone: true, LaneReused: true,
	})
}

func readPID(path string) (int, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", filepath.Base(path), err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil || pid <= 1 {
		return 0, fmt.Errorf("invalid pid in %s: %q", filepath.Base(path), content)
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
