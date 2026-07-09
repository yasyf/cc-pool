package daemon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"golang.org/x/sys/unix"
)

// staleWidgetSlack absorbs second-granularity rounding between the kernel's
// process start time and the binary's ctime.
const staleWidgetSlack = 5 * time.Second

// Seams for tests: the process scan, the appex binary location, and the kill.
var (
	listWidgetProcs  = procscan.ProcsByExecutable
	widgetBinaryPath = pool.WidgetAppexBinaryPath
	killPID          = func(pid int) error { return unix.Kill(pid, unix.SIGKILL) }
)

// WidgetAppex is a live CCPoolStatusWidget appex running a binary older than
// the one now installed.
type WidgetAppex struct {
	PID       int
	StartedAt time.Time
}

// StaleWidgetAppexes returns the live widget appex processes whose start time
// predates the ctime of the binary at binaryPath — ctime is the install time
// (a cask's ditto copy preserves mtime, the build time). Such a process runs a
// binary a cask upgrade replaced: WidgetKit never recycles a live appex, so
// its render stays frozen until the process dies. A missing binary (widget not
// installed) is normal and yields none. Shared by the daemon's reconcile and
// `ccp doctor`.
func StaleWidgetAppexes(ctx context.Context, binaryPath string) ([]WidgetAppex, error) {
	fi, err := os.Stat(binaryPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stat widget binary: %w", err)
	}
	ct := fi.Sys().(*syscall.Stat_t).Ctimespec
	installedAt := time.Unix(ct.Sec, ct.Nsec)
	procs, err := listWidgetProcs(ctx, binaryPath)
	if err != nil {
		return nil, err
	}
	var stale []WidgetAppex
	for _, p := range procs {
		if installedAt.Sub(p.StartedAt) > staleWidgetSlack {
			stale = append(stale, WidgetAppex{PID: p.PID, StartedAt: p.StartedAt})
		}
	}
	return stale, nil
}

// reconcileStaleWidget SIGKILLs stale widget appexes; chronod respawns the
// current binary at the next reload. Each candidate is reconfirmed against a
// fresh scan on (pid, start time) immediately before the kill, so a PID reused
// since the first scan — or an appex already respawned from the new binary —
// is spared.
func (s *Server) reconcileStaleWidget(ctx context.Context) {
	stale, err := StaleWidgetAppexes(ctx, widgetBinaryPath())
	if err != nil {
		s.log.Printf("stale widget scan: %v", err)
		return
	}
	if len(stale) == 0 {
		return
	}
	for _, p := range stale {
		// Reconfirm THIS candidate against a fresh scan immediately before its kill —
		// per candidate, not one batch confirm reused across the loop — so a later
		// candidate is never killed on staler evidence: a pid reused since an earlier
		// kill, or an appex already respawned from the new binary, is spared. The same
		// kill-time reconfirm the FP orphan reap does.
		confirm, err := StaleWidgetAppexes(ctx, widgetBinaryPath())
		if err != nil {
			s.log.Printf("stale widget reconfirm: %v", err)
			return
		}
		if !widgetStillStale(confirm, p) {
			continue
		}
		if err := killPID(p.PID); err != nil {
			s.log.Printf("kill stale widget appex pid %d: %v", p.PID, err)
			continue
		}
		s.log.Printf("killed stale widget appex pid %d (started %s): an upgrade replaced its binary; chronod respawns the current one on the next reload",
			p.PID, p.StartedAt.Format("15:04:05"))
	}
}

// widgetStillStale reports whether p is still present in a fresh scan at the SAME
// (pid, start time) — the per-candidate kill-time reconfirm that spares a reused pid
// or an appex already respawned from the new binary.
func widgetStillStale(confirm []WidgetAppex, p WidgetAppex) bool {
	for _, c := range confirm {
		if c.PID == p.PID && c.StartedAt.Equal(p.StartedAt) {
			return true
		}
	}
	return false
}
