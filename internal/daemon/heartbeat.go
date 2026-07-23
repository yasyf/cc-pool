package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
)

const (
	defaultSessionHeartbeatInterval = 5 * time.Second
	heartbeatOnDemandFreshness      = time.Second
)

type heartbeatSnapshot struct {
	initialized bool
	lastScanOK  bool
	scannedAt   time.Time
	sessions    []procscan.Session
	counts      map[string]int
	claude      map[int]time.Time
	processes   map[int]time.Time
}

func (s heartbeatSnapshot) sessionCount(dir string) int { return s.counts[dir] }

func (s heartbeatSnapshot) idle(dir string) bool {
	return s.initialized && s.lastScanOK && s.counts[dir] == 0
}

type heartbeatDelta struct {
	snapshot    heartbeatSnapshot
	newlyActive []string
	idle        []string
	success     bool
}

type sessionHeartbeat struct {
	server *Server

	refreshMu sync.Mutex
	mu        sync.RWMutex
	snapshot  heartbeatSnapshot
	active    map[string]bool
	lastErr   string
}

func newSessionHeartbeat(s *Server) *sessionHeartbeat {
	return &sessionHeartbeat{server: s, active: map[string]bool{}}
}

func (h *sessionHeartbeat) view() heartbeatSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return cloneHeartbeatSnapshot(h.snapshot)
}

func cloneHeartbeatSnapshot(in heartbeatSnapshot) heartbeatSnapshot {
	out := in
	out.sessions = append([]procscan.Session(nil), in.sessions...)
	out.counts = make(map[string]int, len(in.counts))
	for dir, count := range in.counts {
		out.counts[dir] = count
	}
	out.claude = make(map[int]time.Time, len(in.claude))
	for pid, started := range in.claude {
		out.claude[pid] = started
	}
	out.processes = make(map[int]time.Time, len(in.processes))
	for pid, started := range in.processes {
		out.processes[pid] = started
	}
	return out
}

func (h *sessionHeartbeat) refresh(ctx context.Context, maxAge time.Duration) heartbeatDelta {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()

	current := h.view()
	if current.initialized && maxAge > 0 && time.Since(current.scannedAt) < maxAge {
		return heartbeatDelta{snapshot: current}
	}
	durableActive := map[string]bool{}
	if !current.initialized && h.server.m != nil && h.server.m.Store != nil {
		rows, err := h.server.m.Store.ListActiveSessions()
		if err != nil {
			h.server.log.Printf("heartbeat seed active sessions: %v", err)
		} else {
			for _, row := range rows {
				if row.ConfigDir != "" {
					durableActive[row.ConfigDir] = true
				}
			}
		}
	}
	processSnapshot, err := h.server.scan(ctx)
	now := time.Now()
	if err != nil {
		h.mu.Lock()
		h.snapshot.lastScanOK = false
		if err.Error() != h.lastErr {
			h.server.log.Printf("procscan failed; retaining last-known active sessions: %v", err)
			h.lastErr = err.Error()
		}
		failed := cloneHeartbeatSnapshot(h.snapshot)
		h.mu.Unlock()
		return heartbeatDelta{snapshot: failed}
	}
	sessions := processSnapshot.Sessions

	counts := map[string]int{}
	for _, session := range sessions {
		counts[session.ConfigDir]++
	}
	claude := procscan.ClaudeProcesses(sessions)
	h.mu.Lock()
	previous := h.snapshot
	for dir := range durableActive {
		h.active[dir] = true
	}
	newlyActive := make([]string, 0)
	for dir, count := range counts {
		if count == 0 {
			continue
		}
		if !previous.initialized || previous.counts[dir] == 0 {
			newlyActive = append(newlyActive, dir)
		}
		h.active[dir] = true
	}
	idle := make([]string, 0)
	for dir := range h.active {
		if counts[dir] == 0 {
			idle = append(idle, dir)
		}
	}
	h.snapshot = heartbeatSnapshot{
		initialized: true,
		lastScanOK:  true,
		scannedAt:   now,
		sessions:    append([]procscan.Session(nil), sessions...),
		counts:      counts,
		claude:      claude,
		processes:   processSnapshot.Processes,
	}
	h.lastErr = ""
	result := cloneHeartbeatSnapshot(h.snapshot)
	h.mu.Unlock()
	return heartbeatDelta{snapshot: result, newlyActive: newlyActive, idle: idle, success: true}
}

func (h *sessionHeartbeat) acknowledgeIdle(dir string) {
	h.mu.Lock()
	if h.snapshot.counts[dir] == 0 {
		delete(h.active, dir)
	}
	h.mu.Unlock()
}

func (h *sessionHeartbeat) run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = defaultSessionHeartbeatInterval
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		delta := h.refresh(ctx, 0)
		h.server.handleHeartbeatDelta(ctx, delta)
		timer.Reset(interval)
	}
}

func (s *Server) heartbeatFor() *sessionHeartbeat {
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	if s.heartbeat == nil {
		s.heartbeat = newSessionHeartbeat(s)
	}
	return s.heartbeat
}

func (s *Server) refreshHeartbeat(ctx context.Context, maxAge time.Duration) heartbeatSnapshot {
	delta := s.heartbeatFor().refresh(ctx, maxAge)
	if delta.success {
		s.handleHeartbeatDelta(ctx, delta)
	}
	return delta.snapshot
}

func (s *Server) startHeartbeat(ctx context.Context) {
	s.refreshHeartbeat(ctx, 0)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.heartbeatFor().run(ctx, s.heartbeatInterval)
	}()
}

func (s *Server) handleHeartbeatDelta(ctx context.Context, delta heartbeatDelta) {
	if !delta.success {
		return
	}
	if n, err := s.m.Store.CloseDeadSessions(delta.snapshot.claude, delta.snapshot.processes, time.Now()); err != nil {
		s.log.Printf("close dead sessions: %v", err)
	} else if n > 0 {
		s.log.Printf("reconciled %d ended session(s)", n)
	}
	for _, dir := range delta.idle {
		s.handleIdleTransition(ctx, dir)
	}
}
