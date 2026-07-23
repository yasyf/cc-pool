package daemon

import (
	"sort"
	"time"
)

type bootstrapState struct {
	started        bool
	total          int
	settled        int
	quarantined    int
	terminal       bool
	failures       map[int]string
	lastProgressAt time.Time
}

func (s *Server) bootstrapTime() time.Time {
	if s.bootstrapNow != nil {
		return s.bootstrapNow()
	}
	return time.Now()
}

func (s *Server) beginBootstrap() {
	s.bootstrapMu.Lock()
	s.bootstrap = bootstrapState{
		started: true, failures: make(map[int]string), lastProgressAt: s.bootstrapTime(),
	}
	s.bootstrapMu.Unlock()
}

func (s *Server) setBootstrapTotal(total int) {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if !s.bootstrap.started || s.bootstrap.terminal {
		return
	}
	s.bootstrap.total = total
	s.bootstrap.lastProgressAt = s.bootstrapTime()
}

func (s *Server) settleBootstrapAccount(accountID int, quarantined bool, err error) {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if !s.bootstrap.started || s.bootstrap.terminal {
		return
	}
	s.bootstrap.settled++
	if quarantined {
		s.bootstrap.quarantined++
	}
	if err != nil {
		s.bootstrap.failures[accountID] = err.Error()
	}
	s.bootstrap.lastProgressAt = s.bootstrapTime()
}

func (s *Server) finishBootstrap(err error) {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if !s.bootstrap.started || s.bootstrap.terminal {
		return
	}
	if err != nil && len(s.bootstrap.failures) == 0 {
		s.bootstrap.failures[0] = err.Error()
	}
	s.bootstrap.terminal = true
	s.bootstrap.lastProgressAt = s.bootstrapTime()
}

type bootstrapProgress struct {
	Total          int
	Settled        int
	Quarantined    int
	Terminal       bool
	Failures       []bootstrapFailure
	LastProgressAt time.Time
}

type bootstrapFailure struct {
	AccountID int
	Error     string
}

func (s *Server) bootstrapSnapshot() bootstrapProgress {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	progress := bootstrapProgress{
		Total: s.bootstrap.total, Settled: s.bootstrap.settled,
		Quarantined: s.bootstrap.quarantined, Terminal: s.bootstrap.terminal,
		LastProgressAt: s.bootstrap.lastProgressAt,
	}
	ids := make([]int, 0, len(s.bootstrap.failures))
	for id := range s.bootstrap.failures {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		progress.Failures = append(progress.Failures, bootstrapFailure{AccountID: id, Error: s.bootstrap.failures[id]})
	}
	return progress
}
