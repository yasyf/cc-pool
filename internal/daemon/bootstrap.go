package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
)

type bootstrapState struct {
	started     bool
	revision    uint64
	total       int
	settled     int
	quarantined int
	terminal    bool
	failures    map[int][32]byte
}

func (s *Server) beginBootstrap() {
	s.bootstrapMu.Lock()
	s.bootstrap = bootstrapState{
		started: true, revision: 1, failures: make(map[int][32]byte),
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
	s.bootstrap.revision++
}

func (s *Server) settleBootstrapAccount(accountID int, quarantined bool, err error) {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if !s.bootstrap.started || s.bootstrap.terminal {
		return
	}
	s.bootstrap.settled++
	s.bootstrap.revision++
	if quarantined {
		s.bootstrap.quarantined++
	}
	if err != nil {
		s.bootstrap.failures[accountID] = sha256.Sum256([]byte(err.Error()))
		if s.log != nil {
			s.log.Printf("tenant bootstrap acct-%02d failed: %v", accountID, err)
		}
	}
}

func (s *Server) finishBootstrap(err error) {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	if !s.bootstrap.started || s.bootstrap.terminal {
		return
	}
	if err != nil && len(s.bootstrap.failures) == 0 {
		s.bootstrap.failures[0] = sha256.Sum256([]byte(err.Error()))
		if s.log != nil {
			s.log.Printf("tenant bootstrap failed before account settlement: %v", err)
		}
	}
	s.bootstrap.terminal = true
	s.bootstrap.revision++
}

type bootstrapProgress struct {
	Schema        uint16 `json:"schema"`
	Revision      uint64 `json:"revision"`
	Total         int    `json:"total"`
	Settled       int    `json:"settled"`
	Quarantined   int    `json:"quarantined"`
	Terminal      bool   `json:"terminal"`
	FailureCount  int    `json:"failure_count"`
	FailureDigest string `json:"failure_digest"`
}

func (s *Server) bootstrapSnapshot() bootstrapProgress {
	s.bootstrapMu.Lock()
	defer s.bootstrapMu.Unlock()
	progress := bootstrapProgress{
		Schema: 1, Revision: s.bootstrap.revision,
		Total: s.bootstrap.total, Settled: s.bootstrap.settled,
		Quarantined: s.bootstrap.quarantined, Terminal: s.bootstrap.terminal,
		FailureCount: len(s.bootstrap.failures),
	}
	ids := make([]int, 0, len(s.bootstrap.failures))
	for id := range s.bootstrap.failures {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	hash := sha256.New()
	for _, id := range ids {
		_, _ = hash.Write([]byte(strconv.Itoa(id)))
		_, _ = hash.Write([]byte{0})
		digest := s.bootstrap.failures[id]
		_, _ = hash.Write(digest[:])
	}
	progress.FailureDigest = hex.EncodeToString(hash.Sum(nil))
	return progress
}

func (s *Server) bootstrapBarrierSnapshot() (uint64, []byte, error) {
	progress := s.bootstrapSnapshot()
	payload, err := json.Marshal(progress)
	if err != nil {
		return 0, nil, err
	}
	return progress.Revision, payload, nil
}
