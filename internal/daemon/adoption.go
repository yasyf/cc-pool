package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

const idleAdoptionRetry = time.Minute

// handleIdleTransition adopts a rotated credential only after the heartbeat and
// durable session rows both prove the account made its last busy-to-idle edge.
// Adoption changes private credential state only: it never dirties content or
// notifies File Provider.
func (s *Server) handleIdleTransition(ctx context.Context, dir string) {
	snapshot := s.heartbeatFor().view()
	if !snapshot.idle(dir) {
		return
	}
	accounts, err := s.m.Store.ListActiveAccounts()
	if err != nil {
		s.log.Printf("idle adoption: list accounts: %v", err)
		return
	}
	var account store.Account
	found := false
	for _, candidate := range accounts {
		if pool.AccountPresentationDir(candidate.ID) == dir {
			account = candidate
			found = true
			break
		}
	}
	if !found {
		s.heartbeatFor().acknowledgeIdle(dir)
		return
	}
	if !s.idleAdoptionDue(dir, time.Now()) {
		return
	}
	fresh, err := s.m.Store.GetAccount(account.ID)
	if errors.Is(err, store.ErrAccountNotFound) {
		s.heartbeatFor().acknowledgeIdle(dir)
		return
	}
	if err != nil {
		s.log.Printf("acct-%02d idle adoption: re-read row: %v", account.ID, err)
		return
	}
	if pool.AccountPresentationDir(fresh.ID) != dir || !s.heartbeatFor().view().idle(dir) {
		return
	}
	if s.cl.reservedCount(fresh.ID) != 0 {
		return
	}
	active, err := s.m.Store.ActiveSessionCount(fresh.ID)
	if err != nil {
		s.log.Printf("acct-%02d idle adoption: count sessions: %v", fresh.ID, err)
		return
	}
	if active != 0 {
		return
	}
	if !s.heartbeatFor().view().idle(dir) {
		return
	}
	adoptCtx, cancel := context.WithTimeout(ctx, preflightTimeout)
	err = s.adoptRotatedToken(adoptCtx, fresh)
	cancel()
	if err != nil {
		s.log.Printf("acct-%02d adopt rotated token on idle transition: %v", fresh.ID, err)
		s.deferIdleAdoption(dir, time.Now().Add(idleAdoptionRetry))
		return
	}
	s.clearIdleAdoption(dir)
	s.heartbeatFor().acknowledgeIdle(dir)
}

func (s *Server) adoptRotatedToken(ctx context.Context, account store.Account) error {
	if s.adoptRotated != nil {
		return s.adoptRotated(ctx, account)
	}
	return s.m.AdoptRotatedToken(ctx, account)
}

func (s *Server) idleAdoptionDue(dir string, now time.Time) bool {
	s.adoptionMu.Lock()
	defer s.adoptionMu.Unlock()
	return !now.Before(s.adoptionNext[dir])
}

func (s *Server) deferIdleAdoption(dir string, next time.Time) {
	s.adoptionMu.Lock()
	if s.adoptionNext == nil {
		s.adoptionNext = map[string]time.Time{}
	}
	s.adoptionNext[dir] = next
	s.adoptionMu.Unlock()
}

func (s *Server) clearIdleAdoption(dir string) {
	s.adoptionMu.Lock()
	delete(s.adoptionNext, dir)
	s.adoptionMu.Unlock()
}
