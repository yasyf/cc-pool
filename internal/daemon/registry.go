package daemon

import (
	"context"

	"github.com/yasyf/cc-pool/internal/store"
)

type tick struct {
	snapshot heartbeatSnapshot
}

func (s *Server) newTick(ctx context.Context) *tick {
	h := s.heartbeatFor()
	snapshot := h.view()
	if !snapshot.initialized {
		snapshot = h.refresh(ctx, 0).snapshot
	}
	return &tick{snapshot: snapshot}
}

func (t *tick) scanOK() bool { return t.snapshot.lastScanOK }

func (t *tick) idle(dir string) bool { return t.snapshot.idle(dir) }

func (t *tick) sessionCount(dir string) int { return t.snapshot.sessionCount(dir) }

type claimScope int

const (
	claimNone claimScope = iota
	claimPerAccount
)

type maintainer struct {
	name       string
	claimScope claimScope
	gate       func(s *Server) bool
	run        func(s *Server, ctx context.Context, t *tick) bool
}

func (s *Server) runTable(ctx context.Context, t *tick, table []maintainer) {
	for _, maintainer := range table {
		if ctx.Err() != nil || s.closing.Load() {
			return
		}
		if maintainer.gate != nil && !maintainer.gate(s) {
			continue
		}
		if !maintainer.run(s, ctx, t) {
			return
		}
	}
}

var pollTable = []maintainer{
	{"sticky.prune", claimNone, nil, func(s *Server, _ context.Context, _ *tick) bool {
		s.pruneStickyRows()
		return true
	}},
	{"credential.receipts.prune", claimNone, nil, func(s *Server, _ context.Context, _ *tick) bool {
		deleted, err := s.m.Store.DeleteExpiredCredentialOperationReceipts(store.CredentialOperationPageLimit)
		if err != nil {
			s.log.Printf("prune credential receipts: %v", err)
		} else if deleted == store.CredentialOperationPageLimit {
			s.log.Printf("pruned %d expired credential receipts; more remain for the next bounded pass", deleted)
		}
		return true
	}},
	{"account.poll", claimPerAccount, nil, func(s *Server, ctx context.Context, t *tick) bool {
		return s.pollAccounts(ctx, t)
	}},
	{"status.snapshot", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		if err := s.writeStatusSnapshot(ctx); err != nil {
			s.log.Printf("status snapshot: %v", err)
		}
		return true
	}},
}

var startupTable = []maintainer{
	{"session.heartbeat", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.startHeartbeat(ctx)
		return true
	}},
	{"ua.detect", claimNone, nil, func(s *Server, ctx context.Context, _ *tick) bool {
		s.detectAndSetUserAgent(ctx)
		return true
	}},
}
