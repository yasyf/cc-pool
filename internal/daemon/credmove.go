package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

// credMoveTimeout bounds one account's credential move and its disposable
// Keychain workers.
const credMoveTimeout = 15 * time.Second

// handleCredMove moves account credentials between backends; only the daemon
// can gate moves against its own select reservations and poll claims.
func (s *Server) handleCredMove(ctx context.Context, req Request) Response {
	var target creds.Source
	switch req.To {
	case "keychain":
		target = creds.SourceKeychain
	case "file":
		target = creds.SourceFile
	default:
		return Response{OK: false, Error: fmt.Sprintf("unknown credential target %q (want keychain or file)", req.To)}
	}

	accts, err := s.m.Store.ListActiveAccounts()
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	if req.Account != nil {
		found := false
		for _, a := range accts {
			if a.ID == *req.Account {
				accts = []store.Account{a}
				found = true
				break
			}
		}
		if !found {
			return Response{OK: false, Error: fmt.Sprintf("account %d not found", *req.Account)}
		}
	}

	results := make([]CredentialMoveResult, 0, len(accts))
	for _, a := range accts {
		if ctx.Err() != nil {
			break
		}
		results = append(results, s.moveAccountCred(ctx, a, target, req.To))
	}
	return Response{OK: true, CredentialMoves: results}
}

// moveAccountCred runs one gated credential move. Unlike convertAccount there
// is no force override for the live-session gate: a live claude holds its
// credential in memory and writes rotations back to the backend it read from,
// so moving under it would fork the refresh-token chain and kill one side.
func (s *Server) moveAccountCred(ctx context.Context, a store.Account, target creds.Source, to string) CredentialMoveResult {
	res := CredentialMoveResult{ID: a.ID, Label: a.Label, To: to}
	// Re-read the row: the caller's list is a stale snapshot and the row names
	// the stores the durable credential operation acts on.
	fresh, err := s.m.Store.GetAccount(a.ID)
	if err != nil {
		res.Outcome = CredentialMoveFailed
		res.Detail = fmt.Sprintf("re-read account row: %v", err)
		return res
	}
	a = fresh
	res.Label = a.Label

	// Existing sessions and already-pending selections prevent a move. A new
	// selection that starts after this check waits on the durable credential lane
	// before its reservation can be returned or activated.
	if s.heartbeatFor().view().sessionCount(pool.AccountPresentationDir(a.ID)) != 0 {
		res.Outcome = CredentialMoveBusy
		res.Detail = "held by a live session; close it, then retry"
		return res
	}
	if s.cl.reservedCount(a.ID) != 0 {
		res.Outcome = CredentialMoveBusy
		res.Detail = "held by a pending selection; retry after it settles"
		return res
	}

	mctx, cancel := context.WithTimeout(ctx, credMoveTimeout)
	defer cancel()
	mv, err := s.m.MoveCredential(mctx, a, target)
	if err != nil {
		res.Outcome = CredentialMoveFailed
		res.Detail = err.Error()
		return res
	}
	res.From = mv.From.String()
	if mv.CleanedStray {
		res.Detail = "cleaned a stray file copy"
		s.log.Printf("acct-%02d cleaned a stray file credential copy", a.ID)
	}
	if !mv.Moved {
		res.Outcome = CredentialMoveAlready
		return res
	}
	res.Outcome = CredentialMoveDone
	s.log.Printf("acct-%02d credential moved %s -> %s", a.ID, res.From, res.To)
	return res
}
