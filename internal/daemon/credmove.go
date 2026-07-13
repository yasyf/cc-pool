package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
)

// credMoveTimeout bounds one account's credential move. The per-account flock
// wait is the only blocking step (the store ops are local security(1) execs
// and file I/O), so a short bound rides out a contended lock without letting
// one account hang the credmove loop or overrun the extended conn deadline.
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

	accts, err := s.m.Store.ListAccounts()
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

	results := make([]MigrationResult, 0, len(accts))
	for _, a := range accts {
		if ctx.Err() != nil {
			break
		}
		results = append(results, s.moveAccountCred(ctx, a, target, req.To))
	}
	return Response{OK: true, Migrations: results}
}

// moveAccountCred runs one gated credential move. Unlike convertAccount there
// is no force override for the live-session gate: a live claude holds its
// credential in memory and writes rotations back to the backend it read from,
// so moving under it would fork the refresh-token chain and kill one side.
func (s *Server) moveAccountCred(ctx context.Context, a store.Account, target creds.Source, to string) MigrationResult {
	res := MigrationResult{ID: a.ID, Label: a.Label, To: to}
	// Claim and re-read the row under it — the caller's list is a stale snapshot
	// and the row names the stores (keychain service, config dir) the move acts on.
	fresh, ok, err := s.ownFresh(a.ID)
	if !ok {
		res.Outcome = MigrationBusy
		res.Detail = "held by a pending select, a daemon poll, or another conversion; retry shortly"
		return res
	}
	defer s.cl.disownConvert(a.ID)
	if err != nil {
		res.Outcome = MigrationFailed
		res.Detail = fmt.Sprintf("re-read account row: %v", err)
		return res
	}
	a = fresh
	res.Label = a.Label

	// A cred move is a local mutation cc-pool performs itself (the store ops touch
	// the account's Keychain item and its config-dir file copy), so fence it under an
	// exclusive session-lease seize: a live session or a select handout — a live
	// claude holds its credential in memory and writes rotations back to the backend
	// it read from, and a handout is invisible to procscan before claude starts —
	// defers the move rather than forking the refresh-token chain.
	fence, err := s.m.SeizeSessionLease(a)
	if err != nil {
		res.Outcome = MigrationBusy
		res.Detail = "held by a live session or a select handout; relaunch or close it, then retry"
		return res
	}
	defer func() { _ = fence.Release() }()

	mctx, cancel := context.WithTimeout(ctx, credMoveTimeout)
	defer cancel()
	mv, err := s.m.MoveCredential(mctx, a, target)
	if err != nil {
		res.Outcome = MigrationFailed
		res.Detail = err.Error()
		return res
	}
	res.From = mv.From.String()
	if mv.CleanedStray {
		res.Detail = "cleaned a stray file copy"
		s.log.Printf("acct-%02d cleaned a stray file credential copy", a.ID)
	}
	if !mv.Moved {
		res.Outcome = MigrationAlready
		return res
	}
	res.Outcome = MigrationDone
	s.log.Printf("acct-%02d credential moved %s -> %s", a.ID, res.From, res.To)
	return res
}
