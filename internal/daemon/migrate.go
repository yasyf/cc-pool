package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// defaultMigrateBudget bounds one migrate request's conversion work so the
// response always lands inside handle()'s extended conn deadline (140s) and
// the client's 150s timeout — the server, not a dead socket, must report the
// outcome. Each conversion can take ~16s worst case (8s mount wait, bounded
// rollback teardown), so a big pool can exceed any fixed deadline; accounts
// the budget cannot reach are reported busy and swept by a re-run, which the
// rollout already requires for session-busy accounts.
const defaultMigrateBudget = 120 * time.Second

// handleMigrate converts accounts between overlay providers. Only the daemon
// can do this: the mounts live in the detached holder, but the daemon alone
// can gate the conversion against its own select reservations atomically.
// Busy accounts
// (live sessions or reservations) are skipped and reported so the client can
// re-run later; per-account failures are rolled back by ConvertOverlay.
func (s *Server) handleMigrate(ctx context.Context, req Request) Response {
	var to fkoverlay.Backend
	switch req.To {
	case "fuse":
		// "fuse" is the user's coarse word; the gate resolves the concrete
		// backend (nfs/fskit) by running Select inside the daemon, so the CLI
		// never names a concrete fuse backend.
		backend, msg := s.fuseGate()
		if msg != "" {
			return Response{OK: false, Error: msg}
		}
		to = backend
	case "symlink":
		to = fkoverlay.BackendSymlink
	default:
		return Response{OK: false, Error: fmt.Sprintf("unknown overlay target %q (want fuse or symlink)", req.To)}
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

	budget := s.migrateBudget
	if budget <= 0 {
		budget = defaultMigrateBudget
	}
	deadline := time.Now().Add(budget)

	results := make([]MigrationResult, 0, len(accts))
	converted := false
	for _, a := range accts {
		if ctx.Err() != nil {
			break
		}
		if time.Now().After(deadline) {
			results = append(results, MigrationResult{
				ID: a.ID, Label: a.Label, From: a.OverlayKind, To: string(to),
				Outcome: MigrationBusy, Detail: "migrate window elapsed; re-run `ccp migrate`",
			})
			continue
		}
		res := s.convertAccount(ctx, a, to, req.Force)
		converted = converted || res.Outcome == MigrationDone
		results = append(results, res)
	}

	resp := Response{OK: true, Migrations: results}
	// Record the new-account default. Toward fuse, reaching here means the fuse
	// gate already passed (the holder probe proved this machine mounts), so adopt
	// fuse even when nothing needed converting — otherwise `ccp fuse enable` on a
	// fresh or already-fuse pool would leave the default on symlink. The symlink
	// retreat only flips the default when an account actually converted back.
	if to.IsFuse() || converted {
		if err := s.m.SetDefaultOverlayKind(to); err != nil {
			resp.OK = false
			resp.Error = fmt.Sprintf("recording %s as the new-account default failed: %v", to, err)
		}
	}
	return resp
}

// fuseGate reports why fuse mirrors cannot be hosted right now, or "" when
// they can. The probe runs in the mount holder deliberately: mount capability
// and the macOS volume-access grant are per-process, and the holder is
// the process that hosts the mounts — so a missing grant fails here, before
// any account is disturbed.
func (s *Server) fuseGate() (fkoverlay.Backend, string) {
	if s.fuseGateFn != nil {
		return s.fuseGateFn()
	}
	if !pool.CanHostFuse() {
		return "", "fuse is not available on this machine; run `ccp fuse enable` to install the fusekit-holder cask"
	}
	// The reason leads verbatim: a declined probe carries its own fuse-t/TCC
	// remedy, while holder spawn or probe-RPC failures name their real cause —
	// advice that fits one would mislead for the others.
	backend, reason := pool.DetectOverlayBackend()
	if !backend.IsFuse() {
		return "", fmt.Sprintf("fuse unavailable: %s — fix this, then re-run `ccp migrate`", reason)
	}
	return backend, ""
}

// convertAccount runs one gated conversion, mapping it to a wire outcome.
// force skips the live-session gate only: the user vouches the sessions are
// idle and accepts that one writing mid-conversion may briefly error. The
// claim and reservation gates always hold — those mean another part of the
// daemon owns the dir right now.
func (s *Server) convertAccount(ctx context.Context, a store.Account, to fkoverlay.Backend, force bool) MigrationResult {
	res := MigrationResult{ID: a.ID, Label: a.Label, From: a.OverlayKind, To: string(to)}
	if a.OverlayKind == string(to) {
		res.Outcome = MigrationAlready
		return res
	}
	if !s.beginConvert(a.ID) {
		res.Outcome = MigrationBusy
		res.Detail = "held by a pending select, a daemon poll, or a holder replacement; retry shortly"
		return res
	}
	defer s.endConvert(a.ID)

	// The caller's account list is a snapshot that ages while earlier
	// conversions run (seconds each); re-read the row now that the claim
	// makes it stable, so the kind decision and the conversion's writes are
	// against current state, not the snapshot.
	a, err := s.m.Store.GetAccount(a.ID)
	if err != nil {
		res.Outcome = MigrationFailed
		res.Detail = fmt.Sprintf("re-read account row: %v", err)
		return res
	}
	res.Label, res.From = a.Label, a.OverlayKind
	if a.OverlayKind == string(to) {
		res.Outcome = MigrationAlready
		return res
	}

	if !force {
		// Never convert blind: a failed scan means we cannot know whether a
		// live claude has this dir as its config dir.
		sessions, err := procscan.Scan(ctx)
		if err != nil {
			res.Outcome = MigrationFailed
			res.Detail = fmt.Sprintf("session scan: %v", err)
			return res
		}
		if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
			res.Outcome = MigrationBusy
			res.Detail = fmt.Sprintf("%d live session(s)", n)
			return res
		}
	}

	if _, err := s.m.ConvertOverlay(a, to); err != nil {
		res.Outcome = MigrationFailed
		res.Detail = err.Error()
		return res
	}
	res.Outcome = MigrationDone
	// Make the conversion's mount state visible to selection immediately — the
	// next cache refresh is up to a full poll away, and mountReady would
	// otherwise exclude every freshly-converted fuse account until then (or
	// keep counting a dismounted mirror in HolderStatus.Mounts after a
	// retreat).
	if to.IsFuse() {
		s.holder.noteMounted(a.ConfigDir)
	} else {
		s.holder.noteUnmounted(a.ConfigDir)
	}
	s.log.Printf("acct-%02d overlay migrated %s -> %s", a.ID, res.From, res.To)
	return res
}
