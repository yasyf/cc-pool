package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// defaultMigrateBudget keeps a migrate response inside handle()'s extended
// 140s conn deadline and the client's 150s timeout.
const defaultMigrateBudget = 120 * time.Second

// convertTimeout bounds one account's conversion, detached from the request
// ctx (context.WithoutCancel): mountd's per-op deadlines (25s mount, 17s
// unmount) already bound each holder RPC, so 60s covers a conversion plus a
// worst-case rollback while never letting one account hang the migrate loop.
const convertTimeout = 60 * time.Second

// handleMigrate converts accounts between overlay providers; only the daemon
// can gate conversions against its own select reservations.
func (s *Server) handleMigrate(ctx context.Context, req Request) Response {
	var to fkoverlay.Backend
	switch req.To {
	case "fuse":
		backend, msg := s.fuseGate(ctx)
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

	// The budget clock starts here, AFTER fuseGate: the gate's awaitHolderHealth
	// already absorbed holder cold-start, so the first account's mount doesn't
	// pay it out of its conversion budget.
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
	// Fuse flips the new-account default even with zero conversions, so a fresh
	// pool doesn't stay on symlink.
	if to.IsFuse() || converted {
		if err := s.m.SetDefaultOverlayKind(to); err != nil {
			resp.OK = false
			resp.Error = fmt.Sprintf("recording %s as the new-account default failed: %v", to, err)
		}
	}
	return resp
}

// fuseGate reports why fuse mirrors cannot be hosted, or "" when they can.
// The probe runs in the mount holder: the macOS volume-access grant is
// per-process, so a missing grant fails here before any account is disturbed.
// A holder older than pool.MinHolderVersion is refused — it predates the NFS
// kernel-panic mitigations, and hosting mirrors on it can panic macOS.
func (s *Server) fuseGate(ctx context.Context) (fkoverlay.Backend, string) {
	if s.fuseGateFn != nil {
		return s.fuseGateFn()
	}
	if !pool.CanHostFuse() {
		return "", "fuse is not available on this machine; run `ccp fuse enable` to install the fusekit-holder cask"
	}
	backend, reason := pool.DetectOverlayBackend(ctx)
	if !backend.IsFuse() {
		return "", fmt.Sprintf("fuse unavailable: %s — fix this, then re-run `ccp migrate`", reason)
	}
	// Fail safe: a holder whose version cannot be observed must not be assumed
	// to carry the kernel-panic mitigations, so a Health failure refuses fuse.
	// The bounded wait doubles as the migrate warm-up: Select may have just
	// spawned the holder, and riding out its socket bind here keeps cold-start
	// off the first account's conversion budget (handleMigrate computes the
	// budget deadline after this gate).
	ver, err := s.awaitHolderHealth(ctx)
	if err != nil {
		return "", fmt.Sprintf("fuse unavailable: mount holder health probe failed: %v — fix this, then re-run `ccp migrate`", err)
	}
	if !pool.HolderVersionMitigated(ver) {
		return "", fmt.Sprintf("fuse unavailable: mount holder %s predates the NFS kernel-panic mitigations (need %s or newer); run `brew upgrade --cask fusekit-holder`, then re-run `ccp migrate`", ver, pool.MinHolderVersion)
	}
	return backend, ""
}

// awaitHolderHealth waits for the shared mount holder to answer Health,
// returning its reported version. Select normally leaves the holder serving
// (EnsureRunning + probe), so the first try answers; the retry loop, bounded
// by the holder's own spawn allowance, rides out a freshly spawned holder
// still binding its socket. The last Health error is returned on timeout or
// cancellation so the caller's refusal names the real fault.
func (s *Server) awaitHolderHealth(ctx context.Context) (string, error) {
	deadline := time.Now().Add(mountd.DefaultSpawnTimeout)
	for {
		ver, err := s.holderClient().Health()
		if err == nil {
			return ver, nil
		}
		if time.Now().After(deadline) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// convertAccount runs one gated conversion. force skips only the live-session
// gate; claim/reservation gates always hold, and teardown stays graceful-only
// (a busy dir fails closed, ErrUnmountWedged) — force-unmounting a busy NFS
// mirror panics the kernel (nfs_vinvalbuf2).
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

	// Re-read under the claim: the caller's list is a stale snapshot.
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
		// Fail closed: a failed scan cannot rule out a live session in this dir.
		sessions, err := s.scan(ctx)
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

	// Detached from the request ctx: a daemon shutdown mid-conversion must not
	// abandon a half-converted account inside the strand window (private files
	// moved, row still symlink); the conversion finishes or rolls back under
	// its own bounded timeout instead.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), convertTimeout)
	defer cancel()
	if _, err := s.m.ConvertOverlay(cctx, a, to); err != nil {
		res.Outcome = MigrationFailed
		res.Detail = err.Error()
		return res
	}
	res.Outcome = MigrationDone
	// Sync the mount cache now: mountReady would exclude the fresh conversion
	// until the next poll.
	if to.IsFuse() {
		s.holder.noteMounted(a.ConfigDir)
	} else {
		s.holder.noteUnmounted(a.ConfigDir)
	}
	s.log.Printf("acct-%02d overlay migrated %s -> %s", a.ID, res.From, res.To)
	return res
}
