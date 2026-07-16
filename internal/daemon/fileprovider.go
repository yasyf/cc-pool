package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/appgroup"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

// groupContainerDir resolves the daemon's app-group container through
// -[NSFileManager containerURLForSecurityApplicationGroupIdentifier:] so the FP
// bridge binds at the prompt-free containerURL path, never a hand-built join. A
// package var so tests point the bridge at a fake container without a real
// entitlement.
var groupContainerDir = appgroup.GroupContainerDir

// fpBackedRow reports whether a stored overlay_kind names the File Provider
// backend. File Provider is a third backend category
// (BackendFileProvider.IsFuse() is false), never folded into the fuse arms.
// Like fuseBackedRow, an unparseable kind is corruption and reads false so the
// safe symlink paths handle the dir.
func fpBackedRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	if err != nil {
		return false
	}
	return b == fkoverlay.BackendFileProvider
}

// defaultFPBridgeBackoff is the delay before serveFPBridge re-binds after its
// serve loop exits abnormally (a restart race with a stale peer, say).
const defaultFPBridgeBackoff = 5 * time.Second

// defaultFPBridgeWait bounds startFPBridge's synchronous wait for the socket to
// accept before it flags the bind as parked on the group-container consent.
const defaultFPBridgeWait = 3 * time.Second

// fpConsentWatchInterval paces the consent watchdog's poll for a late bind.
const fpConsentWatchInterval = 250 * time.Millisecond

// startFPBridge resolves the app-group container, binds the File Provider
// data-socket content.BridgeServer (startContentBridge's sibling, same
// PoolContentSource) inside it, waits a bounded fpBridgeWait, and flags
// fpConsentPending when the bind parks on the app-group-container TCC consent.
// Prompt-free access comes from the signed CCPoolDaemon.app bundle (app-group
// entitlement + embedded Developer ID profile), so a release daemon never
// prompts. An unresolvable container (a bare, unbundled daemon) takes the
// hard-error path — never consent-pending, which is reserved for a live TCC
// denial of a container that DOES resolve.
func (s *Server) startFPBridge(ctx context.Context) {
	dir, err := groupContainerDir(pool.AppGroupID)
	if err != nil {
		s.fpBridgeHardErr.Store(true)
		s.log.Printf("file provider bridge: no app-group container for %s (%v); the bridge stays down — a release daemon binds it from the signed CCPoolDaemon.app bundle", pool.AppGroupID, err)
		return
	}
	sock := filepath.Join(dir, pool.FPBridgeSocketLeaf)
	s.fpBridgeSock.Store(&sock)
	bridge := &content.BridgeServer{Socket: sock, Source: s.contentSource, Version: version.String(), Log: s.log}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serveFPBridge(ctx, bridge, sock)
	}()
	wait := s.fpBridgeWait
	if wait <= 0 {
		wait = defaultFPBridgeWait
	}
	cl := content.NewBridgeClient(sock)
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil || cl.Available() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.fpBridgeHardErr.Load() {
		// A genuine bind failure is already logged with its cause by
		// serveFPBridge's retry loop; do not mislabel it as consent-pending.
		return
	}
	// Load-bearing: the VM app-group-noprompt oracle greps the verbatim
	// 'app-group-container consent' token (CONSENT_MARK) — keep it stable.
	s.log.Printf("file provider bridge: socket %s did not come up within %s — still awaiting the one-time app-group-container consent; a release daemon ships the signed CCPoolDaemon.app bundle (embedded Developer ID profile) and binds prompt-free, so this signals an unprofiled build — reinstall with `ccp service install`; enumerations defer until it is up", sock, wait)
	s.fpConsentPending.Store(true)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(fpConsentWatchInterval):
			}
			if cl.Available() {
				s.fpConsentPending.Store(false)
				s.log.Printf("file provider bridge: socket %s is up", sock)
				return
			}
		}
	}()
}

// serveFPBridge runs the FP bridge with capped-backoff retry; the MkdirAll is
// inside the loop because the container is the TCC-gated piece and a retry
// picks up a late approval — see ccn doc f71e9b1.
func (s *Server) serveFPBridge(ctx context.Context, bridge *content.BridgeServer, sock string) {
	backoff := s.fpBridgeBackoff
	if backoff <= 0 {
		backoff = defaultFPBridgeBackoff
	}
	for {
		err := os.MkdirAll(filepath.Dir(sock), 0o700)
		if err == nil {
			err = bridge.Run(ctx)
		}
		if err == nil || ctx.Err() != nil {
			return
		}
		// os.ErrPermission is the TCC denied/parked signature; anything else is
		// a hard bind failure that must not be reported as a consent prompt.
		s.fpBridgeHardErr.Store(!errors.Is(err, os.ErrPermission))
		s.log.Printf("file provider bridge: serve %s exited abnormally; retrying in %s: %v", sock, backoff, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// fpOutcome classifies one reconcileFileProvider attempt.
type fpOutcome int

const (
	fpHealthy   fpOutcome = iota // domain registered, bridge symlink intact
	fpRepaired                   // Check failed; Reconcile re-registered and re-linked
	fpRetry                      // transient control condition; retry next cycle
	fpRetreated                  // ErrCannotControl: permanently retreated to symlink
	fpDeferred                   // retreat refused, or the self-heal ladder owns the wedged domain; retry next cycle
)

// reconcileFileProvider brings one File Provider row in line with its domain:
// Check, then on failure one idempotent Reconcile. Only fileproviderd.ErrCannotControl
// retreats the row to symlink; everything else retries next cycle. Callers hold
// the account's poll claim.
func (s *Server) reconcileFileProvider(ctx context.Context, a store.Account) fpOutcome {
	prov := s.overlayForRow(a)
	if prov == nil || prov.Backend() != fkoverlay.BackendFileProvider {
		s.log.Printf("no file provider resolved for acct-%02d; refusing to reconcile through it", a.ID)
		return fpRetry
	}
	base, dir := pool.ClaudeDir(), a.ConfigDir
	// Defer to the self-heal ladder while it holds this domain wedged and is backing
	// off between recovery attempts: another Check+Reconcile would pile control ops
	// on a domain the ladder is already recovering — the reconcile storm (defect 3)
	// this gate removes.
	if s.fpWedged(dir) && !s.fpRecoveryDue(dir, time.Now()) {
		return fpDeferred
	}
	checkErr := prov.Check(ctx, base, dir)
	if checkErr == nil {
		return fpHealthy
	}
	if errors.Is(checkErr, fileproviderd.ErrAppUnavailable) {
		// A down app is not an unhealthy domain (it survives the app's death): defer on
		// it rather than pile a Reconcile onto a domain the app simply can't answer for now.
		s.log.Printf("acct-%02d file provider reconcile deferred: companion app unavailable: %v", a.ID, checkErr)
		return fpDeferred
	}
	switch err := prov.Reconcile(ctx, base, dir); {
	case err == nil:
		s.log.Printf("acct-%02d file provider domain repaired (check: %v)", a.ID, checkErr)
		return fpRepaired
	case errors.Is(err, fileproviderd.ErrCannotControl):
		s.log.Printf("acct-%02d file provider cannot serve on this machine (no entitlement or extension disabled); falling back to symlink: %v", a.ID, err)
		if s.retreatFPToSymlink(ctx, a) {
			return fpRetreated
		}
		return fpDeferred
	default:
		s.log.Printf("acct-%02d file provider reconcile deferred, retrying next cycle: %v", a.ID, err)
		return fpRetry
	}
}

// retreatFPToSymlink converts a File Provider account to symlink after
// ErrCannotControl, with fallbackToSymlink's claim-first gating: never blind,
// never under a live session — defers instead. Callers hold the poll claim.
func (s *Server) retreatFPToSymlink(ctx context.Context, a store.Account) bool {
	if !s.cl.ownHeld(a.ID) {
		s.log.Printf("acct-%02d deferring file-provider→symlink retreat: reserved by a pending select or already converting", a.ID)
		return false
	}
	defer s.cl.disownConvert(a.ID)
	if !s.convertFPToSymlinkHeld(ctx, a) {
		return false
	}
	s.log.Printf("acct-%02d fell back to symlink: File Provider cannot serve on this machine", a.ID)
	return true
}

// convertFPToSymlinkHeld is the shared File-Provider→symlink retreat body,
// live-session-gated (a live session's open fds break on domain removal) and
// clearing the row's wedge/recovery state on success. The session lease is the
// authoritative fence, including select handouts not yet visible to procscan.
// Caller holds the convert claim and logs the success reason.
func (s *Server) convertFPToSymlinkHeld(ctx context.Context, a store.Account) bool {
	// A File Provider source has no holder mount, so ConvertOverlay mutates the plain
	// account dir directly (a live session's open fds break on the domain removal):
	// fence it under an exclusive session-lease seize so a live session or a select
	// handout — invisible to procscan before claude starts — defers the retreat.
	fence, err := s.m.SeizeSessionLease(a)
	if err != nil {
		s.log.Printf("acct-%02d deferring file-provider→symlink retreat: %s is held by a live session or a select handout: %v", a.ID, a.ConfigDir, err)
		return false
	}
	defer func() { _ = fence.Release() }()
	if _, err := s.m.ConvertOverlay(ctx, a, fkoverlay.BackendSymlink); err != nil {
		s.log.Printf("acct-%02d file-provider→symlink retreat: %v", a.ID, err)
		return false
	}
	s.fpReset(a.ConfigDir)
	return true
}

// handleFPRepair re-registers wedged File Provider domains on demand — the manual
// lever for a domain the auto-heal ladder has parked (breaker tripped) or that an
// operator wants reset now. With req.Account set it repairs that one account
// regardless of its wedge verdict; without, it repairs every currently-wedged FP
// domain. It routes through the daemon rather than the CLI so no select hands a
// dir to a launching session mid-re-register (each repair runs under the convert
// claim). A non-File-Provider or unknown account is an op-level error.
func (s *Server) handleFPRepair(ctx context.Context, req Request) Response {
	// A non-retreat repair tears down live replica state; gate it on a serving
	// bridge (make no domain claims and no Teardown otherwise). Retreat needs no
	// bridge, so it stays un-gated.
	if !req.Retreat {
		if st := s.fpBridgeCheck(ctx); st.Verdict != FPBridgeServing {
			return Response{OK: false, Error: "cannot repair: " + st.Detail}
		}
	}
	fp, err := s.fpAccounts()
	if err != nil {
		return Response{OK: false, Error: err.Error()}
	}
	var targets []store.Account
	if req.Account != nil {
		var found *store.Account
		for i := range fp {
			if fp[i].ID == *req.Account {
				found = &fp[i]
				break
			}
		}
		if found == nil {
			if _, gerr := s.m.Store.GetAccount(*req.Account); gerr != nil {
				return Response{OK: false, Error: fmt.Sprintf("account %d not found", *req.Account)}
			}
			return Response{OK: false, Error: fmt.Sprintf("account %d is not a file provider account", *req.Account)}
		}
		targets = []store.Account{*found}
	} else {
		for _, a := range fp {
			if s.fpWedged(a.ConfigDir) {
				targets = append(targets, a)
			}
		}
	}
	results := make([]FPRepairResult, 0, len(targets))
	for _, a := range targets {
		if ctx.Err() != nil {
			break
		}
		results = append(results, s.repairFPDomain(ctx, a, req.Retreat))
	}
	return Response{OK: true, FPRepairs: results}
}

// repairFPDomain re-registers one File Provider domain (Teardown+Reconcile, the reset
// that discards fileproviderd's poisoned replica state) under a standalone convert
// claim, so no select hands the dir to a launching session mid-re-register. On a
// clean re-register it resets the domain's wedge/recovery state so the next probe
// re-verifies; ErrCannotControl retreats the row to symlink. Re-registration
// proceeds even under live sessions (a wedged domain already fails their reads). It
// does its own Teardown+Reconcile rather than reRegisterFP so a transient Reconcile failure
// is reported as FPRepairFailed, not silently as repaired. When retreat is true it
// takes the explicit-retreat path instead: the ONLY caller of convertFPToSymlinkHeld
// left, now that the heal breaker parks rather than auto-retreats.
func (s *Server) repairFPDomain(ctx context.Context, a store.Account, retreat bool) FPRepairResult {
	res := FPRepairResult{ID: a.ID, Label: a.Label}
	if !s.cl.own(a.ID) {
		res.Outcome = FPRepairBusy
		res.Detail = "held by a pending select, a daemon poll, or a conversion; retry shortly"
		return res
	}
	defer s.cl.disownConvert(a.ID)

	// Re-read under the claim: the caller's list is a stale snapshot.
	fresh, err := s.m.Store.GetAccount(a.ID)
	if err != nil {
		res.Outcome = FPRepairFailed
		res.Detail = fmt.Sprintf("re-read account row: %v", err)
		return res
	}
	res.Label = fresh.Label
	if !fpBackedRow(fresh.OverlayKind) {
		res.Outcome = FPRepairFailed
		res.Detail = fmt.Sprintf("converted off file provider (now %s) in the claim gap", fresh.OverlayKind)
		return res
	}
	if retreat {
		return s.retreatFPDomainHeld(ctx, fresh, res)
	}
	prov := s.fpProvider(fresh)
	if prov == nil {
		res.Outcome = FPRepairFailed
		res.Detail = "no file provider resolved for this account"
		return res
	}
	base, dir := pool.ClaudeDir(), fresh.ConfigDir
	warning, terr := prov.Teardown(ctx, base, dir)
	s.warnTeardown(fresh.ID, warning)
	if terr != nil {
		// A wedged domain may refuse a clean Teardown; Reconcile below
		// re-adds regardless, so log and press on rather than failing the repair.
		s.log.Printf("acct-%02d file provider repair: teardown: %v", fresh.ID, terr)
	}
	switch serr := prov.Reconcile(ctx, base, dir); {
	case serr == nil:
		s.fpReset(dir)
		s.log.Printf("acct-%02d file provider domain re-registered by `ccp fp repair`; the next probe verifies it — relaunch any sessions on it", fresh.ID)
		res.Outcome = FPRepairRepaired
		res.Detail = "re-registered; the next probe verifies it"
	case errors.Is(serr, fileproviderd.ErrCannotControl):
		if s.convertFPToSymlinkHeld(ctx, fresh) {
			res.Outcome = FPRepairRetreated
			res.Detail = "File Provider cannot serve on this machine; fell back to symlink"
		} else {
			res.Outcome = FPRepairFailed
			res.Detail = "File Provider cannot serve here and the symlink retreat is blocked by a live session — relaunch it, then re-run"
		}
	default:
		res.Outcome = FPRepairFailed
		res.Detail = fmt.Sprintf("re-register failed: %v", serr)
	}
	return res
}

// retreatFPDomainHeld is the explicit-retreat arm of `ccp fp repair --retreat`:
// it converts a File Provider row to the symlink floor at operator request — the
// ONLY path that reaches convertFPToSymlinkHeld now the heal breaker parks rather
// than auto-retreats a wedged-but-controllable domain. The caller holds the
// convert claim; the retreat stays live-session-gated (a live session's open fds
// break on the domain removal), so a blocked retreat reports FPRepairFailed with
// the relaunch guidance rather than tearing a domain out from under a session.
func (s *Server) retreatFPDomainHeld(ctx context.Context, fresh store.Account, res FPRepairResult) FPRepairResult {
	if s.convertFPToSymlinkHeld(ctx, fresh) {
		s.log.Printf("acct-%02d retreated to the symlink floor by `ccp fp repair --retreat`", fresh.ID)
		res.Outcome = FPRepairRetreated
		res.Detail = "retreated to the symlink floor at operator request"
		return res
	}
	res.Outcome = FPRepairFailed
	res.Detail = "symlink retreat is blocked by a live session or a select handout on it — relaunch or close it, then re-run"
	return res
}

// fpBridgeReady reports whether the File Provider data bridge is up, the one-time
// group-container consent has settled, and the fp.bridge row is not faulted — the
// precondition for probing FP domains, since a probe through a down or
// bound-but-dead bridge reads every domain as wedged. fpBridgeReadyFn is a test seam.
func (s *Server) fpBridgeReady() bool {
	if s.fpBridgeReadyFn != nil {
		return s.fpBridgeReadyFn()
	}
	sock := s.fpBridgeSock.Load()
	return sock != nil && !s.fpConsentPending.Load() && !s.fpBridgeFaulted() && content.NewBridgeClient(*sock).Available()
}

// fpBridgeUp reports whether the File Provider data socket accepts a connection,
// for the status wire's FPBridgeUp field. The daemon is the only process that
// dials the group-container bridge, so the CLI reads this off status instead of
// touching the socket. Unlike fpBridgeReady it ignores the consent flag — it is
// the raw socket-liveness fact the CLI renders. An unresolved container (bridge
// never started) reads false.
func (s *Server) fpBridgeUp() bool {
	sock := s.fpBridgeSock.Load()
	return sock != nil && content.NewBridgeClient(*sock).Available()
}
