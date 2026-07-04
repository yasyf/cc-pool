package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

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

// startFPBridge binds the daemon's second content.BridgeServer on the File
// Provider data socket the sandboxed extension dials for computed content — the
// sibling of startContentBridge, sharing the SAME PoolContentSource instance
// (the Source contract requires concurrency safety, and the write-through mutex
// is package-level, so write-throughs serialize across both servers in this one
// process). The socket now lives in ~/.cc-pool (see pool.FPBridgeSocketPath),
// which the daemon owns and always exists, so — unlike a group-container socket
// behind macOS 15+ TCC — the bind is unconditional, exactly as startContentBridge
// binds the holder socket. Like its sibling it waits for the socket to accept,
// so an FP Setup's first enumeration finds the bridge up; unlike its sibling the
// serve loop RETRIES on abnormal exit (serveFPBridge), so a transient bind
// failure self-heals instead of leaving every FP domain dead until a daemon
// restart. The loop runs until ctx is cancelled, tracked by wg.
func (s *Server) startFPBridge(ctx context.Context) {
	sock := pool.FPBridgeSocketPath()
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		s.log.Printf("file provider bridge: create socket dir: %v", err)
		return
	}
	bridge := &content.BridgeServer{Socket: sock, Source: s.contentSource, Version: version.String(), Log: s.log}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serveFPBridge(ctx, bridge, sock)
	}()
	cl := content.NewBridgeClient(sock)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil || cl.Available() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.log.Printf("file provider bridge: socket %s did not come up within 3s; enumerations may defer until it does", sock)
}

// serveFPBridge runs the FP bridge and, unlike the holder content bridge,
// retries with a capped backoff: every FP domain's computed content depends on
// this bridge, so a transient bind failure — a restart race, a stale peer
// mid-teardown — must self-heal rather than wait out a daemon restart. It logs
// the actual Run error on every abnormal exit. A clean ctx-cancel shutdown (Run
// returns nil) exits the loop, and ctx cancellation also cuts the backoff sleep
// short so wg.Wait never blocks on a sleeping retry.
func (s *Server) serveFPBridge(ctx context.Context, bridge *content.BridgeServer, sock string) {
	backoff := s.fpBridgeBackoff
	if backoff <= 0 {
		backoff = defaultFPBridgeBackoff
	}
	for {
		err := bridge.Run(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
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
	fpRepaired                   // Health failed; the idempotent Setup re-registered + re-linked
	fpRetry                      // transient control condition; retry next cycle
	fpRetreated                  // ErrCannotControl: permanently retreated to symlink
	fpDeferred                   // retreat refused (pending select or live session); retry next cycle
)

// reconcileFileProvider brings one File Provider row in line with its domain:
// Health, and on failure one idempotent Setup (re-register the domain +
// re-lay the account-dir symlink). No breakers, carcass clearing, or
// force-unmounts — domains are OS-supervised and survive app and daemon death.
// Error dispatch follows the mountd.ErrContentUnavailable deferral precedent
// (healFuse): every transient control condition (ErrAppUnavailable /
// ErrRegisterFailed / ErrBusy / ErrNoDomain) retries next cycle; only
// fileproviderd.ErrCannotControl — the app provably cannot drive File Provider
// here — takes the one irreversible step, retreating the row to symlink.
// Used by the startup reconcile and the per-poll self-heal; callers hold the
// account's poll claim.
func (s *Server) reconcileFileProvider(ctx context.Context, a store.Account) fpOutcome {
	prov := s.overlayForRow(a)
	if prov == nil || prov.Backend() != fkoverlay.BackendFileProvider {
		s.log.Printf("no file provider resolved for acct-%02d; refusing to reconcile through it", a.ID)
		return fpRetry
	}
	base, dir := pool.ClaudeDir(), a.ConfigDir
	healthErr := prov.Health(base, dir)
	if healthErr == nil {
		return fpHealthy
	}
	switch err := prov.Setup(base, dir); {
	case err == nil:
		s.log.Printf("acct-%02d file provider domain repaired (health: %v)", a.ID, healthErr)
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

// retreatFPToSymlink converts a File Provider account to symlink after the
// companion app reported it cannot control File Provider — mirroring
// fallbackToSymlink's claim-first gating: beginConvertUnderPoll refuses over a
// pending select reservation, and once the converting claim is set tryReserve
// refuses for the whole conversion, so no select can land mid-retreat. Never
// converts blind (a failed scan cannot rule out a live claude on the dir) and
// never under a live session — defers instead. Callers must hold the account's
// poll claim.
func (s *Server) retreatFPToSymlink(ctx context.Context, a store.Account) bool {
	if !s.beginConvertUnderPoll(a.ID) {
		s.log.Printf("acct-%02d deferring file-provider→symlink retreat: reserved by a pending select or already converting", a.ID)
		return false
	}
	defer s.endConvert(a.ID)
	sessions, err := s.scan(ctx)
	if err != nil {
		s.log.Printf("acct-%02d deferring file-provider→symlink retreat: session scan: %v", a.ID, err)
		return false
	}
	if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
		s.log.Printf("acct-%02d deferring file-provider→symlink retreat: %d live session(s)", a.ID, n)
		return false
	}
	if _, err := s.m.ConvertOverlay(ctx, a, fkoverlay.BackendSymlink); err != nil {
		s.log.Printf("acct-%02d file-provider→symlink retreat: %v", a.ID, err)
		return false
	}
	s.log.Printf("acct-%02d fell back to symlink: File Provider cannot serve on this machine", a.ID)
	return true
}
