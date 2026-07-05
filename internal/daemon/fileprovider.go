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

// defaultFPBridgeWait bounds startFPBridge's synchronous wait for the socket to
// accept before it flags the bind as parked on the group-container consent.
const defaultFPBridgeWait = 3 * time.Second

// fpConsentWatchInterval paces the consent watchdog's poll for a late bind.
const fpConsentWatchInterval = 250 * time.Millisecond

// startFPBridge binds the daemon's second content.BridgeServer on the File
// Provider data socket the sandboxed extension dials for computed content — the
// sibling of startContentBridge, sharing the SAME PoolContentSource instance
// (the Source contract requires concurrency safety, and the write-through mutex
// is package-level, so write-throughs serialize across both servers in this one
// process). The socket lives in the App Group container (pool.FPBridgeSocketPath)
// — the one location both the sandboxed appex and the daemon may touch — which
// macOS gates behind a TCC consent keyed to the daemon's cdhash, so the first
// bind after every install or upgrade can park on a prompt launchd never
// surfaces. The container MkdirAll therefore runs inside the tracked serve loop
// (serveFPBridge), which retries abnormal exits, and startFPBridge waits only a
// bounded fpBridgeWait for the socket to accept (so an FP Setup's first
// enumeration finds the bridge up); if it doesn't come up in time while the
// daemon is alive, that consent-pending signature is flagged on fpConsentPending
// for status/doctor and a watchdog clears it the moment the socket lands. All
// goroutines run until ctx is cancelled, tracked by wg.
func (s *Server) startFPBridge(ctx context.Context) {
	sock := pool.FPBridgeSocketPath()
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
	s.log.Printf("file provider bridge: socket %s did not come up within %s — likely awaiting the one-time app-group-container consent (approve it, then restart the daemon); enumerations defer until it is up", sock, wait)
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

// serveFPBridge runs the FP bridge and, unlike the holder content bridge,
// retries with a capped backoff: every FP domain's computed content depends on
// this bridge, so a transient failure — a restart race, a stale peer
// mid-teardown, the group-container MkdirAll blocked pending TCC consent — must
// self-heal rather than wait out a daemon restart. The MkdirAll lives inside
// the loop because the container is the TCC-gated piece: it can hang on the
// consent prompt (which is why it runs here, off startFPBridge's caller) or
// fail while consent is denied, and retrying picks up a late approval. It logs
// the actual error on every abnormal exit. A clean ctx-cancel shutdown (Run
// returns nil) exits the loop, and ctx cancellation also cuts the backoff sleep
// short so wg.Wait never blocks on a sleeping retry.
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
		// A permission error is the TCC denied/parked signature (keep it eligible
		// for the consent-pending signal); anything else is a genuine bind
		// failure that must not be reported to the user as a consent prompt.
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
