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

// startFPBridge binds the File Provider data-socket content.BridgeServer
// (startContentBridge's sibling, same PoolContentSource), waits a bounded
// fpBridgeWait, and flags fpConsentPending when the bind parks on the
// app-group-container TCC consent — see ccn doc f71e9b1.
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
	fpRepaired                   // Health failed; the idempotent Setup re-registered + re-linked
	fpRetry                      // transient control condition; retry next cycle
	fpRetreated                  // ErrCannotControl: permanently retreated to symlink
	fpDeferred                   // retreat refused (pending select or live session); retry next cycle
)

// reconcileFileProvider brings one File Provider row in line with its domain:
// Health, then on failure one idempotent Setup. Only fileproviderd.ErrCannotControl
// retreats the row to symlink; everything else retries next cycle. Callers hold
// the account's poll claim.
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

// retreatFPToSymlink converts a File Provider account to symlink after
// ErrCannotControl, with fallbackToSymlink's claim-first gating: never blind,
// never under a live session — defers instead. Callers hold the poll claim.
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
