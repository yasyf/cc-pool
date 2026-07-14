package daemon

import (
	"context"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpLeakSweepTimeout bounds the zero-spawn domain-registration check the leak
// sweep runs per symlink row. State is a socket round-trip to the companion app
// (never a spawn, never a through-domain read): a registered or unregistered
// domain answers fast; a down app answers ErrAppUnavailable.
const fpLeakSweepTimeout = 3 * time.Second

// dirIsOverlaySymlink reports whether dir is a symlink (a mux bridge, a File
// Provider domain bridge, or any overlay stand-in) via Lstat, which never follows
// the link and so cannot hang on a wedged mount or a materializing domain.
func (s *Server) dirIsOverlaySymlink(dir string) bool {
	kind, _ := pool.ClassifyAccountDir(dir)
	return kind != pool.DirReal && kind != pool.DirAbsent
}

// convergeSymlinkRowBridge retracts a File Provider domain a crashed
// symlink→fileprovider conversion left bridged at a symlink row's account dir
// (Setup done, row not flipped), then recreates the real account dir so
// HealStrandedPrivate can move the private files back. Reports whether the dir is
// ready for healing; on any failure it logs and returns false so the caller skips
// this account until the next tick. Caller holds the account's poll claim.
func (s *Server) convergeSymlinkRowBridge(a store.Account) bool {
	target, _ := os.Readlink(a.ConfigDir)
	s.log.Printf("acct-%02d: symlink row but the dir is an overlay bridge symlink -> %s (a conversion died after Setup, before the row flip); retracting the domain and recreating the dir", a.ID, target)
	prov := s.overlayFor(fkoverlay.BackendFileProvider)
	if prov == nil {
		s.log.Printf("acct-%02d: cannot resolve the file provider to retract the leaked bridge; skipping this tick", a.ID)
		return false
	}
	// Idempotent: Teardown removes the bridge symlink and deregisters the domain
	// (deregistering a never-registered domain is a no-op).
	warning, err := prov.Teardown(pool.ClaudeDir(), a.ConfigDir)
	s.warnTeardown(a.ID, warning)
	if err != nil {
		s.log.Printf("acct-%02d: retract leaked overlay bridge: %v", a.ID, err)
		return false
	}
	if err := os.MkdirAll(a.ConfigDir, 0o700); err != nil {
		s.log.Printf("acct-%02d: recreate account dir after retracting the leaked bridge: %v", a.ID, err)
		return false
	}
	s.fpReset(a.ConfigDir)
	return true
}

// healStrandedRows runs on the heal ticker. It converges every symlink row whose
// dir is a leaked overlay bridge (a conversion that laid the bridge but crashed
// before flipping the row) and restores its stranded private files — the
// crash-window treatment promoted off the startup-only reconcile — then sweeps any
// File Provider domain still registered against a symlink row, deregistering the
// leaks retractFileProviderIfLaid's old real-dir arm and pre-fix retreats left
// behind (this clears yasyf-home's leaked domains automatically once it is back).
func (s *Server) healStrandedRows(ctx context.Context, t *tick) {
	// claim=true: forEach yields only symlink rows (fuse/FP rows keep their private
	// root in active use and reconcile through their own arms) and wraps each in the
	// poll claim — skip-don't-race if the scheduler or a conversion owns the dir.
	s.forEach(ctx, symlinkRows, true, func(a store.Account) {
		s.healStrandedSymlinkRow(ctx, t, a)
	})
}

// healStrandedSymlinkRow gives one symlink row the crash-window treatment (retract
// a leaked bridge, restore stranded private files) and sweeps a leaked File
// Provider domain still registered against it. Caller holds the account's poll
// claim. It re-reads the row under that claim first: a conversion can complete
// between the heal listing and this claim (the poll claim only blocks a NEW
// conversion), flipping the row to File Provider — whose dir is legitimately a
// domain bridge symlink — so the stale symlink snapshot must not drive the
// destructive retract/sweep. The heal then runs only under the convert claim and
// with no live session on the dir (beginSymlinkHealHeld), since beginPoll does not
// hide the dir from a select landing on the crash-window bridge.
func (s *Server) healStrandedSymlinkRow(ctx context.Context, t *tick, a store.Account) {
	fresh, err := s.m.Store.GetAccount(a.ID)
	if err != nil {
		s.log.Printf("acct-%02d stranded-row heal: re-read row: %v", a.ID, err)
		return
	}
	if fpBackedRow(fresh.OverlayKind) || fuseBackedRow(fresh.OverlayKind) {
		return // converted off symlink under the aging listing; its dir is a legit bridge/mount
	}
	bridged := s.dirIsOverlaySymlink(fresh.ConfigDir)
	stranded, strandedErr := fkoverlay.HasPrivateEntries(fkoverlay.FusePrivateRoot(fresh.ConfigDir), s.m.OverlaySpec())
	_, _, leaked, leakedErr := s.probeLeakedFPDomain(ctx, fresh)
	if !bridged && !stranded && strandedErr == nil && !leaked && leakedErr == nil {
		return
	}
	if !s.beginSymlinkHealHeld(t, fresh) {
		return
	}
	defer s.cl.disownConvert(fresh.ID)

	// This heal retracts a leaked File Provider domain and recreates the account dir —
	// a LOCAL destructive op cc-pool performs itself on the plain symlink dir (not a
	// holder-delegated teardown), so fence it under the row's session-lease key. An
	// `ccp env --account` that acquires the lease after the poll's advisory
	// live-session scan would otherwise have its domain removed underneath it; a held
	// lease defers the heal to a later tick.
	fence, err := s.m.SeizeSessionLease(fresh)
	if err != nil {
		s.log.Printf("acct-%02d deferring stranded-row heal: session lease held by a live handout (%v)", fresh.ID, err)
		return
	}
	defer func() { _ = fence.Release() }()

	if bridged {
		if !s.convergeSymlinkRowBridge(fresh) {
			return
		}
	}
	switch healed, err := s.m.HealStrandedPrivate(fresh); {
	case err != nil:
		s.log.Printf("acct-%02d heal stranded private files: %v", fresh.ID, err)
		return
	case healed:
		s.log.Printf("acct-%02d restored private files stranded by an interrupted migration", fresh.ID)
	}
	s.sweepLeakedFPDomain(ctx, fresh)
}

// beginSymlinkHealHeld claims a symlink row for a destructive strand heal — retract
// a leaked domain bridge, restore stranded private files, sweep a leaked domain —
// and confirms no live claude sits on its dir. The crash-window bridge may still
// back a running session, and beginPoll does not hide the dir from select, so a new
// `ccp run` can land on it mid-tick; the convert claim (tryReserve refuses while it
// is held) plus the live-session scan close both windows, the discipline
// convertFPToSymlinkHeld uses. Caller holds the account's poll claim. Returns true
// with the convert claim HELD (caller must endConvert); false, claim released, to
// skip this tick — a pending select, a live session, or a scan failure (which
// cannot rule a session out). Live-session liveness reads the shared heal tick,
// whose scan-fail-means-busy rule (tick.idle) defers a destructive retract; a
// stranded symlink row carries no fuse mount, so the shared pass scan suffices,
// and the ownHeld/reserve gate remains the primary race close.
func (s *Server) beginSymlinkHealHeld(t *tick, a store.Account) bool {
	if !s.cl.ownHeld(a.ID) {
		s.log.Printf("acct-%02d deferring stranded-bridge heal: reserved by a pending select", a.ID)
		return false
	}
	if !t.idle(a.ConfigDir) {
		if n := t.sessionCount(a.ConfigDir); n > 0 {
			s.log.Printf("acct-%02d deferring stranded-bridge heal: %d live session(s)", a.ID, n)
		}
		s.cl.disownConvert(a.ID)
		return false
	}
	return true
}

// probeLeakedFPDomain checks whether a File Provider domain is still registered
// against a symlink row without spawning the companion app.
func (s *Server) probeLeakedFPDomain(ctx context.Context, a store.Account) (overlay.FPDomainRemover, string, bool, error) {
	if !s.fpEnabled() || !s.fpBridgeReady() {
		return nil, "", false, nil
	}
	resolve := pool.OverlayProviderFor
	if s.m.OverlayFor != nil {
		resolve = s.m.OverlayFor
	}
	prov, err := resolve(fkoverlay.BackendFileProvider)
	if err != nil {
		return nil, "", false, err
	}
	registry, ok := prov.(overlay.FPDomainRegistry)
	remover, isRemover := prov.(overlay.FPDomainRemover)
	if !ok || !isRemover {
		return nil, "", false, nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, fpLeakSweepTimeout)
	root, err := registry.DomainRoot(probeCtx, a.ConfigDir)
	cancel()
	if err != nil {
		return nil, "", false, nil // ErrNoDomain (no leak) or ErrAppUnavailable (unknown): nothing to sweep
	}
	return remover, root, true, nil
}

// sweepLeakedFPDomain deregisters a File Provider domain still registered against
// a symlink row. Caller holds the account's poll claim.
func (s *Server) sweepLeakedFPDomain(ctx context.Context, a store.Account) {
	remover, root, leaked, err := s.probeLeakedFPDomain(ctx, a)
	if err != nil {
		s.log.Printf("resolve overlay provider for backend %q: %v", fkoverlay.BackendFileProvider, err)
		return
	}
	if !leaked {
		return
	}
	s.log.Printf("acct-%02d symlink row still has a file provider domain registered (root %s); deregistering the leak", a.ID, root)
	if err := remover.RemoveDomain(a.ConfigDir); err != nil {
		s.log.Printf("acct-%02d deregister leaked file provider domain: %v", a.ID, err)
	}
}
