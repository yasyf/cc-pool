package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
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
	case "fileprovider":
		backend, msg := s.fpGate(ctx)
		if msg != "" {
			return Response{OK: false, Error: msg}
		}
		to = backend
	case "symlink":
		to = fkoverlay.BackendSymlink
	default:
		return Response{OK: false, Error: fmt.Sprintf("unknown overlay target %q (want fuse, symlink, or fileprovider)", req.To)}
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

	// The budget clock starts here, AFTER the capability gate: fuseGate's
	// awaitHolderHealth already absorbed holder cold-start, so the first
	// account's mount doesn't pay it out of its conversion budget.
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
	// A passed fuse or fileprovider gate flips the new-account default even
	// with zero conversions, so a fresh pool doesn't stay on symlink.
	if to == fkoverlay.BackendFileProvider || to.IsFuse() || converted {
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
// A holder missing a required capability (pool.HolderMountFeatures) is refused
// via the feature handshake, naming the cask upgrade.
func (s *Server) fuseGate(ctx context.Context) (fkoverlay.Backend, string) {
	if s.fuseGateFn != nil {
		return s.fuseGateFn()
	}
	if !pool.CanHostFuse() {
		return "", "fuse is not available on this machine; run `ccp fuse enable` to install the fusekit-holder cask"
	}
	// Fuse-only detection: DetectOverlayBackend prefers File Provider when the
	// extension is enabled, and an FP verdict here would wrongly refuse fuse.
	backend, reason := pool.DetectFuseBackend(ctx)
	if !backend.IsFuse() {
		return "", fmt.Sprintf("fuse unavailable: %s — fix this, then re-run `ccp migrate`", reason)
	}
	// The bounded wait doubles as the migrate warm-up: Select may have just
	// spawned the holder, and riding out its socket bind here keeps cold-start
	// off the first account's conversion budget (handleMigrate computes the
	// budget deadline after this gate).
	ver, err := s.awaitHolderHealth(ctx)
	if err != nil {
		return "", fmt.Sprintf("fuse unavailable: mount holder health probe failed: %v — fix this, then re-run `ccp migrate`", err)
	}
	// The feature handshake replaces version arithmetic: a holder missing a
	// required capability (or a proto-mismatched one) is refused, naming the cask
	// upgrade in Require's own message.
	info, err := s.holderClient().Hello()
	if err != nil {
		return "", fmt.Sprintf("fuse unavailable: mount holder %s capability handshake failed: %v — run `brew upgrade --cask fusekit-holder`, then re-run `ccp migrate`", ver, err)
	}
	if ferr := info.Require(pool.HolderMountFeatures...); ferr != nil {
		return "", fmt.Sprintf("fuse unavailable: %v — then re-run `ccp migrate`", ferr)
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

// fpAvailable is a test seam over fkoverlay.FileProviderAvailable, whose
// pluginkit query cannot be scripted in tests.
var fpAvailable = fkoverlay.FileProviderAvailable

// fpCapabilityProbe is a test seam over the companion app's throwaway-domain
// capability probe (register+enumerate+remove a throwaway domain) — the truthful
// "can File Provider serve on this machine?" verdict, which a control Health
// ok:true ping is NOT. The production probe dials the app's control socket.
var fpCapabilityProbe = func(ctx context.Context, socket string) (bool, error) {
	return fileproviderd.NewAppClient(socket).Probe(ctx)
}

// fpGate reports why File Provider domains cannot be hosted, or "" when they
// can — fuseGate's analog for `ccp migrate --to fileprovider`. The pluginkit
// election (installed AND enabled) is the fast pre-check, but it only proves the
// extension is registered, not that it can serve: a control Health round-trip is a
// hard-coded ok:true socket ping an installed-but-unconsented provider also passes,
// so every conversion then fails slowly through Setup's readiness poll. The second
// rung is therefore the truthful throwaway-domain capability probe — register,
// enumerate, and remove a throwaway domain — which alone distinguishes "serving"
// from "enabled but not serving". Both must hold before any account is disturbed.
func (s *Server) fpGate(ctx context.Context) (fkoverlay.Backend, string) {
	spec := s.m.OverlaySpec()
	if !fpAvailable(spec) {
		return "", fmt.Sprintf("fileprovider unavailable: the %s extension is not installed and enabled — run `ccp fp onboard` to set it up end to end, then re-run `ccp migrate`", pool.FPExtensionBundleID)
	}
	switch ok, err := fpCapabilityProbe(ctx, spec.FileProvider.ControlSocket); {
	case err != nil:
		return "", fmt.Sprintf("fileprovider unavailable: companion app control probe failed: %v — start %s, then re-run `ccp migrate`", err, pool.WidgetAppPath())
	case !ok:
		return "", "fileprovider unavailable: extension enabled but not serving — approve it in System Settings or run `ccp fp onboard`"
	}
	return fkoverlay.BackendFileProvider, ""
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
	// Claim and re-read the row under it — the caller's list is a stale snapshot.
	fresh, ok, err := s.ownFresh(a.ID)
	if !ok {
		res.Outcome = MigrationBusy
		res.Detail = "held by a pending select, a daemon poll, or a holder replacement; retry shortly"
		return res
	}
	defer s.cl.disownConvert(a.ID)
	if err != nil {
		res.Outcome = MigrationFailed
		res.Detail = fmt.Sprintf("re-read account row: %v", err)
		return res
	}
	a = fresh
	res.Label, res.From = a.Label, a.OverlayKind
	if a.OverlayKind == string(to) {
		res.Outcome = MigrationAlready
		return res
	}

	from, ferr := fkoverlay.Parse(a.OverlayKind)
	if ferr != nil {
		res.Outcome = MigrationFailed
		res.Detail = fmt.Sprintf("parse source backend: %v", ferr)
		return res
	}
	// A non-fuse source (symlink/File Provider) has no holder mount, so ConvertOverlay
	// mutates the plain account dir directly — fence the whole conversion under an
	// exclusive session-lease seize so a live session or a select handout (invisible
	// to procscan before claude starts) defers it. A FUSE source is instead gated by
	// its holder-delegated Teardown returning mountd.ErrBusy (mapped below): seizing
	// the mux subtree here would self-bounce the holder's own Seize. --force keeps its
	// forensic-only meaning; the lease gate is not force-skippable (a live session's
	// mount cannot be safely torn down regardless).
	if !from.IsFuse() {
		fence, err := s.m.SeizeSessionLease(a)
		if err != nil {
			res.Outcome = MigrationBusy
			res.Detail = "held by a live session or a select handout; relaunch or close it, then retry"
			return res
		}
		defer func() { _ = fence.Release() }()
	}
	if !force {
		// Advisory forensics only (the lease seize / holder ErrBusy is the real
		// gate): a failed scan cannot rule out a live session in this dir.
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
		// A fuse source's holder-delegated teardown answers ErrBusy under a live
		// session's held lease: a graceful deferral, never a failure.
		if errors.Is(err, mountd.ErrBusy) {
			res.Outcome = MigrationBusy
			res.Detail = "a live session holds the mount's lease; relaunch or close it, then retry"
			return res
		}
		res.Outcome = MigrationFailed
		res.Detail = err.Error()
		return res
	}
	res.Outcome = MigrationDone
	// Forced past the live-session gate: record how many sessions the flip
	// happened under, the forensic line this incident's diagnosis leaned on.
	// Best-effort — a failed scan is not a gate here, so drop it silently.
	if force {
		if sessions, err := s.scan(ctx); err == nil {
			if n := procscan.CountByConfigDir(sessions, a.ConfigDir); n > 0 {
				res.Detail = fmt.Sprintf("converted under %d live session(s)", n)
			}
		}
	}
	// A conversion remakes the overlay, so any File Provider wedge/recovery state
	// for this dir is now stale — forget it (the row may be leaving OR entering FP).
	s.fpReset(a.ConfigDir)
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
