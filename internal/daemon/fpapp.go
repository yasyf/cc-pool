package daemon

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/fileproviderd"
)

// fpAppPolicy backs the fp.app.ensure row: a fixed-window backoff with no
// debounce and no breaker, keyed by the single fpAppResource — there is exactly
// one CCPoolStatus companion app for the whole pool.
var fpAppPolicy = policies["fp.app"]

// fpAppResource is the fp.app ledger's single resource key.
const fpAppResource = "app"

// fpAppSpawnTimeout mirrors the overlay FileProviderSpec.SpawnTimeout
// (pool/overlay.go): File Provider bring-up is heavier than a Go child, so the
// wait for the freshly launched app's control socket is generous.
const fpAppSpawnTimeout = 30 * time.Second

// fpAppAvailable reports whether the companion app serves its control socket
// (it is alive). Zero-spawn — a plain socket dial. Test seam.
var fpAppAvailable = func() bool {
	return fileproviderd.NewAppClient(pool.FPControlSocketPath()).Available()
}

// fpAppSpawn launches CCPoolStatus.app exactly the way the File Provider
// bring-up does — fusekit's AppSpawn: `open -g` then wait for the control
// socket — reusing the overlay's app path, control socket, and spawn budget.
// Blocks up to fpAppSpawnTimeout, so it runs only inside the tracked ensure
// goroutine. Test seam.
var fpAppSpawn = func(ctx context.Context) error {
	return fileproviderd.AppSpawn{
		AppPath:       pool.WidgetAppPath(),
		ControlSocket: pool.FPControlSocketPath(),
		Timeout:       fpAppSpawnTimeout,
	}.EnsureRunning(ctx)
}

// fpCloudStorageDomains lists the pool account indices with a live
// ~/Library/CloudStorage File Provider domain root (FPDomainFolderPrefix +
// acct-NN) — the OS artifact of a registered domain, readable with the app down
// (zero-spawn). A missing dir is no error. Test seam, and the shared candidate
// source for the fp.app.ensure wanted gate and the fp.orphan.reap sweep.
var fpCloudStorageDomains = func() ([]int, error) {
	entries, err := os.ReadDir(pool.FPCloudStorageDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []int
	for _, e := range entries {
		if id, ok := pool.ParseFPDomainFolder(e.Name()); ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// shouldEnsureFPApp is the fp.app.ensure row gate. Each earlier condition
// short-circuits: File Provider must be wired on this host (fpEnabled, false in
// bare test servers and on a machine that never opted into FP); the
// group-container consent must have settled (a fresh app enumerating against a
// down bridge reproduces the incident churn); File Provider must be in play — at
// least one FP row or one CloudStorage domain artifact (the artifact arm covers
// the zero-row incident shape and gives the orphan reap a live app to confirm
// against); the app must not already be serving its control socket (a live, even
// post-upgrade-stale, app is left alone); and the fp.app backoff must have
// elapsed.
func (s *Server) shouldEnsureFPApp() bool {
	if !s.fpEnabled() || s.fpConsentPending.Load() {
		return false
	}
	if !s.fpAppWanted() {
		return false
	}
	if fpAppAvailable() {
		return false
	}
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	return s.led.due(fpAppPolicy, fpAppResource, time.Now())
}

// fpAppWanted reports whether File Provider is in play: at least one FP-backed
// row, else at least one CloudStorage domain artifact. On any listing error it
// logs and returns false — a listing failure never launches a GUI app on a guess.
func (s *Server) fpAppWanted() bool {
	rows, err := s.fpAccounts()
	if err != nil {
		s.log.Printf("fp app ensure: list file provider rows: %v", err)
		return false
	}
	if len(rows) > 0 {
		return true
	}
	ids, err := fpCloudStorageDomains()
	if err != nil {
		s.log.Printf("fp app ensure: list cloud storage domains: %v", err)
		return false
	}
	return len(ids) > 0
}

// ensureFPAppAsync launches the companion app in a tracked, fire-and-forget
// goroutine: AppSpawn.EnsureRunning blocks up to fpAppSpawnTimeout, so the heal
// tick and the startup pass must never stall on it, yet shutdown still waits on
// s.wg. The fp.app backoff is booked BEFORE the launch, so a later tick's gate
// stays closed for fpAppEnsureBackoff — bounding a crash-looping app to one loud
// launch per window and fencing out a second launch while this one is in flight.
// The row gate (shouldEnsureFPApp) has already established the app is wanted and
// down; this only acts. Distinct from the widget-appex reaper (reconcileStaleWidget
// kills the CCPoolStatusWidget appex) and the FP-appex bouncer (fpAppexBounce
// kills the CCPoolFileProvider extension): those kill extensions, this launches
// the host app.
func (s *Server) ensureFPAppAsync(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	s.bookFPAppEnsure(time.Now())
	s.log.Printf("file provider companion app is down; launching %s in the background", pool.WidgetAppPath())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if ctx.Err() != nil {
			return
		}
		if err := fpAppSpawn(ctx); err != nil {
			s.log.Printf("launch file provider companion app: %v", err)
			return
		}
		s.log.Printf("file provider companion app is up; file provider probe coverage resumes")
	}()
}

// bookFPAppEnsure stamps the fp.app backoff clock so the next ensure holds off
// for fpAppEnsureBackoff.
func (s *Server) bookFPAppEnsure(now time.Time) {
	s.ledMu.Lock()
	defer s.ledMu.Unlock()
	s.led.attempt(fpAppPolicy, fpAppResource, attemptPrimary, now)
}
