package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fpCloudStorageDomains lists the account indices with a ~/Library/CloudStorage
// File Provider folder (FPDomainFolderPrefix + acct-NN); a missing dir is no
// error. A seam so tests never read the real CloudStorage dir.
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

// reportOrphanFPDomains flags ~/Library/CloudStorage File Provider folders that
// no pool account owns — a domain a failed add's rollback Remove never retracted
// (an orphan CCPoolStatus-acct-NN with no accounts row and no account dir). It is
// read-only unless fix, and removes ONLY a domain it can positively confirm is
// registered yet unowned; anything ambiguous (an in-flight add, a down app, an
// unknown scan state) is left untouched.
func reportOrphanFPDomains(ctx context.Context, m *pool.Manager, accts []store.Account, fix bool, report func(string, bool, string)) {
	if !fpAvailable(m.OverlaySpec()) {
		return
	}
	ids, err := fpCloudStorageDomains()
	if err != nil {
		report("file provider orphans", false, fmt.Sprintf(
			"couldn't scan %s: %v — leaving any folders untouched", abbreviateHome(pool.FPCloudStorageDir()), err))
		return
	}
	if len(ids) == 0 {
		return
	}
	rows := make(map[int]bool, len(accts))
	for _, a := range accts {
		rows[a.ID] = true
	}
	prov, err := fpOverlayProvider(fkoverlay.BackendFileProvider)
	if err != nil {
		return
	}
	registry, okReg := prov.(overlay.FPDomainRegistry)
	remover, okRem := prov.(overlay.FPDomainRemover)
	if !okReg || !okRem {
		return
	}
	for _, id := range ids {
		if rows[id] {
			continue
		}
		dir := pool.AccountDir(id)
		// A live account dir or its private backing root means an add is in flight
		// (between PrepareAdd and FinalizeAdd) or a kept resume owns the folder — not
		// an orphan. An unreadable path can't be confirmed absent, so it can't be
		// confirmed an orphan: surface it advisory and never remove (even with fix),
		// mirroring the scan/probe unknown-state handling.
		backed, err := fpCandidateBacked(dir)
		if err != nil {
			report(fmt.Sprintf("acct-%02d file provider", id), false, fmt.Sprintf(
				"can't confirm orphaned File Provider folder %s: %v — leaving it untouched (--fix never removes an unconfirmed domain)",
				abbreviateHome(fpOrphanFolder(id)), err))
			continue
		}
		if backed {
			continue
		}
		reconcileOrphanFPDomain(ctx, m, id, dir, registry, remover, fix, report)
	}
}

// fpCandidateBacked reports whether a candidate orphan's account dir or its private
// backing root exists on disk (an in-flight add or kept resume owns the folder). A
// stat error names the offending path so the caller can refuse to treat an
// unreadable path as absent.
func fpCandidateBacked(dir string) (bool, error) {
	for _, p := range []string{dir, fkoverlay.FusePrivateRoot(dir)} {
		exists, err := fpPathExists(p)
		if err != nil {
			return false, fmt.Errorf("stat %s: %w", abbreviateHome(p), err)
		}
		if exists {
			return true, nil
		}
	}
	return false, nil
}

// reconcileOrphanFPDomain classifies one unowned File Provider folder via the
// zero-spawn DomainRoot registration query and, only for a confirmed
// registration, reports it (and removes it with fix). An unregistered folder is
// silent; an inconclusive answer (app down, timeout) is advisory and never removed.
func reconcileOrphanFPDomain(ctx context.Context, m *pool.Manager, id int, dir string, registry overlay.FPDomainRegistry, remover overlay.FPDomainRemover, fix bool, report func(string, bool, string)) {
	probeCtx, cancel := context.WithTimeout(ctx, fpDomainProbeTimeout)
	root, err := registry.DomainRoot(probeCtx, dir)
	cancel()

	label := fmt.Sprintf("acct-%02d file provider", id)
	folder := abbreviateHome(fpOrphanFolder(id))
	switch {
	case errors.Is(err, fileproviderd.ErrNoDomain):
		return // a lingering folder with nothing registered — nothing to reconcile
	case err != nil:
		// App down or any inconclusive class: cannot confirm the domain is unowned, so
		// surface it advisory and NEVER remove (even with --fix).
		report(label, false, fmt.Sprintf(
			"orphaned File Provider folder %s exists but cc-pool can't confirm its domain is registered (%v); launch %s and re-run `ccp doctor` — --fix never removes an unconfirmed domain",
			folder, err, pool.WidgetAppPath()))
		return
	}
	if !fix {
		report(label, false, fmt.Sprintf(
			"orphaned File Provider domain %s (root %s) with no pool account — a failed add's rollback left it registered; re-run `ccp doctor --fix` to remove it",
			folder, abbreviateHome(root)))
		return
	}
	// Re-confirm against fresh state, adjacent to the remove: in the scan→now window a
	// concurrent add can reuse the freed index and adopt this domain, so a snapshot
	// verdict would deregister a live account's. (Racing the earlier reserve→seed gap is
	// harmless — fusekit Reconcile re-registers an absent domain — only this window matters.)
	if advisory, ok := reconfirmOrphanFPDomain(ctx, m, id, dir, registry); !ok {
		report(label, false, advisory)
		return
	}
	if err := remover.RemoveDomain(ctx, dir); err != nil {
		report(label, false, fmt.Sprintf("couldn't remove orphaned File Provider domain %s: %v", folder, err))
		return
	}
	report(label, true, fmt.Sprintf("removed orphaned File Provider domain %s", folder))
}

// reconfirmOrphanFPDomain re-runs the full orphan predicate against fresh state —
// a fresh accounts query (not the scan-time slice), the backing-dir existence
// checks, and the DomainRoot registration probe. It returns ok only when every
// check still confirms an unowned, registered, unbacked domain; any ownership,
// error, or inconclusive answer yields an advisory and ok=false so the caller
// leaves the domain untouched.
func reconfirmOrphanFPDomain(ctx context.Context, m *pool.Manager, id int, dir string, registry overlay.FPDomainRegistry) (advisory string, ok bool) {
	const untouched = "state changed while confirming — leaving the domain untouched"
	accts, err := m.Store.ListAccounts()
	if err != nil {
		return fmt.Sprintf("%s (couldn't re-read accounts: %v)", untouched, err), false
	}
	for _, a := range accts {
		if a.ID == id {
			return fmt.Sprintf("%s (a pool account now owns it)", untouched), false
		}
	}
	backed, err := fpCandidateBacked(dir)
	if err != nil {
		return fmt.Sprintf("%s (couldn't re-check its backing dirs: %v)", untouched, err), false
	}
	if backed {
		return fmt.Sprintf("%s (its backing dir now exists)", untouched), false
	}
	probeCtx, cancel := context.WithTimeout(ctx, fpDomainProbeTimeout)
	_, err = registry.DomainRoot(probeCtx, dir)
	cancel()
	if err != nil {
		return fmt.Sprintf("%s (can't re-confirm its registration: %v)", untouched, err), false
	}
	return "", true
}

// fpOrphanFolder is the ~/Library/CloudStorage folder path for an orphaned domain.
func fpOrphanFolder(id int) string {
	return filepath.Join(pool.FPCloudStorageDir(), pool.FPDomainFolderPrefix+pool.AccountDirName(id))
}

// fpPathExists reports whether path exists without following symlinks. A nil error
// distinguishes confirmed-absent (false) from present (true); any non-ENOENT stat
// error propagates so an unreadable path is never mistaken for absent.
func fpPathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
