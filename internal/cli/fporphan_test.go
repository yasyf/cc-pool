package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// orphanFP is a File Provider provider whose zero-spawn DomainRoot registration
// query and RemoveDomain are scriptable, so orphan reconciliation never registers
// or removes a real domain.
type orphanFP struct {
	fakeFPProvider
	rootErr    error // DomainRoot error; nil means registered
	root       string
	roots      int // DomainRoot call count
	removed    int // RemoveDomain call count
	removeErr  error
	rootDirs   []string // accountDir of every DomainRoot call, in order
	removeDirs []string // accountDir of every RemoveDomain call, in order
	rootHook   func()   // side effect run inside DomainRoot, to mutate state mid-confirmation
}

func (f *orphanFP) DomainRoot(_ context.Context, accountDir string) (string, error) {
	f.roots++
	f.rootDirs = append(f.rootDirs, accountDir)
	if f.rootHook != nil {
		f.rootHook()
	}
	if f.rootErr != nil {
		return "", f.rootErr
	}
	return f.root, nil
}

func (f *orphanFP) RemoveDomain(accountDir string) error {
	f.removed++
	f.removeDirs = append(f.removeDirs, accountDir)
	return f.removeErr
}

// setupOrphan wires a temp HOME plus the File-Provider-available and CloudStorage
// scan seams so reportOrphanFPDomains never reads real state.
func setupOrphan(t *testing.T, ids []int, scanErr error) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	swapVar(t, &fpAvailable, func(fkoverlay.Spec) bool { return true })
	swapVar(t, &fpCloudStorageDomains, func() ([]int, error) { return ids, scanErr })
}

// orphanManager builds a store-backed Manager seeded with the given owned account
// ids. The removal path re-queries this store fresh right before RemoveDomain, so
// the --fix cases need a live store, not a zero Manager.
func orphanManager(t *testing.T, ownedIDs ...int) *pool.Manager {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "orphan.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, id := range ownedIDs {
		row := store.Account{ID: id, ConfigDir: pool.AccountDir(id), KeychainService: fmt.Sprintf("svc-%02d", id)}
		if err := st.UpsertAccount(row); err != nil {
			t.Fatal(err)
		}
	}
	return &pool.Manager{Store: st}
}

func TestReportOrphanFPDomains(t *testing.T) {
	t.Run("confirmed orphan is reported without --fix and never removed", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, false, report)

		if len(*calls) != 1 || (*calls)[0].healthy {
			t.Fatalf("want one unhealthy report, got %+v", *calls)
		}
		if !strings.Contains((*calls)[0].label, "acct-14 file provider") || !strings.Contains((*calls)[0].detail, "--fix") {
			t.Errorf("report = %+v, want acct-14 flagged with the --fix pointer", (*calls)[0])
		}
		if want := []string{pool.AccountDir(14)}; !reflect.DeepEqual(fake.rootDirs, want) {
			t.Errorf("DomainRoot probed %v, want exactly %v (the scanned account's dir)", fake.rootDirs, want)
		}
		if fake.removed != 0 {
			t.Errorf("RemoveDomain called %d times without --fix, want 0", fake.removed)
		}
	})

	t.Run("with --fix the orphan is removed exactly once and reported healthy", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), orphanManager(t), nil, true, report)

		if fake.removed != 1 {
			t.Fatalf("RemoveDomain called %d times, want exactly 1", fake.removed)
		}
		if want := []string{pool.AccountDir(14)}; !reflect.DeepEqual(fake.removeDirs, want) {
			t.Errorf("RemoveDomain targeted %v, want exactly %v — no other account dir may be removed", fake.removeDirs, want)
		}
		// One initial probe plus the removal-time re-confirmation probe.
		if want := []string{pool.AccountDir(14), pool.AccountDir(14)}; !reflect.DeepEqual(fake.rootDirs, want) {
			t.Errorf("DomainRoot probed %v, want %v (initial + re-confirm)", fake.rootDirs, want)
		}
		if len(*calls) != 1 || !(*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "removed") {
			t.Errorf("want one healthy 'removed' report, got %+v", *calls)
		}
	})

	t.Run("an existing account dir is untouched", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		if err := os.MkdirAll(pool.AccountDir(14), 0o700); err != nil {
			t.Fatal(err)
		}
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, true, report)

		if len(*calls) != 0 {
			t.Errorf("an in-flight/kept account dir must be untouched, got %+v", *calls)
		}
		if fake.roots != 0 || fake.removed != 0 {
			t.Errorf("probed=%d removed=%d, want 0/0 (skip before any probe)", fake.roots, fake.removed)
		}
	})

	t.Run("an existing private backing dir is untouched", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		if err := os.MkdirAll(fkoverlay.FusePrivateRoot(pool.AccountDir(14)), 0o700); err != nil {
			t.Fatal(err)
		}
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, true, report)

		if len(*calls) != 0 || fake.roots != 0 || fake.removed != 0 {
			t.Errorf("a private backing dir (in-flight add) must be untouched: reports=%+v probed=%d removed=%d", *calls, fake.roots, fake.removed)
		}
	})

	t.Run("a folder with a live account row is skipped", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, []store.Account{{ID: 14}}, true, report)

		if len(*calls) != 0 || fake.roots != 0 || fake.removed != 0 {
			t.Errorf("an owned folder must be skipped: reports=%+v probed=%d removed=%d", *calls, fake.roots, fake.removed)
		}
	})

	t.Run("an unregistered folder (ErrNoDomain) is silent", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{rootErr: fmt.Errorf("state: %w", fileproviderd.ErrNoDomain)}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, true, report)

		if len(*calls) != 0 {
			t.Errorf("a lingering unregistered folder must be silent, got %+v", *calls)
		}
		if fake.removed != 0 {
			t.Errorf("RemoveDomain called %d times, want 0 (nothing registered)", fake.removed)
		}
	})

	t.Run("a down app (ErrAppUnavailable) is advisory only, never removed even with --fix", func(t *testing.T) {
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{rootErr: fmt.Errorf("state: %w", fileproviderd.ErrAppUnavailable)}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, true, report)

		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "can't confirm") {
			t.Fatalf("want one advisory report, got %+v", *calls)
		}
		if fake.removed != 0 {
			t.Errorf("RemoveDomain called %d times, want 0 (unconfirmed → never remove)", fake.removed)
		}
	})

	t.Run("a scan error aborts and removes nothing", func(t *testing.T) {
		setupOrphan(t, nil, errors.New("permission denied"))
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, true, report)

		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "couldn't scan") {
			t.Fatalf("want one unhealthy scan-error report, got %+v", *calls)
		}
		if fake.roots != 0 || fake.removed != 0 {
			t.Errorf("unknown scan state must touch nothing: probed=%d removed=%d", fake.roots, fake.removed)
		}
	})

	t.Run("with a non-first orphan among owned folders, only its dir is probed and removed", func(t *testing.T) {
		// 13 and 15 are owned; 14 is the sole orphan. A hardcoded or off-by-one index
		// would target 13 or 15 instead of 14, so pin the exact dir and prove the
		// owned dirs are never probed or removed.
		setupOrphan(t, []int{13, 14, 15}, nil)
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), orphanManager(t, 13, 15), []store.Account{{ID: 13}, {ID: 15}}, true, report)

		// 14 is probed twice (initial + re-confirm); it is removed once.
		if want := []string{pool.AccountDir(14), pool.AccountDir(14)}; !reflect.DeepEqual(fake.rootDirs, want) {
			t.Errorf("DomainRoot probed %v, want exactly %v", fake.rootDirs, want)
		}
		if want := []string{pool.AccountDir(14)}; !reflect.DeepEqual(fake.removeDirs, want) {
			t.Errorf("RemoveDomain targeted %v, want exactly %v", fake.removeDirs, want)
		}
		for _, other := range []string{pool.AccountDir(13), pool.AccountDir(15)} {
			if slices.Contains(fake.rootDirs, other) || slices.Contains(fake.removeDirs, other) {
				t.Errorf("an owned account dir %q was probed/removed: probed=%v removed=%v", other, fake.rootDirs, fake.removeDirs)
			}
		}
		if len(*calls) != 1 || !(*calls)[0].healthy || !strings.Contains((*calls)[0].label, "acct-14 file provider") {
			t.Errorf("want one healthy acct-14 report, got %+v", *calls)
		}
	})

	t.Run("an unreadable candidate dir is advisory only and never probed or removed", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses directory permissions; cannot provoke EACCES")
		}
		setupOrphan(t, []int{14}, nil)
		// A dir whose parent is unreadable makes Lstat fail with EACCES, not ENOENT —
		// which must never be read as "absent" and thus a confirmed orphan.
		parent := filepath.Dir(pool.AccountDir(14))
		if err := os.MkdirAll(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(parent, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(parent, 0o700) }) //nolint:gosec // G302: restoring a test dir to traversable perms in cleanup
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), &pool.Manager{}, nil, true, report)

		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "can't confirm") {
			t.Fatalf("want one advisory report for the unreadable candidate, got %+v", *calls)
		}
		if fake.roots != 0 || fake.removed != 0 {
			t.Errorf("an unconfirmable candidate must never be probed or removed: probed=%d removed=%d", fake.roots, fake.removed)
		}
	})

	t.Run("a row that appears after the scan aborts the removal", func(t *testing.T) {
		// The scan-time slice (nil) has no account 14, so it reaches the removal path;
		// the store already carries the row a racing add finalized. The re-confirmation
		// must catch it and never deregister the now-live account's domain.
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{root: "/cloud/acct-14"}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), orphanManager(t, 14), nil, true, report)

		if fake.removed != 0 {
			t.Fatalf("RemoveDomain called %d times, want 0 — a re-owned domain must never be removed", fake.removed)
		}
		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "state changed while confirming") {
			t.Fatalf("want one advisory 'state changed' report, got %+v", *calls)
		}
		// The store check fails first, before the re-confirm re-probes DomainRoot.
		if fake.roots != 1 {
			t.Errorf("DomainRoot probed %d times, want 1 (only the initial scan-time probe)", fake.roots)
		}
	})

	t.Run("a backing dir that appears after the scan aborts the removal", func(t *testing.T) {
		// The dir is absent through the initial candidate check, then a racing add seeds
		// it during the confirmation window (modeled by the first DomainRoot probe's
		// side effect). The re-confirmation's backing-dir check must catch it.
		setupOrphan(t, []int{14}, nil)
		fake := &orphanFP{root: "/cloud/acct-14"}
		fake.rootHook = func() {
			if err := os.MkdirAll(fkoverlay.FusePrivateRoot(pool.AccountDir(14)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		swapVar(t, &fpOverlayProvider, func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil })

		report, calls := captureReports()
		reportOrphanFPDomains(t.Context(), orphanManager(t), nil, true, report)

		if fake.removed != 0 {
			t.Fatalf("RemoveDomain called %d times, want 0 — a freshly-backed domain must never be removed", fake.removed)
		}
		if len(*calls) != 1 || (*calls)[0].healthy || !strings.Contains((*calls)[0].detail, "state changed while confirming") {
			t.Fatalf("want one advisory 'state changed' report, got %+v", *calls)
		}
	})
}
