package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fakeReapFP is a File Provider provider fake for the orphan-reap tests. It
// records RemoveDomain calls and supports per-domain DomainRoot error injection
// (a down-app no-verdict), a RemoveDomain failure, and a beforeRoot hook that
// fires at the start of each DomainRoot call — the seam the reconfirm-race case
// uses to mutate on-disk state between the strike and the remove.
type fakeReapFP struct {
	registered    map[string]bool
	domainRootErr map[string]error
	removeErr     error
	removes       []string
	beforeRoot    func(dir string)
}

func (f *fakeReapFP) Backend() fkoverlay.Backend    { return fkoverlay.BackendFileProvider }
func (f *fakeReapFP) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }
func (f *fakeReapFP) Health(_, _ string) error      { return nil }
func (f *fakeReapFP) Sync(_, _ string) error        { return nil }
func (f *fakeReapFP) Setup(_, _ string) error       { return nil }
func (f *fakeReapFP) Teardown(_, _ string) error    { return nil }

func (f *fakeReapFP) DomainRoot(_ context.Context, dir string) (string, error) {
	if f.beforeRoot != nil {
		f.beforeRoot(dir)
	}
	base := filepath.Base(dir)
	if err := f.domainRootErr[base]; err != nil {
		return "", err
	}
	if !f.registered[base] {
		return "", fmt.Errorf("state %s: %w", dir, fileproviderd.ErrNoDomain)
	}
	return "/domain/" + base, nil
}

func (f *fakeReapFP) RemoveDomain(dir string) error {
	f.removes = append(f.removes, dir)
	if f.removeErr != nil {
		return f.removeErr
	}
	delete(f.registered, filepath.Base(dir))
	return nil
}

// newReapServer builds a File-Provider-wired test server with the given domain
// ids registered in a fakeReapFP, plus a mutable candidate-listing seam the test
// drives.
func newReapServer(t *testing.T, registered ...int) (*Server, *fakeReapFP, *[]int) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	s.fpSynth = alwaysNonEmpty
	s.fpBridgeReadyFn = func() bool { return true }

	reg := map[string]bool{}
	for _, id := range registered {
		reg[pool.AccountDirName(id)] = true
	}
	fp := &fakeReapFP{registered: reg, domainRootErr: map[string]error{}}
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())

	candidates := append([]int(nil), registered...)
	old := fpCloudStorageDomains
	fpCloudStorageDomains = func() ([]int, error) { return candidates, nil }
	t.Cleanup(func() { fpCloudStorageDomains = old })
	return s, fp, &candidates
}

// TestOrphanReapFiresOnThirdConfirm pins the debounce: a rowless registered
// domain is deregistered only on the third consecutive confirmed sweep.
func TestOrphanReapFiresOnThirdConfirm(t *testing.T) {
	s, fp, _ := newReapServer(t, 13)

	s.sweepOrphanFPDomains(t.Context())
	s.sweepOrphanFPDomains(t.Context())
	if len(fp.removes) != 0 {
		t.Fatalf("reaped after %d confirmations, want none before %d", 2, fpOrphanReapStrikes)
	}
	s.sweepOrphanFPDomains(t.Context())
	if len(fp.removes) != 1 || fp.removes[0] != pool.AccountDir(13) {
		t.Fatalf("third confirm removes = %v, want exactly [%s]", fp.removes, pool.AccountDir(13))
	}
	if fp.registered[pool.AccountDirName(13)] {
		t.Fatal("orphan domain still registered after the reap")
	}
}

// TestOrphanReapNegatives pins every guard: across three sweeps none deregisters
// a domain. Each case is the safe direction — an owned, mid-add, kept, residual,
// no-verdict, or bridge-down domain is never reaped.
func TestOrphanReapNegatives(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, s *Server, fp *fakeReapFP, candidates *[]int)
	}{
		{"live account row", func(_ *testing.T, _ *Server, fp *fakeReapFP, candidates *[]int) {
			// acct-1 is a live row (newTestServer). Register + list it as a candidate.
			fp.registered[pool.AccountDirName(1)] = true
			*candidates = []int{1}
		}},
		{"live add reservation", func(t *testing.T, s *Server, fp *fakeReapFP, candidates *[]int) {
			id, err := s.m.Store.ReserveAccountIndex() // 3, with rows 1,2 present
			if err != nil {
				t.Fatal(err)
			}
			fp.registered[pool.AccountDirName(id)] = true
			*candidates = []int{id}
		}},
		{"kept account dir", func(t *testing.T, _ *Server, _ *fakeReapFP, _ *[]int) {
			if err := os.MkdirAll(pool.AccountDir(13), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{"kept private root", func(t *testing.T, _ *Server, _ *fakeReapFP, _ *[]int) {
			if err := os.MkdirAll(fkoverlay.FusePrivateRoot(pool.AccountDir(13)), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{"no domain registered", func(_ *testing.T, _ *Server, fp *fakeReapFP, _ *[]int) {
			delete(fp.registered, pool.AccountDirName(13)) // DomainRoot -> ErrNoDomain
		}},
		{"app unavailable no verdict", func(_ *testing.T, _ *Server, fp *fakeReapFP, _ *[]int) {
			fp.domainRootErr[pool.AccountDirName(13)] = fmt.Errorf("state: %w", fileproviderd.ErrAppUnavailable)
		}},
		{"bridge not ready", func(_ *testing.T, s *Server, _ *fakeReapFP, _ *[]int) {
			s.fpBridgeReadyFn = func() bool { return false }
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, fp, candidates := newReapServer(t, 13)
			tc.setup(t, s, fp, candidates)
			for range fpOrphanReapStrikes {
				s.sweepOrphanFPDomains(t.Context())
			}
			if len(fp.removes) != 0 {
				t.Fatalf("%s: deregistered %v across %d sweeps, want none", tc.name, fp.removes, fpOrphanReapStrikes)
			}
		})
	}
}

// TestOrphanReapNoVerdictResetsStreak pins that only CONSECUTIVE confirmations
// count: a no-verdict sweep in the middle resets the streak.
func TestOrphanReapNoVerdictResetsStreak(t *testing.T) {
	s, fp, _ := newReapServer(t, 13)
	name := pool.AccountDirName(13)

	s.sweepOrphanFPDomains(t.Context()) // confirm 1
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	fp.domainRootErr[name] = fmt.Errorf("state: %w", fileproviderd.ErrAppUnavailable)
	s.sweepOrphanFPDomains(t.Context()) // no verdict -> reset
	delete(fp.domainRootErr, name)
	s.sweepOrphanFPDomains(t.Context()) // confirm 1 (again)
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	if len(fp.removes) != 0 {
		t.Fatalf("reaped after a reset streak: removes = %v", fp.removes)
	}
	s.sweepOrphanFPDomains(t.Context()) // confirm 3 -> reap
	if len(fp.removes) != 1 {
		t.Fatalf("no reap on the third consecutive confirm after a reset: removes = %v", fp.removes)
	}
}

// TestOrphanReapBridgeDownResetsStreak pins that a bridge-down tick is a no-verdict
// that resets the streak: confirmations either side of it are not consecutive.
func TestOrphanReapBridgeDownResetsStreak(t *testing.T) {
	s, fp, _ := newReapServer(t, 13)
	bridge := true
	s.fpBridgeReadyFn = func() bool { return bridge }

	s.sweepOrphanFPDomains(t.Context()) // confirm 1
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	bridge = false
	s.sweepOrphanFPDomains(t.Context()) // bridge down -> reset
	bridge = true
	s.sweepOrphanFPDomains(t.Context()) // confirm 1 (again)
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	if len(fp.removes) != 0 {
		t.Fatalf("reaped across a bridge-down reset: removes = %v", fp.removes)
	}
	s.sweepOrphanFPDomains(t.Context()) // confirm 3 -> reap
	if len(fp.removes) != 1 {
		t.Fatalf("no reap on the third consecutive confirm after a bridge-down reset: removes = %v", fp.removes)
	}
}

// TestOrphanReapVanishedCandidateResetsStreak pins that a candidate dropping out
// of the CloudStorage listing resets its strikes (pruneFPOrphanLedger).
func TestOrphanReapVanishedCandidateResetsStreak(t *testing.T) {
	s, fp, candidates := newReapServer(t, 13)

	s.sweepOrphanFPDomains(t.Context()) // confirm 1
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	*candidates = nil                   // the artifact vanished this sweep
	s.sweepOrphanFPDomains(t.Context()) // prune resets the strike ledger
	*candidates = []int{13}
	s.sweepOrphanFPDomains(t.Context()) // confirm 1 (again)
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	if len(fp.removes) != 0 {
		t.Fatalf("reaped after a vanished-candidate reset: removes = %v", fp.removes)
	}
	s.sweepOrphanFPDomains(t.Context()) // confirm 3 -> reap
	if len(fp.removes) != 1 {
		t.Fatalf("no reap on the third confirm after the candidate reappeared: removes = %v", fp.removes)
	}
}

// TestOrphanReapReconfirmBeforeKill pins the reconfirm: a guard appearing between
// the third strike's confirm and the remove (a concurrent add seeds the account
// dir) spares the domain.
func TestOrphanReapReconfirmBeforeKill(t *testing.T) {
	s, fp, _ := newReapServer(t, 13)

	s.sweepOrphanFPDomains(t.Context()) // confirm 1
	s.sweepOrphanFPDomains(t.Context()) // confirm 2
	// On the third sweep, a concurrent add seeds the account dir during the strike
	// confirm — after the sweep's initial guard read but before the remove.
	fp.beforeRoot = func(dir string) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	s.sweepOrphanFPDomains(t.Context())
	if len(fp.removes) != 0 {
		t.Fatalf("reconfirm did not spare a domain a guard reclaimed: removes = %v", fp.removes)
	}
}

// TestOrphanReapRetriesFailedRemoveAfterBackoff pins that a failed RemoveDomain
// is retried only after the fp.orphan backoff, not on the very next sweep.
func TestOrphanReapRetriesFailedRemoveAfterBackoff(t *testing.T) {
	s, fp, _ := newReapServer(t, 13)
	fp.removeErr = errors.New("remove failed")

	for range fpOrphanReapStrikes {
		s.sweepOrphanFPDomains(t.Context())
	}
	if len(fp.removes) != 1 {
		t.Fatalf("failed-remove first attempt = %d removes, want 1", len(fp.removes))
	}
	// A sweep within the backoff must not re-attempt.
	s.sweepOrphanFPDomains(t.Context())
	if len(fp.removes) != 1 {
		t.Fatalf("retried the failed remove within the backoff: %d removes", len(fp.removes))
	}
	// Backdate the backoff clock and let the remove succeed: the next sweep retries.
	s.ledMu.Lock()
	s.led.setNextDue(fpOrphanPolicy, pool.AccountDir(13), time.Now().Add(-time.Hour))
	s.ledMu.Unlock()
	fp.removeErr = nil
	s.sweepOrphanFPDomains(t.Context())
	if len(fp.removes) != 2 {
		t.Fatalf("no retry after the backoff elapsed: %d removes, want 2", len(fp.removes))
	}
	if fp.registered[pool.AccountDirName(13)] {
		t.Fatal("orphan still registered after the successful retry")
	}
}

// TestOrphanReapFailsClosedOnListingError pins that any listing error skips the
// whole pass and resets nothing: a partial guard set is the catastrophic
// direction, so the reap never proceeds against it.
func TestOrphanReapFailsClosedOnListingError(t *testing.T) {
	t.Run("cloud storage listing error", func(t *testing.T) {
		s, fp, _ := newReapServer(t, 13)
		fpCloudStorageDomains = func() ([]int, error) { return nil, errors.New("readdir failed") }
		for range fpOrphanReapStrikes {
			s.sweepOrphanFPDomains(t.Context())
		}
		if len(fp.removes) != 0 {
			t.Fatalf("reaped despite a listing error: removes = %v", fp.removes)
		}
	})

	t.Run("account listing error", func(t *testing.T) {
		s, fp, _ := newReapServer(t, 13)
		_ = s.m.Store.Close() // ListAccounts / PendingAddIndexes now error
		for range fpOrphanReapStrikes {
			s.sweepOrphanFPDomains(t.Context())
		}
		if len(fp.removes) != 0 {
			t.Fatalf("reaped despite an account listing error: removes = %v", fp.removes)
		}
	})
}

// TestFPCloudStorageDomainsFiltersMalformed pins the candidate seam: only
// well-formed CCPoolStatus-acct-NN folders parse; junk and foreign-app folders
// are never candidates, so the reap never touches them.
func TestFPCloudStorageDomainsFiltersMalformed(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := pool.FPCloudStorageDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		pool.FPDomainFolderPrefix + pool.AccountDirName(13), // valid: acct-13
		pool.FPDomainFolderPrefix + "junk",                  // malformed suffix
		pool.FPDomainFolderPrefix + "acct-1",                // unpadded, rejected
		"GoogleDrive-x",                                     // foreign app
	} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := fpCloudStorageDomains()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 13 {
		t.Fatalf("fpCloudStorageDomains = %v, want only [13]", ids)
	}
}
