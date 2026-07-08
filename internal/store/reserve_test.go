package store

import (
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func openReserveTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustReserve(t *testing.T, s *Store) int {
	t.Helper()
	n, err := s.ReserveAccountIndex()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReserveAccountIndexAllocation(t *testing.T) {
	t.Run("empty pool starts at 1 and counts up", func(t *testing.T) {
		s := openReserveTest(t)
		if n := mustReserve(t, s); n != 1 {
			t.Fatalf("first index = %d, want 1", n)
		}
		if n := mustReserve(t, s); n != 2 {
			t.Fatalf("second index = %d, want 2 (the first reservation must be visible)", n)
		}
	})

	t.Run("skips finalized accounts and fills gaps", func(t *testing.T) {
		s := openReserveTest(t)
		for _, a := range []Account{
			{ID: 1, ConfigDir: "a", KeychainService: "s1", KeychainAccount: "u"},
			{ID: 3, ConfigDir: "c", KeychainService: "s3", KeychainAccount: "u"},
		} {
			if err := s.UpsertAccount(a); err != nil {
				t.Fatal(err)
			}
		}
		if n := mustReserve(t, s); n != 2 {
			t.Fatalf("gap index = %d, want 2", n)
		}
		if n := mustReserve(t, s); n != 4 {
			t.Fatalf("next index = %d, want 4 (1,3 taken by rows, 2 by reservation)", n)
		}
	})

	t.Run("release frees the index for reuse", func(t *testing.T) {
		s := openReserveTest(t)
		n := mustReserve(t, s)
		if err := s.ReleaseAccountIndex(n); err != nil {
			t.Fatal(err)
		}
		if got := mustReserve(t, s); got != n {
			t.Fatalf("re-reserve = %d, want the released %d", got, n)
		}
	})

	t.Run("release is idempotent and never frees a finalized account", func(t *testing.T) {
		s := openReserveTest(t)
		n := mustReserve(t, s)
		if err := s.UpsertAccount(Account{ID: n, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"}); err != nil {
			t.Fatal(err)
		}
		// Promotion: the row exists, the reservation is dropped — twice, for the
		// retry paths.
		if err := s.ReleaseAccountIndex(n); err != nil {
			t.Fatal(err)
		}
		if err := s.ReleaseAccountIndex(n); err != nil {
			t.Fatalf("second release: %v", err)
		}
		if got := mustReserve(t, s); got == n {
			t.Fatalf("reserve = %d, but index %d is held by an accounts row", got, n)
		}
	})
}

func TestReserveAccountIndexConcurrent(t *testing.T) {
	s := openReserveTest(t)
	const workers = 8
	start := make(chan struct{})
	got := make(chan int, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			n, err := s.ReserveAccountIndex()
			if err != nil {
				errs <- err
				return
			}
			got <- n
		}()
	}
	close(start)
	wg.Wait()
	close(got)
	close(errs)
	for err := range errs {
		t.Fatalf("ReserveAccountIndex: %v", err)
	}
	var ids []int
	for n := range got {
		ids = append(ids, n)
	}
	sort.Ints(ids)
	for i, n := range ids {
		if n != i+1 {
			t.Fatalf("indices = %v, want 1..%d with no duplicates or gaps", ids, workers)
		}
	}
}

func TestConsumeAccountIndex(t *testing.T) {
	t.Run("spends a live reservation exactly once", func(t *testing.T) {
		s := openReserveTest(t)
		n := mustReserve(t, s)
		if err := s.ConsumeAccountIndex(n); err != nil {
			t.Fatal(err)
		}
		if err := s.ConsumeAccountIndex(n); err == nil {
			t.Fatal("second consume succeeded, want fail-loud on a spent reservation")
		}
		if got := mustReserve(t, s); got != n {
			t.Fatalf("reserve after consume = %d, want the spent %d (no accounts row landed)", got, n)
		}
	})

	t.Run("fails loud after a sweep reclaimed the reservation", func(t *testing.T) {
		s := openReserveTest(t)
		n := mustReserve(t, s)
		if _, err := s.SweepPendingAdds(time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := s.ConsumeAccountIndex(n); err == nil {
			t.Fatal("consume after sweep succeeded, want fail-loud")
		}
	})

	t.Run("fails loud on a never-reserved index", func(t *testing.T) {
		s := openReserveTest(t)
		if err := s.ConsumeAccountIndex(7); err == nil {
			t.Fatal("consume of an unreserved index succeeded, want fail-loud")
		}
	})
}

func TestPendingAddIndexes(t *testing.T) {
	s := openReserveTest(t)

	if ids, err := s.PendingAddIndexes(); err != nil || len(ids) != 0 {
		t.Fatalf("fresh store PendingAddIndexes = %v, %v; want empty, nil", ids, err)
	}

	a := mustReserve(t, s)
	b := mustReserve(t, s)
	ids, err := s.PendingAddIndexes()
	if err != nil {
		t.Fatal(err)
	}
	sort.Ints(ids)
	if len(ids) != 2 || ids[0] != a || ids[1] != b {
		t.Fatalf("PendingAddIndexes = %v, want the two live reservations %d,%d", ids, a, b)
	}

	// Consume promotes a to a row; the reservation must drop from the list.
	if err := s.ConsumeAccountIndex(a); err != nil {
		t.Fatal(err)
	}
	if ids, err := s.PendingAddIndexes(); err != nil || len(ids) != 1 || ids[0] != b {
		t.Fatalf("after consume PendingAddIndexes = %v, %v; want only %d", ids, err, b)
	}

	// Release drops the last reservation.
	if err := s.ReleaseAccountIndex(b); err != nil {
		t.Fatal(err)
	}
	if ids, err := s.PendingAddIndexes(); err != nil || len(ids) != 0 {
		t.Fatalf("after release PendingAddIndexes = %v, %v; want empty", ids, err)
	}
}

func TestSweepPendingAdds(t *testing.T) {
	s := openReserveTest(t)
	n := mustReserve(t, s)

	swept, err := s.SweepPendingAdds(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if swept != 0 {
		t.Fatalf("swept %d fresh reservations, want 0", swept)
	}
	if got := mustReserve(t, s); got == n {
		t.Fatalf("reserve = %d, but a fresh reservation must survive the sweep", got)
	}

	swept, err = s.SweepPendingAdds(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if swept != 2 {
		t.Fatalf("swept = %d, want both stale reservations", swept)
	}
	if got := mustReserve(t, s); got != n {
		t.Fatalf("reserve after sweep = %d, want the reclaimed %d", got, n)
	}
}
