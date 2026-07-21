package store

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func openReserveTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustReserve(t *testing.T, s *Store) PendingAccountReservation {
	t.Helper()
	n, err := s.ReserveAccountIndex(credentialOperationTestOwner("reservation-owner"))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReserveAccountIndexAllocation(t *testing.T) {
	t.Run("empty pool starts at 1 and counts up", func(t *testing.T) {
		s := openReserveTest(t)
		if n := mustReserve(t, s); n.ID != 1 {
			t.Fatalf("first index = %d, want 1", n.ID)
		}
		if n := mustReserve(t, s); n.ID != 2 {
			t.Fatalf("second index = %d, want 2 (the first reservation must be visible)", n.ID)
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
		if n := mustReserve(t, s); n.ID != 2 {
			t.Fatalf("gap index = %d, want 2", n.ID)
		}
		if n := mustReserve(t, s); n.ID != 4 {
			t.Fatalf("next index = %d, want 4 (1,3 taken by rows, 2 by reservation)", n.ID)
		}
	})

	t.Run("release frees the index for reuse", func(t *testing.T) {
		s := openReserveTest(t)
		n := mustReserve(t, s)
		if err := s.ReleaseAccountIndex(n); err != nil {
			t.Fatal(err)
		}
		if got := mustReserve(t, s); got.ID != n.ID {
			t.Fatalf("re-reserve = %d, want the released %d", got.ID, n.ID)
		}
	})

	t.Run("release is idempotent and never frees a finalized account", func(t *testing.T) {
		s := openReserveTest(t)
		n := mustReserve(t, s)
		if err := s.PromoteReservedAccount(n, Account{ID: n.ID, InstanceID: n.InstanceID, Generation: n.Generation, ConfigDir: "a", KeychainService: "s", KeychainAccount: "u"}); err != nil {
			t.Fatal(err)
		}
		if err := s.ReleaseAccountIndex(n); err == nil {
			t.Fatal("release after promotion succeeded, want exact fence rejection")
		}
		if got := mustReserve(t, s); got.ID == n.ID {
			t.Fatalf("reserve = %d, but index %d is held by an accounts row", got.ID, n.ID)
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
			n, err := s.ReserveAccountIndex(credentialOperationTestOwner("reservation-owner"))
			if err != nil {
				errs <- err
				return
			}
			got <- n.ID
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
		if got := mustReserve(t, s); got.ID != n.ID {
			t.Fatalf("reserve after consume = %d, want the spent %d (no accounts row landed)", got.ID, n.ID)
		}
	})

	t.Run("fails loud on a never-reserved index", func(t *testing.T) {
		s := openReserveTest(t)
		if err := s.ConsumeAccountIndex(PendingAccountReservation{ID: 7, InstanceID: "missing", Generation: 1}); err == nil {
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
	if len(ids) != 2 || ids[0] != a.ID || ids[1] != b.ID {
		t.Fatalf("PendingAddIndexes = %v, want the two live reservations %d,%d", ids, a.ID, b.ID)
	}

	// Consume promotes a to a row; the reservation must drop from the list.
	if err := s.ConsumeAccountIndex(a); err != nil {
		t.Fatal(err)
	}
	if ids, err := s.PendingAddIndexes(); err != nil || len(ids) != 1 || ids[0] != b.ID {
		t.Fatalf("after consume PendingAddIndexes = %v, %v; want only %d", ids, err, b.ID)
	}

	// Release drops the last reservation.
	if err := s.ReleaseAccountIndex(b); err != nil {
		t.Fatal(err)
	}
	if ids, err := s.PendingAddIndexes(); err != nil || len(ids) != 0 {
		t.Fatalf("after release PendingAddIndexes = %v, %v; want empty", ids, err)
	}
}

func TestRetiredPendingAddRequiresExactReapReceipt(t *testing.T) {
	s := openReserveTest(t)
	owner := credentialOperationTestOwner("pending-owner")
	reservation, err := s.ReserveAccountIndex(owner)
	if err != nil {
		t.Fatal(err)
	}
	newOwner := credentialOperationTestOwner("pending-recovery")
	if err := s.ReleaseRetiredPendingAdd(
		t.Context(), reservation, newOwner, procZeroReceipt(), nil,
	); err == nil {
		t.Fatal("retired pending add released without an exact receipt")
	}
	if err := s.ReleaseAccountIndex(PendingAccountReservation{
		ID: reservation.ID, InstanceID: reservation.InstanceID,
		Generation: reservation.Generation, Owner: newOwner,
	}); err == nil {
		t.Fatal("foreign owner released pending add")
	}
	receipt, verifier := credentialOperationTestRetirement(t, owner, newOwner)
	if err := s.ReleaseRetiredPendingAdd(
		t.Context(), reservation, newOwner, receipt, verifier,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := pendingAccountReservationByID(s.db, reservation.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("retired reservation survived: %v", err)
	}
}

func TestPromoteReservedAccount(t *testing.T) {
	t.Run("spends the reservation and lands the row", func(t *testing.T) {
		s := openReserveTest(t)
		reservation := mustReserve(t, s)
		acct := Account{ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation, ConfigDir: "dir", KeychainService: "svc", KeychainAccount: "u"}
		if err := s.PromoteReservedAccount(reservation, acct); err != nil {
			t.Fatal(err)
		}
		if got, err := s.GetAccount(reservation.ID); err != nil {
			t.Fatalf("GetAccount after promote: %v", err)
		} else if got.InstanceID != reservation.InstanceID || got.Generation != reservation.Generation {
			t.Fatalf("promoted identity = (%q,%d), want (%q,%d)", got.InstanceID, got.Generation, reservation.InstanceID, reservation.Generation)
		}
		// The reservation is spent and the index is held by the accounts row.
		if got := mustReserve(t, s); got.ID == reservation.ID {
			t.Fatalf("reserve = %d, but index %d is held by the promoted row", got.ID, reservation.ID)
		}
	})

	t.Run("fails loud on a mismatched owner without writing a row", func(t *testing.T) {
		s := openReserveTest(t)
		reservation := mustReserve(t, s)
		reservation.Owner = credentialOperationTestOwner("different-owner")
		acct := Account{ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation, ConfigDir: "dir", KeychainService: "svc", KeychainAccount: "u"}
		if err := s.PromoteReservedAccount(reservation, acct); err == nil {
			t.Fatal("promote with a mismatched owner succeeded, want fail-loud")
		}
		if _, err := s.GetAccount(reservation.ID); !errors.Is(err, ErrAccountNotFound) {
			t.Fatalf("GetAccount after refused promote = %v, want ErrAccountNotFound (no row written)", err)
		}
	})
}

// TestPromoteReservedAccountConcurrent pins the atomic promote: many adds racing
// reserve→promote each land a distinct index. A non-atomic consume-then-upsert
// leaves a half-open window in which a concurrent ReserveAccountIndex reuses the
// index, handing the same id to two adds.
func TestPromoteReservedAccountConcurrent(t *testing.T) {
	s := openReserveTest(t)
	const workers = 24
	start := make(chan struct{})
	got := make(chan int, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reservation, err := s.ReserveAccountIndex(credentialOperationTestOwner("reservation-owner"))
			if err != nil {
				errs <- err
				return
			}
			acct := Account{
				ID:              reservation.ID,
				InstanceID:      reservation.InstanceID,
				Generation:      reservation.Generation,
				ConfigDir:       fmt.Sprintf("dir-%d", reservation.ID),
				KeychainService: fmt.Sprintf("svc-%d", reservation.ID),
				KeychainAccount: "u",
			}
			if err := s.PromoteReservedAccount(reservation, acct); err != nil {
				errs <- err
				return
			}
			got <- reservation.ID
		}()
	}
	close(start)
	wg.Wait()
	close(got)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent add: %v", err)
	}
	seen := map[int]bool{}
	var ids []int
	for id := range got {
		if seen[id] {
			t.Fatalf("index %d handed out to two concurrent adds", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for i, id := range ids {
		if id != i+1 {
			t.Fatalf("indices = %v, want 1..%d with no duplicates or gaps", ids, workers)
		}
	}
}
