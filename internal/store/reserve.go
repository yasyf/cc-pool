package store

import (
	"fmt"
	"time"
)

// PendingAddTTL is how long an account-index reservation may sit unfinalized
// before the daemon's startup sweep reclaims it. Generous, because a
// reservation spans an interactive `claude /login` the user may walk away
// from mid-add.
const PendingAddTTL = 24 * time.Hour

// ReserveAccountIndex atomically allocates the smallest unused account index
// (>= 1, gap-filling) by inserting a reservation row into pending_adds. The
// free-index computation and the insert are one SQL statement, so two
// concurrent callers can never be handed the same index: the loser's
// statement already sees the winner's reservation (and a hypothetical
// duplicate would fail loudly on the primary key). The index stays taken
// until FinalizeAdd consumes the reservation (ConsumeAccountIndex),
// ReleaseAccountIndex frees it, or SweepPendingAdds reclaims it.
func (s *Store) ReserveAccountIndex() (int, error) {
	var id int
	err := s.db.QueryRow(`
		INSERT INTO pending_adds(id, created_at)
		SELECT MIN(candidate), ?
		FROM (SELECT 1 AS candidate
		      UNION ALL
		      SELECT id+1 FROM (SELECT id FROM accounts UNION SELECT id FROM pending_adds))
		WHERE candidate NOT IN (SELECT id FROM accounts UNION SELECT id FROM pending_adds)
		RETURNING id`,
		time.Now().Unix()).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("reserve account index: %w", err)
	}
	return id, nil
}

// ReleaseAccountIndex frees an index reservation. Idempotent: releasing an
// absent reservation (already promoted, released, or swept) is a no-op, so
// every abandon/rollback path may call it unconditionally.
func (s *Store) ReleaseAccountIndex(id int) error {
	if _, err := s.db.Exec(`DELETE FROM pending_adds WHERE id=?`, id); err != nil {
		return fmt.Errorf("release account index %d: %w", id, err)
	}
	return nil
}

// ConsumeAccountIndex spends an index reservation for promotion to an
// accounts row: exactly one pending_adds row must be deleted, else it fails
// loud — the reservation was released or swept, so a concurrent add may
// already hold the index and a blind promote would silently collide on the
// same index/dir/Keychain service.
func (s *Store) ConsumeAccountIndex(id int) error {
	res, err := s.db.Exec(`DELETE FROM pending_adds WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("consume account index %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("consume account index %d: %w", id, err)
	}
	if n != 1 {
		return fmt.Errorf("consume account index %d: reservation gone (released or swept)", id)
	}
	return nil
}

// SweepPendingAdds reclaims index reservations created before cutoff — adds
// whose process died before FinalizeAdd or AbandonAdd could run (e.g. a
// killed `ccp add`). Returns the number reclaimed.
func (s *Store) SweepPendingAdds(cutoff time.Time) (int, error) {
	res, err := s.db.Exec(`DELETE FROM pending_adds WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, fmt.Errorf("sweep pending adds: %w", err)
	}
	n, err := res.RowsAffected()
	return int(n), err
}
