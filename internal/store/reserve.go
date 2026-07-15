package store

import (
	"fmt"
	"time"
)

// PendingAddTTL is how long an index reservation may sit unfinalized before
// the startup sweep reclaims it; generous, an add spans an interactive login.
const PendingAddTTL = 24 * time.Hour

// ReserveAccountIndex atomically allocates the smallest unused account index
// (>= 1, gap-filling): the free-index computation and insert are one SQL
// statement, so two concurrent callers can never be handed the same index.
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

// ReleaseAccountIndex frees an index reservation; idempotent, so every
// abandon/rollback path may call it unconditionally.
func (s *Store) ReleaseAccountIndex(id int) error {
	if _, err := s.db.Exec(`DELETE FROM pending_adds WHERE id=?`, id); err != nil {
		return fmt.Errorf("release account index %d: %w", id, err)
	}
	return nil
}

// ConsumeAccountIndex spends a reservation for promotion to an accounts row;
// exactly one row must be deleted, else it fails loud — a blind promote could
// collide on the index.
func (s *Store) ConsumeAccountIndex(id int) error {
	return consumeReservation(s.db, id)
}

// consumeReservation runs the consume against e (a *sql.DB or a *sql.Tx), so
// PromoteReservedAccount can spend the reservation inside its transaction.
func consumeReservation(e rowExecer, id int) error {
	res, err := e.Exec(`DELETE FROM pending_adds WHERE id=?`, id)
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

// PromoteReservedAccount spends account a's index reservation and inserts its
// row in one transaction, so a concurrent ReserveAccountIndex never observes
// the index free between the two. A reservation already gone (released or
// swept) fails loud and writes no row.
func (s *Store) PromoteReservedAccount(a Account) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("promote account %d: %w", a.ID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := consumeReservation(tx, a.ID); err != nil {
		return err
	}
	if err := upsertAccount(tx, a); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("promote account %d: %w", a.ID, err)
	}
	return nil
}

// PendingAddIndexes lists every live account-index reservation, ascending. It
// is the daemon orphan reap's mid-add guard: a domain whose account row has not
// yet been promoted (PrepareAdd registers the domain before FinalizeAdd upserts
// the row) is reserved here, so it is never mistaken for an orphan.
func (s *Store) PendingAddIndexes() ([]int, error) {
	rows, err := s.db.Query(`SELECT id FROM pending_adds ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list pending adds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("list pending adds: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pending adds: %w", err)
	}
	return ids, nil
}

// SweepPendingAdds reclaims reservations created before cutoff (adds whose
// process died mid-add); returns the number reclaimed.
func (s *Store) SweepPendingAdds(cutoff time.Time) (int, error) {
	res, err := s.db.Exec(`DELETE FROM pending_adds WHERE created_at < ?`, cutoff.Unix())
	if err != nil {
		return 0, fmt.Errorf("sweep pending adds: %w", err)
	}
	n, err := res.RowsAffected()
	return int(n), err
}
