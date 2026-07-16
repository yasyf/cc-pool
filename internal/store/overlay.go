package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GetOverlayApplied returns the last successfully applied overlay generation.
func (s *Store) GetOverlayApplied(accountID int) (OverlayApplied, bool, error) {
	var row OverlayApplied
	var appliedAt int64
	err := s.db.QueryRow(`SELECT account_id,backend,canonical_stamp,settings_stamp,structure_stamp,app_stamp,applied_at
		FROM overlay_applied WHERE account_id=?`, accountID).Scan(
		&row.AccountID, &row.Backend, &row.CanonicalStamp, &row.SettingsStamp,
		&row.StructureStamp, &row.AppStamp, &appliedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return OverlayApplied{}, false, nil
	}
	if err != nil {
		return OverlayApplied{}, false, fmt.Errorf("get overlay applied for account %d: %w", accountID, err)
	}
	row.AppliedAt = time.Unix(appliedAt, 0)
	return row, true, nil
}

// SetOverlayApplied records row only while its account still exists. The
// account foreign key and INSERT-SELECT close the remove-versus-apply race.
func (s *Store) SetOverlayApplied(row OverlayApplied) error {
	res, err := s.db.Exec(`INSERT INTO overlay_applied(
		account_id,backend,canonical_stamp,settings_stamp,structure_stamp,app_stamp,applied_at)
		SELECT id,?,?,?,?,?,? FROM accounts WHERE id=?
		ON CONFLICT(account_id) DO UPDATE SET
			backend=excluded.backend,
			canonical_stamp=excluded.canonical_stamp,
			settings_stamp=excluded.settings_stamp,
			structure_stamp=excluded.structure_stamp,
			app_stamp=excluded.app_stamp,
			applied_at=excluded.applied_at`,
		row.Backend, row.CanonicalStamp, row.SettingsStamp, row.StructureStamp,
		row.AppStamp, row.AppliedAt.Unix(), row.AccountID,
	)
	if err != nil {
		return fmt.Errorf("set overlay applied for account %d: %w", row.AccountID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("set overlay applied for account %d: rows affected: %w", row.AccountID, err)
	}
	if n == 0 {
		return ErrAccountNotFound
	}
	return nil
}
