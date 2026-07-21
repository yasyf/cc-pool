package store

import (
	"context"
	"errors"
	"time"
)

// AccountRemovalPageLimit is the hard maximum interrupted-removal page size.
const AccountRemovalPageLimit = 256

// AccountRemovalPage is one bounded page after an exclusive account cursor.
type AccountRemovalPage struct {
	Removals []AccountRemoval
	Next     int
}

// PageAccountRemovals returns durable removal intents after accountID.
func (s *Store) PageAccountRemovals(
	ctx context.Context,
	after, limit int,
) (AccountRemovalPage, error) {
	if after < 0 {
		return AccountRemovalPage{}, errors.New("account removal cursor must not be negative")
	}
	if limit < 1 || limit > AccountRemovalPageLimit {
		return AccountRemovalPage{}, errors.New("account removal page limit is invalid")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT account_id,account_instance_id,account_generation,registry_sequence,delete_credential,created_at
		FROM account_removals
		WHERE account_id > ?
		ORDER BY account_id
		LIMIT ?`, after, limit+1)
	if err != nil {
		return AccountRemovalPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := AccountRemovalPage{Removals: make([]AccountRemoval, 0, limit)}
	for rows.Next() {
		var removal AccountRemoval
		var deleteCredential int
		var created int64
		if err := rows.Scan(
			&removal.AccountID,
			&removal.AccountInstanceID,
			&removal.AccountGeneration,
			&removal.RegistrySequence,
			&deleteCredential,
			&created,
		); err != nil {
			return AccountRemovalPage{}, err
		}
		if len(page.Removals) == limit {
			page.Next = page.Removals[len(page.Removals)-1].AccountID
			break
		}
		removal.DeleteCredential = deleteCredential != 0
		removal.CreatedAt = time.Unix(0, created)
		page.Removals = append(page.Removals, removal)
	}
	if err := rows.Err(); err != nil {
		return AccountRemovalPage{}, err
	}
	return page, nil
}
