package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func insertRemovalFleet(t *testing.T, s *Store, total int) {
	t.Helper()
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	account, err := tx.PrepareContext(t.Context(), `
		INSERT INTO accounts(
			id,instance_id,generation,account_uuid,config_dir,keychain_service,keychain_account,created_at
		) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = account.Close() }()
	removal, err := tx.PrepareContext(t.Context(), `
		INSERT INTO account_removals(
			account_id,account_instance_id,account_generation,registry_sequence,delete_credential,created_at
		) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = removal.Close() }()
	for id := 1; id <= total; id++ {
		instanceID := fmt.Sprintf("%032x", id)
		if _, err := account.ExecContext(
			t.Context(), id, instanceID, 1, fmt.Sprintf("uuid-%d", id), fmt.Sprintf("/accounts/%d", id),
			"service", "account", 1,
		); err != nil {
			t.Fatalf("insert account %d: %v", id, err)
		}
		if _, err := removal.ExecContext(t.Context(), id, instanceID, 1, 1, 1, 1); err != nil {
			t.Fatalf("insert account removal %d: %v", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestAccountRemovalPagesAreBoundedAndExclusive(t *testing.T) {
	s := openTest(t)
	const total = 10_000
	insertRemovalFleet(t, s, total)

	var got []AccountRemoval
	after := 0
	for {
		page, err := s.PageAccountRemovals(t.Context(), after, AccountRemovalPageLimit)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Removals) > AccountRemovalPageLimit {
			t.Fatalf("page length = %d", len(page.Removals))
		}
		if cap(page.Removals) > AccountRemovalPageLimit {
			t.Fatalf("page capacity = %d", cap(page.Removals))
		}
		for _, removal := range page.Removals {
			if removal.AccountID <= after {
				t.Fatalf("non-exclusive page after %d contains %d", after, removal.AccountID)
			}
		}
		got = append(got, page.Removals...)
		if page.Next == 0 {
			break
		}
		if page.Next <= after {
			t.Fatalf("cursor did not advance: %d after %d", page.Next, after)
		}
		after = page.Next
	}
	if len(got) != total {
		t.Fatalf("removals = %d, want %d", len(got), total)
	}
	for index, removal := range got {
		if removal.AccountID != index+1 {
			t.Fatalf("removal[%d] = %d", index, removal.AccountID)
		}
	}
}

func TestAccountRemovalPageRejectsInvalidBounds(t *testing.T) {
	s := openTest(t)
	for _, test := range []struct {
		after int
		limit int
	}{
		{after: -1, limit: 1},
		{limit: 0},
		{limit: AccountRemovalPageLimit + 1},
	} {
		if _, err := s.PageAccountRemovals(t.Context(), test.after, test.limit); err == nil {
			t.Fatalf("PageAccountRemovals(%d, %d) succeeded", test.after, test.limit)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.PageAccountRemovals(canceled, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled page = %v", err)
	}
}
