package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite" // pure-Go "sqlite" driver, for the old-schema migration test
)

// TestAccountUUIDColumnMigration seeds a pre-account_uuid accounts table, then
// reopens through Open and asserts the column was added in place and the
// pre-existing row reads back as the empty-string default (not NULL).
func TestAccountUUIDColumnMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")

	// The old accounts schema, verbatim, before the account_uuid column.
	const oldSchema = `
CREATE TABLE accounts (
  id               INTEGER PRIMARY KEY,
  config_dir       TEXT NOT NULL UNIQUE,
  keychain_service TEXT NOT NULL,
  keychain_account TEXT NOT NULL,
  label            TEXT NOT NULL DEFAULT '',
  overlay_kind     TEXT NOT NULL DEFAULT 'symlink',
  created_at       INTEGER NOT NULL
);`
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO accounts(id,config_dir,keychain_service,keychain_account,label,overlay_kind,created_at)
		 VALUES(1,'/cfg/acct-01','svc1','me','work','symlink',?)`,
		time.Now().Add(-time.Hour).Unix()); err != nil {
		t.Fatalf("seed old row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("reopen migrates in place: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// The column exists and the migrated row defaults to "".
	got, err := s.GetAccount(1)
	if err != nil {
		t.Fatalf("get migrated account: %v", err)
	}
	if got.AccountUUID != "" {
		t.Fatalf("migrated AccountUUID = %q, want empty default", got.AccountUUID)
	}
	if got.Label != "work" || got.ConfigDir != "/cfg/acct-01" {
		t.Fatalf("non-uuid fields lost across migration: %+v", got)
	}

	// The index landed after the column migration, not in the schema block.
	var idx string
	if err := s.db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='index' AND name='idx_accounts_uuid'`).Scan(&idx); err != nil {
		t.Fatalf("idx_accounts_uuid absent after migration: %v", err)
	}

	// Backfill still works against the migrated schema.
	if err := s.SetAccountUUID(1, "u-backfilled"); err != nil {
		t.Fatalf("backfill after migration: %v", err)
	}
	found, ok, err := s.GetAccountByUUID("u-backfilled")
	if err != nil || !ok {
		t.Fatalf("lookup after backfill: ok=%v err=%v", ok, err)
	}
	if found.ID != 1 {
		t.Fatalf("backfilled lookup id = %d, want 1", found.ID)
	}
}

// TestChainHashColumnsMigration pins the in-place chain-hash column migration:
// empty-string defaults, SetChainHashes round-trips, and a zero-value
// re-upsert never wipes them.
func TestChainHashColumnsMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")

	// The old accounts schema, verbatim, before the chain-hash columns.
	const oldSchema = `
CREATE TABLE accounts (
  id               INTEGER PRIMARY KEY,
  config_dir       TEXT NOT NULL UNIQUE,
  keychain_service TEXT NOT NULL,
  keychain_account TEXT NOT NULL,
  label            TEXT NOT NULL DEFAULT '',
  overlay_kind     TEXT NOT NULL DEFAULT 'symlink',
  account_uuid     TEXT NOT NULL DEFAULT '',
  created_at       INTEGER NOT NULL
);`
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatalf("create old schema: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO accounts(id,config_dir,keychain_service,keychain_account,label,overlay_kind,account_uuid,created_at)
		 VALUES(1,'/cfg/acct-01','svc1','me','work','symlink','u-1',?)`,
		time.Now().Add(-time.Hour).Unix()); err != nil {
		t.Fatalf("seed old row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("reopen migrates in place: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.GetAccount(1)
	if err != nil {
		t.Fatalf("get migrated account: %v", err)
	}
	if got.CredHash != "" || got.CredParentHash != "" {
		t.Fatalf("migrated chain columns = (%q,%q), want empty defaults", got.CredHash, got.CredParentHash)
	}
	if got.Label != "work" || got.AccountUUID != "u-1" {
		t.Fatalf("non-chain fields lost across migration: %+v", got)
	}

	if err := s.SetChainHashes(1, "h-cred", "h-parent"); err != nil {
		t.Fatalf("SetChainHashes after migration: %v", err)
	}
	got, err = s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.CredHash != "h-cred" || got.CredParentHash != "h-parent" {
		t.Fatalf("chain columns = (%q,%q), want (h-cred,h-parent)", got.CredHash, got.CredParentHash)
	}

	// A generic re-upsert with zero-value chain hashes must not wipe them.
	got.CredHash, got.CredParentHash = "", ""
	got.Label = "renamed"
	if err := s.UpsertAccount(got); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	got, err = s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.CredHash != "h-cred" || got.CredParentHash != "h-parent" {
		t.Fatalf("re-upsert clobbered chain columns: (%q,%q)", got.CredHash, got.CredParentHash)
	}
	if got.Label != "renamed" {
		t.Fatalf("re-upsert did not update label: %q", got.Label)
	}

	// Unknown id fails loud.
	if err := s.SetChainHashes(99, "h", "p"); err == nil {
		t.Fatal("SetChainHashes on unknown id: want error, got nil")
	}
}

func TestGetAccountByUUID(t *testing.T) {
	s := openTest(t)

	// One account carries a uuid at insert; another keeps the empty default.
	withUUID := Account{ID: 1, ConfigDir: "/cfg/acct-01", KeychainService: "svc1", KeychainAccount: "me", Label: "alpha", OverlayKind: "symlink", AccountUUID: "u-alpha"}
	blank := Account{ID: 2, ConfigDir: "/cfg/acct-02", KeychainService: "svc2", KeychainAccount: "me", Label: "beta", OverlayKind: "symlink"}
	for _, a := range []Account{withUUID, blank} {
		if err := s.UpsertAccount(a); err != nil {
			t.Fatalf("upsert %d: %v", a.ID, err)
		}
	}

	// UpsertAccount round-trips the uuid set at insert.
	got, err := s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountUUID != "u-alpha" {
		t.Fatalf("insert-time AccountUUID = %q, want u-alpha", got.AccountUUID)
	}

	tests := []struct {
		name   string
		uuid   string
		wantOK bool
		wantID int
	}{
		{name: "match", uuid: "u-alpha", wantOK: true, wantID: 1},
		{name: "no-such-uuid", uuid: "u-missing", wantOK: false},
		{name: "empty-never-matches-blank-row", uuid: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acct, ok, err := s.GetAccountByUUID(tc.uuid)
			if err != nil {
				t.Fatalf("GetAccountByUUID(%q): %v", tc.uuid, err)
			}
			if ok != tc.wantOK {
				t.Fatalf("GetAccountByUUID(%q) ok = %v, want %v", tc.uuid, ok, tc.wantOK)
			}
			if ok && acct.ID != tc.wantID {
				t.Fatalf("GetAccountByUUID(%q) id = %d, want %d", tc.uuid, acct.ID, tc.wantID)
			}
			if !ok && acct.AccountUUID != "" {
				t.Fatalf("GetAccountByUUID(%q) not-found returned non-zero account: %+v", tc.uuid, acct)
			}
		})
	}
}

// TestGetAccountByUUIDDuplicatePicksLowestID pins deterministic duplicate-uuid
// resolution: always the lowest id, stable across calls.
func TestGetAccountByUUIDDuplicatePicksLowestID(t *testing.T) {
	s := openTest(t)
	// Higher id inserted first, so insertion order disagrees with id order.
	for _, a := range []Account{
		{ID: 5, ConfigDir: "/cfg/acct-05", KeychainService: "svc5", KeychainAccount: "me", AccountUUID: "u-dup"},
		{ID: 2, ConfigDir: "/cfg/acct-02", KeychainService: "svc2", KeychainAccount: "me", AccountUUID: "u-dup"},
	} {
		if err := s.UpsertAccount(a); err != nil {
			t.Fatalf("upsert %d: %v", a.ID, err)
		}
	}
	for i := range 3 {
		acct, ok, err := s.GetAccountByUUID("u-dup")
		if err != nil || !ok {
			t.Fatalf("call %d: ok=%v err=%v", i, ok, err)
		}
		if acct.ID != 2 {
			t.Fatalf("call %d: id = %d, want the lowest id 2", i, acct.ID)
		}
	}
}

// TestAccountsByUUID pins the multi-row resolver ambiguity-refusing callers
// depend on: every row sharing the uuid, ordered by id; empty uuid matches
// nothing; a unique uuid returns exactly its row.
func TestAccountsByUUID(t *testing.T) {
	s := openTest(t)
	for _, a := range []Account{
		{ID: 5, ConfigDir: "/cfg/acct-05", KeychainService: "svc5", KeychainAccount: "me", AccountUUID: "u-dup"},
		{ID: 2, ConfigDir: "/cfg/acct-02", KeychainService: "svc2", KeychainAccount: "me", AccountUUID: "u-dup"},
		{ID: 3, ConfigDir: "/cfg/acct-03", KeychainService: "svc3", KeychainAccount: "me", AccountUUID: "u-solo"},
		{ID: 4, ConfigDir: "/cfg/acct-04", KeychainService: "svc4", KeychainAccount: "me"},
	} {
		if err := s.UpsertAccount(a); err != nil {
			t.Fatalf("upsert %d: %v", a.ID, err)
		}
	}

	dup, err := s.AccountsByUUID("u-dup")
	if err != nil {
		t.Fatal(err)
	}
	if len(dup) != 2 || dup[0].ID != 2 || dup[1].ID != 5 {
		t.Fatalf("AccountsByUUID(u-dup) = %+v, want ids [2 5] in order", dup)
	}
	solo, err := s.AccountsByUUID("u-solo")
	if err != nil || len(solo) != 1 || solo[0].ID != 3 {
		t.Fatalf("AccountsByUUID(u-solo) = %+v (err %v), want exactly id 3", solo, err)
	}
	if got, err := s.AccountsByUUID("u-missing"); err != nil || len(got) != 0 {
		t.Fatalf("AccountsByUUID(u-missing) = %+v (err %v), want none", got, err)
	}
	if got, err := s.AccountsByUUID(""); err != nil || got != nil {
		t.Fatalf("AccountsByUUID(\"\") = %+v (err %v); empty must never match un-backfilled rows", got, err)
	}
}

func TestSetAccountUUIDRoundTrip(t *testing.T) {
	s := openTest(t)
	a := Account{ID: 1, ConfigDir: "/cfg/acct-01", KeychainService: "svc1", KeychainAccount: "me", Label: "work", OverlayKind: "symlink"}
	if err := s.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}

	// Fresh row starts with the empty default.
	got, err := s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountUUID != "" {
		t.Fatalf("fresh AccountUUID = %q, want empty", got.AccountUUID)
	}

	if err := s.SetAccountUUID(1, "u-set"); err != nil {
		t.Fatalf("set uuid: %v", err)
	}
	got, err = s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountUUID != "u-set" {
		t.Fatalf("AccountUUID = %q, want u-set", got.AccountUUID)
	}
	// The targeted UPDATE leaves the row's other columns untouched.
	if got.Label != "work" || got.ConfigDir != a.ConfigDir || got.OverlayKind != "symlink" {
		t.Fatalf("SetAccountUUID clobbered other columns: %+v", got)
	}
	if _, ok, err := s.GetAccountByUUID("u-set"); err != nil || !ok {
		t.Fatalf("lookup after set: ok=%v err=%v", ok, err)
	}

	// A generic re-upsert with a zero-value AccountUUID must not wipe the
	// backfilled value: the column is insert-only in UpsertAccount.
	a.Label = "renamed"
	if err := s.UpsertAccount(a); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	got, err = s.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccountUUID != "u-set" {
		t.Fatalf("re-upsert clobbered backfilled uuid: %q", got.AccountUUID)
	}
	if got.Label != "renamed" {
		t.Fatalf("re-upsert did not update label: %q", got.Label)
	}

	// Unknown id fails loud.
	if err := s.SetAccountUUID(99, "u-ghost"); err == nil {
		t.Fatal("SetAccountUUID on unknown id: want error, got nil")
	}
}
