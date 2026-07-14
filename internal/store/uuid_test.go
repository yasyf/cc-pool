package store

import (
	"testing"
)

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
