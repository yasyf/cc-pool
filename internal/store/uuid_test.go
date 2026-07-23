package store

import (
	"errors"
	"testing"
)

func TestGetAccountByUUID(t *testing.T) {
	s := openTest(t)

	withUUID := Account{ID: 1, ConfigDir: "/cfg/acct-01", KeychainService: "svc1", KeychainAccount: "me", Label: "alpha", AccountUUID: "u-alpha"}
	other := Account{ID: 2, ConfigDir: "/cfg/acct-02", KeychainService: "svc2", KeychainAccount: "me", Label: "beta", AccountUUID: "u-beta"}
	for _, a := range []Account{withUUID, other} {
		admitTestAccount(t, s, a)
	}

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
		{name: "empty-never-matches", uuid: "", wantOK: false},
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

func TestDuplicateAccountUUIDIsRejected(t *testing.T) {
	s := openTest(t)
	admitTestAccount(t, s, Account{ID: 1, ConfigDir: "/cfg/acct-01", KeychainService: "svc1", KeychainAccount: "me", AccountUUID: "u-dup"})
	reservation := mustReserve(t, s)
	second := Account{
		ID: reservation.ID, InstanceID: reservation.InstanceID, Generation: reservation.Generation,
		ConfigDir: "/cfg/acct-02", KeychainService: "svc2", KeychainAccount: "me", AccountUUID: "u-dup",
	}
	proof := presentationTestProof(second, second.ConfigDir, "activation-duplicate")
	if err := s.PromoteReservedSyncedAccount(reservation, second, proof); !errors.Is(err, ErrDuplicateAccountUUID) {
		t.Fatalf("duplicate promotion = %v", err)
	}
}

// TestAccountsByUUID pins exact lookup and empty-UUID behavior.
func TestAccountsByUUID(t *testing.T) {
	s := openTest(t)
	for _, a := range []Account{
		{ID: 1, ConfigDir: "/cfg/acct-01", KeychainService: "svc1", KeychainAccount: "me", AccountUUID: "u-solo"},
		{ID: 2, ConfigDir: "/cfg/acct-02", KeychainService: "svc2", KeychainAccount: "me", AccountUUID: "u-other"},
	} {
		admitTestAccount(t, s, a)
	}

	solo, err := s.AccountsByUUID("u-solo")
	if err != nil || len(solo) != 1 || solo[0].ID != 1 {
		t.Fatalf("AccountsByUUID(u-solo) = %+v (err %v), want exactly id 1", solo, err)
	}
	if got, err := s.AccountsByUUID("u-missing"); err != nil || len(got) != 0 {
		t.Fatalf("AccountsByUUID(u-missing) = %+v (err %v), want none", got, err)
	}
	if got, err := s.AccountsByUUID(""); err != nil || got != nil {
		t.Fatalf("AccountsByUUID(\"\") = %+v (err %v); empty must never match", got, err)
	}
}
