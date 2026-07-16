package store

import (
	"errors"
	"testing"
	"time"
)

func TestOverlayAppliedRoundTripBackendAndDeleteCascade(t *testing.T) {
	s := openTest(t)
	a := Account{ID: 1, ConfigDir: "/pool/acct-01", KeychainService: "svc", KeychainAccount: "acct", OverlayKind: "symlink", CreatedAt: time.Now()}
	if err := s.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	want := OverlayApplied{
		AccountID: 1, Backend: "symlink", CanonicalStamp: "canonical-v1",
		SettingsStamp: "settings-v1", StructureStamp: "structure-v1",
		AppStamp: "app-v1", AppliedAt: time.Unix(1234, 0),
	}
	if err := s.SetOverlayApplied(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetOverlayApplied(a.ID)
	if err != nil || !ok {
		t.Fatalf("GetOverlayApplied = (%+v, %v, %v), want row", got, ok, err)
	}
	if got != want {
		t.Fatalf("GetOverlayApplied = %+v, want %+v", got, want)
	}

	want.Backend = "fileprovider"
	want.AppStamp = "app-v2"
	want.AppliedAt = time.Unix(2345, 0)
	if err := s.SetOverlayApplied(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err = s.GetOverlayApplied(a.ID)
	if err != nil || !ok || got != want {
		t.Fatalf("updated GetOverlayApplied = (%+v, %v, %v), want %+v", got, ok, err, want)
	}

	if err := s.DeleteAccount(a.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.GetOverlayApplied(a.ID); err != nil || ok {
		t.Fatalf("GetOverlayApplied after delete = (ok=%v, err=%v), want absent", ok, err)
	}
}

func TestSetOverlayAppliedRefusesRemovedAccount(t *testing.T) {
	s := openTest(t)
	err := s.SetOverlayApplied(OverlayApplied{AccountID: 99, AppliedAt: time.Now()})
	if !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("SetOverlayApplied missing account = %v, want ErrAccountNotFound", err)
	}
}
