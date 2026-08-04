package pool

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func TestReconcileAccountPresentationRetargetsOnlyStableLink(t *testing.T) {
	home := t.TempDir()
	testhome.Sandbox(t, home)
	st, account, previous := stablePresentationTestAccount(t, filepath.Join(t.TempDir(), "pool-v1.db"))
	manager := &Manager{Store: st}
	if err := EnsureAccountConfigDir(account.InstanceID, previous.PublicPath); err != nil {
		t.Fatal(err)
	}
	target := previous
	target.PublicPath = filepath.Join(home, "Library", "CloudStorage", "Retargeted")
	before := account
	committed, err := manager.ReconcileAccountPresentation(t.Context(), account, target)
	if err != nil {
		t.Fatal(err)
	}
	after, err := st.GetAccount(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after != before || committed.Identity != target {
		t.Fatalf("account/presentation = %+v/%+v, want unchanged account and %+v", after, committed, target)
	}
	link, _ := AccountConfigDir(account.InstanceID)
	assertLinkTarget(t, link, target.PublicPath)
}

func TestRecoverAccountPresentationRepairsHandlesBothCrashSides(t *testing.T) {
	for _, test := range []struct {
		name        string
		prepareLink func(t *testing.T, account store.Account, previous, target string)
	}{
		{name: "database old link new", prepareLink: func(t *testing.T, account store.Account, previous, target string) {
			if err := EnsureAccountConfigDir(account.InstanceID, previous); err != nil {
				t.Fatal(err)
			}
			if err := RetargetAccountConfigDir(account.InstanceID, previous, target); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "database old link missing", prepareLink: func(_ *testing.T, _ store.Account, _, _ string) {}},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			testhome.Sandbox(t, home)
			databasePath := filepath.Join(t.TempDir(), "pool-v1.db")
			st, account, previous := stablePresentationTestAccount(t, databasePath)
			target := previous
			target.PublicPath = filepath.Join(home, "Library", "CloudStorage", "Recovered")
			if err := st.ObserveAccountPresentation(account, target); !errors.Is(err, store.ErrAccountPresentationQuarantined) {
				t.Fatal(err)
			}
			if _, err := st.StageAccountPresentationRepair(account, target); err != nil {
				t.Fatal(err)
			}
			test.prepareLink(t, account, previous.PublicPath, target.PublicPath)
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if err := (&Manager{Store: reopened}).RecoverAccountPresentationRepairs(t.Context()); err != nil {
				t.Fatal(err)
			}
			presentation, err := reopened.AccountPresentation(account.ID)
			if err != nil || presentation.Identity != target {
				t.Fatalf("presentation = %+v err=%v", presentation, err)
			}
			link, _ := AccountConfigDir(account.InstanceID)
			assertLinkTarget(t, link, target.PublicPath)
		})
	}
}

func TestRecoverAccountConfigDirRepairsMissingCommittedLink(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, account, presentation := stablePresentationTestAccount(t, filepath.Join(t.TempDir(), "pool-v1.db"))
	if err := (&Manager{Store: st}).RecoverAccountConfigDir(account); err != nil {
		t.Fatal(err)
	}
	link, _ := AccountConfigDir(account.InstanceID)
	assertLinkTarget(t, link, presentation.PublicPath)
}

func TestReconcileAccountPresentationRepairsMissingSteadyLink(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, account, presentation := stablePresentationTestAccount(t, filepath.Join(t.TempDir(), "pool-v1.db"))
	committed, err := (&Manager{Store: st}).ReconcileAccountPresentation(t.Context(), account, presentation)
	if err != nil || committed.Identity != presentation {
		t.Fatalf("reconcile = %+v err=%v", committed, err)
	}
	link, _ := AccountConfigDir(account.InstanceID)
	assertLinkTarget(t, link, presentation.PublicPath)
}

func TestReconcileAccountPresentationFailsClosedOnForeignLink(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, account, previous := stablePresentationTestAccount(t, filepath.Join(t.TempDir(), "pool-v1.db"))
	link, _ := AccountConfigDir(account.InstanceID)
	if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/foreign/target", link); err != nil {
		t.Fatal(err)
	}
	target := previous
	target.PublicPath = filepath.Join(mustHome(), "Library", "CloudStorage", "Target")
	if _, err := (&Manager{Store: st}).ReconcileAccountPresentation(t.Context(), account, target); !errors.Is(err, ErrAccountConfigLinkConflict) {
		t.Fatalf("foreign link reconcile = %v", err)
	}
	current, err := st.AccountPresentation(account.ID)
	if err != nil || current.Identity != previous {
		t.Fatalf("presentation committed despite foreign link: %+v err=%v", current, err)
	}
}

func TestReconcileAccountPresentationRejectsExecutionIdentityMismatch(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	st, account, presentation := stablePresentationTestAccount(t, filepath.Join(t.TempDir(), "pool-v1.db"))
	account.ConfigDir += "-foreign"
	if _, err := (&Manager{Store: st}).ReconcileAccountPresentation(t.Context(), account, presentation); err == nil {
		t.Fatal("reconcile accepted foreign execution identity")
	}
	if _, err := st.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected identity created quarantine: %v", err)
	}
}

func stablePresentationTestAccount(
	t *testing.T,
	databasePath string,
) (*store.Store, store.Account, store.FileProviderPresentationIdentity) {
	t.Helper()
	st, err := store.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	owner := store.OwnerRecord(`{"v":2,"nonce":"stable-presentation"}`)
	reservation, err := st.ReserveAccountIndex(owner)
	if err != nil {
		t.Fatal(err)
	}
	configDir, err := AccountConfigDir(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	service, err := AccountKeychainService(reservation.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	publicPath := filepath.Join(mustHome(), "Library", "CloudStorage", "Initial")
	account := commitPoolTestAccountAtPresentation(t, st, store.Account{
		ID: reservation.ID, ConfigDir: configDir, KeychainService: service,
		KeychainAccount: "owner", AccountUUID: "external-uuid",
	}, reservation, owner, publicPath)
	t.Cleanup(func() {
		if _, err := os.Stat(databasePath); err == nil {
			_ = st.Close()
		}
	})
	return st, account, poolTestPresentationProof(reservation, publicPath)
}
