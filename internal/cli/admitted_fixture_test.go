package cli

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

func admitCLITestAccount(t *testing.T, database *store.Store, requested store.Account) store.Account {
	t.Helper()
	for {
		owner := proc.Record{
			RecoveryClass: proc.RecoveryTask,
			PID:           42, StartTime: "1.0", Boot: "cli-test", Comm: "cc-pool",
			Generation: fmt.Sprintf("cli-account-%d", requested.ID),
		}
		reservation, err := database.ReserveAccountIndex(owner)
		if err != nil {
			t.Fatal(err)
		}
		if reservation.ID > requested.ID {
			t.Fatalf("requested account %d is already consumed", requested.ID)
		}
		account := requested
		account.ID = reservation.ID
		account.InstanceID = reservation.InstanceID
		account.Generation = reservation.Generation
		if reservation.ID != requested.ID {
			account.ConfigDir = filepath.Join(
				"/tmp/cc-pool-cli-fixture", fmt.Sprintf("filler-%d-%s", reservation.ID, reservation.InstanceID),
			)
			account.KeychainService = fmt.Sprintf("cli-filler-service-%d", reservation.ID)
			account.KeychainAccount = fmt.Sprintf("cli-filler-account-%d", reservation.ID)
			account.Label = "filler"
		}
		if account.AccountUUID == "" {
			account.AccountUUID = "cli-test-uuid-" + reservation.InstanceID
		}
		proof := cliTestPresentationProof(account, "cli-test-promotion")
		if err := database.PromoteReservedSyncedAccount(reservation, account, proof); err != nil {
			t.Fatal(err)
		}
		fresh := proof
		fresh.FileProvider.ActivationGeneration = "cli-test-admitted"
		admitted, err := database.AdmitSyncedAccount(account, proof, fresh)
		if err != nil {
			t.Fatal(err)
		}
		if !admitted {
			t.Fatal("synced fixture did not clear awaiting-origin state")
		}
		if reservation.ID == requested.ID {
			stored, err := database.GetAccount(reservation.ID)
			if err != nil {
				t.Fatal(err)
			}
			return stored
		}
		if err := database.DeleteAccount(reservation.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func cliTestPresentationProof(account store.Account, activation string) store.PresentationPreparationProof {
	tenantID := "account-" + account.InstanceID
	return store.PresentationPreparationProof{
		CatalogTenantID: tenantID, CatalogGeneration: account.Generation,
		Requested: 1, Desired: 1, Observed: 1, Verified: 1, Applied: 1,
		SourceAuthority: "cli-test", SourceRevision: 1, CatalogRevision: 1,
		ChangeID: "cli-test-change", OperationID: "cli-test-operation",
		PresentationKind: store.PresentationKindFileProvider,
		FileProvider: store.FileProviderPreparationProof{
			TenantID: tenantID, DomainID: "domain-" + account.InstanceID,
			Generation: account.Generation, ActivationGeneration: activation,
			PublicPath: account.ConfigDir,
		},
	}
}
