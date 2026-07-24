package cli

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
)

func admitCLITestAccount(t *testing.T, database *store.Store, requested store.Account) store.Account {
	t.Helper()
	return admitCLITestAccountAtPublicPath(
		t, database, requested, testFileProviderPublicPath(requested.ID),
	)
}

func admitCLITestAccountAtPublicPath(
	t *testing.T,
	database *store.Store,
	requested store.Account,
	targetPublicPath string,
) store.Account {
	t.Helper()
	for {
		owner := proc.Record{
			RecoveryID: pool.CredentialOwnerRecoveryID,
			PID:        42, StartTime: "1.0", Boot: "cli-test", Comm: "cc-pool",
			Generation: cliTestOwnerGeneration(fmt.Sprintf("cli-account-%d", requested.ID)),
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
		account.ConfigDir, err = pool.AccountConfigDir(reservation.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		account.KeychainService, err = pool.AccountKeychainService(reservation.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		publicPath := testFileProviderPublicPath(reservation.ID)
		if reservation.ID == requested.ID {
			publicPath = targetPublicPath
		}
		if err := os.MkdirAll(publicPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := pool.EnsureAccountConfigDir(reservation.InstanceID, publicPath); err != nil {
			t.Fatal(err)
		}
		if reservation.ID != requested.ID {
			account.KeychainAccount = fmt.Sprintf("cli-filler-account-%d", reservation.ID)
			account.Label = "filler"
		}
		if account.AccountUUID == "" {
			account.AccountUUID = "cli-test-uuid-" + reservation.InstanceID
		}
		proof := cliTestPresentationProof(account, publicPath)
		if err := database.PromoteReservedSyncedAccount(reservation, account, proof); err != nil {
			t.Fatal(err)
		}
		fresh := proof
		fence := cliTestAdmissionFence(account)
		if _, err := database.StageSyncedAccountAdmission(account, proof, fresh, fence); err != nil {
			t.Fatal(err)
		}
		candidate, err := database.CommitSyncedAccountAdmissionCandidate(account, fresh, fence)
		if err != nil {
			t.Fatal(err)
		}
		if !candidate {
			t.Fatal("synced fixture did not commit admission candidate")
		}
		settled, err := database.SettleSyncedAccountAdmission(account, fresh, fence)
		if err != nil {
			t.Fatal(err)
		}
		if !settled {
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

func cliTestOwnerGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func cliTestAdmissionFence(account store.Account) store.SyncedCredentialAdmissionFence {
	external := store.CredentialDigest(sha256.Sum256([]byte("cli-test-external-" + account.InstanceID)))
	tokenChain := store.CredentialDigest(sha256.Sum256([]byte("cli-test-chain-" + account.InstanceID)))
	access := store.CredentialDigest(sha256.Sum256([]byte("cli-test-access-" + account.InstanceID)))
	return store.SyncedCredentialAdmissionFence{
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		LocatorDigest: store.CredentialKeychainLocatorDigest(
			account.KeychainService, account.KeychainAccount,
		),
		ExternalStateDigest: external, TokenChainDigest: tokenChain, AccessHashDigest: access,
	}
}

func cliTestPresentationProof(account store.Account, publicPath string) store.FileProviderPresentationIdentity {
	tenantID := "account-" + account.InstanceID
	return store.FileProviderPresentationIdentity{
		TenantID: tenantID, DomainID: "domain-" + account.InstanceID,
		Generation: account.Generation, PublicPath: publicPath,
	}
}
