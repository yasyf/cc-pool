package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	ownerExclusionChildEnv    = "CC_POOL_STORE_OWNER_EXCLUSION_CHILD"
	ownerExclusionIntent      = "owner-exclusion"
	ownerExclusionStagedName  = "staged.json"
	ownerExclusionChildParked = 10 * time.Minute
)

type ownerExclusionStagedFence struct {
	Token string `json:"token"`
	Owner []byte `json:"owner"`
	Epoch uint64 `json:"epoch"`
}

func TestMain(m *testing.M) {
	if dir := os.Getenv(ownerExclusionChildEnv); dir != "" {
		if err := runOwnerExclusionChild(dir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		time.Sleep(ownerExclusionChildParked)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runOwnerExclusionChild(dir string) error {
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		return err
	}
	owner, err := MintOwnerRecord(time.Now())
	if err != nil {
		return err
	}
	account, err := s.GetAccount(1)
	if err != nil {
		return err
	}
	request, err := ownerExclusionRequest(account, owner)
	if err != nil {
		return err
	}
	begin, err := s.BeginCredentialOperation(request)
	if err != nil {
		return err
	}
	if !begin.Created || begin.Active == nil {
		return fmt.Errorf("child begin = %+v", begin)
	}
	fence := begin.Active.Fence()
	if _, err := s.MarkCredentialOperationApplying(fence, nil); err != nil {
		return err
	}
	if _, err := s.StageCredentialOperationPublication(
		fence, []byte(`{"version":1,"staged":"before-applied"}`),
	); err != nil {
		return err
	}
	staged, err := json.Marshal(ownerExclusionStagedFence{
		Token: fence.Token, Owner: fence.Owner, Epoch: fence.Epoch,
	})
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, ownerExclusionStagedName+".tmp")
	// #nosec G703 G304 -- dir is the parent test's private temporary root.
	if err := os.WriteFile(tmp, staged, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, ownerExclusionStagedName)) // #nosec G703 -- same private root.
}

func ownerExclusionRequest(account Account, owner OwnerRecord) (BeginCredentialOperationRequest, error) {
	locator := CredentialKeychainLocatorDigest(account.KeychainService, account.KeychainAccount)
	intent := credentialOperationTestDigest(ownerExclusionIntent)
	expected := CredentialExternalState{
		Keychain: CredentialSlotObservation{State: CredentialSlotEmpty},
	}
	operationID, err := NewCredentialOperationID(
		account.InstanceID, account.Generation, account.ConfigDir,
		account.KeychainService, account.KeychainAccount,
		CredentialOperationEnsureFresh, CredentialTargetKeychain,
		locator, expected, intent,
	)
	if err != nil {
		return BeginCredentialOperationRequest{}, err
	}
	return BeginCredentialOperationRequest{
		OperationID: operationID, AccountID: account.ID,
		AccountInstanceID: account.InstanceID, AccountGeneration: account.Generation,
		ConfigDir: account.ConfigDir, KeychainService: account.KeychainService,
		KeychainAccount: account.KeychainAccount, LocatorDigest: locator,
		Owner: owner, Kind: CredentialOperationEnsureFresh,
		Target: CredentialTargetKeychain, IntentDigest: intent, Expected: expected,
	}, nil
}

// TestTwoOwnerExclusionAcrossProcessDeath is the store/pool arm of the design's
// two-owner exclusion walk: a real child process owns a lane, dies by SIGKILL
// between Stage and Applied, and a successor claims the lane with exactly one
// epoch bump; the dead generation's fence then loses every write.
// TODO(dk-v021 integration): add the daemon-boot arm — spawn the real daemon
// spec (Lane D), SIGKILL it, and assert Serve → Start → ClaimForeignLanes
// performs this claim end to end.
func TestTwoOwnerExclusionAcrossProcessDeath(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	account := admitTestAccount(t, s, Account{
		ID: 1, ConfigDir: "/tmp/acct-exclusion",
		KeychainService: "service-exclusion", KeychainAccount: "account-exclusion",
		CreatedAt: time.Unix(1_800_000_000, 0),
	})

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable) // #nosec G204 -- self-exec of this .test binary.
	child.Env = append(os.Environ(), ownerExclusionChildEnv+"="+dir)
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	}()

	stagedPath := filepath.Join(dir, ownerExclusionStagedName)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(stagedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child never staged its publication")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()

	// #nosec G304 -- stagedPath is beneath this test's private temporary root.
	encoded, err := os.ReadFile(stagedPath)
	if err != nil {
		t.Fatal(err)
	}
	var staged ownerExclusionStagedFence
	if err := json.Unmarshal(encoded, &staged); err != nil {
		t.Fatal(err)
	}
	staleFence := CredentialOperationFence{
		Token: staged.Token, Owner: OwnerRecord(staged.Owner), Epoch: staged.Epoch,
	}
	if staleFence.Epoch != 1 {
		t.Fatalf("child fence epoch = %d, want 1", staleFence.Epoch)
	}

	successor, err := MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	foreign, more, err := s.CredentialOperationsNotOwnedBy(successor, 0, CredentialOperationPageLimit)
	if err != nil || more || len(foreign) != 1 {
		t.Fatalf("foreign lanes = %+v more=%v err=%v", foreign, more, err)
	}
	orphan := foreign[0]
	if orphan.Token != staleFence.Token || !bytes.Equal(orphan.Owner, staleFence.Owner) ||
		orphan.State != CredentialOperationApplying || len(orphan.PublicationPayload) == 0 {
		t.Fatalf("orphan lane = %+v", orphan)
	}
	if orphan.AccountID != account.ID {
		t.Fatalf("orphan account = %d, want %d", orphan.AccountID, account.ID)
	}

	taken, err := s.TakeoverCredentialOperation(orphan.Fence(), successor)
	if err != nil {
		t.Fatal(err)
	}
	if taken.OwnerEpoch != staleFence.Epoch+1 || !bytes.Equal(taken.Owner, successor) {
		t.Fatalf("claimed lane = epoch %d owner %q, want epoch %d successor bytes",
			taken.OwnerEpoch, taken.Owner, staleFence.Epoch+1)
	}

	if _, err := s.StageCredentialOperationPublication(
		staleFence, []byte(`{"version":1,"staged":"stale"}`),
	); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("stale-fence stage = %v, want ErrCredentialOperationOwner", err)
	}
	if _, err := s.MarkCredentialOperationApplied(
		staleFence, taken.Expected, CredentialTerminalSucceeded,
		CredentialResultUnchanged, CredentialFailureNone, nil,
	); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("stale-fence applied = %v, want ErrCredentialOperationOwner", err)
	}
	if _, err := s.TakeoverCredentialOperation(staleFence, successor); !errors.Is(err, ErrCredentialOperationOwner) {
		t.Fatalf("stale-fence re-takeover = %v, want ErrCredentialOperationOwner", err)
	}

	receipt, err := s.ResolveCredentialOperation(
		taken.Fence(), taken.Expected, CredentialTerminalQuarantined,
		CredentialResultAmbiguous, CredentialFailureInternal, nil,
		time.Now().Add(10*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(receipt.Owner, successor) || receipt.OwnerEpoch != taken.OwnerEpoch {
		t.Fatalf("settled receipt owner = %q epoch %d", receipt.Owner, receipt.OwnerEpoch)
	}
}
