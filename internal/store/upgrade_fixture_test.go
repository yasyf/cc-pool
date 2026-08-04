package store

import (
	"bytes"
	"testing"
	"time"
)

// The goldens are literal v0.20.9 owner_record bytes: proc.Record values
// encoded by daemonkit@v0.20.9's Record.MarshalJSON from the module cache,
// Validate- and round-trip-verified under that exact codec before capture.
const (
	upgradeGoldenOwnerA = `{"recovery_id":"com.yasyf.cc-pool.credential-owner.v1","pid":4242,"start_time":"1722700000.123456","boot":"9f2a6c1e-5b4d-4e3a-8890-abcdef012345","comm":"cc-pool","executable":"/opt/homebrew/Cellar/cc-pool/0.20.9/bin/cc-pool","audit_token":[245,1,0,0,245,1,0,0,245,1,0,0,20,0,0,0,245,1,0,0,146,16,0,0,109,135,1,0,105,122,0,0],"generation":"5aa1b2c3d4e5f60718293a4b5c6d7e8f","process_group":false,"session_id":0,"role":"","operation_id":"","stop_session":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"preparation_nonce":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"runtime_protocol":0,"target_process_generation":null,"stop_authority_state":"","expires_unix_milli":0}`
	upgradeGoldenOwnerB = `{"recovery_id":"com.yasyf.cc-pool.credential-owner.v1","pid":577,"start_time":"1722700123.654321","boot":"9f2a6c1e-5b4d-4e3a-8890-abcdef012345","comm":"cc-pool","executable":"/opt/homebrew/Cellar/cc-pool/0.20.9/bin/cc-pool","audit_token":[245,1,0,0,245,1,0,0,245,1,0,0,20,0,0,0,245,1,0,0,65,2,0,0,109,135,1,0,105,122,0,0],"generation":"00112233445566778899aabbccddeeff","process_group":false,"session_id":0,"role":"","operation_id":"","stop_session":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"preparation_nonce":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"runtime_protocol":0,"target_process_generation":null,"stop_authority_state":"","expires_unix_milli":0}`
)

func TestUpgradeAdoptsVZeroTwentyNineOwnerRowsAcrossAllFiveTables(t *testing.T) {
	s := openTest(t)
	now := time.Unix(1_900_000_000, 0)
	s.now = func() time.Time { return now }
	goldenA := OwnerRecord(upgradeGoldenOwnerA)
	goldenB := OwnerRecord(upgradeGoldenOwnerB)
	if goldenA.Validate() != nil || goldenB.Validate() != nil {
		t.Fatal("golden v0.20.9 owner bytes fail OwnerRecord validation")
	}
	v2, err := MintOwnerRecord(now)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(v2, []byte(`"v":2`)) || bytes.Contains(goldenA, []byte(`"v":`)) {
		t.Fatalf("owner eras are confusable: v2=%q golden=%q", v2, goldenA)
	}

	laneAccount := credentialOperationTestAccountID(t, s, 1)
	receiptAccount := credentialOperationTestAccountID(t, s, 2)
	mutationAccount := credentialOperationTestAccountID(t, s, 3)
	committedAccount := credentialOperationTestAccountID(t, s, 4)

	stagedPayload := []byte(`{"version":1,"upgrade":"staged"}`)
	laneBegin, err := s.BeginCredentialOperation(credentialOperationTestRequest(
		t, laneAccount, CredentialOperationEnsureFresh, CredentialTargetKeychain,
		credentialOperationTestState("upgrade-before", ""), "upgrade-fresh", goldenA,
	))
	if err != nil {
		t.Fatal(err)
	}
	laneFence := laneBegin.Active.Fence()
	if _, err := s.MarkCredentialOperationApplying(laneFence, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StageCredentialOperationPublication(laneFence, stagedPayload); err != nil {
		t.Fatal(err)
	}

	installPayload := []byte(`{"version":1,"upgrade":"install"}`)
	installOutcome := credentialOperationTestState("upgrade-installed", "")
	installBegin, err := s.BeginCredentialOperation(credentialOperationTestRequest(
		t, receiptAccount, CredentialOperationInstallSynced, CredentialTargetKeychain,
		credentialOperationTestState("", ""), "upgrade-install", goldenB,
	))
	if err != nil {
		t.Fatal(err)
	}
	installFence := installBegin.Active.Fence()
	if _, err := s.MarkCredentialOperationApplying(installFence, installPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkCredentialOperationApplied(
		installFence, installOutcome, CredentialTerminalSucceeded,
		CredentialResultInstalled, CredentialFailureNone, installPayload,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitCredentialOperation(
		installFence, installOutcome, nil, now.Add(10*time.Minute),
	); err != nil {
		t.Fatal(err)
	}

	reloginBegin, err := s.BeginAccountMutation(t.Context(), existingAccountMutationTestRequest(
		t, mutationAccount, AccountMutationRelogin, goldenA,
	))
	if err != nil {
		t.Fatal(err)
	}

	committedRequest := existingAccountMutationTestRequest(
		t, committedAccount, AccountMutationRelogin, goldenB,
	)
	committedBegin, err := s.BeginAccountMutation(t.Context(), committedRequest)
	if err != nil {
		t.Fatal(err)
	}
	committedFence, err := s.MarkAccountMutationInputProvided(
		committedBegin.Active.Fence(), credentialOperationTestDigest("upgrade-input"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if committedFence, err = s.MarkAccountMutationApplying(committedFence); err != nil {
		t.Fatal(err)
	}
	if committedFence, err = s.MarkAccountMutationApplied(
		committedFence, credentialOperationTestDigest("upgrade-written"),
	); err != nil {
		t.Fatal(err)
	}
	if committedFence, err = s.SetAccountMutationMetadata(
		committedFence, "upgrade-label", "upgrade-uuid",
	); err != nil {
		t.Fatal(err)
	}
	if committedFence, err = s.MarkAccountMutationPublishing(committedFence); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitAccountMutation(committedFence, now.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	freeReservation, err := s.ReserveAccountIndex(goldenA)
	if err != nil {
		t.Fatal(err)
	}
	addReservation, err := s.ReserveAccountIndex(goldenB)
	if err != nil {
		t.Fatal(err)
	}
	addRequest := accountMutationTestRequest(t, addReservation, AccountMutationAdd)
	addRequest.Owner = goldenB
	addBegin, err := s.BeginAccountMutation(t.Context(), addRequest)
	if err != nil {
		t.Fatal(err)
	}

	scanned, err := s.CredentialOperation(laneAccount.ID)
	if err != nil || !bytes.Equal(scanned.Owner, goldenA) ||
		scanned.State != CredentialOperationApplying ||
		!bytes.Equal(scanned.PublicationPayload, stagedPayload) {
		t.Fatalf("v0.20.9-owned lane scan = %+v err=%v", scanned, err)
	}

	foreignLanes, more, err := s.CredentialOperationsNotOwnedBy(v2, 0, CredentialOperationPageLimit)
	if err != nil || more || len(foreignLanes) != 1 || foreignLanes[0].AccountID != laneAccount.ID {
		t.Fatalf("foreign credential lanes = %+v more=%v err=%v", foreignLanes, more, err)
	}
	foreignMutations, more, err := s.AccountMutationsNotOwnedBy(v2, 0, CredentialOperationPageLimit)
	if err != nil || more || len(foreignMutations) != 2 {
		t.Fatalf("foreign account mutations = %+v more=%v err=%v", foreignMutations, more, err)
	}
	foreignPending, more, err := s.PendingAddReservationsNotOwnedBy(v2, 0, CredentialOperationPageLimit)
	if err != nil || more || len(foreignPending) != 1 || foreignPending[0].ID != freeReservation.ID {
		t.Fatalf("foreign pending adds = %+v more=%v err=%v", foreignPending, more, err)
	}

	claimedLane, err := s.TakeoverCredentialOperation(foreignLanes[0].Fence(), v2)
	if err != nil || claimedLane.OwnerEpoch != foreignLanes[0].OwnerEpoch+1 ||
		!bytes.Equal(claimedLane.Owner, v2) {
		t.Fatalf("claimed credential lane = %+v err=%v", claimedLane, err)
	}
	claimedRelogin, err := s.TakeoverAccountMutation(t.Context(), reloginBegin.Active.Fence(), v2)
	if err != nil || claimedRelogin.OwnerEpoch != reloginBegin.Active.OwnerEpoch+1 ||
		!bytes.Equal(claimedRelogin.Owner, v2) {
		t.Fatalf("claimed relogin mutation = %+v err=%v", claimedRelogin, err)
	}
	claimedAdd, err := s.TakeoverAccountMutation(t.Context(), addBegin.Active.Fence(), v2)
	if err != nil || claimedAdd.OwnerEpoch != addBegin.Active.OwnerEpoch+1 ||
		!bytes.Equal(claimedAdd.Owner, v2) {
		t.Fatalf("claimed add mutation = %+v err=%v", claimedAdd, err)
	}
	var swappedPendingOwner []byte
	if err := s.db.QueryRow(
		`SELECT owner_record FROM pending_adds WHERE id=?`, addReservation.ID,
	).Scan(&swappedPendingOwner); err != nil || !bytes.Equal(swappedPendingOwner, v2) {
		t.Fatalf("claimed add pending owner = %q err=%v", swappedPendingOwner, err)
	}

	if err := s.ReleaseAccountIndex(freeReservation); err != nil {
		t.Fatalf("release v0.20.9-owned reservation = %v", err)
	}

	installQuery := CredentialOperationEvidenceQuery{
		AccountID: receiptAccount.ID, AccountInstanceID: receiptAccount.InstanceID,
		AccountGeneration: receiptAccount.Generation, ConfigDir: receiptAccount.ConfigDir,
		KeychainService: receiptAccount.KeychainService, KeychainAccount: receiptAccount.KeychainAccount,
		LocatorDigest: CredentialKeychainLocatorDigest(
			receiptAccount.KeychainService, receiptAccount.KeychainAccount,
		),
		Kind: CredentialOperationInstallSynced, Target: CredentialTargetKeychain,
		IntentDigest: credentialOperationTestDigest("upgrade-install"),
	}
	active, evidence, err := s.CredentialOperationEvidence(installQuery)
	if err != nil || active != nil || evidence == nil ||
		!bytes.Equal(evidence.Owner, goldenB) ||
		!bytes.Equal(evidence.PublicationPayload, installPayload) {
		t.Fatalf("evidence replay = active=%v receipt=%+v err=%v", active, evidence, err)
	}

	pendingWrites, more, err := s.UnacknowledgedCredentialWriteReceipts(0, CredentialOperationPageLimit)
	if err != nil || more || len(pendingWrites) != 1 ||
		!bytes.Equal(pendingWrites[0].PublicationPayload, installPayload) ||
		!bytes.Equal(pendingWrites[0].Owner, goldenB) {
		t.Fatalf("pending publication replay = %+v more=%v err=%v", pendingWrites, more, err)
	}
	if err := s.AcknowledgeCredentialOperation(pendingWrites[0].Token); err != nil {
		t.Fatal(err)
	}
	acked, err := s.CredentialOperationReceipt(pendingWrites[0].Token)
	if err != nil || acked.AcknowledgedAt.IsZero() {
		t.Fatalf("acknowledged v0.20.9-owned receipt = %+v err=%v", acked, err)
	}

	committedReceipt, err := s.AccountMutationReceipt(committedRequest.OperationID)
	if err != nil || !bytes.Equal(committedReceipt.Owner, goldenB) ||
		committedReceipt.Terminal != AccountMutationCommitted {
		t.Fatalf("v0.20.9-owned mutation receipt = %+v err=%v", committedReceipt, err)
	}
	if err := s.AcknowledgeAccountMutationReceipt(committedRequest.OperationID); err != nil {
		t.Fatal(err)
	}

	settled, err := s.ResolveCredentialOperation(
		claimedLane.Fence(), claimedLane.Expected, CredentialTerminalQuarantined,
		CredentialResultAmbiguous, CredentialFailureInternal, nil, now.Add(10*time.Minute),
	)
	if err != nil || !bytes.Equal(settled.Owner, v2) || settled.OwnerEpoch != claimedLane.OwnerEpoch {
		t.Fatalf("new v2-owned receipt = %+v err=%v", settled, err)
	}

	if remaining, _, err := s.CredentialOperationsNotOwnedBy(v2, 0, CredentialOperationPageLimit); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("unclaimed credential lanes remain = %+v err=%v", remaining, err)
	}
	if remaining, _, err := s.AccountMutationsNotOwnedBy(v2, 0, CredentialOperationPageLimit); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("unclaimed account mutations remain = %+v err=%v", remaining, err)
	}
	if remaining, _, err := s.PendingAddReservationsNotOwnedBy(v2, 0, CredentialOperationPageLimit); err != nil ||
		len(remaining) != 0 {
		t.Fatalf("unclaimed pending adds remain = %+v err=%v", remaining, err)
	}
}
