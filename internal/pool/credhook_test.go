package pool

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/store"
)

func hookCred() *creds.Credential {
	c := &creds.Credential{}
	c.ClaudeAiOauth.AccessToken = "at-hook"
	c.ClaudeAiOauth.RefreshToken = "rt-hook"
	c.ClaudeAiOauth.ExpiresAt = 1_800_000_000_000
	return c
}

func TestWriteCredDoesNotPublishBeforeTerminalSettlement(t *testing.T) {
	fake := credstest.NewFake()
	st := openTestStore(t)
	account := persistTestAccount(t, st, store.Account{
		ID: 3, ConfigDir: t.TempDir(), KeychainService: "svc-hook", KeychainAccount: "user",
	})
	manager := &Manager{Store: st, Creds: fake}
	settlements := 0
	manager.SettleCredentialWrite = func(context.Context, CredentialWriteSettlement) error {
		settlements++
		return nil
	}

	if err := manager.writeCred(t.Context(), account, creds.SourceKeychain, hookCred()); err != nil {
		t.Fatal(err)
	}
	if settlements != 0 {
		t.Fatalf("pre-terminal settlements = %d, want 0", settlements)
	}
	if fake.WriteCount() != 1 {
		t.Fatalf("credential writes = %d, want 1", fake.WriteCount())
	}
}

func TestCredentialWriteSettlementStartupDrainRetriesExactReceipt(t *testing.T) {
	fixture := newInstallFixture(t)
	incoming := envCred("settlement", 5_000)
	started := make(chan CredentialWriteSettlement, 1)
	fixture.m.SettleCredentialWrite = func(ctx context.Context, settlement CredentialWriteSettlement) error {
		started <- settlement
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(t.Context())
	type result struct {
		installed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		installed, err := fixture.m.InstallSyncedCredential(ctx, fixture.a, incoming)
		done <- result{installed: installed, err: err}
	}()
	first := <-started
	cancel()
	got := <-done
	if !got.installed || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("first result = installed:%v err:%v", got.installed, got.err)
	}
	receipt, err := fixture.m.Store.CredentialOperationReceiptByID(first.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.AcknowledgedAt.IsZero() {
		t.Fatalf("failed settlement acknowledged at %v", receipt.AcknowledgedAt)
	}
	replayFailure := errors.New("replay unavailable")
	fixture.m.SettleCredentialWrite = func(context.Context, CredentialWriteSettlement) error {
		t.Fatal("settlement ran after replay failure")
		return nil
	}
	if _, err := replayCredentialOperation(
		t.Context(), fixture.m, fixture.a,
		credentialOperationCodec[struct{}]{
			replay: func(
				context.Context, *Manager, store.Account, store.CredentialOperationReceipt,
			) (struct{}, error) {
				return struct{}{}, replayFailure
			},
		},
		receipt,
	); !errors.Is(err, replayFailure) {
		t.Fatalf("failed replay error = %v", err)
	}
	receipt, err = fixture.m.Store.CredentialOperationReceiptByID(first.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.AcknowledgedAt.IsZero() {
		t.Fatalf("failed replay acknowledged at %v", receipt.AcknowledgedAt)
	}
	if fixture.fk.WriteCount() != 1 {
		t.Fatalf("first credential writes = %d, want 1", fixture.fk.WriteCount())
	}

	var replayed CredentialWriteSettlement
	fixture.m.SettleCredentialWrite = func(_ context.Context, settlement CredentialWriteSettlement) error {
		replayed = settlement
		return nil
	}
	if err := fixture.m.SettlePendingCredentialWrites(t.Context()); err != nil {
		t.Fatalf("settle pending credential writes: %v", err)
	}
	if replayed.OperationID != first.OperationID ||
		!bytes.Equal(replayed.PublicationPayload, first.PublicationPayload) {
		t.Fatalf("settlement replay drifted: first=%+v replayed=%+v", first, replayed)
	}
	if fixture.fk.WriteCount() != 1 {
		t.Fatalf("replay repeated external write: writes=%d", fixture.fk.WriteCount())
	}
	receipt, err = fixture.m.Store.CredentialOperationReceiptByID(first.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.AcknowledgedAt.IsZero() {
		t.Fatal("successful exact settlement did not acknowledge the receipt")
	}
	installed, err := fixture.m.InstallSyncedCredential(t.Context(), fixture.a, incoming)
	if err != nil || installed {
		t.Fatalf("post-settlement install = installed:%v err:%v, want clean skip", installed, err)
	}
	if fixture.fk.WriteCount() != 1 {
		t.Fatalf("post-settlement operation repeated external write: writes=%d", fixture.fk.WriteCount())
	}
}

func TestCredentialWritePublicationWiringFailsClosedPerOperation(t *testing.T) {
	t.Run("missing builder never crosses the external write boundary", func(t *testing.T) {
		fixture := newInstallFixture(t)
		fixture.m.BuildCredentialWritePublication = nil

		installed, err := fixture.m.InstallSyncedCredential(
			t.Context(), fixture.a, envCred("missing-builder", 5_000),
		)
		if installed || err == nil || !strings.Contains(err.Error(), "builder is unavailable") {
			t.Fatalf("install = (%v, %v), want a pre-write builder failure", installed, err)
		}
		if fixture.fk.WriteCount() != 0 {
			t.Fatalf("credential writes = %d, want zero", fixture.fk.WriteCount())
		}
	})

	t.Run("missing settler retains the terminal publication receipt", func(t *testing.T) {
		fixture := newInstallFixture(t)
		fixture.m.SettleCredentialWrite = nil

		installed, err := fixture.m.InstallSyncedCredential(
			t.Context(), fixture.a, envCred("missing-settler", 5_000),
		)
		if !installed || err == nil || !strings.Contains(err.Error(), "settlement is unavailable") {
			t.Fatalf("install = (%v, %v), want a post-write settlement failure", installed, err)
		}
		if fixture.fk.WriteCount() != 1 {
			t.Fatalf("credential writes = %d, want one", fixture.fk.WriteCount())
		}
		receipts, more, err := fixture.m.Store.UnacknowledgedCredentialWriteReceipts(0, 8)
		if err != nil {
			t.Fatal(err)
		}
		if more || len(receipts) != 1 || !receipts[0].AcknowledgedAt.IsZero() {
			t.Fatalf("unacknowledged receipts = %+v more=%v, want one retained receipt", receipts, more)
		}
		if err := fixture.m.SettlePendingCredentialWrites(t.Context()); err == nil ||
			!strings.Contains(err.Error(), "settlement is unavailable") {
			t.Fatalf("startup settlement without wiring = %v, want fail closed", err)
		}
		receipt, err := fixture.m.Store.CredentialOperationReceiptByID(receipts[0].OperationID)
		if err != nil {
			t.Fatal(err)
		}
		if !receipt.AcknowledgedAt.IsZero() {
			t.Fatalf("missing settler acknowledged receipt at %v", receipt.AcknowledgedAt)
		}
	})
}
