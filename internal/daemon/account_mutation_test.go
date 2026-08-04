package daemon

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/accountterminal"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestAccountMutationStartOrAttachCoalescesExactIntentBeforeCredentialIO(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	first, err := runAccountMutationTest(t, s, request)
	if err != nil || first.State != AccountMutationAwaitingInput {
		t.Fatalf("first begin = %+v err=%v", first, err)
	}
	touched := len(fake.TouchedServices())
	second, err := runAccountMutationTest(t, s, request)
	if err != nil || second.OperationID != first.OperationID {
		t.Fatalf("second begin = %+v err=%v; want operation %x", second, err, first.OperationID)
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("coalesced begin performed credential I/O: touched %d -> %d", touched, got)
	}
	request.Label = "different-intent"
	if _, err := runAccountMutationTest(t, s, request); err == nil {
		t.Fatal("different-label begin attached to active mutation")
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("mismatched begin performed credential I/O: touched %d -> %d", touched, got)
	}
}

func TestAccountMutationReceiptReplaysBeforeCredentialIO(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	beginRequest := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, beginRequest)
	if err != nil {
		t.Fatal(err)
	}
	cancel := beginRequest
	cancel.Action = AccountMutationCancel
	cancel.Fence = begin.Fence
	terminal, err := runAccountMutationTest(t, s, cancel)
	if err != nil || terminal.State != AccountMutationCancelled {
		t.Fatalf("cancel = %+v err=%v", terminal, err)
	}
	touched := len(fake.TouchedServices())
	replayed, err := runAccountMutationTest(t, s, beginRequest)
	if err != nil || replayed.OperationID != begin.OperationID || replayed.State != AccountMutationCancelled {
		t.Fatalf("receipt replay = %+v err=%v", replayed, err)
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("receipt replay performed credential I/O: touched %d -> %d", touched, got)
	}
	beginRequest.Label = "different-intent"
	if _, err := runAccountMutationTest(t, s, beginRequest); err == nil {
		t.Fatal("different-label begin replayed an unrelated receipt")
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("receipt mismatch performed credential I/O: touched %d -> %d", touched, got)
	}
}

func TestAccountMutationAddReceiptZeroScopeReplaysWithoutReservationOrCredentialIO(t *testing.T) {
	s, fake, _ := newAccountMutationTestServer(t, false)
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationStartOrAttach, Label: "new-account",
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil || begin.AccountID <= 0 {
		t.Fatalf("add begin = %+v err=%v", begin, err)
	}
	cancel := request
	cancel.Action = AccountMutationCancel
	cancel.Fence = begin.Fence
	if _, err := runAccountMutationTest(t, s, cancel); err != nil {
		t.Fatal(err)
	}
	touched := len(fake.TouchedServices())
	replayed, err := runAccountMutationTest(t, s, request)
	if err != nil || replayed.OperationID != begin.OperationID {
		t.Fatalf("add receipt replay = %+v err=%v", replayed, err)
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("add receipt replay performed credential I/O: touched %d -> %d", touched, got)
	}
	reservation, err := s.m.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ID != begin.AccountID {
		t.Fatalf("cancel did not transactionally consume add reservation: next=%d want=%d", reservation.ID, begin.AccountID)
	}
}

func TestAccountMutationAddCommitsOneImmutablePresentationIdentity(t *testing.T) {
	s, fake, _ := newAccountMutationTestServer(t, false)
	var preparations atomic.Int64
	s.provisionPresentationIdentity = func(
		_ context.Context,
		account store.Account,
	) (store.FileProviderPresentationIdentity, error) {
		preparations.Add(1)
		return accountMutationTestPresentation(t, account)
	}
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationStartOrAttach, Label: "new-account",
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil || begin.State != AccountMutationAwaitingInput {
		t.Fatalf("add begin = %+v err=%v", begin, err)
	}
	mutation, err := s.m.Store.AccountMutation(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	wantConfigDir, err := pool.AccountConfigDir(mutation.AccountInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	wantService, err := pool.AccountKeychainService(mutation.AccountInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if mutation.ConfigDir != wantConfigDir || mutation.KeychainService != wantService ||
		mutation.PresentationIdentity.PublicPath == mutation.ConfigDir {
		t.Fatalf("add execution/presentation identity = %+v", mutation)
	}
	if target, err := os.Readlink(mutation.ConfigDir); err != nil || target != mutation.PresentationIdentity.PublicPath {
		t.Fatalf("stable execution link before add input = %q, %v", target, err)
	}
	touched := fake.TouchedServices()
	for _, service := range touched {
		if service != wantService {
			t.Fatalf("add touched presentation-derived credential service %q; stable=%q", service, wantService)
		}
	}
	if _, ok := fake.Get(creds.ServiceName(mutation.PresentationIdentity.PublicPath), mutation.KeychainAccount); ok {
		t.Fatal("add probed a presentation-derived credential slot")
	}
	replayed, err := runAccountMutationTest(t, s, request)
	if err != nil || replayed.OperationID != begin.OperationID || replayed.ConfigDir != wantConfigDir {
		t.Fatalf("replayed add bind = %+v err=%v", replayed, err)
	}
	if len(fake.TouchedServices()) != len(touched) {
		t.Fatal("replayed add bind repeated credential I/O")
	}
	if target, err := os.Readlink(mutation.ConfigDir); err != nil || target != mutation.PresentationIdentity.PublicPath {
		t.Fatalf("replayed stable execution link = %q, %v", target, err)
	}
	fence, err := s.m.Store.MarkAccountMutationInputProvided(
		mutation.Fence(), accountMutationInputDigest(mutation.OperationID),
	)
	if err == nil {
		fence, err = s.m.Store.MarkAccountMutationApplying(fence)
	}
	if err != nil {
		t.Fatal(err)
	}
	mutation, err = s.m.Store.AccountMutation(fence.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := s.m.PrepareReservedAdd(
		t.Context(), accountMutationReservation(mutation), mutation.PresentationIdentity.PublicPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	if pending.ConfigDir != mutation.ConfigDir || pending.KeychainService != mutation.KeychainService {
		t.Fatalf("prepared add identity = %+v, mutation = %+v", pending, mutation)
	}
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "add-access"
	credential.ClaudeAiOauth.RefreshToken = "add-refresh"
	credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
	fake.Put(mutation.KeychainService, mutation.KeychainAccount, credential)
	if err := s.m.WriteIdentity(
		t.Context(), mutation.AccountID, mutation.ConfigDir,
		[]byte(`{"accountUuid":"added-uuid","emailAddress":"added@example.com"}`),
	); err != nil {
		t.Fatal(err)
	}
	completed, err := s.finishAccountMutation(t.Context(), mutation)
	if err != nil || !completed.Completed {
		t.Fatalf("completed add = %+v err=%v", completed, err)
	}
	if preparations.Load() != 1 {
		t.Fatalf("presentation provisions = %d, want one", preparations.Load())
	}
	receipt, err := s.m.Store.AccountMutationReceipt(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.PublicationPending {
		t.Fatal("completed add retained publication fence")
	}
	presentation, err := s.m.Store.AccountPresentation(receipt.AccountID)
	if err != nil || presentation.Identity != receipt.PresentationIdentity {
		t.Fatalf("committed presentation = %+v receipt=%+v err=%v", presentation, receipt, err)
	}
	if active, err := s.m.Store.ListActiveAccounts(); err != nil || len(active) != 1 || active[0].ID != receipt.AccountID {
		t.Fatalf("active accounts = %+v err=%v", active, err)
	}
	request.Action = AccountMutationStartOrAttach
	request.Fence = AccountMutationFence{}
	replayed, err = runAccountMutationTest(t, s, request)
	if err != nil || !replayed.Completed || replayed.OperationID != begin.OperationID {
		t.Fatalf("receipt replay = %+v err=%v", replayed, err)
	}
}

func TestAccountMutationAddStableIdentitySurvivesStoreReopen(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	dbPath := filepath.Join(t.TempDir(), "account-mutation-restart.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	s, _, _ := newAccountMutationTestServerWithStore(t, st, false)
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationStartOrAttach, Label: "restart-account",
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := store.OpenReadOnly(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	mutation, err := reopened.AccountMutation(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	wantConfigDir, err := pool.AccountConfigDir(mutation.AccountInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	wantService, err := pool.AccountKeychainService(mutation.AccountInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if mutation.ConfigDir != wantConfigDir || mutation.KeychainService != wantService ||
		mutation.PresentationIdentity.PublicPath == wantConfigDir {
		t.Fatalf("reopened add identity = %+v", mutation)
	}
	if target, err := os.Readlink(wantConfigDir); err != nil || target != mutation.PresentationIdentity.PublicPath {
		t.Fatalf("reopened stable execution link = %q, %v", target, err)
	}
}

func TestAccountMutationAddExecutionConflictRetainsStableReplayState(t *testing.T) {
	s, _, _ := newAccountMutationTestServer(t, false)
	var injected atomic.Bool
	var conflictedPath string
	s.provisionPresentationIdentity = func(
		_ context.Context,
		account store.Account,
	) (store.FileProviderPresentationIdentity, error) {
		identity, err := accountMutationTestPresentation(t, account)
		if err != nil || injected.Swap(true) {
			return identity, err
		}
		conflictedPath = account.ConfigDir
		if err := os.MkdirAll(filepath.Dir(conflictedPath), 0o700); err != nil {
			return store.FileProviderPresentationIdentity{}, err
		}
		if err := os.Symlink("/foreign/presentation", conflictedPath); err != nil {
			return store.FileProviderPresentationIdentity{}, err
		}
		return identity, nil
	}
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationStartOrAttach, Label: "failed-account",
	}
	if _, err := runAccountMutationTest(t, s, request); err == nil {
		t.Fatal("add bind unexpectedly replaced a foreign execution link")
	}
	mutation, err := s.m.Store.ActiveAccountMutation(1)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := s.m.Store.PendingAddIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if len(reservations) != 1 || reservations[0] != mutation.AccountID {
		t.Fatalf("failed bind did not retain exact reservation: %v", reservations)
	}
	if mutation.State != store.AccountMutationAwaitingPresentation {
		t.Fatalf("failed bind mutation = %+v", mutation)
	}
	configDir, err := pool.AccountConfigDir(mutation.AccountInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := expectedPresentationIdentity(store.Account{
		ID: mutation.AccountID, InstanceID: mutation.AccountInstanceID, Generation: mutation.AccountGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if configDir != conflictedPath {
		t.Fatalf("conflicted path = %q, stable path = %q", conflictedPath, configDir)
	}
	if target, err := os.Readlink(configDir); err != nil || target != "/foreign/presentation" {
		t.Fatalf("foreign execution link = %q, %v", target, err)
	}
	if err := os.Remove(configDir); err != nil {
		t.Fatal(err)
	}
	replayed, err := runAccountMutationTest(t, s, request)
	if err != nil || replayed.OperationID != [32]byte(mutation.OperationID) ||
		replayed.State != AccountMutationAwaitingInput || replayed.ConfigDir != configDir {
		t.Fatalf("replayed bind = %+v err=%v", replayed, err)
	}
	if target, err := os.Readlink(configDir); err != nil || target != identity.PublicPath {
		t.Fatalf("replayed stable execution link = %q, %v", target, err)
	}
}

func TestAccountMutationAddRetiresUnboundPresentationBeforeCancellation(t *testing.T) {
	s, _, _ := newAccountMutationTestServer(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	var publicPath string
	var retired store.PendingAccountReservation
	s.provisionPresentationIdentity = func(
		_ context.Context,
		account store.Account,
	) (store.FileProviderPresentationIdentity, error) {
		identity, err := accountMutationTestPresentation(t, account)
		if err != nil {
			return store.FileProviderPresentationIdentity{}, err
		}
		publicPath = identity.PublicPath
		identity.TenantID = "invalid-tenant"
		cancel()
		return identity, nil
	}
	s.m.RetirePendingAdd = func(
		cleanup context.Context,
		reservation store.PendingAccountReservation,
	) (pool.PendingAddRetirementProof, error) {
		if err := cleanup.Err(); err != nil {
			return pool.PendingAddRetirementProof{}, fmt.Errorf("cleanup inherited cancellation: %w", err)
		}
		retired = reservation
		return pool.PendingAddRetirementProof{
			AccountID: reservation.ID, AccountInstanceID: reservation.InstanceID,
			AccountGeneration: reservation.Generation, PublicPath: publicPath,
		}, nil
	}
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationStartOrAttach, Label: "cancel-account",
	}
	if _, err := s.runAccountMutation(ctx, daemonkit.Session{}, request); err == nil {
		t.Fatal("invalid presentation bind unexpectedly succeeded")
	}
	if retired.ID != 1 {
		t.Fatalf("retired reservation = %+v, want acct-01", retired)
	}
	if _, err := s.m.Store.ActiveAccountMutation(1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unbound mutation survived exact retirement: %v", err)
	}
	indexes, err := s.m.Store.PendingAddIndexes()
	if err != nil || len(indexes) != 0 {
		t.Fatalf("pending reservation after exact retirement = %v err=%v", indexes, err)
	}
	configDir, err := pool.AccountConfigDir(retired.InstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(configDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stable execution link survived exact retirement: %v", err)
	}
	if info, err := os.Stat(publicPath); err != nil || !info.IsDir() {
		t.Fatalf("public presentation target after retirement = %v err=%v", info, err)
	}
}

func TestAccountMutationCancelUnknownFenceDoesNotReserveOrReadCredential(t *testing.T) {
	s, fake, _ := newAccountMutationTestServer(t, false)
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationCancel,
		Fence: AccountMutationFence{CanonicalOperationID: [32]byte{1}},
	}
	if _, err := runAccountMutationTest(t, s, request); err == nil {
		t.Fatal("cancel with an unknown fence succeeded")
	}
	if got := len(fake.TouchedServices()); got != 0 {
		t.Fatalf("unknown cancel performed credential I/O: %d touches", got)
	}
	reservation, err := s.m.ReserveAdd()
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ID != 1 {
		t.Fatalf("unknown cancel reserved account index: next=%d want=1", reservation.ID)
	}
}

func TestAccountMutationAttachRequiresExactFenceBeforeInput(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	var loginCalls atomic.Int64
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		context.Context, store.AccountMutation, accountterminal.TerminalInput, accountterminal.TerminalSize,
		func(context.Context, []byte) error,
	) error {
		loginCalls.Add(1)
		return nil
	})
	beginRequest := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, beginRequest)
	if err != nil {
		t.Fatal(err)
	}
	attach := beginRequest
	attach.Action = AccountMutationProvideInput
	attach.Fence = begin.Fence
	attach.Fence.RegistrySequence++
	attach.Input = accountMutationInputPayload(t, []byte("input that must remain unread"))
	if _, err := runAccountMutationTest(t, s, attach); err == nil {
		t.Fatal("attach accepted a mismatched fence")
	}
	if loginCalls.Load() != 0 {
		t.Fatalf("mismatched fence started %d terminal workers", loginCalls.Load())
	}
	if got := len(fake.TouchedServices()); got == 0 {
		t.Fatal("begin did not establish the credential baseline")
	}
}

func TestAccountMutationDisconnectBeforeInputLeavesAwaitingInput(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	var loginCalls atomic.Int64
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		context.Context, store.AccountMutation, accountterminal.TerminalInput, accountterminal.TerminalSize,
		func(context.Context, []byte) error,
	) error {
		loginCalls.Add(1)
		return nil
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	touched := len(fake.TouchedServices())
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationEOFPayload(t)
	result, err := runAccountMutationTest(t, s, request)
	if err != nil || result.State != AccountMutationAwaitingInput {
		t.Fatalf("pre-input disconnect = %+v err=%v", result, err)
	}
	if loginCalls.Load() != 0 {
		t.Fatalf("pre-input disconnect started %d terminal workers", loginCalls.Load())
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("pre-input disconnect performed credential I/O: touched %d -> %d", touched, got)
	}
}

func TestAccountMutationDuplicateTerminalAttachRunsOneSemanticOperation(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	started := make(chan struct{})
	release := make(chan struct{})
	var loginCalls atomic.Int64
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		context.Context, store.AccountMutation, accountterminal.TerminalInput, accountterminal.TerminalSize,
		func(context.Context, []byte) error,
	) error {
		if loginCalls.Add(1) == 1 {
			close(started)
		}
		<-release
		return errors.New("shared terminal ended")
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationInputPayload(t, []byte("\n"))
	firstDone := make(chan error, 1)
	go func() {
		_, err := runAccountMutationTest(t, s, request)
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first terminal did not start")
	}
	secondCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	second, secondErr := s.runAccountMutation(secondCtx, daemonkit.Session{}, request)
	if secondErr != nil || second.OperationID != request.Fence.CanonicalOperationID {
		close(release)
		t.Fatalf("duplicate provide-input = %+v err=%v, want the shared operation", second, secondErr)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("primary provide-input = %v, want the run started", err)
	}
	if _, err := driveSettledAccountMutation(t, s, request); err == nil {
		t.Fatal("primary unchanged terminal unexpectedly reported success")
	}
	if loginCalls.Load() != 1 {
		t.Fatalf("duplicate attach started %d terminal workers, want 1", loginCalls.Load())
	}
}

func TestAccountMutationGenerationCancellationCancelsReapsAndJoinsWatcher(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	lifetime, cancelLifetime := context.WithCancel(t.Context())
	s.accountMutationLifetime = lifetime
	terminal := newTestAccountMutationTerminal()
	started := make(chan struct{})
	s.accountMutationTerminal = accountMutationTerminalStarter{terminal: terminal, started: started}
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationInputPayload(t, []byte("\n"))
	done := make(chan error, 1)
	go func() {
		_, err := driveAccountMutationTest(t, s, request)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("terminal did not dispatch")
	}
	cancelLifetime()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("generation-canceled mutation = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("generation-canceled mutation did not settle")
	}
	waited := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("generation-canceled terminal watcher did not join")
	}
	rearmed, err := s.m.Store.AccountMutation(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	if rearmed.State != store.AccountMutationAwaitingInput {
		t.Fatalf("generation-canceled mutation state = %s, want awaiting input", rearmed.State)
	}
	s.accountMutationMu.Lock()
	running := s.accountMutationRuns[store.AccountMutationID(begin.OperationID)]
	s.accountMutationMu.Unlock()
	if running != nil {
		t.Fatal("generation-canceled mutation retained a terminal run")
	}
	outcome, err := terminal.Wait(t.Context())
	if err != nil || outcome.Kind != accountterminal.TerminalCanceled {
		t.Fatalf("generation-canceled terminal outcome = %+v err=%v", outcome, err)
	}
}

func TestAccountMutationTerminalFailureRearmsUnchangedOperation(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	wantErr := errors.New("terminal exited")
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		context.Context, store.AccountMutation, accountterminal.TerminalInput, accountterminal.TerminalSize,
		func(context.Context, []byte) error,
	) error {
		return wantErr
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationInputPayload(t, []byte("\n"))
	if _, err := driveAccountMutationTest(t, s, request); !errors.Is(err, wantErr) {
		t.Fatalf("terminal failure = %v, want %v", err, wantErr)
	}
	active, err := s.m.Store.AccountMutation(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	if active.State != store.AccountMutationAwaitingInput || !active.HasInput {
		t.Fatalf("failed terminal was not rearmed exactly: %+v", active)
	}
}

func TestAccountMutationOldOwnerFenceCannotAdvanceWithoutRetirementReceipt(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.m.Store.AccountMutation(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	newOwner, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s.accountMutationOwner = func() (store.OwnerRecord, error) { return newOwner, nil }
	request.Action = AccountMutationCancel
	request.Fence = begin.Fence
	if _, err := runAccountMutationTest(t, s, request); !errors.Is(err, store.ErrAccountMutationRecoveryRequired) {
		t.Fatalf("old-owner cancel = %v, want recovery required", err)
	}
	request.Action = AccountMutationProvideInput
	request.Input = accountMutationInputPayload(t, []byte("must remain unread"))
	if _, err := runAccountMutationTest(t, s, request); !errors.Is(err, store.ErrAccountMutationRecoveryRequired) {
		t.Fatalf("old-owner attach = %v, want recovery required", err)
	}
	after, err := s.m.Store.AccountMutation(before.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after.Owner, before.Owner) || after.OwnerEpoch != before.OwnerEpoch || after.State != before.State {
		t.Fatalf("old-owner fence advanced mutation: before=%+v after=%+v", before, after)
	}
}

func TestAccountMutationInvalidPostBoundaryCredentialQuarantines(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		_ context.Context,
		mutation store.AccountMutation,
		_ accountterminal.TerminalInput,
		_ accountterminal.TerminalSize,
		_ func(context.Context, []byte) error,
	) error {
		fake.Put(mutation.KeychainService, mutation.KeychainAccount, &creds.Credential{})
		return errors.New("terminal failed after write")
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationInputPayload(t, []byte("\n"))
	result, err := driveAccountMutationTest(t, s, request)
	if err != nil || result.State != AccountMutationQuarantined {
		t.Fatalf("post-boundary invalid credential = %+v err=%v", result, err)
	}
	if _, err := s.m.Store.AccountMutation(store.AccountMutationID(begin.OperationID)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("quarantined mutation active lookup = %v, want no rows", err)
	}
	receipt, err := s.m.Store.AccountMutationReceipt(store.AccountMutationID(begin.OperationID))
	if err != nil || receipt.Terminal != store.AccountMutationQuarantined {
		t.Fatalf("quarantine receipt = %+v err=%v", receipt, err)
	}
	if response := s.handleAccountMutationAck(t.Context(), Request{MutationReceipt: &begin.OperationID}); !response.OK {
		t.Fatalf("ack quarantine receipt = %+v", response)
	}
	quarantine, err := s.m.Store.CredentialQuarantine(account.ID)
	if err != nil || quarantine.AccountInstanceID != account.InstanceID ||
		quarantine.AccountGeneration != account.Generation {
		t.Fatalf("durable quarantine after ACK = %+v err=%v", quarantine, err)
	}
}

func TestAccountMutationReceiptIntentSurvivesDerivedLabelPublication(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		ctx context.Context,
		mutation store.AccountMutation,
		_ accountterminal.TerminalInput,
		_ accountterminal.TerminalSize,
		_ func(context.Context, []byte) error,
	) error {
		credential := &creds.Credential{}
		credential.ClaudeAiOauth.AccessToken = "new-access"
		credential.ClaudeAiOauth.RefreshToken = "new-refresh"
		credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
		fake.Put(mutation.KeychainService, mutation.KeychainAccount, credential)
		return s.m.WriteIdentity(
			ctx, mutation.AccountID, mutation.ConfigDir,
			[]byte(`{"accountUuid":"new-uuid","emailAddress":"derived@example.com"}`),
		)
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationInputPayload(t, []byte("\n"))
	completed, err := driveAccountMutationTest(t, s, request)
	if err != nil || completed.State != AccountMutationCompleted || completed.Label == "" {
		t.Fatalf("completed relogin = %+v err=%v", completed, err)
	}
	request.Action = AccountMutationStartOrAttach
	request.Fence = AccountMutationFence{}
	replayed, err := runAccountMutationTest(t, s, request)
	if err != nil || replayed.OperationID != begin.OperationID || replayed.Label != completed.Label {
		t.Fatalf("derived-label receipt replay = %+v err=%v", replayed, err)
	}
}

func TestPresentationQuarantinedReloginRepairsPathBeforeOrdinaryLogin(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	s.sessionLeases = &testSessionLeaseManager{}
	bound, err := s.m.Store.AccountPresentation(account.ID)
	if err != nil {
		t.Fatal(err)
	}
	home, err := pool.Home()
	if err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(home, "Library", "CloudStorage", "CCPoolStatus-moved-acct-01")
	if err := os.MkdirAll(targetPath, 0o700); err != nil {
		t.Fatal(err)
	}
	observed := bound.Identity
	observed.PublicPath = targetPath
	if err := s.m.Store.ObserveAccountPresentation(account, observed); !errors.Is(err, store.ErrAccountPresentationQuarantined) {
		t.Fatalf("seed presentation quarantine: %v", err)
	}
	var preparations, terminalCalls atomic.Int64
	s.prepareAccount = func(
		_ context.Context,
		got store.Account,
		_ tenantfs.PreparationLease,
	) (catalogproto.TenantPreparationProof, error) {
		preparations.Add(1)
		return daemonTestPreparationProof(got, targetPath), nil
	}
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		ctx context.Context,
		mutation store.AccountMutation,
		_ accountterminal.TerminalInput,
		_ accountterminal.TerminalSize,
		_ func(context.Context, []byte) error,
	) error {
		terminalCalls.Add(1)
		if mutation.Kind != store.AccountMutationRelogin ||
			mutation.AccountGeneration != account.Generation ||
			mutation.ConfigDir != account.ConfigDir ||
			mutation.KeychainService != account.KeychainService {
			t.Fatalf("ordinary relogin mutation = %+v", mutation)
		}
		if current, ok := fake.Get(account.KeychainService, account.KeychainAccount); !ok ||
			current.ClaudeAiOauth.AccessToken != "old-access" {
			t.Fatalf("credential changed before ordinary login: %+v", current)
		}
		credential := &creds.Credential{}
		credential.ClaudeAiOauth.AccessToken = "relogin-access"
		credential.ClaudeAiOauth.RefreshToken = "relogin-refresh"
		credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(2 * time.Hour).UnixMilli()
		fake.Put(mutation.KeychainService, mutation.KeychainAccount, credential)
		return s.m.WriteIdentity(
			ctx, mutation.AccountID, mutation.ConfigDir,
			[]byte(`{"accountUuid":"relogin-uuid","emailAddress":"relogin@example.com"}`),
		)
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil || begin.Kind != AccountMutationRelogin || begin.State != AccountMutationAwaitingInput ||
		begin.ConfigDir != account.ConfigDir || begin.Fence.AccountGeneration != account.Generation {
		t.Fatalf("ordinary relogin begin = %+v err=%v", begin, err)
	}
	if preparations.Load() != 1 || terminalCalls.Load() != 0 {
		t.Fatalf("repair side effects: preparations=%d terminals=%d", preparations.Load(), terminalCalls.Load())
	}
	repaired, err := s.m.Store.GetAccount(account.ID)
	if err != nil || repaired.InstanceID != account.InstanceID || repaired.Generation != account.Generation ||
		repaired.ConfigDir != account.ConfigDir || repaired.KeychainService != account.KeychainService ||
		repaired.AccountUUID != account.AccountUUID {
		t.Fatalf("path repair changed account identity: %+v err=%v", repaired, err)
	}
	presentation, err := s.m.Store.AccountPresentation(account.ID)
	if err != nil || presentation.Identity.PublicPath != targetPath {
		t.Fatalf("repaired presentation = %+v err=%v", presentation, err)
	}
	if _, err := s.m.Store.AccountPresentationQuarantine(account.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("presentation quarantine survived repair: %v", err)
	}
	if target, err := os.Readlink(account.ConfigDir); err != nil || target != targetPath {
		t.Fatalf("stable config target = %q, %v; want %q", target, err, targetPath)
	}
	if current, ok := fake.Get(account.KeychainService, account.KeychainAccount); !ok ||
		current.ClaudeAiOauth.AccessToken != "old-access" {
		t.Fatalf("repair moved credential: %+v", current)
	}
	if _, ok := fake.Get(creds.ServiceName(targetPath), account.KeychainAccount); ok {
		t.Fatal("repair created a presentation-derived credential slot")
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	request.Input = accountMutationInputPayload(t, []byte("\n"))
	completed, err := driveAccountMutationTest(t, s, request)
	if err != nil || !completed.Completed || completed.Kind != AccountMutationRelogin ||
		completed.State != AccountMutationCompleted || completed.ConfigDir != account.ConfigDir {
		t.Fatalf("completed relogin = %+v err=%v", completed, err)
	}
	if terminalCalls.Load() != 1 {
		t.Fatalf("ordinary relogin terminals = %d, want one", terminalCalls.Load())
	}
	updated, err := s.m.Store.GetAccount(account.ID)
	if err != nil || updated.Generation != account.Generation || updated.ConfigDir != account.ConfigDir ||
		updated.KeychainService != account.KeychainService || updated.AccountUUID != "relogin-uuid" {
		t.Fatalf("ordinary relogin result = %+v err=%v", updated, err)
	}
}

func newAccountMutationTestServer(
	t *testing.T,
	withAccount bool,
) (*Server, *credstest.Fake, store.Account) {
	t.Helper()
	home := t.TempDir()
	testhome.Sandbox(t, home)
	st, err := store.Open(filepath.Join(t.TempDir(), "account-mutation.db"))
	if err != nil {
		t.Fatal(err)
	}
	return newAccountMutationTestServerWithStore(t, st, withAccount)
}

func newAccountMutationTestServerWithStore(
	t *testing.T,
	st *store.Store,
	withAccount bool,
) (*Server, *credstest.Fake, store.Account) {
	t.Helper()
	fake := credstest.NewFake()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	authority, err := pool.NewWorkerAuthority(
		accountMutationTestTaskRunner{
			credentials: fake, refresher: accountMutationTestRefresher{},
		},
		executable, owner,
	)
	if err != nil {
		t.Fatal(err)
	}
	m, err := pool.NewManager(
		st, accountMutationTestRefresher{},
		func(context.Context) ([]procscan.Session, error) { return nil, nil },
		authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	m.Creds = fake
	m.BuildCredentialWritePublication = credentialWritePublicationBuilder("account-mutation-test")
	m.SettleCredentialWrite = func(context.Context, pool.CredentialWriteSettlement) error {
		return nil
	}
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Init(); err != nil {
		t.Fatal(err)
	}
	s := &Server{
		m: m, cl: newClaims(), log: log.New(io.Discard, "", 0), accountMutationLifetime: t.Context(),
		accountMutationOwner: func() (store.OwnerRecord, error) { return owner, nil },
		provisionPresentationIdentity: func(_ context.Context, account store.Account) (store.FileProviderPresentationIdentity, error) {
			return accountMutationTestPresentation(t, account)
		},
		prepareAccount: func(_ context.Context, account store.Account, _ tenantfs.PreparationLease) (catalogproto.TenantPreparationProof, error) {
			return daemonTestPreparationProof(
				account, testFileProviderPublicPath(account.ID),
			), nil
		},
		activatePrepared: func(_ context.Context, _ store.Account, _ tenantfs.PreparationLease, _ catalogproto.TenantPreparationProof, activate func() error) error {
			return activate()
		},
	}
	m.ClaimCredentialMutation = func(accountID int) (func(), error) {
		if !s.cl.ownExclusive(accountID) {
			return nil, errAccountExclusive
		}
		return func() { s.cl.releaseExclusive(accountID) }, nil
	}
	if !withAccount {
		return s, fake, store.Account{}
	}
	dir := testAccountConfigDir(1)
	account := store.Account{
		ID: 1, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 1,
		ConfigDir: dir, KeychainService: "cc-pool-test-account-1",
		KeychainAccount: "claude", Label: "existing",
	}
	account = admitDaemonTestAccount(t, st, account)
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "old-access"
	credential.ClaudeAiOauth.RefreshToken = "old-refresh"
	credential.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
	fake.Put(account.KeychainService, account.KeychainAccount, credential)
	if err := os.MkdirAll(pool.AccountBackingDir(account.ID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(pool.AccountBackingDir(account.ID), ".claude.json"),
		[]byte(`{"oauthAccount":{"accountUuid":"old-uuid","emailAddress":"old@example.com"}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	return s, fake, account
}

func accountMutationTestPresentation(
	t *testing.T,
	account store.Account,
) (store.FileProviderPresentationIdentity, error) {
	t.Helper()
	identity, err := expectedPresentationIdentity(account)
	if err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	if err := os.MkdirAll(identity.PublicPath, 0o700); err != nil {
		return store.FileProviderPresentationIdentity{}, err
	}
	return identity, nil
}

func runAccountMutationTest(
	t *testing.T,
	s *Server,
	request AccountMutationRequest,
) (AccountMutationResult, error) {
	t.Helper()
	return s.runAccountMutation(t.Context(), daemonkit.Session{}, request)
}

func accountMutationResizePayload(t *testing.T) []byte {
	t.Helper()
	encoded, err := encodeAccountTerminalInput(accountterminal.TerminalInput{
		Kind: accountterminal.TerminalInputResize,
		Size: accountterminal.TerminalSize{Rows: 24, Cols: 80},
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func accountMutationEOFPayload(t *testing.T) []byte {
	t.Helper()
	encoded, err := encodeAccountTerminalInput(accountterminal.TerminalInput{
		Kind: accountterminal.TerminalInputEOF,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// driveAccountMutationTest sends one ProvideInput and waits the run to its
// terminal result — the settlement the retired streaming call blocked for.
func driveAccountMutationTest(
	t *testing.T,
	s *Server,
	request AccountMutationRequest,
) (AccountMutationResult, error) {
	t.Helper()
	result, err := runAccountMutationTest(t, s, request)
	if err != nil || accountMutationTerminalState(result.State) {
		return result, err
	}
	return driveSettledAccountMutation(t, s, request)
}

func driveSettledAccountMutation(
	t *testing.T,
	s *Server,
	request AccountMutationRequest,
) (AccountMutationResult, error) {
	t.Helper()
	operationID := store.AccountMutationID(request.Fence.CanonicalOperationID)
	s.accountMutationMu.Lock()
	running := s.accountMutationRuns[operationID]
	s.accountMutationMu.Unlock()
	if running == nil {
		t.Fatal("account mutation run settled before it could be observed")
	}
	select {
	case <-running.done:
		return running.result, running.err
	case <-time.After(5 * time.Second):
		t.Fatal("account mutation run did not settle")
		return AccountMutationResult{}, nil
	}
}

func accountMutationInputPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	encoded, err := encodeAccountTerminalInput(accountterminal.TerminalInput{
		Kind: accountterminal.TerminalInputBytes, Data: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
