package daemon

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
)

func TestAccountMutationStartOrAttachCoalescesExactIntentBeforeCredentialIO(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	first, err := runAccountMutationTest(t, s, request, nil)
	if err != nil || first.State != AccountMutationAwaitingInput {
		t.Fatalf("first begin = %+v err=%v", first, err)
	}
	touched := len(fake.TouchedServices())
	second, err := runAccountMutationTest(t, s, request, nil)
	if err != nil || second.OperationID != first.OperationID {
		t.Fatalf("second begin = %+v err=%v; want operation %x", second, err, first.OperationID)
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("coalesced begin performed credential I/O: touched %d -> %d", touched, got)
	}
	request.Label = "different-intent"
	if _, err := runAccountMutationTest(t, s, request, nil); err == nil {
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
	begin, err := runAccountMutationTest(t, s, beginRequest, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel := beginRequest
	cancel.Action = AccountMutationCancel
	cancel.Fence = begin.Fence
	terminal, err := runAccountMutationTest(t, s, cancel, nil)
	if err != nil || terminal.State != AccountMutationCancelled {
		t.Fatalf("cancel = %+v err=%v", terminal, err)
	}
	touched := len(fake.TouchedServices())
	replayed, err := runAccountMutationTest(t, s, beginRequest, nil)
	if err != nil || replayed.OperationID != begin.OperationID || replayed.State != AccountMutationCancelled {
		t.Fatalf("receipt replay = %+v err=%v", replayed, err)
	}
	if got := len(fake.TouchedServices()); got != touched {
		t.Fatalf("receipt replay performed credential I/O: touched %d -> %d", touched, got)
	}
	beginRequest.Label = "different-intent"
	if _, err := runAccountMutationTest(t, s, beginRequest, nil); err == nil {
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
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil || begin.AccountID <= 0 {
		t.Fatalf("add begin = %+v err=%v", begin, err)
	}
	cancel := request
	cancel.Action = AccountMutationCancel
	cancel.Fence = begin.Fence
	if _, err := runAccountMutationTest(t, s, cancel, nil); err != nil {
		t.Fatal(err)
	}
	touched := len(fake.TouchedServices())
	replayed, err := runAccountMutationTest(t, s, request, nil)
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

func TestAccountMutationCancelUnknownFenceDoesNotReserveOrReadCredential(t *testing.T) {
	s, fake, _ := newAccountMutationTestServer(t, false)
	request := AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationCancel,
		Fence: AccountMutationFence{CanonicalOperationID: [32]byte{1}},
	}
	if _, err := runAccountMutationTest(t, s, request, nil); err == nil {
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
		context.Context, store.AccountMutation, supervise.TerminalInput, supervise.TerminalSize,
		<-chan wire.Chunk, func(context.Context, []byte) error,
	) error {
		loginCalls.Add(1)
		return nil
	})
	beginRequest := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, beginRequest, nil)
	if err != nil {
		t.Fatal(err)
	}
	attach := beginRequest
	attach.Action = AccountMutationProvideInput
	attach.Fence = begin.Fence
	attach.Fence.RegistrySequence++
	input := make(chan wire.Chunk, 1)
	input <- accountMutationInputChunk(t, []byte("input that must remain unread"))
	if _, err := runAccountMutationTest(t, s, attach, input); err == nil {
		t.Fatal("attach accepted a mismatched fence")
	}
	if loginCalls.Load() != 0 {
		t.Fatalf("mismatched fence started %d terminal workers", loginCalls.Load())
	}
	if len(input) != 1 {
		t.Fatal("mismatched fence consumed terminal input")
	}
	if got := len(fake.TouchedServices()); got == 0 {
		t.Fatal("begin did not establish the credential baseline")
	}
}

func TestAccountMutationDisconnectBeforeInputLeavesAwaitingInput(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	var loginCalls atomic.Int64
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		context.Context, store.AccountMutation, supervise.TerminalInput, supervise.TerminalSize,
		<-chan wire.Chunk, func(context.Context, []byte) error,
	) error {
		loginCalls.Add(1)
		return nil
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	touched := len(fake.TouchedServices())
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	closed := make(chan wire.Chunk)
	close(closed)
	result, err := runAccountMutationTest(t, s, request, closed)
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
		context.Context, store.AccountMutation, supervise.TerminalInput, supervise.TerminalSize,
		<-chan wire.Chunk, func(context.Context, []byte) error,
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
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	input := func() <-chan wire.Chunk {
		chunks := make(chan wire.Chunk, 1)
		chunks <- accountMutationInputChunk(t, []byte("\n"))
		close(chunks)
		return chunks
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := runAccountMutationTest(t, s, request, input())
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first terminal did not start")
	}
	secondCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, secondErr := s.runAccountMutation(secondCtx, request, input(), make(chan []byte, 8))
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("duplicate attach = %v, want wait on shared operation until context deadline", secondErr)
	}
	close(release)
	if err := <-firstDone; err == nil {
		t.Fatal("primary unchanged terminal unexpectedly reported success")
	}
	if loginCalls.Load() != 1 {
		t.Fatalf("duplicate attach started %d terminal workers, want 1", loginCalls.Load())
	}
}

func TestAccountMutationObserverReceivesOutputAndTakesControlAfterDisconnect(t *testing.T) {
	terminal := newTestAccountMutationTerminal()
	started := make(chan struct{})
	terminal.start = func(supervise.TerminalInput) { close(started) }
	running := &accountMutationRun{
		ready: make(chan struct{}), done: make(chan struct{}), terminal: terminal,
	}
	close(running.ready)

	primary, controller, err := claimAccountMutationAttachment(t.Context(), running, nil)
	if err != nil || !controller {
		t.Fatalf("primary attachment = controller %t, err %v", controller, err)
	}
	primaryCtx, cancelPrimary := context.WithCancel(t.Context())
	primaryInput := make(chan wire.Chunk, 1)
	primaryInput <- accountMutationInputChunk(t, []byte("first-input"))
	primaryOutput := make(chan []byte, 1)
	primaryDone := make(chan error, 1)
	server := &Server{}
	go func() {
		_, err := server.relayAccountMutationRun(
			primaryCtx, running, primary, true, primaryInput, primaryOutput,
		)
		primaryDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("primary attachment did not start terminal input")
	}
	terminal.publish([]byte("shared-output"))
	select {
	case frame := <-primaryOutput:
		if string(frame[8:]) != "shared-output" {
			t.Fatalf("primary output = %q", frame[8:])
		}
	case <-time.After(time.Second):
		t.Fatal("primary attachment did not receive terminal output")
	}

	secondary, controller, err := claimAccountMutationAttachment(t.Context(), running, nil)
	if err != nil || controller {
		t.Fatalf("secondary attachment = controller %t, err %v", controller, err)
	}
	secondaryInput := make(chan wire.Chunk, 1)
	secondaryInput <- accountMutationInputChunk(t, []byte("second-input"))
	secondaryOutput := make(chan []byte, 1)
	secondaryDone := make(chan error, 1)
	go func() {
		_, err := server.relayAccountMutationRun(
			t.Context(), running, secondary, false, secondaryInput, secondaryOutput,
		)
		secondaryDone <- err
	}()
	select {
	case frame := <-secondaryOutput:
		if string(frame[8:]) != "shared-output" {
			t.Fatalf("secondary replay = %q", frame[8:])
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not receive retained terminal output")
	}

	cancelPrimary()
	if err := <-primaryDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("primary disconnect = %v, want context canceled", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		terminal.mu.Lock()
		inputs := append([]supervise.TerminalInput(nil), terminal.inputs...)
		terminal.mu.Unlock()
		if len(inputs) == 2 {
			if string(inputs[1].Data) != "second-input" {
				t.Fatalf("handoff input = %q", inputs[1].Data)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("secondary never took control; inputs = %#v", inputs)
		}
		time.Sleep(time.Millisecond)
	}

	terminal.settle(supervise.TerminalOutcome{
		Kind: supervise.TerminalExited, Digest: [32]byte{1},
	}, nil)
	running.result = AccountMutationResult{State: AccountMutationCompleted, Completed: true}
	close(running.done)
	if err := <-secondaryDone; err != nil {
		t.Fatalf("secondary settlement = %v", err)
	}
}

func TestAccountMutationTerminalDisconnectReplaysFromExactCursor(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	started := make(chan struct{})
	release := make(chan struct{})
	var loginCalls atomic.Int64
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		ctx context.Context,
		_ store.AccountMutation,
		_ supervise.TerminalInput,
		_ supervise.TerminalSize,
		_ <-chan wire.Chunk,
		emit func(context.Context, []byte) error,
	) error {
		loginCalls.Add(1)
		if err := emit(ctx, []byte("first")); err != nil {
			return err
		}
		close(started)
		<-release
		if err := emit(ctx, []byte("second")); err != nil {
			return err
		}
		return errors.New("terminal exited after replay test")
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstInput := make(chan wire.Chunk, 1)
	firstInput <- accountMutationInputChunk(t, []byte("\n"))
	close(firstInput)
	firstOutput := make(chan []byte, 2)
	firstDone := make(chan error, 1)
	go func() {
		_, err := s.runAccountMutation(firstCtx, request, firstInput, firstOutput)
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("terminal did not start")
	}
	var firstFrame []byte
	select {
	case firstFrame = <-firstOutput:
	case <-time.After(time.Second):
		t.Fatal("first terminal frame was not delivered")
	}
	if got := binary.BigEndian.Uint64(firstFrame[:8]); got != 0 || string(firstFrame[8:]) != "first" {
		t.Fatalf("first frame = sequence %d data %q", got, firstFrame[8:])
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("disconnected attach = %v, want context canceled", err)
	}

	cursor := uint64(1)
	request.TerminalCursor = &cursor
	secondInput := make(chan wire.Chunk)
	close(secondInput)
	secondOutput := make(chan []byte, 2)
	secondDone := make(chan error, 1)
	go func() {
		_, err := s.runAccountMutation(t.Context(), request, secondInput, secondOutput)
		secondDone <- err
	}()
	close(release)
	var secondFrame []byte
	select {
	case secondFrame = <-secondOutput:
	case <-time.After(time.Second):
		t.Fatal("reconnected terminal frame was not delivered")
	}
	if got := binary.BigEndian.Uint64(secondFrame[:8]); got != 1 || string(secondFrame[8:]) != "second" {
		t.Fatalf("second frame = sequence %d data %q", got, secondFrame[8:])
	}
	if err := <-secondDone; err == nil {
		t.Fatal("unchanged replay terminal unexpectedly succeeded")
	}
	if loginCalls.Load() != 1 {
		t.Fatalf("reconnect started %d terminal workers, want 1", loginCalls.Load())
	}
}

func TestAccountMutationTerminalReplaysSettledReceiptBeforeAcknowledgement(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	emitted := make(chan struct{})
	release := make(chan struct{})
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		ctx context.Context,
		mutation store.AccountMutation,
		_ supervise.TerminalInput,
		_ supervise.TerminalSize,
		_ <-chan wire.Chunk,
		emit func(context.Context, []byte) error,
	) error {
		if err := emit(ctx, []byte("before-drop")); err != nil {
			return err
		}
		close(emitted)
		<-release
		if err := emit(ctx, []byte("after-drop")); err != nil {
			return err
		}
		fake.Put(mutation.KeychainService, mutation.KeychainAccount, &creds.Credential{})
		return errors.New("terminal failed after retained output")
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstInput := make(chan wire.Chunk, 1)
	firstInput <- accountMutationInputChunk(t, []byte("\n"))
	close(firstInput)
	firstOutput := make(chan []byte, 2)
	firstDone := make(chan error, 1)
	go func() {
		_, err := s.runAccountMutation(firstCtx, request, firstInput, firstOutput)
		firstDone <- err
	}()
	select {
	case <-emitted:
	case <-time.After(time.Second):
		t.Fatal("terminal did not emit before disconnect")
	}
	select {
	case <-firstOutput:
	case <-time.After(time.Second):
		t.Fatal("pre-disconnect output was not delivered")
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("disconnected attach = %v, want context canceled", err)
	}
	close(release)
	operationID := store.AccountMutationID(begin.OperationID)
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := s.m.Store.AccountMutationReceipt(operationID); err == nil {
			break
		} else if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal receipt was not persisted before reconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}

	resume := request
	cursor := uint64(1)
	resume.TerminalCursor = &cursor
	resumeInput := make(chan wire.Chunk)
	close(resumeInput)
	resumeOutput := make(chan []byte, 2)
	result, err := s.runAccountMutation(t.Context(), resume, resumeInput, resumeOutput)
	if err != nil || result.State != AccountMutationQuarantined {
		t.Fatalf("settled reconnect = %+v err=%v", result, err)
	}
	frame := <-resumeOutput
	if got := binary.BigEndian.Uint64(frame[:8]); got != 1 || string(frame[8:]) != "after-drop" {
		t.Fatalf("retained frame = sequence %d data %q", got, frame[8:])
	}
	if response := s.handleAccountMutationAck(Request{MutationReceipt: &begin.OperationID}); !response.OK {
		t.Fatalf("ack retained receipt = %+v", response)
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
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	input := make(chan wire.Chunk, 1)
	input <- accountMutationInputChunk(t, []byte("\n"))
	close(input)
	done := make(chan error, 1)
	go func() {
		_, err := s.runAccountMutation(t.Context(), request, input, make(chan []byte, 2))
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
	if err != nil || outcome.Kind != supervise.TerminalCanceled {
		t.Fatalf("generation-canceled terminal outcome = %+v err=%v", outcome, err)
	}
}

func TestAccountMutationTerminalFailureRearmsUnchangedOperation(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	wantErr := errors.New("terminal exited")
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		context.Context, store.AccountMutation, supervise.TerminalInput, supervise.TerminalSize,
		<-chan wire.Chunk, func(context.Context, []byte) error,
	) error {
		return wantErr
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	input := make(chan wire.Chunk, 1)
	input <- accountMutationInputChunk(t, []byte("\n"))
	close(input)
	if _, err := runAccountMutationTest(t, s, request, input); !errors.Is(err, wantErr) {
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
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.m.Store.AccountMutation(store.AccountMutationID(begin.OperationID))
	if err != nil {
		t.Fatal(err)
	}
	newOwner := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           84, StartTime: "2.0", Boot: "test-boot", Comm: "cc-pool",
		Generation: "new-daemon-without-receipt",
	}
	s.accountMutationOwner = func() (proc.Record, error) { return newOwner, nil }
	request.Action = AccountMutationCancel
	request.Fence = begin.Fence
	if _, err := runAccountMutationTest(t, s, request, nil); !errors.Is(err, store.ErrAccountMutationRecoveryRequired) {
		t.Fatalf("old-owner cancel = %v, want recovery required", err)
	}
	request.Action = AccountMutationProvideInput
	input := make(chan wire.Chunk, 1)
	input <- accountMutationInputChunk(t, []byte("must remain unread"))
	if _, err := runAccountMutationTest(t, s, request, input); !errors.Is(err, store.ErrAccountMutationRecoveryRequired) {
		t.Fatalf("old-owner attach = %v, want recovery required", err)
	}
	if len(input) != 1 {
		t.Fatal("old-owner attach consumed input before retirement proof")
	}
	after, err := s.m.Store.AccountMutation(before.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Owner != before.Owner || after.OwnerEpoch != before.OwnerEpoch || after.State != before.State {
		t.Fatalf("old-owner fence advanced mutation: before=%+v after=%+v", before, after)
	}
}

func TestAccountMutationInvalidPostBoundaryCredentialQuarantines(t *testing.T) {
	s, fake, account := newAccountMutationTestServer(t, true)
	s.accountMutationTerminal = accountMutationTerminalRunnerFunc(func(
		ctx context.Context,
		mutation store.AccountMutation,
		_ supervise.TerminalInput,
		_ supervise.TerminalSize,
		_ <-chan wire.Chunk,
		_ func(context.Context, []byte) error,
	) error {
		fake.Put(mutation.KeychainService, mutation.KeychainAccount, &creds.Credential{})
		return errors.New("terminal failed after write")
	})
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	input := make(chan wire.Chunk, 1)
	input <- accountMutationInputChunk(t, []byte("\n"))
	close(input)
	result, err := runAccountMutationTest(t, s, request, input)
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
	if response := s.handleAccountMutationAck(Request{MutationReceipt: &begin.OperationID}); !response.OK {
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
		_ supervise.TerminalInput,
		_ supervise.TerminalSize,
		_ <-chan wire.Chunk,
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
	begin, err := runAccountMutationTest(t, s, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Action = AccountMutationProvideInput
	request.Fence = begin.Fence
	input := make(chan wire.Chunk, 1)
	input <- accountMutationInputChunk(t, []byte("\n"))
	close(input)
	completed, err := runAccountMutationTest(t, s, request, input)
	if err != nil || completed.State != AccountMutationCompleted || completed.Label == "" {
		t.Fatalf("completed relogin = %+v err=%v", completed, err)
	}
	request.Action = AccountMutationStartOrAttach
	request.Fence = AccountMutationFence{}
	replayed, err := runAccountMutationTest(t, s, request, nil)
	if err != nil || replayed.OperationID != begin.OperationID || replayed.Label != completed.Label {
		t.Fatalf("derived-label receipt replay = %+v err=%v", replayed, err)
	}
}

func newAccountMutationTestServer(
	t *testing.T,
	withAccount bool,
) (*Server, *credstest.Fake, store.Account) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	st, err := store.Open(filepath.Join(t.TempDir(), "account-mutation.db"))
	if err != nil {
		t.Fatal(err)
	}
	fake := credstest.NewFake()
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	owner := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable,
		AuditToken: identity.AuditToken, Generation: "account-mutation-test",
	}
	authority, err := pool.NewWorkerAuthority(
		accountMutationTestTaskRunner{credentials: fake}, identity.Executable, owner,
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
		m: m, log: log.New(io.Discard, "", 0), accountMutationLifetime: t.Context(),
		accountMutationOwner: func() (proc.Record, error) { return owner, nil },
	}
	if !withAccount {
		return s, fake, store.Account{}
	}
	dir := pool.AccountDir(1)
	account := store.Account{
		ID: 1, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 1,
		ConfigDir: dir, KeychainService: "cc-pool-test-account-1",
		KeychainAccount: "claude", Label: "existing",
	}
	if err := st.UpsertAccount(account); err != nil {
		t.Fatal(err)
	}
	account, err = st.GetAccount(account.ID)
	if err != nil {
		t.Fatal(err)
	}
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

func runAccountMutationTest(
	t *testing.T,
	s *Server,
	request AccountMutationRequest,
	input <-chan wire.Chunk,
) (AccountMutationResult, error) {
	t.Helper()
	if input == nil {
		closed := make(chan wire.Chunk)
		close(closed)
		input = closed
	}
	return s.runAccountMutation(t.Context(), request, input, make(chan []byte, 8))
}

func accountMutationInputChunk(t *testing.T, payload []byte) wire.Chunk {
	t.Helper()
	encoded, err := encodeAccountTerminalInput(supervise.TerminalInput{
		Kind: supervise.TerminalInputBytes, Data: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return wire.Chunk{Payload: encoded}
}
