package daemon

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
)

func TestAccountMutationTerminalEndpointAdvancesExactCursor(t *testing.T) {
	endpoint := &accountMutationTerminalEndpoint{}
	first, err := endpoint.decodeOutput(accountTerminalClientFrame(0, []byte("first")))
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 0 || string(first.Data) != "first" {
		t.Fatalf("first output = %#v", first)
	}
	second, err := endpoint.decodeOutput(accountTerminalClientFrame(1, []byte("second")))
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 1 || string(second.Data) != "second" {
		t.Fatalf("second output = %#v", second)
	}
	if !endpoint.haveCursor || endpoint.nextSequence != 2 {
		t.Fatalf("cursor = (%t, %d), want (true, 2)", endpoint.haveCursor, endpoint.nextSequence)
	}
}

func TestAccountMutationTerminalEndpointRejectsOutputGap(t *testing.T) {
	endpoint := &accountMutationTerminalEndpoint{}
	if _, err := endpoint.decodeOutput(accountTerminalClientFrame(7, []byte("retained beginning"))); err != nil {
		t.Fatal(err)
	}
	_, err := endpoint.decodeOutput(accountTerminalClientFrame(9, []byte("skipped")))
	if err == nil || !strings.Contains(err.Error(), "sequence 9, want 8") {
		t.Fatalf("decode error = %v", err)
	}
}

func TestAccountMutationTerminalEndpointRejectsMalformedOutput(t *testing.T) {
	endpoint := &accountMutationTerminalEndpoint{}
	for _, payload := range [][]byte{nil, make([]byte, 8)} {
		if _, err := endpoint.decodeOutput(payload); err == nil {
			t.Fatalf("decode accepted %d-byte frame", len(payload))
		}
	}
}

func TestAccountMutationTerminalEndpointWaitsForReconnectState(t *testing.T) {
	endpoint := &accountMutationTerminalEndpoint{stateChanged: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- endpoint.Send(t.Context(), supervise.TerminalInput{
			Kind: supervise.TerminalInputResize,
			Size: supervise.TerminalSize{Rows: 24, Cols: 80},
		})
	}()
	select {
	case err := <-done:
		t.Fatalf("Send returned before reconnect settled: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	endpoint.mu.Lock()
	endpoint.settled = true
	endpoint.signalStateLocked()
	endpoint.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("Send after terminal settlement = %v", err)
	}
}

func TestRetryableTerminalCallClassifiesAdmission(t *testing.T) {
	if !retryableTerminalCall(wire.Result{
		Outcome: wire.Rejected, Response: wire.Response{Reason: wire.ErrDraining.Error()},
	}, nil) {
		t.Fatal("draining rejection is not retryable")
	}
	if retryableTerminalCall(wire.Result{
		Outcome: wire.Rejected, Response: wire.Response{Reason: wire.ErrBuildMismatch.Error()},
	}, nil) {
		t.Fatal("build mismatch rejection is retryable")
	}
	if !retryableTerminalCall(wire.Result{Outcome: wire.DeliveryUnknown}, errors.New("lost response")) {
		t.Fatal("unknown delivery is not retryable for exact terminal attachment")
	}
}

func TestValidateAccountMutationTerminalResultRequiresExactFence(t *testing.T) {
	operationID := [32]byte{1}
	fence := AccountMutationFence{
		CanonicalOperationID: operationID, AccountInstanceID: "instance", AccountGeneration: 2,
	}
	result := AccountMutationResult{
		OperationID: operationID, Kind: AccountMutationRelogin,
		State: AccountMutationCompleted, AccountID: 7, Fence: fence, Completed: true,
	}
	if err := validateAccountMutationTerminalResult(
		result, AccountMutationRelogin, 7, &fence,
	); err != nil {
		t.Fatal(err)
	}
	wrong := fence
	wrong.AccountGeneration++
	if err := validateAccountMutationTerminalResult(
		result, AccountMutationRelogin, 7, &wrong,
	); err == nil || !strings.Contains(err.Error(), "different account mutation fence") {
		t.Fatalf("validation error = %v", err)
	}
}

func TestAccountMutationTerminalRejectsNonzeroFenceRemainder(t *testing.T) {
	client := &Client{}
	_, err := client.AccountMutationTerminal(context.Background(), AccountMutationRequest{
		Kind: AccountMutationAdd, Action: AccountMutationStartOrAttach,
		Fence: AccountMutationFence{AccountGeneration: 1},
	}, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "without a fence") {
		t.Fatalf("AccountMutationTerminal error = %v", err)
	}
}

func accountTerminalClientFrame(sequence uint64, data []byte) []byte {
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(payload[:8], sequence)
	copy(payload[8:], data)
	return payload
}
