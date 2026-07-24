package accountterminal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const terminalTestTimeout = 8 * time.Second

func newTerminalTestManager(t *testing.T) (*Manager, *proc.FileStore) {
	t.Helper()
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	reaper := &proc.Reaper{
		Store: store, Generation: generation,
		Grace: terminationGrace, Settlement: 2 * time.Second,
	}
	pool, err := NewManager(2, reaper)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
		defer cancel()
		if err := pool.Close(ctx); err != nil {
			t.Errorf("close terminal manager: %v", err)
		}
	})
	return pool, store
}

func startTerminalTest(t *testing.T, pool *Manager, script string, size TerminalSize) *Terminal {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	terminal, err := pool.Start(ctx, TerminalSpec{
		Path:          "/bin/sh",
		Args:          []string{"-c", script},
		Size:          size,
		AttachTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return terminal
}

func attachTerminalTest(t *testing.T, terminal *Terminal, policy TerminalDisconnectPolicy) *TerminalAttachment {
	t.Helper()
	attachment, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{
		Role: TerminalController, DisconnectPolicy: policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	return attachment
}

func observeTerminalTest(t *testing.T, terminal *Terminal) *TerminalAttachment {
	t.Helper()
	attachment, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{Role: TerminalObserver})
	if err != nil {
		t.Fatal(err)
	}
	return attachment
}

func receiveUntil(t *testing.T, attachment *TerminalAttachment, want string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	var output strings.Builder
	for !strings.Contains(output.String(), want) {
		chunk, err := attachment.Receive(ctx)
		if err != nil {
			t.Fatalf("receive %q after %q: %v", want, output.String(), err)
		}
		output.Write(chunk.Data)
	}
	return output.String()
}

func receiveAll(t *testing.T, attachment *TerminalAttachment) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	var output strings.Builder
	for {
		chunk, err := attachment.Receive(ctx)
		if errors.Is(err, io.EOF) {
			return output.String()
		}
		if err != nil {
			t.Fatalf("receive terminal output: %v", err)
		}
		output.Write(chunk.Data)
	}
}

func waitTerminalTest(t *testing.T, terminal *Terminal) TerminalOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	outcome, err := terminal.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Digest == ([32]byte{}) {
		t.Fatal("terminal outcome digest is empty")
	}
	return outcome
}

func assertTerminalStoreEmpty(t *testing.T, store *proc.FileStore) {
	t.Helper()
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("durable terminal records remain: %+v", records)
	}
}

func TestManagerRequiresRecoveryAndPermanentlyClosesAdmission(t *testing.T) {
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "processes.db")}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(1, &proc.Reaper{Store: store, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	spec := TerminalSpec{Path: "/usr/bin/true"}
	if _, err := manager.Start(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "recovery has not completed") {
		t.Fatalf("Start before recovery = %v", err)
	}
	if err := manager.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "manager is closed") {
		t.Fatalf("Start after Close = %v", err)
	}
}

func TestManagerCloseCancelsAndJoinsLiveTerminal(t *testing.T) {
	manager, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, manager, `trap '' HUP TERM; printf ready; while :; do sleep 1; done`, TerminalSize{})
	attachment := attachTerminalTest(t, terminal, DetachOnDisconnect)
	receiveUntil(t, attachment, "ready")
	if err := manager.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	outcome, err := terminal.Wait(t.Context())
	if err != nil || outcome.Kind != TerminalCanceled {
		t.Fatalf("Wait after manager close = %+v, %v", outcome, err)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalPTYResizeInputEOFAndTypedExit(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `
test -t 0 && test -t 1 && printf 'tty-ok\n'
stty size
IFS= read -r line
stty size
printf 'got:%s\n' "$line"
IFS= read -r rest || printf 'eof-ok\n'
exit 7
`, TerminalSize{Rows: 31, Cols: 91})
	attachment := attachTerminalTest(t, terminal, DetachOnDisconnect)
	output := receiveUntil(t, attachment, "31 91")

	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputResize, Size: TerminalSize{Rows: 40, Cols: 100}}); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: []byte("hello\n")}); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputEOF}); err != nil {
		t.Fatal(err)
	}
	if err := attachment.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: []byte("late")}); !errors.Is(err, ErrTerminalInputClosed) {
		t.Fatalf("send after EOF = %v", err)
	}
	output += receiveAll(t, attachment)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 7 {
		t.Fatalf("outcome = %+v", outcome)
	}
	for _, want := range []string{"tty-ok", "31 91", "40 100", "got:hello", "eof-ok"} {
		if !strings.Contains(output, want) {
			t.Errorf("output missing %q: %q", want, output)
		}
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalReportsSignalExactly(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `kill -TERM $$`, TerminalSize{})
	attachment := attachTerminalTest(t, terminal, DetachOnDisconnect)
	_ = receiveAll(t, attachment)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalSignaled || outcome.Signal != syscall.SIGTERM || outcome.ExitCode != -1 {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalDetachAndReattach(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `printf 'ready\n'; IFS= read -r line; printf 'done:%s\n' "$line"`, TerminalSize{})
	first := attachTerminalTest(t, terminal, DetachOnDisconnect)
	_ = receiveUntil(t, first, "ready")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("wrong\n")}); !errors.Is(err, ErrTerminalDetached) {
		t.Fatalf("detached send = %v", err)
	}
	second := attachTerminalTest(t, terminal, CancelOnDisconnect)
	if err := second.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("right\n")}); err != nil {
		t.Fatal(err)
	}
	output := receiveAll(t, second)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 0 || !strings.Contains(output, "done:right") {
		t.Fatalf("outcome=%+v output=%q", outcome, output)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalLostAttachNeverDispatches(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	marker := filepath.Join(t.TempDir(), "dispatched")
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	terminal, err := pool.Start(ctx, TerminalSpec{
		Path:          "/bin/sh",
		Args:          []string{"-c", "printf dispatched > " + marker},
		AttachTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("undispatched marker exists or stat failed unexpectedly: %v", err)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalSlowObserverDoesNotStallController(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `
printf 'ready-start\n'
IFS= read -r _
i=0
while [ "$i" -lt 40 ]; do printf '%032768d' 0; sleep 0.01; i=$((i+1)); done
printf '\nready-end\n'
IFS= read -r line
printf 'done:%s\n' "$line"
`, TerminalSize{})
	observer := observeTerminalTest(t, terminal)
	controller := attachTerminalTest(t, terminal, CancelOnDisconnect)
	_ = receiveUntil(t, observer, "ready-start")
	_ = receiveUntil(t, controller, "ready-start")
	controllerOutput := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
		defer cancel()
		var output strings.Builder
		for !strings.Contains(output.String(), "ready-end") {
			chunk, receiveErr := controller.Receive(ctx)
			if receiveErr != nil {
				controllerOutput <- receiveErr
				return
			}
			output.Write(chunk.Data)
		}
		controllerOutput <- nil
	}()
	if err := controller.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("go\n")}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-controllerOutput:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(terminalTestTimeout):
		t.Fatal("controller stalled behind slow observer")
	}
	if _, err := observer.Receive(context.Background()); !errors.Is(err, ErrTerminalOutputLagged) {
		t.Fatalf("slow observer receive = %v, want ErrTerminalOutputLagged", err)
	}
	owned, err := pool.reaper.Owns(terminal.Record())
	if err != nil || !owned {
		t.Fatalf("slow observer stopped terminal: owned=%v err=%v", owned, err)
	}
	if err := controller.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("finish\n")}); err != nil {
		t.Fatal(err)
	}
	_ = receiveAll(t, controller)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalObserversReplayAndControllerHandoff(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `printf 'ready\n'; IFS= read -r line; printf 'done:%s\n' "$line"`, TerminalSize{})
	firstObserver := observeTerminalTest(t, terminal)
	controller := attachTerminalTest(t, terminal, CancelOnDisconnect)
	_ = receiveUntil(t, controller, "ready")
	lateObserver := observeTerminalTest(t, terminal)
	if output := receiveUntil(t, lateObserver, "ready"); !strings.Contains(output, "ready") {
		t.Fatalf("late observer replay = %q", output)
	}
	_ = receiveUntil(t, firstObserver, "ready")
	if _, err := controller.HandoffControl(firstObserver, DetachOnDisconnect, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := controller.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("stale\n")}); !errors.Is(err, ErrTerminalNotController) {
		t.Fatalf("old controller send = %v, want ErrTerminalNotController", err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstObserver.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("fresh\n")}); err != nil {
		t.Fatal(err)
	}
	if output := receiveAll(t, firstObserver); !strings.Contains(output, "done:fresh") {
		t.Fatalf("handoff output = %q", output)
	}
	if output := receiveAll(t, lateObserver); !strings.Contains(output, "done:fresh") {
		t.Fatalf("observer output = %q", output)
	}
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalExactReplayCursorAndSettledAcknowledgement(t *testing.T) {
	terminal := &Terminal{
		attachments: make(map[uint64]*terminalAttachmentState),
		outputs:     [][]byte{[]byte("seven"), []byte("eight")},
		outputBase:  7,
		outputNext:  9,
		settled:     true,
		result:      TerminalOutcome{Digest: [32]byte{9}},
		retired:     make(chan struct{}),
	}
	if _, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{
		Role: TerminalController, DisconnectPolicy: DetachOnDisconnect,
	}); !errors.Is(err, ErrTerminalSettled) {
		t.Fatalf("controller attach after settlement = %v", err)
	}
	cursor := TerminalOutputCursor{NextSequence: 8}
	attachment, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{
		Role: TerminalObserver, Cursor: &cursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := attachment.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Sequence != 8 || string(output.Data) != "eight" || output.NextCursor().NextSequence != 9 {
		t.Fatalf("resumed output = %+v", output)
	}
	if _, err := attachment.Receive(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("settled replay terminal = %v, want EOF", err)
	}
	if err := attachment.Close(); err != nil {
		t.Fatal(err)
	}
	missing := TerminalOutputCursor{NextSequence: 6}
	if _, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{
		Role: TerminalObserver, Cursor: &missing,
	}); !errors.Is(err, ErrTerminalOutputCursor) {
		t.Fatalf("expired cursor attach = %v", err)
	}
	if err := terminal.Acknowledge(context.Background(), [32]byte{8}); !errors.Is(err, ErrTerminalOutcomeMismatch) {
		t.Fatalf("wrong outcome acknowledgement = %v", err)
	}
	if err := terminal.Acknowledge(context.Background(), [32]byte{9}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-terminal.Retired():
	default:
		t.Fatal("acknowledged terminal was not retired")
	}
	if _, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{Role: TerminalObserver}); !errors.Is(err, ErrTerminalRetentionExpired) {
		t.Fatalf("attach after acknowledgement = %v", err)
	}
}

func TestTerminalControllerLeaseRenewalAndExpiryFence(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `IFS= read -r line; printf 'done:%s\n' "$line"`, TerminalSize{})
	observer := observeTerminalTest(t, terminal)
	first, err := observer.ClaimControl(context.Background(), DetachOnDisconnect, 80*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	renewed, err := observer.RenewControl(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if renewed.Fence != first.Fence || !renewed.Expires.After(first.Expires) {
		t.Fatalf("renewed lease = %+v, first = %+v", renewed, first)
	}
	select {
	case <-observer.state.closed:
	case <-time.After(time.Second):
		t.Fatal("controller lease did not expire")
	}
	if err := observer.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("stale\n")}); !errors.Is(err, ErrTerminalControlExpired) {
		t.Fatalf("expired controller send = %v", err)
	}
	replacement := attachTerminalTest(t, terminal, CancelOnDisconnect)
	second, err := replacement.ControllerLease()
	if err != nil {
		t.Fatal(err)
	}
	if second.Fence == first.Fence {
		t.Fatalf("replacement reused fence %d", second.Fence)
	}
	if err := replacement.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("fresh\n")}); err != nil {
		t.Fatal(err)
	}
	_ = receiveAll(t, replacement)
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalDetachedSessionExpiresAndReaps(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), terminalTestTimeout)
	defer cancel()
	terminal, err := pool.Start(ctx, TerminalSpec{
		Path:          "/bin/sh",
		Args:          []string{"-c", "sleep 60"},
		AttachTimeout: time.Second,
		DetachTimeout: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	attachment := attachTerminalTest(t, terminal, DetachOnDisconnect)
	if err := attachment.Close(); err != nil {
		t.Fatal(err)
	}
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	if _, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{Role: TerminalObserver}); !errors.Is(err, ErrTerminalDetachExpired) {
		t.Fatalf("attach after detached deadline = %v", err)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalOutcomeDigestBindsCompleteRecord(t *testing.T) {
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	base := TerminalOutcome{
		Kind: TerminalExited, ExitCode: 0,
		Record: proc.Record{
			RecoveryID: RecoveryID, PID: 42, StartTime: "start", Boot: "boot-a",
			Comm: "task", Executable: "/bin/task", Generation: generation, ProcessGroup: true, SessionID: 42,
		},
	}
	changed := base
	changed.Record.Boot = "boot-b"
	if terminalOutcomeDigest(base) == terminalOutcomeDigest(changed) {
		t.Fatal("terminal digest ignored boot identity")
	}
}

func TestTerminalAttachmentLimitAndReleasedControllerClaim(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `IFS= read -r line; printf 'done:%s\n' "$line"`, TerminalSize{})
	attachments := make([]*TerminalAttachment, 0, TerminalAttachmentLimit)
	for range TerminalAttachmentLimit - 1 {
		attachments = append(attachments, observeTerminalTest(t, terminal))
	}
	controller := attachTerminalTest(t, terminal, DetachOnDisconnect)
	attachments = append(attachments, controller)
	if _, err := terminal.Attach(context.Background(), TerminalAttachmentSpec{Role: TerminalObserver}); !errors.Is(err, ErrTerminalAttachmentLimit) {
		t.Fatalf("attachment above limit = %v", err)
	}
	if err := controller.ReleaseControl(); err != nil {
		t.Fatal(err)
	}
	if _, err := attachments[0].ClaimControl(context.Background(), DetachOnDisconnect, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := controller.Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("stale\n")}); !errors.Is(err, ErrTerminalNotController) {
		t.Fatalf("released controller send = %v", err)
	}
	if err := attachments[0].Send(context.Background(), TerminalInput{Kind: TerminalInputBytes, Data: []byte("claimed\n")}); err != nil {
		t.Fatal(err)
	}
	_ = receiveAll(t, attachments[0])
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalExited || outcome.ExitCode != 0 {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalAcceptedInputSettlesAfterContextCancellation(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	terminal := startTerminalTest(t, pool, `sleep 60`, TerminalSize{})
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	terminal.write = func([]byte) error {
		select {
		case <-writeStarted:
		default:
			close(writeStarted)
		}
		<-releaseWrite
		return nil
	}
	attachment := attachTerminalTest(t, terminal, CancelOnDisconnect)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- attachment.Send(ctx, TerminalInput{Kind: TerminalInputBytes, Data: []byte("accepted")})
	}()
	select {
	case <-writeStarted:
	case <-time.After(terminalTestTimeout):
		t.Fatal("terminal write did not start")
	}
	<-ctx.Done()
	select {
	case err := <-sendDone:
		t.Fatalf("accepted send returned before settlement: %v", err)
	default:
	}
	close(releaseWrite)
	if err := <-sendDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("settled canceled send = %v, want context deadline", err)
	}
	_ = attachment.Close()
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	assertTerminalStoreEmpty(t, store)
}

func TestTerminalCancellationKillsAndReapsDescendant(t *testing.T) {
	pool, store := newTerminalTestManager(t)
	pidPath := filepath.Join(t.TempDir(), "descendant.pid")
	script := fmt.Sprintf(`(trap '' TERM; while :; do sleep 1; done) & child=$!; printf '%%s' "$child" > %q; wait`, pidPath)
	terminal := startTerminalTest(t, pool, script, TerminalSize{})
	attachment := attachTerminalTest(t, terminal, CancelOnDisconnect)

	deadline := time.Now().Add(terminalTestTimeout)
	var pid int
	for time.Now().Before(deadline) {
		// #nosec G304 -- pidPath is a test-owned file under t.TempDir.
		raw, err := os.ReadFile(pidPath)
		if err == nil {
			pid, err = strconv.Atoi(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("descendant pid was not written")
	}
	_ = attachment.Close()
	outcome := waitTerminalTest(t, terminal)
	if outcome.Kind != TerminalCanceled {
		t.Fatalf("outcome = %+v", outcome)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d remains: %v", pid, err)
	}
	assertTerminalStoreEmpty(t, store)
}
