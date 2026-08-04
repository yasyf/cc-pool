package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit"
)

type fakePollSession struct {
	id           uint64
	disconnected chan struct{}
	done         chan struct{}
}

func newFakePollSession(id uint64) fakePollSession {
	return fakePollSession{id: id, disconnected: make(chan struct{}), done: make(chan struct{})}
}

func (s fakePollSession) ID() uint64                    { return s.id }
func (s fakePollSession) Disconnected() <-chan struct{} { return s.disconnected }
func (s fakePollSession) Done() <-chan struct{}         { return s.done }

func newPollTestRun(t *testing.T) (*Server, *testAccountMutationTerminal, *accountMutationRun) {
	t.Helper()
	s := &Server{accountMutationLifetime: t.Context()}
	terminal := newTestAccountMutationTerminal()
	running := &accountMutationRun{
		ready: make(chan struct{}), done: make(chan struct{}), terminal: terminal,
	}
	close(running.ready)
	return s, terminal, running
}

// TestPollSupersessionSerializesPagesAndRepositionsTheCursor is the test F3
// lacked: a poll admitted while its predecessor still pages must wait out the
// in-flight page and then answer its own request cursor — never consume the
// next sequence and return it under the requested one.
func TestPollSupersessionSerializesPagesAndRepositionsTheCursor(t *testing.T) {
	s, terminal, running := newPollTestRun(t)
	operationID := store.AccountMutationID{1}
	terminal.publish([]byte("c0"))

	cursor := uint64(0)
	pa, err := s.accountMutationAttachment(newFakePollSession(1), operationID, running, &cursor)
	if err != nil {
		t.Fatal(err)
	}

	pa.pageMu.Lock()
	chunksA, settled, err := pa.page(nil)
	if err != nil || settled || len(chunksA) != 1 || string(chunksA[0]) != "c0" {
		t.Fatalf("predecessor page = %q settled=%t err=%v", chunksA, settled, err)
	}

	type pollResult struct {
		chunks [][]byte
		err    error
	}
	second := make(chan pollResult, 1)
	go func() {
		pa.pageMu.Lock()
		defer pa.pageMu.Unlock()
		if err := pa.reposition(t.Context(), running, 0); err != nil {
			second <- pollResult{err: err}
			return
		}
		chunks, _, err := pa.page(nil)
		second <- pollResult{chunks: chunks, err: err}
	}()

	select {
	case result := <-second:
		pa.pageMu.Unlock()
		t.Fatalf("superseding poll paged concurrently with its predecessor: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}

	terminal.publish([]byte("c1"))
	pa.pageMu.Unlock()

	select {
	case result := <-second:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.chunks) != 2 || string(result.chunks[0]) != "c0" || string(result.chunks[1]) != "c1" {
			t.Fatalf(
				"cursor-0 page = %q, want the replay to begin at sequence 0, never its neighbour's chunk",
				result.chunks,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("superseding poll never completed")
	}
	if !pa.cursorMatches(2) {
		t.Fatal("attachment cursor did not advance to 2 after the replayed page")
	}
}

// TestSessionTeardownClosesLateAttachmentsAndRejectsDisconnected is the test
// F4 lacked: Disconnected precedes in-flight handler completion, so an
// attachment registered after the first sweep must still be released — at
// Done — and a handler on an already-disconnected session must be refused.
func TestSessionTeardownClosesLateAttachmentsAndRejectsDisconnected(t *testing.T) {
	s, _, running := newPollTestRun(t)
	operationID := store.AccountMutationID{2}
	session := newFakePollSession(7)

	cursor := uint64(0)
	pa, err := s.accountMutationAttachment(session, operationID, running, &cursor)
	if err != nil {
		t.Fatal(err)
	}

	close(session.disconnected)
	select {
	case <-pa.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("disconnect sweep did not close the session's attachment")
	}

	if _, err := s.accountMutationAttachment(session, operationID, running, &cursor); err == nil ||
		!strings.Contains(err.Error(), "disconnected") {
		t.Fatalf("post-disconnect registration = %v, want a disconnected refusal", err)
	}

	// The in-flight race the rejection cannot see: a handler admitted before
	// the transport died registers between the Disconnected sweep and Done.
	late := &pollAttachment{
		server: s, key: pollKey{session: session.id, operation: operationID},
		attachment: newTestAccountMutationTerminal().newAttachmentForTest(), closed: make(chan struct{}),
	}
	s.registerPollAttachment(late.key, late)

	close(session.done)
	select {
	case <-late.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("late attachment survived Session.Done")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.pollMu.Lock()
		_, marked := s.pollSessions[session.id]
		s.pollMu.Unlock()
		if !marked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session marker survived Session.Done")
		}
		time.Sleep(time.Millisecond)
	}
}

func (t *testAccountMutationTerminal) newAttachmentForTest() accountMutationTerminalAttachment {
	t.mu.Lock()
	defer t.mu.Unlock()
	attachment := &testAccountMutationAttachment{terminal: t}
	t.attachments[attachment] = struct{}{}
	return attachment
}

// TestPreStartPollParksAndEOFResolvesTerminal is the test F6 lacked, both
// halves: a poll before any run exists must park rather than answer
// immediately (an immediate empty answer is a busy loop), and a pre-start EOF
// must resolve the mutation as Aborted so the released poll turns terminal.
func TestPreStartPollParksAndEOFResolvesTerminal(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	request := AccountMutationRequest{
		Kind: AccountMutationRelogin, Action: AccountMutationStartOrAttach, AccountID: account.ID,
	}
	begin, err := runAccountMutationTest(t, s, request)
	if err != nil || begin.State != AccountMutationAwaitingInput {
		t.Fatalf("begin = %+v err=%v", begin, err)
	}

	resize := request
	resize.Action = AccountMutationProvideInput
	resize.Fence = begin.Fence
	resize.Input = accountMutationResizePayload(t)
	if _, err := runAccountMutationTest(t, s, resize); err != nil {
		t.Fatal(err)
	}

	type pollOutcome struct {
		page AccountMutationPollResponse
		err  error
	}
	polled := make(chan pollOutcome, 1)
	go func() {
		reply, err := s.handleAccountMutationPoll(t.Context(), daemonkit.Request{
			Op: string(OpAccountMutationPoll),
		}, Request{MutationPoll: &AccountMutationPollRequest{
			Fence: begin.Fence, WaitMillis: 10_000,
		}})
		if err != nil {
			polled <- pollOutcome{err: err}
			return
		}
		var page AccountMutationPollResponse
		if err := json.Unmarshal(reply.Body, &page); err != nil {
			polled <- pollOutcome{err: err}
			return
		}
		polled <- pollOutcome{page: page}
	}()

	select {
	case outcome := <-polled:
		t.Fatalf("pre-start poll answered immediately instead of parking: %+v", outcome)
	case <-time.After(150 * time.Millisecond):
	}

	eof := request
	eof.Action = AccountMutationProvideInput
	eof.Fence = begin.Fence
	eof.Input = accountMutationEOFPayload(t)
	resolved, err := runAccountMutationTest(t, s, eof)
	if err != nil || resolved.State != AccountMutationCancelled {
		t.Fatalf("pre-start EOF = %+v err=%v, want an Aborted receipt", resolved, err)
	}
	if _, err := s.m.Store.AccountMutationReceipt(store.AccountMutationID(begin.OperationID)); err != nil {
		t.Fatalf("pre-start EOF left no receipt: %v", err)
	}
	s.accountMutationMu.Lock()
	stashed := len(s.accountMutationSizes)
	s.accountMutationMu.Unlock()
	if stashed != 0 {
		t.Fatalf("resize-then-EOF leaked %d stashed sizes", stashed)
	}

	select {
	case outcome := <-polled:
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		if !outcome.page.Done || outcome.page.State != AccountMutationCancelled || outcome.page.NextCursor != 0 {
			t.Fatalf("released poll = %+v, want Done at the Cancelled receipt", outcome.page)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pre-start poll was not released by the resolution")
	}
}

// TestPollDeadlineBoundsTheWholePollOnce covers the doubled-park defect: a
// poll may park before a run exists and again while paging, and both must
// derive from one instant, so a 25s poll answers in 25s rather than 50.
func TestPollDeadlineBoundsTheWholePollOnce(t *testing.T) {
	t.Run("requested wait, less the reply margin", func(t *testing.T) {
		got := time.Until(pollDeadline(context.Background(), 25_000))
		if want := 25*time.Second - pollReplyMargin; got > want || got < want-time.Second {
			t.Fatalf("deadline in %s, want ~%s", got, want)
		}
	})
	t.Run("zero wait takes the protocol ceiling", func(t *testing.T) {
		got := time.Until(pollDeadline(context.Background(), 0))
		if want := MaxPollWaitMillis*time.Millisecond - pollReplyMargin; got > want || got < want-time.Second {
			t.Fatalf("deadline in %s, want ~%s", got, want)
		}
	})
	t.Run("a sooner caller deadline wins", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		got := time.Until(pollDeadline(ctx, 25_000))
		if want := 2*time.Second - pollReplyMargin; got > want || got < want-time.Second {
			t.Fatalf("deadline in %s, want the caller's ~%s", got, want)
		}
	})
	t.Run("an already-spent budget parks not at all", func(t *testing.T) {
		s := &Server{accountMutationLifetime: t.Context()}
		if parkCtx := s.pollParkContext(t.Context(), time.Now().Add(-time.Second), nil); parkCtx != nil {
			t.Fatal("a spent deadline still produced a park context")
		}
	})
}

// TestRepositionAfterTeardownClosesTheReplacement covers the leak where
// teardown released the wrapper while a replacement attachment was being
// created: the fresh attachment belongs to no sweep, so reposition must close
// it itself rather than leave its lease held.
func TestRepositionAfterTeardownClosesTheReplacement(t *testing.T) {
	s, terminal, running := newPollTestRun(t)
	operationID := store.AccountMutationID{3}
	terminal.publish([]byte("c0"))
	session := newFakePollSession(9)

	cursor := uint64(0)
	pa, err := s.accountMutationAttachment(session, operationID, running, &cursor)
	if err != nil {
		t.Fatal(err)
	}
	pa.close()

	before := terminal.openAttachmentCount()
	if err := pa.reposition(t.Context(), running, 0); err == nil ||
		!strings.Contains(err.Error(), "closed") {
		t.Fatalf("reposition on a closed wrapper = %v, want a closed refusal", err)
	}
	if got := terminal.openAttachmentCount(); got != before {
		t.Fatalf("reposition leaked an attachment: open %d -> %d", before, got)
	}
}

func (t *testAccountMutationTerminal) openAttachmentCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.attachments)
}
