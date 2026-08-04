package daemon

import (
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit"
)

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
	pa, err := s.accountMutationAttachment(daemonkit.Session{}, operationID, running, &cursor)
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
