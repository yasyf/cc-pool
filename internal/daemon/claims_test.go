package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
)

func TestSelectionReservationExpiresAndRestartDropsIt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c := newClaims()
	c.now = func() time.Time { return now }
	a := store.Account{ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 4}
	token, err := c.beginSelection(a, selectionLaunch{cwd: "/project"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if c.reservedCount(a.ID) != 1 {
		t.Fatal("reservation was not live")
	}
	now = now.Add(time.Minute + time.Nanosecond)
	if c.reservedCount(a.ID) != 0 {
		t.Fatal("expired reservation remained live")
	}
	if resp := c.commitSelection(t.Context(), token, func(context.Context, string, reservation, selectionLaunch) Response {
		t.Fatal("expired reservation reached activation")
		return Response{}
	}); resp.OK {
		t.Fatalf("expired commit = %+v", resp)
	}
	if restarted := newClaims(); restarted.reservedCount(a.ID) != 0 {
		t.Fatal("daemon restart retained an in-memory reservation")
	}
}

func TestSelectionReservationCancelIsTerminal(t *testing.T) {
	c := newClaims()
	a := store.Account{ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 4}
	token, err := c.beginSelection(a, selectionLaunch{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if resp := c.abortSelection(t.Context(), token); !resp.OK {
		t.Fatalf("abort = %+v", resp)
	}
	if resp := c.abortSelection(t.Context(), token); !resp.OK {
		t.Fatalf("replayed abort = %+v", resp)
	}
	if c.reservedCount(a.ID) != 0 {
		t.Fatal("aborted reservation remained live")
	}
}

func TestSelectionReservationSerializesConcurrentCommit(t *testing.T) {
	c := newClaims()
	a := store.Account{ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 4}
	launch := selectionLaunch{pid: 4242, processStartedAt: time.Unix(1_700_000_000, 0), cwd: "/project", recordSticky: true}
	token, err := c.beginSelection(a, launch, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	activate := func(_ context.Context, gotToken string, got reservation, gotLaunch selectionLaunch) Response {
		mu.Lock()
		calls++
		mu.Unlock()
		if gotToken != token || got.accountInstanceID != a.InstanceID || got.accountGeneration != a.Generation {
			t.Errorf("reservation = %+v", got)
		}
		if gotLaunch != launch {
			t.Errorf("launch = %+v, want %+v", gotLaunch, launch)
		}
		close(started)
		<-release
		return Response{OK: true}
	}
	responses := make(chan Response, 2)
	go func() { responses <- c.commitSelection(context.Background(), token, activate) }()
	<-started
	go func() { responses <- c.commitSelection(context.Background(), token, activate) }()
	close(release)
	for range 2 {
		if resp := <-responses; !resp.OK {
			t.Fatalf("commit = %+v", resp)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("activation calls = %d, want 1", calls)
	}
	if c.reservedCount(a.ID) != 0 {
		t.Fatal("successful activation retained a reservation")
	}
}

func TestBeginSelectionPrunesTerminalHistory(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c := newClaims()
	c.now = func() time.Time { return now }
	a := store.Account{ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 4}
	for range 100 {
		token, err := c.beginSelection(a, selectionLaunch{}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if resp := c.commitSelection(t.Context(), token, func(context.Context, string, reservation, selectionLaunch) Response {
			return Response{OK: true}
		}); !resp.OK {
			t.Fatalf("commit = %+v", resp)
		}
		now = now.Add(provisionalSelectionTTL + time.Nanosecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if got := len(c.selections); got != 1 {
		t.Fatalf("terminal selection history = %d, want only current terminal", got)
	}
}
