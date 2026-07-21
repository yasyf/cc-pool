package daemon

import (
	"errors"
	"testing"
	"time"
)

func TestLedgerAuthDebounceAndClear(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	ledgers := newLedgers()
	for attempt := 1; attempt <= needsLoginAfter; attempt++ {
		ledgers.strike(authStreakPolicy, "acct-18", now, errors.New("invalid_grant"))
		faulted := ledgers.peek(authStreakPolicy, "acct-18").faulted
		if faulted != (attempt == needsLoginAfter) {
			t.Fatalf("strike %d faulted = %v", attempt, faulted)
		}
	}
	row := ledgers.peek(authStreakPolicy, "acct-18")
	if row == nil || !row.faulted || row.lastErr == nil {
		t.Fatalf("faulted auth row = %+v", row)
	}
	ledgers.clear(authStreakPolicy, "acct-18")
	if row := ledgers.peek(authStreakPolicy, "acct-18"); row != nil {
		t.Fatalf("cleared auth row = %+v", row)
	}
}

func TestLedgerRateLimitBackoff(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	ledgers := newLedgers()
	ledgers.attempt(acctRateLimitPolicy, "acct-18", now)
	if ledgers.due(acctRateLimitPolicy, "acct-18", now) {
		t.Fatal("rate-limit row was due immediately")
	}
	want := now.Add(rateLimitBackoffBase)
	if !ledgers.due(acctRateLimitPolicy, "acct-18", want) {
		t.Fatalf("rate-limit row was not due at %v", want)
	}
	row := ledgers.peek(acctRateLimitPolicy, "acct-18")
	if row == nil || row.attempts != 1 || !row.nextDue.Equal(want) {
		t.Fatalf("rate-limit row = %+v", row)
	}
}
