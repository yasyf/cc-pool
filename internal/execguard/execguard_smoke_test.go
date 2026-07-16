//go:build darwin

package execguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSetiopolicySmoke proves the raw iopolicysys syscall (322) is reachable and
// accepts the process/thread scope sets on this host — a fast fail if the syscall
// number or struct layout is wrong.
func TestSetiopolicySmoke(t *testing.T) {
	restore, err := enable()
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

// TestPrimeForExecReadsFile proves the bounded materialize read completes on a real
// present file.
func TestPrimeForExecReadsFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(p, []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PrimeForExec(p); err != nil {
		t.Fatalf("PrimeForExec on a present file: %v", err)
	}
}

// TestPrimeForExecMissingFileAborts proves an unreadable settings.json aborts the
// launch (no silent continue).
func TestPrimeForExecMissingFileAborts(t *testing.T) {
	if err := PrimeForExec(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("PrimeForExec on a missing file must return an error")
	}
}

func TestPrimeForExecContextRequiresRemainingBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := PrimeForExecContext(ctx, filepath.Join(t.TempDir(), "absent.json"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PrimeForExecContext err = %v, want deadline before materialization", err)
	}
}
