package daemon

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func writeLegacyTerminalLedger(t *testing.T, records int) string {
	t.Helper()
	path := pool.AccountTerminalProcessStorePath()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("records"))
		if err != nil {
			return err
		}
		for index := range records {
			if err := bucket.Put(fmt.Appendf(nil, "record-%d", index), []byte("{}")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestSweepLegacyAccountTerminalsRefusesWhileRecordsRemain pins the whole
// design: any recorded login refuses the boot with the exact command that
// clears the ledger, touches no process, and archives nothing. The machine
// does not try to prove the login is gone — the human confirms it and clears
// the ledger, which is the only race-free version of this check.
func TestSweepLegacyAccountTerminalsRefusesWhileRecordsRemain(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	path := writeLegacyTerminalLedger(t, 2)

	err := sweepLegacyAccountTerminals()
	if err == nil {
		t.Fatal("sweep with recorded terminals did not refuse")
	}
	for _, want := range []string{
		"2 interactive login terminal(s)",
		"claude auth login",
		fmt.Sprintf("rm '%s'", path),
		"retries automatically",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not carry %q", err, want)
		}
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("refusal disturbed the ledger: %v", err)
	}
	if _, err := os.Lstat(path + ".archived"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refusal created an archive: %v", err)
	}

	// The user's advertised command is the recovery: after it, the boot
	// proceeds and nothing is left to archive.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := sweepLegacyAccountTerminals(); err != nil {
		t.Fatalf("sweep after the advertised command = %v, want a clean boot", err)
	}
}

func TestSweepLegacyAccountTerminalsArchivesAnEmptyLedger(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	path := writeLegacyTerminalLedger(t, 0)

	if err := sweepLegacyAccountTerminals(); err != nil {
		t.Fatalf("sweep over an empty ledger = %v, want the ordinary upgrade to proceed", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty ledger survived: %v", err)
	}
	if _, err := os.Lstat(path + ".archived"); err != nil {
		t.Fatalf("empty ledger was not archived: %v", err)
	}
}

func TestSweepLegacyAccountTerminalsWithoutALedgerIsANoOp(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	if err := sweepLegacyAccountTerminals(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(pool.AccountTerminalProcessStorePath() + ".archived"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no-op sweep created an archive: %v", err)
	}
}
