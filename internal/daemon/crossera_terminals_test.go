package daemon

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

// advertisedRecovery extracts the one command a refusal tells the user to run,
// so a test can run exactly what the user would rather than a paraphrase of it.
func advertisedRecovery(t *testing.T, refusal error) string {
	t.Helper()
	const prefix = "then run `"
	message := refusal.Error()
	start := strings.Index(message, prefix)
	if start < 0 {
		t.Fatalf("refusal %q advertises no recovery command", message)
	}
	command, _, terminated := strings.Cut(message[start+len(prefix):], "`")
	if !terminated {
		t.Fatalf("refusal %q leaves its recovery command unterminated", message)
	}
	return command
}

// TestSweepLegacyAccountTerminalsRefusesWhileRecordsRemain pins the whole
// design: any recorded login refuses the boot with a recovery command that
// runs, touches no process, and archives nothing. The advertised command is
// the only documented way out of the refusal, so the test executes it through
// `/bin/sh` — including from a home directory whose name carries an
// apostrophe, where an unquoted path renders a command the shell cannot parse.
func TestSweepLegacyAccountTerminalsRefusesWhileRecordsRemain(t *testing.T) {
	tests := []struct {
		name string
		home string
	}{
		{"plain home", "home"},
		{"home carrying an apostrophe", "O'Connor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := filepath.Join(t.TempDir(), tt.home)
			if err := os.MkdirAll(home, 0o700); err != nil {
				t.Fatal(err)
			}
			testhome.Sandbox(t, home)
			if err := pool.EnsureStateDir(); err != nil {
				t.Fatal(err)
			}
			path := writeLegacyTerminalLedger(t, 2)

			refusal := sweepLegacyAccountTerminals()
			if refusal == nil {
				t.Fatal("sweep with recorded terminals did not refuse")
			}
			for _, want := range []string{
				"2 interactive login terminal(s)",
				"claude auth login",
				"retries automatically",
			} {
				if !strings.Contains(refusal.Error(), want) {
					t.Fatalf("refusal %q does not carry %q", refusal, want)
				}
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("refusal disturbed the ledger: %v", err)
			}
			if _, err := os.Lstat(path + ".archived"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refusal created an archive: %v", err)
			}

			command := advertisedRecovery(t, refusal)
			output, err := exec.Command("/bin/sh", "-c", command).CombinedOutput()
			if err != nil {
				t.Fatalf("advertised recovery %s = %v: %s", command, err, output)
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("advertised recovery left the ledger behind: %v", err)
			}
			if err := sweepLegacyAccountTerminals(); err != nil {
				t.Fatalf("sweep after the advertised command = %v, want a clean boot", err)
			}
		})
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
