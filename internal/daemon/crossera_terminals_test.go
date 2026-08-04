package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/testhome"
)

func writeLegacyTerminalLedger(t *testing.T, records []legacyTerminalRecord) string {
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
		for index, record := range records {
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := bucket.Put(fmt.Appendf(nil, "record-%d", index), encoded); err != nil {
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

func terminalStamp(t *testing.T, pid int) string {
	t.Helper()
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		t.Fatal(err)
	}
	return legacyTerminalStamp(kp)
}

func testBootSession(t *testing.T) string {
	t.Helper()
	boot, err := currentBootSession()
	if err != nil {
		t.Fatal(err)
	}
	return boot
}

// startLegacyTerminalSurvivor starts a process standing in for a v0.20.9 PTY
// child that outlived its daemon: its own session, so the sweep's session
// scan has a real scope to find.
func startLegacyTerminalSurvivor(t *testing.T) (pid, sid int) {
	t.Helper()
	survivor := exec.Command("/bin/sleep", "60")
	survivor.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := survivor.Start(); err != nil {
		t.Fatal(err)
	}
	settled := make(chan struct{})
	go func() {
		_ = survivor.Wait()
		close(settled)
	}()
	t.Cleanup(func() {
		_ = survivor.Process.Kill()
		select {
		case <-settled:
		case <-time.After(5 * time.Second):
			t.Error("survivor never settled")
		}
	})
	pid = survivor.Process.Pid
	sid, err := unix.Getsid(pid)
	if err != nil {
		t.Fatal(err)
	}
	return pid, sid
}

// TestSweepLegacyAccountTerminalsRefusesWithoutSignallingALiveSurvivor is the
// blocker regression: the sweep decides from an observation it cannot hold
// across a signal, so it must never signal at all. A live recorded child
// refuses the boot, keeps the ledger, and is left running — the alternative,
// a kill aimed at a PID that may have been freed since the check, destroys
// whatever process took that PID.
func TestSweepLegacyAccountTerminalsRefusesWithoutSignallingALiveSurvivor(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	pid, sid := startLegacyTerminalSurvivor(t)
	path := writeLegacyTerminalLedger(t, []legacyTerminalRecord{{
		PID: pid, StartTime: terminalStamp(t, pid), Boot: testBootSession(t),
		ProcessGroup: true, SessionID: sid,
	}})

	err := sweepLegacyAccountTerminals(t.Context())
	if err == nil || !strings.Contains(err.Error(), "may still be running") {
		t.Fatalf("sweep with a live survivor = %v, want a refusal naming the login", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(pid)) {
		t.Fatalf("refusal does not name the surviving pid %d: %v", pid, err)
	}

	// The survivor must be untouched: never signalled, still its own instance.
	if got := terminalStamp(t, pid); got == "" {
		t.Fatal("survivor was signalled by a sweep that must never signal")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("ledger was archived despite an unsettled record: %v", err)
	}
	if _, err := os.Lstat(path + ".archived"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsettled sweep created an archive: %v", err)
	}
}

// TestSweepLegacyAccountTerminalsArchivesOnlyProvablySettledRecords covers the
// two shapes that prove absence without any signal: a PID that now names
// another instance, and a record from a previous boot session.
func TestSweepLegacyAccountTerminalsArchivesOnlyProvablySettledRecords(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	livePID, liveSID := startLegacyTerminalSurvivor(t)
	path := writeLegacyTerminalLedger(t, []legacyTerminalRecord{
		// PID reuse: this process is alive, but under another start stamp, so
		// the record names an instance that is gone.
		{PID: os.Getpid(), StartTime: "1.000001", Boot: testBootSession(t)},
		// Cross-boot: the identity was captured before the last reboot, so no
		// process can carry it — and its session id belongs to that numbering,
		// which is why a live session here must not read as this record's.
		{PID: livePID, StartTime: terminalStamp(t, livePID), Boot: "1.000001", SessionID: liveSID},
	})

	if err := sweepLegacyAccountTerminals(t.Context()); err != nil {
		t.Fatalf("sweep over provably settled records = %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("settled ledger survived: %v", err)
	}
	if _, err := os.Lstat(path + ".archived"); err != nil {
		t.Fatalf("settled ledger was not archived: %v", err)
	}
}

func TestClassifyLegacyTerminalNeverReadsAProbeFailureAsAbsence(t *testing.T) {
	boot := testBootSession(t)
	// A session id nothing can be in, on the current boot, with an absent
	// leader: the honest verdict is settled.
	verdict, _ := classifyLegacyTerminal(legacyTerminalRecord{
		PID: 1 << 30, StartTime: "1.000001", Boot: boot, SessionID: 1 << 30,
	}, boot)
	if verdict != legacyTerminalSettled {
		t.Fatalf("absent leader with an empty session = %v, want settled", verdict)
	}
	// The live shape must never be settled.
	pid, sid := startLegacyTerminalSurvivor(t)
	verdict, detail := classifyLegacyTerminal(legacyTerminalRecord{
		PID: pid, StartTime: terminalStamp(t, pid), Boot: boot, SessionID: sid,
	}, boot)
	if verdict != legacyTerminalLive || detail == "" {
		t.Fatalf("live leader = %v (%q), want live with a detail", verdict, detail)
	}
	// A surviving session member with a settled leader is still live: the
	// leader alone is not the scope that holds the config dir.
	verdict, detail = classifyLegacyTerminal(legacyTerminalRecord{
		PID: 1 << 30, StartTime: "1.000001", Boot: boot, SessionID: sid,
	}, boot)
	if verdict != legacyTerminalLive || !strings.Contains(detail, fmt.Sprint(pid)) {
		t.Fatalf("surviving session member = %v (%q), want live naming pid %d", verdict, detail, pid)
	}
}

func TestSweepLegacyAccountTerminalsWithoutALedgerIsANoOp(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	if err := sweepLegacyAccountTerminals(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(pool.AccountTerminalProcessStorePath() + ".archived"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no-op sweep created an archive: %v", err)
	}
}
