package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	st := kp.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", st.Sec, st.Usec)
}

// TestSweepLegacyAccountTerminalsKillsSurvivorsSkipsReuseAndArchives is the
// test F2 lacked: a live child recorded by the v0.20.9 ledger is settled
// before admission, a live PID whose start stamp disagrees (PID reuse) is
// left untouched, and the ledger archives only after settlement.
func TestSweepLegacyAccountTerminalsKillsSurvivorsSkipsReuseAndArchives(t *testing.T) {
	testhome.Sandbox(t, t.TempDir())
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	survivor := exec.Command("/bin/sleep", "60")
	survivor.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := survivor.Start(); err != nil {
		t.Fatal(err)
	}
	// settled closes rather than carrying a value: the assertion below and
	// the cleanup both wait on it, and a one-value channel would park the
	// second receiver forever.
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
			t.Error("recorded survivor never settled")
		}
	})
	pid := survivor.Process.Pid
	sid, err := unix.Getsid(pid)
	if err != nil {
		t.Fatal(err)
	}
	path := writeLegacyTerminalLedger(t, []legacyTerminalRecord{
		{PID: pid, StartTime: terminalStamp(t, pid), Boot: "boot-1", ProcessGroup: true, SessionID: sid},
		// The test's own live PID under a wrong stamp is the PID-reuse shape:
		// the sweep must read it as settled and never signal it.
		{PID: os.Getpid(), StartTime: "1.000001", Boot: "boot-1", ProcessGroup: true, SessionID: 1},
	})

	if err := sweepLegacyAccountTerminals(t.Context()); err != nil {
		t.Fatalf("sweep legacy account terminals: %v", err)
	}

	select {
	case <-settled:
	case <-time.After(5 * time.Second):
		t.Fatal("recorded survivor was not settled")
	}
	if legacyTerminalAlive(legacyTerminalRecord{PID: pid, StartTime: terminalStamp(t, os.Getpid())}) {
		t.Fatal("survivor identity still present after sweep")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ledger survived settlement: %v", err)
	}
	if _, err := os.Lstat(path + ".archived"); err != nil {
		t.Fatalf("ledger was not archived: %v", err)
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
