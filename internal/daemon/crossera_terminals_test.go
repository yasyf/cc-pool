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

// TestClassifyLegacyTerminalReadsTheSnapshotNotAPerPIDProbe pins the three
// settled proofs and the two live ones against a constructed table. An absent
// PID is the ordinary upgrade case — the old login exited cleanly and left its
// record behind — and reading it as anything but settled would refuse every
// upgrade; darwin answers a per-PID probe for a missing process with EIO, so
// only the enumeration can tell absence from failure.
func TestClassifyLegacyTerminalReadsTheSnapshotNotAPerPIDProbe(t *testing.T) {
	const boot = "1000.000001"
	table := processSnapshot{
		stamps:   map[int]string{100: "50.000001", 200: "60.000002"},
		sessions: map[int]int{100: 100, 200: 100},
	}
	tests := []struct {
		name   string
		record legacyTerminalRecord
		want   legacyTerminalVerdict
		detail string
	}{
		{
			name:   "absent pid with no session is settled",
			record: legacyTerminalRecord{PID: 999, StartTime: "1.000001", Boot: boot},
			want:   legacyTerminalSettled,
		},
		{
			name:   "absent pid whose session is empty is settled",
			record: legacyTerminalRecord{PID: 999, StartTime: "1.000001", Boot: boot, SessionID: 777},
			want:   legacyTerminalSettled,
		},
		{
			name:   "a pid now naming another instance is settled",
			record: legacyTerminalRecord{PID: 100, StartTime: "1.000001", Boot: boot, SessionID: 777},
			want:   legacyTerminalSettled,
		},
		{
			name:   "another boot session is settled without consulting the table",
			record: legacyTerminalRecord{PID: 100, StartTime: "50.000001", Boot: "9.000009", SessionID: 100},
			want:   legacyTerminalSettled,
		},
		{
			name:   "the recorded leader still running is live",
			record: legacyTerminalRecord{PID: 100, StartTime: "50.000001", Boot: boot, SessionID: 100},
			want:   legacyTerminalLive,
			detail: "kill 100",
		},
		{
			name:   "a surviving session member outlives its settled leader",
			record: legacyTerminalRecord{PID: 999, StartTime: "1.000001", Boot: boot, SessionID: 100},
			want:   legacyTerminalLive,
			detail: "kill 100 && kill 200",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verdict, detail := classifyLegacyTerminal(tt.record, boot, table)
			if verdict != tt.want {
				t.Fatalf("verdict = %v, want %v (detail %q)", verdict, tt.want, detail)
			}
			if tt.detail != "" && !strings.Contains(detail, tt.detail) {
				t.Fatalf("detail = %q, want it to name %q", detail, tt.detail)
			}
			if tt.want == legacyTerminalLive && !strings.Contains(detail, "claude auth login") {
				t.Fatalf("live detail = %q, want an actionable instruction", detail)
			}
		})
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

// TestSnapshotAuthorityRequiresOurOwnPID pins the instrument validation: an
// enumeration that omits a process known with certainty to be alive — this
// one — has proven nothing about absence, and SysctlKinfoProcSlice can return
// exactly that shape as a success (nil, nil on a zero-size probe).
func TestSnapshotAuthorityRequiresOurOwnPID(t *testing.T) {
	empty := processSnapshot{stamps: map[int]string{}, sessions: map[int]int{}}
	if err := empty.authoritative(); err == nil {
		t.Fatal("an empty enumeration passed as authoritative")
	}
	withoutUs := processSnapshot{stamps: map[int]string{1: "1.000001"}, sessions: map[int]int{}}
	if err := withoutUs.authoritative(); err == nil {
		t.Fatal("an enumeration omitting this process passed as authoritative")
	}
	real, err := snapshotProcessTable()
	if err != nil {
		t.Fatalf("real process enumeration = %v, want authoritative", err)
	}
	if _, present := real.stamps[os.Getpid()]; !present {
		t.Fatal("real snapshot passed authority without containing this process")
	}
}
