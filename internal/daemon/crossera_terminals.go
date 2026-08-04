package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"

	"github.com/yasyf/cc-pool/internal/pool"
)

// legacyTerminalSweepTimeout bounds the whole flag-day terminal sweep: a
// SIGKILLed PTY group settles in milliseconds, and a child that cannot be
// proven gone must fail the boot rather than race a new login.
const legacyTerminalSweepTimeout = 30 * time.Second

// legacyTerminalRecord is the projection of a v0.20.9 proc.Record this sweep
// reads from the account-terminal ledger: the {pid, start_time, boot} identity
// and the group scope TrackGroup recorded. The remaining fields are carried
// era-blind, never decoded.
type legacyTerminalRecord struct {
	PID          int    `json:"pid"`
	StartTime    string `json:"start_time"`
	Boot         string `json:"boot"`
	ProcessGroup bool   `json:"process_group"`
	SessionID    int    `json:"session_id"`
}

// sweepLegacyAccountTerminals settles every account-terminal child the
// v0.20.9 era tracked in its own ledger before business admission opens.
// daemonkit's own recovery covers only the record store Serve derives; the
// legacy PTY children lived in account-terminals-v1.db, which nothing in
// v0.21 reads — a survivor from a crashed v0.20.9 daemon would race a rearmed
// `claude auth login` into the same CLAUDE_CONFIG_DIR and overwrite the
// user's credentials. The ledger is archived only after exact settlement.
// One transition cycle only — ships in v0.21.x, deleted in v0.22, the same
// lifespan as the cross-era gate.
func sweepLegacyAccountTerminals(ctx context.Context) error {
	path := pool.AccountTerminalProcessStorePath()
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect legacy account-terminal ledger: %w", err)
	}
	sweepCtx, cancel := context.WithTimeout(ctx, legacyTerminalSweepTimeout)
	defer cancel()
	records, err := loadLegacyTerminalRecords(path)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := settleLegacyTerminal(sweepCtx, record); err != nil {
			return fmt.Errorf(
				"settle legacy account terminal pid=%d start=%s: %w",
				record.PID, record.StartTime, err,
			)
		}
	}
	if err := os.Rename(path, path+".archived"); err != nil {
		return fmt.Errorf("archive legacy account-terminal ledger: %w", err)
	}
	return nil
}

func loadLegacyTerminalRecords(path string) ([]legacyTerminalRecord, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open legacy account-terminal ledger: %w", err)
	}
	defer func() { _ = db.Close() }()
	var records []legacyTerminalRecord
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("records"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, value []byte) error {
			var record legacyTerminalRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return fmt.Errorf("decode legacy account-terminal record: %w", err)
			}
			if record.PID <= 0 || record.StartTime == "" {
				return fmt.Errorf("legacy account-terminal record names no process identity: %+v", record)
			}
			records = append(records, record)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("read legacy account-terminal ledger: %w", err)
	}
	return records, nil
}

// settleLegacyTerminal proves one recorded child gone: absent or PID-reused
// records are already settled; a live match is SIGKILLed — group-wide only
// while the group still sits in its recorded dedicated session — and observed
// out of the process table.
func settleLegacyTerminal(ctx context.Context, record legacyTerminalRecord) error {
	if !legacyTerminalAlive(record) {
		return nil
	}
	target := record.PID
	if record.ProcessGroup && record.SessionID != 0 {
		if sid, err := unix.Getsid(record.PID); err == nil && sid == record.SessionID {
			target = -record.PID
		}
	}
	if err := unix.Kill(target, unix.SIGKILL); err != nil && !errors.Is(err, unix.ESRCH) {
		return fmt.Errorf("kill legacy terminal target %d: %w", target, err)
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !legacyTerminalAlive(record) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("legacy terminal did not settle: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// legacyTerminalAlive reports whether the recorded {pid, start_time} identity
// is still present, with the v0.20.9 stamp encoding: "%d.%06d" over the
// kernel's P_starttime (daemonkit@v0.20.9 proc/reaper_darwin.go:61-67). A
// reused PID reads as settled.
func legacyTerminalAlive(record legacyTerminalRecord) bool {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", record.PID)
	if err != nil {
		return false
	}
	st := kp.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", st.Sec, st.Usec) == record.StartTime
}
