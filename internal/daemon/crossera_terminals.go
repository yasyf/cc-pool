package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"

	"github.com/yasyf/cc-pool/internal/pool"
)

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

// legacyTerminalVerdict is what observation proved about one recorded child.
// Only settled is actionable; the sweep never signals, so live and
// undetermined both refuse the boot.
type legacyTerminalVerdict int

const (
	// legacyTerminalSettled means the recorded identity provably cannot be
	// running: a different boot session, an absent PID, or a PID that now
	// names another process instance.
	legacyTerminalSettled legacyTerminalVerdict = iota
	// legacyTerminalLive means the recorded identity, or another process in
	// its recorded session, is present.
	legacyTerminalLive
	// legacyTerminalUndetermined means observation failed. It is never read
	// as absence: a permission-denied or transient probe would otherwise
	// archive the ledger with a live login still holding the config dir.
	legacyTerminalUndetermined
)

// sweepLegacyAccountTerminals refuses the boot while any account-terminal
// child the v0.20.9 era tracked in its own ledger may still be running.
// daemonkit's own recovery covers only the record store Serve derives; the
// legacy PTY children lived in account-terminals-v1.db, which nothing in
// v0.21 reads — a survivor from a crashed v0.20.9 daemon would race a rearmed
// `claude auth login` into the same CLAUDE_CONFIG_DIR and overwrite the
// user's credentials.
//
// The sweep never signals. POSIX offers no atomic signal-by-instance, so a
// kill decided by a prior observation can only be aimed at a PID, and a PID
// freed between the two lands the signal on whatever took it: a false SIGKILL
// destroys an unrelated process of the user's, while a refused boot is
// recoverable by closing the login it names. The ledger is archived only once
// every record is provably settled.
//
// One transition cycle only — ships in v0.21.x, deleted in v0.22, the same
// lifespan as the cross-era gate.
func sweepLegacyAccountTerminals(ctx context.Context) error {
	path := pool.AccountTerminalProcessStorePath()
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect legacy account-terminal ledger: %w", err)
	}
	records, err := loadLegacyTerminalRecords(path)
	if err != nil {
		return err
	}
	boot, err := currentBootSession()
	if err != nil {
		return fmt.Errorf("resolve boot session for the legacy account-terminal sweep: %w", err)
	}
	var unsettled []string
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sweep legacy account terminals: %w", err)
		}
		verdict, detail := classifyLegacyTerminal(record, boot)
		if verdict != legacyTerminalSettled {
			unsettled = append(unsettled, detail)
		}
	}
	if len(unsettled) > 0 {
		return fmt.Errorf(
			"cc-pool refused to start: an account login left over from before the upgrade may still be running, "+
				"and letting it finish would overwrite credentials this daemon manages. %s. "+
				"The daemon will keep retrying and start on its own once that process is gone",
			strings.Join(unsettled, ". "),
		)
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
			if record.PID <= 0 || record.StartTime == "" || record.Boot == "" {
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

// classifyLegacyTerminal observes one record. Over-detection is the safe
// direction throughout — a live verdict costs a refused boot, an absent one
// costs the credentials this whole sweep exists to protect — so every
// ambiguity resolves away from settled.
func classifyLegacyTerminal(record legacyTerminalRecord, boot string) (legacyTerminalVerdict, string) {
	// A record captured in another boot session names a process that cannot
	// have survived, and its session id belongs to that boot's numbering.
	if record.Boot != boot {
		return legacyTerminalSettled, ""
	}
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", record.PID)
	switch {
	case errors.Is(err, unix.ESRCH):
		// The leader is gone; the session may still hold descendants.
	case err != nil:
		return legacyTerminalUndetermined, fmt.Sprintf(
			"The old login's process (pid %d) could not be checked (%v) — this usually clears on its own; "+
				"if it persists, restart your machine", record.PID, err,
		)
	case legacyTerminalStamp(kp) == record.StartTime:
		return legacyTerminalLive, fmt.Sprintf(
			"Close the `claude auth login` window if one is open, or run `kill %d`", record.PID,
		)
	}
	// The PID is absent or now names another instance. Descendants of the
	// recorded session survive it, and they hold the same config dir, so the
	// session is swept whole rather than through its leader alone.
	if record.SessionID == 0 {
		return legacyTerminalSettled, ""
	}
	members, err := legacyTerminalSessionMembers(record.SessionID)
	if err != nil {
		return legacyTerminalUndetermined, fmt.Sprintf(
			"The old login's terminal session (%d) could not be checked (%v) — this usually clears on its own; "+
				"if it persists, restart your machine", record.SessionID, err,
		)
	}
	if len(members) > 0 {
		commands := make([]string, 0, len(members))
		for _, pid := range members {
			commands = append(commands, fmt.Sprintf("kill %d", pid))
		}
		return legacyTerminalLive, fmt.Sprintf(
			"Close the `claude auth login` window if one is open, or run `%s`",
			strings.Join(commands, " && "),
		)
	}
	return legacyTerminalSettled, ""
}

// legacyTerminalSessionMembers reports every live process still in session,
// which is the scope a recorded process-group leader stood for: a login that
// forked into another group outlives its leader inside the same session.
func legacyTerminalSessionMembers(sessionID int) ([]int, error) {
	all, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, err
	}
	var members []int
	for index := range all {
		pid := int(all[index].Proc.P_pid)
		if pid <= 0 {
			continue
		}
		sid, err := unix.Getsid(pid)
		if err != nil {
			// The process left between enumeration and the probe; a live one
			// would still be named by its own entry.
			continue
		}
		if sid == sessionID {
			members = append(members, pid)
		}
	}
	return members, nil
}

// legacyTerminalStamp renders the process start stamp in the v0.20.9 encoding
// (daemonkit@v0.20.9 proc/reaper_darwin.go:61-67).
func legacyTerminalStamp(kp *unix.KinfoProc) string {
	st := kp.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", st.Sec, st.Usec)
}

// currentBootSession renders the boot stamp in the v0.20.9 encoding
// (daemonkit@v0.20.9 proc/boot_darwin.go).
func currentBootSession() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d.%06d", tv.Sec, tv.Usec), nil
}
