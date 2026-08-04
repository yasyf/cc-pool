package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
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
// There is no per-record undetermined: the whole sweep reads one process-table
// snapshot, so observation either succeeded for every record or failed for all
// of them, and the failure refuses the boot before any record is classified.
type legacyTerminalVerdict int

const (
	// legacyTerminalSettled means the recorded identity provably cannot be
	// running: a different boot session, an absent PID, or a PID that now
	// names another process instance.
	legacyTerminalSettled legacyTerminalVerdict = iota
	// legacyTerminalLive means the recorded identity, or another process in
	// its recorded session, is present.
	legacyTerminalLive
)

// processSnapshot is one observation of the process table: the start stamp of
// every live process, and the session each belongs to. It is the sweep's only
// source of truth about liveness, because a per-PID probe cannot distinguish
// "no such process" from "the probe failed" — darwin answers a missing PID
// with EIO, the same class a real failure would raise — while an enumeration
// that returns at all is authoritative about every PID absent from it.
type processSnapshot struct {
	stamps   map[int]string
	sessions map[int]int
}

// errUnstableProcessScan means a process enumerated at the start of a scan
// had vanished by the time its session was read. The scan is then worthless
// for proving absence: that process may have forked a replacement into the
// same session after the enumeration, so the replacement is in neither the
// enumeration nor the session map, and an empty session would be a lie.
var errUnstableProcessScan = errors.New("process table changed during the scan")

// processScanAttempts bounds the retry. A vanish is rare — a scan is a few
// milliseconds over a table that changes a few times a second — so a stable
// scan normally lands on the first attempt, and exhausting the retries is
// itself a signal that the machine is too busy to prove anything on.
const processScanAttempts = 12

// snapshotProcessTable returns a scan that provably held still. Session
// emptiness is monotonic — setsid always creates a new session and nothing
// can join an existing one, so a session with no members can never gain one —
// which is what makes a single stable scan permanent proof rather than an
// instantaneous one.
func snapshotProcessTable() (processSnapshot, error) {
	return stableProcessSnapshot(func() (processSnapshot, error) {
		return scanProcessTable(liveProcessProbe())
	})
}

// processProbe is the pair of kernel reads a scan composes. It is a seam
// because the defect this scan exists to catch — a process vanishing between
// the two — cannot be staged against the real kernel on demand.
type processProbe struct {
	enumerate func() ([]unix.KinfoProc, error)
	sessionOf func(pid int) (int, error)
}

func liveProcessProbe() processProbe {
	return processProbe{
		enumerate: func() ([]unix.KinfoProc, error) {
			return unix.SysctlKinfoProcSlice("kern.proc.all")
		},
		sessionOf: unix.Getsid,
	}
}

func stableProcessSnapshot(scan func() (processSnapshot, error)) (processSnapshot, error) {
	var err error
	for attempt := 0; attempt < processScanAttempts; attempt++ {
		var snapshot processSnapshot
		snapshot, err = scan()
		if err == nil {
			return snapshot, nil
		}
		if !errors.Is(err, errUnstableProcessScan) {
			return processSnapshot{}, err
		}
	}
	return processSnapshot{}, fmt.Errorf("%w after %d attempts", err, processScanAttempts)
}

func scanProcessTable(probe processProbe) (processSnapshot, error) {
	all, err := probe.enumerate()
	if err != nil {
		return processSnapshot{}, err
	}
	// SysctlKinfoProcSlice answers a zero-size probe with an empty slice and
	// a nil error, and treating that as "nothing is alive" would settle every
	// record and archive the ledger with a live login running. The check
	// below refuses it as unauthoritative.
	snapshot := processSnapshot{
		stamps:   make(map[int]string, len(all)),
		sessions: make(map[int]int, len(all)),
	}
	for index := range all {
		pid := int(all[index].Proc.P_pid)
		if pid <= 0 {
			continue
		}
		snapshot.stamps[pid] = legacyTerminalStamp(&all[index])
		// Getsid succeeds across sessions and users on darwin (verified: zero
		// failures over a 1039-process table), so an error here means this
		// process vanished between the enumeration and now — the one event
		// that can hide a forked replacement from both halves of the scan.
		sid, err := probe.sessionOf(pid)
		if err != nil {
			return processSnapshot{}, fmt.Errorf("%w: pid %d left mid-scan", errUnstableProcessScan, pid)
		}
		snapshot.sessions[pid] = sid
	}
	if err := snapshot.authoritative(); err != nil {
		return processSnapshot{}, err
	}
	return snapshot, nil
}

// authoritative is the positive proof that the enumeration means anything:
// this process is alive with certainty, so a snapshot that does not contain
// it has proven nothing about absence, whatever its size. This closes the
// zero-size success return and any future silent-empty path in one check —
// it validates the instrument, not the reading.
func (snapshot processSnapshot) authoritative() error {
	if _, present := snapshot.stamps[os.Getpid()]; !present {
		return fmt.Errorf(
			"process enumeration omitted this process (pid %d) and is not authoritative about absence",
			os.Getpid(),
		)
	}
	return nil
}

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
	table, err := snapshotProcessTable()
	if err != nil {
		return fmt.Errorf(
			"cc-pool refused to start: it could not check whether an account login left over from before "+
				"the upgrade is still running (%w). This usually clears on its own — the daemon keeps "+
				"retrying; if it persists, restart your machine", err,
		)
	}
	var unsettled []string
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sweep legacy account terminals: %w", err)
		}
		verdict, detail := classifyLegacyTerminal(record, boot, table)
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

// classifyLegacyTerminal reads one record against the snapshot. Over-detection
// is the safe direction throughout — a live verdict costs a refused boot, a
// wrong settled one costs the credentials this sweep exists to protect — so
// every ambiguity resolves away from settled.
func classifyLegacyTerminal(
	record legacyTerminalRecord,
	boot string,
	table processSnapshot,
) (legacyTerminalVerdict, string) {
	// A record captured in another boot session names a process that cannot
	// have survived, and its session id belongs to that boot's numbering.
	if record.Boot != boot {
		return legacyTerminalSettled, ""
	}
	surviving := make(map[int]struct{})
	if stamp, present := table.stamps[record.PID]; present && stamp == record.StartTime {
		surviving[record.PID] = struct{}{}
	}
	// The session is collected whether or not the leader survived. Ending the
	// leader alone can leave its login child running, so a refusal that named
	// only the leader would print a command the user runs to no effect.
	if record.SessionID != 0 {
		for pid, sid := range table.sessions {
			if sid == record.SessionID {
				surviving[pid] = struct{}{}
			}
		}
	}
	if len(surviving) == 0 {
		return legacyTerminalSettled, ""
	}
	pids := make([]int, 0, len(surviving))
	for pid := range surviving {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	commands := make([]string, 0, len(pids))
	for _, pid := range pids {
		commands = append(commands, fmt.Sprintf("kill %d", pid))
	}
	return legacyTerminalLive, fmt.Sprintf(
		"Close the `claude auth login` window if one is open, or run `%s`",
		strings.Join(commands, " && "),
	)
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
