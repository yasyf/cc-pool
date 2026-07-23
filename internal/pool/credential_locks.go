package pool

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	daemonstate "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

const (
	credentialLockJournalSchema = 1
	lockMarkerName              = ".cc-pool-owner-v1.json"
	credentialLockPollInterval  = 25 * time.Millisecond
	credentialLockMaxJournal    = 64 << 10
)

type credentialLockPhase string

const (
	credentialLockIntended     credentialLockPhase = "intended"
	credentialLockStageCreated credentialLockPhase = "stage-created"
	credentialLockPrepared     credentialLockPhase = "prepared"
	credentialLockAcquired     credentialLockPhase = "acquired"
	credentialLockReleasing    credentialLockPhase = "releasing"
	credentialLockReleased     credentialLockPhase = "released"
)

type credentialLockFingerprint struct {
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	BirthSecond int64  `json:"birth_second"`
	BirthNanos  int64  `json:"birth_nanos"`
}

func (fingerprint credentialLockFingerprint) zero() bool {
	return fingerprint == credentialLockFingerprint{}
}

type credentialLockTarget struct {
	Path        string                    `json:"path"`
	Stage       string                    `json:"stage"`
	Phase       credentialLockPhase       `json:"phase"`
	Fingerprint credentialLockFingerprint `json:"fingerprint"`
}

type credentialLockJournal struct {
	Schema    int                    `json:"schema"`
	AccountID int                    `json:"account_id"`
	Nonce     string                 `json:"nonce"`
	Worker    proc.Record            `json:"worker"`
	Targets   []credentialLockTarget `json:"targets"`
}

type credentialLockMarker struct {
	Schema      int                       `json:"schema"`
	AccountID   int                       `json:"account_id"`
	Nonce       string                    `json:"nonce"`
	Worker      proc.Record               `json:"worker"`
	Target      string                    `json:"target"`
	Fingerprint credentialLockFingerprint `json:"fingerprint"`
}

type credentialLockLease struct {
	journalPath  string
	journal      credentialLockJournal
	guard        *proc.FileLockHandle
	journalOwned bool
	released     bool
}

var credentialLockFailpoint func(string)

func credentialLockCheckpoint(name string) {
	if credentialLockFailpoint != nil {
		credentialLockFailpoint(name)
	}
}

func credentialLockJournalPath(accountID int) string {
	return statePath("credential-locks", AccountDirName(accountID)+".json")
}

func credentialRefreshLockPaths(configDir string) ([]string, error) {
	realConfigDir, err := filepath.EvalSymlinks(configDir)
	if err != nil {
		return nil, fmt.Errorf("resolve credential lock directory: %w", err)
	}
	return []string{filepath.Join(configDir, ".oauth_refresh.lock"), realConfigDir + ".lock"}, nil
}

func acquireCredentialRefreshLocks(
	ctx context.Context,
	accountID int,
	configDir string,
) (*credentialLockLease, error) {
	paths, err := credentialRefreshLockPaths(configDir)
	if err != nil {
		return nil, err
	}
	journalPath := credentialLockJournalPath(accountID)
	guard, err := (proc.FileLockSpec{
		Path: journalPath + ".guard", Mode: proc.FileLockExclusive,
		Deadline: credentialCASWorkerTimeout,
	}).Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire credential lock journal: %w", err)
	}
	lease := &credentialLockLease{journalPath: journalPath, guard: guard}
	fail := func(cause error) (*credentialLockLease, error) {
		var cleanupErr error
		if lease.journalOwned {
			cleanupErr = lease.Release(ctx)
		} else if lease.guard != nil {
			cleanupErr = lease.guard.Close()
			lease.guard = nil
		}
		return nil, errors.Join(cause, cleanupErr)
	}
	if err := recoverCredentialLockJournal(ctx, journalPath, accountID, paths); err != nil {
		return fail(err)
	}
	worker, err := currentCredentialLockWorker()
	if err != nil {
		return fail(err)
	}
	nonce, err := newCredentialLockNonce()
	if err != nil {
		return fail(err)
	}
	lease.journal = credentialLockJournal{
		Schema: credentialLockJournalSchema, AccountID: accountID,
		Nonce: nonce, Worker: worker, Targets: make([]credentialLockTarget, len(paths)),
	}
	for index, path := range paths {
		lease.journal.Targets[index] = credentialLockTarget{
			Path:  path,
			Stage: credentialLockStagePath(path, nonce, index),
			Phase: credentialLockIntended,
		}
	}
	if err := lease.writeJournal(); err != nil {
		return fail(err)
	}
	lease.journalOwned = true
	credentialLockCheckpoint("journal-intended")
	for index := range lease.journal.Targets {
		if err := lease.acquireTarget(ctx, index); err != nil {
			return fail(err)
		}
	}
	credentialLockCheckpoint("locks-acquired")
	return lease, nil
}

func (lease *credentialLockLease) acquireTarget(ctx context.Context, index int) error {
	target := &lease.journal.Targets[index]
	if err := os.Mkdir(target.Stage, 0o700); err != nil {
		return fmt.Errorf("create credential lock stage: %w", err)
	}
	target.Phase = credentialLockStageCreated
	credentialLockCheckpoint(fmt.Sprintf("stage-created-%d", index))
	fingerprint, err := credentialLockFingerprintForPath(target.Stage)
	if err != nil {
		return err
	}
	marker := credentialLockMarker{
		Schema: credentialLockJournalSchema, AccountID: lease.journal.AccountID,
		Nonce: lease.journal.Nonce, Worker: lease.journal.Worker,
		Target: target.Path, Fingerprint: fingerprint,
	}
	if err := writeCredentialLockMarker(target.Stage, marker); err != nil {
		return err
	}
	target.Fingerprint = fingerprint
	target.Phase = credentialLockPrepared
	if err := lease.writeJournal(); err != nil {
		return err
	}
	credentialLockCheckpoint(fmt.Sprintf("stage-prepared-%d", index))
	ticker := time.NewTicker(credentialLockPollInterval)
	defer ticker.Stop()
	for {
		err := publishCredentialLockDirectory(target.Stage, target.Path)
		if err == nil {
			if err := daemonstate.SyncDir(filepath.Dir(target.Path)); err != nil {
				return err
			}
			credentialLockCheckpoint(fmt.Sprintf("target-published-%d", index))
			target.Phase = credentialLockAcquired
			if err := lease.writeJournal(); err != nil {
				return err
			}
			credentialLockCheckpoint(fmt.Sprintf("target-acquired-%d", index))
			return nil
		}
		if !errors.Is(err, os.ErrExist) && !errors.Is(err, syscall.EEXIST) {
			return fmt.Errorf("publish credential refresh lock: %w", err)
		}
		credentialLockCheckpoint(fmt.Sprintf("target-contended-%d", index))
		if err := validateCredentialLockDirectory(target.Path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (lease *credentialLockLease) Release(ctx context.Context) error {
	if lease == nil || lease.released {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		credentialCASWorkerTimeout,
	)
	defer cancel()

	var releaseErr error
	checkCleanup := func() bool {
		if err := cleanupCtx.Err(); err != nil {
			releaseErr = errors.Join(releaseErr, err)
			return false
		}
		return true
	}

releaseTargets:
	for index := len(lease.journal.Targets) - 1; index >= 0; index-- {
		if !checkCleanup() {
			break
		}
		target := &lease.journal.Targets[index]
		switch target.Phase {
		case credentialLockAcquired:
			target.Phase = credentialLockReleasing
			if err := lease.writeJournal(); err != nil {
				releaseErr = errors.Join(releaseErr, err)
				continue
			}
			credentialLockCheckpoint(fmt.Sprintf("release-intended-%d", index))
			if !checkCleanup() {
				break releaseTargets
			}
			if err := releaseCredentialLockTarget(lease.journal, *target, true); err != nil {
				releaseErr = errors.Join(releaseErr, err)
				continue
			}
			if !checkCleanup() {
				break releaseTargets
			}
			target.Phase = credentialLockReleased
			if err := lease.writeJournal(); err != nil {
				releaseErr = errors.Join(releaseErr, err)
				continue
			}
			credentialLockCheckpoint(fmt.Sprintf("target-released-%d", index))
		case credentialLockPrepared:
			stageExisted, err := cleanupCredentialLockStage(lease.journal, *target)
			if err != nil {
				releaseErr = errors.Join(releaseErr, err)
				continue
			}
			if !stageExisted {
				if !checkCleanup() {
					break releaseTargets
				}
				if err := releaseCredentialLockTarget(lease.journal, *target, true); err != nil {
					releaseErr = errors.Join(releaseErr, err)
				}
			}
		case credentialLockStageCreated, credentialLockIntended:
			if _, err := cleanupCredentialLockStage(lease.journal, *target); err != nil {
				releaseErr = errors.Join(releaseErr, err)
			}
		case credentialLockReleasing:
			if err := releaseCredentialLockTarget(lease.journal, *target, false); err != nil {
				releaseErr = errors.Join(releaseErr, err)
			}
		}
	}
	if releaseErr == nil && lease.journalOwned {
		if err := cleanupCtx.Err(); err != nil {
			releaseErr = errors.Join(releaseErr, err)
		} else if err := removeCredentialLockJournal(lease.journalPath); err != nil {
			releaseErr = err
		} else {
			lease.journalOwned = false
			credentialLockCheckpoint("journal-removed")
		}
	}
	if lease.guard != nil {
		releaseErr = errors.Join(releaseErr, lease.guard.Close())
		lease.guard = nil
	}
	lease.released = releaseErr == nil
	return releaseErr
}

func releaseCredentialLockTarget(
	journal credentialLockJournal,
	target credentialLockTarget,
	requireMarker bool,
) error {
	info, err := os.Lstat(target.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("credential lock target changed type before release")
	}
	fingerprint, err := credentialLockFingerprintForPath(target.Path)
	if err != nil {
		return err
	}
	if fingerprint != target.Fingerprint {
		return errors.New("credential lock target identity changed before release")
	}
	markerPath := filepath.Join(target.Path, lockMarkerName)
	marker, markerErr := readCredentialLockMarker(markerPath)
	if markerErr == nil {
		if err := validateCredentialLockMarker(journal, target, marker); err != nil {
			return err
		}
		if err := os.Remove(markerPath); err != nil {
			return err
		}
		if err := daemonstate.SyncDir(target.Path); err != nil {
			return err
		}
		credentialLockCheckpoint(fmt.Sprintf(
			"marker-removed-%d", credentialLockTargetIndex(journal, target.Path),
		))
	} else if requireMarker || !errors.Is(markerErr, os.ErrNotExist) {
		return errors.Join(errors.New("credential lock owner marker is unavailable"), markerErr)
	}
	if err := os.Remove(target.Path); err != nil {
		return fmt.Errorf("remove exact credential lock target: %w", err)
	}
	if err := daemonstate.SyncDir(filepath.Dir(target.Path)); err != nil {
		return err
	}
	credentialLockCheckpoint(fmt.Sprintf(
		"target-removed-%d", credentialLockTargetIndex(journal, target.Path),
	))
	return nil
}

func cleanupCredentialLockStage(
	journal credentialLockJournal,
	target credentialLockTarget,
) (bool, error) {
	info, err := os.Lstat(target.Stage)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	index := credentialLockTargetIndex(journal, target.Path)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		index < 0 || target.Stage != credentialLockStagePath(target.Path, journal.Nonce, index) {
		return true, errors.New("credential lock stage identity is invalid")
	}
	markerPath := filepath.Join(target.Stage, lockMarkerName)
	marker, markerErr := readCredentialLockMarker(markerPath)
	if markerErr == nil {
		if target.Fingerprint.zero() {
			target.Fingerprint = marker.Fingerprint
		}
		if err := validateCredentialLockMarker(journal, target, marker); err != nil {
			return true, err
		}
		if err := os.Remove(markerPath); err != nil {
			return true, err
		}
	} else if !errors.Is(markerErr, os.ErrNotExist) {
		return true, markerErr
	}
	entries, err := os.ReadDir(target.Stage)
	if err != nil {
		return true, err
	}
	if len(entries) != 0 {
		return true, errors.New("credential lock stage contains foreign entries")
	}
	if err := os.Remove(target.Stage); err != nil {
		return true, err
	}
	return true, daemonstate.SyncDir(filepath.Dir(target.Stage))
}

func recoverCredentialLockJournal(
	ctx context.Context,
	journalPath string,
	accountID int,
	paths []string,
) error {
	journal, err := readCredentialLockJournal(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateCredentialLockJournal(journal, accountID, paths); err != nil {
		return err
	}
	for _, target := range journal.Targets {
		stageExisted, err := cleanupCredentialLockStage(journal, target)
		if err != nil {
			return err
		}
		if stageExisted && target.Phase == credentialLockPrepared {
			continue
		}
		if stageExisted && target.Phase == credentialLockAcquired {
			return errors.New("acquired credential lock still has its staging directory")
		}
		info, statErr := os.Lstat(target.Path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return statErr
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("credential lock recovery target changed type")
		}
		switch target.Phase {
		case credentialLockPrepared, credentialLockAcquired:
			if target.Fingerprint.zero() {
				marker, markerErr := readCredentialLockMarker(
					filepath.Join(target.Path, lockMarkerName),
				)
				if markerErr != nil {
					return errors.Join(errors.New("credential lock recovery lacks exact identity"), markerErr)
				}
				target.Fingerprint = marker.Fingerprint
			}
			if err := releaseCredentialLockTarget(journal, target, true); err != nil {
				return err
			}
		case credentialLockReleasing:
			if err := releaseCredentialLockTarget(journal, target, false); err != nil {
				return err
			}
		case credentialLockIntended, credentialLockStageCreated, credentialLockReleased:
			// This operation never published this target, or already removed it.
		default:
			return errors.New("credential lock journal phase is invalid")
		}
	}
	if err := removeCredentialLockJournal(journalPath); err != nil {
		return err
	}
	credentialLockCheckpoint("journal-recovered")
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func currentCredentialLockWorker() (proc.Record, error) {
	identity, err := proc.CurrentIdentity()
	if err != nil {
		return proc.Record{}, err
	}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return proc.Record{}, err
	}
	record := proc.Record{
		RecoveryClass: proc.RecoveryTask,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Executable: identity.Executable, AuditToken: identity.AuditToken,
		Generation: generation,
	}
	if err := record.Validate(); err != nil {
		return proc.Record{}, err
	}
	return record, nil
}

func newCredentialLockNonce() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (lease *credentialLockLease) writeJournal() error {
	if err := validateCredentialLockJournal(
		lease.journal,
		lease.journal.AccountID,
		credentialLockTargetPaths(lease.journal.Targets),
	); err != nil {
		return err
	}
	payload, err := json.Marshal(lease.journal)
	if err != nil {
		return err
	}
	return daemonstate.WriteFileDurable(lease.journalPath, payload, 0o600)
}

func readCredentialLockJournal(path string) (credentialLockJournal, error) {
	var journal credentialLockJournal
	if err := decodeCredentialLockFile(path, &journal); err != nil {
		return credentialLockJournal{}, err
	}
	return journal, nil
}

func removeCredentialLockJournal(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return daemonstate.SyncDir(filepath.Dir(path))
}

func writeCredentialLockMarker(directory string, marker credentialLockMarker) error {
	payload, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	return daemonstate.WriteFileDurable(
		filepath.Join(directory, lockMarkerName), payload, 0o600,
	)
}

func readCredentialLockMarker(path string) (credentialLockMarker, error) {
	var marker credentialLockMarker
	if err := decodeCredentialLockFile(path, &marker); err != nil {
		return credentialLockMarker{}, err
	}
	return marker, nil
}

func decodeCredentialLockFile(path string, value any) error {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(filepath.Base(path))
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	payload, err := io.ReadAll(io.LimitReader(file, credentialLockMaxJournal+1))
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > credentialLockMaxJournal {
		return errors.New("credential lock state has invalid size")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("credential lock state has trailing data")
	}
	return nil
}

func validateCredentialLockJournal(
	journal credentialLockJournal,
	accountID int,
	paths []string,
) error {
	if journal.Schema != credentialLockJournalSchema || journal.AccountID != accountID ||
		len(journal.Nonce) != 32 || len(journal.Targets) != len(paths) {
		return errors.New("credential lock journal identity is invalid")
	}
	if _, err := hex.DecodeString(journal.Nonce); err != nil {
		return errors.New("credential lock journal nonce is invalid")
	}
	if err := journal.Worker.Validate(); err != nil {
		return err
	}
	if journal.Worker.RecoveryClass != proc.RecoveryTask || journal.Worker.ProcessGroup {
		return errors.New("credential lock journal worker kind is invalid")
	}
	for index, target := range journal.Targets {
		if target.Path != paths[index] ||
			target.Stage != credentialLockStagePath(target.Path, journal.Nonce, index) {
			return errors.New("credential lock journal target is invalid")
		}
		switch target.Phase {
		case credentialLockIntended, credentialLockStageCreated:
			if !target.Fingerprint.zero() {
				return errors.New("unprepared credential lock has a fingerprint")
			}
		case credentialLockPrepared, credentialLockAcquired,
			credentialLockReleasing, credentialLockReleased:
			if target.Fingerprint.zero() {
				return errors.New("prepared credential lock lacks a fingerprint")
			}
		default:
			return errors.New("credential lock journal phase is invalid")
		}
	}
	return nil
}

func validateCredentialLockMarker(
	journal credentialLockJournal,
	target credentialLockTarget,
	marker credentialLockMarker,
) error {
	if marker.Schema != credentialLockJournalSchema || marker.AccountID != journal.AccountID ||
		marker.Nonce != journal.Nonce || marker.Worker != journal.Worker ||
		marker.Target != target.Path || marker.Fingerprint != target.Fingerprint {
		return errors.New("credential lock owner marker does not match its journal")
	}
	return nil
}

func credentialLockTargetPaths(targets []credentialLockTarget) []string {
	paths := make([]string, len(targets))
	for index, target := range targets {
		paths[index] = target.Path
	}
	return paths
}

func credentialLockStagePath(path, nonce string, index int) string {
	return filepath.Join(filepath.Dir(path), fmt.Sprintf(".cc-pool-lock-v1-%s-%d", nonce, index))
}

func credentialLockTargetIndex(journal credentialLockJournal, path string) int {
	for index, target := range journal.Targets {
		if target.Path == path {
			return index
		}
	}
	return -1
}

func validateCredentialLockDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("credential refresh lock path is not a private directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return errors.New("credential refresh lock directory has the wrong owner")
	}
	return nil
}
