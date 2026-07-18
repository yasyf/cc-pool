package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// ErrAlreadyCutOver means the database already uses the exact current schema.
var ErrAlreadyCutOver = errors.New("store is already cut over")

// CutoverResult reports the source rows retained by an offline cutover.
type CutoverResult struct {
	Accounts int
	Backup   string
}

var cutoverFailpoint func(string) error

const deployedLegacySchema = `
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE accounts (
  id               INTEGER PRIMARY KEY,
  config_dir       TEXT NOT NULL UNIQUE,
  keychain_service TEXT NOT NULL,
  keychain_account TEXT NOT NULL,
  label            TEXT NOT NULL DEFAULT '',
  overlay_kind     TEXT NOT NULL DEFAULT 'symlink',
  created_at       INTEGER NOT NULL
, account_uuid TEXT NOT NULL DEFAULT '');
CREATE TABLE pending_adds (
  id         INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL
);
CREATE TABLE usage_samples (
  account_id   INTEGER NOT NULL,
  ts           INTEGER NOT NULL,
  util_5h      REAL,
  util_7d      REAL,
  resets_5h    INTEGER,
  resets_7d    INTEGER,
  rate_limited INTEGER NOT NULL DEFAULT 0, extra_enabled INTEGER NOT NULL DEFAULT 0, extra_used REAL NOT NULL DEFAULT 0, extra_limit REAL NOT NULL DEFAULT 0, scoped_7d_util REAL, scoped_7d_resets INTEGER, scoped_7d_model TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (account_id, ts)
);
CREATE INDEX idx_usage_acct_ts ON usage_samples(account_id, ts DESC);
CREATE TABLE sessions (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id INTEGER NOT NULL,
  pid        INTEGER,
  config_dir TEXT,
  started_at INTEGER NOT NULL,
  ended_at   INTEGER
, cwd TEXT NOT NULL DEFAULT '', last_seen_at INTEGER);
CREATE INDEX idx_sessions_active ON sessions(account_id) WHERE ended_at IS NULL;
CREATE INDEX idx_sessions_cwd ON sessions(cwd, ended_at);
CREATE TABLE refresh_log (
  account_id INTEGER NOT NULL,
  ts         INTEGER NOT NULL,
  ok         INTEGER NOT NULL,
  err        TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (account_id, ts)
);
CREATE TABLE sticky (
  cwd         TEXT PRIMARY KEY,
  account_id  INTEGER NOT NULL,
  selected_at INTEGER NOT NULL
, manual INTEGER NOT NULL DEFAULT 0);
CREATE TABLE auth_health (
  account_id  INTEGER PRIMARY KEY,
  needs_login INTEGER NOT NULL DEFAULT 0,
  since       INTEGER,
  last_err    TEXT NOT NULL DEFAULT ''
, kind TEXT NOT NULL DEFAULT '', gen INTEGER NOT NULL DEFAULT 0);
CREATE TABLE journal_risks (
  dir         TEXT PRIMARY KEY,
  warning     TEXT NOT NULL DEFAULT '',
  recorded_at INTEGER NOT NULL
);
CREATE TABLE overlay_applied (
  account_id      INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
  backend         TEXT NOT NULL,
  canonical_stamp TEXT NOT NULL,
  settings_stamp  TEXT NOT NULL,
  structure_stamp TEXT NOT NULL,
  app_stamp       TEXT NOT NULL,
  applied_at      INTEGER NOT NULL
);
CREATE INDEX idx_accounts_uuid ON accounts(account_uuid);
`

var (
	legacySchemaOnce sync.Once
	legacySchemaHash string
	legacySchemaErr  error
)

// Cutover rewrites a stopped v1 database into a fresh exact v2 database. It
// preserves source account/config/usage/auth state, assigns every account a new
// immutable instance id at generation 1, and deliberately drops process/session
// state whose kernel identities cannot survive restart.
func Cutover(path, backup string) (result CutoverResult, err error) {
	if path == "" {
		return result, errors.New("cut over store: database path is required")
	}
	if backup == "" {
		backup = path + ".pre-v2"
	}
	displayBackup := backup
	info, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("inspect cutover source: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return result, fmt.Errorf("cut over store: source path is a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return result, fmt.Errorf("cut over store: source is not a regular file: %s", path)
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return result, err
	}
	backupAbs, err := filepath.Abs(backup)
	if err != nil {
		return result, err
	}
	if pathAbs == backupAbs {
		return result, errors.New("cut over store: backup path must differ from the database path")
	}
	path, err = filepath.EvalSymlinks(pathAbs)
	if err != nil {
		return result, fmt.Errorf("resolve cutover source: %w", err)
	}
	backupParent, err := filepath.EvalSymlinks(filepath.Dir(backupAbs))
	if err != nil {
		return result, fmt.Errorf("resolve cutover backup parent: %w", err)
	}
	backup = filepath.Join(backupParent, filepath.Base(backupAbs))
	lifecycleLock, err := proc.FileLockSpec{
		Path: path + ".lifecycle.lock", Mode: proc.FileLockExclusive, Deadline: time.Second,
	}.TryAcquire()
	if err != nil {
		return result, fmt.Errorf("cut over store lifecycle: %w", err)
	}
	defer func() {
		if closeErr := lifecycleLock.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	source, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		return result, err
	}
	var version int
	if err := source.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		_ = source.Close()
		return result, err
	}
	if version == SchemaVersion {
		if err := verifySchema(source); err == nil {
			_ = source.Close()
			return result, ErrAlreadyCutOver
		}
	}
	if version != 0 {
		_ = source.Close()
		return result, fmt.Errorf("%w: offline source version %d is not the v1 cutover input", ErrSchemaMismatch, version)
	}
	if err := verifyLegacySchema(source); err != nil {
		_ = source.Close()
		return result, err
	}
	if _, err := source.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		_ = source.Close()
		return result, fmt.Errorf("checkpoint source store: %w", err)
	}
	if err := source.Close(); err != nil {
		return result, err
	}
	sourceInfo, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("restat cutover source: %w", err)
	}
	backupExists := false
	if backupInfo, statErr := os.Lstat(backup); statErr == nil {
		if backupInfo.Mode()&os.ModeSymlink != 0 {
			return result, fmt.Errorf("cut over store: backup path is a symlink: %s", backup)
		}
		if !os.SameFile(sourceInfo, backupInfo) {
			return result, fmt.Errorf("cut over store: backup exists but is not the pre-install hard link: %s", backup)
		}
		backupExists = true
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return result, fmt.Errorf("inspect cutover backup: %w", statErr)
	}

	tmp := path + ".v2-new"
	if tmpInfo, err := os.Lstat(tmp); err == nil {
		if tmpInfo.Mode()&os.ModeSymlink != 0 {
			return result, fmt.Errorf("cut over store: staging path is a symlink: %s", tmp)
		}
		if err := removeExactStaging(tmp); err != nil {
			return result, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
			_ = os.Remove(tmp + "-wal")
			_ = os.Remove(tmp + "-shm")
		}
	}()
	dest, err := Open(tmp)
	if err != nil {
		return result, err
	}
	if _, err = dest.db.Exec(`ATTACH DATABASE ? AS legacy`, path); err != nil {
		_ = dest.Close()
		return result, fmt.Errorf("attach source store: %w", err)
	}
	tx, err := dest.db.Begin()
	if err != nil {
		_ = dest.Close()
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(`SELECT id,config_dir,keychain_service,keychain_account,label,overlay_kind,account_uuid,created_at FROM legacy.accounts ORDER BY id`)
	if err != nil {
		_ = dest.Close()
		return result, err
	}
	for rows.Next() {
		var a Account
		var created int64
		if err = rows.Scan(&a.ID, &a.ConfigDir, &a.KeychainService, &a.KeychainAccount, &a.Label, &a.OverlayKind, &a.AccountUUID, &created); err != nil {
			_ = rows.Close()
			_ = dest.Close()
			return result, err
		}
		a.CreatedAt = time.Unix(created, 0)
		if err = upsertAccount(tx, a); err != nil {
			_ = rows.Close()
			_ = dest.Close()
			return result, err
		}
		result.Accounts++
	}
	if err = rows.Close(); err != nil {
		_ = dest.Close()
		return result, err
	}
	for _, copySQL := range []string{
		`INSERT INTO meta SELECT key,value FROM legacy.meta WHERE key IN ('initialized','overlay_kind','sync_enabled')`,
		`INSERT INTO usage_samples SELECT * FROM legacy.usage_samples`,
		`INSERT INTO refresh_log SELECT * FROM legacy.refresh_log`,
		`INSERT INTO sticky SELECT * FROM legacy.sticky`,
		`INSERT INTO auth_health SELECT * FROM legacy.auth_health`,
	} {
		if _, err = tx.Exec(copySQL); err != nil {
			_ = dest.Close()
			return result, fmt.Errorf("copy source store: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		_ = dest.Close()
		return result, err
	}
	if _, err = dest.db.Exec(`DETACH DATABASE legacy`); err != nil {
		_ = dest.Close()
		return result, err
	}
	if err = dest.Close(); err != nil {
		return result, err
	}
	check, err := Open(tmp)
	if err != nil {
		return result, fmt.Errorf("verify cutover store: %w", err)
	}
	if err = check.Close(); err != nil {
		return result, err
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm", tmp + "-wal", tmp + "-shm"} {
		if _, statErr := os.Stat(sidecar); statErr == nil {
			return result, fmt.Errorf("cut over store: live sqlite sidecar remains: %s", sidecar)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return result, statErr
		}
	}
	createdBackup := false
	installed := false
	defer func() {
		if err == nil || installed || !createdBackup {
			return
		}
		sourceInfo, sourceErr := os.Stat(path)
		backupInfo, backupErr := os.Stat(backup)
		if sourceErr != nil || backupErr != nil || !os.SameFile(sourceInfo, backupInfo) {
			err = errors.Join(err, errors.New("preserved pre-install backup because the source path no longer names the same database"))
			return
		}
		if removeErr := os.Remove(backup); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove pre-install backup: %w", removeErr))
			return
		}
		if syncErr := syncDirectory(filepath.Dir(backup)); syncErr != nil {
			err = errors.Join(err, fmt.Errorf("sync removed pre-install backup: %w", syncErr))
		}
	}()
	if !backupExists {
		if err = os.Link(path, backup); err != nil {
			return result, fmt.Errorf("create same-filesystem source backup: %w", err)
		}
		createdBackup = true
		if err = syncDirectory(filepath.Dir(backup)); err != nil {
			return result, fmt.Errorf("sync source backup: %w", err)
		}
	} else if err = syncDirectory(filepath.Dir(backup)); err != nil {
		return result, fmt.Errorf("sync resumed source backup: %w", err)
	}
	if cutoverFailpoint != nil {
		if err = cutoverFailpoint("after-backup"); err != nil {
			return result, err
		}
	}
	if err = os.Rename(tmp, path); err != nil {
		return result, fmt.Errorf("atomically install cutover store: %w", err)
	}
	installed = true
	if cutoverFailpoint != nil {
		if err = cutoverFailpoint("after-install"); err != nil {
			return result, err
		}
	}
	if err = syncDirectory(filepath.Dir(path)); err != nil {
		return result, fmt.Errorf("sync installed store: %w", err)
	}
	result.Backup = displayBackup
	return result, nil
}

func removeExactStaging(path string) error {
	check, err := Open(path)
	if err != nil {
		return fmt.Errorf("cut over store: staging database is not exact v2: %w", err)
	}
	if err := check.Close(); err != nil {
		return err
	}
	for _, sidecar := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Stat(sidecar); err == nil {
			return fmt.Errorf("cut over store: staging database has a live sqlite sidecar: %s", sidecar)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove exact v2 staging database: %w", err)
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	// path is a resolved database or backup parent selected by the cutover caller.
	//nolint:gosec // Opening that exact directory is required to durably sync the rename.
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	return errors.Join(err, closeErr)
}

func verifyLegacySchema(db *sql.DB) error {
	want, err := exactLegacySchemaHash()
	if err != nil {
		return err
	}
	got, err := schemaHash(db)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: v1 schema fingerprint %s, want %s", ErrSchemaMismatch, got, want)
	}
	return nil
}

func exactLegacySchemaHash() (string, error) {
	legacySchemaOnce.Do(func() {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			legacySchemaErr = err
			return
		}
		defer func() { _ = db.Close() }()
		if _, err := db.Exec(deployedLegacySchema); err != nil {
			legacySchemaErr = fmt.Errorf("build expected v1 store schema: %w", err)
			return
		}
		legacySchemaHash, legacySchemaErr = schemaHash(db)
	})
	return legacySchemaHash, legacySchemaErr
}
