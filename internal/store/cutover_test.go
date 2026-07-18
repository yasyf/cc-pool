package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

const freshLegacySchema = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY,config_dir TEXT NOT NULL UNIQUE,keychain_service TEXT NOT NULL,
  keychain_account TEXT NOT NULL,label TEXT NOT NULL DEFAULT '',overlay_kind TEXT NOT NULL DEFAULT 'symlink',
  account_uuid TEXT NOT NULL DEFAULT '',created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_adds (id INTEGER PRIMARY KEY,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS usage_samples (
  account_id INTEGER NOT NULL,ts INTEGER NOT NULL,util_5h REAL,util_7d REAL,resets_5h INTEGER,resets_7d INTEGER,
  rate_limited INTEGER NOT NULL DEFAULT 0,extra_enabled INTEGER NOT NULL DEFAULT 0,extra_used REAL NOT NULL DEFAULT 0,
  extra_limit REAL NOT NULL DEFAULT 0,scoped_7d_util REAL,scoped_7d_resets INTEGER,scoped_7d_model TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (account_id,ts)
);
CREATE INDEX IF NOT EXISTS idx_usage_acct_ts ON usage_samples(account_id,ts DESC);
CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,account_id INTEGER NOT NULL,pid INTEGER,config_dir TEXT,cwd TEXT NOT NULL DEFAULT '',
  started_at INTEGER NOT NULL,last_seen_at INTEGER,ended_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(account_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd,ended_at);
CREATE TABLE IF NOT EXISTS refresh_log (
  account_id INTEGER NOT NULL,ts INTEGER NOT NULL,ok INTEGER NOT NULL,err TEXT NOT NULL DEFAULT '',PRIMARY KEY (account_id,ts)
);
CREATE TABLE IF NOT EXISTS sticky (cwd TEXT PRIMARY KEY,account_id INTEGER NOT NULL,selected_at INTEGER NOT NULL,manual INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS auth_health (
  account_id INTEGER PRIMARY KEY,needs_login INTEGER NOT NULL DEFAULT 0,since INTEGER,last_err TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT '',gen INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS journal_risks (dir TEXT PRIMARY KEY,warning TEXT NOT NULL DEFAULT '',recorded_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS overlay_applied (
  account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,backend TEXT NOT NULL,canonical_stamp TEXT NOT NULL,
  settings_stamp TEXT NOT NULL,structure_stamp TEXT NOT NULL,app_stamp TEXT NOT NULL,applied_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_accounts_uuid ON accounts(account_uuid);
`

func createLegacyStore(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(deployedLegacySchema); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func tableCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil { //nolint:gosec // table is a test constant.
		t.Fatal(err)
	}
	return count
}

func requireDeployedLegacyStore(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := verifyLegacySchema(db); err != nil {
		t.Fatalf("%s is not the exact deployed v1 store: %v", path, err)
	}
}

func TestCutoverPreservesSourceStateAndDropsDerivedState(t *testing.T) {
	const deployedFingerprint = "292f9c86c80eac75461b9ce8adf46490dad3191d8503e5c543b0c7424435e953"
	if got, err := exactLegacySchemaHash(); err != nil || got != deployedFingerprint {
		t.Fatalf("deployed schema fixture fingerprint = %q, err=%v; want %s", got, err, deployedFingerprint)
	}
	path := filepath.Join(t.TempDir(), "pool.db")
	backup := path + ".v1"
	db := createLegacyStore(t, path)
	for _, statement := range []string{
		`INSERT INTO meta VALUES('initialized','1')`,
		`INSERT INTO meta VALUES('overlay_kind','fileprovider')`,
		`INSERT INTO meta VALUES('sync_enabled','1')`,
		`INSERT INTO meta VALUES('schema_version','1')`,
		`INSERT INTO accounts VALUES(7,'/exact/../acct-07','svc:exact','account:exact','label','fileprovider',1700000000,'uuid-7')`,
		`INSERT INTO usage_samples(account_id,ts,util_5h,util_7d) VALUES(7,1700000010,12.5,44.5)`,
		`INSERT INTO refresh_log VALUES(7,1700000020,0,'network')`,
		`INSERT INTO sticky VALUES('/project',7,1700000030,1)`,
		`INSERT INTO auth_health VALUES(7,1,1700000040,'expired','credential',9)`,
		`INSERT INTO pending_adds VALUES(8,1700000050)`,
		`INSERT INTO sessions(account_id,pid,config_dir,cwd,started_at) VALUES(7,4242,'/exact/../acct-07','/project',1700000060)`,
		`INSERT INTO journal_risks VALUES('/exact/../acct-07','warning',1700000070)`,
		`INSERT INTO overlay_applied VALUES(7,'fileprovider','c','s','t','a',1700000080)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := Cutover(path, backup)
	if err != nil {
		t.Fatal(err)
	}
	if result.Accounts != 1 || result.Backup != backup {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	requireDeployedLegacyStore(t, backup)
	pathInfo, _ := os.Stat(path)
	backupInfo, _ := os.Stat(backup)
	if os.SameFile(pathInfo, backupInfo) {
		t.Fatal("installed database still names the archived v1 inode")
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	a, err := s.GetAccount(7)
	if err != nil {
		t.Fatal(err)
	}
	if a.ConfigDir != "/exact/../acct-07" || a.KeychainService != "svc:exact" || a.KeychainAccount != "account:exact" {
		t.Fatalf("account strings changed: %+v", a)
	}
	if len(a.InstanceID) != 32 || a.Generation != 1 {
		t.Fatalf("new account identity = %q/%d", a.InstanceID, a.Generation)
	}
	for _, table := range []string{"accounts", "usage_samples", "refresh_log", "sticky", "auth_health"} {
		if got := tableCount(t, s, table); got != 1 {
			t.Errorf("preserved %s rows = %d, want 1", table, got)
		}
	}
	if got := tableCount(t, s, "meta"); got != 3 {
		t.Errorf("preserved meta rows = %d, want 3", got)
	}
	for key, want := range map[string]string{"initialized": "1", "overlay_kind": "fileprovider", "sync_enabled": "1"} {
		var got string
		if err := s.db.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&got); err != nil {
			t.Fatalf("preserved meta %q: %v", key, err)
		}
		if got != want {
			t.Errorf("preserved meta %q = %q, want %q", key, got, want)
		}
	}
	var obsolete int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM meta WHERE key='schema_version'`).Scan(&obsolete); err != nil {
		t.Fatal(err)
	}
	if obsolete != 0 {
		t.Fatal("obsolete schema_version meta row survived cutover")
	}
	for _, table := range []string{"pending_adds", "sessions", "journal_risks", "overlay_applied"} {
		if got := tableCount(t, s, table); got != 0 {
			t.Errorf("derived %s rows = %d, want 0", table, got)
		}
	}
}

func TestCutoverRejectsUnexpectedLegacySchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	db := createLegacyStore(t, path)
	if _, err := db.Exec(`ALTER TABLE accounts ADD COLUMN tolerated_cruft TEXT`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Cutover(path, path+".backup")
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Cutover unexpected v1 schema = %v, want ErrSchemaMismatch", err)
	}
	if _, statErr := os.Stat(path + ".backup"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unexpected backup after refusal: %v", statErr)
	}
}

func TestCutoverRejectsFreshLegacySchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(freshLegacySchema); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Cutover(path, path+".backup")
	if !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Cutover fresh v1 schema = %v, want ErrSchemaMismatch", err)
	}
}

func TestCutoverPreInstallFailureKeepsOldPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	backup := path + ".backup"
	db := createLegacyStore(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	want := errors.New("stop before install")
	cutoverFailpoint = func(stage string) error {
		if stage == "after-backup" {
			return want
		}
		return nil
	}
	t.Cleanup(func() { cutoverFailpoint = nil })

	if _, err := Cutover(path, backup); !errors.Is(err, want) {
		t.Fatalf("Cutover failpoint = %v, want %v", err, want)
	}
	requireDeployedLegacyStore(t, path)
	if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new pre-install backup was not cleaned up: %v", err)
	}
}

func TestCutoverPostInstallFailureKeepsNewPathAndOldBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	backup := path + ".backup"
	db := createLegacyStore(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	want := errors.New("stop after install")
	cutoverFailpoint = func(stage string) error {
		if stage == "after-install" {
			return want
		}
		return nil
	}
	t.Cleanup(func() { cutoverFailpoint = nil })

	if _, err := Cutover(path, backup); !errors.Is(err, want) {
		t.Fatalf("Cutover failpoint = %v, want %v", err, want)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatalf("installed v2 path is not exact: %v", err)
	}
	_ = s.Close()
	requireDeployedLegacyStore(t, backup)
	if _, err := Cutover(path, backup); !errors.Is(err, ErrAlreadyCutOver) {
		t.Fatalf("rerun after installed cutover = %v, want ErrAlreadyCutOver", err)
	}
}

func TestCutoverResumesBackupCreatedPreInstallState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	backup := path + ".backup"
	db := createLegacyStore(t, path)
	if _, err := db.Exec(`INSERT INTO accounts VALUES(7,'/acct-07','svc','acct','label','symlink',1700000000,'uuid')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, backup); err != nil {
		t.Fatal(err)
	}
	staging, err := Open(path + ".v2-new")
	if err != nil {
		t.Fatal(err)
	}
	if err := staging.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Cutover(path, backup); err != nil {
		t.Fatalf("resume Cutover = %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if accounts, err := s.ListAccounts(); err != nil || len(accounts) != 1 || accounts[0].ConfigDir != "/acct-07" {
		t.Fatalf("resumed accounts = %+v, err=%v", accounts, err)
	}
	requireDeployedLegacyStore(t, backup)
}

func TestCutoverRejectsUnrelatedExistingBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	backup := path + ".backup"
	db := createLegacyStore(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(backup, []byte("not the source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Cutover(path, backup); err == nil || !strings.Contains(err.Error(), "not the pre-install hard link") {
		t.Fatalf("Cutover unrelated backup = %v", err)
	}
	requireDeployedLegacyStore(t, path)
}

func TestCutoverRejectsSymlinkSource(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.db")
	db := createLegacyStore(t, realPath)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "pool.db")
	if err := os.Symlink(realPath, path); err != nil {
		t.Fatal(err)
	}
	if _, err := Cutover(path, path+".backup"); err == nil || !strings.Contains(err.Error(), "source path is a symlink") {
		t.Fatalf("Cutover symlink source = %v", err)
	}
	requireDeployedLegacyStore(t, realPath)
}

func TestCutoverRejectsSymlinkBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	db := createLegacyStore(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	backup := path + ".backup"
	if err := os.Symlink(path, backup); err != nil {
		t.Fatal(err)
	}
	if _, err := Cutover(path, backup); err == nil || !strings.Contains(err.Error(), "backup path is a symlink") {
		t.Fatalf("Cutover symlink backup = %v", err)
	}
	requireDeployedLegacyStore(t, path)
}

func TestCutoverRefusesEveryLiveStoreAliasUntilAllClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(path), "pool-alias.db")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	second, err := Open(alias)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Cutover(path, path+".backup"); !errors.Is(err, proc.ErrLockBusy) {
		t.Fatalf("Cutover with two aliased live stores = %v, want ErrLockBusy", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Cutover(path, path+".backup"); !errors.Is(err, proc.ErrLockBusy) {
		t.Fatalf("Cutover with aliased live store = %v, want ErrLockBusy", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Cutover(path, path+".backup"); !errors.Is(err, ErrAlreadyCutOver) {
		t.Fatalf("Cutover after every store closed = %v, want ErrAlreadyCutOver", err)
	}
}
