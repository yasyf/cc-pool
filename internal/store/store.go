// Package store is cc-pool's sole state layer, a pure-Go (modernc.org/sqlite)
// database. It stores NO secrets — the Keychain is the only secret store.
package store

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
	_ "modernc.org/sqlite" // pure-Go "sqlite" driver
)

// Store wraps the sqlite connection.
type Store struct {
	db            *sql.DB
	lifecycleLock *proc.FileLockHandle
	now           func() time.Time
}

const schema = `
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE accounts (
  id               INTEGER PRIMARY KEY CHECK(id > 0),
  instance_id      TEXT NOT NULL UNIQUE CHECK(length(instance_id) = 32 AND instance_id NOT GLOB '*[^0-9a-f]*'),
  generation       INTEGER NOT NULL CHECK(generation > 0),
  config_dir       TEXT NOT NULL UNIQUE CHECK(config_dir <> ''),
  keychain_service TEXT NOT NULL CHECK(keychain_service <> ''),
  keychain_account TEXT NOT NULL CHECK(keychain_account <> ''),
  label            TEXT NOT NULL DEFAULT '',
  overlay_kind     TEXT NOT NULL DEFAULT 'symlink',
  account_uuid     TEXT NOT NULL DEFAULT '',
  created_at       INTEGER NOT NULL
);
CREATE TABLE pending_adds (
  id         INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL
);
CREATE TABLE usage_samples (
  account_id    INTEGER NOT NULL,
  ts            INTEGER NOT NULL,
  util_5h       REAL,
  util_7d       REAL,
  resets_5h     INTEGER,
  resets_7d     INTEGER,
  rate_limited  INTEGER NOT NULL DEFAULT 0,
  extra_enabled INTEGER NOT NULL DEFAULT 0,
  extra_used    REAL NOT NULL DEFAULT 0,
  extra_limit   REAL NOT NULL DEFAULT 0,
  scoped_7d_util   REAL,
  scoped_7d_resets INTEGER,
  scoped_7d_model  TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (account_id, ts)
);
CREATE INDEX idx_usage_acct_ts ON usage_samples(account_id, ts DESC);
CREATE TABLE sessions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id   INTEGER NOT NULL CHECK(account_id > 0),
  account_instance_id TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation INTEGER NOT NULL CHECK(account_generation > 0),
  pid          INTEGER NOT NULL CHECK(pid > 0),
  process_started_at INTEGER NOT NULL CHECK(process_started_at > 0),
  config_dir   TEXT NOT NULL CHECK(config_dir <> ''),
  cwd          TEXT NOT NULL DEFAULT '',
  started_at   INTEGER NOT NULL CHECK(started_at > 0),
  last_seen_at INTEGER,
  ended_at     INTEGER
);
CREATE INDEX idx_sessions_active ON sessions(account_id) WHERE ended_at IS NULL;
CREATE INDEX idx_sessions_cwd ON sessions(cwd, ended_at);
CREATE TABLE selection_terminals (
  token               TEXT PRIMARY KEY CHECK(length(token) = 32 AND token NOT GLOB '*[^0-9a-f]*'),
  account_id          INTEGER NOT NULL CHECK(account_id > 0),
  account_instance_id TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation  INTEGER NOT NULL CHECK(account_generation > 0),
  committed_at        INTEGER NOT NULL CHECK(committed_at > 0),
  expires_at          INTEGER NOT NULL CHECK(expires_at > committed_at)
);
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
  selected_at INTEGER NOT NULL,
  manual      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE auth_health (
  account_id  INTEGER PRIMARY KEY,
  needs_login INTEGER NOT NULL DEFAULT 0,
  since       INTEGER,
  last_err    TEXT NOT NULL DEFAULT '',
  kind        TEXT NOT NULL DEFAULT '',
  gen         INTEGER NOT NULL DEFAULT 0
);
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

// SchemaVersion is the only runtime schema accepted by this binary.
const SchemaVersion = 2

// ErrSchemaMismatch means the database must be cut over explicitly while the
// service is stopped. Open never mutates an existing schema.
var ErrSchemaMismatch = errors.New("store schema mismatch")

const (
	selectionTerminalTTL   = 10 * time.Minute
	selectionTerminalLimit = 4096
)

var (
	expectedSchemaOnce sync.Once
	expectedSchemaHash string
	expectedSchemaErr  error
)

const upsertStickySQL = `INSERT INTO sticky(cwd,account_id,selected_at,manual) VALUES(?,?,?,0)
 ON CONFLICT(cwd) DO UPDATE SET
   account_id=excluded.account_id,
   selected_at=excluded.selected_at
 WHERE manual = 0 OR account_id = excluded.account_id`

// Open opens path. It creates the current schema only for a completely empty
// database; every existing database must match the exact current schema.
func Open(path string) (*Store, error) {
	path, err := canonicalDatabasePath(path)
	if err != nil {
		return nil, err
	}
	lifecycleLock, err := proc.FileLockSpec{
		Path: path + ".lifecycle.lock", Mode: proc.FileLockShared, Deadline: time.Second,
	}.TryAcquire()
	if err != nil {
		return nil, fmt.Errorf("open store lifecycle: %w", err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)")
	if err != nil {
		_ = lifecycleLock.Close()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize writes
	s := &Store{db: db, lifecycleLock: lifecycleLock, now: time.Now}
	if err := s.initializeOrVerifySchema(); err != nil {
		_ = db.Close()
		_ = lifecycleLock.Close()
		return nil, err
	}
	if err := requireSingleLinkDatabase(path); err != nil {
		_ = db.Close()
		_ = lifecycleLock.Close()
		return nil, err
	}
	return s, nil
}

func canonicalDatabasePath(path string) (string, error) {
	if path == "" {
		return "", errors.New("open store: database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("resolve store path: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", fmt.Errorf("resolve store parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(abs)), nil
}

func requireSingleLinkDatabase(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect store database: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Nlink != 1 {
		return fmt.Errorf("open store: database must be one regular single-link file: %s", path)
	}
	return nil
}

func (s *Store) initializeOrVerifySchema() error {
	var objects, version int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL`).Scan(&objects); err != nil {
		return fmt.Errorf("inspect store schema: %w", err)
	}
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("inspect store schema version: %w", err)
	}
	if objects == 0 {
		if version != 0 {
			return fmt.Errorf("%w: empty database has version %d", ErrSchemaMismatch, version)
		}
		if _, err := s.db.Exec(schema); err != nil {
			return fmt.Errorf("create store schema: %w", err)
		}
		if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, SchemaVersion)); err != nil {
			return fmt.Errorf("stamp store schema: %w", err)
		}
	}
	return verifySchema(s.db)
}

func verifySchema(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read store schema version: %w", err)
	}
	if version != SchemaVersion {
		return fmt.Errorf("%w: database=%d binary=%d; stop cc-pool and run `ccp store-cutover`", ErrSchemaMismatch, version, SchemaVersion)
	}
	want, err := exactSchemaHash()
	if err != nil {
		return err
	}
	got, err := schemaHash(db)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: schema fingerprint %s, want %s", ErrSchemaMismatch, got, want)
	}
	return nil
}

func exactSchemaHash() (string, error) {
	expectedSchemaOnce.Do(func() {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			expectedSchemaErr = err
			return
		}
		defer func() { _ = db.Close() }()
		if _, err := db.Exec(schema); err != nil {
			expectedSchemaErr = fmt.Errorf("build expected store schema: %w", err)
			return
		}
		expectedSchemaHash, expectedSchemaErr = schemaHash(db)
	})
	return expectedSchemaHash, expectedSchemaErr
}

func schemaHash(db *sql.DB) (string, error) {
	rows, err := db.Query(`SELECT type,name,tbl_name,sql FROM sqlite_schema
		WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL ORDER BY type,name`)
	if err != nil {
		return "", fmt.Errorf("read store schema: %w", err)
	}
	defer func() { _ = rows.Close() }()
	h := sha256.New()
	for rows.Next() {
		var kind, name, table, statement string
		if err := rows.Scan(&kind, &name, &table, &statement); err != nil {
			return "", fmt.Errorf("scan store schema: %w", err)
		}
		for _, field := range []string{kind, name, table, statement} {
			_, _ = h.Write([]byte(field))
			_, _ = h.Write([]byte{0})
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// GetMeta returns the meta value for key, ok=false if absent.
func (s *Store) GetMeta(key string) (string, bool, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get meta %q: %w", key, err)
	}
	return v, true, nil
}

// SetMeta upserts a meta key.
func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO meta(key,value) VALUES(?,?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	if err != nil {
		return fmt.Errorf("set meta %q: %w", key, err)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error {
	dbErr := s.db.Close()
	lockErr := s.lifecycleLock.Close()
	return errors.Join(dbErr, lockErr)
}

// rowExecer is the write subset shared by *sql.DB and *sql.Tx, so an account
// upsert composes into a caller's transaction (see PromoteReservedAccount).
type rowExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// UpsertAccount inserts or replaces an account row by id; account_uuid is
// insert-only so a re-upsert can't wipe a backfilled value.
func (s *Store) UpsertAccount(a Account) error {
	return upsertAccount(s.db, a)
}

func upsertAccount(e rowExecer, a Account) error {
	instanceID, err := NewAccountInstanceID()
	if err != nil {
		return err
	}
	created := a.CreatedAt
	if created.IsZero() {
		created = time.Now()
	}
	_, err = e.Exec(
		`INSERT INTO accounts(id,instance_id,generation,config_dir,keychain_service,keychain_account,label,overlay_kind,account_uuid,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		   config_dir=excluded.config_dir,
		   keychain_service=excluded.keychain_service,
		   keychain_account=excluded.keychain_account,
		   label=excluded.label,
		   generation=accounts.generation + CASE
		     WHEN accounts.config_dir <> excluded.config_dir OR accounts.overlay_kind <> excluded.overlay_kind THEN 1
		     ELSE 0 END,
		   overlay_kind=excluded.overlay_kind`,
		a.ID, instanceID, 1, a.ConfigDir, a.KeychainService, a.KeychainAccount, a.Label, a.OverlayKind, a.AccountUUID, created.Unix())
	if err != nil {
		return fmt.Errorf("upsert account %d: %w", a.ID, err)
	}
	return nil
}

// NewAccountInstanceID returns a random immutable 128-bit account instance id.
func NewAccountInstanceID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate account instance id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// SetAccountLabel updates an account's label.
func (s *Store) SetAccountLabel(id int, label string) error {
	res, err := s.db.Exec(`UPDATE accounts SET label=? WHERE id=?`, label, id)
	if err != nil {
		return fmt.Errorf("set label for account %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("account %d not found", id)
	}
	return nil
}

// SetAccountOverlayKind records an account's overlay provider; a targeted
// UPDATE so it can't clobber concurrent updates to the row's other columns.
func (s *Store) SetAccountOverlayKind(id int, kind string) error {
	res, err := s.db.Exec(`UPDATE accounts SET overlay_kind=?, generation=generation+1 WHERE id=? AND overlay_kind<>?`, kind, id, kind)
	if err != nil {
		return fmt.Errorf("set overlay kind for account %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		if _, err := s.GetAccount(id); err != nil {
			return err
		}
	}
	return nil
}

func scanAccount(rows interface{ Scan(...any) error }) (Account, error) {
	var a Account
	var created int64
	if err := rows.Scan(&a.ID, &a.InstanceID, &a.Generation, &a.ConfigDir, &a.KeychainService, &a.KeychainAccount,
		&a.Label, &a.OverlayKind, &a.AccountUUID, &created); err != nil {
		return a, err
	}
	a.CreatedAt = time.Unix(created, 0)
	return a, nil
}

const accountCols = `id,instance_id,generation,config_dir,keychain_service,keychain_account,label,overlay_kind,account_uuid,created_at`

// ListAccounts returns all accounts ordered by id.
func (s *Store) ListAccounts() ([]Account, error) {
	rows, err := s.db.Query(`SELECT ` + accountCols + ` FROM accounts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ErrAccountNotFound is returned by GetAccount when no row matches the id, so
// callers can distinguish a removed account from a real query failure.
var ErrAccountNotFound = errors.New("account not found")

// GetAccount returns one account by id, wrapping ErrAccountNotFound when the
// row is absent.
func (s *Store) GetAccount(id int) (Account, error) {
	row := s.db.QueryRow(`SELECT `+accountCols+` FROM accounts WHERE id=?`, id)
	a, err := scanAccount(row)
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("account %d: %w", id, ErrAccountNotFound)
	}
	return a, err
}

// SetAccountUUID records an account's Claude accountUuid; a targeted UPDATE so
// it can't clobber concurrent updates to the row's other columns.
func (s *Store) SetAccountUUID(id int, uuid string) error {
	res, err := s.db.Exec(`UPDATE accounts SET account_uuid=? WHERE id=?`, uuid, id)
	if err != nil {
		return fmt.Errorf("set account_uuid for account %d: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("account %d not found", id)
	}
	return nil
}

// GetAccountByUUID returns the account whose Claude accountUuid is uuid,
// ok=false if none. An empty uuid never matches (every un-backfilled row holds
// the empty-string default), and duplicate uuids resolve to the lowest id so
// repeated calls never flap.
func (s *Store) GetAccountByUUID(uuid string) (Account, bool, error) {
	if uuid == "" {
		return Account{}, false, nil
	}
	row := s.db.QueryRow(`SELECT `+accountCols+` FROM accounts WHERE account_uuid=? ORDER BY id LIMIT 1`, uuid)
	a, err := scanAccount(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, false, nil
	}
	if err != nil {
		return Account{}, false, fmt.Errorf("get account by uuid %q: %w", uuid, err)
	}
	return a, true, nil
}

// AccountsByUUID returns every account whose Claude accountUuid is uuid,
// ordered by id, so callers can refuse an ambiguous match; an empty uuid
// matches nothing.
func (s *Store) AccountsByUUID(uuid string) ([]Account, error) {
	if uuid == "" {
		return nil, nil
	}
	rows, err := s.db.Query(`SELECT `+accountCols+` FROM accounts WHERE account_uuid=? ORDER BY id`, uuid)
	if err != nil {
		return nil, fmt.Errorf("accounts by uuid %q: %w", uuid, err)
	}
	defer func() { _ = rows.Close() }()
	var out []Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, fmt.Errorf("accounts by uuid %q: %w", uuid, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteAccount removes an account and its dependent rows.
func (s *Store) DeleteAccount(id int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`DELETE FROM usage_samples WHERE account_id=?`,
		`DELETE FROM sessions WHERE account_id=?`,
		`DELETE FROM refresh_log WHERE account_id=?`,
		`DELETE FROM sticky WHERE account_id=?`,
		`DELETE FROM auth_health WHERE account_id=?`,
		`DELETE FROM accounts WHERE id=?`,
	} {
		if _, err := tx.Exec(q, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func tsOrNil(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Unix()
}

const usageSampleCols = `account_id,ts,util_5h,util_7d,resets_5h,resets_7d,rate_limited,extra_enabled,extra_used,extra_limit,scoped_7d_util,scoped_7d_resets,scoped_7d_model`

// InsertUsageSample records one usage poll.
func (s *Store) InsertUsageSample(u UsageSample) error {
	rl, xe := 0, 0
	if u.RateLimited {
		rl = 1
	}
	if u.ExtraEnabled {
		xe = 1
	}
	_, err := s.db.Exec(
		`INSERT INTO usage_samples(`+usageSampleCols+`)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(account_id,ts) DO NOTHING`,
		u.AccountID, u.TS.Unix(), u.Util5h, u.Util7d,
		tsOrNil(u.Resets5h), tsOrNil(u.Resets7d), rl, xe, u.ExtraUsed, u.ExtraLimit,
		u.Scoped7dUtil, tsOrNil(u.Scoped7dResets), u.Scoped7dModel)
	return err
}

func scanUsageSample(row interface{ Scan(...any) error }) (UsageSample, error) {
	var u UsageSample
	var ts int64
	var u5, u7, us sql.NullFloat64
	var r5, r7, rs sql.NullInt64
	var rl, xe int
	if err := row.Scan(&u.AccountID, &ts, &u5, &u7, &r5, &r7, &rl, &xe, &u.ExtraUsed, &u.ExtraLimit,
		&us, &rs, &u.Scoped7dModel); err != nil {
		return u, err
	}
	u.TS = time.Unix(ts, 0)
	u.Util5h, u.Util7d = u5.Float64, u7.Float64
	if r5.Valid {
		u.Resets5h = time.Unix(r5.Int64, 0)
	}
	if r7.Valid {
		u.Resets7d = time.Unix(r7.Int64, 0)
	}
	u.RateLimited = rl != 0
	u.ExtraEnabled = xe != 0
	u.Scoped7dUtil = us.Float64
	if rs.Valid {
		u.Scoped7dResets = time.Unix(rs.Int64, 0)
	}
	return u, nil
}

// LatestUsageSample returns the most recent sample for an account, or ok=false.
func (s *Store) LatestUsageSample(accountID int) (UsageSample, bool, error) {
	row := s.db.QueryRow(
		`SELECT `+usageSampleCols+`
		 FROM usage_samples WHERE account_id=? ORDER BY ts DESC LIMIT 1`, accountID)
	u, err := scanUsageSample(row)
	if errors.Is(err, sql.ErrNoRows) {
		return u, false, nil
	}
	if err != nil {
		return u, false, err
	}
	return u, true, nil
}

// LatestGoodUsageSample returns the most recent non-rate-limited sample for an
// account, or ok=false. A 429 stores a zeroed placeholder for the daemon's
// backoff; this reads through to the last real utilization instead.
func (s *Store) LatestGoodUsageSample(accountID int) (UsageSample, bool, error) {
	row := s.db.QueryRow(
		`SELECT `+usageSampleCols+`
		 FROM usage_samples WHERE account_id=? AND rate_limited=0 ORDER BY ts DESC LIMIT 1`, accountID)
	u, err := scanUsageSample(row)
	if errors.Is(err, sql.ErrNoRows) {
		return u, false, nil
	}
	if err != nil {
		return u, false, err
	}
	return u, true, nil
}

// UsageSamplesSince returns an account's samples at or after since, newest
// first. A time bound (not a row limit) keeps burn estimators from
// under-covering the window after a backoff gap.
func (s *Store) UsageSamplesSince(accountID int, since time.Time) ([]UsageSample, error) {
	rows, err := s.db.Query(
		`SELECT `+usageSampleCols+`
		 FROM usage_samples WHERE account_id=? AND ts>=? ORDER BY ts DESC`,
		accountID, since.Unix())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []UsageSample
	for rows.Next() {
		u, err := scanUsageSample(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ErrAccountGenerationChanged means a reserved account was replaced or its
// tenant-defining shape changed before activation.
var ErrAccountGenerationChanged = errors.New("account generation changed")

// ActivateSelection atomically verifies the reserved account identity and
// generation, then records sticky/session state. No filesystem or provider I/O
// occurs in this transaction.
func (s *Store) ActivateSelection(a SelectionActivation) (err error) {
	if err := validateSelectionToken(a.Token); err != nil {
		return fmt.Errorf("activate selection: %w", err)
	}
	if a.Process.PID <= 0 {
		return errors.New("activate selection: positive process pid is required")
	}
	if a.Process.StartedAt.IsZero() {
		return errors.New("activate selection: process start time is required")
	}
	if a.At.IsZero() {
		a.At = s.now()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin selection activation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	terminalNow := s.now()
	if err = pruneSelectionTerminals(tx, terminalNow); err != nil {
		return fmt.Errorf("prune selection terminals: %w", err)
	}
	var terminalAccountID int
	var terminalInstanceID string
	var terminalGeneration uint64
	err = tx.QueryRow(
		`SELECT account_id,account_instance_id,account_generation FROM selection_terminals WHERE token=?`, a.Token,
	).Scan(&terminalAccountID, &terminalInstanceID, &terminalGeneration)
	if err == nil {
		if terminalAccountID != a.AccountID || terminalInstanceID != a.ExpectedInstanceID || terminalGeneration != a.ExpectedGeneration {
			return fmt.Errorf("activate selection: token %s belongs to account %d %s/%d", a.Token,
				terminalAccountID, terminalInstanceID, terminalGeneration)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read selection terminal: %w", err)
	}
	var instanceID, configDir string
	var generation uint64
	if err = tx.QueryRow(`SELECT instance_id,generation,config_dir FROM accounts WHERE id=?`, a.AccountID).Scan(&instanceID, &generation, &configDir); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("activate selection account %d: %w", a.AccountID, ErrAccountNotFound)
		}
		return fmt.Errorf("activate selection account %d: %w", a.AccountID, err)
	}
	if instanceID != a.ExpectedInstanceID || generation != a.ExpectedGeneration {
		return fmt.Errorf("%w: account=%d reserved=%s/%d current=%s/%d", ErrAccountGenerationChanged,
			a.AccountID, a.ExpectedInstanceID, a.ExpectedGeneration, instanceID, generation)
	}
	if a.RecordSticky && a.Cwd != "" {
		if _, err = tx.Exec(upsertStickySQL, a.Cwd, a.AccountID, a.At.Unix()); err != nil {
			return fmt.Errorf("activate selection sticky for %s: %w", a.Cwd, err)
		}
	}
	if _, err = tx.Exec(
		`INSERT INTO sessions(account_id,account_instance_id,account_generation,pid,process_started_at,config_dir,cwd,started_at)
		 VALUES(?,?,?,?,?,?,?,?)`,
		a.AccountID, instanceID, generation, a.Process.PID, a.Process.StartedAt.UnixMicro(), configDir, a.Cwd, a.At.Unix()); err != nil {
		return fmt.Errorf("activate selection session for account %d: %w", a.AccountID, err)
	}
	if _, err = tx.Exec(
		`INSERT INTO selection_terminals(token,account_id,account_instance_id,account_generation,committed_at,expires_at) VALUES(?,?,?,?,?,?)`,
		a.Token, a.AccountID, instanceID, generation, terminalNow.Unix(), terminalNow.Add(selectionTerminalTTL).Unix()); err != nil {
		return fmt.Errorf("record selection terminal: %w", err)
	}
	if err = pruneSelectionTerminals(tx, terminalNow); err != nil {
		return fmt.Errorf("bound selection terminals: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit selection activation: %w", err)
	}
	return nil
}

// SelectionCommitted reports whether token's activation committed durably.
func (s *Store) SelectionCommitted(token string) (bool, error) {
	if err := validateSelectionToken(token); err != nil {
		return false, err
	}
	if err := pruneSelectionTerminals(s.db, s.now()); err != nil {
		return false, fmt.Errorf("prune selection terminals: %w", err)
	}
	var present int
	err := s.db.QueryRow(`SELECT 1 FROM selection_terminals WHERE token=?`, token).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

type selectionTerminalExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func pruneSelectionTerminals(exec selectionTerminalExecer, now time.Time) error {
	if _, err := exec.Exec(`DELETE FROM selection_terminals WHERE expires_at<=?`, now.Unix()); err != nil {
		return err
	}
	_, err := exec.Exec(`DELETE FROM selection_terminals WHERE rowid IN (
		SELECT rowid FROM selection_terminals
		ORDER BY committed_at DESC, rowid DESC
		LIMIT -1 OFFSET ?
	)`, selectionTerminalLimit)
	return err
}

func validateSelectionToken(token string) error {
	raw, err := hex.DecodeString(token)
	if err != nil || len(raw) != 16 {
		return errors.New("selection token must be exactly 16 bytes of lowercase hex")
	}
	if token != strings.ToLower(token) {
		return errors.New("selection token must be exactly 16 bytes of lowercase hex")
	}
	return nil
}

// CloseSession marks a session ended by id at time at.
func (s *Store) CloseSession(id int64, at time.Time) error {
	_, err := s.db.Exec(`UPDATE sessions SET ended_at=? WHERE id=? AND ended_at IS NULL`,
		at.Unix(), id)
	return err
}

// ActiveSessionCount returns the number of live sessions for an account.
func (s *Store) ActiveSessionCount(accountID int) (int, error) {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM sessions WHERE account_id=? AND ended_at IS NULL`, accountID).Scan(&n)
	return n, err
}

// ListActiveSessions returns all live sessions across accounts.
func (s *Store) ListActiveSessions() ([]Session, error) {
	rows, err := s.db.Query(
		`SELECT id,account_id,account_instance_id,account_generation,pid,process_started_at,config_dir,cwd,started_at,last_seen_at
		 FROM sessions WHERE ended_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Session
	for rows.Next() {
		var se Session
		var started int64
		var processStarted int64
		var seen sql.NullInt64
		if err := rows.Scan(&se.ID, &se.AccountID, &se.AccountInstanceID, &se.AccountGeneration,
			&se.PID, &processStarted, &se.ConfigDir, &se.Cwd, &started, &seen); err != nil {
			return nil, err
		}
		se.ProcessStartedAt = time.UnixMicro(processStarted)
		se.StartedAt = time.Unix(started, 0)
		if seen.Valid {
			t := time.Unix(seen.Int64, 0)
			se.LastSeenAt = &t
		}
		out = append(out, se)
	}
	return out, rows.Err()
}

// SessionReapGrace is how long a freshly opened session is immune to
// CloseDeadSessions: `ccp run` marks its checkout before exec'ing into claude,
// so briefly the pid is a ccp process no claude-only scan sees, and reaping it
// would fabricate a "session ended" signal for the sticky rules.
const SessionReapGrace = time.Minute

func (s *Store) touchSession(id int64, at time.Time) error {
	_, err := s.db.Exec(`UPDATE sessions SET last_seen_at=? WHERE id=? AND ended_at IS NULL`,
		at.Unix(), id)
	return err
}

// CloseDeadSessions reconciles active sessions against the live claude pids in
// alive at time at: live rows are stamped last-seen, dead rows older than
// SessionReapGrace are closed. A dead row's end is stamped at its last-seen (or
// start), never observation time — else a reap after a long observer gap
// fabricates a warm sticky cache from a session that died hours ago.
func (s *Store) CloseDeadSessions(alive map[int]time.Time, at time.Time) (int, error) {
	sessions, err := s.ListActiveSessions()
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, se := range sessions {
		if se.PID <= 0 {
			continue
		}
		if started, ok := alive[se.PID]; ok && started.Equal(se.ProcessStartedAt) {
			if err := s.touchSession(se.ID, at); err != nil {
				return closed, err
			}
			continue
		}
		if at.Sub(se.StartedAt) < SessionReapGrace {
			continue
		}
		end := se.StartedAt
		if se.LastSeenAt != nil && se.LastSeenAt.After(end) {
			end = *se.LastSeenAt
		}
		if err := s.CloseSession(se.ID, end); err != nil {
			return closed, err
		}
		closed++
	}
	return closed, nil
}

// GetCwdActivity aggregates tracked session activity for cwd on one account —
// the prompt cache a pin protects is per-account, so sessions on other accounts
// in the same directory don't count. Never ErrNoRows: an untracked cwd reads as
// the zero CwdActivity.
func (s *Store) GetCwdActivity(cwd string, accountID int) (CwdActivity, error) {
	var act CwdActivity
	var lastEnded int64
	err := s.db.QueryRow(
		`SELECT COALESCE(SUM(CASE WHEN ended_at IS NULL THEN 1 ELSE 0 END), 0),
		        COALESCE(MAX(ended_at), 0)
		 FROM sessions WHERE cwd = ? AND account_id = ?`, cwd, accountID).Scan(&act.Live, &lastEnded)
	if err != nil {
		return CwdActivity{}, fmt.Errorf("cwd activity for %s: %w", cwd, err)
	}
	if lastEnded > 0 {
		act.LastEnded = time.Unix(lastEnded, 0)
	}
	return act, nil
}

// UpsertSticky is the select-path write recording the account picked for cwd.
// It never downgrades or repoints a manual pin: a conflict repoints/refreshes
// an auto pin, refreshes a manual pin only when the select landed on the pinned
// account, and is a no-op when a manual pin points elsewhere. One atomic
// statement, since daemon activation and manual pin commands can race a
// read-modify-write.
func (s *Store) UpsertSticky(cwd string, accountID int, at time.Time) error {
	_, err := s.db.Exec(upsertStickySQL, cwd, accountID, at.Unix())
	if err != nil {
		return fmt.Errorf("upsert sticky for %s: %w", cwd, err)
	}
	return nil
}

// PinManual pins cwd to accountID at time at, overriding any existing pin
// (manual or auto) for that directory.
func (s *Store) PinManual(cwd string, accountID int, at time.Time) error {
	_, err := s.db.Exec(
		`INSERT INTO sticky(cwd,account_id,selected_at,manual) VALUES(?,?,?,1)
		 ON CONFLICT(cwd) DO UPDATE SET
		   account_id=excluded.account_id,
		   selected_at=excluded.selected_at,
		   manual=1`,
		cwd, accountID, at.Unix())
	if err != nil {
		return fmt.Errorf("pin %s: %w", cwd, err)
	}
	return nil
}

// DeleteSticky removes cwd's pin (manual or auto). Idempotent: deleting an
// absent row is not an error (a toggle's read-then-delete may race a prune).
func (s *Store) DeleteSticky(cwd string) error {
	if _, err := s.db.Exec(`DELETE FROM sticky WHERE cwd=?`, cwd); err != nil {
		return fmt.Errorf("delete sticky for %s: %w", cwd, err)
	}
	return nil
}

// DeleteStickyVersion removes cwd's pin only if it still matches the version the
// caller read (selected_at + manual), so a concurrent writer's newer row is
// never erased on a stale read.
func (s *Store) DeleteStickyVersion(cwd string, selectedAt time.Time, manual bool) error {
	if _, err := s.db.Exec(
		`DELETE FROM sticky WHERE cwd=? AND selected_at=? AND manual=?`,
		cwd, selectedAt.Unix(), manual); err != nil {
		return fmt.Errorf("delete sticky for %s: %w", cwd, err)
	}
	return nil
}

// GetSticky returns the sticky record for cwd, ok=false if none exists.
func (s *Store) GetSticky(cwd string) (Sticky, bool, error) {
	row := s.db.QueryRow(`SELECT cwd,account_id,selected_at,manual FROM sticky WHERE cwd=?`, cwd)
	var st Sticky
	var at int64
	if err := row.Scan(&st.Cwd, &st.AccountID, &at, &st.Manual); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return st, false, nil
		}
		return st, false, err
	}
	st.SelectedAt = time.Unix(at, 0)
	return st, true, nil
}

// PruneSticky deletes sticky rows whose last activity predates cutoff, returning
// the count. Activity is max(selected_at, latest tracked session end in the cwd);
// a row with a live tracked session always survives, so the pin expires one TTL
// after the cache last saw traffic, not after the last select.
func (s *Store) PruneSticky(cutoff time.Time) (int, error) {
	res, err := s.db.Exec(
		`DELETE FROM sticky WHERE
		   NOT EXISTS (SELECT 1 FROM sessions se
		               WHERE se.cwd = sticky.cwd AND se.account_id = sticky.account_id
		                 AND se.ended_at IS NULL)
		   AND MAX(selected_at,
		           COALESCE((SELECT MAX(se.ended_at) FROM sessions se
		                     WHERE se.cwd = sticky.cwd AND se.account_id = sticky.account_id
		                       AND se.ended_at IS NOT NULL), 0)) < ?`,
		cutoff.Unix())
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// LogRefresh records a refresh attempt outcome.
func (s *Store) LogRefresh(accountID int, ok bool, errMsg string) error {
	v := 0
	if ok {
		v = 1
	}
	_, err := s.db.Exec(
		`INSERT INTO refresh_log(account_id,ts,ok,err) VALUES(?,?,?,?)
		 ON CONFLICT(account_id,ts) DO NOTHING`,
		accountID, time.Now().Unix(), v, errMsg)
	return err
}

// LastRefresh returns the most recent refresh attempt for an account, ok=false
// if none.
func (s *Store) LastRefresh(accountID int) (RefreshEntry, bool, error) {
	row := s.db.QueryRow(
		`SELECT account_id,ts,ok,err FROM refresh_log WHERE account_id=? ORDER BY ts DESC LIMIT 1`, accountID)
	var e RefreshEntry
	var ts int64
	var ok int
	if err := row.Scan(&e.AccountID, &ts, &ok, &e.Err); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e, false, nil
		}
		return e, false, err
	}
	e.TS = time.Unix(ts, 0)
	e.OK = ok != 0
	return e, true, nil
}

// GetAuthHealth returns an account's auth health. An account with no row reads
// as healthy (NeedsLogin false).
func (s *Store) GetAuthHealth(accountID int) (AuthHealth, error) {
	row := s.db.QueryRow(
		`SELECT account_id,needs_login,since,last_err,kind,gen FROM auth_health WHERE account_id=?`, accountID)
	var h AuthHealth
	var needs int
	var since sql.NullInt64
	var kind string
	if err := row.Scan(&h.AccountID, &needs, &since, &h.LastErr, &kind, &h.Gen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AuthHealth{AccountID: accountID}, nil
		}
		return AuthHealth{}, fmt.Errorf("get auth health for account %d: %w", accountID, err)
	}
	h.NeedsLogin = needs != 0
	if since.Valid {
		h.Since = time.Unix(since.Int64, 0)
	}
	h.Kind = AuthKind(kind)
	return h, nil
}

// ListAuthHealth returns the needs-login accounts keyed by id; healthy accounts
// are omitted.
func (s *Store) ListAuthHealth() (map[int]AuthHealth, error) {
	rows, err := s.db.Query(`SELECT account_id,needs_login,since,last_err,kind,gen FROM auth_health WHERE needs_login=1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[int]AuthHealth{}
	for rows.Next() {
		var h AuthHealth
		var needs int
		var since sql.NullInt64
		var kind string
		if err := rows.Scan(&h.AccountID, &needs, &since, &h.LastErr, &kind, &h.Gen); err != nil {
			return nil, err
		}
		h.NeedsLogin = needs != 0
		if since.Valid {
			h.Since = time.Unix(since.Int64, 0)
		}
		h.Kind = AuthKind(kind)
		out[h.AccountID] = h
	}
	return out, rows.Err()
}

// SetNeedsLogin flags an account as needing re-login with its kind, stamping
// Since only on the false→true transition and returning changed=true only then
// (so the daemon logs the hint once). Kind is refreshed and Gen increments on
// every call. The scheduler goroutine is the sole setter of needs_login=1; CLI
// clears use a generation CAS to preserve a fresher verdict.
func (s *Store) SetNeedsLogin(accountID int, at time.Time, errMsg string, kind AuthKind) (bool, error) {
	if !kind.Valid() {
		return false, fmt.Errorf("set needs-login for account %d: invalid auth kind %q", accountID, kind)
	}
	prev, err := s.GetAuthHealth(accountID)
	if err != nil {
		return false, err
	}
	if _, err := s.db.Exec(
		`INSERT INTO auth_health(account_id,needs_login,since,last_err,kind,gen) VALUES(?,1,?,?,?,1)
		 ON CONFLICT(account_id) DO UPDATE SET
		   needs_login=1,
		   last_err=excluded.last_err,
		   kind=excluded.kind,
		   since=CASE WHEN auth_health.needs_login=1 THEN auth_health.since ELSE excluded.since END,
		   gen=auth_health.gen+1`,
		accountID, at.Unix(), errMsg, string(kind)); err != nil {
		return false, fmt.Errorf("set needs-login for account %d: %w", accountID, err)
	}
	return !prev.NeedsLogin, nil
}

// ClearNeedsLogin clears an account's needs-login flag, returning changed=true
// only on the true→false transition, so the daemon logs recovery exactly once.
func (s *Store) ClearNeedsLogin(accountID int) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, last_err='', kind='' WHERE account_id=? AND needs_login=1`,
		accountID)
	if err != nil {
		return false, fmt.Errorf("clear needs-login for account %d: %w", accountID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ClearNeedsLoginIfGen clears an account's needs-login flag only when gen still
// matches the caller's observed generation.
func (s *Store) ClearNeedsLoginIfGen(accountID int, gen int64) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, last_err='', kind='' WHERE account_id=? AND needs_login=1 AND gen=?`,
		accountID, gen)
	if err != nil {
		return false, fmt.Errorf("clear needs-login for account %d: %w", accountID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// RecordJournalRisk upserts a stale-journal risk for dir: cc-pool forgot the row while
// the holder's Unmount still reported a persist-warning, so a holder restart may replay
// dir. The latest warning and time overwrite any prior entry for the same dir.
func (s *Store) RecordJournalRisk(dir, warning string, at time.Time) error {
	if _, err := s.db.Exec(
		`INSERT INTO journal_risks(dir,warning,recorded_at) VALUES(?,?,?)
		 ON CONFLICT(dir) DO UPDATE SET warning=excluded.warning, recorded_at=excluded.recorded_at`,
		dir, warning, at.Unix()); err != nil {
		return fmt.Errorf("record journal risk for %s: %w", dir, err)
	}
	return nil
}

// ListJournalRisks returns every recorded stale-journal risk, oldest first.
func (s *Store) ListJournalRisks() ([]JournalRisk, error) {
	rows, err := s.db.Query(`SELECT dir,warning,recorded_at FROM journal_risks ORDER BY recorded_at`)
	if err != nil {
		return nil, fmt.Errorf("list journal risks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []JournalRisk
	for rows.Next() {
		var r JournalRisk
		var at int64
		if err := rows.Scan(&r.Dir, &r.Warning, &at); err != nil {
			return nil, fmt.Errorf("scan journal risk: %w", err)
		}
		r.RecordedAt = time.Unix(at, 0)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ClearJournalRisk drops the stale-journal risk for dir; a no-op when none is recorded.
func (s *Store) ClearJournalRisk(dir string) error {
	if _, err := s.db.Exec(`DELETE FROM journal_risks WHERE dir=?`, dir); err != nil {
		return fmt.Errorf("clear journal risk for %s: %w", dir, err)
	}
	return nil
}
