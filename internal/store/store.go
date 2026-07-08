// Package store is cc-pool's sole state layer, a pure-Go (modernc.org/sqlite)
// database. It stores NO secrets — the Keychain is the only secret store.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go "sqlite" driver
)

// Store wraps the sqlite connection.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (
  id               INTEGER PRIMARY KEY,
  config_dir       TEXT NOT NULL UNIQUE,
  keychain_service TEXT NOT NULL,
  keychain_account TEXT NOT NULL,
  label            TEXT NOT NULL DEFAULT '',
  overlay_kind     TEXT NOT NULL DEFAULT 'symlink',
  account_uuid     TEXT NOT NULL DEFAULT '',
  cred_hash        TEXT NOT NULL DEFAULT '',
  cred_parent_hash TEXT NOT NULL DEFAULT '',
  created_at       INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_adds (
  id         INTEGER PRIMARY KEY,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS usage_samples (
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
CREATE INDEX IF NOT EXISTS idx_usage_acct_ts ON usage_samples(account_id, ts DESC);
CREATE TABLE IF NOT EXISTS sessions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id   INTEGER NOT NULL,
  pid          INTEGER,
  config_dir   TEXT,
  cwd          TEXT NOT NULL DEFAULT '',
  started_at   INTEGER NOT NULL,
  last_seen_at INTEGER,
  ended_at     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(account_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd, ended_at);
CREATE TABLE IF NOT EXISTS refresh_log (
  account_id INTEGER NOT NULL,
  ts         INTEGER NOT NULL,
  ok         INTEGER NOT NULL,
  err        TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (account_id, ts)
);
CREATE TABLE IF NOT EXISTS sticky (
  cwd         TEXT PRIMARY KEY,
  account_id  INTEGER NOT NULL,
  selected_at INTEGER NOT NULL,
  manual      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS auth_health (
  account_id  INTEGER PRIMARY KEY,
  needs_login INTEGER NOT NULL DEFAULT 0,
  since       INTEGER,
  last_err    TEXT NOT NULL DEFAULT ''
);
`

// Open opens (creating if needed) the database at path and applies the schema.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize writes
	s := &Store{db: db}
	if err := s.applySchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) applySchema() error {
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	// Columns added after usage_samples first shipped: CREATE TABLE IF NOT
	// EXISTS never alters an existing table, so pre-existing databases need
	// these added in place (no pool.db wipe).
	if err := s.ensureColumn("usage_samples", "scoped_7d_util", "REAL"); err != nil {
		return err
	}
	if err := s.ensureColumn("usage_samples", "scoped_7d_resets", "INTEGER"); err != nil {
		return err
	}
	if err := s.ensureColumn("usage_samples", "scoped_7d_model", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn("accounts", "account_uuid", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn("accounts", "cred_hash", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn("accounts", "cred_parent_hash", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	// The index must follow the ensureColumn: on a pre-existing db the column is
	// added above, so it cannot live in the CREATE TABLE schema block (which runs
	// before the migration and would reference a not-yet-added column).
	if _, err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_accounts_uuid ON accounts(account_uuid)`); err != nil {
		return fmt.Errorf("create idx_accounts_uuid: %w", err)
	}
	return nil
}

// ensureColumn adds column (with declaration decl) to table when it is absent.
// Errors fail Open — running against a half-migrated schema is never
// acceptable.
func (s *Store) ensureColumn(table, column, decl string) error {
	var n int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		return fmt.Errorf("inspect %s.%s: %w", table, column, err)
	}
	if n > 0 {
		return nil
	}
	// table/column/decl are compile-time constants; nothing user-controlled.
	if _, err := s.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, decl)); err != nil {
		// Two processes can race the check-then-ALTER on the first open after
		// an upgrade (sqlite has no ADD COLUMN IF NOT EXISTS). The
		// postcondition is "column exists" — re-check before failing so the
		// duplicate-column loser swallows its error.
		var again int
		if err2 := s.db.QueryRow(
			`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&again); err2 == nil && again > 0 {
			return nil
		}
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
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
func (s *Store) Close() error { return s.db.Close() }

// UpsertAccount inserts or replaces an account row by id; account_uuid and the
// chain-hash columns are insert-only so a re-upsert can't wipe backfilled values.
func (s *Store) UpsertAccount(a Account) error {
	created := a.CreatedAt
	if created.IsZero() {
		created = time.Now()
	}
	_, err := s.db.Exec(
		`INSERT INTO accounts(id,config_dir,keychain_service,keychain_account,label,overlay_kind,account_uuid,cred_hash,cred_parent_hash,created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		   config_dir=excluded.config_dir,
		   keychain_service=excluded.keychain_service,
		   keychain_account=excluded.keychain_account,
		   label=excluded.label,
		   overlay_kind=excluded.overlay_kind`,
		a.ID, a.ConfigDir, a.KeychainService, a.KeychainAccount, a.Label, a.OverlayKind, a.AccountUUID, a.CredHash, a.CredParentHash, created.Unix())
	if err != nil {
		return fmt.Errorf("upsert account %d: %w", a.ID, err)
	}
	return nil
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
	res, err := s.db.Exec(`UPDATE accounts SET overlay_kind=? WHERE id=?`, kind, id)
	if err != nil {
		return fmt.Errorf("set overlay kind for account %d: %w", id, err)
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

func scanAccount(rows interface{ Scan(...any) error }) (Account, error) {
	var a Account
	var created int64
	if err := rows.Scan(&a.ID, &a.ConfigDir, &a.KeychainService, &a.KeychainAccount,
		&a.Label, &a.OverlayKind, &a.AccountUUID, &a.CredHash, &a.CredParentHash, &created); err != nil {
		return a, err
	}
	a.CreatedAt = time.Unix(created, 0)
	return a, nil
}

const accountCols = `id,config_dir,keychain_service,keychain_account,label,overlay_kind,account_uuid,cred_hash,cred_parent_hash,created_at`

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

// GetAccount returns one account by id.
func (s *Store) GetAccount(id int) (Account, error) {
	row := s.db.QueryRow(`SELECT `+accountCols+` FROM accounts WHERE id=?`, id)
	a, err := scanAccount(row)
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("account %d not found", id)
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

// SetChainHashes records the last written credential's hash and its parent's;
// a targeted UPDATE so it can't clobber concurrent updates to other columns.
func (s *Store) SetChainHashes(id int, credHash, parentHash string) error {
	res, err := s.db.Exec(`UPDATE accounts SET cred_hash=?, cred_parent_hash=? WHERE id=?`, credHash, parentHash, id)
	if err != nil {
		return fmt.Errorf("set chain hashes for account %d: %w", id, err)
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

// OpenSession records a new checkout at time at and returns its id. cwd is the
// launch directory ("" when unknown), feeding the sticky activity rules.
func (s *Store) OpenSession(accountID, pid int, configDir, cwd string, at time.Time) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO sessions(account_id,pid,config_dir,cwd,started_at) VALUES(?,?,?,?,?)`,
		accountID, pid, configDir, cwd, at.Unix())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
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
		`SELECT id,account_id,pid,config_dir,cwd,started_at,last_seen_at FROM sessions WHERE ended_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Session
	for rows.Next() {
		var se Session
		var started int64
		var pid, seen sql.NullInt64
		var cd sql.NullString
		if err := rows.Scan(&se.ID, &se.AccountID, &pid, &cd, &se.Cwd, &started, &seen); err != nil {
			return nil, err
		}
		se.PID = int(pid.Int64)
		se.ConfigDir = cd.String
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
func (s *Store) CloseDeadSessions(alive map[int]bool, at time.Time) (int, error) {
	sessions, err := s.ListActiveSessions()
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, se := range sessions {
		if se.PID <= 0 {
			continue
		}
		if alive[se.PID] {
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
// statement, since the daemon and a live-path CLI would race a read-modify-write.
func (s *Store) UpsertSticky(cwd string, accountID int, at time.Time) error {
	_, err := s.db.Exec(
		`INSERT INTO sticky(cwd,account_id,selected_at,manual) VALUES(?,?,?,0)
		 ON CONFLICT(cwd) DO UPDATE SET
		   account_id=excluded.account_id,
		   selected_at=excluded.selected_at
		 WHERE manual = 0 OR account_id = excluded.account_id`,
		cwd, accountID, at.Unix())
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
		`SELECT account_id,needs_login,since,last_err FROM auth_health WHERE account_id=?`, accountID)
	var h AuthHealth
	var needs int
	var since sql.NullInt64
	if err := row.Scan(&h.AccountID, &needs, &since, &h.LastErr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AuthHealth{AccountID: accountID}, nil
		}
		return AuthHealth{}, fmt.Errorf("get auth health for account %d: %w", accountID, err)
	}
	h.NeedsLogin = needs != 0
	if since.Valid {
		h.Since = time.Unix(since.Int64, 0)
	}
	return h, nil
}

// ListAuthHealth returns the needs-login accounts keyed by id; healthy accounts
// are omitted.
func (s *Store) ListAuthHealth() (map[int]AuthHealth, error) {
	rows, err := s.db.Query(`SELECT account_id,needs_login,since,last_err FROM auth_health WHERE needs_login=1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[int]AuthHealth{}
	for rows.Next() {
		var h AuthHealth
		var needs int
		var since sql.NullInt64
		if err := rows.Scan(&h.AccountID, &needs, &since, &h.LastErr); err != nil {
			return nil, err
		}
		h.NeedsLogin = needs != 0
		if since.Valid {
			h.Since = time.Unix(since.Int64, 0)
		}
		out[h.AccountID] = h
	}
	return out, rows.Err()
}

// SetNeedsLogin flags an account as needing re-login, stamping Since only on the
// false→true transition (a repeat preserves the original Since, so the timer
// measures the whole outage) and returning changed=true only then, so the daemon
// logs the hint once. The daemon poll loop is the sole writer, so the
// read-then-write is race-free.
func (s *Store) SetNeedsLogin(accountID int, at time.Time, errMsg string) (bool, error) {
	prev, err := s.GetAuthHealth(accountID)
	if err != nil {
		return false, err
	}
	if _, err := s.db.Exec(
		`INSERT INTO auth_health(account_id,needs_login,since,last_err) VALUES(?,1,?,?)
		 ON CONFLICT(account_id) DO UPDATE SET
		   needs_login=1,
		   last_err=excluded.last_err,
		   since=CASE WHEN auth_health.needs_login=1 THEN auth_health.since ELSE excluded.since END`,
		accountID, at.Unix(), errMsg); err != nil {
		return false, fmt.Errorf("set needs-login for account %d: %w", accountID, err)
	}
	return !prev.NeedsLogin, nil
}

// ClearNeedsLogin clears an account's needs-login flag, returning changed=true
// only on the true→false transition, so the daemon logs recovery exactly once.
func (s *Store) ClearNeedsLogin(accountID int) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, last_err='' WHERE account_id=? AND needs_login=1`,
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
