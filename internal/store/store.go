// Package store is cc-pool's sole state layer, a pure-Go (modernc.org/sqlite)
// database. It stores NO secrets — the Keychain is the only secret store.
package store

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
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
  config_dir       TEXT NOT NULL CHECK(config_dir <> ''),
  keychain_service TEXT NOT NULL CHECK(keychain_service <> ''),
  keychain_account TEXT NOT NULL CHECK(keychain_account <> ''),
  label            TEXT NOT NULL DEFAULT '',
  account_uuid     TEXT NOT NULL UNIQUE CHECK(account_uuid <> ''),
  created_at       INTEGER NOT NULL,
  deleted_at       INTEGER CHECK(deleted_at IS NULL OR deleted_at > 0)
);
CREATE TABLE account_removals (
  account_id         INTEGER PRIMARY KEY CHECK(account_id > 0),
  account_instance_id TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation INTEGER NOT NULL CHECK(account_generation > 0),
  registry_sequence  INTEGER NOT NULL CHECK(registry_sequence > 0),
  delete_credential  INTEGER NOT NULL CHECK(delete_credential IN (0,1)),
  created_at         INTEGER NOT NULL
);
CREATE TABLE pending_adds (
  id          INTEGER PRIMARY KEY CHECK(id > 0),
  instance_id TEXT NOT NULL UNIQUE CHECK(length(instance_id) = 32 AND instance_id NOT GLOB '*[^0-9a-f]*'),
  generation  INTEGER NOT NULL CHECK(generation = 1),
	owner_record BLOB NOT NULL CHECK(length(owner_record) > 0),
  created_at  INTEGER NOT NULL CHECK(created_at > 0)
);
CREATE TABLE account_registry_sequences (
  account_id INTEGER PRIMARY KEY CHECK(account_id > 0),
  sequence   INTEGER NOT NULL CHECK(sequence >= 0)
);
CREATE TABLE account_mutations (
  operation_id               BLOB PRIMARY KEY CHECK(length(operation_id) = 32),
  account_id                 INTEGER NOT NULL UNIQUE CHECK(account_id > 0),
  kind                       TEXT NOT NULL CHECK(kind IN ('add','relogin','presentation-rebind')),
  state                      TEXT NOT NULL CHECK(state IN ('awaiting-presentation','awaiting-input','reserved','applying','applied','publishing','compensating','rebind-published')),
  registry_sequence          INTEGER NOT NULL CHECK(registry_sequence > 0),
  account_instance_id        TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation         INTEGER NOT NULL CHECK(account_generation > 0),
  locator_digest             BLOB NOT NULL CHECK(length(locator_digest) = 32),
  expected_credential_digest BLOB NOT NULL CHECK(length(expected_credential_digest) = 32),
  intent_digest              BLOB NOT NULL CHECK(length(intent_digest) = 32),
  input_digest               BLOB CHECK(input_digest IS NULL OR length(input_digest) = 32),
  written_credential_digest  BLOB NOT NULL DEFAULT (zeroblob(32)) CHECK(length(written_credential_digest) = 32),
  credential_written         INTEGER NOT NULL DEFAULT 0 CHECK(credential_written IN (0,1)),
  config_dir                 TEXT NOT NULL,
  keychain_service           TEXT NOT NULL,
  keychain_account           TEXT NOT NULL,
  label                      TEXT NOT NULL DEFAULT '',
  account_uuid               TEXT NOT NULL DEFAULT '',
	presentation_tenant_id    TEXT NOT NULL,
	presentation_domain_id    TEXT NOT NULL,
	presentation_generation   INTEGER NOT NULL CHECK(presentation_generation >= 0),
	presentation_public_path  TEXT NOT NULL,
	previous_config_dir       TEXT NOT NULL,
	previous_keychain_service TEXT NOT NULL,
	previous_keychain_account TEXT NOT NULL,
	previous_locator_digest   BLOB NOT NULL CHECK(length(previous_locator_digest)=32),
	previous_credential_state TEXT NOT NULL CHECK(previous_credential_state IN ('','empty','present')),
	previous_credential_digest BLOB CHECK(previous_credential_digest IS NULL OR length(previous_credential_digest)=32),
  owner_record               BLOB NOT NULL CHECK(length(owner_record) > 0),
  owner_epoch                INTEGER NOT NULL CHECK(owner_epoch > 0),
  created_at                 INTEGER NOT NULL CHECK(created_at > 0),
  updated_at                 INTEGER NOT NULL CHECK(updated_at >= created_at),
  CHECK((kind IN ('add','presentation-rebind') AND state='awaiting-presentation' AND
         locator_digest=zeroblob(32) AND expected_credential_digest=zeroblob(32) AND
         config_dir='' AND keychain_service='' AND keychain_account='' AND
		 presentation_tenant_id='' AND presentation_domain_id='' AND
		 presentation_generation=0 AND presentation_public_path='') OR
        (state<>'awaiting-presentation' AND config_dir<>'' AND keychain_service<>'' AND keychain_account<>'' AND
		 ((kind IN ('add','presentation-rebind') AND presentation_tenant_id<>'' AND
		   presentation_domain_id<>'' AND account_generation=presentation_generation) OR
		  (kind NOT IN ('add','presentation-rebind') AND presentation_tenant_id='' AND
		   presentation_domain_id='' AND presentation_generation=0 AND presentation_public_path='')))),
	CHECK((kind='presentation-rebind' AND previous_config_dir<>'' AND previous_keychain_service<>'' AND
	       previous_keychain_account<>'' AND previous_locator_digest<>zeroblob(32) AND
	       ((previous_credential_state='empty' AND previous_credential_digest IS NULL) OR
	        (previous_credential_state='present' AND previous_credential_digest IS NOT NULL AND
	         previous_credential_digest<>zeroblob(32)))) OR
	      (kind<>'presentation-rebind' AND previous_config_dir='' AND previous_keychain_service='' AND
	       previous_keychain_account='' AND previous_locator_digest=zeroblob(32) AND
	       previous_credential_state='' AND previous_credential_digest IS NULL)),
	CHECK(state<>'rebind-published' OR kind='presentation-rebind')
);
CREATE TABLE account_mutation_receipts (
  operation_id      BLOB PRIMARY KEY CHECK(length(operation_id) = 32),
  account_id        INTEGER NOT NULL CHECK(account_id > 0),
  kind              TEXT NOT NULL CHECK(kind IN ('add','relogin','presentation-rebind')),
  registry_sequence INTEGER NOT NULL CHECK(registry_sequence > 0),
  account_instance_id TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation INTEGER NOT NULL CHECK(account_generation > 0),
  locator_digest     BLOB NOT NULL CHECK(length(locator_digest) = 32),
  expected_credential_digest BLOB NOT NULL CHECK(length(expected_credential_digest) = 32),
  intent_digest      BLOB NOT NULL CHECK(length(intent_digest) = 32),
  input_digest       BLOB CHECK(input_digest IS NULL OR length(input_digest) = 32),
  written_credential_digest BLOB CHECK(written_credential_digest IS NULL OR length(written_credential_digest) = 32),
  credential_written INTEGER NOT NULL CHECK(credential_written IN (0,1)),
  outcome_digest      BLOB NOT NULL CHECK(length(outcome_digest) = 32),
  terminal          TEXT NOT NULL CHECK(terminal IN ('committed','superseded','aborted','quarantined')),
	quarantine_reason TEXT CHECK(quarantine_reason IS NULL OR quarantine_reason IN ('ambiguous','diverged','cleanup-failed','changed-underfoot')),
	resolution TEXT CHECK(resolution IS NULL OR resolution='compensated-release'),
	resolution_observed_digest BLOB CHECK(resolution_observed_digest IS NULL OR length(resolution_observed_digest) = 32),
	resolved_at INTEGER,
  config_dir        TEXT NOT NULL CHECK(config_dir <> ''),
  keychain_service  TEXT NOT NULL CHECK(keychain_service <> ''),
  keychain_account  TEXT NOT NULL CHECK(keychain_account <> ''),
  label             TEXT NOT NULL DEFAULT '',
  account_uuid      TEXT NOT NULL DEFAULT '',
	presentation_tenant_id TEXT NOT NULL,
	presentation_domain_id TEXT NOT NULL,
	presentation_generation INTEGER NOT NULL CHECK(presentation_generation >= 0),
	presentation_public_path TEXT NOT NULL,
	previous_config_dir TEXT NOT NULL,
	previous_keychain_service TEXT NOT NULL,
	previous_keychain_account TEXT NOT NULL,
	previous_locator_digest BLOB NOT NULL CHECK(length(previous_locator_digest)=32),
	previous_credential_state TEXT NOT NULL CHECK(previous_credential_state IN ('','empty','present')),
	previous_credential_digest BLOB CHECK(previous_credential_digest IS NULL OR length(previous_credential_digest)=32),
  owner_record      BLOB NOT NULL CHECK(length(owner_record) > 0),
  owner_epoch       INTEGER NOT NULL CHECK(owner_epoch > 0),
	publication_pending INTEGER NOT NULL CHECK(publication_pending IN (0,1)),
  committed_at      INTEGER NOT NULL CHECK(committed_at > 0),
  acknowledged_at   INTEGER CHECK(acknowledged_at IS NULL OR acknowledged_at >= committed_at),
  expires_at        INTEGER NOT NULL CHECK(expires_at > committed_at),
	CHECK((credential_written=1) = (written_credential_digest IS NOT NULL)),
	CHECK((kind IN ('add','presentation-rebind') AND presentation_tenant_id<>'' AND
	       presentation_domain_id<>'' AND account_generation=presentation_generation) OR
	      (kind NOT IN ('add','presentation-rebind') AND presentation_tenant_id='' AND
	       presentation_domain_id='' AND presentation_generation=0 AND presentation_public_path='')),
	CHECK((kind='presentation-rebind' AND previous_config_dir<>'' AND previous_keychain_service<>'' AND
	       previous_keychain_account<>'' AND previous_locator_digest<>zeroblob(32) AND
	       ((previous_credential_state='empty' AND previous_credential_digest IS NULL) OR
	        (previous_credential_state='present' AND previous_credential_digest IS NOT NULL AND
	         previous_credential_digest<>zeroblob(32)))) OR
	      (kind<>'presentation-rebind' AND previous_config_dir='' AND previous_keychain_service='' AND
	       previous_keychain_account='' AND previous_locator_digest=zeroblob(32) AND
	       previous_credential_state='' AND previous_credential_digest IS NULL)),
	CHECK((terminal='quarantined') = (quarantine_reason IS NOT NULL)),
	CHECK(publication_pending=0 OR
	      (terminal='committed' AND kind IN ('add','presentation-rebind'))),
	CHECK((resolution IS NULL AND resolution_observed_digest IS NULL AND resolved_at IS NULL)
	   OR (resolution IS NOT NULL AND resolution_observed_digest IS NOT NULL AND resolved_at IS NOT NULL AND resolved_at>=committed_at))
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
  selection_token TEXT NOT NULL UNIQUE CHECK(length(selection_token) = 32 AND selection_token NOT GLOB '*[^0-9a-f]*'),
  account_id   INTEGER NOT NULL CHECK(account_id > 0),
  account_instance_id TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation INTEGER NOT NULL CHECK(account_generation > 0),
  pid          INTEGER NOT NULL CHECK(pid > 0),
  process_started_at INTEGER NOT NULL CHECK(process_started_at > 0),
  config_dir   TEXT NOT NULL CHECK(config_dir <> ''),
  cwd          TEXT NOT NULL DEFAULT '',
  started_at   INTEGER NOT NULL CHECK(started_at > 0),
  file_provider_lease_state TEXT NOT NULL CHECK(file_provider_lease_state IN ('pending','active','released')),
  file_provider_lease_receipt BLOB NOT NULL CHECK(length(file_provider_lease_receipt) BETWEEN 1 AND 65536),
  file_provider_lease_expires_at INTEGER NOT NULL CHECK(file_provider_lease_expires_at > 0),
  lease_renewal_expires_at INTEGER CHECK(lease_renewal_expires_at > 0),
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
	file_provider_provisional_lease_receipt BLOB NOT NULL CHECK(length(file_provider_provisional_lease_receipt) BETWEEN 1 AND 65536),
	file_provider_committed_lease_receipt BLOB NOT NULL CHECK(length(file_provider_committed_lease_receipt) BETWEEN 1 AND 65536),
  committed_at        INTEGER NOT NULL CHECK(committed_at > 0),
  expires_at          INTEGER NOT NULL CHECK(expires_at > committed_at)
);
CREATE TABLE refresh_log (
  account_id INTEGER NOT NULL,
  ts         INTEGER NOT NULL,
	category   TEXT NOT NULL CHECK(category IN ('succeeded','canceled','network','invalid_grant','rejected','server','internal')),
	digest     BLOB NOT NULL CHECK(length(digest) = 32),
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
	reason      TEXT NOT NULL CHECK(reason IN ('none','auth_required','awaiting_origin','internal')),
	digest      BLOB NOT NULL CHECK(length(digest) = 32),
	kind        TEXT NOT NULL DEFAULT 'owned' CHECK(kind IN ('owned','awaiting_origin','unverified')),
	gen         INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE credential_operations (
	account_id             INTEGER PRIMARY KEY CHECK(account_id > 0),
  operation_id           BLOB NOT NULL UNIQUE CHECK(length(operation_id) = 32),
  token                  TEXT NOT NULL UNIQUE CHECK(length(token) = 32 AND token NOT GLOB '*[^0-9a-f]*'),
  kind                   TEXT NOT NULL CHECK(kind IN ('ensure-fresh','refresh-current','install-synced','adopt-rotated','compensate','remove')),
  target                 TEXT NOT NULL CHECK(target='keychain'),
  intent_digest          BLOB NOT NULL CHECK(length(intent_digest) = 32),
  account_instance_id    TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation     INTEGER NOT NULL CHECK(account_generation > 0),
  config_dir             TEXT NOT NULL CHECK(config_dir<>''),
  keychain_service       TEXT NOT NULL CHECK(keychain_service<>''),
  keychain_account       TEXT NOT NULL CHECK(keychain_account<>''),
  locator_digest         BLOB NOT NULL CHECK(length(locator_digest) = 32),
  owner_record           BLOB NOT NULL CHECK(length(owner_record) > 0),
  owner_epoch            INTEGER NOT NULL CHECK(owner_epoch > 0),
  state                  TEXT NOT NULL CHECK(state IN ('prepared','applying','applied')),
  expected_keychain_state TEXT NOT NULL CHECK(expected_keychain_state IN ('empty','present','unsearchable','unreadable')),
  expected_keychain_digest BLOB CHECK(expected_keychain_digest IS NULL OR length(expected_keychain_digest) = 32),
  outcome_keychain_state  TEXT CHECK(outcome_keychain_state IS NULL OR outcome_keychain_state IN ('empty','present','unsearchable','unreadable')),
  outcome_keychain_digest BLOB CHECK(outcome_keychain_digest IS NULL OR length(outcome_keychain_digest) = 32),
  terminal_status         TEXT CHECK(terminal_status IS NULL OR terminal_status IN ('succeeded','failed','quarantined')),
  result_category         TEXT CHECK(result_category IS NULL OR result_category IN ('done','unchanged','refreshed','needs-login','no-tokens','installed','adopted','skipped','failed','ambiguous','diverged','cleanup-failed','changed-underfoot')),
  failure_class           TEXT CHECK(failure_class IS NULL OR failure_class IN ('internal','network','refresh-unauthorized','refresh-rejected','refresh-server')),
  publication_payload     BLOB CHECK(publication_payload IS NULL OR (length(publication_payload) > 0 AND length(publication_payload) <= 4096)),
  created_at             INTEGER NOT NULL CHECK(created_at > 0),
  updated_at             INTEGER NOT NULL CHECK(updated_at >= created_at),
  CHECK(
    (state IN ('prepared','applying') AND outcome_keychain_state IS NULL AND terminal_status IS NULL AND result_category IS NULL AND failure_class IS NULL)
    OR
    (state='applied' AND outcome_keychain_state IS NOT NULL AND terminal_status IS NOT NULL AND result_category IS NOT NULL)
  ),
	CHECK(failure_class IS NULL OR failure_class='internal' OR
	      (kind IN ('ensure-fresh','refresh-current') AND failure_class IN ('network','refresh-unauthorized','refresh-rejected','refresh-server'))),
	CHECK(terminal_status IS NULL OR
	      (terminal_status='succeeded' AND failure_class IS NULL AND
	       result_category IN ('done','unchanged','refreshed','needs-login','no-tokens','installed','adopted','skipped')) OR
	      (terminal_status='failed' AND result_category='failed' AND failure_class IS NOT NULL AND
	       failure_class IN ('internal','network','refresh-unauthorized','refresh-rejected','refresh-server')) OR
	      (terminal_status='quarantined' AND failure_class IS NOT NULL AND
	       result_category IN ('ambiguous','diverged','cleanup-failed','changed-underfoot') AND
	       failure_class IN ('internal','network','refresh-server'))),
  CHECK((expected_keychain_state='present') = (expected_keychain_digest IS NOT NULL)),
	CHECK(outcome_keychain_state IS NULL OR ((outcome_keychain_state='present') = (outcome_keychain_digest IS NOT NULL))),
	CHECK(publication_payload IS NULL OR state='applying' OR
	      (state='applied' AND terminal_status='succeeded' AND result_category IN ('refreshed','installed','adopted'))),
	CHECK(state!='applied' OR terminal_status!='succeeded' OR
	      result_category NOT IN ('refreshed','installed','adopted') OR publication_payload IS NOT NULL)
);
CREATE TABLE credential_operation_receipts (
  operation_id         BLOB PRIMARY KEY CHECK(length(operation_id) = 32),
  token                TEXT NOT NULL UNIQUE CHECK(length(token) = 32 AND token NOT GLOB '*[^0-9a-f]*'),
	account_id           INTEGER NOT NULL CHECK(account_id > 0),
  account_instance_id  TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation   INTEGER NOT NULL CHECK(account_generation > 0),
  locator_digest       BLOB NOT NULL CHECK(length(locator_digest) = 32),
  config_dir           TEXT NOT NULL CHECK(config_dir<>''),
  keychain_service     TEXT NOT NULL CHECK(keychain_service<>''),
  keychain_account     TEXT NOT NULL CHECK(keychain_account<>''),
  kind                 TEXT NOT NULL CHECK(kind IN ('ensure-fresh','refresh-current','install-synced','adopt-rotated','compensate','remove')),
  target               TEXT NOT NULL CHECK(target='keychain'),
  intent_digest        BLOB NOT NULL CHECK(length(intent_digest) = 32),
  expected_keychain_state TEXT NOT NULL CHECK(expected_keychain_state IN ('empty','present','unsearchable','unreadable')),
  expected_keychain_digest BLOB CHECK(expected_keychain_digest IS NULL OR length(expected_keychain_digest) = 32),
  owner_record         BLOB NOT NULL CHECK(length(owner_record) > 0),
  owner_epoch          INTEGER NOT NULL CHECK(owner_epoch > 0),
  terminal_status      TEXT NOT NULL CHECK(terminal_status IN ('succeeded','failed','quarantined')),
  result_category      TEXT NOT NULL CHECK(result_category IN ('done','unchanged','refreshed','needs-login','no-tokens','installed','adopted','skipped','failed','ambiguous','diverged','cleanup-failed','changed-underfoot')),
  failure_class        TEXT CHECK(failure_class IS NULL OR failure_class IN ('internal','network','refresh-unauthorized','refresh-rejected','refresh-server')),
  outcome_keychain_state  TEXT NOT NULL CHECK(outcome_keychain_state IN ('empty','present','unsearchable','unreadable')),
  outcome_keychain_digest BLOB CHECK(outcome_keychain_digest IS NULL OR length(outcome_keychain_digest) = 32),
  publication_payload     BLOB CHECK(publication_payload IS NULL OR (length(publication_payload) > 0 AND length(publication_payload) <= 4096)),
  committed_at         INTEGER NOT NULL CHECK(committed_at > 0),
  acknowledged_at      INTEGER CHECK(acknowledged_at IS NULL OR acknowledged_at >= committed_at),
  expires_at           INTEGER NOT NULL CHECK(expires_at > committed_at),
	CHECK(failure_class IS NULL OR failure_class='internal' OR
	      (kind IN ('ensure-fresh','refresh-current') AND failure_class IN ('network','refresh-unauthorized','refresh-rejected','refresh-server'))),
	CHECK((terminal_status='succeeded' AND failure_class IS NULL AND
	       result_category IN ('done','unchanged','refreshed','needs-login','no-tokens','installed','adopted','skipped')) OR
	      (terminal_status='failed' AND result_category='failed' AND failure_class IS NOT NULL AND
	       failure_class IN ('internal','network','refresh-unauthorized','refresh-rejected','refresh-server')) OR
	      (terminal_status='quarantined' AND failure_class IS NOT NULL AND
	       result_category IN ('ambiguous','diverged','cleanup-failed','changed-underfoot') AND
	       failure_class IN ('internal','network','refresh-server'))),
  CHECK((expected_keychain_state='present') = (expected_keychain_digest IS NOT NULL)),
	CHECK((outcome_keychain_state='present') = (outcome_keychain_digest IS NOT NULL)),
	CHECK((publication_payload IS NOT NULL) =
	      (terminal_status='succeeded' AND result_category IN ('refreshed','installed','adopted')))
);
CREATE TABLE credential_quarantines (
	account_id               INTEGER PRIMARY KEY CHECK(account_id > 0),
  account_instance_id      TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation       INTEGER NOT NULL CHECK(account_generation > 0),
  locator_digest           BLOB NOT NULL CHECK(length(locator_digest) = 32),
  observation_keychain_state  TEXT NOT NULL CHECK(observation_keychain_state IN ('empty','present','unsearchable','unreadable')),
  observation_keychain_digest BLOB CHECK(observation_keychain_digest IS NULL OR length(observation_keychain_digest) = 32),
  token_chain_digest          BLOB CHECK(token_chain_digest IS NULL OR length(token_chain_digest) = 32),
  reason                    TEXT NOT NULL CHECK(reason IN ('ambiguous','diverged','cleanup-failed','changed-underfoot')),
  failure_class             TEXT NOT NULL CHECK(failure_class IN ('internal','network','refresh-server')),
  created_at                INTEGER NOT NULL CHECK(created_at > 0),
  CHECK((observation_keychain_state='present') = (observation_keychain_digest IS NOT NULL))
);
CREATE TABLE synced_credential_admissions (
	account_id               INTEGER NOT NULL CHECK(account_id > 0),
	account_instance_id      TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
	account_generation       INTEGER NOT NULL CHECK(account_generation > 0),
	locator_digest           BLOB NOT NULL CHECK(length(locator_digest) = 32 AND locator_digest <> zeroblob(32)),
	external_state_digest    BLOB NOT NULL CHECK(length(external_state_digest) = 32 AND external_state_digest <> zeroblob(32)),
	token_chain_digest       BLOB NOT NULL CHECK(length(token_chain_digest) = 32 AND token_chain_digest <> zeroblob(32)),
	access_hash_digest       BLOB NOT NULL CHECK(length(access_hash_digest) = 32 AND access_hash_digest <> zeroblob(32)),
	admitted_at              INTEGER NOT NULL CHECK(admitted_at > 0),
	PRIMARY KEY (account_id, account_instance_id, account_generation)
);
CREATE TABLE pending_synced_credential_admissions (
	account_id               INTEGER PRIMARY KEY CHECK(account_id > 0),
	account_instance_id      TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
	account_generation       INTEGER NOT NULL CHECK(account_generation > 0),
	locator_digest           BLOB NOT NULL CHECK(length(locator_digest) = 32 AND locator_digest <> zeroblob(32)),
	external_state_digest    BLOB NOT NULL CHECK(length(external_state_digest) = 32 AND external_state_digest <> zeroblob(32)),
	token_chain_digest       BLOB NOT NULL CHECK(length(token_chain_digest) = 32 AND token_chain_digest <> zeroblob(32)),
	access_hash_digest       BLOB NOT NULL CHECK(length(access_hash_digest) = 32 AND access_hash_digest <> zeroblob(32)),
	staged_at                INTEGER NOT NULL CHECK(staged_at > 0),
	candidate_at             INTEGER NOT NULL CHECK(candidate_at = 0 OR candidate_at >= staged_at)
);
CREATE TABLE account_presentations (
  account_id              INTEGER PRIMARY KEY CHECK(account_id > 0),
  account_instance_id     TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation      INTEGER NOT NULL CHECK(account_generation > 0),
  tenant_id               TEXT NOT NULL CHECK(tenant_id <> ''),
  domain_id               TEXT NOT NULL CHECK(domain_id <> ''),
  presentation_generation INTEGER NOT NULL CHECK(presentation_generation > 0),
  public_path             TEXT NOT NULL CHECK(public_path <> ''),
  observed_at             INTEGER NOT NULL CHECK(observed_at > 0)
);
CREATE TABLE account_presentation_quarantines (
  account_id              INTEGER PRIMARY KEY CHECK(account_id > 0),
  account_instance_id     TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation      INTEGER NOT NULL CHECK(account_generation > 0),
  expected_config_dir     TEXT NOT NULL CHECK(expected_config_dir <> ''),
  observed_tenant_id      TEXT NOT NULL CHECK(observed_tenant_id <> ''),
  observed_domain_id      TEXT NOT NULL CHECK(observed_domain_id <> ''),
  observed_generation     INTEGER NOT NULL CHECK(observed_generation > 0),
  observed_public_path    TEXT NOT NULL CHECK(observed_public_path <> ''),
  reason                  TEXT NOT NULL CHECK(reason IN ('public-path-drift','tenant-id-drift','domain-id-drift','generation-drift')),
  created_at              INTEGER NOT NULL CHECK(created_at > 0)
);
CREATE TABLE account_presentation_repairs (
  account_id                       INTEGER PRIMARY KEY CHECK(account_id > 0),
  account_instance_id              TEXT NOT NULL CHECK(length(account_instance_id) = 32 AND account_instance_id NOT GLOB '*[^0-9a-f]*'),
  account_generation               INTEGER NOT NULL CHECK(account_generation > 0),
  previous_tenant_id               TEXT NOT NULL CHECK(previous_tenant_id <> ''),
  previous_domain_id               TEXT NOT NULL CHECK(previous_domain_id <> ''),
  previous_presentation_generation INTEGER NOT NULL CHECK(previous_presentation_generation > 0),
  previous_public_path             TEXT NOT NULL CHECK(previous_public_path <> ''),
  target_tenant_id                 TEXT NOT NULL CHECK(target_tenant_id <> ''),
  target_domain_id                 TEXT NOT NULL CHECK(target_domain_id <> ''),
  target_presentation_generation   INTEGER NOT NULL CHECK(target_presentation_generation > 0),
  target_public_path               TEXT NOT NULL CHECK(target_public_path <> ''),
  created_at                       INTEGER NOT NULL CHECK(created_at > 0)
);
CREATE UNIQUE INDEX idx_accounts_live_config_dir ON accounts(config_dir) WHERE deleted_at IS NULL;
CREATE INDEX idx_account_mutations_owner ON account_mutations(owner_record,account_id);
CREATE UNIQUE INDEX idx_account_mutations_single_add ON account_mutations(kind) WHERE kind='add';
CREATE INDEX idx_account_mutation_receipts_scope ON account_mutation_receipts(kind,account_id,acknowledged_at,committed_at);
CREATE INDEX idx_account_mutation_receipts_expiry ON account_mutation_receipts(acknowledged_at,expires_at,operation_id);
CREATE INDEX idx_credential_operations_owner ON credential_operations(owner_record,account_id);
CREATE INDEX idx_credential_operation_receipts_expiry ON credential_operation_receipts(acknowledged_at,expires_at,token);
CREATE UNIQUE INDEX idx_credential_write_receipts_pending ON credential_operation_receipts(account_id)
  WHERE acknowledged_at IS NULL AND terminal_status='succeeded'
  AND result_category IN ('refreshed','installed','adopted');
CREATE UNIQUE INDEX idx_account_presentations_public_path ON account_presentations(public_path);
`

// SchemaVersion is the only runtime schema accepted by this binary.
const SchemaVersion = 1

// ErrSchemaMismatch means the database is not the exact schema accepted by this binary.
var ErrSchemaMismatch = errors.New("store schema mismatch")

// ErrAwaitingOriginAdmission means a generic auth-health path attempted to
// bypass credential-bound synced admission.
var ErrAwaitingOriginAdmission = errors.New("awaiting-origin admission requires exact credential evidence")

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
	return open(path, false)
}

// OpenReadOnly opens an existing exact-schema store without initializing or
// reconciling any durable state.
func OpenReadOnly(path string) (*Store, error) {
	return open(path, true)
}

func open(path string, readOnly bool) (*Store, error) {
	path, err := canonicalDatabasePath(path)
	if err != nil {
		return nil, err
	}
	if readOnly {
		if err := requireSingleLinkDatabase(path); err != nil {
			return nil, err
		}
		if _, err := os.Lstat(path + ".lifecycle.lock"); err != nil {
			return nil, fmt.Errorf("open read-only store lifecycle: %w", err)
		}
	}
	lifecycleLock, err := proc.FileLockSpec{
		Path: path + ".lifecycle.lock", Mode: proc.FileLockShared, Deadline: time.Second,
	}.TryAcquire()
	if err != nil {
		return nil, fmt.Errorf("open store lifecycle: %w", err)
	}
	db, err := sql.Open("sqlite", storeDSN(path, readOnly))
	if err != nil {
		_ = lifecycleLock.Close()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize writes
	s := &Store{db: db, lifecycleLock: lifecycleLock, now: time.Now}
	if readOnly {
		err = verifySchema(db)
	} else {
		err = s.initializeOrVerifySchema()
	}
	if err != nil {
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

func storeDSN(path string, readOnly bool) string {
	location := &url.URL{Scheme: "file", Path: path}
	query := location.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(on)")
	if readOnly {
		query.Add("_pragma", "query_only(on)")
		query.Set("mode", "ro")
	} else {
		query.Add("_pragma", "journal_mode(WAL)")
	}
	location.RawQuery = query.Encode()
	return location.String()
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
		return fmt.Errorf("%w: database=%d binary=%d", ErrSchemaMismatch, version, SchemaVersion)
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

// rowExecer is the write subset shared by *sql.DB and *sql.Tx.
type rowExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
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
	res, err := s.db.Exec(`UPDATE accounts SET label=? WHERE id=? AND deleted_at IS NULL`, label, id)
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

func scanAccount(rows interface{ Scan(...any) error }) (Account, error) {
	var a Account
	var created int64
	if err := rows.Scan(&a.ID, &a.InstanceID, &a.Generation, &a.ConfigDir, &a.KeychainService, &a.KeychainAccount,
		&a.Label, &a.AccountUUID, &created); err != nil {
		return a, err
	}
	a.CreatedAt = time.Unix(created, 0)
	return a, nil
}

const accountCols = `id,instance_id,generation,config_dir,keychain_service,keychain_account,label,account_uuid,created_at`

const desiredAccountPredicate = `accounts.deleted_at IS NULL
	  AND NOT EXISTS (SELECT 1 FROM account_removals WHERE account_id=accounts.id)
	  AND NOT EXISTS (SELECT 1 FROM account_presentation_quarantines WHERE account_id=accounts.id)
	  AND NOT EXISTS (SELECT 1 FROM account_mutations WHERE account_id=accounts.id)
	  AND NOT EXISTS (
	    SELECT 1 FROM account_mutation_receipts
	    WHERE account_id=accounts.id AND publication_pending=1
	  )
	  AND NOT EXISTS (SELECT 1 FROM credential_operations WHERE account_id=accounts.id)
	  AND NOT EXISTS (SELECT 1 FROM credential_quarantines WHERE account_id=accounts.id)
	  AND NOT EXISTS (
	    SELECT 1 FROM pending_synced_credential_admissions
	    WHERE account_id=accounts.id
	  )
	  AND NOT EXISTS (
	    SELECT 1 FROM credential_operation_receipts
	    WHERE account_id=accounts.id AND publication_payload IS NOT NULL
	      AND acknowledged_at IS NULL
	  )`

// ListAccounts returns all accounts ordered by id.
func (s *Store) ListAccounts() ([]Account, error) {
	rows, err := s.db.Query(`SELECT ` + accountCols + ` FROM accounts WHERE deleted_at IS NULL ORDER BY id`)
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

// ListDesiredAccounts returns exact account generations eligible for live
// presentation convergence, whether or not a presentation is already bound.
func (s *Store) ListDesiredAccounts() ([]Account, error) {
	rows, err := s.db.Query(`SELECT ` + accountCols + ` FROM accounts
		WHERE ` + desiredAccountPredicate + `
		ORDER BY accounts.id`)
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

// ListActiveAccounts returns exactly presented accounts not fenced by durable
// removal, presentation quarantine, or unsettled credential publication.
func (s *Store) ListActiveAccounts() ([]Account, error) {
	rows, err := s.db.Query(`SELECT ` + accountCols + ` FROM accounts
		JOIN account_presentations ON account_presentations.account_id=accounts.id
		  AND account_presentations.account_instance_id=accounts.instance_id
		  AND account_presentations.account_generation=accounts.generation
		WHERE deleted_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM account_removals WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM account_presentation_quarantines WHERE account_id=accounts.id)
		  AND NOT EXISTS (
		    SELECT 1 FROM account_mutation_receipts
		    WHERE account_id=accounts.id AND publication_pending=1
		  )
		ORDER BY accounts.id`)
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

// ListPublishableOrigins returns accounts with exact presentation, ownership,
// and publication state suitable for host-sync origin publication.
func (s *Store) ListPublishableOrigins() ([]Account, error) {
	rows, err := s.db.Query(`SELECT ` + accountCols + ` FROM accounts
		JOIN account_presentations ON account_presentations.account_id=accounts.id
		  AND account_presentations.account_instance_id=accounts.instance_id
		  AND account_presentations.account_generation=accounts.generation
		LEFT JOIN auth_health ON auth_health.account_id=accounts.id
		WHERE deleted_at IS NULL
		  AND accounts.account_uuid<>''
		  AND COALESCE(auth_health.needs_login,0)=0
		  AND COALESCE(auth_health.kind,'owned')='owned'
		  AND NOT EXISTS (SELECT 1 FROM account_removals WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM account_presentation_quarantines WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM account_mutations WHERE account_id=accounts.id)
		  AND NOT EXISTS (
		    SELECT 1 FROM account_mutation_receipts
		    WHERE account_id=accounts.id AND publication_pending=1
		  )
		  AND NOT EXISTS (SELECT 1 FROM credential_operations WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM credential_quarantines WHERE account_id=accounts.id)
		  AND NOT EXISTS (
		    SELECT 1 FROM pending_synced_credential_admissions
		    WHERE account_id=accounts.id
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM credential_operation_receipts
		    WHERE account_id=accounts.id AND publication_payload IS NOT NULL
		      AND acknowledged_at IS NULL
		  )
		ORDER BY accounts.id`)
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

// ErrDuplicateAccountUUID rejects two live rows for one external Claude identity.
var ErrDuplicateAccountUUID = errors.New("duplicate external account UUID")

// ErrAccountSessionActive means removal cannot begin while a session is live.
var ErrAccountSessionActive = errors.New("account session is active")

// ErrAccountSelectionIneligible means an account lost a durable identity,
// presentation, health, or publication fence required to launch.
var ErrAccountSelectionIneligible = errors.New("account is not eligible for selection")

// GetAccount returns one account by id, wrapping ErrAccountNotFound when the
// row is absent.
func (s *Store) GetAccount(id int) (Account, error) {
	row := s.db.QueryRow(`SELECT `+accountCols+` FROM accounts WHERE id=? AND deleted_at IS NULL`, id)
	a, err := scanAccount(row)
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("account %d: %w", id, ErrAccountNotFound)
	}
	return a, err
}

// GetAccountByUUID returns the account whose Claude accountUuid is uuid,
// ok=false if none. An empty uuid never matches.
func (s *Store) GetAccountByUUID(uuid string) (Account, bool, error) {
	if uuid == "" {
		return Account{}, false, nil
	}
	row := s.db.QueryRow(`SELECT `+accountCols+` FROM accounts WHERE account_uuid=? AND deleted_at IS NULL ORDER BY id LIMIT 1`, uuid)
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
	rows, err := s.db.Query(`SELECT `+accountCols+` FROM accounts WHERE account_uuid=? AND deleted_at IS NULL ORDER BY id`, uuid)
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

// DeleteAccount tombstones an account after active and unacknowledged credential evidence clears.
func (s *Store) DeleteAccount(id int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`UPDATE accounts SET id=id WHERE id=? AND deleted_at IS NULL`, id); err != nil {
		return err
	}
	var evidence int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM account_mutations WHERE account_id=?)
		      OR EXISTS(SELECT 1 FROM account_mutation_receipts WHERE account_id=? AND acknowledged_at IS NULL)
		      OR EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?)
		      OR EXISTS(SELECT 1 FROM credential_operation_receipts WHERE account_id=? AND acknowledged_at IS NULL)
		      OR EXISTS(SELECT 1 FROM credential_quarantines WHERE account_id=?)`,
		id, id, id, id, id,
	).Scan(&evidence); err != nil {
		return err
	}
	if evidence != 0 {
		return ErrCredentialOperationEvidenceActive
	}
	for _, q := range []string{
		`DELETE FROM usage_samples WHERE account_id=?`,
		`DELETE FROM sessions WHERE account_id=?`,
		`DELETE FROM refresh_log WHERE account_id=?`,
		`DELETE FROM sticky WHERE account_id=?`,
		`DELETE FROM pending_synced_credential_admissions WHERE account_id=?`,
		`DELETE FROM synced_credential_admissions WHERE account_id=?`,
		`DELETE FROM auth_health WHERE account_id=?`,
		`DELETE FROM account_presentation_quarantines WHERE account_id=?`,
		`DELETE FROM account_presentations WHERE account_id=?`,
		`DELETE FROM account_removals WHERE account_id=?`,
	} {
		if _, err := tx.Exec(q, id); err != nil {
			return err
		}
	}
	result, err := tx.Exec(`UPDATE accounts SET deleted_at=? WHERE id=? AND deleted_at IS NULL`, s.now().UnixNano(), id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrAccountNotFound
	}
	return tx.Commit()
}

// BeginAccountRemoval durably fences an account before external deprovisioning.
func (s *Store) BeginAccountRemoval(id int, deleteCredential bool) (AccountRemoval, error) {
	if id <= 0 {
		return AccountRemoval{}, ErrAccountNotFound
	}
	tx, err := s.db.Begin()
	if err != nil {
		return AccountRemoval{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		`INSERT INTO account_registry_sequences(account_id,sequence) VALUES(?,0)
		 ON CONFLICT(account_id) DO NOTHING`, id,
	); err != nil {
		return AccountRemoval{}, err
	}
	if existing, err := accountRemovalByID(tx, id); err == nil {
		if existing.DeleteCredential != deleteCredential {
			return AccountRemoval{}, fmt.Errorf("account %d removal intent conflicts with current request", id)
		}
		if err := tx.Commit(); err != nil {
			return AccountRemoval{}, err
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AccountRemoval{}, err
	}
	var instanceID string
	var generation uint64
	err = tx.QueryRow(
		`SELECT instance_id,generation FROM accounts WHERE id=? AND deleted_at IS NULL`, id,
	).Scan(&instanceID, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRow(
			`SELECT instance_id,generation FROM pending_adds WHERE id=?`, id,
		).Scan(&instanceID, &generation)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return AccountRemoval{}, ErrAccountNotFound
	}
	if err != nil {
		return AccountRemoval{}, err
	}
	var activeSession, activeCredentialOperation int
	var unacknowledgedAccountMutation, unacknowledgedCredentialOperation, credentialQuarantine int
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM sessions WHERE account_id=? AND ended_at IS NULL),
		        EXISTS(SELECT 1 FROM credential_operations WHERE account_id=?),
		        EXISTS(SELECT 1 FROM account_mutation_receipts WHERE account_id=? AND acknowledged_at IS NULL),
		        EXISTS(SELECT 1 FROM credential_operation_receipts WHERE account_id=? AND acknowledged_at IS NULL),
		        EXISTS(SELECT 1 FROM credential_quarantines WHERE account_id=?)`,
		id, id, id, id, id,
	).Scan(
		&activeSession, &activeCredentialOperation,
		&unacknowledgedAccountMutation, &unacknowledgedCredentialOperation, &credentialQuarantine,
	); err != nil {
		return AccountRemoval{}, err
	}
	if activeSession != 0 {
		return AccountRemoval{}, ErrAccountSessionActive
	}
	if activeCredentialOperation != 0 {
		return AccountRemoval{}, ErrCredentialOperationBusy
	}
	if unacknowledgedAccountMutation != 0 || unacknowledgedCredentialOperation != 0 || credentialQuarantine != 0 {
		return AccountRemoval{}, ErrCredentialOperationEvidenceActive
	}
	if mutation, err := accountMutationByAccount(tx, id); err == nil {
		allowed, err := pendingAddMutationAllowsRemoval(tx, mutation)
		if err != nil {
			return AccountRemoval{}, err
		}
		if !allowed {
			return AccountRemoval{}, ErrAccountMutationBusy
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return AccountRemoval{}, err
	}
	deleteValue := 0
	if deleteCredential {
		deleteValue = 1
	}
	now := s.now()
	var sequence uint64
	if err := tx.QueryRow(
		`UPDATE account_registry_sequences SET sequence=sequence+1
		 WHERE account_id=? RETURNING sequence`, id,
	).Scan(&sequence); err != nil {
		return AccountRemoval{}, err
	}
	if _, err := tx.Exec(`INSERT INTO account_removals(
		account_id,account_instance_id,account_generation,registry_sequence,delete_credential,created_at
	) VALUES(?,?,?,?,?,?)`,
		id, instanceID, generation, sequence, deleteValue, now.UnixNano()); err != nil {
		return AccountRemoval{}, fmt.Errorf("begin account %d removal: %w", id, err)
	}
	removal, err := accountRemovalByID(tx, id)
	if err != nil {
		return AccountRemoval{}, err
	}
	if removal.AccountInstanceID != instanceID || removal.AccountGeneration != generation ||
		removal.RegistrySequence != sequence ||
		removal.DeleteCredential != deleteCredential {
		return AccountRemoval{}, fmt.Errorf("account %d removal intent conflicts with current request", id)
	}
	if err := tx.Commit(); err != nil {
		return AccountRemoval{}, err
	}
	return removal, nil
}

func pendingAddMutationAllowsRemoval(tx *sql.Tx, mutation AccountMutation) (bool, error) {
	if mutation.Kind != AccountMutationAdd ||
		(mutation.State != AccountMutationPublishing && mutation.State != AccountMutationCompensating) {
		return false, nil
	}
	err := validateAccountMutationSubject(tx, BeginAccountMutationRequest{
		AccountID: mutation.AccountID, Kind: mutation.Kind,
		AccountInstanceID: mutation.AccountInstanceID, AccountGeneration: mutation.AccountGeneration,
		Owner: mutation.Owner,
	})
	if errors.Is(err, ErrAccountGenerationChanged) {
		return false, nil
	}
	return err == nil, err
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

type selectionEligibilityQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func selectionEligible(
	queryer selectionEligibilityQueryer,
	accountID int,
	instanceID string,
	generation uint64,
	configDir string,
) error {
	var eligible int
	err := queryer.QueryRow(`SELECT 1 FROM accounts
		JOIN account_presentations ON account_presentations.account_id=accounts.id
		  AND account_presentations.account_instance_id=accounts.instance_id
		  AND account_presentations.account_generation=accounts.generation
		WHERE accounts.id=? AND accounts.instance_id=? AND accounts.generation=?
		  AND accounts.config_dir=? AND accounts.deleted_at IS NULL
		  AND NOT EXISTS (SELECT 1 FROM account_removals WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM account_presentation_quarantines WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM account_mutations WHERE account_id=accounts.id)
		  AND NOT EXISTS (
		    SELECT 1 FROM account_mutation_receipts
		    WHERE account_id=accounts.id AND publication_pending=1
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM auth_health
		    WHERE account_id=accounts.id AND (needs_login=1 OR kind<>'owned')
		  )
		  AND NOT EXISTS (SELECT 1 FROM credential_operations WHERE account_id=accounts.id)
		  AND NOT EXISTS (SELECT 1 FROM credential_quarantines WHERE account_id=accounts.id)
		  AND NOT EXISTS (
		    SELECT 1 FROM pending_synced_credential_admissions
		    WHERE account_id=accounts.id
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM credential_operation_receipts
		    WHERE account_id=accounts.id AND publication_payload IS NOT NULL
		      AND acknowledged_at IS NULL
		  )`, accountID, instanceID, generation, configDir).Scan(&eligible)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAccountSelectionIneligible
	}
	return err
}

// SelectionEligible verifies the exact durable fences required before tenant
// preparation or credential preflight starts.
func (s *Store) SelectionEligible(account Account) error {
	return selectionEligible(s.db, account.ID, account.InstanceID, account.Generation, account.ConfigDir)
}

// ErrAccountGenerationChanged means a reserved account was replaced or its
// tenant-defining shape changed before activation.
var ErrAccountGenerationChanged = errors.New("account generation changed")

// ErrSessionLeaseConflict means an exact session lease receipt or renewal fence changed.
var ErrSessionLeaseConflict = errors.New("session File Provider lease changed")

func validateFileProviderLeaseReceipt(receipt FileProviderLeaseReceipt) error {
	if len(receipt) == 0 || len(receipt) > 64*1024 {
		return errors.New("File Provider lease receipt must contain 1..65536 bytes")
	}
	return nil
}

func cloneFileProviderLeaseReceipt(receipt FileProviderLeaseReceipt) FileProviderLeaseReceipt {
	return append(FileProviderLeaseReceipt(nil), receipt...)
}

// StageSelection atomically records the exact provisional File Provider lease
// before external lease promotion begins.
func (s *Store) StageSelection(a SelectionActivation) (_ Session, err error) {
	if err := validateSelectionToken(a.Token); err != nil {
		return Session{}, fmt.Errorf("stage selection: %w", err)
	}
	if a.Process.PID <= 0 {
		return Session{}, errors.New("stage selection: positive process pid is required")
	}
	if a.Process.StartedAt.IsZero() {
		return Session{}, errors.New("stage selection: process start time is required")
	}
	if a.ConfigDir == "" {
		return Session{}, errors.New("stage selection: config dir is required")
	}
	if err := validateFileProviderLeaseReceipt(a.FileProviderLease); err != nil {
		return Session{}, fmt.Errorf("stage selection: %w", err)
	}
	if a.LeaseExpiresAt.IsZero() || a.LeaseExpiresAt.UnixNano() <= 0 {
		return Session{}, errors.New("stage selection: lease expiry is required")
	}
	if a.At.IsZero() {
		a.At = s.now()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Session{}, fmt.Errorf("begin selection staging: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	terminalNow := s.now()
	if err = pruneSelectionTerminals(tx, terminalNow); err != nil {
		return Session{}, fmt.Errorf("prune selection terminals: %w", err)
	}
	var terminalAccountID int
	var terminalInstanceID string
	var terminalGeneration uint64
	var terminalProvisional []byte
	err = tx.QueryRow(
		`SELECT account_id,account_instance_id,account_generation,file_provider_provisional_lease_receipt FROM selection_terminals WHERE token=?`, a.Token,
	).Scan(&terminalAccountID, &terminalInstanceID, &terminalGeneration, &terminalProvisional)
	if err == nil {
		if terminalAccountID != a.AccountID || terminalInstanceID != a.ExpectedInstanceID || terminalGeneration != a.ExpectedGeneration {
			return Session{}, fmt.Errorf("stage selection: token %s belongs to account %d %s/%d", a.Token,
				terminalAccountID, terminalInstanceID, terminalGeneration)
		}
		if !bytes.Equal(terminalProvisional, a.FileProviderLease) {
			return Session{}, fmt.Errorf("stage selection: token %s: %w", a.Token, ErrSessionLeaseConflict)
		}
		return sessionBySelectionToken(tx, a.Token)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Session{}, fmt.Errorf("read selection terminal: %w", err)
	}
	staged, err := sessionBySelectionToken(tx, a.Token)
	if err == nil {
		if staged.AccountID != a.AccountID || staged.AccountInstanceID != a.ExpectedInstanceID ||
			staged.AccountGeneration != a.ExpectedGeneration || staged.PID != a.Process.PID ||
			!staged.ProcessStartedAt.Equal(a.Process.StartedAt) || staged.ConfigDir != a.ConfigDir ||
			!bytes.Equal(staged.FileProviderLease, a.FileProviderLease) ||
			!staged.LeaseExpiresAt.Equal(a.LeaseExpiresAt) {
			return Session{}, ErrSessionLeaseConflict
		}
		return staged, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Session{}, err
	}
	var instanceID string
	var generation uint64
	if err = tx.QueryRow(
		`SELECT instance_id,generation FROM accounts WHERE id=? AND deleted_at IS NULL`, a.AccountID,
	).Scan(&instanceID, &generation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, fmt.Errorf("stage selection account %d: %w", a.AccountID, ErrAccountNotFound)
		}
		return Session{}, fmt.Errorf("stage selection account %d: %w", a.AccountID, err)
	}
	if instanceID != a.ExpectedInstanceID || generation != a.ExpectedGeneration {
		return Session{}, fmt.Errorf("%w: account=%d reserved=%s/%d current=%s/%d", ErrAccountGenerationChanged,
			a.AccountID, a.ExpectedInstanceID, a.ExpectedGeneration, instanceID, generation)
	}
	if _, removalErr := accountRemovalByID(tx, a.AccountID); removalErr == nil {
		return Session{}, fmt.Errorf("stage selection account %d: %w", a.AccountID, ErrAccountRemoving)
	} else if !errors.Is(removalErr, sql.ErrNoRows) {
		return Session{}, fmt.Errorf("read selection removal for account %d: %w", a.AccountID, removalErr)
	}
	if err = selectionEligible(
		tx, a.AccountID, a.ExpectedInstanceID, a.ExpectedGeneration, a.ConfigDir,
	); err != nil {
		return Session{}, fmt.Errorf("stage selection account %d: %w", a.AccountID, err)
	}
	result, err := tx.Exec(
		`INSERT INTO sessions(selection_token,account_id,account_instance_id,account_generation,pid,process_started_at,config_dir,cwd,started_at,
		 file_provider_lease_state,file_provider_lease_receipt,file_provider_lease_expires_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		a.Token, a.AccountID, instanceID, generation, a.Process.PID, a.Process.StartedAt.UnixMicro(), a.ConfigDir, a.Cwd,
		a.At.Unix(), SessionLeasePending, []byte(a.FileProviderLease), a.LeaseExpiresAt.UTC().UnixNano())
	if err != nil {
		return Session{}, fmt.Errorf("stage selection session for account %d: %w", a.AccountID, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Session{}, err
	}
	if err = tx.Commit(); err != nil {
		return Session{}, fmt.Errorf("commit selection staging: %w", err)
	}
	return Session{
		ID: id, SelectionToken: a.Token, AccountID: a.AccountID, AccountInstanceID: instanceID,
		AccountGeneration: generation, PID: a.Process.PID, ProcessStartedAt: a.Process.StartedAt,
		ConfigDir: a.ConfigDir, Cwd: a.Cwd, StartedAt: a.At, LeaseState: SessionLeasePending,
		FileProviderLease: cloneFileProviderLeaseReceipt(a.FileProviderLease), LeaseExpiresAt: a.LeaseExpiresAt.UTC(),
	}, nil
}

// CommitSelection atomically promotes one staged session after FuseKit returns
// the exact committed lease receipt.
func (s *Store) CommitSelection(
	token string,
	provisional FileProviderLeaseReceipt,
	committed FileProviderLeaseReceipt,
	recordSticky bool,
	at time.Time,
) (err error) {
	if err := validateSelectionToken(token); err != nil {
		return err
	}
	if err := validateFileProviderLeaseReceipt(provisional); err != nil {
		return err
	}
	if err := validateFileProviderLeaseReceipt(committed); err != nil {
		return err
	}
	if at.IsZero() {
		at = s.now()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	terminalNow := s.now()
	if err = pruneSelectionTerminals(tx, terminalNow); err != nil {
		return err
	}
	var terminalProvisional, terminalCommitted []byte
	err = tx.QueryRow(
		`SELECT file_provider_provisional_lease_receipt,file_provider_committed_lease_receipt FROM selection_terminals WHERE token=?`, token,
	).Scan(&terminalProvisional, &terminalCommitted)
	if err == nil {
		if bytes.Equal(terminalProvisional, provisional) && bytes.Equal(terminalCommitted, committed) {
			return nil
		}
		return ErrSessionLeaseConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	staged, err := sessionBySelectionToken(tx, token)
	if err != nil {
		return err
	}
	if staged.LeaseState != SessionLeasePending || !bytes.Equal(staged.FileProviderLease, provisional) {
		return ErrSessionLeaseConflict
	}
	result, err := tx.Exec(
		`UPDATE sessions SET file_provider_lease_state=?,file_provider_lease_receipt=?
		 WHERE id=? AND ended_at IS NULL AND file_provider_lease_state=? AND file_provider_lease_receipt=?`,
		SessionLeaseActive, []byte(committed), staged.ID, SessionLeasePending, []byte(provisional),
	)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrSessionLeaseConflict
	}
	if recordSticky && staged.Cwd != "" {
		if _, err = tx.Exec(upsertStickySQL, staged.Cwd, staged.AccountID, at.Unix()); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(
		`INSERT INTO selection_terminals(token,account_id,account_instance_id,account_generation,
		 file_provider_provisional_lease_receipt,file_provider_committed_lease_receipt,committed_at,expires_at)
		 VALUES(?,?,?,?,?,?,?,?)`, token, staged.AccountID, staged.AccountInstanceID, staged.AccountGeneration,
		[]byte(provisional), []byte(committed), terminalNow.Unix(), terminalNow.Add(selectionTerminalTTL).Unix(),
	); err != nil {
		return err
	}
	if err = pruneSelectionTerminals(tx, terminalNow); err != nil {
		return err
	}
	return tx.Commit()
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

const activeSessionColumns = `id,selection_token,account_id,account_instance_id,account_generation,
pid,process_started_at,config_dir,cwd,started_at,file_provider_lease_state,
file_provider_lease_receipt,file_provider_lease_expires_at,lease_renewal_expires_at,last_seen_at`

type sessionScanner interface {
	Scan(...any) error
}

func scanSession(row sessionScanner) (Session, error) {
	var session Session
	var processStarted, started, leaseExpires int64
	var leaseReceipt []byte
	var renewalExpires, seen sql.NullInt64
	err := row.Scan(
		&session.ID, &session.SelectionToken, &session.AccountID, &session.AccountInstanceID,
		&session.AccountGeneration, &session.PID, &processStarted, &session.ConfigDir, &session.Cwd,
		&started, &session.LeaseState, &leaseReceipt, &leaseExpires, &renewalExpires, &seen,
	)
	if err != nil {
		return Session{}, err
	}
	session.ProcessStartedAt = time.UnixMicro(processStarted)
	session.StartedAt = time.Unix(started, 0)
	session.FileProviderLease = cloneFileProviderLeaseReceipt(leaseReceipt)
	session.LeaseExpiresAt = time.Unix(0, leaseExpires).UTC()
	if renewalExpires.Valid {
		value := time.Unix(0, renewalExpires.Int64).UTC()
		session.LeaseRenewalExpiresAt = &value
	}
	if seen.Valid {
		value := time.Unix(seen.Int64, 0)
		session.LastSeenAt = &value
	}
	return session, nil
}

type sessionQueryer interface {
	QueryRow(string, ...any) *sql.Row
}

func sessionBySelectionToken(queryer sessionQueryer, token string) (Session, error) {
	return scanSession(queryer.QueryRow(
		`SELECT `+activeSessionColumns+` FROM sessions WHERE selection_token=? AND ended_at IS NULL`, token,
	))
}

// ActiveSessionCount returns the number of live sessions for an account.
func (s *Store) ActiveSessionCount(accountID int) (int, error) {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM sessions WHERE account_id=? AND ended_at IS NULL`, accountID).Scan(&n)
	return n, err
}

// ActiveSessionTotal returns the number of live sessions across the pool.
func (s *Store) ActiveSessionTotal() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM sessions WHERE ended_at IS NULL`).Scan(&n)
	return n, err
}

// ListActiveSessions returns all live sessions across accounts.
func (s *Store) ListActiveSessions() ([]Session, error) {
	rows, err := s.db.Query(
		`SELECT ` + activeSessionColumns + ` FROM sessions WHERE ended_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Session
	for rows.Next() {
		se, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, se)
	}
	return out, rows.Err()
}

// SessionReapGrace bounds how long the exact pre-exec ccp process may remain
// live without yet appearing as Claude.
const SessionReapGrace = time.Minute

func (s *Store) touchSession(id int64, at time.Time) error {
	_, err := s.db.Exec(`UPDATE sessions SET last_seen_at=? WHERE id=? AND ended_at IS NULL`,
		at.Unix(), id)
	return err
}

// ReconcileSessions partitions sessions against one atomic process snapshot.
// An exact Claude identity is touched. An exact live non-Claude identity is
// retained only during SessionReapGrace; an absent or reused identity closes
// only after its File Provider lease is released by the caller.
func (s *Store) ReconcileSessions(claude, processes map[int]time.Time, at time.Time) (SessionReconciliation, error) {
	sessions, err := s.ListActiveSessions()
	if err != nil {
		return SessionReconciliation{}, err
	}
	var result SessionReconciliation
	for _, se := range sessions {
		if se.LeaseState == SessionLeaseReleased ||
			(se.LeaseState == SessionLeasePending && !at.Before(se.LeaseExpiresAt)) {
			result.Dead = append(result.Dead, se)
			continue
		}
		if started, ok := claude[se.PID]; ok && started.Equal(se.ProcessStartedAt) {
			if err := s.touchSession(se.ID, at); err != nil {
				return SessionReconciliation{}, err
			}
			seen := at
			se.LastSeenAt = &seen
			result.Live = append(result.Live, se)
			continue
		}
		if started, ok := processes[se.PID]; ok && started.Equal(se.ProcessStartedAt) &&
			at.Sub(se.StartedAt) < SessionReapGrace {
			result.Live = append(result.Live, se)
			continue
		}
		result.Dead = append(result.Dead, se)
	}
	return result, nil
}

// PlanSessionLeaseRenewal durably chooses one exact renewal expiry. Retries
// return the existing request until CompleteSessionLeaseRenewal settles it.
func (s *Store) PlanSessionLeaseRenewal(id int64, receipt FileProviderLeaseReceipt, expires time.Time) (_ time.Time, err error) {
	if err := validateFileProviderLeaseReceipt(receipt); err != nil {
		return time.Time{}, err
	}
	if expires.IsZero() {
		return time.Time{}, errors.New("session lease renewal expiry is required")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var current []byte
	var state SessionLeaseState
	var pending sql.NullInt64
	err = tx.QueryRow(
		`SELECT file_provider_lease_state,file_provider_lease_receipt,lease_renewal_expires_at FROM sessions WHERE id=? AND ended_at IS NULL`, id,
	).Scan(&state, &current, &pending)
	if errors.Is(err, sql.ErrNoRows) || state != SessionLeaseActive || !bytes.Equal(current, receipt) {
		return time.Time{}, ErrSessionLeaseConflict
	}
	if err != nil {
		return time.Time{}, err
	}
	if pending.Valid {
		return time.Unix(0, pending.Int64).UTC(), nil
	}
	expires = expires.UTC()
	if _, err = tx.Exec(
		`UPDATE sessions SET lease_renewal_expires_at=? WHERE id=? AND ended_at IS NULL
		 AND file_provider_lease_state=? AND file_provider_lease_receipt=? AND lease_renewal_expires_at IS NULL`,
		expires.UnixNano(), id, SessionLeaseActive, []byte(receipt),
	); err != nil {
		return time.Time{}, err
	}
	if err = tx.Commit(); err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

// CompleteSessionLeaseRenewal atomically replaces the exact receipt and clears
// the matching pending renewal. Replaying an already completed response succeeds.
func (s *Store) CompleteSessionLeaseRenewal(
	id int64,
	previous FileProviderLeaseReceipt,
	renewed FileProviderLeaseReceipt,
	expires time.Time,
) error {
	if err := validateFileProviderLeaseReceipt(previous); err != nil {
		return err
	}
	if err := validateFileProviderLeaseReceipt(renewed); err != nil {
		return err
	}
	if expires.IsZero() {
		return errors.New("session lease renewal expiry is required")
	}
	result, err := s.db.Exec(
		`UPDATE sessions SET file_provider_lease_receipt=?,file_provider_lease_expires_at=?,lease_renewal_expires_at=NULL
		 WHERE id=? AND ended_at IS NULL AND file_provider_lease_state=?
		 AND file_provider_lease_receipt=? AND lease_renewal_expires_at=?`,
		[]byte(renewed), expires.UTC().UnixNano(), id, SessionLeaseActive, []byte(previous), expires.UTC().UnixNano(),
	)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil {
		return err
	} else if changed == 1 {
		return nil
	}
	var current []byte
	var pending sql.NullInt64
	err = s.db.QueryRow(
		`SELECT file_provider_lease_receipt,lease_renewal_expires_at FROM sessions WHERE id=? AND ended_at IS NULL`, id,
	).Scan(&current, &pending)
	if err == nil && bytes.Equal(current, renewed) && !pending.Valid {
		return nil
	}
	return ErrSessionLeaseConflict
}

// CompleteSessionLeaseRelease records the exact released receipt and only then
// closes the session. Replaying the same release succeeds.
func (s *Store) CompleteSessionLeaseRelease(
	id int64,
	previous FileProviderLeaseReceipt,
	released FileProviderLeaseReceipt,
	at time.Time,
) error {
	if err := validateFileProviderLeaseReceipt(previous); err != nil {
		return err
	}
	if err := validateFileProviderLeaseReceipt(released); err != nil {
		return err
	}
	if at.IsZero() {
		return errors.New("session lease close time is required")
	}
	result, err := s.db.Exec(
		`UPDATE sessions SET file_provider_lease_state=?,file_provider_lease_receipt=?,ended_at=?
		 WHERE id=? AND ended_at IS NULL AND file_provider_lease_receipt=?`,
		SessionLeaseReleased, []byte(released), at.Unix(), id, []byte(previous),
	)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil {
		return err
	} else if changed == 1 {
		return nil
	}
	var current []byte
	var state SessionLeaseState
	var ended sql.NullInt64
	err = s.db.QueryRow(`SELECT file_provider_lease_state,file_provider_lease_receipt,ended_at FROM sessions WHERE id=?`, id).Scan(&state, &current, &ended)
	if err == nil && state == SessionLeaseReleased && bytes.Equal(current, released) && ended.Valid {
		return nil
	}
	return ErrSessionLeaseConflict
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

// LogRefresh records a typed refresh outcome and an opaque error digest.
func (s *Store) LogRefresh(accountID int, category RefreshCategory, digest [32]byte) error {
	_, err := s.db.Exec(
		`INSERT INTO refresh_log(account_id,ts,category,digest) VALUES(?,?,?,?)
		 ON CONFLICT(account_id,ts) DO NOTHING`,
		accountID, time.Now().Unix(), category, digest[:])
	return err
}

// LastRefresh returns the most recent refresh attempt for an account, ok=false
// if none.
func (s *Store) LastRefresh(accountID int) (RefreshEntry, bool, error) {
	row := s.db.QueryRow(
		`SELECT account_id,ts,category,digest FROM refresh_log WHERE account_id=? ORDER BY ts DESC LIMIT 1`, accountID)
	var e RefreshEntry
	var ts int64
	var digest []byte
	if err := row.Scan(&e.AccountID, &ts, &e.Category, &digest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e, false, nil
		}
		return e, false, err
	}
	if len(digest) != len(e.Digest) {
		return e, false, errors.New("refresh log digest has invalid length")
	}
	copy(e.Digest[:], digest)
	e.TS = time.Unix(ts, 0)
	return e, true, nil
}

// GetAuthHealth returns an account's auth health. An account with no row reads
// as healthy (NeedsLogin false).
func (s *Store) GetAuthHealth(accountID int) (AuthHealth, error) {
	row := s.db.QueryRow(
		`SELECT account_id,needs_login,since,reason,digest,kind,gen FROM auth_health WHERE account_id=?`, accountID)
	var h AuthHealth
	var needs int
	var since sql.NullInt64
	var digest []byte
	var kind string
	if err := row.Scan(&h.AccountID, &needs, &since, &h.Reason, &digest, &kind, &h.Gen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return AuthHealth{AccountID: accountID, Kind: AuthKindOwned}, nil
		}
		return AuthHealth{}, fmt.Errorf("get auth health for account %d: %w", accountID, err)
	}
	if len(digest) != len(h.Digest) {
		return AuthHealth{}, errors.New("auth health digest has invalid length")
	}
	copy(h.Digest[:], digest)
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
	rows, err := s.db.Query(`SELECT account_id,needs_login,since,reason,digest,kind,gen FROM auth_health WHERE needs_login=1`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[int]AuthHealth{}
	for rows.Next() {
		var h AuthHealth
		var needs int
		var since sql.NullInt64
		var digest []byte
		var kind string
		if err := rows.Scan(&h.AccountID, &needs, &since, &h.Reason, &digest, &kind, &h.Gen); err != nil {
			return nil, err
		}
		if len(digest) != len(h.Digest) {
			return nil, errors.New("auth health digest has invalid length")
		}
		copy(h.Digest[:], digest)
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
func (s *Store) SetNeedsLogin(
	accountID int,
	at time.Time,
	reason AuthReasonCategory,
	digest [32]byte,
	kind AuthKind,
) (bool, error) {
	if !kind.Valid() || !reason.Valid() || reason == AuthReasonNone ||
		(kind == AuthKindAwaitingOrigin) != (reason == AuthReasonAwaitingOrigin) {
		return false, fmt.Errorf("set needs-login for account %d: invalid auth kind %q", accountID, kind)
	}
	prev, err := s.GetAuthHealth(accountID)
	if err != nil {
		return false, err
	}
	result, err := s.db.Exec(
		`INSERT INTO auth_health(account_id,needs_login,since,reason,digest,kind,gen) VALUES(?,1,?,?,?,?,1)
		 ON CONFLICT(account_id) DO UPDATE SET
		   needs_login=1,
		   reason=excluded.reason,
		   digest=excluded.digest,
		   kind=excluded.kind,
		   since=CASE WHEN auth_health.needs_login=1 THEN auth_health.since ELSE excluded.since END,
		   gen=auth_health.gen+1
		 WHERE auth_health.kind<>'awaiting_origin' OR excluded.kind='awaiting_origin'`,
		accountID, at.Unix(), reason, digest[:], string(kind))
	if err != nil {
		return false, fmt.Errorf("set needs-login for account %d: %w", accountID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows == 0 {
		current, readErr := s.GetAuthHealth(accountID)
		if readErr != nil {
			return false, readErr
		}
		if current.NeedsLogin && current.Kind == AuthKindAwaitingOrigin && kind != AuthKindAwaitingOrigin {
			return false, ErrAwaitingOriginAdmission
		}
		return false, errors.New("set needs-login changed no rows")
	}
	return !prev.NeedsLogin, nil
}

// ClearNeedsLogin clears an account's needs-login flag, returning changed=true
// only on the true→false transition, so the daemon logs recovery exactly once.
func (s *Store) ClearNeedsLogin(accountID int) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE auth_health SET needs_login=0, since=NULL, reason='none', digest=zeroblob(32), kind='owned'
		 WHERE account_id=? AND needs_login=1 AND kind<>'awaiting_origin'`,
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
		`UPDATE auth_health SET needs_login=0, since=NULL, reason='none', digest=zeroblob(32), kind='owned'
		 WHERE account_id=? AND needs_login=1 AND gen=? AND kind<>'awaiting_origin'`,
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
