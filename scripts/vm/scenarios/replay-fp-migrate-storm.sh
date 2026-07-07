# shellcheck shell=bash
# scenarios/replay-fp-migrate-storm.sh — sourced by `vmctl run`; lib.sh is
# already loaded and `set -euo pipefail` is inherited.
#
# Replays the 2026-07-05 File Provider wedge incident (acct-12 plan, § How this
# happened): a 10-account pool on symlink overlays, live read pressure, then the
# exact fleet command `ccp migrate --to fileprovider --force`. On the FIXED
# build it must settle cleanly — the daemon converts account-by-account behind
# the readiness gate (fusekit WaitDomainServes), the Swift per-domain claims
# never bounce a cross-account op busy, and every domain serves reads once its
# row flips. On a regressed build the migrated domains materialize
# simultaneously, fileproviderd wedges, and the Swift global gate mints
# "domain … is busy with another operation" — which this scenario fails on.
#
# EXPECT=clean: a clean settle inside the run window exits 0; any failed
# assertion exits nonzero and vmctl maps it to an infra/workload failure (1); a
# kernel panic (the shared macOS overlay stack can still panic) maps to 2.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

# --- Guest paths (host-computed strings, executed guest-side via vm_ssh) ----------
GUEST_STATE="$VM_GUEST_HOME/.cc-pool"
GUEST_ACCTS="$GUEST_STATE/accounts"
GUEST_DB="$GUEST_STATE/pool.db"
GUEST_DAEMON_SOCK="$GUEST_STATE/daemon.sock"
GUEST_FP_CONTROL_SOCK="$GUEST_STATE/domains.sock"
# The App Group container the daemon binds the FP bridge socket in. The group id
# is pool.AppGroupID (SXKCTF23Q2.ccp); push.sh asserts it matches the signed appex.
GUEST_FP_BRIDGE_SOCK="$VM_GUEST_HOME/Library/Group Containers/SXKCTF23Q2.ccp/b.sock"
GUEST_READERS_STOP="/tmp/ccpool-readers-stop"
NACCTS=10

# require_seconds dies if the run window has already elapsed before a phase.
require_seconds() {
  (($(vm_seconds_left) > 0)) || die "run window elapsed before $1 — raise VMCTL_RUN_TIMEOUT_MIN"
}

# fp_consent_die reports the File Provider provider-consent blocker and dies. The
# CCPoolFileProvider extension can be pluginkit-enabled ('+') yet still sit
# user-disabled in fileproviderd — its Domains.plist NSFileProviderDomainDefaultIdentifier:Enabled
# is false, the per-provider consent gate that pluginkit does NOT flip. Until it is
# granted every domain registers but never serves (FP -2011 domainDisabled), so the
# daemon's File Provider capability gate refuses ("extension enabled but not serving")
# before any account converts. On a real machine the user grants it in System
# Settings; in this harness the push-time fp_grant_consent step (lib.sh) flips the
# plist boolean directly. $* is the observed evidence.
fp_consent_die() {
  die "File Provider provider consent is NOT granted (fileproviderd has the provider user-disabled).
pluginkit enablement ('+') is insufficient — this is the separate per-provider consent gate.
Fix (harness): re-run 'scripts/vm/vmctl push' — its fp_grant_consent step flips the
Domains.plist NSFileProviderDomainDefaultIdentifier:Enabled boolean and kickstarts
fileproviderd. (On a real machine: System Settings > General > Login Items &
Extensions > File Provider, turn ON the cc-pool (CCPoolStatus) extension.)
Evidence: $*"
}

# ---------------------------------------------------------------------------------
# 1. Seed a synthetic 10-account pool: config dirs + fake .claude.json identities
#    + matching sqlite rows on the symlink overlay. Migration never touches the
#    Keychain, so no real logins are needed.
# ---------------------------------------------------------------------------------
vm_phase seed
require_seconds "seed"

# Reset for idempotency (no destroy/create cycle needed): stop any daemon / app /
# readers left by a prior run so the seed can wipe pool state without racing a
# live writer. The seed script then wipes ~/.cc-pool's pool state and recreates it.
vm_ssh "touch '$GUEST_READERS_STOP' 2>/dev/null; pkill -x cc-pool 2>/dev/null; osascript -e 'quit app \"CCPoolStatus\"' 2>/dev/null; pkill -f CCPoolStatus 2>/dev/null; true"
sleep 3

cat >"$VMCTL_RESULTS_DIR/seed.sh" <<'SEED'
set -euo pipefail
N=10
STATE="$HOME/.cc-pool"
ACCTS="$STATE/accounts"
DB="$STATE/pool.db"
# Full reset for idempotency: wipe prior pool state — the DB and the account
# dirs, which include the acct-NN.private fuse/FP backing roots
# (fkoverlay.FusePrivateRoot = accountDir + ".private") — so a re-run starts from
# a clean symlink pool. The daemon/app were stopped by the scenario's reset
# preamble. OS-registered FP domains from a prior successful run persist in
# fileproviderd and are re-registered (idempotent add) by the next migrate.
rm -rf "$ACCTS" "$DB" "$DB-wal" "$DB-shm"
mkdir -p "$ACCTS"
chmod 700 "$STATE"
# A base ~/.claude tree + ~/.claude.json so the symlink overlay has a base and
# the domain-served MERGED .claude.json has something to merge onto.
mkdir -p "$HOME/.claude"
[ -f "$HOME/.claude.json" ] || printf '{}\n' >"$HOME/.claude.json"

# Schema mirrors internal/store/store.go — we create the meta + accounts tables
# (the ones we seed); cc-pool's store.Open() creates every other table via
# CREATE TABLE IF NOT EXISTS on first daemon start.
sqlite3 "$DB" <<'SQL'
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts (
  id               INTEGER PRIMARY KEY,
  config_dir       TEXT NOT NULL UNIQUE,
  keychain_service TEXT NOT NULL,
  keychain_account TEXT NOT NULL,
  label            TEXT NOT NULL DEFAULT '',
  overlay_kind     TEXT NOT NULL DEFAULT 'symlink',
  created_at       INTEGER NOT NULL
);
SQL

# The pool-initialized markers pool.Init writes (internal/pool/account.go): the
# `initialized` meta row gates every daemon op (cli.requireInit), and
# `overlay_kind`=symlink is the new-account default the pre-migrate pool starts
# on. Without `initialized`, `ccp migrate` fails "pool not initialized".
sqlite3 "$DB" "INSERT OR REPLACE INTO meta (key,value) VALUES ('initialized','1'),('overlay_kind','symlink');"

now=$(date +%s)
for i in $(seq 1 "$N"); do
  nn=$(printf '%02d' "$i")
  dir="$ACCTS/acct-$nn"
  mkdir -p "$dir"
  # A real oauthAccount identity (accountUuid is what pool.readIdentity requires;
  # a distinct email per account so the domain-served merge is verifiable).
  uuid="00000000-0000-4000-8000-0000000000$nn"
  printf '{"oauthAccount":{"accountUuid":"%s","emailAddress":"acct-%s@ccpool.test"}}\n' \
    "$uuid" "$nn" >"$dir/.claude.json"
  # keychain_* are placeholders: migration never reads them, and the poll loop's
  # cred lookup failing is harmless noise (it never emits an FP storm line).
  sqlite3 "$DB" \
    "INSERT INTO accounts (id,config_dir,keychain_service,keychain_account,label,overlay_kind,created_at)
     VALUES ($i,'$dir','ccp-vm-replay-$nn','acct-$nn','vm-$nn','symlink',$now);"
done
echo "seeded $(sqlite3 "$DB" 'SELECT COUNT(*) FROM accounts;') accounts on symlink"
SEED

vm_scp_to "$VMCTL_RESULTS_DIR/seed.sh" "/tmp/ccpool-seed.sh" || die "could not stage the seed script"
vm_ssh "bash /tmp/ccpool-seed.sh" || die "seeding failed"
# Confirm the store really holds 10 symlink rows before proceeding.
seeded="$(vm_ssh "sqlite3 '$GUEST_DB' \"SELECT COUNT(*) FROM accounts WHERE overlay_kind='symlink';\"")" || die "could not read back seeded rows"
[[ "$seeded" == "$NACCTS" ]] || die "expected $NACCTS symlink rows seeded, store has '$seeded'"
log "seeded $seeded symlink accounts"

# ---------------------------------------------------------------------------------
# 2. Launch the companion app (serves the FP control socket) and start the daemon
#    (serves daemon.sock, binds the FP bridge socket in the App Group container).
# ---------------------------------------------------------------------------------
vm_phase daemon-start
require_seconds "daemon-start"

# Clear any daemon/app from a prior run so we watch THIS build's sockets/log.
vm_ssh "pkill -x cc-pool 2>/dev/null; osascript -e 'quit app \"CCPoolStatus\"' 2>/dev/null; pkill -f CCPoolStatus 2>/dev/null; rm -f '$GUEST_READERS_STOP'; true"

log "launching $VMCTL_GUEST_APP (File Provider control server)"
vm_ssh "open -g '$VMCTL_GUEST_APP'" || die "failed to launch $VMCTL_GUEST_APP (open needs a logged-in GUI session; on a headless boot use VMCTL_GRAPHICS=1 — README: FP provisioning)"

log "starting the daemon (log: $VMCTL_GUEST_DAEMON_LOG)"
vm_ssh "rm -f '$VMCTL_GUEST_DAEMON_LOG'; nohup '$VMCTL_GUEST_CCPOOL' daemon >'$VMCTL_GUEST_DAEMON_LOG' 2>&1 </dev/null & echo started" \
  || die "failed to start the daemon"

vm_wait_guest_path "$GUEST_DAEMON_SOCK" 60 \
  || die "daemon socket never came up — the daemon failed to start (fetch $VMCTL_GUEST_DAEMON_LOG)"
vm_wait_guest_path "$GUEST_FP_CONTROL_SOCK" 60 \
  || die "FP control socket never came up — the CCPoolStatus app is not serving (launch or entitlement issue)"
# The daemon's first bind of the bridge socket inside the App Group container is
# the app-group-data TCC gate. If it never appears, that grant is missing.
vm_wait_guest_path "$GUEST_FP_BRIDGE_SOCK" 120 \
  || die "FP bridge socket in the App Group container never came up — the daemon's group-container bind was denied.
This is the app-group-data TCC gate: re-run 'vmctl provision' with the grant, or boot VMCTL_GRAPHICS=1 and click Allow when the daemon first binds (README: FP provisioning).
Daemon log: $VMCTL_GUEST_DAEMON_LOG"
log "daemon + companion app up; FP bridge bound"

# Preflight the File Provider provider-consent gate. The AUTHORITATIVE signal is
# fileproviderd's per-provider consent boolean in the provider's Domains.plist
# (NSFileProviderDomainDefaultIdentifier:Enabled) — the same bit the push-time
# fp_grant_consent step (lib.sh) flips. `fileproviderctl dump`'s `enabled:` line is
# a known-unreliable zero-domain default (it reads `no` even when the provider will
# serve), kept here as secondary color only. A disabled provider fails every probe
# with FP -2011 (domainDisabled), so the migrate's capability gate refuses before any
# account converts: fail fast here with the fp_grant_consent remediation. `unknown`
# (no plist yet — a first-ever run before the appex was elected) defers to the
# migrate, whose gate-failure detector below carries the same remediation.
cat >"$VMCTL_RESULTS_DIR/fpcheck.sh" <<'FPCHECK'
set -u
BUNDLE=com.yasyf.cc-pool.status.fileprovider
PL="$HOME/Library/Application Support/FileProvider/$BUNDLE/Domains.plist"
# Authoritative: the provider-consent boolean. true=yes, false=no, absent=unknown.
v="$(/usr/libexec/PlistBuddy -c 'Print :NSFileProviderDomainDefaultIdentifier:Enabled' "$PL" 2>/dev/null | tr -d '[:space:]')"
case "$v" in
  true)  plist=yes ;;
  false) plist=no ;;
  *)     plist=unknown ;;
esac
# Secondary color: fileproviderd's own dump reads the provider's `enabled:` line just
# above its bundle-id line (unreliable — recorded, never gating).
dump="$(fileproviderctl dump 2>/dev/null | awk '
  /enabled:/ { e=$2 }
  /^com\.yasyf\.cc-pool\.status\.fileprovider$/ { print (e==""?"unknown":e); found=1; exit }
  END { if (!found) print "unknown" }')"
printf 'plist=%s dump=%s\n' "$plist" "${dump:-unknown}"
FPCHECK
vm_scp_to "$VMCTL_RESULTS_DIR/fpcheck.sh" "/tmp/ccpool-fpcheck.sh" || die "could not stage the FP consent check"
fp_preflight="$(vm_ssh "bash /tmp/ccpool-fpcheck.sh")" || die "FP consent preflight command failed in the guest (vm_ssh rc=$?) — not a consent verdict; check guest reachability and PlistBuddy/fileproviderctl"
# Parse "plist=<x> dump=<y>".
fp_plist="${fp_preflight#plist=}"; fp_plist="${fp_plist%% *}"
fp_dump="${fp_preflight#*dump=}"
case "$fp_plist" in yes|no|unknown) ;; *) die "FP consent preflight printed unexpected output: '$fp_preflight'" ;; esac
log "FP consent preflight: provider Domains.plist=$fp_plist (fileproviderctl dump=$fp_dump, advisory)"
# plist=no is the authoritative disabled verdict — stop now with the remediation
# rather than let the migrate fail slowly through the capability gate. plist=unknown
# defers: on a first-ever run the plist may not exist until the appex is elected.
[[ "$fp_plist" == "no" ]] &&
  fp_consent_die "Domains.plist NSFileProviderDomainDefaultIdentifier:Enabled=false (provider user-disabled; fileproviderctl dump=$fp_dump)"

# ---------------------------------------------------------------------------------
# 3. Synthetic live sessions: per account, a process holding an open fd on the
#    account's .claude.json plus a 1s read loop through the account dir — the
#    materialization read pressure that crushed fileproviderd in the incident.
# ---------------------------------------------------------------------------------
vm_phase live-sessions
require_seconds "live-sessions"

cat >"$VMCTL_RESULTS_DIR/readers.sh" <<'READERS'
set -u
ACCTS="$HOME/.cc-pool/accounts"
STOP=/tmp/ccpool-readers-stop
rm -f "$STOP"
for i in $(seq 1 10); do
  nn=$(printf '%02d' "$i")
  f="$ACCTS/acct-$nn/.claude.json"
  # Hold a persistent fd on the identity file (survives the rename into the
  # private backing root during conversion), and cat the path every second (a
  # NEW open each tick — lands on the domain once the row flips). Both tolerate
  # the brief dir-gone window mid-conversion.
  nohup sh -c '
    f="$1"
    exec 3<"$f" 2>/dev/null || true
    while [ ! -e /tmp/ccpool-readers-stop ]; do
      cat "$f" >/dev/null 2>&1 || true
      sleep 1
    done
  ' _ "$f" >/dev/null 2>&1 </dev/null &
done
echo "started 10 readers"
READERS

vm_scp_to "$VMCTL_RESULTS_DIR/readers.sh" "/tmp/ccpool-readers.sh" || die "could not stage the readers script"
vm_ssh "bash /tmp/ccpool-readers.sh" || die "failed to start synthetic readers"
log "10 synthetic live readers running"

# ---------------------------------------------------------------------------------
# 4. Fire the EXACT incident command: fleet migrate to fileprovider, forced.
# ---------------------------------------------------------------------------------
vm_phase migrate-storm
require_seconds "migrate"

log "firing: ccp migrate --to fileprovider --force (fleet, $NACCTS accounts)"
migrate_out="$VMCTL_RESULTS_DIR/migrate.out"
set +e
vm_ssh "'$VMCTL_GUEST_CCP' migrate --to fileprovider --force" >"$migrate_out" 2>&1
migrate_rc=$?
set -e
sed 's/^/  migrate| /' "$migrate_out" >&2 # fold the CLI output into scenario.log
if ((migrate_rc != 0)); then
  # The signature of an un-granted provider consent (the preflight caught only the
  # plist=no case; a first-ever run reaches here 'unknown'). The daemon's File
  # Provider capability gate — a throwaway register+enumerate+remove probe — refuses
  # UPFRONT, so handleMigrate fails with no account touched. The gate emits one of two
  # cc-pool-owned reasons (internal/daemon/migrate.go fpGate): "extension enabled but
  # not serving" (probe RPC ok, throwaway domain did not serve) or "companion app
  # control probe failed: <class>" (probe RPC errored, e.g. FP -2011 domainDisabled).
  # This scenario already confirmed the companion app's control socket is up before
  # the migrate, so either reason here is the consent blocker.
  if grep -qE 'extension enabled but not serving|companion app control probe failed' "$migrate_out"; then
    fp_consent_die "the fleet migrate's File Provider capability gate refused (\"extension enabled but not serving\" / \"companion app control probe failed\"); no account converted (see $migrate_out)"
  fi
  die "ccp migrate exited $migrate_rc — the fleet migration did not succeed (see the CLI output above and $VMCTL_GUEST_DAEMON_LOG)"
fi

# A claim-held account (daemon poll / pending select) reports MigrationBusy with
# the product's documented "retry shortly" contract — --force never skips claims.
# Follow that contract with bounded retries until every row has flipped.
for attempt in 2 3 4; do
  fp_rows="$(vm_ssh "sqlite3 '$GUEST_DB' \"SELECT COUNT(*) FROM accounts WHERE overlay_kind='fileprovider';\"")" \
    || die "could not count fileprovider rows before retry $attempt"
  [[ "$fp_rows" == "$NACCTS" ]] && break
  (($(vm_seconds_left) > 60)) || die "run window too small to retry busy-skipped accounts ($fp_rows/$NACCTS converted)"
  log "retry $attempt: $fp_rows/$NACCTS rows on fileprovider — re-running the fleet migrate for the busy-skipped accounts"
  sleep 5
  retry_out="$VMCTL_RESULTS_DIR/migrate-retry$attempt.out"
  set +e
  vm_ssh "'$VMCTL_GUEST_CCP' migrate --to fileprovider --force" >"$retry_out" 2>&1
  retry_rc=$?
  set -e
  sed "s/^/  migrate#$attempt| /" "$retry_out" >&2
  ((retry_rc == 0)) || die "retry $attempt: ccp migrate exited $retry_rc (see $retry_out)"
done

# ---------------------------------------------------------------------------------
# 5. Assertions — the incident regression checks. All evidence lands in the
#    results dir; any failure dies (→ exit 1 under EXPECT=clean).
# ---------------------------------------------------------------------------------
vm_phase assert
require_seconds "assert"

log_txt="$VMCTL_RESULTS_DIR/daemon.log"
vm_scp_from "$VMCTL_GUEST_DAEMON_LOG" "$log_txt" || die "could not fetch the daemon log for assertions"

# (a) All 10 accounts migrated symlink -> fileprovider (daemon's completion line,
#     migrate.go: "acct-NN overlay migrated symlink -> fileprovider").
done_count="$(grep -c 'overlay migrated symlink -> fileprovider' "$log_txt" || true)"
[[ "$done_count" == "$NACCTS" ]] \
  || die "expected $NACCTS accounts migrated symlink->fileprovider; daemon log shows $done_count (see $log_txt)"
# Belt and braces: the store rows all read fileprovider.
fp_rows="$(vm_ssh "sqlite3 '$GUEST_DB' \"SELECT COUNT(*) FROM accounts WHERE overlay_kind='fileprovider';\"")" || fp_rows="?"
[[ "$fp_rows" == "$NACCTS" ]] || die "expected $NACCTS fileprovider rows in the store, have '$fp_rows'"
log "assert: all $NACCTS accounts migrated to fileprovider"

# (b) ZERO File Provider storm signatures in the daemon log. The headline is the
#     Swift global-gate message from the incident; the others are the daemon-side
#     storm amplifiers (defect 2/3) the fix removes.
storm="$(grep -nE 'busy with another operation|is busy with another|file provider domain wedged|recovery attempt [0-9]|reconcile deferred, retrying' "$log_txt" || true)"
[[ -z "$storm" ]] || die "File Provider storm signatures in the daemon log — the incident regressed:
$storm"
log "assert: zero 'domain busy'/wedge/recovery storm lines"

# (c) Every account's through-domain read of .claude.json returns non-empty
#     (carrying its oauthAccount identity) within 5s.
for i in $(seq 1 "$NACCTS"); do
  nn="$(printf '%02d' "$i")"
  dir="$GUEST_ACCTS/acct-$nn"
  ok=0
  deadline=$(($(date +%s) + 5))
  while (($(date +%s) < deadline)) && (($(vm_seconds_left) > 0)); do
    content="$(vm_ssh "cat '$dir/.claude.json' 2>/dev/null")" || content=""
    if printf '%s' "$content" | grep -q 'oauthAccount'; then
      ok=1
      break
    fi
    sleep 1
  done
  ((ok == 1)) || die "acct-$nn: through-domain read of .claude.json empty or missing oauthAccount after 5s (domain not serving — see $log_txt)"
done
log "assert: all $NACCTS domains serve a non-empty .claude.json within 5s"

# (d) Every account converted exactly once and the migrated set is exactly
#     acct-01..NN. Conversions are sequential by construction (each is one
#     blocking RPC; the daemon's entrancy test pins max-in-flight == 1), and a
#     busy-skipped account legitimately converts on a retry pass — so the id
#     ORDER is recorded evidence, not an assertion.
order="$(grep -oE 'acct-[0-9]{2} overlay migrated symlink -> fileprovider' "$log_txt" | grep -oE 'acct-[0-9]{2}')"
sorted="$(printf '%s\n' "$order" | sort)"
expected="$(for i in $(seq 1 "$NACCTS"); do printf 'acct-%02d\n' "$i"; done)"
[[ "$sorted" == "$expected" ]] || die "migrated-account set is wrong (dup or missing conversion):
got (in log order):
$order"
log "assert: each of the $NACCTS accounts converted exactly once (order: $(printf '%s' "$order" | tr '\n' ' '))"

# ---------------------------------------------------------------------------------
# 6. Soak. The incident storm was self-sustaining for ~45 min AFTER the migrate
#    (62 busy / 19 reconcile-defers / 5 repairs over the following hour). Hold the
#    run window with the readers still pounding the domains and keep re-scraping
#    the daemon log for storm signatures, so a storm that only compounds on later
#    poll/heal cycles is still caught. A clean multi-minute soak is the decisive
#    negative — the failure surfaces within a few minutes, so the 10-min window is
#    conclusive without a longer run.
# ---------------------------------------------------------------------------------
vm_phase soak
soak_margin=20
soak_checks=0
while (($(vm_seconds_left) > soak_margin)); do
  sleep 15
  vm_scp_from "$VMCTL_GUEST_DAEMON_LOG" "$log_txt" || true
  storm="$(grep -nE 'busy with another operation|is busy with another|file provider domain wedged|recovery attempt [0-9]|reconcile deferred, retrying' "$log_txt" || true)"
  [[ -z "$storm" ]] || die "File Provider storm emerged during the soak — the self-sustaining incident regressed:
$storm"
  soak_checks=$((soak_checks + 1))
done
log "soak: $soak_checks clean re-checks across the run window"

# --- Clean up the synthetic readers (best effort) --------------------------------
vm_ssh "touch '$GUEST_READERS_STOP'; true" || true

log "replay clean: $NACCTS accounts migrated symlink->fileprovider, no storm across the run window, all domains serving, settled account-by-account"
