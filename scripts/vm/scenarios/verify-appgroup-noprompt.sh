# shellcheck shell=bash
# verify-appgroup-noprompt.sh — prove the daemon-bundle App-Group no-prompt bind.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

# Prereqs: `vmctl push`, then `VMCTL_SKIP_TCC=1 vmctl provision` (README: no-prompt).
GUEST_STATE="$VM_GUEST_HOME/.cc-pool"
GUEST_DB="$GUEST_STATE/pool.db"
# The daemon binds the FP bridge socket here, inside the App Group container.
GUEST_FP_BRIDGE_SOCK="$VM_GUEST_HOME/Library/Group Containers/SXKCTF23Q2.ccp/b.sock"
APP_GROUP="SXKCTF23Q2.ccp"
NACCTS=3
# The exact daemon-log substring startFPBridge emits when the bind parks on consent.
CONSENT_MARK='awaiting the one-time app-group-container consent'

# The upgrade-replay's second keg path (a fresh install location, same bundle id).
GUEST_DAEMON_APP2="$VMCTL_GUEST_DIR/CCPoolDaemon-upgrade.app"
GUEST_DAEMON_EXE2="$GUEST_DAEMON_APP2/Contents/MacOS/cc-pool"

# Host workspace for the per-mode re-wrapped daemon bundles.
WORK="$VMCTL_RESULTS_DIR/daemon-bundles"
mkdir -p "$WORK"

# require_seconds dies if the run window has already elapsed before a phase.
require_seconds() {
  (($(vm_seconds_left) > 0)) || die "run window elapsed before $1 — raise VMCTL_RUN_TIMEOUT_MIN"
}

# stop_daemon kills the daemon whether it runs from the bundle or a bare re-exec.
stop_daemon() {
  vm_ssh "pkill -f CCPoolDaemon 2>/dev/null; pkill -x cc-pool 2>/dev/null; true" || true
  sleep 2
}

# start_daemon launches `<exe> daemon` detached and prints its PID — stable across a
# syscall.Exec re-exec, so it stays valid even if the daemon self-execs.
start_daemon() {
  local exe="$1" logf="$2" pid
  # shellcheck disable=SC2029
  pid="$(vm_ssh "rm -f '$logf'; nohup '$exe' daemon >'$logf' 2>&1 </dev/null & echo \$!" | tail -n1 | tr -dc '0-9')"
  [[ -n "$pid" ]] || die "could not launch the daemon from $exe (no PID returned)"
  printf '%s\n' "$pid"
}

# bridge_accepts polls up to 3s for a client to CONNECT to the FP bridge socket (a
# bound-but-dead socket does not accept), via perl (always present on macOS).
bridge_accepts() {
  local deadline
  deadline=$(($(date +%s) + 3))
  while (($(date +%s) <= deadline)); do
    # shellcheck disable=SC2029
    if vm_ssh "perl -e 'use IO::Socket::UNIX; exit(IO::Socket::UNIX->new(Peer=>shift) ? 0 : 1)' '$GUEST_FP_BRIDGE_SOCK'" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# assert_no_consent fetches the daemon log and dies if it carries the consent line.
assert_no_consent() {
  local logf="$1" local_log="$2"
  vm_scp_from "$logf" "$local_log" || die "could not fetch the daemon log $logf for the consent-line check"
  ! grep -qF "$CONSENT_MARK" "$local_log" ||
    die "daemon logged the app-group-container consent-pending line — the bind parked awaiting a GUI prompt, NOT self-authorized: $(grep -F "$CONSENT_MARK" "$local_log" | head -n1)"
}

# assert_runs_from_bundle dies unless PID's argv still points at the bundle exe.
assert_runs_from_bundle() {
  local pid="$1" app="$2" cmd
  # shellcheck disable=SC2029
  cmd="$(vm_ssh "ps -p '$pid' -o command= 2>/dev/null")" || cmd=""
  [[ -n "$cmd" ]] || die "daemon PID $pid is not running (crashed on start? check the daemon log)"
  [[ "$cmd" == *"$app"* ]] ||
    die "daemon PID $pid runs from '$cmd', not the bundle at $app — the run-from-bundle contract is not in effect (the daemon re-exec'd to a bare stable-bin copy: this build predates the go-rewire that deleted reexecFromStableBin, so it forfeits the bundle's identifier-keyed TCC identity)"
}

# assert_entitlement_validated dies unless the kernel-validated process entitlements
# (launchctl procinfo, root) carry the App Group.
assert_entitlement_validated() {
  local pid="$1" info
  info="$(vm_sudo "launchctl procinfo '$pid' 2>/dev/null")" || info=""
  printf '%s\n' "$info" >"$VMCTL_RESULTS_DIR/procinfo-$pid.txt"
  printf '%s' "$info" | grep -q "$APP_GROUP" ||
    die "launchctl procinfo (PID $pid) does not show the $APP_GROUP app-group entitlement validated — the running daemon lacks the bundle's group grant (procinfo saved to procinfo-$pid.txt)"
}

# ccp_tcc_counts prints "RESULT user=<n> system=<n>" for cc-pool App-Group TCC rows.
ccp_tcc_counts() {
  vm_sudo "env HOME='$VM_GUEST_HOME' bash /tmp/ccp-tcccount.sh" | grep -E '^RESULT user=[0-9]+ system=[0-9]+$' | tail -n1
}

# assert_zero_tcc dies unless BOTH TCC.dbs hold zero cc-pool App-Group rows.
assert_zero_tcc() {
  local label="$1" counts uc sc
  counts="$(ccp_tcc_counts)" || die "$label: could not read the guest TCC.db counts"
  [[ "$counts" =~ ^RESULT\ user=([0-9]+)\ system=([0-9]+)$ ]] || die "$label: unexpected TCC count output: '$counts'"
  uc="${BASH_REMATCH[1]}"
  sc="${BASH_REMATCH[2]}"
  log "$label: cc-pool kTCCServiceSystemPolicyAppData rows — user=$uc system=$sc"
  [[ "$uc" == "0" && "$sc" == "0" ]] ||
    die "$label: a cc-pool App-Group TCC row exists (user=$uc system=$sc) — a consent prompt fired; the daemon bundle did NOT self-authorize the group-container bind"
}

# run_mode drives one arm (profiled|unprofiled) through cold-start + upgrade-replay.
run_mode() {
  local mode="$1"
  local app1="$WORK/CCPoolDaemon-$mode.app" app2="$WORK/CCPoolDaemon-$mode-v2.app"
  local log1="$VMCTL_GUEST_DIR/daemon-$mode.log" log2="$VMCTL_GUEST_DIR/daemon-$mode-v2.log"
  local pid

  # (b) Cold start, no grant: the bundle entitlement (+ profile) must self-authorize.
  vm_phase "cold-$mode"
  require_seconds "cold-$mode"
  stop_daemon
  assert_zero_tcc "cold-$mode baseline"
  vm_build_daemon_bundle "$SRC_BIN" "$app1" "$SIGN_ID" "0.1.0" "$mode"
  vm_install_daemon_bundle "$app1" "$VMCTL_GUEST_DAEMON_APP"
  log "cold-$mode: starting the daemon from $VMCTL_GUEST_DAEMON_EXE (no TCC grant pre-seeded)"
  pid="$(start_daemon "$VMCTL_GUEST_DAEMON_EXE" "$log1")"
  bridge_accepts || die "cold-$mode: FP bridge socket did not accept within 3s of cold start — the group-container bind did not land (daemon log: $log1)"
  log "cold-$mode: FP bridge accepts (pid $pid)"
  assert_no_consent "$log1" "$VMCTL_RESULTS_DIR/daemon-$mode.log"
  assert_zero_tcc "cold-$mode post-start"
  assert_runs_from_bundle "$pid" "$VMCTL_GUEST_DAEMON_APP"
  assert_entitlement_validated "$pid"
  log "cold-$mode: clean — bridge up, no consent line, zero TCC rows, entitlement validated"

  # (c) Upgrade-replay: a second build at a new keg path must add no TCC row.
  vm_phase "upgrade-$mode"
  require_seconds "upgrade-$mode"
  stop_daemon
  vm_build_daemon_bundle "$SRC_BIN" "$app2" "$SIGN_ID" "0.2.0" "$mode"
  vm_install_daemon_bundle "$app2" "$GUEST_DAEMON_APP2"
  log "upgrade-$mode: starting the second build from $GUEST_DAEMON_EXE2 (new keg path, same bundle id)"
  pid="$(start_daemon "$GUEST_DAEMON_EXE2" "$log2")"
  bridge_accepts || die "upgrade-$mode: FP bridge socket did not accept within 3s after the upgrade (daemon log: $log2)"
  assert_no_consent "$log2" "$VMCTL_RESULTS_DIR/daemon-$mode-v2.log"
  assert_zero_tcc "upgrade-$mode post-upgrade"
  assert_runs_from_bundle "$pid" "$GUEST_DAEMON_APP2"
  assert_entitlement_validated "$pid"
  log "upgrade-$mode: clean — zero new TCC rows across the keg-path change, no re-prompt"
  stop_daemon
}

# 0. Preflight: resolve the mode set, signing identity, and daemon source binary.
vm_phase preflight
require_seconds "preflight"

# VMCTL_DAEMON_MODE: auto picks profiled iff VMCTL_PROFILE_DAEMON is set; both A/Bs.
MODE="${VMCTL_DAEMON_MODE:-auto}"
case "$MODE" in
auto) if [[ -n "${VMCTL_PROFILE_DAEMON:-}" ]]; then MODES=(profiled); else MODES=(unprofiled); fi ;;
profiled) MODES=(profiled) ;;
unprofiled) MODES=(unprofiled) ;;
both) MODES=(profiled unprofiled) ;;
*) die "VMCTL_DAEMON_MODE must be auto|profiled|unprofiled|both, got: $MODE" ;;
esac
for m in "${MODES[@]}"; do
  [[ "$m" == "profiled" && -z "${VMCTL_PROFILE_DAEMON:-}" ]] &&
    die "mode 'profiled' needs VMCTL_PROFILE_DAEMON (a .provisionprofile path or its base64)"
done
log "validating daemon-bundle mode(s): ${MODES[*]}"

# The signing identity re-wraps the per-mode bundles the way push.sh built the installed one.
IFS=$'\t' read -r SIGN_ID SIGN_TEAM < <(vm_discover_signing)
[[ "$SIGN_TEAM.ccp" == "$APP_GROUP" ]] ||
  die "signing team $SIGN_TEAM yields ${SIGN_TEAM}.ccp but the scenario expects $APP_GROUP (paths.go drift)"
log "re-wrapping bundles with Developer ID $SIGN_ID (team $SIGN_TEAM)"

# The universal daemon binary to re-wrap, pulled from the bundle push installed.
SRC_BIN="$WORK/cc-pool-daemon-src"
vm_scp_from "$VMCTL_GUEST_DAEMON_EXE" "$SRC_BIN" ||
  die "could not fetch the daemon binary from $VMCTL_GUEST_DAEMON_EXE — run 'vmctl push' first"
chmod 755 "$SRC_BIN"

# 1. Seed a small pool so the daemon starts normally; no Keychain, no logins.
vm_phase seed
require_seconds "seed"
stop_daemon

cat >"$VMCTL_RESULTS_DIR/seed.sh" <<'SEED'
set -euo pipefail
N=3
STATE="$HOME/.cc-pool"
ACCTS="$STATE/accounts"
DB="$STATE/pool.db"
rm -rf "$ACCTS" "$DB" "$DB-wal" "$DB-shm"
mkdir -p "$ACCTS"
chmod 700 "$STATE"
mkdir -p "$HOME/.claude"
[ -f "$HOME/.claude.json" ] || printf '{}\n' >"$HOME/.claude.json"
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
sqlite3 "$DB" "INSERT OR REPLACE INTO meta (key,value) VALUES ('initialized','1'),('overlay_kind','symlink');"
now=$(date +%s)
for i in $(seq 1 "$N"); do
  nn=$(printf '%02d' "$i")
  dir="$ACCTS/acct-$nn"
  mkdir -p "$dir"
  uuid="00000000-0000-4000-8000-0000000000$nn"
  printf '{"oauthAccount":{"accountUuid":"%s","emailAddress":"acct-%s@ccpool.test"}}\n' "$uuid" "$nn" >"$dir/.claude.json"
  sqlite3 "$DB" \
    "INSERT INTO accounts (id,config_dir,keychain_service,keychain_account,label,overlay_kind,created_at)
     VALUES ($i,'$dir','ccp-vm-np-$nn','acct-$nn','vm-$nn','symlink',$now);"
done
echo "seeded $(sqlite3 "$DB" 'SELECT COUNT(*) FROM accounts;') accounts"
SEED
vm_scp_to "$VMCTL_RESULTS_DIR/seed.sh" "/tmp/ccp-np-seed.sh" || die "could not stage the seed script"
vm_ssh "bash /tmp/ccp-np-seed.sh" || die "seeding failed"
seeded="$(vm_ssh "sqlite3 '$GUEST_DB' \"SELECT COUNT(*) FROM accounts;\"")" || die "could not read back seeded rows"
[[ "$seeded" == "$NACCTS" ]] || die "expected $NACCTS accounts seeded, store has '$seeded'"
log "seeded $seeded accounts"

# Stage the TCC-count probe (run as root so it can read the user AND system TCC.db).
cat >"$VMCTL_RESULTS_DIR/tcccount.sh" <<'TCC'
set -u
SVC=kTCCServiceSystemPolicyAppData
WHERE="service='$SVC' AND (client='com.yasyf.cc-pool.daemon' OR client LIKE '%cc-pool%' OR client LIKE '%CCPoolDaemon%' OR client LIKE '%ccpool%' OR client='com.apple.sshd-keygen-wrapper')"
probe() {
  db="$1"
  label="$2"
  [ -f "$db" ] || {
    echo 0
    return 0
  }
  tmp="$(mktemp -d)"
  cp "$db" "$tmp/TCC.db" 2>/dev/null || {
    rm -rf "$tmp"
    echo ERR
    return 0
  }
  cp "$db-wal" "$tmp/TCC.db-wal" 2>/dev/null || true
  cp "$db-shm" "$tmp/TCC.db-shm" 2>/dev/null || true
  rows="$(sqlite3 "$tmp/TCC.db" "SELECT client,client_type,auth_value FROM access WHERE $WHERE;" 2>/dev/null)"
  [ -n "$rows" ] && printf '%s TCC rows:\n%s\n' "$label" "$rows" >&2
  n="$(sqlite3 "$tmp/TCC.db" "SELECT COUNT(*) FROM access WHERE $WHERE;" 2>/dev/null)"
  rm -rf "$tmp"
  [ -n "$n" ] && echo "$n" || echo ERR
}
uc="$(probe "$HOME/Library/Application Support/com.apple.TCC/TCC.db" user)"
sc="$(probe "/Library/Application Support/com.apple.TCC/TCC.db" system)"
printf 'RESULT user=%s system=%s\n' "$uc" "$sc"
TCC
vm_scp_to "$VMCTL_RESULTS_DIR/tcccount.sh" "/tmp/ccp-tcccount.sh" || die "could not stage the TCC-count probe"

# 2. Drive every selected mode through cold-start + upgrade-replay.
for mode in "${MODES[@]}"; do
  run_mode "$mode"
done
log "verify-appgroup-noprompt: all modes clean (${MODES[*]})"
