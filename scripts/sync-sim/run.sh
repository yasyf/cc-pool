#!/usr/bin/env bash
#
# Two-host cross-host sync sim for cc-pool's ORIGIN-ONLY credential redesign.
# It drives the REAL binary end to end between two simulated Macs ("a" and "b")
# that share nothing but a hand-written synckit mesh, and it proves the two
# load-bearing invariants of the redesign:
#
#   1. A peer NEVER refreshes: only the chain's origin host holds a refresh
#      token and ever POSTs the token endpoint. Each host has its OWN fake-oauth
#      instance, so a peer's token-POST count is provably its own — and zero.
#   2. Claude refresh tokens are SINGLE-USE. fake-oauth enforces rotation: any
#      reuse of a spent refresh token is a double-spend that kills the chain
#      family. The suite fails if the detector ever fires.
#
# HARD SAFETY: every process runs with HOME (and XDG_CONFIG_HOME) inside
# /tmp/ccp-sim/{a,b}; credentials are file-backed with FAKE tokens; the real
# login Keychain is never touched (CLAUDE_POOL_SECURITY_BIN -> a file-backend
# shim); the token/usage endpoints are redirected to a per-host fake-oauth
# (CLAUDE_POOL_TOKEN_URL / CLAUDE_POOL_USAGE_URL); all NON-loopback egress is
# black-holed to a dead proxy; no launchctl, no `ccp service`. Every daemon and
# fake-oauth this script starts is killed on exit. The sim NEVER spawns an
# unbounded process tree: it drives the pre-built binary, never `go test`.
#
# Usage:  scripts/sync-sim/run.sh [--runs N] [--keep]
#   --runs N   run the whole scenario suite N times from clean state (default 2)
#   --keep     leave /tmp/ccp-sim and the daemons in place at the end (debug)
#
# Exit status is 0 only if every scenario passes on every run.

set -euo pipefail

SIM=/tmp/ccp-sim
BIN="$SIM/bin"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SIMUSER=simuser
RUNS=2
KEEP=0

while [ $# -gt 0 ]; do
  case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ---------------------------------------------------------------------------
# Fake identity + chain tokens. Every refresh token contains RT_MARKER, and
# every access token contains AT (never RT_MARKER), so a case-sensitive grep for
# a family's refresh marker on a peer is an exact secret-leak canary.
# ---------------------------------------------------------------------------
UUID="1f1e1d1c-0b0a-4090-8807-060504030201"
EMAIL="acct-x@sim.example"

RT_MARKER="RTSECRET"          # substring of every refresh token, never of an access token
A_FAMILY="c1"                 # the chain A owns; B mirrors it as a read-only peer
A_AT="ATOK-c1-g1"; A_RT="RTSECRET-c1-g1"
A_RT_FAMILY="RTSECRET-c1"     # A's refresh-token family prefix
B_FAMILY="cb"                 # the independent chain B mints when it becomes an origin
B_AT="ATOK-cb-g1"; B_RT="RTSECRET-cb-g1"
B_RT_FAMILY="RTSECRET-cb"     # B's refresh-token family prefix

NOW_MS=$(( $(date +%s) * 1000 ))
HOUR_MS=3600000
E_INIT=$(( NOW_MS + HOUR_MS ))       # c1 initial expiry (+1h): a real refresh (+8h) is strictly fresher
E_NEAR=$(( NOW_MS + 5*60*1000 ))     # near expiry (+5m < 10m RefreshLeadTime): forces an idle refresh
E_PAST=$(( NOW_MS - 60*1000 ))       # already expired (-1m)
E_B_INIT=$(( NOW_MS + 2*HOUR_MS ))   # cb initial expiry (+2h): fresher than c1, so the origin flips to B

DEAD_PEER="exec:false"               # a peer transport that always fails: models an unreachable origin

# ---------------------------------------------------------------------------
# Process bookkeeping + cleanup
# ---------------------------------------------------------------------------
DAEMON_PIDS=()
OAUTH_PIDS=()

cleanup() {
  set +e
  for pid in "${DAEMON_PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null; done
  for pid in "${OAUTH_PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null; done
  pkill -f "$BIN/cc-pool" 2>/dev/null
  pkill -f "$BIN/fakeoauth" 2>/dev/null
  sleep 0.3
  pkill -9 -f "$BIN/cc-pool" 2>/dev/null
  pkill -9 -f "$BIN/fakeoauth" 2>/dev/null
  if [ "$KEEP" = 0 ]; then
    rm -rf "$SIM/a" "$SIM/b" "$SIM/logs" "$SIM/run"
  fi
}
trap cleanup EXIT

fail() { echo "  ✗ FAIL: $*" >&2; exit 1; }
ok()   { echo "  ✓ $*"; }
hdr()  { echo; echo "=== $* ==="; }

# ---------------------------------------------------------------------------
# fake-oauth: one instance per host, distinct ephemeral ports
# ---------------------------------------------------------------------------
oauth_addrfile() { echo "$SIM/run/oauth-$1.addr"; }
oauth_logfile()  { echo "$SIM/logs/fakeoauth-$1.jsonl"; }
oauth_base()     { cat "$(oauth_addrfile "$1")"; }   # host:port
oauth_token_url() { echo "http://$(oauth_base "$1")/v1/oauth/token"; }
oauth_usage_url() { echo "http://$(oauth_base "$1")/api/oauth/usage"; }

# start_oauth HOST — launch HOST's fake-oauth, wait for it to bind, record pid.
start_oauth() {
  local h="$1" af lf
  af="$(oauth_addrfile "$h")"; lf="$(oauth_logfile "$h")"
  rm -f "$af" "$lf"
  "$BIN/fakeoauth" -addr 127.0.0.1:0 -host "$h" -log "$lf" -portfile "$af" \
    >"$SIM/logs/fakeoauth-$h.out" 2>&1 &
  local pid=$!
  OAUTH_PIDS+=("$pid")
  for _ in $(seq 1 100); do
    [ -s "$af" ] && { ok "fake-oauth $h up ($(cat "$af"), pid $pid)"; return 0; }
    if ! kill -0 "$pid" 2>/dev/null; then cat "$SIM/logs/fakeoauth-$h.out" >&2; fail "fake-oauth $h died"; fi
    sleep 0.05
  done
  fail "fake-oauth $h never bound a port"
}

stop_all_oauth() {
  for pid in "${OAUTH_PIDS[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; done
  OAUTH_PIDS=()
}

# register_oauth HOST FAMILY GEN ACCESS REFRESH EXPIRES_MS — register a chain's
# initial access+refresh pair with HOST's fake-oauth as live (so a later refresh
# recognizes the seeded refresh token).
register_oauth() {
  curl -s --noproxy '*' -X POST "http://$(oauth_base "$1")/admin/seed" \
    -d "{\"family\":\"$2\",\"gen\":$3,\"access\":\"$4\",\"refresh\":\"$5\",\"expiresAtMs\":$6}" >/dev/null \
    || fail "register_oauth $1 $2 failed"
}

# oauth_field HOST FIELD — read a numeric field from HOST's /admin/report.
oauth_field() {
  curl -s --noproxy '*' "http://$(oauth_base "$1")/admin/report" \
    | python3 -c "import json,sys;print(json.load(sys.stdin)['$2'])"
}

# ---------------------------------------------------------------------------
# Per-host command runner and exec: peer transport strings
# ---------------------------------------------------------------------------
# hrun HOST CMD...  — run CMD with HOST's sandboxed environment. The token/usage
# endpoints point at HOST's own fake-oauth; loopback bypasses the dead proxy so
# ONLY the fake-oauth is reachable and any accidental real egress fails loud.
hrun() {
  local h="$1"; shift
  env -i \
    HOME="$SIM/$h" \
    XDG_CONFIG_HOME="$SIM/$h/.config" \
    TMPDIR="$SIM/$h/tmp" \
    CLAUDE_POOL_SECURITY_BIN="$BIN/fake-security" \
    CLAUDE_POOL_TOKEN_URL="$(oauth_token_url "$h")" \
    CLAUDE_POOL_USAGE_URL="$(oauth_usage_url "$h")" \
    CCP_SYNC_EXEC_PEER=1 \
    HTTPS_PROXY="http://127.0.0.1:1" HTTP_PROXY="http://127.0.0.1:1" NO_PROXY="127.0.0.1,localhost" \
    PATH="$BIN:/usr/bin:/bin:/usr/sbin:/sbin" \
    USER="$SIMUSER" \
    "$@"
}

# exec_peer HOST — the exec: transport a peer uses to reach HOST's rpc-serve
# bridge, carrying HOST's full sandboxed env (its state, Keychain shim, and its
# own fake-oauth) so the spawned bridge resolves HOST's world, not the caller's.
exec_peer() {
  local h="$1"
  printf 'exec:env -i HOME=%s XDG_CONFIG_HOME=%s TMPDIR=%s CLAUDE_POOL_SECURITY_BIN=%s CLAUDE_POOL_TOKEN_URL=%s CLAUDE_POOL_USAGE_URL=%s CCP_SYNC_EXEC_PEER=1 HTTPS_PROXY=http://127.0.0.1:1 HTTP_PROXY=http://127.0.0.1:1 NO_PROXY=127.0.0.1,localhost PATH=%s:/usr/bin:/bin:/usr/sbin:/sbin USER=%s %s sync rpc-serve' \
    "$SIM/$h" "$SIM/$h/.config" "$SIM/$h/tmp" "$BIN/fake-security" \
    "$(oauth_token_url "$h")" "$(oauth_usage_url "$h")" "$BIN" "$SIMUSER" "$BIN/cc-pool"
}

# write_state HOST SELF_PEER PEER_PEER — hand-write HOST's synckit mesh
# state.json (self transport + one peer host transport).
write_state() {
  local h="$1" selfp="$2" peerp="$3"
  mkdir -p "$SIM/$h/.config/synckit"
  printf '{\n  "self": "%s",\n  "hosts": [\n    "%s"\n  ]\n}\n' "$selfp" "$peerp" \
    > "$SIM/$h/.config/synckit/state.json"
}

# start_daemon HOST — launch HOST's cc-pool daemon, record its pid, wait for its
# sync socket to bind. Pre-starting both daemons means peer rpc-serve bridges
# find them Available and never spawn extras.
start_daemon() {
  local h="$1"
  hrun "$h" "$BIN/cc-pool" daemon >"$SIM/logs/daemon-$h.log" 2>&1 &
  local pid=$!
  echo "$pid" >"$SIM/run/daemon-$h.pid"
  DAEMON_PIDS+=("$pid")
  local sock="$SIM/$h/.cc-pool/sync.sock"
  for _ in $(seq 1 100); do
    [ -S "$sock" ] && { ok "daemon $h up (pid $pid, sync.sock bound)"; return 0; }
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "--- daemon-$h.log ---" >&2; cat "$SIM/logs/daemon-$h.log" >&2
      fail "daemon $h died before binding sync.sock"
    fi
    sleep 0.1
  done
  echo "--- daemon-$h.log ---" >&2; tail -30 "$SIM/logs/daemon-$h.log" >&2
  fail "daemon $h never bound sync.sock"
}

# stop_daemon HOST — kill HOST's tracked daemon and wait for it to exit.
stop_daemon() {
  local h="$1" pid
  pid="$(cat "$SIM/run/daemon-$h.pid" 2>/dev/null || true)"
  [ -n "$pid" ] || return 0
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 50); do
    kill -0 "$pid" 2>/dev/null || { rm -f "$SIM/run/daemon-$h.pid"; return 0; }
    sleep 0.1
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$SIM/run/daemon-$h.pid"
}

converge() { hrun "$1" "$BIN/cc-pool" sync converge; }

# Bring both registries to a fixpoint (Save/TouchStamp are no-ops at rest).
quiesce() { converge b >/dev/null; converge a >/dev/null; converge b >/dev/null; converge a >/dev/null; }

# force_refresh HOST WANT_AT — refresh HOST's OWNED chain until its access token
# is WANT_AT. PreflightRefresh fails closed (skips the refresh, no POST) when a
# transient procscan EIO aborts the idle scan under sim process churn; a real
# select/daemon-poll just retries, so we do too. HOST's daemon is stopped so the
# manual select is the sole refresher (no cross-process double-spend), then
# restarted.
force_refresh() {
  local h="$1" want="$2" i
  stop_daemon "$h"
  for i in $(seq 1 40); do
    hrun "$h" "$BIN/seed" setexp --id 1 --expires-ms "$E_NEAR" >/dev/null
    hrun "$h" "$BIN/cc-pool" select --account 1 --no-daemon >/dev/null 2>&1 || true
    if [ "$(cred_get "$(credfile "$h")" accessToken)" = "$want" ]; then
      start_daemon "$h" >/dev/null
      return 0
    fi
    sleep 0.2
  done
  hrun "$h" "$BIN/cc-pool" select --account 1 --no-daemon 2>&1 | sed 's/^/    /' >&2
  start_daemon "$h" >/dev/null
  fail "$h never refreshed its owned chain to $want (procscan idle-scan kept failing closed)"
}

# ---------------------------------------------------------------------------
# Registry + credential inspection helpers
# ---------------------------------------------------------------------------
regfile()   { echo "$SIM/$1/.cc-pool/sync/registry.json"; }
credfile()  { echo "$SIM/$1/.cc-pool/accounts/acct-01/.credentials.json"; }
identfile() { echo "$SIM/$1/.cc-pool/accounts/acct-01/.claude.json"; }

# reg_get HOST UUID FIELD — read a registry entry field: present | hash | origin
# | expiresat | added | removed. Prints MISSING when the uuid has no entry.
reg_get() {
  python3 - "$(regfile "$1")" "$2" "$3" <<'PY'
import json,sys
path,uuid,field=sys.argv[1],sys.argv[2],sys.argv[3]
try:
    reg=json.load(open(path))
except FileNotFoundError:
    print("NOFILE"); sys.exit(0)
e=reg.get(uuid)
if e is None:
    print("MISSING"); sys.exit(0)
added=e.get("added_at",0); removed=e.get("removed_at",0)
ch=e["value"]["chain"]
out={
 "present": "yes" if added>removed else "no",
 "hash": ch["hash"],
 "origin": ch["origin"],
 "expiresat": str(ch["expiresAt"]),
 "added": str(added),
 "removed": str(removed),
}
print(out[field])
PY
}

cred_get() {
  python3 - "$1" "$2" <<'PY'
import json,sys
try:
    c=json.load(open(sys.argv[1]))["claudeAiOauth"]
except FileNotFoundError:
    print("NOFILE"); sys.exit(0)
print(c.get(sys.argv[2],""))
PY
}

ident_get() {
  python3 - "$1" "$2" <<'PY'
import json,sys
try:
    o=json.load(open(sys.argv[1]))["oauthAccount"]
except (FileNotFoundError,KeyError):
    print("MISSING"); sys.exit(0)
print(o.get(sys.argv[2],""))
PY
}

assert_reg_identical() {
  if diff -q "$(regfile a)" "$(regfile b)" >/dev/null; then
    ok "registry.json byte-identical on a and b"
  else
    echo "--- a ---" >&2; cat "$(regfile a)" >&2
    echo "--- b ---" >&2; cat "$(regfile b)" >&2
    fail "registry.json differs between a and b"
  fi
}

# GLOBAL INVARIANT (a): the double-spend detector must be silent on BOTH hosts.
assert_no_double_spend() {
  for h in a b; do
    local ds; ds="$(oauth_field "$h" doubleSpends)"
    [ "$ds" = "0" ] || fail "fake-oauth $h reported $ds double-spend(s)"
    if grep -qF '"outcome":"double_spend"' "$(oauth_logfile "$h")" 2>/dev/null; then
      fail "a double_spend was logged on fake-oauth $h"
    fi
  done
  ok "no double-spend on either fake-oauth (detector silent)"
}

# assert_zero_posts HOST — HOST made ZERO token POSTs (a peer never refreshes).
assert_zero_posts() {
  local h="$1" n; n="$(oauth_field "$h" tokenPosts)"
  [ "$n" = "0" ] || fail "$h made $n token POST(s); a peer must never refresh"
  ok "$h token-POST count == 0 (never refreshed)"
}

# GLOBAL INVARIANT (b): a foreign refresh-token family must never touch a host's
# files or its daemon log.
assert_no_foreign_rt() {
  local h="$1" marker="$2"
  if grep -rqF "$marker" "$SIM/$h" 2>/dev/null; then
    grep -rlF "$marker" "$SIM/$h" >&2 2>/dev/null || true
    fail "foreign refresh token '$marker' leaked into $h's files"
  fi
  if grep -qF "$marker" "$SIM/logs/daemon-$h.log" 2>/dev/null; then
    fail "foreign refresh token '$marker' leaked into $h's daemon log"
  fi
  ok "no '$marker' on $h (files + daemon log clean)"
}

# ---------------------------------------------------------------------------
# Base setup: fake-oauth per host, A owns c1, both daemons up
# ---------------------------------------------------------------------------
setup_hosts() {
  stop_daemon a; stop_daemon b
  stop_all_oauth
  pkill -f "$BIN/cc-pool" 2>/dev/null || true
  pkill -f "$BIN/fakeoauth" 2>/dev/null || true
  sleep 0.2
  rm -rf "$SIM/a" "$SIM/b" "$SIM/logs" "$SIM/run"
  mkdir -p "$SIM/a/tmp" "$SIM/b/tmp" "$SIM/logs" "$SIM/run"
  DAEMON_PIDS=()

  start_oauth a
  start_oauth b

  local pa pb
  pa="$(exec_peer a)"; pb="$(exec_peer b)"

  for h in a b; do hrun "$h" "$BIN/seed" init >/dev/null; done
  write_state a "$pa" "$pb"
  write_state b "$pb" "$pa"

  # Host A owns account X with chain c1; register c1 with A's fake-oauth so A can
  # rotate it. B holds nothing yet (enable + converge materialize a peer copy).
  hrun a "$BIN/seed" account --id 1 --uuid "$UUID" --email "$EMAIL" \
    --access "$A_AT" --refresh "$A_RT" --expires-ms "$E_INIT" --label "acct-x" >/dev/null
  register_oauth a "$A_FAMILY" 1 "$A_AT" "$A_RT" "$E_INIT"

  for h in a b; do hrun "$h" "$BIN/cc-pool" sync enable >/dev/null; done

  start_daemon a
  start_daemon b
}

# materialize_peer — drive B to materialize its read-only peer copy of c1.
materialize_peer() {
  quiesce
  [ -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B never materialized acct-01"
}

# ---------------------------------------------------------------------------
# Scenario 1 — enable + materialize; peer copy carries NO refresh token
# ---------------------------------------------------------------------------
scenario_1_materialize() {
  hdr "Scenario 1: materialize — A's account appears on B with NO refresh token"
  [ "$(reg_get a "$UUID" present)" = "yes" ] || fail "X not present in A registry after enable"
  [ "$(reg_get a "$UUID" origin)" = "$(exec_peer a)" ] || fail "A did not stamp itself origin"
  ok "A published c1 (origin = A)"

  materialize_peer
  [ "$(ident_get "$(identfile b)" accountUuid)" = "$UUID" ] || fail "B identity accountUuid mismatch"
  [ "$(ident_get "$(identfile b)" emailAddress)" = "$EMAIL" ] || fail "B identity email mismatch"
  ok "B materialized acct-01 (identity verbatim)"

  [ "$(cred_get "$(credfile b)" accessToken)" = "$A_AT" ] || fail "B access token != c1"
  # THE INVARIANT: the peer copy has NO refresh token.
  [ -z "$(cred_get "$(credfile b)" refreshToken)" ] || fail "B blob carries a refreshToken field"
  ! grep -qF "$RT_MARKER" "$(credfile b)" || fail "a refresh token string is present in B's blob"
  ok "B's peer copy has NO refresh token (grep + field both clean)"

  # A's blob is unchanged: still owned (refresh token present).
  [ "$(cred_get "$(credfile a)" refreshToken)" = "$A_RT" ] || fail "A's refresh token changed"
  ok "A's blob unchanged (still owned)"

  [ "$(hrun a "$BIN/seed" hash --id 1)" = "$(hrun b "$BIN/seed" hash --id 1)" ] || fail "AccessHash a != b"
  ok "AccessHash identical on a and b (owned and stripped hash alike)"

  # Capture the exact wire bytes A serves B and prove no refresh token crosses.
  local wire; wire="$(hrun b "$BIN/seed" wirecap --peer "$(exec_peer a)" --uuid "$UUID")"
  echo "    wire envelope: $wire"
  echo "$wire" | grep -qF "$A_AT" || fail "wire envelope missing the access token"
  echo "$wire" | grep -qF "$RT_MARKER" && fail "wire envelope carries a refresh token" || true
  ok "captured wire envelope carries the access token but NO refresh token"

  assert_reg_identical
  assert_zero_posts b
  assert_no_double_spend
  assert_no_foreign_rt b "$A_RT_FAMILY"
}

# ---------------------------------------------------------------------------
# Scenario 2 — origin rotation propagates; the peer never refreshes
# ---------------------------------------------------------------------------
scenario_2_rotation() {
  hdr "Scenario 2: origin rotation propagates; peer token-POSTs == 0"
  # Force A's owned token near expiry, then drive a REAL refresh through the
  # binary (ccp select runs PreflightRefresh -> EnsureFreshToken -> token POST,
  # the same refresh path the daemon poll uses, but synchronously).
  force_refresh a "ATOK-c1-g2"
  local newAT; newAT="$(cred_get "$(credfile a)" accessToken)"
  [ "$newAT" != "$A_AT" ] || fail "A did not rotate its access token"
  [ "$newAT" = "ATOK-c1-g2" ] || fail "A rotated to unexpected token $newAT"
  ok "A refreshed c1 -> $newAT (real POST to A's fake-oauth)"
  [ "$(oauth_field a tokenPosts)" != "0" ] || fail "A made no token POST"
  grep -qF "$A_RT_FAMILY" "$(credfile a)" || fail "A lost its refresh token after rotation"
  ok "A still owns the (rotated) chain — refresh token present"

  # Propagate: A folds the fresher chain into the registry; B pulls it.
  quiesce
  [ "$(cred_get "$(credfile b)" accessToken)" = "$newAT" ] || fail "B did not install the rotated AT"
  ok "B installed the rotated AT (pulled stripped from A)"
  [ -z "$(cred_get "$(credfile b)" refreshToken)" ] || fail "B's rotated copy carries a refreshToken"
  ! grep -qF "$RT_MARKER" "$(credfile b)" || fail "a refresh token appeared in B's blob after rotation"
  ok "B's rotated copy still has NO refresh token"

  # THE INVARIANT: the peer never refreshed.
  assert_zero_posts b
  assert_reg_identical
  assert_no_double_spend
  assert_no_foreign_rt b "$A_RT_FAMILY"
}

# ---------------------------------------------------------------------------
# Scenario 5 — tombstone heal: a claude tombstone is restored by a pull
# ---------------------------------------------------------------------------
scenario_5_tombstone_heal() {
  hdr "Scenario 5: tombstone heal — a claude tombstone on B is restored by a pull"
  local liveAT; liveAT="$(cred_get "$(credfile a)" accessToken)"
  # Overwrite B's peer copy with claude's own dead-chain tombstone shape.
  printf '{"claudeAiOauth":{"accessToken":"","refreshToken":"","expiresAt":0}}\n' > "$(credfile b)"
  [ -z "$(cred_get "$(credfile b)" accessToken)" ] || fail "tombstone not written"
  ok "B overwritten with a tombstone (accessToken/refreshToken empty, expiresAt 0)"

  quiesce
  [ "$(cred_get "$(credfile b)" accessToken)" = "$liveAT" ] || fail "B tombstone not healed to the live AT"
  [ -z "$(cred_get "$(credfile b)" refreshToken)" ] || fail "healed copy carries a refreshToken"
  ! grep -qF "$RT_MARKER" "$(credfile b)" || fail "healed copy carries a refresh token string"
  ok "B's stripped copy restored by a pull (tombstone -> $liveAT, no refresh token)"

  assert_zero_posts b
  assert_reg_identical
  assert_no_double_spend
  assert_no_foreign_rt b "$A_RT_FAMILY"
}

# ---------------------------------------------------------------------------
# Scenario 6 — remove/teardown, and a BUSY peer still refreshes zero times
# ---------------------------------------------------------------------------
scenario_6_remove_busy() {
  hdr "Scenario 6: remove/teardown + a busy peer performs ZERO refreshes"
  # A removes the account (peer tombstone). Quiesce A's daemon around the remove
  # so its poll can't re-assert the overlay under the local teardown.
  stop_daemon a
  hrun a "$BIN/cc-pool" remove 1 >/dev/null
  start_daemon a
  [ "$(reg_get a "$UUID" present)" = "no" ] || fail "A did not tombstone X"
  ok "A tombstoned X (peer removal)"

  # A busy (fake live session) peer must DEFER teardown and refresh zero times.
  # busydefer runs one converge in-process with a fake Sessions seam (the daemon
  # can't inject a live session from outside), stopping B's daemon so it owns B's
  # store; A stays up as the peer.
  stop_daemon b
  local busy_out idle_out
  busy_out="$(hrun b "$BIN/busydefer" --busy=true)"
  echo "    busydefer(busy)=$busy_out"
  python3 - "$busy_out" <<'PY' || fail "busy converge did not defer"
import json,sys
r=json.loads(sys.argv[1])
assert r["skippedBusy"]>=1, "expected skippedBusy>=1"
assert r["accounts"]==1, "account destroyed while busy"
PY
  [ -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B acct-01 destroyed while busy"
  assert_zero_posts b
  ok "busy peer: teardown deferred, nothing destroyed, ZERO refreshes"

  idle_out="$(hrun b "$BIN/busydefer" --busy=false)"
  echo "    busydefer(idle)=$idle_out"
  python3 - "$idle_out" <<'PY' || fail "idle converge did not tear down"
import json,sys
r=json.loads(sys.argv[1])
assert r["accounts"]==0, "account not torn down after the session cleared"
PY
  [ ! -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B acct-01 survived idle teardown"
  [ ! -f "$(credfile b)" ] || fail "B credential survived idle teardown"
  ok "idle: teardown completed (dir + credential gone)"

  assert_zero_posts b
  assert_no_double_spend
  assert_no_foreign_rt b "$A_RT_FAMILY"
}

# ---------------------------------------------------------------------------
# Scenario 3 — origin offline: the peer degrades to "origin stale"
# ---------------------------------------------------------------------------
# Own setup: the sim's rpc-serve auto-resurrects a stopped daemon (EnsureRunning),
# so "origin offline" is modeled as an unreachable origin — B's origin and peer
# transports are pointed at a dead endpoint (a genuine partition from B's view).
scenario_3_origin_offline() {
  hdr "Scenario 3: origin offline — peer degrades to 'origin stale', still ZERO posts"
  materialize_peer
  [ "$(cred_get "$(credfile b)" accessToken)" = "$A_AT" ] || fail "B not synced before the outage"

  stop_daemon b
  # Partition B from the origin: dead peer transport + dead origin in B's registry.
  write_state b "$(exec_peer b)" "$DEAD_PEER"
  python3 - "$(regfile b)" "$UUID" <<'PY'
import json,sys
p,uuid=sys.argv[1],sys.argv[2]
reg=json.load(open(p))
reg[uuid]["value"]["chain"]["origin"]="exec:false"
json.dump(reg,open(p,"w"))
PY
  # Expire B's synced copy (still stripped: no refresh token reappears).
  hrun b "$BIN/seed" setexp --id 1 --expires-ms "$E_PAST" >/dev/null
  ! grep -qF "$RT_MARKER" "$(credfile b)" || fail "expiring B's copy reintroduced a refresh token"
  start_daemon b

  # The daemon polls at startup: an expired synced token is unrefreshable here,
  # so it flags awaiting-origin. Wait for the user-visible flag to land.
  local seen=0
  for _ in $(seq 1 60); do
    if hrun b "$BIN/cc-pool" status --plain 2>/dev/null | grep -qF "origin stale"; then seen=1; break; fi
    sleep 0.5
  done
  [ "$seen" = 1 ] || { hrun b "$BIN/cc-pool" status --plain 2>&1 | sed 's/^/    /' >&2; fail "B never surfaced 'origin stale'"; }
  ok "B surfaced 'origin stale' (AwaitingOrigin) with the origin unreachable"

  # Score sinks: NeedsLogin penalty applies to both kinds.
  local score; score="$(hrun b "$BIN/cc-pool" status --json 2>/dev/null \
    | python3 -c "import json,sys;d=json.load(sys.stdin);print(next(a['score'] for a in d['accounts']))" 2>/dev/null || echo "")"
  echo "    B awaiting-origin score = ${score:-<unreadable>}"

  assert_zero_posts b
  assert_no_double_spend
  assert_no_foreign_rt b "$A_RT_FAMILY"
}

# ---------------------------------------------------------------------------
# Scenario 4 — peer becomes origin; both hosts refresh independently
# ---------------------------------------------------------------------------
scenario_4_peer_origin() {
  hdr "Scenario 4: peer becomes origin — both refresh their OWN chain, owned never overwritten"
  materialize_peer
  [ "$(cred_get "$(credfile b)" accessToken)" = "$A_AT" ] || fail "B not synced before login"

  # B logs in a fresh independent chain (RT-bearing, fresher expiry) and scan-
  # publishes — models `ccp login` on B. Register cb with B's own fake-oauth.
  hrun b "$BIN/seed" rotate --id 1 --access "$B_AT" --refresh "$B_RT" --expires-ms "$E_B_INIT" >/dev/null
  register_oauth b "$B_FAMILY" 1 "$B_AT" "$B_RT" "$E_B_INIT"
  converge b >/dev/null
  [ "$(reg_get b "$UUID" origin)" = "$(exec_peer b)" ] || fail "registry origin did not flip to B"
  ok "registry ORIGIN flipped to B (B's fresher chain won the scan-publish)"
  grep -qF "$B_RT_FAMILY" "$(credfile b)" || fail "B is not owning its new chain"
  ok "B LOCAL = owned (holds its own refresh token)"

  # A converges: it owns c1, so it must NOT install B's stamp over its own blob.
  converge a >/dev/null
  [ "$(cred_get "$(credfile a)" accessToken)" = "$A_AT" ] || fail "A's owned AT was overwritten by B's stamp"
  [ "$(cred_get "$(credfile a)" refreshToken)" = "$A_RT" ] || fail "A's owned RT was overwritten"
  ok "A's owned blob NEVER overwritten by B's synced stamp"
  [ "$(reg_get b "$UUID" origin)" = "$(exec_peer b)" ] || fail "origin no longer B after A converged"

  # Both refresh INDEPENDENTLY, each only against its OWN chain / fake-oauth.
  force_refresh a "ATOK-c1-g2"
  force_refresh b "ATOK-cb-g2"
  ok "A rotated c1 (ATOK-c1-g2) and B rotated cb (ATOK-cb-g2) — each its own chain"

  # No cross-spend: each fake-oauth saw only its own family; neither holds the
  # other's refresh token.
  [ "$(oauth_field a tokenPosts)" != "0" ] || fail "A made no token POST"
  [ "$(oauth_field b tokenPosts)" != "0" ] || fail "B (now an origin) made no token POST"
  assert_no_foreign_rt a "$B_RT_FAMILY"
  assert_no_foreign_rt b "$A_RT_FAMILY"
  assert_no_double_spend
  ok "both origins rotated their own single-use chains — no cross-spend, no double-spend"
}

# ---------------------------------------------------------------------------
# Scenario 7 — a v1 (holder/lease) registry fails the schema gate loud
# ---------------------------------------------------------------------------
scenario_7_schema_gate() {
  hdr "Scenario 7: schema gate — a v1 registry fails loud with the runbook message"
  # Plant a pre-origin (v1) registry with holder/lease/parentHash markers.
  cat > "$(regfile a)" <<PY
{"$UUID":{"value":{"uuid":"$UUID","email":"$EMAIL","label":"acct-x","oauthAccount":{"accountUuid":"$UUID","emailAddress":"$EMAIL"},"chain":{"holder":"legacy-host","expiresAt":$E_INIT,"hash":"deadbeef","parentHash":"cafebabe","lease":0}},"added_at":1000,"removed_at":0}}
PY
  local out rc=0
  out="$(hrun a "$BIN/cc-pool" sync converge 2>&1)" || rc=$?
  [ "$rc" != 0 ] || fail "converge accepted a v1 (holder/lease) registry"
  echo "$out" | grep -qF "pre-origin registry" \
    || fail "converge failed but without the ErrRegistrySchema runbook message: $out"
  ok "v1 registry rejected loud: $(echo "$out" | grep -oF 'pre-origin registry' | head -1) ... (upgrade + delete registry.json)"
}

# ---------------------------------------------------------------------------
# One full suite run
# ---------------------------------------------------------------------------
run_suite() {
  local n="$1"
  echo
  echo "############################################################"
  echo "# SUITE RUN $n/$RUNS"
  echo "############################################################"

  # Group 1 (linear, non-destructive): materialize, rotate, tombstone-heal, remove.
  setup_hosts
  scenario_1_materialize
  scenario_2_rotation
  scenario_5_tombstone_heal
  scenario_6_remove_busy

  # Group 2 (own setup — destructive to B's view): origin offline.
  setup_hosts
  scenario_3_origin_offline

  # Group 3 (own setup): peer becomes origin, then the schema gate on those hosts.
  setup_hosts
  scenario_4_peer_origin
  scenario_7_schema_gate

  stop_daemon a; stop_daemon b
  stop_all_oauth
  pkill -f "$BIN/cc-pool" 2>/dev/null || true
  pkill -f "$BIN/fakeoauth" 2>/dev/null || true
  sleep 0.4
  echo
  echo ">>> SUITE RUN $n: ALL SCENARIOS PASSED"
}

# ---------------------------------------------------------------------------
# Preflight: build once, install shims
# ---------------------------------------------------------------------------
command -v python3 >/dev/null || { echo "python3 required" >&2; exit 2; }
command -v curl >/dev/null || { echo "curl required" >&2; exit 2; }

rm -rf "$SIM"
mkdir -p "$BIN"
echo "Building cc-pool + sim helpers into $BIN ..."
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/cc-pool" ./cmd/cc-pool )
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/seed" ./scripts/sync-sim/seed )
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/busydefer" ./scripts/sync-sim/busydefer )
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/fakeoauth" ./scripts/sync-sim/fakeoauth )
cp "$REPO/scripts/sync-sim/shims/fake-security" "$BIN/fake-security"
cp "$REPO/scripts/sync-sim/shims/synckitd" "$BIN/synckitd"
chmod +x "$BIN/fake-security" "$BIN/synckitd"
ok "build complete"

for n in $(seq 1 "$RUNS"); do
  run_suite "$n"
done

echo
echo "############################################################"
echo "# ALL $RUNS SUITE RUNS PASSED"
echo "############################################################"

# Safety: prove no sim process is left running.
sleep 0.3
leftover="$(ps -Ao pid,command | grep -F "$BIN/" | grep -v ' grep ' || true)"
if [ -n "$leftover" ]; then
  echo "WARNING: sim processes still running:" >&2
  echo "$leftover" >&2
else
  ok "no sim processes left running"
fi
