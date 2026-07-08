#!/usr/bin/env bash
#
# Two-host cross-host sync sim for cc-pool. Drives the full host-sync story
# between two simulated Macs ("a" and "b") that share nothing but a hand-written
# synckit mesh, exercising the REAL binary end to end: the daemon's sync socket,
# the `sync rpc-serve` stdio bridge (the exec: peer transport), converge,
# credential pull over ccp.fetch_credential, materialize, and teardown.
#
# HARD SAFETY: every process runs with HOME (and XDG_CONFIG_HOME) inside
# /tmp/ccp-sim/{a,b}; credentials are file-backed with FAKE tokens; the real
# login Keychain is never touched (CLAUDE_POOL_SECURITY_BIN points at a shim
# that forces the file backend); all network egress is black-holed to a dead
# localhost proxy; no launchctl, no `ccp service`. Every daemon this script
# starts is killed on exit.
#
# Usage:  scripts/sync-sim/run.sh [--runs N] [--keep]
#   --runs N   run the whole 6-scenario suite N times from clean state (default 2)
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
# Fake credentials + identity (fixed, distinctive so token-leak greps are exact)
# ---------------------------------------------------------------------------
UUID="1f1e1d1c-0b0a-4090-8807-060504030201"
EMAIL="acct-x@sim.example"

AC1="SIMACCESS-c1-0a1b2c3d4e"; RT1="SIMREFRESH-c1-0a1b2c3d4e"
AC2="SIMACCESS-c2-1a2b3c4d5e"; RT2="SIMREFRESH-c2-1a2b3c4d5e"
AC3="SIMACCESS-c3-2a3b4c5d6e"; RT3="SIMREFRESH-c3-2a3b4c5d6e"
AC5="SIMACCESS-c5-3a4b5c6d7e"; RT5="SIMREFRESH-c5-3a4b5c6d7e"
ALL_TOKENS=("$AC1" "$RT1" "$AC2" "$RT2" "$AC3" "$RT3" "$AC5" "$RT5")

NOW_MS=$(( $(date +%s) * 1000 ))
DAY_MS=86400000
E1=$(( NOW_MS + 100*DAY_MS ))   # c1 expiry
E2=$(( NOW_MS + 200*DAY_MS ))   # c2 expiry: strictly later than c1
E3=$(( NOW_MS + 150*DAY_MS ))   # c3 expiry: EARLIER than c2 (the skew inversion), still future
E5=$(( NOW_MS + 120*DAY_MS ))   # c5 expiry (re-add)

# ---------------------------------------------------------------------------
# Process/daemon bookkeeping + cleanup
# ---------------------------------------------------------------------------
DAEMON_PIDS=()

cleanup() {
  set +e
  for pid in "${DAEMON_PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  # Net for EnsureRunning-spawned daemons and any rpc-serve stragglers.
  pkill -f "$BIN/cc-pool" 2>/dev/null
  sleep 0.3
  pkill -9 -f "$BIN/cc-pool" 2>/dev/null
  if [ "$KEEP" = 0 ]; then
    rm -rf "$SIM/a" "$SIM/b" "$SIM/logs" "$SIM/run"
  fi
}
trap cleanup EXIT

fail() { echo "  ✗ FAIL: $*" >&2; exit 1; }
ok()   { echo "  ✓ $*"; }
hdr()  { echo; echo "=== $* ==="; }

# ---------------------------------------------------------------------------
# Per-host command runner and exec: peer transport strings
# ---------------------------------------------------------------------------
# hrun HOST CMD...  — run CMD with HOST's sandboxed environment.
hrun() {
  local h="$1"; shift
  env -i \
    HOME="$SIM/$h" \
    XDG_CONFIG_HOME="$SIM/$h/.config" \
    TMPDIR="$SIM/$h/tmp" \
    CLAUDE_POOL_SECURITY_BIN="$BIN/fake-security" \
    HTTPS_PROXY="http://127.0.0.1:1" HTTP_PROXY="http://127.0.0.1:1" NO_PROXY="" \
    PATH="$BIN:/usr/bin:/bin:/usr/sbin:/sbin" \
    USER="$SIMUSER" \
    "$@"
}

# exec_peer HOST — the exec: transport a peer uses to reach HOST's rpc-serve
# bridge. It carries HOST's full sandboxed env so the spawned bridge resolves
# HOST's own state, Keychain shim, and dead proxy — not the caller's.
exec_peer() {
  local h="$1"
  printf 'exec:env -i HOME=%s XDG_CONFIG_HOME=%s TMPDIR=%s CLAUDE_POOL_SECURITY_BIN=%s HTTPS_PROXY=http://127.0.0.1:1 HTTP_PROXY=http://127.0.0.1:1 PATH=%s:/usr/bin:/bin:/usr/sbin:/sbin USER=%s %s sync rpc-serve' \
    "$SIM/$h" "$SIM/$h/.config" "$SIM/$h/tmp" "$BIN/fake-security" "$BIN" "$SIMUSER" "$BIN/cc-pool"
}

# write_state HOST SELF_PEER PEER_PEER — hand-write HOST's synckit mesh
# state.json: self is HOST's own exec: transport (so peers dialing the chain
# holder reach it directly), hosts is the other host's exec: transport.
write_state() {
  local h="$1" selfp="$2" peerp="$3"
  mkdir -p "$SIM/$h/.config/synckit"
  printf '{\n  "self": "%s",\n  "hosts": [\n    "%s"\n  ]\n}\n' "$selfp" "$peerp" \
    > "$SIM/$h/.config/synckit/state.json"
}

# start_daemon HOST — launch HOST's cc-pool daemon (logging to logs/), record
# its pid, and wait for its sync socket to bind. Pre-starting both daemons means
# the peer rpc-serve bridges find them Available and never spawn extras.
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

# stop_daemon HOST — kill HOST's tracked daemon by pid and wait for it to exit,
# so a poll can't resurrect a just-removed account's overlay dir underfoot.
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

# ---------------------------------------------------------------------------
# Registry + credential inspection helpers (python3 for structured reads)
# ---------------------------------------------------------------------------
regfile() { echo "$SIM/$1/.cc-pool/sync/registry.json"; }
credfile() { echo "$SIM/$1/.cc-pool/accounts/acct-01/.credentials.json"; }
identfile() { echo "$SIM/$1/.cc-pool/accounts/acct-01/.claude.json"; }

# reg_get HOST UUID DOTTED — read a field of an entry: DOTTED is present |
# hash | holder | parenthash | expiresat | added | removed. Prints MISSING if
# the uuid has no entry.
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
 "holder": ch["holder"],
 "parenthash": ch["parentHash"],
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
print(c[sys.argv[2]])
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

# Assert a registry.json holds NONE of the fake token substrings (secretless).
assert_token_clean() {
  local h="$1" rf; rf="$(regfile "$h")"
  [ -f "$rf" ] || fail "registry $h missing for token check"
  for t in "${ALL_TOKENS[@]}"; do
    if grep -qF "$t" "$rf"; then fail "token '$t' leaked into $h registry.json"; fi
  done
  ok "registry $h is token-clean (no access/refresh substrings)"
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

# ---------------------------------------------------------------------------
# One full suite run from clean host state
# ---------------------------------------------------------------------------
setup_hosts() {
  rm -rf "$SIM/a" "$SIM/b" "$SIM/logs" "$SIM/run"
  mkdir -p "$SIM/a/tmp" "$SIM/b/tmp" "$SIM/logs" "$SIM/run"
  DAEMON_PIDS=()

  local pa pb
  pa="$(exec_peer a)"; pb="$(exec_peer b)"

  for h in a b; do hrun "$h" "$BIN/seed" init >/dev/null; done
  write_state a "$pa" "$pb"
  write_state b "$pb" "$pa"

  # Host A owns account X with chain c1; row uuid left empty (enable backfills it).
  hrun a "$BIN/seed" account --id 1 --uuid "$UUID" --email "$EMAIL" \
    --access "$AC1" --refresh "$RT1" --expires-ms "$E1" --label "acct-x" >/dev/null

  for h in a b; do hrun "$h" "$BIN/cc-pool" sync enable >/dev/null; done

  start_daemon a
  start_daemon b
}

scenario_1() {
  hdr "Scenario 1: enable + materialize (host A's account X appears on host B)"
  # Enable already scan-published X on A and backfilled A's row uuid.
  [ "$(hrun a "$BIN/seed" rowuuid --id 1)" = "$UUID" ] \
    && ok "host A row uuid backfilled by 'sync enable' ($UUID)" \
    || fail "host A row uuid not backfilled"
  [ "$(reg_get a "$UUID" present)" = "yes" ] || fail "X not present in A registry after enable"

  quiesce

  [ -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "acct-01 dir not materialized on B"
  ok "B materialized acct-01 dir"
  [ "$(ident_get "$(identfile b)" accountUuid)" = "$UUID" ] || fail "B identity accountUuid mismatch"
  [ "$(ident_get "$(identfile b)" emailAddress)" = "$EMAIL" ] || fail "B identity email mismatch"
  ok "B identity verbatim (accountUuid + emailAddress match A)"
  [ "$(cred_get "$(credfile b)" accessToken)" = "$AC1" ] || fail "B access token != c1"
  [ "$(cred_get "$(credfile b)" refreshToken)" = "$RT1" ] || fail "B refresh token != c1"
  [ "$(hrun a "$BIN/seed" hash --id 1)" = "$(hrun b "$BIN/seed" hash --id 1)" ] || fail "B chain hash != A (c1)"
  ok "B credential == c1 (tokens + hash match A)"
  [ "$(hrun b "$BIN/seed" rowuuid --id 1)" = "$UUID" ] || fail "B row uuid not backfilled"
  ok "B row present + uuid backfilled"

  assert_reg_identical
  assert_token_clean a
  assert_token_clean b
}

scenario_2() {
  hdr "Scenario 2: rotation propagates (A rotates to c2, B installs c2)"
  hrun a "$BIN/seed" rotate --id 1 --access "$AC2" --refresh "$RT2" --expires-ms "$E2" >/dev/null
  local c1hash; c1hash="$(reg_get a "$UUID" hash)"   # registry still c1 pre-converge
  converge a >/dev/null                               # A scan-folds c2 into its registry
  [ "$(reg_get a "$UUID" hash)" != "$c1hash" ] || fail "A registry did not advance to c2"
  [ "$(reg_get a "$UUID" parenthash)" = "$c1hash" ] || fail "c2 parentHash != hash(c1)"
  ok "A registry advertises c2 (parentHash = hash(c1))"

  quiesce

  local ahash; ahash="$(hrun a "$BIN/seed" hash --id 1)"
  [ "$(cred_get "$(credfile b)" accessToken)" = "$AC2" ] || fail "B did not install c2 access token"
  [ "$(hrun b "$BIN/seed" hash --id 1)" = "$ahash" ] || fail "B chain hash != A (c2)"
  ok "B installed c2 (fresher chain pulled from A)"
  # A must never regress to c1.
  [ "$(cred_get "$(credfile a)" accessToken)" = "$AC2" ] || fail "A regressed off c2"
  [ "$(reg_get a "$UUID" hash)" = "$ahash" ] || fail "A registry hash != c2"
  ok "A never regressed (still c2)"

  assert_reg_identical
  assert_token_clean a
  assert_token_clean b
}

scenario_3() {
  hdr "Scenario 3: skew inversion — lineage beats a lower expiry (B rotates to c3, A installs it)"
  local c2hash; c2hash="$(hrun b "$BIN/seed" hash --id 1)"       # == c2
  hrun b "$BIN/seed" rotate --id 1 --access "$AC3" --refresh "$RT3" --expires-ms "$E3" >/dev/null
  converge b >/dev/null                                          # B scan-folds c3 despite lower expiry
  local c3hash; c3hash="$(hrun b "$BIN/seed" hash --id 1)"
  [ "$(reg_get b "$UUID" hash)" = "$c3hash" ] || fail "B registry did not advance to c3"
  [ "$(reg_get b "$UUID" parenthash)" = "$c2hash" ] || fail "c3 parentHash != hash(c2)"
  # Prove the inversion: c3 expiry is EARLIER than c2 expiry.
  [ "$E3" -lt "$E2" ] || fail "sim misconfig: E3 not < E2"
  ok "B advertises c3 (parentHash = hash(c2)) even though c3 expiry ($E3) < c2 expiry ($E2)"

  quiesce

  [ "$(cred_get "$(credfile a)" accessToken)" = "$AC3" ] || fail "A did not install c3"
  [ "$(hrun a "$BIN/seed" hash --id 1)" = "$c3hash" ] || fail "A chain hash != c3"
  ok "A installed c3 (child-of-advertised beat the lower expiry)"
  # A must never re-serve c2 as 'fresher' after adopting its own child.
  quiesce
  [ "$(hrun a "$BIN/seed" hash --id 1)" = "$c3hash" ] || fail "A reverted off c3"
  [ "$(reg_get a "$UUID" hash)" = "$c3hash" ] || fail "registry reverted off c3"
  [ "$(cred_get "$(credfile b)" accessToken)" = "$AC3" ] || fail "B not on c3"
  ok "A never re-served c2 as fresher (registry + both creds pinned to c3)"

  assert_reg_identical
  assert_token_clean a
  assert_token_clean b
}

scenario_4() {
  hdr "Scenario 4: remove propagates (A removes X, B tears down)"
  # Quiesce A's daemon around the removal: its ~20s poll re-asserts overlays and
  # would race the local teardown, so stop it, remove, then restart it fresh
  # (post-remove store has no acct-01, so it polls nothing to resurrect).
  stop_daemon a
  hrun a "$BIN/cc-pool" remove 1 >/dev/null
  start_daemon a
  [ "$(reg_get a "$UUID" present)" = "no" ] || fail "A registry did not tombstone X"
  [ ! -d "$SIM/a/.cc-pool/accounts/acct-01" ] || fail "A local acct-01 dir survived remove"
  ok "A tombstoned X and tore down its local copy"

  quiesce

  [ ! -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B acct-01 dir survived teardown"
  [ ! -f "$(credfile b)" ] || fail "B credential file survived teardown"
  [ -z "$(hrun b "$BIN/seed" rowuuid --id 1 2>/dev/null || true)" ] || fail "B row survived teardown"
  ok "B tore down (dir, credential file, and row all gone)"
  [ "$(reg_get a "$UUID" present)" = "no" ] || fail "A lost the tombstone"
  [ "$(reg_get b "$UUID" present)" = "no" ] || fail "B lost the tombstone"
  ok "tombstone still carried on both a and b"

  assert_reg_identical
  assert_token_clean a
  assert_token_clean b
}

scenario_5() {
  hdr "Scenario 5: re-add overrides the tombstone (A re-adds X, B re-materializes)"
  hrun a "$BIN/seed" account --id 1 --uuid "$UUID" --email "$EMAIL" \
    --access "$AC5" --refresh "$RT5" --expires-ms "$E5" --label "acct-x" >/dev/null
  hrun a "$BIN/seed" publish --id 1 >/dev/null          # PublishAccount: force-override the tombstone
  [ "$(reg_get a "$UUID" present)" = "yes" ] || fail "PublishAccount did not override the tombstone"
  ok "A re-added X (PublishAccount overrode the tombstone)"

  quiesce

  [ -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B did not re-materialize acct-01"
  [ "$(cred_get "$(credfile b)" accessToken)" = "$AC5" ] || fail "B not on the re-added chain c5"
  [ "$(hrun b "$BIN/seed" rowuuid --id 1)" = "$UUID" ] || fail "B re-add row uuid missing"
  ok "B re-materialized X on the new chain c5"
  [ "$(reg_get b "$UUID" present)" = "yes" ] || fail "B registry not present after re-add"

  assert_reg_identical
  assert_token_clean a
  assert_token_clean b
}

scenario_6() {
  hdr "Scenario 6: busy defer (a fake live session on B defers teardown, then completes)"
  # A fake live session is not injectable from outside the daemon, so run B's
  # converge in-process with a fake Sessions seam (the sanctioned substitution).
  # Stop B's daemon first so the in-process runner owns B's store; A stays up as
  # the peer.
  stop_daemon b
  stop_daemon a
  hrun a "$BIN/cc-pool" remove 1 >/dev/null
  start_daemon a
  [ "$(reg_get a "$UUID" present)" = "no" ] || fail "A did not tombstone X for the busy test"
  ok "A tombstoned X (peer removal)"

  local busy_out idle_out
  busy_out="$(hrun b "$BIN/busydefer" --busy=true)"
  echo "    busydefer(busy)=$busy_out"
  python3 - "$busy_out" <<'PY' || fail "busy converge did not defer"
import json,sys
r=json.loads(sys.argv[1])
assert r["skippedBusy"]>=1, "expected skippedBusy>=1"
assert r["accounts"]==1, "account was destroyed while busy"
PY
  [ -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B acct-01 dir destroyed while busy"
  [ -f "$(credfile b)" ] || fail "B credential destroyed while busy"
  ok "busy: teardown deferred (SkippedBusy), nothing destroyed"

  idle_out="$(hrun b "$BIN/busydefer" --busy=false)"
  echo "    busydefer(idle)=$idle_out"
  python3 - "$idle_out" <<'PY' || fail "idle converge did not tear down"
import json,sys
r=json.loads(sys.argv[1])
assert r["accounts"]==0, "account not torn down after session cleared"
PY
  [ ! -d "$SIM/b/.cc-pool/accounts/acct-01" ] || fail "B acct-01 dir survived idle teardown"
  [ ! -f "$(credfile b)" ] || fail "B credential survived idle teardown"
  ok "idle: teardown completed (dir + credential gone)"
  [ "$(reg_get a "$UUID" present)" = "no" ] || fail "A lost the tombstone"
  [ "$(reg_get b "$UUID" present)" = "no" ] || fail "B lost the tombstone"
  ok "tombstone still carried on both a and b"
  assert_token_clean a
  assert_token_clean b
}

run_suite() {
  local n="$1"
  echo
  echo "############################################################"
  echo "# SUITE RUN $n/$RUNS"
  echo "############################################################"
  setup_hosts
  scenario_1
  scenario_2
  scenario_3
  scenario_4
  scenario_5
  scenario_6
  # Tear down this run's daemons before the next clean run.
  for pid in "${DAEMON_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  pkill -f "$BIN/cc-pool" 2>/dev/null || true
  sleep 0.4
  echo
  echo ">>> SUITE RUN $n: ALL SCENARIOS PASSED"
}

# ---------------------------------------------------------------------------
# Preflight: build once, install shims
# ---------------------------------------------------------------------------
command -v python3 >/dev/null || { echo "python3 required" >&2; exit 2; }

rm -rf "$SIM"
mkdir -p "$BIN"
echo "Building cc-pool + sim helpers into $BIN ..."
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/cc-pool" ./cmd/cc-pool )
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/seed" ./scripts/sync-sim/seed )
( cd "$REPO" && CGO_ENABLED=0 go build -o "$BIN/busydefer" ./scripts/sync-sim/busydefer )
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

# Safety: prove no sim process is left running. Match on the sim binary path via
# ps (not `pgrep -f`, whose argv-match can flag the very shell running the check).
sleep 0.3
leftover="$(ps -Ao pid,command | grep -F "$BIN/cc-pool" | grep -v ' grep ' || true)"
if [ -n "$leftover" ]; then
  echo "WARNING: cc-pool sim processes still running:" >&2
  echo "$leftover" >&2
else
  ok "no sim cc-pool processes left running"
fi
