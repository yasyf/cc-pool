# shellcheck shell=bash
# verify-signed-topology.sh — prove the fixed signed-app/App Group boundary
# without requiring any provisioned tenant.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

APP_GROUP="SXKCTF23Q2.ccp"
BROKER_SOCKET="$VM_GUEST_HOME/Library/Group Containers/$APP_GROUP/fusekit.sock"
HOLDER_SOCKET="$VM_GUEST_HOME/.cc-pool/fusekit/fusekit.sock"
TCC_PROBE="$VMCTL_GUEST_DIR/tcc-snapshot.sh"
TOPOLOGY_PROBE="$VMCTL_GUEST_DIR/assert-signed-topology.sh"
APP_PID=""

cleanup() {
  if [[ -n "$APP_PID" ]]; then
    vm_ssh "kill '$APP_PID' 2>/dev/null; true" >/dev/null 2>&1 || true
  fi
  vm_ssh "osascript -e 'quit app \"CCPoolStatus\"' 2>/dev/null; pkill -x CCPoolStatus 2>/dev/null; true" \
    >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_seconds() {
  (($(vm_seconds_left) > 0)) || die "run window elapsed before $1 — raise VMCTL_RUN_TIMEOUT_MIN"
}

# Copy both TCC databases before querying them. Direct sqlite reads can miss WAL
# rows, and root is required to copy the system database on a clean VM.
vm_scp_to "$SCRIPT_DIR/tcc-snapshot.sh" "$TCC_PROBE" \
  || die "could not stage the TCC snapshot probe"

tcc_snapshot() {
  local output="$1"
  vm_sudo "env HOME='$VM_GUEST_HOME' bash '$TCC_PROBE'" \
    | LC_ALL=C sort >"$output" \
    || die "could not snapshot the guest TCC databases"
  if grep -Fq '|daemon|' "$output"; then
    grep -F '|daemon|' "$output" >&2
    die "unsigned cc-pool daemon acquired a protected-filesystem TCC row"
  fi
}

# Release and VM use the same exact signature, entitlement, and pure-Go
# boundary assertions.
vm_phase signatures
require_seconds signatures
vm_scp_to "$SCRIPT_DIR/../assert-signed-topology.sh" "$TOPOLOGY_PROBE" \
  || die "could not stage the signed-topology assertions"
vm_ssh "APP_PATH='$VMCTL_GUEST_APP' TEAM_ID=SXKCTF23Q2 APP_GROUP='$APP_GROUP' \
  CLI_PATH='$VMCTL_GUEST_CCPOOL' bash '$TOPOLOGY_PROBE'" \
  || die "installed signed-app topology is invalid"

# Cold-launch only the fixed signed app. The embedded holder keeps the broker's
# outbound session alive; no fake server or provisioned tenant is involved.
vm_phase broker-cold-launch
require_seconds broker-cold-launch

vm_ssh "osascript -e 'quit app \"CCPoolStatus\"' 2>/dev/null; pkill -x CCPoolStatus 2>/dev/null; true"
sleep 2
tcc_snapshot "$VMCTL_RESULTS_DIR/tcc-before.txt"

vm_ssh "open -gj '$VMCTL_GUEST_APP'" \
  || die "could not cold-launch $VMCTL_GUEST_APP"
# shellcheck disable=SC2016 # the loop and PID expansion intentionally run in the guest
APP_PID="$(vm_ssh 'for _ in $(seq 1 100); do
  for pid in $(pgrep -x CCPoolStatus); do
    command=$(ps -p "$pid" -o command=)
    case "$command" in *--fusekit-*) continue ;; esac
    echo "$pid"
    exit 0
  done
  sleep 0.2
done
exit 1')" \
  || die "CCPoolStatus did not stay running after launch"
APP_PID="$(printf '%s' "$APP_PID" | tr -dc '0-9')"
[[ -n "$APP_PID" ]] || die "CCPoolStatus launch returned no PID"

# Observe the app-owned Unix listener through its fd table. The test process
# never opens or traverses the Group Container itself, avoiding a false sshd
# protected-filesystem authorization event.
vm_ssh "for _ in \$(seq 1 100); do
  sockets=\$(/usr/sbin/lsof -a -n -p '$APP_PID' -U 2>/dev/null)
  if grep -Fq '$BROKER_SOCKET' <<<\"\$sockets\" && grep -Fq '$HOLDER_SOCKET' <<<\"\$sockets\"; then
    exit 0
  fi
  sleep 0.2
done
exit 1" \
  || die "fixed signed app did not bind both $BROKER_SOCKET and $HOLDER_SOCKET"

vm_ssh "! ps -ax -o command= | grep '[f]usekit-native-v1'" \
  || die "File Provider-only holder launched a native filesystem child"

vm_sudo "launchctl procinfo '$APP_PID'" >"$VMCTL_RESULTS_DIR/host-procinfo.txt" 2>&1 \
  || die "could not inspect the kernel-validated CCPoolStatus process identity"
grep -Fq "$APP_GROUP" "$VMCTL_RESULTS_DIR/host-procinfo.txt" \
  || die "running CCPoolStatus lacks the kernel-validated $APP_GROUP entitlement"

tcc_snapshot "$VMCTL_RESULTS_DIR/tcc-after.txt"
if ! cmp -s "$VMCTL_RESULTS_DIR/tcc-before.txt" "$VMCTL_RESULTS_DIR/tcc-after.txt"; then
  diff -u "$VMCTL_RESULTS_DIR/tcc-before.txt" "$VMCTL_RESULTS_DIR/tcc-after.txt" >&2 || true
  die "cold broker launch changed the fixed app/File Provider TCC rows"
fi

log "verify-signed-topology: one fixed signed host owned holder, broker, and native child with unchanged TCC state; the account daemon has no App Group capability"
