# shellcheck shell=bash
# verify-tcc-upgrade-reboot.sh — prove one signed identity survives replacement and reboot.
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

signature_snapshot() {
  local output="$1"
  vm_ssh "for bundle in \
    '$VMCTL_GUEST_APP' \
    '$VMCTL_GUEST_APP/Contents/PlugIns/CCPoolFileProvider.appex' \
    '$VMCTL_GUEST_APP/Contents/PlugIns/CCPoolStatusWidget.appex'; do \
      /usr/bin/codesign -d -r- \"\$bundle\" 2>&1; \
    done" >"$output" \
    || die "could not snapshot installed designated requirements"
}

assert_signed_topology() {
  vm_ssh "APP_PATH='$VMCTL_GUEST_APP' TEAM_ID=SXKCTF23Q2 APP_GROUP='$APP_GROUP' \
    CLI_PATH='$VMCTL_GUEST_CCPOOL' bash '$TOPOLOGY_PROBE'" \
    || die "installed signed-app topology is invalid"
}

launch_and_assert_runtime() {
  local phase="$1" pid
  vm_phase "$phase"
  require_seconds "$phase"
  vm_ssh "open -gj '$VMCTL_GUEST_APP'" || die "could not launch $VMCTL_GUEST_APP"
  # shellcheck disable=SC2016 # the loops and PID expansion intentionally run in the guest
  pid="$(vm_ssh 'for _ in $(seq 1 100); do
    for candidate in $(pgrep -x CCPoolStatus); do
      command=$(ps -p "$candidate" -o command=)
      case "$command" in *--fusekit-*) continue ;; esac
      echo "$candidate"
      exit 0
    done
    sleep 0.2
  done
  exit 1')" || die "CCPoolStatus did not stay running during $phase"
  APP_PID="$(printf '%s' "$pid" | tr -dc '0-9')"
  [[ -n "$APP_PID" ]] || die "CCPoolStatus launch returned no PID during $phase"

  vm_ssh "for _ in \$(seq 1 100); do
    sockets=\$(/usr/sbin/lsof -a -n -p '$APP_PID' -U 2>/dev/null)
    if grep -Fq '$BROKER_SOCKET' <<<\"\$sockets\" && grep -Fq '$HOLDER_SOCKET' <<<\"\$sockets\"; then
      exit 0
    fi
    sleep 0.2
  done
  exit 1" || die "signed app did not bind both broker and runtime sockets during $phase"

  vm_ssh "! ps -ax -o command= | grep '[f]usekit-native-v1'" \
    || die "File Provider-only holder launched a native filesystem child during $phase"
}

vm_scp_to "$SCRIPT_DIR/tcc-snapshot.sh" "$TCC_PROBE" \
  || die "could not stage the TCC snapshot probe"
vm_scp_to "$SCRIPT_DIR/../assert-signed-topology.sh" "$TOPOLOGY_PROBE" \
  || die "could not stage the signed-topology assertions"

vm_phase upgrade-baseline
require_seconds upgrade-baseline
assert_signed_topology
launch_and_assert_runtime baseline-runtime
tcc_snapshot "$VMCTL_RESULTS_DIR/tcc-before-upgrade.txt"
signature_snapshot "$VMCTL_RESULTS_DIR/requirements-before-upgrade.txt"
baseline_version="$(vm_ssh "/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' '$VMCTL_GUEST_APP/Contents/Info.plist'")" \
  || die "could not read the baseline app version"
IFS=. read -r version_major version_minor version_patch <<<"$baseline_version"
[[ "$version_major" =~ ^[0-9]+$ && "$version_minor" =~ ^[0-9]+$ && "$version_patch" =~ ^[0-9]+$ ]] \
  || die "baseline app version is not dotted numeric: $baseline_version"
UPGRADE_VERSION="${VMCTL_UPGRADE_APP_VERSION:-${version_major}.${version_minor}.$((version_patch + 1))}"
[[ "$UPGRADE_VERSION" != "$baseline_version" ]] \
  || die "upgrade version must differ from installed version $baseline_version"

vm_phase in-place-upgrade
require_seconds in-place-upgrade
base_rev="$(vm_ssh cat "$VMCTL_GUEST_DIR/BUILD_REV")" \
  || die "installed build has no BUILD_REV"
BUILD_REV="${base_rev}-upgrade-${UPGRADE_VERSION}" VMCTL_APP_VERSION="$UPGRADE_VERSION" \
  "$SCRIPT_DIR/push.sh" || die "in-place signed-app upgrade failed"
APP_PID=""
assert_signed_topology
launch_and_assert_runtime upgraded-runtime
tcc_snapshot "$VMCTL_RESULTS_DIR/tcc-after-upgrade.txt"
signature_snapshot "$VMCTL_RESULTS_DIR/requirements-after-upgrade.txt"
cmp -s "$VMCTL_RESULTS_DIR/tcc-before-upgrade.txt" "$VMCTL_RESULTS_DIR/tcc-after-upgrade.txt" \
  || die "in-place signed-app upgrade changed protected-filesystem TCC rows"
cmp -s "$VMCTL_RESULTS_DIR/requirements-before-upgrade.txt" "$VMCTL_RESULTS_DIR/requirements-after-upgrade.txt" \
  || die "in-place signed-app upgrade changed a designated requirement"

installed_version="$(vm_ssh "/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' '$VMCTL_GUEST_APP/Contents/Info.plist'")" \
  || die "could not read the upgraded app version"
[[ "$installed_version" == "$UPGRADE_VERSION" ]] \
  || die "upgrade installed version $installed_version, want $UPGRADE_VERSION"

vm_phase intentional-reboot
require_seconds intentional-reboot
boot_before="$(vm_boottime)" || die "could not read pre-reboot boottime"
: >"$VMCTL_RESULTS_DIR/intentional-reboot"
vm_sudo "/sbin/shutdown -r now" >/dev/null 2>&1 || true
sleep 5
vm_ip_forget
vm_wait_ssh 600 || die "guest did not return after the intentional reboot"
vm_assert_guest
boot_after="$(vm_boottime)" || die "could not read post-reboot boottime"
[[ "$boot_after" != "$boot_before" ]] || die "intentional reboot did not change boottime"

APP_PID=""
assert_signed_topology
launch_and_assert_runtime post-reboot-runtime
tcc_snapshot "$VMCTL_RESULTS_DIR/tcc-after-reboot.txt"
signature_snapshot "$VMCTL_RESULTS_DIR/requirements-after-reboot.txt"
cmp -s "$VMCTL_RESULTS_DIR/tcc-before-upgrade.txt" "$VMCTL_RESULTS_DIR/tcc-after-reboot.txt" \
  || die "reboot changed protected-filesystem TCC rows"
cmp -s "$VMCTL_RESULTS_DIR/requirements-before-upgrade.txt" "$VMCTL_RESULTS_DIR/requirements-after-reboot.txt" \
  || die "reboot changed a designated requirement"

log "verify-tcc-upgrade-reboot: signed payload changed version, rebooted, and retained exact requirements and TCC rows"
