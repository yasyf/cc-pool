# shellcheck shell=bash
# scripts/vm/lib.sh — shared plumbing for the disposable tart VM harness.
#
# Adapted from fusekit/scripts/vm/lib.sh (see scripts/vm/README.md § Provenance).
# Sourced by vmctl and push.sh; `vmctl run` sources scenarios with this lib
# already loaded. Every function here executes on the HOST; guest work goes
# through vm_ssh/vm_sudo/vm_scp_to/vm_scp_from. All mutable state lives under
# $VM_ROOT (default /tmp/ccpool-vm) — vm_tart pins TART_HOME under it on every
# invocation, so tart never touches ~/.tart. The tart image cache alone can be
# repointed via VMCTL_TART_HOME to share a warm base-image pull with a sibling
# harness (see VM_TART_HOME below).
#
# Safety: this harness drives File Provider fleet migrations that historically
# wedged fileproviderd, and rides the same kernel-panic-capable macOS overlay
# stack. Nothing here mounts anything on the host, and vm_assert_guest refuses
# any target that is not a VM (kern.hv_vmm_present != 1).

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "lib.sh is a library: source it (see scripts/vm/vmctl)" >&2
  exit 64
fi

# --- Tunables (env-overridable) ----------------------------------------------

export VMCTL_NAME="${VMCTL_NAME:-ccpool-test}"
export VMCTL_IMAGE="${VMCTL_IMAGE:-ghcr.io/cirruslabs/macos-tahoe-base:latest}"
export VMCTL_CPUS="${VMCTL_CPUS:-4}"
export VMCTL_MEMORY_MB="${VMCTL_MEMORY_MB:-8192}"
export VMCTL_DISK_GB="${VMCTL_DISK_GB:-60}"
# 10 min is the standing validation window: the FP-migrate storm this harness
# replays wedged fileproviderd within seconds of the fleet migrate, so a clean
# 10 min run is a decisive pass. Raise it per-invocation for a longer soak.
export VMCTL_RUN_TIMEOUT_MIN="${VMCTL_RUN_TIMEOUT_MIN:-10}"
export VMCTL_GRAPHICS="${VMCTL_GRAPHICS:-0}"

# Non-empty skips the provision-time App-Group TCC pre-seed (no-prompt scenario).
export VMCTL_SKIP_TCC="${VMCTL_SKIP_TCC:-}"

# Daemon App-Group provisioning profile (a `.provisionprofile` path or its
# base64). Set = "profiled" bundle; unset = "unprofiled" (entitlement alone).
export VMCTL_PROFILE_DAEMON="${VMCTL_PROFILE_DAEMON:-}"

# --- Fixed layout: harness state under $VM_ROOT (default /tmp/ccpool-vm) -------

export VM_ROOT="${VM_ROOT:-/tmp/ccpool-vm}"
# The tart image cache + VM disk. Defaults under VM_ROOT so `destroy` wipes it
# with everything else, but VMCTL_TART_HOME can repoint ONLY this cache to share
# the multi-GB pulled base image with a sibling harness — e.g.
# VMCTL_TART_HOME=/tmp/fusekit-vm/tart reuses fusekit's already-warm
# macos-tahoe-base pull. A shared cache lives OUTSIDE VM_ROOT, so `destroy`
# leaves it intact (it only `tart delete`s this harness's own VMCTL_NAME clone);
# keep VMCTL_NAME distinct from any sibling harness sharing the same cache.
export VM_TART_HOME="${VMCTL_TART_HOME:-$VM_ROOT/tart}"
# Belt and braces: exported once here AND pinned per-invocation in vm_tart /
# vm_start, so no code path can reach ~/.tart.
export TART_HOME="$VM_TART_HOME"
# The cc-pool repo root (this lib lives at scripts/vm/lib.sh) — the daemon-bundle
# helpers derive the App Group from internal/pool/paths.go so it cannot drift.
VM_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export VM_REPO_ROOT
VM_SSH_DIR="$VM_ROOT/ssh"
export VM_SSH_KEY="$VM_SSH_DIR/id_ed25519"
export VM_RESULTS_ROOT="$VM_ROOT/results"
VM_LOG_DIR="$VM_ROOT/logs"
export VM_STATE_DIR="$VM_ROOT/state"

export VM_GUEST_USER="admin"
export VM_GUEST_PASS="admin"
export VM_GUEST_HOME="/Users/$VM_GUEST_USER"
export VM_GUEST_MARKER="$VM_GUEST_HOME/.vmctl-run-marker"

# Guest install layout (written by push.sh, consumed by scenarios). The
# CCPoolStatus companion app sits at the production cask path
# (pool.WidgetAppPath() = /Applications/CCPoolStatus.app) so the File Provider
# control/bridge sockets and the extension bundle id resolve unmodified in the
# guest. The cc-pool binary + its ccp symlink live under the guest harness dir;
# scenarios call them by absolute path.
export VMCTL_GUEST_DIR="$VM_GUEST_HOME/ccpool-vm"
export VMCTL_GUEST_BIN="$VMCTL_GUEST_DIR/bin"
export VMCTL_GUEST_CCPOOL="$VMCTL_GUEST_BIN/cc-pool"
export VMCTL_GUEST_CCP="$VMCTL_GUEST_BIN/ccp"
export VMCTL_GUEST_APP="/Applications/CCPoolStatus.app"
# The daemon .app bundle (push installs it; scenarios run its exe). Its
# CFBundleIdentifier is the durable, upgrade-stable TCC key for the group bind.
export VMCTL_DAEMON_BUNDLE_ID="${VMCTL_DAEMON_BUNDLE_ID:-com.yasyf.cc-pool.daemon}"
export VMCTL_GUEST_DAEMON_APP="$VMCTL_GUEST_DIR/CCPoolDaemon.app"
export VMCTL_GUEST_DAEMON_EXE="$VMCTL_GUEST_DAEMON_APP/Contents/MacOS/cc-pool"
# The daemon log the replay scenario scrapes; the daemon-start phase writes it.
export VMCTL_GUEST_DAEMON_LOG="$VMCTL_GUEST_DIR/daemon.log"
# The File Provider extension bundle id (MUST equal pool.FPExtensionBundleID);
# provision/push register+enable it with pluginkit, and cc-pool's FP gate reads
# it back via `pluginkit -m -i`.
export VMCTL_FP_BUNDLE_ID="${VMCTL_FP_BUNDLE_ID:-com.yasyf.cc-pool.status.fileprovider}"

# Space-separated App-Data TCC grantees (kTCCServiceSystemPolicyAppData): the
# non-sandboxed cc-pool daemon reaches into the CCPoolStatus App Group container
# to bind the File Provider bridge socket, which macOS gates behind a one-time
# app-group-data prompt. sshd-keygen-wrapper is the TCC responsible process for
# everything run over ssh; the installed daemon binary is a belt-and-braces
# second grantee, granted by absolute path (client_type 1).
export VMCTL_TCC_APPDATA_CLIENTS="${VMCTL_TCC_APPDATA_CLIENTS:-com.apple.sshd-keygen-wrapper $VMCTL_GUEST_CCPOOL}"

# --- Logging -------------------------------------------------------------------

# log writes a timestamped progress line to stderr.
log() { printf '%s vmctl: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

# warn writes a timestamped warning to stderr.
warn() { printf '%s vmctl: WARN: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

# die writes a fatal message and exits 1 (the harness-wide infra-failure code).
die() {
  printf '%s vmctl: FATAL: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2
  exit 1
}

# require_cmd dies unless $1 is on PATH; $2 is an optional install hint.
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1${2:+ ($2)}"; }

# vm_require_tart dies unless tart is installed (create installs it, loudly).
vm_require_tart() {
  command -v tart >/dev/null 2>&1 || die "tart is not installed; run: vmctl create (installs it via brew, loudly)"
}

# vm_ensure_dirs creates the $VM_ROOT tree.
vm_ensure_dirs() {
  mkdir -p "$VM_TART_HOME" "$VM_SSH_DIR" "$VM_RESULTS_ROOT" "$VM_LOG_DIR" "$VM_STATE_DIR"
}

# --- tart ----------------------------------------------------------------------

# vm_tart runs tart with TART_HOME pinned inside $VM_TART_HOME.
vm_tart() { TART_HOME="$VM_TART_HOME" command tart "$@"; }

# vm_list_field prints one header-named column of the VM's `tart list` row,
# or nothing when the VM has no row. Column positions are resolved from the
# header line, so a tart version reordering or inserting columns cannot
# silently repoint the parse. The LAST header column is read from the row's
# END ($NF): tart 2.32's `tart list` has a multi-word "Accessed" column
# ("4 seconds ago") that shifts every field after it, which made a
# field-indexed State read "seconds" — never "running" — and sent cmd_run's
# watcher into a one-a-second tart-relaunch storm over a perfectly live VM
# (the storm behind the 2026-07-03 polluted-guest diagnosis runs).
vm_list_field() {
  vm_tart list 2>/dev/null | awk -v n="$VMCTL_NAME" -v f="$1" '
    NR == 1 {
      for (i = 1; i <= NF; i++) col[$i] = i
      last = $NF
      next
    }
    col["Name"] && col[f] && $col["Name"] == n { print(f == last ? $NF : $col[f]) }'
}

# vm_exists reports whether the VM has been cloned.
vm_exists() { [[ -n "$(vm_list_field Name)" ]]; }

# vm_is_running reports whether tart itself considers the VM running. This is
# the authoritative liveness signal: the pidfile can point at a relaunched
# `tart run` that lost the race to the surviving owner and exited "already
# running", so keying liveness off the pidfile desyncs and storms.
vm_is_running() { [[ "$(vm_list_field State)" == "running" ]]; }

# vm_start launches `tart run` detached (nohup, pidfile). Mode "headless"
# forces --no-graphics; the default "auto" honors VMCTL_GRAPHICS=1, the
# one-time window used for the TCC click-Allow path.
vm_start() {
  local mode="${1:-auto}" logf
  vm_ensure_dirs
  # tart refuses a second `run` of a VM that is already up ("VM is already
  # running!"); that competitor exits instantly. Adopt the running VM instead
  # of racing it. tart's own run-state (vm_is_running) is the single liveness
  # signal — there is no pidfile to desync.
  if vm_is_running; then
    log "VM $VMCTL_NAME already running; adopting it"
    return 0
  fi
  local args=("run" "$VMCTL_NAME")
  if [[ "$mode" == "headless" || "$VMCTL_GRAPHICS" != "1" ]]; then
    args+=("--no-graphics")
  fi
  logf="$VM_LOG_DIR/tart-run-$(date +%Y%m%d-%H%M%S).log"
  log "starting: tart ${args[*]} (log: $logf)"
  TART_HOME="$VM_TART_HOME" nohup tart "${args[@]}" >>"$logf" 2>&1 &
  disown
}

# --- Guest reachability ---------------------------------------------------------

# vm_ip prints the guest IP, cached per vmctl process; vm_ip_forget drops the
# cache (call it whenever the guest drops, the lease can change across reboots).
vm_ip() {
  if [[ -n "${VM_IP_CACHE:-}" ]]; then
    printf '%s\n' "$VM_IP_CACHE"
    return 0
  fi
  local ip
  ip="$(vm_tart ip "$VMCTL_NAME" 2>/dev/null)" || return 1
  [[ -n "$ip" ]] || return 1
  VM_IP_CACHE="$ip"
  printf '%s\n' "$ip"
}

# vm_ip_forget invalidates the cached guest IP.
vm_ip_forget() { VM_IP_CACHE=""; }

VM_SSH_OPTS=(
  -i "$VM_SSH_KEY"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ConnectTimeout=8
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=3
  -o BatchMode=yes
)

# vm_ssh runs its arguments as a command in the guest (key auth only). Returns
# nonzero when the guest is unreachable — callers own the reaction, the panic
# watcher polls through this. Non-interactive sshd commands get the bare
# /usr/bin:/bin PATH, so Homebrew's prefix is prepended for every command.
vm_ssh() {
  local ip
  ip="$(vm_ip)" || return 1
  # shellcheck disable=SC2029 # helpers take remote command strings built host-side by design
  ssh "${VM_SSH_OPTS[@]}" "$VM_GUEST_USER@$ip" "export PATH=/opt/homebrew/bin:/opt/homebrew/sbin:\$PATH; $*"
}

# vm_ssh_ok reports whether key-based ssh into the guest works right now.
vm_ssh_ok() { vm_ssh true >/dev/null 2>&1; }

# vm_sudo runs a single remote command string as root. Provision established
# passwordless sudo, so -n either works or fails loudly.
vm_sudo() { vm_ssh "sudo -n -- $*"; }

# vm_scp_to copies a local file or directory into the guest: vm_scp_to <local> <remote>.
vm_scp_to() {
  local ip
  ip="$(vm_ip)" || return 1
  scp -q "${VM_SSH_OPTS[@]}" -r "$1" "$VM_GUEST_USER@$ip:$2"
}

# vm_scp_from copies a guest file or directory out: vm_scp_from <remote> <local>.
vm_scp_from() {
  local ip
  ip="$(vm_ip)" || return 1
  scp -q "${VM_SSH_OPTS[@]}" -r "$VM_GUEST_USER@$ip:$1" "$2"
}

# vm_wait_port22 waits up to $1 seconds for the guest to accept TCP on 22.
vm_wait_port22() {
  local timeout="$1" t0 ip
  t0="$(date +%s)"
  while (($(date +%s) - t0 < timeout)); do
    vm_ip_forget
    if ip="$(vm_ip)" && nc -z -G 4 "$ip" 22 >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  return 1
}

# vm_wait_ssh waits up to $1 seconds for key-based ssh to work.
vm_wait_ssh() {
  local timeout="$1" t0
  t0="$(date +%s)"
  while (($(date +%s) - t0 < timeout)); do
    if vm_ssh_ok; then
      return 0
    fi
    vm_ip_forget
    sleep 5
  done
  return 1
}

# vm_ensure_running makes sure the VM process is up and key-ssh works, starting
# tart if needed. Fails (rather than creates) when the VM does not exist.
vm_ensure_running() {
  local timeout="${1:-300}"
  if vm_ssh_ok; then
    return 0
  fi
  vm_exists || die "VM $VMCTL_NAME does not exist — run: vmctl create && vmctl provision"
  if ! vm_is_running; then
    vm_start auto
  fi
  vm_wait_ssh "$timeout"
}

# --- One-time guest bootstrap ---------------------------------------------------

# vm_authorize_ssh_key installs the harness public key into the guest's
# authorized_keys. First contact drives ssh-copy-id's password prompt with
# /usr/bin/expect (ships with macOS; sshpass is not a host dependency); every
# later connection is key-only.
vm_authorize_ssh_key() {
  vm_ensure_dirs
  if [[ ! -f "$VM_SSH_KEY" ]]; then
    log "generating harness ssh key: $VM_SSH_KEY"
    ssh-keygen -q -t ed25519 -N "" -C "ccpool-vmctl" -f "$VM_SSH_KEY"
  fi
  if vm_ssh_ok; then
    log "ssh key already authorized in guest"
    return 0
  fi
  local ip
  ip="$(vm_ip)" || die "cannot resolve guest IP for the ssh-key bootstrap"
  log "installing ssh key via password auth (expect, one time; creds $VM_GUEST_USER/$VM_GUEST_PASS)"
  /usr/bin/expect <<EOF || die "ssh key bootstrap failed (see expect output)"
set timeout 90
spawn /usr/bin/ssh-copy-id -i "$VM_SSH_KEY.pub" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o PubkeyAuthentication=no "$VM_GUEST_USER@$ip"
expect {
    -re {[Pp]assword:} { send -- "$VM_GUEST_PASS\r"; exp_continue }
    timeout { exit 92 }
    eof {}
}
catch wait result
exit [lindex \$result 3]
EOF
  vm_ssh_ok || die "key installed but key-based ssh still fails"
  log "ssh key authorized"
}

# vm_ensure_sudo makes guest sudo passwordless (cirrus images usually ship this
# way; when they do not, install a NOPASSWD rule once — the guest is disposable).
vm_ensure_sudo() {
  if vm_ssh "sudo -n true" >/dev/null 2>&1; then
    return 0
  fi
  log "guest sudo wants a password; installing a NOPASSWD rule (one time, guest-only)"
  # sudo -S consumes line 1 (the password); tee writes line 2 into sudoers.d.
  printf '%s\n%s\n' "$VM_GUEST_PASS" "$VM_GUEST_USER ALL=(ALL) NOPASSWD: ALL" |
    vm_ssh "sudo -S -p '' tee /etc/sudoers.d/vmctl-nopasswd >/dev/null"
  vm_ssh "sudo -n chmod 440 /etc/sudoers.d/vmctl-nopasswd"
  vm_ssh "sudo -n true" >/dev/null 2>&1 || die "could not establish passwordless sudo in the guest"
}

# --- VM-only guard ---------------------------------------------------------------

# vm_assert_guest dies unless the ssh target is a virtual machine. This is the
# structural rail that keeps deliberate panic workloads off bare metal.
vm_assert_guest() {
  local v
  v="$(vm_ssh sysctl -n kern.hv_vmm_present 2>/dev/null)" || die "cannot read kern.hv_vmm_present from the guest"
  v="${v//[^0-9]/}"
  [[ "$v" == "1" ]] || die "ssh target is NOT a VM (kern.hv_vmm_present=$v); refusing to drive fuse workloads"
}

# --- Panic detection and evidence -------------------------------------------------

# vm_boottime prints the guest kern.boottime seconds, or returns 1 when the
# guest is unreachable. A change against the run's baseline means the guest
# rebooted underneath us — on this harness, that is a kernel panic signal.
vm_boottime() {
  local out
  out="$(vm_ssh sysctl -n kern.boottime 2>/dev/null)" || return 1
  # sysctl prints "{ sec = N, usec = N } <date>". Anchor on the opening brace:
  # a bare ".*sec = " greedily matches the trailing "usec = N" field and
  # records microseconds instead of the boot epoch seconds.
  out="$(printf '%s\n' "$out" | sed -nE 's/.*\{ *sec = ([0-9]+).*/\1/p' | head -n 1)"
  [[ -n "$out" ]] || return 1
  printf '%s\n' "$out"
}

# vm_mark_run_start (re)creates the guest-side marker file that timestamps the
# run; vm_new_panic_count counts .panic reports newer than it. The marker lives
# in the guest home, which survives reboots (unlike guest /tmp).
vm_mark_run_start() { vm_ssh "rm -f '$VM_GUEST_MARKER' && touch '$VM_GUEST_MARKER'"; }

# vm_new_panic_count prints how many guest .panic reports are newer than the
# run-start marker; prints nothing and returns 1 when the probe cannot run.
vm_new_panic_count() {
  local n
  # shellcheck disable=SC2029 # host-side expansion of the marker path is intended
  n="$(vm_sudo "find /Library/Logs/DiagnosticReports /Library/Logs/DiagnosticReports/Retired -maxdepth 1 -name '*.panic' -newer '$VM_GUEST_MARKER' 2>/dev/null | wc -l" 2>/dev/null)" || return 1
  n="$(printf '%s' "$n" | tr -d '[:space:]')"
  [[ "$n" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$n"
}

# vm_scrape_panics copies every guest .panic report (current and Retired) into
# <dest>/panics, staging through a root-readable guest dir.
vm_scrape_panics() {
  local dest="$1"
  mkdir -p "$dest"
  vm_sudo "rm -rf /tmp/vmctl-panics && mkdir -p /tmp/vmctl-panics && find /Library/Logs/DiagnosticReports /Library/Logs/DiagnosticReports/Retired -maxdepth 1 -name '*.panic' -exec cp {} /tmp/vmctl-panics/ ';' 2>/dev/null; chmod -R a+rX /tmp/vmctl-panics" || return 1
  rm -rf "$dest/panics"
  vm_scp_from "/tmp/vmctl-panics" "$dest" || return 1
  mv "$dest/vmctl-panics" "$dest/panics"
  log "panic reports in $dest/panics: $(find "$dest/panics" -name '*.panic' | wc -l | tr -d ' ')"
}

# --- Scenario helpers --------------------------------------------------------------

# vm_phase records the scenario's active workload phase; the label lands in
# meta.json verbatim, so it is restricted to [A-Za-z0-9._-]+.
vm_phase() {
  local label="$1"
  [[ "$label" =~ ^[A-Za-z0-9._-]+$ ]] || die "vm_phase: label must match [A-Za-z0-9._-]+: $label"
  [[ -n "${VMCTL_PHASE_FILE:-}" ]] || die "vm_phase: only valid inside vmctl run"
  printf '%s\n' "$label" >"$VMCTL_PHASE_FILE"
  log "workload phase: $label"
}

# vm_seconds_left prints the seconds remaining before the run deadline (0 when
# past it); scenarios loop on this to fill the bounded window.
vm_seconds_left() {
  [[ -n "${VMCTL_DEADLINE_EPOCH:-}" ]] || die "vm_seconds_left: only valid inside vmctl run"
  local left=$((VMCTL_DEADLINE_EPOCH - $(date +%s)))
  ((left > 0)) || left=0
  printf '%s\n' "$left"
}

# --- Guest waits ------------------------------------------------------------------

# vm_wait_guest_path waits up to $2 seconds (default 30) for a path to exist in
# the guest — a bound socket, a marker file, a freshly installed binary.
# Returns 1 on timeout.
vm_wait_guest_path() {
  local path="$1" timeout="${2:-30}" t0
  t0="$(date +%s)"
  while (($(date +%s) - t0 < timeout)); do
    # shellcheck disable=SC2029 # host-side path expansion into the guest is intended
    if vm_ssh "test -e '$path'" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# --- File Provider provisioning ---------------------------------------------------

# fp_register_and_enable makes the File Provider extension inside the installed
# CCPoolStatus.app both REGISTERED and ENABLED with pluginkit — the exact gate
# cc-pool's FileProviderAvailable checks (`pluginkit -m -i <id>` must lead with
# '+'). Installing the app is not enough: LaunchServices must scan it and
# pluginkit must be told to `use` the extension. Requires the app already
# pushed. Returns nonzero (with a click-Allow hint) when '+' is not reached.
# Idempotent; push.sh calls it, and `vmctl provision` re-calls it once the app
# exists so a re-provision after a push also enables the extension.
fp_register_and_enable() {
  # shellcheck disable=SC2029
  vm_ssh "test -d '$VMCTL_GUEST_APP'" >/dev/null 2>&1 || {
    warn "fp: $VMCTL_GUEST_APP not installed yet — run 'vmctl push' first, then re-provision or let push enable it"
    return 1
  }
  local lsregister="/System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/LaunchServices.framework/Versions/A/Support/lsregister"
  local appex="$VMCTL_GUEST_APP/Contents/PlugIns/CCPoolFileProvider.appex"
  log "fp: registering + enabling $VMCTL_FP_BUNDLE_ID"
  # shellcheck disable=SC2029
  vm_ssh "'$lsregister' -f '$VMCTL_GUEST_APP' >/dev/null 2>&1 || true"
  # shellcheck disable=SC2029
  vm_ssh "pluginkit -a '$appex' >/dev/null 2>&1 || true"
  # shellcheck disable=SC2029
  vm_ssh "pluginkit -e use -i '$VMCTL_FP_BUNDLE_ID' >/dev/null 2>&1 || true"
  local line
  # shellcheck disable=SC2029
  line="$(vm_ssh "pluginkit -m -i '$VMCTL_FP_BUNDLE_ID' 2>/dev/null" | head -n1)"
  if [[ "$line" == +* ]]; then
    log "fp: extension enabled ($line)"
    return 0
  fi
  warn "fp: extension $VMCTL_FP_BUNDLE_ID not enabled (pluginkit: '${line:-<not registered>}')"
  warn "fp: the Settings File-Provider toggle is a SEPARATE gate from pluginkit — if headless enable did not stick, boot with VMCTL_GRAPHICS=1 and enable it in System Settings > General > Login Items & Extensions (README: FP provisioning)"
  return 1
}

# fp_grant_consent flips fileproviderd's per-provider consent boolean ON for the
# CCPoolFileProvider extension. This is a SEPARATE gate from pluginkit enablement
# (fp_register_and_enable): the tart base image ships residual File Provider state
# that defaults every provider to DISABLED, so fileproviderd treats the provider as
# user-disabled and every probe/serve fails FP -2011 (domainDisabled) — the migrate's
# capability gate then refuses ("extension enabled but not serving") before any
# account converts. On a real machine the user grants this in System Settings >
# General > Login Items & Extensions > File Provider; that consumer path has no CLI,
# so this is a TEST-HARNESS-ONLY lever (never a substitute for the Settings toggle
# on a real install). fileproviderd's consent is a single boolean in the provider's
# Domains.plist (NSFileProviderDomainDefaultIdentifier:Enabled); flipping it true and
# kickstarting fileproviderd makes the provider serve, with every future domain
# auto-enabled. Requires the appex already registered (fp_register_and_enable). Dies
# loudly unless the PlistBuddy read-back is true.
fp_grant_consent() {
  log "fp: granting provider consent (Domains.plist default-identifier Enabled)…"
  # $VMCTL_FP_BUNDLE_ID is baked in host-side; everything else (\$HOME, \$(id -u),
  # the PlistBuddy calls) runs guest-side. `set -eu` makes the guest recipe fail
  # fast; the SIGKILL-before-Set ordering matters (see the inline WHY).
  local remote
  remote=$(cat <<REMOTE
set -eu
PL="\$HOME/Library/Application Support/FileProvider/$VMCTL_FP_BUNDLE_ID/Domains.plist"
SVC="gui/\$(id -u)/com.apple.FileProvider"
PB=/usr/libexec/PlistBuddy
# Seed the state dir + default-identifier Enabled key when absent. fileproviderd
# creates these once the appex has been elected + launched; on a pristine clone the
# Add arm seeds them (mkdir first — PlistBuddy cannot create the parent directory).
mkdir -p "\$(dirname "\$PL")"
"\$PB" -c "Print :NSFileProviderDomainDefaultIdentifier:Enabled" "\$PL" >/dev/null 2>&1 \
  || "\$PB" -c "Add :NSFileProviderDomainDefaultIdentifier:Enabled bool true" "\$PL"
# SIGKILL fileproviderd BEFORE the Set: on a clean shutdown it rewrites Domains.plist
# from its in-memory (disabled) state and would clobber the flip. Tolerate a
# not-running service (kill returns nonzero), then wait — bounded — for it to exit.
launchctl kill KILL "\$SVC" 2>/dev/null || true
for _ in \$(seq 1 50); do
  pgrep -x fileproviderd >/dev/null 2>&1 || break
  sleep 0.2
done
"\$PB" -c "Set :NSFileProviderDomainDefaultIdentifier:Enabled true" "\$PL"
# Kickstart so fileproviderd re-reads the now-enabled plist (a wrong service label
# fails here and fails the whole grant — the read-back below is the real gate).
launchctl kickstart "\$SVC"
REMOTE
)
  vm_ssh "$remote" || die "fp: could not grant provider consent (Domains.plist edit / fileproviderd kickstart failed) — see the guest output above"
  # Give fileproviderd a beat to re-read, then verify by authoritative read-back.
  # Push-time verification is plist-only by design: the full serving probe needs the
  # daemon's bridge socket, which is not bound until the replay's daemon-start phase.
  sleep 3
  local pl state
  # shellcheck disable=SC2016 # $HOME is deliberately literal — it expands guest-side, not here
  pl='$HOME/Library/Application Support/FileProvider/'"$VMCTL_FP_BUNDLE_ID"'/Domains.plist'
  # shellcheck disable=SC2029 # guest-side $HOME expansion into the PlistBuddy arg is intended
  state="$(vm_ssh "/usr/libexec/PlistBuddy -c 'Print :NSFileProviderDomainDefaultIdentifier:Enabled' \"$pl\" 2>/dev/null")" || true
  state="$(printf '%s' "$state" | tr -d '[:space:]')"
  [[ "$state" == "true" ]] ||
    die "fp: provider consent read-back is '${state:-<absent>}', not 'true' — the Domains.plist flip did not stick (fileproviderd rewrote it, or the state dir is absent on a pristine clone; README: FP provisioning)"
  log "fp: provider consent granted (Domains.plist default-identifier Enabled=true)"
}

# --- Daemon bundle helpers ---
# Print "<identity>\t<team-id>" for a Developer ID Application identity, or die.
vm_discover_signing() {
  if [[ -n "${VMCTL_SIGN_IDENTITY:-}" && -n "${VMCTL_SIGN_TEAM:-}" ]]; then
    printf '%s\t%s\n' "$VMCTL_SIGN_IDENTITY" "$VMCTL_SIGN_TEAM"
    return 0
  fi
  local line sha team
  line="$(security find-identity -v -p codesigning 2>/dev/null | grep 'Developer ID Application' | head -n1)" || true
  if [[ -z "$line" ]]; then
    die "no 'Developer ID Application' code-signing identity in the login keychain.
  The File Provider appex and CCPoolDaemon.app will NOT register/validate ad-hoc-signed, so a Developer ID cert is REQUIRED for the VM e2e. Options:
    1. Import the team SXKCTF23Q2 'Developer ID Application' cert + key into the login keychain (Keychain Access, or 'security import <cert.p12>').
    2. Pin one you know is present: VMCTL_SIGN_IDENTITY=<sha1-or-name> VMCTL_SIGN_TEAM=<teamid> scripts/vm/vmctl push
    3. If none is available, the FP / daemon-bundle e2e cannot be validated in the VM — report that back rather than shipping an ad-hoc build."
  fi
  # rows: `  1) <SHA1> "Developer ID Application: Name (TEAMID)"`.
  sha="$(printf '%s' "$line" | awk '{print $2}')"
  team="$(printf '%s' "$line" | sed -nE 's/.*\(([A-Z0-9]{10})\).*/\1/p')"
  [[ -n "$sha" && -n "$team" ]] || die "could not parse a Developer ID identity from: $line"
  printf '%s\t%s\n' "$sha" "$team"
}

# Print AppGroupID from paths.go (the entitlement + guest-bind source of truth).
vm_app_group() {
  local g
  g="$(sed -n 's/^const AppGroupID = "\(.*\)"$/\1/p' "$VM_REPO_ROOT/internal/pool/paths.go")"
  [[ -n "$g" ]] || die "could not read AppGroupID from $VM_REPO_ROOT/internal/pool/paths.go"
  printf '%s\n' "$g"
}

# Assemble + Developer ID-sign CCPoolDaemon.app (release.yml's wrap-daemon-bundle@v1).
# Args: <src_bin> <out_app> <sign_id> <version> <profiled|unprofiled>.
vm_build_daemon_bundle() {
  local src_bin="$1" out_app="$2" sign_id="$3" version="$4" mode="$5"
  [[ -f "$src_bin" ]] || die "daemon bundle: source binary not found: $src_bin"
  case "$mode" in
  profiled | unprofiled) ;;
  *) die "daemon bundle: mode must be 'profiled' or 'unprofiled', got: $mode" ;;
  esac
  local group bid exe
  group="$(vm_app_group)"
  bid="$VMCTL_DAEMON_BUNDLE_ID"
  exe="cc-pool"

  rm -rf "$out_app"
  mkdir -p "$out_app/Contents/MacOS"
  cp "$src_bin" "$out_app/Contents/MacOS/$exe"
  chmod 755 "$out_app/Contents/MacOS/$exe"

  # Contract Info.plist. LSUIElement (agent); LSBackgroundOnly deliberately absent.
  cat >"$out_app/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleIdentifier</key><string>${bid}</string>
  <key>CFBundleName</key><string>CCPoolDaemon</string>
  <key>CFBundleExecutable</key><string>${exe}</string>
  <key>CFBundleShortVersionString</key><string>${version}</string>
  <key>CFBundleVersion</key><string>${version}</string>
  <key>LSUIElement</key><true/>
</dict>
</plist>
PLIST

  # App-group entitlement only (mirrors release.yml dist/daemon.entitlements).
  local ents
  ents="$(mktemp -t ccp-daemon-ents.XXXXXX)"
  cat >"$ents" <<ENTS
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>com.apple.security.application-groups</key>
  <array><string>${group}</string></array>
</dict>
</plist>
ENTS

  if [[ "$mode" == "profiled" ]]; then
    [[ -n "${VMCTL_PROFILE_DAEMON:-}" ]] || {
      rm -f "$ents"
      die "daemon bundle: profiled mode needs VMCTL_PROFILE_DAEMON (a .provisionprofile path or its base64)"
    }
    local prof="$out_app/Contents/embedded.provisionprofile" decoded
    if [[ -f "$VMCTL_PROFILE_DAEMON" ]]; then
      cp "$VMCTL_PROFILE_DAEMON" "$prof"
    elif ! printf '%s' "$VMCTL_PROFILE_DAEMON" | base64 --decode >"$prof" 2>/dev/null; then
      rm -f "$ents"
      die "daemon bundle: VMCTL_PROFILE_DAEMON is neither a readable file nor valid base64"
    fi
    # The embedded profile must CMS-decode and authorize this exact group.
    decoded="$(security cms -D -i "$prof" 2>/dev/null)" || {
      rm -f "$ents"
      die "daemon bundle: embedded provisioning profile does not CMS-decode"
    }
    printf '%s' "$decoded" | grep -q "com.apple.security.application-groups" || {
      rm -f "$ents"
      die "daemon bundle: embedded profile lacks application-groups"
    }
    printf '%s' "$decoded" | grep -q "$group" || {
      rm -f "$ents"
      die "daemon bundle: embedded profile does not authorize $group"
    }
  fi

  # Developer ID sign: hardened runtime, timestamp, daemon identifier, app-group
  # entitlement (an embedded profile is sealed as a bundle resource).
  if ! codesign --force --sign "$sign_id" --identifier "$bid" \
    --options runtime --timestamp \
    --entitlements "$ents" "$out_app" 2>&1; then
    rm -f "$ents"
    die "daemon bundle: codesign failed for $out_app"
  fi
  rm -f "$ents"

  codesign --verify --deep --strict --verbose=2 "$out_app" || die "daemon bundle: codesign verify failed for $out_app"
  codesign -d --entitlements - "$out_app" 2>&1 | grep -q "$group" ||
    die "daemon bundle: signed bundle is missing the $group app-group entitlement"
  log "daemon bundle assembled: $out_app (mode=$mode, group=$group, version=$version)"
}

# Install a locally-built CCPoolDaemon.app into the guest at <guest_app>, keeping
# its signature (plain tar preserves _CodeSignature). Args: <local_app> <guest_app>.
vm_install_daemon_bundle() {
  local local_app="$1" guest_app="$2"
  [[ -d "$local_app" ]] || die "install daemon bundle: not a bundle: $local_app"
  local base parent
  base="$(basename "$local_app")"
  parent="$(dirname "$guest_app")"
  # shellcheck disable=SC2029 # host-side interpolation of the guest paths is intended
  tar -C "$(dirname "$local_app")" -cf - "$base" |
    vm_ssh "rm -rf '$guest_app' && mkdir -p '$parent' && tmp=\"\$(mktemp -d)\" && tar -xf - -C \"\$tmp\" && mv \"\$tmp/$base\" '$guest_app' && rmdir \"\$tmp\"" ||
    die "install daemon bundle: tar into the guest failed ($local_app -> $guest_app)"
}
