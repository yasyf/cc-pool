# shellcheck shell=bash
# verify-panic-watcher.sh — positive control for vmctl's real kernel-panic classification.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=panic

vm_phase panic-preflight
vm_ssh "test -x /usr/sbin/dtrace" || die "guest lacks /usr/sbin/dtrace required for the panic positive control"
boot_before="$(vm_boottime)" || die "could not read pre-panic guest boottime"
panic_log="$VMCTL_GUEST_DIR/panic-positive-control.log"

vm_phase panic-trigger
# DTrace's destructive panic action is deliberately confined to the verified
# disposable VM. No intentional-reboot marker is created: vmctl must classify
# the resulting boot transition/new panic report as a panic.
# Run in the foreground so ssh keeps the trigger alive until the kernel drops
# the connection. A nonzero ssh result is expected when the guest panics; the
# boottime/report watcher below, not the transport result, decides success.
vm_ssh "rm -f '$panic_log'; sudo -n -- /usr/sbin/dtrace -w -q \
  -n 'BEGIN { panic(); }' >'$panic_log' 2>&1" >/dev/null 2>&1 || true

observed_transition=0
for _ in $(seq 1 30); do
  if ! boot_now="$(vm_boottime 2>/dev/null)"; then
    observed_transition=1
    break
  fi
  if [[ "$boot_now" != "$boot_before" ]]; then
    observed_transition=1
    break
  fi
  sleep 1
done
if [[ "$observed_transition" != "1" ]]; then
  vm_ssh "cat '$panic_log' 2>/dev/null; true" >&2 || true
  die "DTrace panic trigger produced no guest shutdown or boottime transition"
fi

vm_phase panic-await-watcher
while (($(vm_seconds_left) > 0)); do
  sleep 5
done
