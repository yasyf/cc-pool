# shellcheck shell=bash
# verify-worker-deadline.sh — exercise the real Darwin process-group kill ladder in the guest.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

# shellcheck source=../acceptance-lib.sh
# shellcheck disable=SC1091 # SCRIPT_DIR is supplied by vmctl before sourcing the scenario.
source "$SCRIPT_DIR/acceptance-lib.sh"

vm_phase worker-build
acceptance_prepare
require_cmd python3
worker_helper="$(acceptance_stage_go_helper ./scripts/vm/helpers/worker-deadline ccpool-worker-deadline)"

vm_phase worker-deadline
worker_result="$VMCTL_RESULTS_DIR/worker-deadline.json"
if ! vm_ssh "'$worker_helper'" >"$worker_result" 2>"$VMCTL_RESULTS_DIR/worker-deadline.stderr"; then
  cat "$VMCTL_RESULTS_DIR/worker-deadline.stderr" >&2
  die "guest worker deadline acceptance failed"
fi

python3 - "$worker_result" <<'PY' || die "worker deadline result did not prove the complete kill/reap contract"
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    result = json.load(stream)
if not (
    result.get("leader_pid", 0) > 1
    and result.get("descendant_pid", 0) > 1
    and result.get("elapsed_ms", 0) >= 500
    and result.get("records_after") == 0
    and result.get("term_observed") is True
    and result.get("group_gone") is True
    and result.get("lane_reused") is True
):
    raise SystemExit(f"incomplete worker acceptance result: {result!r}")
PY

log "verify-worker-deadline: deadline delivered TERM, escalated to process-group KILL, reaped leader and descendant, removed the durable record, and reused the bounded worker lane"
