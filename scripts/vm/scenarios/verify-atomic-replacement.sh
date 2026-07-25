# shellcheck shell=bash
# verify-atomic-replacement.sh — prove replace-over-target identity and catalog atomicity.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

# shellcheck source=../acceptance-lib.sh
# shellcheck disable=SC1091 # SCRIPT_DIR is supplied by vmctl before sourcing the scenario.
source "$SCRIPT_DIR/acceptance-lib.sh"

vm_phase replacement-build
acceptance_prepare
catalog_tests="$(acceptance_stage_test_binary github.com/yasyf/fusekit/catalog fusekit-replacement-tests)"
sourceauthority_tests="$(acceptance_stage_test_binary github.com/yasyf/fusekit/sourceauthority fusekit-replacement-source-tests)"
log_start="$(vm_ssh date -u '+%Y-%m-%d %H:%M:%S')" || die "could not timestamp replacement log window"

# FuseKit v1.14.0 splits catalog commit atomicity from source locator reconciliation.
vm_phase replacement-transaction
acceptance_run_tests \
  "$catalog_tests" \
  '^TestReplaceKeepsSourceIdentityAndOldHandleContent$|^TestConcurrentReplaceHasOneWinner$|^TestReplacePublishesFinalMetadataAndContentInOneRevision$|^TestPrivateAtomicReplaceIsOldOrNewAcrossCommitFailpoints$' \
  "$VMCTL_RESULTS_DIR/replacement-tests.log" \
  TestReplaceKeepsSourceIdentityAndOldHandleContent \
  TestConcurrentReplaceHasOneWinner \
  TestReplacePublishesFinalMetadataAndContentInOneRevision \
  TestPrivateAtomicReplaceIsOldOrNewAcrossCommitFailpoints

acceptance_run_tests \
  "$sourceauthority_tests" \
  '^TestIncrementalAtomicReplaceRetainsLocatorBinding$' \
  "$VMCTL_RESULTS_DIR/replacement-source-tests.log" \
  TestIncrementalAtomicReplaceRetainsLocatorBinding

vm_phase replacement-fileprovider-log
fp_errors="$VMCTL_RESULTS_DIR/replacement-fileprovider-errors.log"
vm_ssh "/usr/bin/log show --style compact --start '$log_start' \
  --predicate 'eventMessage CONTAINS[c] \"itemCollision\" OR eventMessage CONTAINS[c] \"ESTALE\" OR eventMessage CONTAINS[c] \"itemDocTrackedButNotOnDisk\"'" \
  >"$fp_errors" || die "could not inspect File Provider reconciliation errors"
if grep -Eiq 'itemCollision|ESTALE|itemDocTrackedButNotOnDisk' "$fp_errors"; then
  cat "$fp_errors" >&2
  die "replacement window contained a File Provider collision, stale document ID, or stale-document cleanup"
fi

log "verify-atomic-replacement: replace-over-target preserved source identity, retained old-handle content, published one atomic revision through failpoints, and emitted no collision/ESTALE/stale-document cleanup"
