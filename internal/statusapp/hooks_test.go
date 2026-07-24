package statusapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
	"github.com/yasyf/fusekit/trustroles"
)

type recordingStopper struct {
	calls   int
	request service.StopRuntimeRequest
}

func (s *recordingStopper) StopRuntime(
	_ context.Context,
	request service.StopRuntimeRequest,
) (service.StopReceipt, error) {
	s.calls++
	s.request = request
	return service.StopReceipt{}, errors.New("test stopper requires projected receipt injection")
}

func TestRuntimeQuiesceStopsExactObservedGeneration(t *testing.T) {
	deployOperation := testOperation(t)
	operation := testRuntimeQuiesceOperation(deployOperation)
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	health := exactHealth(target)
	stopper := &recordingStopper{}
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
	hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
	hooks.stopRuntime = projectedStopRuntime(t, health, &stopper.request)
	proof, err := hooks.runtimeQuiesce(t.Context(), stopper, operation)
	if err != nil {
		t.Fatal(err)
	}
	wantGeneration, err := proc.ParseOwnerGeneration(health.ProcessGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Role != operation.Role || proof.Absent || proof.ProcessGeneration != wantGeneration ||
		proof.Digest == (deployment.SHA256{}) ||
		stopper.request.OperationID != operation.ID ||
		stopper.request.ExpectedRuntimeBuild != health.RuntimeBuild ||
		stopper.request.ControlRole != trustroles.StopController ||
		stopper.request.RuntimeClientConfig.Client.WireBuild != transportproto.WireBuild ||
		stopper.request.RuntimeClientConfig.Client.Role != trustroles.StopController ||
		stopper.request.RuntimeClientConfig.Client.Dial == nil ||
		stopper.request.RuntimeClientConfig.NoProgressTimeout != holderbridge.ReadinessContract().PreparationNoProgressTimeout() {
		t.Fatalf("proof/request = %#v/%#v", proof, stopper.request)
	}
}

func TestRuntimeQuiesceRejectsUnboundStopReceipt(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t))
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	health := exactHealth(target)
	tests := map[string]func(*runtimeStopProof){
		"operation":      func(result *runtimeStopProof) { result.operationID = "other" },
		"generation":     func(result *runtimeStopProof) { result.target.ProcessGeneration[0]++ },
		"runtime build":  func(result *runtimeStopProof) { result.target.RuntimeBuild = "v0.61.0" },
		"process record": func(result *runtimeStopProof) { result.processRecord = proc.RecordDigest{} },
		"settlement":     func(result *runtimeStopProof) { result.settlement = 0 },
		"receipt digest": func(result *runtimeStopProof) { result.digest = service.StopReceiptDigest{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := exactRuntimeStopProof(t, operation.ID, health)
			mutate(&result)
			hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
			hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
			hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
			hooks.stopRuntime = func(context.Context, deployment.RuntimeStopper, service.StopRuntimeRequest) (runtimeStopProof, error) {
				return result, nil
			}
			if _, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{}, operation); err == nil {
				t.Fatalf("accepted stop result with mismatched %s", name)
			}
		})
	}
}

func TestRuntimeQuiesceProofBindsCallerAndObservedBuilds(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t))
	quiesce := func(callerBuild, observedBuild string) deployment.SHA256 {
		t.Helper()
		target := runtimeTarget{
			executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: observedBuild,
		}
		health := exactHealth(target)
		stopper := &recordingStopper{}
		hooks := newProductHooks(callerBuild, deployment.SHA256{1})
		hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
		hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
		hooks.stopRuntime = projectedStopRuntime(t, health, nil)
		proof, err := hooks.runtimeQuiesce(t.Context(), stopper, operation)
		if err != nil {
			t.Fatal(err)
		}
		return proof.Digest
	}
	base := quiesce("v0.63.0", "v0.62.0")
	if base == quiesce("v0.64.0", "v0.62.0") || base == quiesce("v0.63.0", "v0.61.0") {
		t.Fatal("runtime proof does not bind both caller and observed builds")
	}
	operation.Role = deployment.ProofRollbackRuntime
	if base == quiesce("v0.63.0", "v0.62.0") {
		t.Fatal("runtime proof does not bind the daemonkit proof role")
	}
}

func TestRuntimeQuiesceProofBindsDurableStopReceipt(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t))
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	quiesce := func(processRecord proc.RecordDigest, receipt service.StopReceiptDigest) deployment.SHA256 {
		t.Helper()
		health := exactHealth(target)
		result := exactRuntimeStopProof(t, operation.ID, health)
		result.processRecord, result.digest = processRecord, receipt
		hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
		hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
		hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
		hooks.stopRuntime = func(context.Context, deployment.RuntimeStopper, service.StopRuntimeRequest) (runtimeStopProof, error) {
			return result, nil
		}
		proof, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{}, operation)
		if err != nil {
			t.Fatal(err)
		}
		return proof.Digest
	}
	processRecord, receipt := proc.RecordDigest{1}, service.StopReceiptDigest{2}
	base := quiesce(processRecord, receipt)
	processRecord[1]++
	receipt[1]++
	if base == quiesce(processRecord, service.StopReceiptDigest{2}) ||
		base == quiesce(proc.RecordDigest{1}, receipt) {
		t.Fatal("runtime proof does not bind both durable stop receipt identities")
	}
}

func TestRuntimeQuiesceProvesAbsenceByExactExecutableInventory(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t))
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
	hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) {
		return mountproto.RuntimeHealthResponse{}, errors.New("endpoint absent")
	}
	hooks.identities = func(string) ([]proc.Identity, error) { return nil, nil }
	proof, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{}, operation)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Role != operation.Role || !proof.Absent || proof.ProcessGeneration != (proc.OwnerGeneration{}) ||
		proof.Digest == (deployment.SHA256{}) {
		t.Fatalf("absence proof = %#v", proof)
	}
	hooks.identities = func(string) ([]proc.Identity, error) { return []proc.Identity{{PID: 42}}, nil }
	if _, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{}, operation); err == nil {
		t.Fatal("accepted unavailable endpoint with an exact process remaining")
	}
}

func TestInstalledAndRestoredProofsRequireExactElection(t *testing.T) {
	operation := testOperation(t)
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	hooks.verifyInstalled = func(deployment.Operation) error { return nil }
	var paths []string
	hooks.proveApp = func(_ context.Context, path string) error {
		paths = append(paths, path)
		return nil
	}
	operation.Role = deployment.ProofPostInstall
	installed, err := hooks.postInstallProof(t.Context(), operation)
	if err != nil {
		t.Fatal(err)
	}
	operation.Role = deployment.ProofPriorRestore
	restored, err := hooks.priorAppRestoreProof(t.Context(), operation)
	if err != nil {
		t.Fatal(err)
	}
	if installed.Role != deployment.ProofPostInstall || restored.Role != deployment.ProofPriorRestore ||
		installed.Digest == (deployment.SHA256{}) || restored.Digest == (deployment.SHA256{}) ||
		installed.Digest == restored.Digest || len(paths) != 2 || paths[0] != operation.Generation.Path || paths[1] != operation.Generation.Path {
		t.Fatalf("proofs/paths = %#v/%#v/%q", installed, restored, paths)
	}
}

func TestReadinessAcceptsCurrentAndStoredPriorBuildPlans(t *testing.T) {
	operation := testOperation(t)
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	for _, buildID := range []string{"v0.63.0", "v0.62.0"} {
		t.Run(buildID, func(t *testing.T) {
			plan := testServicePlan(t, operation, buildID)
			operation.Role = deployment.ProofCandidateReady
			operation.PlanDigest = deployment.SHA256(plan.Digest())
			target := runtimeTarget{
				executable: holderExecutablePath(operation.Generation.Path),
				socket:     filepath.Join(t.TempDir(), "fusekit.sock"),
				buildID:    buildID,
			}
			hooks.servicePlanBuild = func(deployment.Operation, string) (service.Plan, error) { return plan, nil }
			hooks.targetBuild = func(deployment.Operation, string) (runtimeTarget, error) { return target, nil }
			health := exactHealth(target)
			hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
			proof, err := hooks.readiness(t.Context(), operation, plan)
			if err != nil || proof.Role != operation.Role || proof.PlanDigest != operation.PlanDigest ||
				proof.Digest == (deployment.SHA256{}) {
				t.Fatalf("readiness = %#v, %v", proof, err)
			}
		})
	}
}

func testServicePlan(t *testing.T, operation deployment.Operation, buildID string) service.Plan {
	t.Helper()
	plan, err := service.NewPlan([]service.Agent{{
		Label:   holderbridge.BundleID + ".fusekit",
		Program: holderExecutablePath(operation.Generation.Path),
		LogPath: filepath.Join(t.TempDir(), "holder.log"),
		Env: map[string]string{
			"FUSEKIT_BUILD_ID": buildID,
		},
		AssociatedBundleIdentifiers: []string{holderbridge.BundleID},
		RestartPolicy:               service.RestartAlways,
		LimitLoadToSessionType:      service.SessionTypeAqua,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRuntimeReadinessRequiresReconciledBrokerFixedPoint(t *testing.T) {
	target := runtimeTarget{buildID: "v0.63.0"}
	health := exactHealth(target)
	health.BrokerPhase = mountproto.BrokerPhaseStarting
	if err := validateRuntimeReadiness(target, health); err == nil {
		t.Fatal("accepted broker before its reconciliation fixed point")
	}
	health.BrokerPhase = mountproto.BrokerPhaseLive
	if err := validateRuntimeReadiness(target, health); err != nil {
		t.Fatalf("reconciled broker rejected: %v", err)
	}
}

func testOperation(t *testing.T) deployment.Operation {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	digest, err := deployment.ParseSHA256(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	operation := deployment.Operation{
		ID: "00112233445566778899aabbccddeeff",
		Generation: deployment.CanonicalGeneration{
			Path: filepath.Join(home, "Applications", "CCPoolStatus.app"),
			Release: deployment.Release{
				Version: "0.63.0", URL: "https://example.invalid/CCPoolStatus.zip", SHA256: digest,
			},
			DesignatedRequirement: "anchor apple generic", CDHash: "0123456789abcdef",
			BundleDigest: deployment.SHA256{2}, Device: "1", Inode: "2",
		},
	}
	executable := holderExecutablePath(operation.Generation.Path)
	if err := os.MkdirAll(filepath.Dir(executable), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("test helper"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(executable, 0o700); err != nil { //nolint:gosec // Executable fixture requires the owner execute bit.
		t.Fatal(err)
	}
	return operation
}

func TestProofDigestBindsFullCanonicalGeneration(t *testing.T) {
	operation := testOperation(t)
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	base := hooks.proofDigest("generation", operation)
	changedCDHash := operation
	changedCDHash.Generation.CDHash = "fedcba9876543210"
	changedBundle := operation
	changedBundle.Generation.BundleDigest[0]++
	if base == hooks.proofDigest("generation", changedCDHash) ||
		base == hooks.proofDigest("generation", changedBundle) {
		t.Fatal("proof digest does not bind CDHash and bundle digest")
	}
}

func testRuntimeQuiesceOperation(operation deployment.Operation) deployment.RuntimeQuiesceOperation {
	return deployment.RuntimeQuiesceOperation{
		ID: operation.ID, Generation: operation.Generation, Role: deployment.ProofPriorRuntime,
	}
}

func exactHealth(target runtimeTarget) mountproto.RuntimeHealthResponse {
	return mountproto.RuntimeHealthResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		RuntimeBuild: target.buildID, RuntimeProtocol: mountproto.RuntimeProtocolVersion,
		RuntimePID: 100, ProcessGeneration: "0102030405060708090a0b0c0d0e0f10", ActivationGeneration: "activation-generation",
		State: mountproto.RuntimeStateHealthy, Ready: true,
		ReadinessPhase: mountproto.ReadinessPhaseReady, ReadinessStep: mountproto.ReadinessStepPublished,
		NativePhase: mountproto.NativePhaseDisabled, BrokerPhase: mountproto.BrokerPhaseLive,
	}
}

func exactRuntimeStopProof(t *testing.T, operationID string, health mountproto.RuntimeHealthResponse) runtimeStopProof {
	t.Helper()
	generation, err := proc.ParseOwnerGeneration(health.ProcessGeneration)
	if err != nil {
		t.Fatal(err)
	}
	return runtimeStopProof{
		operationID:   operationID,
		target:        wire.RuntimeIdentity{RuntimeBuild: health.RuntimeBuild, ProcessGeneration: generation},
		processRecord: proc.RecordDigest{1}, settlement: service.StopSettlementGone,
		digest: service.StopReceiptDigest{2},
	}
}

func projectedStopRuntime(
	t *testing.T,
	health mountproto.RuntimeHealthResponse,
	captured *service.StopRuntimeRequest,
) func(context.Context, deployment.RuntimeStopper, service.StopRuntimeRequest) (runtimeStopProof, error) {
	t.Helper()
	return func(
		_ context.Context,
		_ deployment.RuntimeStopper,
		request service.StopRuntimeRequest,
	) (runtimeStopProof, error) {
		if captured != nil {
			*captured = request
		}
		return exactRuntimeStopProof(t, request.OperationID, health), nil
	}
}
