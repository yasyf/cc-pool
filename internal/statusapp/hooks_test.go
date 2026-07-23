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
)

type recordingStopper struct {
	result wire.StopResult
	err    error
	calls  int
	spec   service.StopControlSpec
}

func (s *recordingStopper) StopRuntime(
	_ context.Context,
	spec service.StopControlSpec,
) (wire.StopResult, error) {
	s.calls++
	s.spec = spec
	return s.result, s.err
}

func TestRuntimeQuiesceStopsExactObservedGeneration(t *testing.T) {
	deployOperation := testOperation(t)
	operation := testRuntimeQuiesceOperation(deployOperation, wire.StopIntentUpgrade)
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	health := exactHealth(target)
	stopper := &recordingStopper{result: exactStopResult(health, target)}
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
	hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
	proof, err := hooks.runtimeQuiesce(t.Context(), stopper, operation)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Role != operation.Role || proof.Absent || proof.ProcessGeneration != health.ProcessGeneration ||
		proof.Digest == (deployment.SHA256{}) ||
		stopper.calls != 1 || stopper.spec.Executable != target.executable ||
		stopper.spec.RuntimeBuild != hooks.buildID || stopper.spec.TargetProcessGeneration != health.ProcessGeneration ||
		stopper.spec.Role != holderbridge.StopRoleID || stopper.spec.Intent != operation.Intent {
		t.Fatalf("proof/spec = %#v/%#v", proof, stopper.spec)
	}
}

func TestRuntimeQuiesceRejectsUnboundStopProcessIdentity(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t), wire.StopIntentUpgrade)
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	health := exactHealth(target)
	tests := map[string]func(*wire.StopResult){
		"not stopped":        func(result *wire.StopResult) { result.Stopped = false },
		"generation":         func(result *wire.StopResult) { result.ProcessGeneration = "other" },
		"runtime build":      func(result *wire.StopResult) { result.RuntimeBuild = "v0.61.0" },
		"runtime protocol":   func(result *wire.StopResult) { result.RuntimeProtocol++ },
		"process pid":        func(result *wire.StopResult) { result.Process.PID++ },
		"process start":      func(result *wire.StopResult) { result.Process.StartTime = "" },
		"process boot":       func(result *wire.StopResult) { result.Process.Boot = "" },
		"process executable": func(result *wire.StopResult) { result.Process.Executable = "/wrong" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			result := exactStopResult(health, target)
			mutate(&result)
			hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
			hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
			hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
			if _, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{result: result}, operation); err == nil {
				t.Fatalf("accepted stop result with mismatched %s", name)
			}
		})
	}
}

func TestRuntimeQuiesceForwardsEveryDaemonkitIntent(t *testing.T) {
	deployOperation := testOperation(t)
	proofs := make(map[wire.StopIntent]deployment.SHA256)
	for _, intent := range []wire.StopIntent{wire.StopIntentUpgrade, wire.StopIntentRestart, wire.StopIntentUninstall} {
		t.Run(string(intent), func(t *testing.T) {
			target := runtimeTarget{
				executable: holderExecutablePath(deployOperation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
			}
			health := exactHealth(target)
			stopper := &recordingStopper{result: exactStopResult(health, target)}
			hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
			hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
			hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
			proof, err := hooks.runtimeQuiesce(t.Context(), stopper, testRuntimeQuiesceOperation(deployOperation, intent))
			if err != nil {
				t.Fatal(err)
			}
			if stopper.spec.Intent != intent || stopper.spec.RuntimeBuild != hooks.buildID {
				t.Fatalf("stop intent = %q, want %q", stopper.spec.Intent, intent)
			}
			proofs[intent] = proof.Digest
		})
	}
	if proofs[wire.StopIntentUpgrade] == proofs[wire.StopIntentRestart] ||
		proofs[wire.StopIntentUpgrade] == proofs[wire.StopIntentUninstall] ||
		proofs[wire.StopIntentRestart] == proofs[wire.StopIntentUninstall] {
		t.Fatalf("runtime proofs do not bind the daemonkit intent: %#v", proofs)
	}
}

func TestRuntimeQuiesceProofBindsCallerAndObservedBuilds(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t), wire.StopIntentRestart)
	quiesce := func(callerBuild, observedBuild string) deployment.SHA256 {
		t.Helper()
		target := runtimeTarget{
			executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: observedBuild,
		}
		health := exactHealth(target)
		stopper := &recordingStopper{result: exactStopResult(health, target)}
		hooks := newProductHooks(callerBuild, deployment.SHA256{1})
		hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
		hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
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

func TestRuntimeQuiesceProofBindsStoppedProcessIdentity(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t), wire.StopIntentRestart)
	target := runtimeTarget{
		executable: holderExecutablePath(operation.Generation.Path), socket: "/tmp/fusekit.sock", buildID: "v0.62.0",
	}
	quiesce := func(identity proc.Identity) deployment.SHA256 {
		t.Helper()
		health := exactHealth(target)
		health.RuntimePID = int64(identity.PID)
		result := exactStopResult(health, target)
		result.Process = identity
		hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
		hooks.target = func(deployment.Operation) (runtimeTarget, error) { return target, nil }
		hooks.observe = func(context.Context, string) (mountproto.RuntimeHealthResponse, error) { return health, nil }
		proof, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{result: result}, operation)
		if err != nil {
			t.Fatal(err)
		}
		return proof.Digest
	}
	baseIdentity := exactStopResult(exactHealth(target), target).Process
	base := quiesce(baseIdentity)
	mutations := []func(*proc.Identity){
		func(identity *proc.Identity) { identity.PID++ },
		func(identity *proc.Identity) { identity.StartTime = "other-start" },
		func(identity *proc.Identity) { identity.Boot = "other-boot" },
		func(identity *proc.Identity) { identity.Comm = "other-command" },
		func(identity *proc.Identity) { identity.AuditToken[0] = 1 },
	}
	for _, mutate := range mutations {
		identity := baseIdentity
		mutate(&identity)
		if base == quiesce(identity) {
			t.Fatal("runtime proof does not bind the complete stopped process identity")
		}
	}
}

func TestRuntimeQuiesceRejectsUnknownDaemonkitIntent(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t), wire.StopIntent("unknown"))
	hooks := newProductHooks("v0.63.0", deployment.SHA256{1})
	if _, err := hooks.runtimeQuiesce(t.Context(), &recordingStopper{}, operation); err == nil {
		t.Fatal("accepted unknown daemonkit runtime stop intent")
	}
}

func TestRuntimeQuiesceProvesAbsenceByExactExecutableInventory(t *testing.T) {
	operation := testRuntimeQuiesceOperation(testOperation(t), wire.StopIntentUninstall)
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
	if proof.Role != operation.Role || !proof.Absent || proof.ProcessGeneration != "" ||
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

func testRuntimeQuiesceOperation(operation deployment.Operation, intent wire.StopIntent) deployment.RuntimeQuiesceOperation {
	return deployment.RuntimeQuiesceOperation{
		ID: operation.ID, Generation: operation.Generation, Intent: intent, Role: deployment.ProofPriorRuntime,
	}
}

func exactHealth(target runtimeTarget) mountproto.RuntimeHealthResponse {
	return mountproto.RuntimeHealthResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		RuntimeBuild: target.buildID, RuntimeProtocol: mountproto.RuntimeProtocolVersion,
		RuntimePID: 100, ProcessGeneration: "process-generation", ActivationGeneration: "activation-generation",
		State: mountproto.RuntimeStateHealthy, Ready: true,
		ReadinessPhase: mountproto.ReadinessPhaseReady, ReadinessStep: mountproto.ReadinessStepPublished,
		NativePhase: mountproto.NativePhaseDisabled, BrokerPhase: mountproto.BrokerPhaseLive,
	}
}

func exactStopResult(health mountproto.RuntimeHealthResponse, target runtimeTarget) wire.StopResult {
	return wire.StopResult{
		Process: proc.Identity{
			PID: int(health.RuntimePID), StartTime: "process-start", Boot: "boot-session",
			Comm: "CCPoolStatus", Executable: target.executable,
		},
		ProcessGeneration: health.ProcessGeneration, RuntimeBuild: health.RuntimeBuild,
		RuntimeProtocol: int(mountproto.RuntimeProtocolVersion), Stopped: true,
	}
}
