package holderbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
)

func TestConsumerBuildForExecutableHashesExactBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ccp")
	payload := []byte("exact updater bytes")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil { //nolint:gosec // Executable fixture requires the owner execute bit.
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	want := consumerBuildDomain + hex.EncodeToString(digest[:])
	got, err := consumerBuildForExecutable(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("consumer build = %q, want %q", got, want)
	}
}

func TestConsumerBuildForExecutableRejectsNonExecutableInput(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("not executable"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"relative": "ccp", "directory": dir, "plain file": plain,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := consumerBuildForExecutable(path); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDeploymentIdentityUsesStartupCacheAndFailsClosed(t *testing.T) {
	originalBuild, originalBuildErr := startupConsumerBuild, startupConsumerBuildErr
	originalPolicy, originalPolicyErr := startupPolicyDigest, startupPolicyDigestErr
	t.Cleanup(func() {
		startupConsumerBuild, startupConsumerBuildErr = originalBuild, originalBuildErr
		startupPolicyDigest, startupPolicyDigestErr = originalPolicy, originalPolicyErr
	})
	wantDigest := deployment.SHA256(sha256.Sum256([]byte("policy")))
	startupConsumerBuild, startupConsumerBuildErr = "cached-build", nil
	startupPolicyDigest, startupPolicyDigestErr = wantDigest, nil
	build, digest, err := DeploymentIdentity()
	if err != nil || build != "cached-build" || digest != wantDigest {
		t.Fatalf("identity = (%q, %x, %v)", build, digest, err)
	}

	unavailable := errors.New("updater unavailable")
	startupConsumerBuild, startupConsumerBuildErr = "", unavailable
	build, digest, err = DeploymentIdentity()
	if !errors.Is(err, unavailable) || build != "" || digest != (deployment.SHA256{}) {
		t.Fatalf("failed identity = (%q, %x, %v)", build, digest, err)
	}
}

func TestDeploymentPolicyJSONAndDigestAreDeterministicAndComplete(t *testing.T) {
	payload, err := deploymentPolicyJSON()
	if err != nil {
		t.Fatal(err)
	}
	var policy deploymentPolicy
	if err := json.Unmarshal(payload, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Identity != deploymentPolicyIdentity || policy.Schema != 1 ||
		policy.Application.BundleID != BundleID || policy.Application.TeamID != TeamID ||
		policy.Application.InstallRootHomeRelative != "Applications" ||
		policy.Application.BundleLeaf != "CCPoolStatus.app" ||
		policy.Application.ExecutableName != ExecutableName ||
		policy.Application.ExecutableRelativePath != "Contents/MacOS/CCPoolStatus" ||
		!policy.Application.RequireCanonicalAccountHome || policy.Application.StopControlRole != StopRoleID ||
		policy.FileProvider.BundleID != "com.yasyf.cc-pool.status.fileprovider" ||
		policy.FileProvider.ExtensionRelativePath != "Contents/PlugIns/CCPoolFileProvider.appex" ||
		policy.FileProvider.AppGroup != AppGroup ||
		!policy.FileProvider.RequireRegistration || !policy.FileProvider.RequireEnabled ||
		!policy.FileProvider.RequireExactElection ||
		policy.FileProvider.ElectionTimeout != DeploymentElectionTimeout ||
		policy.FileProvider.ElectionPoll != DeploymentPollInterval ||
		policy.Runtime.State.HomeRelativeDirectory != ".cc-pool/fusekit" ||
		policy.Runtime.State.SocketName != "fusekit.sock" || policy.Runtime.State.CatalogName != "catalog.sqlite" ||
		policy.Runtime.State.ProcessStoreName != "processes.db" || policy.Runtime.State.LogName != "holder.log" ||
		policy.Runtime.State.SourceObserverDirectory != "source-observer-0000000000" ||
		policy.Runtime.State.SourceObserverSocketName != "observer.sock" ||
		policy.Runtime.State.RuntimePolicyDigest != hex.EncodeToString(runtimePolicyDigest[:]) ||
		policy.Runtime.Native.Enabled || policy.Runtime.Native.RequiredPhase != mountproto.NativePhaseDisabled ||
		!policy.Runtime.Native.RequireNoProof || !policy.Runtime.Source.Capable || !policy.Runtime.Broker.Enabled ||
		!policy.Runtime.Broker.RequireReconciledFixedPoint ||
		policy.Runtime.Readiness.StartupTimeout != ReadinessContract().StartupTimeout() ||
		policy.Runtime.Readiness.SettlementTimeout != ReadinessContract().SettlementTimeout() ||
		policy.Runtime.Readiness.ObservationTimeout != ReadinessContract().ObservationTimeout() ||
		policy.Runtime.Readiness.NativeReadinessTimeout != NativeReadinessTimeout ||
		policy.Runtime.Readiness.SourceReadinessTimeout != SourceReadinessTimeout ||
		policy.Runtime.Readiness.CatalogReadinessTimeout != CatalogReadinessTimeout ||
		policy.Runtime.Readiness.CatalogOperationTimeout != CatalogOperationTimeout ||
		policy.Runtime.Readiness.RuntimeShutdownTimeout != RuntimeShutdownTimeout ||
		policy.Runtime.Readiness.PollInterval != DeploymentPollInterval ||
		!policy.Runtime.Readiness.RequireReady || !policy.Runtime.Readiness.RequireNotDraining ||
		!policy.Runtime.Readiness.RequireNotBusy || !policy.Runtime.Readiness.RequireRuntimeBuildMatch ||
		!policy.Runtime.Readiness.RequirePositiveRuntimePID ||
		!policy.Runtime.Readiness.RequireProcessGeneration ||
		!policy.Runtime.Readiness.RequireActivationGeneration ||
		!policy.Runtime.Readiness.RequireEmptyMessage ||
		policy.Runtime.Readiness.RequiredErrorCode != mountproto.ErrorCodeOk ||
		policy.Runtime.Readiness.RequiredBrokerPhase != mountproto.BrokerPhaseLive ||
		!policy.Service.ExactSingleAgentPlan || !policy.Service.ReplacementOwnsRestartFence ||
		!policy.Service.RequireExactAssociatedBundleID || policy.Service.StartInterval != 0 ||
		policy.Service.ProcessType != 0 || !policy.Service.LogPathIsRuntimeStateLog ||
		!policy.Service.RequireNoWatchPaths || !policy.Service.RequireNoCalendarIntervals ||
		!policy.Service.ProgramIsFixedBundleExecutable || !policy.Service.RequireNoArguments ||
		policy.Service.BuildEnvironmentKey != "FUSEKIT_BUILD_ID" || !policy.Service.RequireExactBuildEnvironment ||
		policy.Proofs.Identity != DeploymentProofIdentity ||
		policy.Proofs.PostInstallRole != deployment.ProofPostInstall ||
		policy.Proofs.CandidateReadyRole != deployment.ProofCandidateReady ||
		policy.Proofs.PriorRestoreRole != deployment.ProofPriorRestore ||
		policy.Proofs.PriorReadyRole != deployment.ProofPriorReady ||
		policy.Proofs.PriorRuntimeRole != deployment.ProofPriorRuntime ||
		policy.Proofs.RollbackRuntimeRole != deployment.ProofRollbackRuntime ||
		!policy.Proofs.RequireReturnedRoleMatch || !policy.Proofs.RequireReadinessPlanDigest ||
		!policy.Proofs.BindGenerationCDHash || !policy.Proofs.BindGenerationBundleDigest ||
		!policy.Service.Quiesce.UseDaemonkitOperationIntent ||
		!reflect.DeepEqual(policy.Service.Quiesce.AcceptedStopIntents, []wire.StopIntent{
			wire.StopIntentUpgrade, wire.StopIntentRestart, wire.StopIntentUninstall,
		}) ||
		!policy.Service.Quiesce.StopAuthorityUsesConsumerBuild ||
		!policy.Service.Quiesce.RuntimeProofBindsIntent ||
		!policy.Service.Quiesce.RuntimeProofBindsCallerBuild ||
		!policy.Service.Quiesce.RuntimeProofBindsObservedBuild ||
		!policy.Service.Quiesce.RequireTargetProcessGeneration ||
		!policy.Service.Quiesce.RequireExactExecutableInventory ||
		!policy.Service.Quiesce.AbsentRequiresEmptyInventory ||
		!policy.Service.Quiesce.RequireExactHealthTarget || !policy.Service.Quiesce.RequireExactStopResult {
		t.Fatalf("deployment policy = %#v", policy)
	}
	second, err := deploymentPolicyJSON()
	if err != nil || !reflect.DeepEqual(payload, second) {
		t.Fatalf("policy encoding is not deterministic: %v", err)
	}
	digest, err := makeDeploymentPolicyDigest()
	if err != nil {
		t.Fatal(err)
	}
	want := deployment.SHA256(sha256.Sum256(payload))
	if digest != want {
		t.Fatalf("policy digest = %x, want %x", digest, want)
	}
}
