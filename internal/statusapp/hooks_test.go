package statusapp

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/transportproto"
	"github.com/yasyf/fusekit/trustroles"
)

func TestRuntimeQuiesceStopsExactObservedGeneration(t *testing.T) {
	generation := exactTestGeneration(t)
	activation := exactTestActivation(t, generation)
	operation := deactivationOperation{id: "deactivation-1", activation: activation}
	builders := newProductHooks("runtime-v1", deployment.SHA256{1})
	installTestBuilders(&builders)
	target, err := builders.runtimeTargetForBuild(generation, "runtime-v1")
	if err != nil {
		t.Fatal(err)
	}
	health := exactHealth(target)
	var request service.StopRuntimeRequest
	hooks := newProductHooks("runtime-v1", deployment.SHA256{1})
	installTestBuilders(&hooks)
	hooks.observe = func(context.Context, string) (holder.LocalRuntimeReadiness, error) { return health, nil }
	hooks.stopRuntime = func(
		_ context.Context,
		_ deployment.RuntimeStopper,
		got service.StopRuntimeRequest,
	) (runtimeStopProof, error) {
		request = got
		return exactRuntimeStopProof(t, got.OperationID, health), nil
	}
	proof, err := hooks.runtimeQuiesce(t.Context(), nil, operation)
	if err != nil {
		t.Fatal(err)
	}
	if !proof.absent || proof.processGeneration != health.ProcessGeneration || proof.digest == (deployment.SHA256{}) ||
		request.OperationID != operation.id || request.ExpectedRuntimeBuild != health.RuntimeBuild ||
		request.ControlRole != trustroles.StopController ||
		request.RuntimeClientConfig.Client.WireBuild != transportproto.WireBuild ||
		request.RuntimeClientConfig.Client.Role != trustroles.StopController ||
		request.RuntimeClientConfig.NoProgressTimeout != holderbridge.ReadinessContract().PreparationNoProgressTimeout() {
		t.Fatalf("proof/request = %#v/%#v", proof, request)
	}
}

func TestRuntimeQuiesceRequiresEmptyExecutableInventoryForAbsentEndpoint(t *testing.T) {
	generation := exactTestGeneration(t)
	operation := deactivationOperation{id: "deactivation-1", activation: exactTestActivation(t, generation)}
	hooks := newProductHooks("runtime-v1", deployment.SHA256{1})
	installTestBuilders(&hooks)
	hooks.observe = func(context.Context, string) (holder.LocalRuntimeReadiness, error) {
		return holder.LocalRuntimeReadiness{}, errors.New("endpoint absent")
	}
	hooks.identities = func(string) ([]proc.Identity, error) { return nil, nil }
	proof, err := hooks.runtimeQuiesce(t.Context(), nil, operation)
	if err != nil || !proof.absent || proof.processGeneration != (proc.OwnerGeneration{}) ||
		proof.digest == (deployment.SHA256{}) {
		t.Fatalf("absence proof = %#v, err = %v", proof, err)
	}
	hooks.identities = func(string) ([]proc.Identity, error) { return []proc.Identity{{PID: 42}}, nil }
	if _, err := hooks.runtimeQuiesce(t.Context(), nil, operation); err == nil {
		t.Fatal("live exact executable inventory was accepted as absent")
	}
}

func TestReadinessBindsExactPlanRuntimeAndElection(t *testing.T) {
	generation := exactTestGeneration(t)
	activation := exactTestActivation(t, generation)
	operation := installedOperation{id: "activation-1", generation: generation, plan: activation.plan}
	hooks := newProductHooks("runtime-v1", deployment.SHA256{1})
	installTestBuilders(&hooks)
	target, err := hooks.runtimeTargetForBuild(generation, "runtime-v1")
	if err != nil {
		t.Fatal(err)
	}
	health := exactHealth(target)
	elections := 0
	hooks.proveApp = func(context.Context, string) error { elections++; return nil }
	hooks.observe = func(context.Context, string) (holder.LocalRuntimeReadiness, error) { return health, nil }
	proof, err := hooks.readiness(t.Context(), operation)
	if err != nil {
		t.Fatal(err)
	}
	if elections != 1 || proof.runtimeBuild != "runtime-v1" || proof.processGeneration != health.ProcessGeneration ||
		proof.digest == (deployment.SHA256{}) {
		t.Fatalf("proof/elections = %#v/%d", proof, elections)
	}
}

func TestRuntimeReadinessRequiresPinnedPublicationIdentity(t *testing.T) {
	target := runtimeTarget{buildID: "runtime-v1"}
	health := exactHealth(target)
	health.ActivationGeneration = ""
	if err := validateRuntimeReadiness(target, health); err == nil {
		t.Fatal("accepted readiness without an activation generation")
	}
	health.ActivationGeneration = "activation-generation"
	health.ProcessGeneration = proc.OwnerGeneration{}
	if err := validateRuntimeReadiness(target, health); err == nil {
		t.Fatal("accepted readiness without a process generation")
	}
	health.ProcessGeneration = proc.OwnerGeneration{1}
	if err := validateRuntimeReadiness(target, health); err != nil {
		t.Fatalf("pinned runtime readiness rejected: %v", err)
	}
}

func exactHealth(target runtimeTarget) holder.LocalRuntimeReadiness {
	return holder.LocalRuntimeReadiness{
		RuntimeBuild: target.buildID, ProcessGeneration: proc.OwnerGeneration{1, 2, 3, 4},
		ActivationGeneration: "activation-generation",
	}
}

func exactRuntimeStopProof(t *testing.T, operationID string, health holder.LocalRuntimeReadiness) runtimeStopProof {
	t.Helper()
	return runtimeStopProof{
		operationID:   operationID,
		target:        wire.RuntimeIdentity{RuntimeBuild: health.RuntimeBuild, ProcessGeneration: health.ProcessGeneration},
		processRecord: proc.RecordDigest{1}, settlement: service.StopSettlementGone,
		digest: service.StopReceiptDigest{2},
	}
}
