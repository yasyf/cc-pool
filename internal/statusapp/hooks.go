package statusapp

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/transportproto"
	"github.com/yasyf/fusekit/trustroles"
)

const readinessPoll = holderbridge.DeploymentPollInterval

type productHooks struct {
	buildID       string
	policyDigest  deployment.SHA256
	servicePlan   func(installedGeneration, string) (service.Plan, error)
	candidatePlan func(installedGeneration, string, string) (deployment.CandidatePlan, error)
	target        func(installedGeneration, string) (runtimeTarget, error)
	observe       func(context.Context, string) (holder.LocalRuntimeReadiness, error)
	identities    func(string) ([]proc.Identity, error)
	proveApp      func(context.Context, string) error
	stopRuntime   func(context.Context, deployment.RuntimeStopper, service.StopRuntimeRequest) (runtimeStopProof, error)
}

func newProductHooks(buildID string, policyDigest deployment.SHA256) productHooks {
	hooks := productHooks{
		buildID: buildID, policyDigest: policyDigest,
		observe: observeRuntimeHealth, identities: proc.ExecutableIdentities,
		proveApp: func(ctx context.Context, appPath string) error {
			return proveElection(ctx, appPath, commandRunner{})
		},
		stopRuntime: func(
			ctx context.Context,
			stopper deployment.RuntimeStopper,
			request service.StopRuntimeRequest,
		) (runtimeStopProof, error) {
			receipt, err := stopper.StopRuntime(ctx, request)
			if err != nil {
				return runtimeStopProof{}, err
			}
			return runtimeStopProof{
				operationID: receipt.OperationID(), target: receipt.Target(),
				processRecord: receipt.ProcessRecordDigest(), settlement: receipt.Settlement(),
				digest: receipt.Digest(),
			}, nil
		},
	}
	hooks.servicePlan = hooks.defaultServicePlanForBuild
	hooks.candidatePlan = hooks.defaultCandidatePlanForBuild
	hooks.target = hooks.defaultRuntimeTargetForBuild
	return hooks
}

type runtimeTarget struct {
	executable string
	socket     string
	buildID    string
}

type runtimeStopProof struct {
	operationID   string
	target        wire.RuntimeIdentity
	processRecord proc.RecordDigest
	settlement    service.StopSettlement
	digest        service.StopReceiptDigest
}

func (h productHooks) runtimeQuiesce(
	ctx context.Context,
	stopper deployment.RuntimeStopper,
	operation deactivationOperation,
) (runtimeProof, error) {
	generation := operation.activation.generation
	target, err := h.runtimeTargetForBuild(generation, operation.activation.readiness.runtimeBuild)
	if err != nil {
		return runtimeProof{}, err
	}
	health, observeErr := h.observe(ctx, target.socket)
	if observeErr != nil {
		identities, inventoryErr := h.identities(target.executable)
		if inventoryErr != nil {
			return runtimeProof{}, fmt.Errorf(
				"CCPoolStatus: prove prior runtime absence: %w",
				errors.Join(observeErr, inventoryErr),
			)
		}
		if len(identities) != 0 {
			return runtimeProof{}, fmt.Errorf(
				"CCPoolStatus: prior runtime endpoint is unavailable while %d exact process(es) remain: %w",
				len(identities), observeErr,
			)
		}
		return runtimeProof{
			absent: true,
			digest: h.evidenceDigest("runtime-absent", operation.id, generation, operation.activation.plan.Digest(),
				h.buildID, target.executable),
		}, nil
	}
	if err := validateRuntimeTarget(health); err != nil {
		return runtimeProof{}, err
	}
	processGeneration := health.ProcessGeneration
	result, err := h.stopRuntime(ctx, stopper, service.StopRuntimeRequest{
		OperationID: operation.id,
		RuntimeClientConfig: wire.RuntimeClientConfig{
			Client: wire.ClientConfig{
				Dial: wire.UnixDialer(target.socket), WireBuild: transportproto.WireBuild,
				Role: trustroles.StopController,
			},
			NoProgressTimeout: holderbridge.ReadinessContract().PreparationNoProgressTimeout(),
		},
		ExpectedRuntimeBuild: health.RuntimeBuild, ControlRole: trustroles.StopController,
	})
	if err != nil {
		return runtimeProof{}, fmt.Errorf("CCPoolStatus: settle prior runtime: %w", err)
	}
	if result.operationID != operation.id || result.target.RuntimeBuild != health.RuntimeBuild ||
		result.target.ProcessGeneration != processGeneration || result.processRecord == (proc.RecordDigest{}) ||
		result.settlement != service.StopSettlementGone || result.digest == (service.StopReceiptDigest{}) {
		return runtimeProof{}, errors.New("CCPoolStatus: stop result does not match the observed runtime generation")
	}
	return runtimeProof{
		absent: true, processGeneration: processGeneration,
		digest: h.evidenceDigest(
			"runtime-quiesced", operation.id, generation, operation.activation.plan.Digest(), h.buildID,
			health.RuntimeBuild, health.ProcessGeneration.String(), hex.EncodeToString(result.processRecord[:]),
			hex.EncodeToString(result.digest[:]),
		),
	}, nil
}

func (h productHooks) readiness(ctx context.Context, operation installedOperation) (runtimeReadiness, error) {
	buildID, err := exactPlanBuildID(operation.generation, operation.plan)
	if err != nil {
		return runtimeReadiness{}, err
	}
	want, err := h.servicePlanForBuild(operation.generation, buildID)
	if err != nil {
		return runtimeReadiness{}, err
	}
	if operation.plan.Digest() != want.Digest() || !reflect.DeepEqual(operation.plan.Agents(), want.Agents()) {
		return runtimeReadiness{}, errors.New("CCPoolStatus: readiness plan is not the exact helper plan")
	}
	if err := h.proveApp(ctx, operation.generation.path); err != nil {
		return runtimeReadiness{}, err
	}
	target, err := h.runtimeTargetForBuild(operation.generation, buildID)
	if err != nil {
		return runtimeReadiness{}, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, holderbridge.ReadinessContract().ObservationTimeout())
	defer cancel()
	var lastErr error
	for {
		health, observeErr := h.observe(readyCtx, target.socket)
		if observeErr == nil {
			validateErr := validateRuntimeReadiness(target, health)
			if validateErr == nil {
				return runtimeReadiness{
					runtimeBuild: health.RuntimeBuild, processGeneration: health.ProcessGeneration,
					digest: h.evidenceDigest(
						"runtime-ready", operation.id, operation.generation, operation.plan.Digest(),
						health.ProcessGeneration.String(), health.ActivationGeneration,
					),
				}, nil
			}
			lastErr = validateErr
		} else {
			lastErr = observeErr
		}
		select {
		case <-readyCtx.Done():
			return runtimeReadiness{}, fmt.Errorf(
				"CCPoolStatus: wait for exact runtime readiness: %w",
				errors.Join(readyCtx.Err(), lastErr),
			)
		case <-time.After(readinessPoll):
		}
	}
}

func (h productHooks) servicePlanForBuild(generation installedGeneration, buildID string) (service.Plan, error) {
	return h.servicePlan(generation, buildID)
}

func (h productHooks) defaultServicePlanForBuild(generation installedGeneration, buildID string) (service.Plan, error) {
	plan, err := holderbridge.NewDeploymentPlan(generation.path, pool.FuseKitRuntimeDir(), buildID)
	if err != nil {
		return service.Plan{}, err
	}
	return service.NewPlan([]service.Agent{plan.Agent()})
}

func (h productHooks) candidatePlanForBuild(
	generation installedGeneration,
	buildID, sourceAppPath string,
) (deployment.CandidatePlan, error) {
	return h.candidatePlan(generation, buildID, sourceAppPath)
}

func (h productHooks) defaultCandidatePlanForBuild(
	generation installedGeneration,
	buildID, sourceAppPath string,
) (deployment.CandidatePlan, error) {
	plan, err := holderbridge.NewDeploymentPlan(generation.path, pool.FuseKitRuntimeDir(), buildID)
	if err != nil {
		return deployment.CandidatePlan{}, err
	}
	return plan.CandidatePlan(sourceAppPath)
}

func (h productHooks) runtimeTargetForBuild(generation installedGeneration, buildID string) (runtimeTarget, error) {
	return h.target(generation, buildID)
}

func (h productHooks) defaultRuntimeTargetForBuild(generation installedGeneration, buildID string) (runtimeTarget, error) {
	plan, err := holderbridge.NewDeploymentPlan(generation.path, pool.FuseKitRuntimeDir(), buildID)
	if err != nil {
		return runtimeTarget{}, err
	}
	return runtimeTarget{
		executable: plan.RuntimeExecutable(), socket: plan.Paths().Socket, buildID: plan.BuildID(),
	}, nil
}

func exactPlanBuildID(generation installedGeneration, plan service.Plan) (string, error) {
	agents := plan.Agents()
	if len(agents) != 1 {
		return "", errors.New("CCPoolStatus: readiness plan must contain exactly one helper agent")
	}
	agent := agents[0]
	buildID := agent.Env["FUSEKIT_BUILD_ID"]
	if agent.Program != holderExecutablePath(generation.path) || buildID == "" {
		return "", errors.New("CCPoolStatus: readiness plan does not target the exact helper generation")
	}
	return buildID, nil
}

func holderExecutablePath(appPath string) string {
	return filepath.Join(appPath, "Contents", "MacOS", holderbridge.ExecutableName)
}

func observeRuntimeHealth(
	ctx context.Context,
	socket string,
) (health holder.LocalRuntimeReadiness, resultErr error) {
	client, err := tenantfs.NewControlClient(ctx, socket)
	if err != nil {
		return holder.LocalRuntimeReadiness{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, client.Close()) }()
	return client.Readiness(ctx)
}

func validateRuntimeTarget(health holder.LocalRuntimeReadiness) error {
	if health.RuntimeBuild == "" || health.ProcessGeneration == (proc.OwnerGeneration{}) {
		return errors.New("CCPoolStatus: prior runtime health has the wrong exact generation")
	}
	return nil
}

func validateRuntimeReadiness(target runtimeTarget, health holder.LocalRuntimeReadiness) error {
	if err := validateRuntimeTarget(health); err != nil {
		return err
	}
	if health.RuntimeBuild != target.buildID || health.ActivationGeneration == "" {
		return errors.New("CCPoolStatus: FuseKit runtime is not the exact healthy deployment activation")
	}
	return nil
}

func (h productHooks) evidenceDigest(
	kind, operationID string,
	generation installedGeneration,
	planDigest service.PlanDigest,
	details ...string,
) deployment.SHA256 {
	digest := sha256.New()
	values := make([]string, 0, 17+len(details))
	values = append(values,
		holderbridge.DeploymentEvidenceIdentity, kind, operationID, planDigest.String(),
		generation.path, generation.version, generation.teamID, generation.signingIdentifier,
		generation.designatedRequirement, generation.cdHash, generation.bundleDigest.String(),
		generation.entitlementsDigest.String(), generation.device, generation.inode, h.policyDigest.String(),
	)
	values = append(values, details...)
	for _, value := range values {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(value))
	}
	var result deployment.SHA256
	copy(result[:], digest.Sum(nil))
	return result
}
