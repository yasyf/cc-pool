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
	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

const (
	readinessPoll = holderbridge.DeploymentPollInterval
)

type productHooks struct {
	buildID          string
	policyDigest     deployment.SHA256
	servicePlan      func(deployment.Operation) (service.Plan, error)
	target           func(deployment.Operation) (runtimeTarget, error)
	servicePlanBuild func(deployment.Operation, string) (service.Plan, error)
	targetBuild      func(deployment.Operation, string) (runtimeTarget, error)
	verifyInstalled  func(deployment.Operation) error
	observe          func(context.Context, string) (mountproto.RuntimeHealthResponse, error)
	identities       func(string) ([]proc.Identity, error)
	proveApp         func(context.Context, string) error
}

func newProductHooks(buildID string, policyDigest deployment.SHA256) productHooks {
	hooks := productHooks{
		buildID: buildID, policyDigest: policyDigest,
		observe: observeRuntimeHealth, identities: proc.ExecutableIdentities,
		proveApp: func(ctx context.Context, appPath string) error {
			return proveElection(ctx, appPath, commandRunner{})
		},
	}
	hooks.servicePlanBuild = hooks.servicePlanForBuild
	hooks.targetBuild = hooks.runtimeTargetForBuild
	hooks.verifyInstalled = hooks.verifyInstalledRuntimePlan
	hooks.servicePlan = hooks.servicePlanForOperation
	hooks.target = hooks.runtimeTargetForOperation
	return hooks
}

type runtimeTarget struct {
	executable string
	socket     string
	buildID    string
}

func (h productHooks) runtimeQuiesce(
	ctx context.Context,
	stopper deployment.RuntimeStopper,
	operation deployment.RuntimeQuiesceOperation,
) (deployment.RuntimeProof, error) {
	if !validRuntimeStopIntent(operation.Intent) {
		return deployment.RuntimeProof{}, fmt.Errorf("CCPoolStatus: unsupported daemonkit runtime stop intent %q", operation.Intent)
	}
	deployOperation := deployment.Operation{ID: operation.ID, Generation: operation.Generation, Role: operation.Role}
	target, err := h.target(deployOperation)
	if err != nil {
		return deployment.RuntimeProof{}, err
	}
	health, observeErr := h.observe(ctx, target.socket)
	if observeErr != nil {
		identities, inventoryErr := h.identities(target.executable)
		if inventoryErr != nil {
			return deployment.RuntimeProof{}, fmt.Errorf(
				"CCPoolStatus: prove prior runtime absence: %w",
				errors.Join(observeErr, inventoryErr),
			)
		}
		if len(identities) != 0 {
			return deployment.RuntimeProof{}, fmt.Errorf(
				"CCPoolStatus: prior runtime endpoint is unavailable while %d exact process(es) remain: %w",
				len(identities), observeErr,
			)
		}
		return deployment.RuntimeProof{
			Role: operation.Role, Absent: true,
			Digest: h.proofDigest(
				"runtime-absent", deployOperation, string(operation.Intent), h.buildID, target.executable,
			),
		}, nil
	}
	if err := validateRuntimeTarget(health); err != nil {
		return deployment.RuntimeProof{}, err
	}
	result, err := stopper.StopRuntime(ctx, service.StopControlSpec{
		Executable: target.executable, Args: holder.StopControlChildArguments(),
		Role: holderbridge.StopRoleID, RuntimeBuild: h.buildID,
		RuntimeProtocol:         int(mountproto.RuntimeProtocolVersion),
		TargetProcessGeneration: health.ProcessGeneration, Intent: operation.Intent,
	})
	if err != nil {
		return deployment.RuntimeProof{}, fmt.Errorf("CCPoolStatus: settle prior runtime: %w", err)
	}
	if !result.Stopped || result.ProcessGeneration != health.ProcessGeneration ||
		result.RuntimeBuild != health.RuntimeBuild || result.RuntimeProtocol != int(mountproto.RuntimeProtocolVersion) ||
		int64(result.Process.PID) != health.RuntimePID || result.Process.StartTime == "" ||
		result.Process.Boot == "" || result.Process.Executable != target.executable {
		return deployment.RuntimeProof{}, errors.New("CCPoolStatus: stop result does not match the observed runtime generation")
	}
	return deployment.RuntimeProof{
		Role: operation.Role, ProcessGeneration: health.ProcessGeneration,
		Digest: h.proofDigest(
			"runtime-quiesced", deployOperation, string(operation.Intent), h.buildID,
			health.RuntimeBuild, health.ProcessGeneration,
			fmt.Sprintf("%d", result.Process.PID), result.Process.StartTime, result.Process.Boot,
			result.Process.Comm, result.Process.Executable, hex.EncodeToString(result.Process.AuditToken[:]),
		),
	}, nil
}

func validRuntimeStopIntent(intent wire.StopIntent) bool {
	switch intent {
	case wire.StopIntentUpgrade, wire.StopIntentRestart, wire.StopIntentUninstall:
		return true
	default:
		return false
	}
}

func (h productHooks) postInstallProof(ctx context.Context, operation deployment.Operation) (deployment.Proof, error) {
	return h.applicationProof(ctx, "post-install", operation)
}

func (h productHooks) priorAppRestoreProof(ctx context.Context, operation deployment.Operation) (deployment.Proof, error) {
	return h.applicationProof(ctx, "prior-restored", operation)
}

func (h productHooks) applicationProof(
	ctx context.Context,
	kind string,
	operation deployment.Operation,
) (deployment.Proof, error) {
	if err := h.verifyInstalled(operation); err != nil {
		return deployment.Proof{}, fmt.Errorf("CCPoolStatus: verify installed runtime plan: %w", err)
	}
	if err := h.proveApp(ctx, operation.Generation.Path); err != nil {
		return deployment.Proof{}, err
	}
	return deployment.Proof{
		Role: operation.Role, PlanDigest: operation.PlanDigest,
		Digest: h.proofDigest(kind, operation, fileProviderExtensionPath(operation.Generation.Path)),
	}, nil
}

func (h productHooks) buildPlan(_ context.Context, operation deployment.Operation) (service.Plan, error) {
	return h.servicePlan(operation)
}

func (h productHooks) readiness(
	ctx context.Context,
	operation deployment.Operation,
	got service.Plan,
) (deployment.Proof, error) {
	buildID, err := exactPlanBuildID(operation, got)
	if err != nil {
		return deployment.Proof{}, err
	}
	want, err := h.servicePlanBuild(operation, buildID)
	if err != nil {
		return deployment.Proof{}, err
	}
	if got.Digest() != want.Digest() || !reflect.DeepEqual(got.Agents(), want.Agents()) {
		return deployment.Proof{}, errors.New("CCPoolStatus: readiness plan is not the exact helper plan")
	}
	target, err := h.targetBuild(operation, buildID)
	if err != nil {
		return deployment.Proof{}, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, holderbridge.ReadinessContract().ObservationTimeout())
	defer cancel()
	var lastErr error
	for {
		health, observeErr := h.observe(readyCtx, target.socket)
		if observeErr == nil {
			validateErr := validateRuntimeReadiness(target, health)
			if validateErr == nil {
				return deployment.Proof{Role: operation.Role, PlanDigest: operation.PlanDigest, Digest: h.proofDigest(
					"runtime-ready", operation, got.Digest().String(), health.ProcessGeneration,
					health.ActivationGeneration,
				)}, nil
			}
			lastErr = validateErr
		} else {
			lastErr = observeErr
		}
		select {
		case <-readyCtx.Done():
			return deployment.Proof{}, fmt.Errorf(
				"CCPoolStatus: wait for deployment readiness: %w",
				errors.Join(readyCtx.Err(), lastErr),
			)
		case <-time.After(readinessPoll):
		}
	}
}

func (h productHooks) planForBuild(operation deployment.Operation, buildID string) (holder.DeploymentPlan, error) {
	return holderbridge.NewDeploymentPlan(operation.Generation.Path, pool.FuseKitRuntimeDir(), buildID)
}

func (h productHooks) servicePlanForOperation(operation deployment.Operation) (service.Plan, error) {
	return h.servicePlanBuild(operation, h.buildID)
}

func (h productHooks) servicePlanForBuild(operation deployment.Operation, buildID string) (service.Plan, error) {
	plan, err := h.planForBuild(operation, buildID)
	if err != nil {
		return service.Plan{}, err
	}
	return service.NewPlan([]service.Agent{plan.Agent()})
}

func (h productHooks) runtimeTargetForOperation(operation deployment.Operation) (runtimeTarget, error) {
	return h.targetBuild(operation, h.buildID)
}

func (h productHooks) verifyInstalledRuntimePlan(operation deployment.Operation) error {
	_, err := holderbridge.NewDeploymentPlan(
		operation.Generation.Path, pool.FuseKitRuntimeDir(), h.buildID,
	)
	return err
}

func (h productHooks) runtimeTargetForBuild(operation deployment.Operation, buildID string) (runtimeTarget, error) {
	plan, err := h.planForBuild(operation, buildID)
	if err != nil {
		return runtimeTarget{}, err
	}
	return runtimeTarget{
		executable: plan.RuntimeExecutable(), socket: plan.Paths().Socket, buildID: plan.BuildID(),
	}, nil
}

func exactPlanBuildID(operation deployment.Operation, plan service.Plan) (string, error) {
	agents := plan.Agents()
	if len(agents) != 1 {
		return "", errors.New("CCPoolStatus: readiness plan must contain exactly one helper agent")
	}
	agent := agents[0]
	buildID := agent.Env["FUSEKIT_BUILD_ID"]
	if agent.Program != holderExecutablePath(operation.Generation.Path) || buildID == "" {
		return "", errors.New("CCPoolStatus: readiness plan does not target the exact helper generation")
	}
	return buildID, nil
}

func holderExecutablePath(appPath string) string {
	return filepath.Join(appPath, "Contents", "MacOS", holderbridge.ExecutableName)
}

func fileProviderExtensionPath(appPath string) string {
	return filepath.Join(appPath, "Contents", "PlugIns", "CCPoolFileProvider.appex")
}

func observeRuntimeHealth(
	ctx context.Context,
	socket string,
) (health mountproto.RuntimeHealthResponse, resultErr error) {
	client, err := mountservice.NewClient(ctx, wire.ClientConfig{Dial: wire.UnixDialer(socket)})
	if err != nil {
		return mountproto.RuntimeHealthResponse{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, client.Close()) }()
	return client.RuntimeHealth(ctx)
}

func validateRuntimeTarget(health mountproto.RuntimeHealthResponse) error {
	if health.Protocol != mountproto.Version || health.Code != mountproto.ErrorCodeOk || health.Message != "" ||
		health.RuntimeBuild == "" || health.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		health.RuntimePID <= 0 || health.ProcessGeneration == "" {
		return errors.New("CCPoolStatus: prior runtime health has the wrong exact generation")
	}
	return nil
}

func validateRuntimeReadiness(target runtimeTarget, health mountproto.RuntimeHealthResponse) error {
	if err := validateRuntimeTarget(health); err != nil {
		return err
	}
	if health.RuntimeBuild != target.buildID || health.ActivationGeneration == "" ||
		health.State != mountproto.RuntimeStateHealthy || health.Draining || health.Busy || !health.Ready ||
		health.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		health.ReadinessStep != mountproto.ReadinessStepPublished ||
		health.NativePhase != mountproto.NativePhaseDisabled || health.NativeMount != nil ||
		health.BrokerPhase != mountproto.BrokerPhaseLive {
		return errors.New("CCPoolStatus: FuseKit runtime is not the exact healthy deployment activation")
	}
	return nil
}

func (h productHooks) proofDigest(kind string, operation deployment.Operation, details ...string) deployment.SHA256 {
	digest := sha256.New()
	values := make([]string, 0, 15+len(details))
	values = append(values,
		holderbridge.DeploymentProofIdentity, kind, operation.ID, string(operation.Role), operation.PlanDigest.String(),
		operation.Generation.Path, operation.Generation.Release.Version,
		operation.Generation.Release.URL, operation.Generation.Release.SHA256.String(),
		operation.Generation.DesignatedRequirement, operation.Generation.CDHash,
		operation.Generation.BundleDigest.String(), operation.Generation.Device, operation.Generation.Inode,
		h.policyDigest.String(),
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
