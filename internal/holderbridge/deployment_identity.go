package holderbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/deployment"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
)

const (
	consumerBuildDomain      = "cc-pool.deployment-callbacks.v1@sha256:"
	deploymentPolicyIdentity = "cc-pool.deployment-callbacks.v1"
	// DeploymentProofIdentity is the v1 product-proof digest domain.
	DeploymentProofIdentity = "cc-pool.deployment-proof.v1"
	// DeploymentServiceLabel is the exact status app launch-agent label.
	DeploymentServiceLabel = BundleID + ".fusekit"
	// DeploymentElectionTimeout is the exact File Provider election deadline.
	DeploymentElectionTimeout = 5 * time.Second
	// DeploymentPollInterval is the exact deployment observation cadence.
	DeploymentPollInterval = 100 * time.Millisecond
)

var (
	startupConsumerBuild, startupConsumerBuildErr = currentConsumerBuild()
	startupPolicyDigest, startupPolicyDigestErr   = makeDeploymentPolicyDigest()
)

type deploymentPolicy struct {
	Identity     string                       `json:"identity"`
	Schema       uint16                       `json:"schema"`
	Application  deploymentApplicationPolicy  `json:"application"`
	FileProvider deploymentFileProviderPolicy `json:"file_provider"`
	Protocols    deploymentProtocolPolicy     `json:"protocols"`
	Proofs       deploymentProofPolicy        `json:"proofs"`
	Runtime      deploymentRuntimePolicy      `json:"runtime"`
	Service      deploymentServicePolicy      `json:"service"`
}

type deploymentApplicationPolicy struct {
	BundleID                    string `json:"bundle_id"`
	TeamID                      string `json:"team_id"`
	InstallRootHomeRelative     string `json:"install_root_home_relative"`
	BundleLeaf                  string `json:"bundle_leaf"`
	ExecutableName              string `json:"executable_name"`
	ExecutableRelativePath      string `json:"executable_relative_path"`
	RequireCanonicalAccountHome bool   `json:"require_canonical_account_home"`
	StopControlRole             string `json:"stop_control_role"`
}

type deploymentFileProviderPolicy struct {
	BundleID              string        `json:"bundle_id"`
	ExtensionRelativePath string        `json:"extension_relative_path"`
	AppGroup              string        `json:"app_group"`
	RequireRegistration   bool          `json:"require_registration"`
	RequireEnabled        bool          `json:"require_enabled"`
	RequireExactElection  bool          `json:"require_exact_election"`
	ElectionTimeout       time.Duration `json:"election_timeout_ns"`
	ElectionPoll          time.Duration `json:"election_poll_ns"`
}

type deploymentProtocolPolicy struct {
	MountProtocol   uint16 `json:"mount_protocol"`
	RuntimeProtocol uint16 `json:"runtime_protocol"`
	WireProtocol    uint16 `json:"wire_protocol"`
	WireBuild       string `json:"wire_build"`
}

type deploymentRuntimePolicy struct {
	State     deploymentRuntimeStatePolicy `json:"state"`
	Native    deploymentNativePolicy       `json:"native"`
	Source    deploymentSourcePolicy       `json:"source"`
	Broker    deploymentBrokerPolicy       `json:"broker"`
	Readiness deploymentReadinessPolicy    `json:"readiness"`
}

type deploymentRuntimeStatePolicy struct {
	HomeRelativeDirectory    string `json:"home_relative_directory"`
	SocketName               string `json:"socket_name"`
	CatalogName              string `json:"catalog_name"`
	ProcessStoreName         string `json:"process_store_name"`
	LogName                  string `json:"log_name"`
	SourceObserverDirectory  string `json:"source_observer_directory_pattern"`
	SourceObserverSocketName string `json:"source_observer_socket_name"`
	RuntimePolicyDigest      string `json:"runtime_policy_digest"`
}

type deploymentNativePolicy struct {
	Enabled        bool                   `json:"enabled"`
	RequiredPhase  mountproto.NativePhase `json:"required_phase"`
	RequireNoProof bool                   `json:"require_no_proof"`
}

type deploymentSourcePolicy struct {
	Capable bool `json:"capable"`
}

type deploymentBrokerPolicy struct {
	Enabled                     bool `json:"enabled"`
	RequireReconciledFixedPoint bool `json:"require_reconciled_fixed_point"`
}

type deploymentReadinessPolicy struct {
	StartupTimeout              time.Duration             `json:"startup_timeout_ns"`
	SettlementTimeout           time.Duration             `json:"settlement_timeout_ns"`
	ObservationTimeout          time.Duration             `json:"observation_timeout_ns"`
	NativeReadinessTimeout      time.Duration             `json:"native_readiness_timeout_ns"`
	SourceReadinessTimeout      time.Duration             `json:"source_readiness_timeout_ns"`
	CatalogReadinessTimeout     time.Duration             `json:"catalog_readiness_timeout_ns"`
	CatalogOperationTimeout     time.Duration             `json:"catalog_operation_timeout_ns"`
	RuntimeShutdownTimeout      time.Duration             `json:"runtime_shutdown_timeout_ns"`
	PollInterval                time.Duration             `json:"poll_interval_ns"`
	RequiredState               mountproto.RuntimeState   `json:"required_state"`
	RequiredPhase               mountproto.ReadinessPhase `json:"required_phase"`
	RequiredStep                mountproto.ReadinessStep  `json:"required_step"`
	RequiredBrokerPhase         mountproto.BrokerPhase    `json:"required_broker_phase"`
	RequireReady                bool                      `json:"require_ready"`
	RequireNotDraining          bool                      `json:"require_not_draining"`
	RequireNotBusy              bool                      `json:"require_not_busy"`
	RequireRuntimeBuildMatch    bool                      `json:"require_runtime_build_match"`
	RequirePositiveRuntimePID   bool                      `json:"require_positive_runtime_pid"`
	RequireProcessGeneration    bool                      `json:"require_process_generation"`
	RequireActivationGeneration bool                      `json:"require_activation_generation"`
	RequireEmptyMessage         bool                      `json:"require_empty_message"`
	RequiredErrorCode           mountproto.ErrorCode      `json:"required_error_code"`
}

type deploymentServicePolicy struct {
	AgentLabel                     string                  `json:"agent_label"`
	ExactSingleAgentPlan           bool                    `json:"exact_single_agent_plan"`
	AssociatedBundleID             string                  `json:"associated_bundle_id"`
	RequireExactAssociatedBundleID bool                    `json:"require_exact_associated_bundle_id"`
	RestartPolicy                  service.RestartPolicy   `json:"restart_policy"`
	StartInterval                  time.Duration           `json:"start_interval"`
	ProcessType                    service.ProcessType     `json:"process_type"`
	SessionType                    service.SessionType     `json:"session_type"`
	ProgramIsFixedBundleExecutable bool                    `json:"program_is_fixed_bundle_executable"`
	RequireNoArguments             bool                    `json:"require_no_arguments"`
	LogPathIsRuntimeStateLog       bool                    `json:"log_path_is_runtime_state_log"`
	RequireNoWatchPaths            bool                    `json:"require_no_watch_paths"`
	RequireNoCalendarIntervals     bool                    `json:"require_no_calendar_intervals"`
	BuildEnvironmentKey            string                  `json:"build_environment_key"`
	RequireExactBuildEnvironment   bool                    `json:"require_exact_build_environment"`
	ReplacementOwnsRestartFence    bool                    `json:"replacement_owns_restart_fence"`
	Quiesce                        deploymentQuiescePolicy `json:"quiesce"`
}

type deploymentProofPolicy struct {
	Identity                   string               `json:"identity"`
	PostInstallRole            deployment.ProofRole `json:"post_install_role"`
	CandidateReadyRole         deployment.ProofRole `json:"candidate_ready_role"`
	PriorRestoreRole           deployment.ProofRole `json:"prior_restore_role"`
	PriorReadyRole             deployment.ProofRole `json:"prior_ready_role"`
	PriorRuntimeRole           deployment.ProofRole `json:"prior_runtime_role"`
	RollbackRuntimeRole        deployment.ProofRole `json:"rollback_runtime_role"`
	RequireReturnedRoleMatch   bool                 `json:"require_returned_role_match"`
	RequireReadinessPlanDigest bool                 `json:"require_readiness_plan_digest"`
	BindGenerationCDHash       bool                 `json:"bind_generation_cdhash"`
	BindGenerationBundleDigest bool                 `json:"bind_generation_bundle_digest"`
}

type deploymentQuiescePolicy struct {
	StopControlArguments            []string          `json:"stop_control_arguments"`
	UseDaemonkitOperationIntent     bool              `json:"use_daemonkit_operation_intent"`
	AcceptedStopIntents             []wire.StopIntent `json:"accepted_stop_intents"`
	StopAuthorityUsesConsumerBuild  bool              `json:"stop_authority_uses_consumer_build"`
	RuntimeProofBindsIntent         bool              `json:"runtime_proof_binds_intent"`
	RuntimeProofBindsCallerBuild    bool              `json:"runtime_proof_binds_caller_build"`
	RuntimeProofBindsObservedBuild  bool              `json:"runtime_proof_binds_observed_build"`
	RequireTargetProcessGeneration  bool              `json:"require_target_process_generation"`
	RequireExactExecutableInventory bool              `json:"require_exact_executable_inventory"`
	AbsentRequiresEmptyInventory    bool              `json:"absent_requires_empty_inventory"`
	RequireExactHealthTarget        bool              `json:"require_exact_health_target"`
	RequireExactStopResult          bool              `json:"require_exact_stop_result"`
}

// DeploymentIdentity returns the startup-frozen updater build and callback policy identities.
func DeploymentIdentity() (string, deployment.SHA256, error) {
	if startupConsumerBuildErr != nil {
		return "", deployment.SHA256{}, fmt.Errorf("CCPoolStatus: cache deployment consumer build: %w", startupConsumerBuildErr)
	}
	if startupPolicyDigestErr != nil {
		return "", deployment.SHA256{}, fmt.Errorf("CCPoolStatus: cache deployment policy digest: %w", startupPolicyDigestErr)
	}
	return startupConsumerBuild, startupPolicyDigest, nil
}

func currentConsumerBuild() (string, error) {
	path, err := service.CanonicalExecutable()
	if err != nil {
		return "", err
	}
	return consumerBuildForExecutable(path)
}

func consumerBuildForExecutable(path string) (_ string, resultErr error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("CCPoolStatus: updater executable path is not exact and absolute")
	}
	file, err := os.OpenInRoot(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: open updater executable: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("CCPoolStatus: close updater executable: %w", err))
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("CCPoolStatus: inspect updater executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("CCPoolStatus: updater executable is not an executable regular file")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fmt.Errorf("CCPoolStatus: hash updater executable: %w", err)
	}
	return consumerBuildDomain + hex.EncodeToString(digest.Sum(nil)), nil
}

func makeDeploymentPolicyDigest() (deployment.SHA256, error) {
	payload, err := deploymentPolicyJSON()
	if err != nil {
		return deployment.SHA256{}, err
	}
	return deployment.SHA256(sha256.Sum256(payload)), nil
}

func deploymentPolicyJSON() ([]byte, error) {
	readiness := ReadinessContract()
	return json.Marshal(deploymentPolicy{
		Identity: deploymentPolicyIdentity,
		Schema:   1,
		Application: deploymentApplicationPolicy{
			BundleID: BundleID, TeamID: TeamID, InstallRootHomeRelative: "Applications",
			BundleLeaf: ExecutableName + ".app", ExecutableName: ExecutableName,
			ExecutableRelativePath:      "Contents/MacOS/" + ExecutableName,
			RequireCanonicalAccountHome: true,
			StopControlRole:             StopRoleID,
		},
		FileProvider: deploymentFileProviderPolicy{
			BundleID:              "com.yasyf.cc-pool.status.fileprovider",
			ExtensionRelativePath: "Contents/PlugIns/CCPoolFileProvider.appex",
			AppGroup:              AppGroup,
			RequireRegistration:   true, RequireEnabled: true, RequireExactElection: true,
			ElectionTimeout: DeploymentElectionTimeout, ElectionPoll: DeploymentPollInterval,
		},
		Protocols: deploymentProtocolPolicy{
			MountProtocol: mountproto.Version, RuntimeProtocol: mountproto.RuntimeProtocolVersion,
			WireProtocol: transportproto.Version, WireBuild: transportproto.WireBuild,
		},
		Proofs: deploymentProofPolicy{
			Identity:        DeploymentProofIdentity,
			PostInstallRole: deployment.ProofPostInstall, CandidateReadyRole: deployment.ProofCandidateReady,
			PriorRestoreRole: deployment.ProofPriorRestore, PriorReadyRole: deployment.ProofPriorReady,
			PriorRuntimeRole: deployment.ProofPriorRuntime, RollbackRuntimeRole: deployment.ProofRollbackRuntime,
			RequireReturnedRoleMatch: true, RequireReadinessPlanDigest: true,
			BindGenerationCDHash: true, BindGenerationBundleDigest: true,
		},
		Runtime: deploymentRuntimePolicy{
			State: deploymentRuntimeStatePolicy{
				HomeRelativeDirectory: ".cc-pool/fusekit", SocketName: "fusekit.sock",
				CatalogName: "catalog.sqlite", ProcessStoreName: "processes.db", LogName: "holder.log",
				SourceObserverDirectory: "source-observer-0000000000", SourceObserverSocketName: "observer.sock",
				RuntimePolicyDigest: hex.EncodeToString(runtimePolicyDigest[:]),
			},
			Native: deploymentNativePolicy{
				Enabled: false, RequiredPhase: mountproto.NativePhaseDisabled, RequireNoProof: true,
			},
			Source: deploymentSourcePolicy{Capable: true},
			Broker: deploymentBrokerPolicy{
				Enabled: true, RequireReconciledFixedPoint: true,
			},
			Readiness: deploymentReadinessPolicy{
				StartupTimeout: readiness.StartupTimeout(), SettlementTimeout: readiness.SettlementTimeout(),
				ObservationTimeout:     readiness.ObservationTimeout(),
				NativeReadinessTimeout: NativeReadinessTimeout, SourceReadinessTimeout: SourceReadinessTimeout,
				CatalogReadinessTimeout: CatalogReadinessTimeout, CatalogOperationTimeout: CatalogOperationTimeout,
				RuntimeShutdownTimeout: RuntimeShutdownTimeout, PollInterval: DeploymentPollInterval,
				RequiredState: mountproto.RuntimeStateHealthy,
				RequiredPhase: mountproto.ReadinessPhaseReady, RequiredStep: mountproto.ReadinessStepPublished,
				RequiredBrokerPhase: mountproto.BrokerPhaseLive, RequireReady: true, RequireNotDraining: true,
				RequireNotBusy: true, RequireRuntimeBuildMatch: true, RequirePositiveRuntimePID: true,
				RequireProcessGeneration: true, RequireActivationGeneration: true,
				RequireEmptyMessage: true, RequiredErrorCode: mountproto.ErrorCodeOk,
			},
		},
		Service: deploymentServicePolicy{
			AgentLabel: DeploymentServiceLabel, ExactSingleAgentPlan: true, AssociatedBundleID: BundleID,
			RequireExactAssociatedBundleID: true,
			RestartPolicy:                  service.RestartAlways, StartInterval: 0, ProcessType: 0,
			SessionType:                    service.SessionTypeAqua,
			ProgramIsFixedBundleExecutable: true, RequireNoArguments: true,
			LogPathIsRuntimeStateLog: true, RequireNoWatchPaths: true, RequireNoCalendarIntervals: true,
			BuildEnvironmentKey: "FUSEKIT_BUILD_ID", RequireExactBuildEnvironment: true,
			ReplacementOwnsRestartFence: true,
			Quiesce: deploymentQuiescePolicy{
				StopControlArguments:        holder.StopControlChildArguments(),
				UseDaemonkitOperationIntent: true,
				AcceptedStopIntents: []wire.StopIntent{
					wire.StopIntentUpgrade, wire.StopIntentRestart, wire.StopIntentUninstall,
				},
				StopAuthorityUsesConsumerBuild: true, RuntimeProofBindsIntent: true,
				RuntimeProofBindsCallerBuild: true, RuntimeProofBindsObservedBuild: true,
				RequireTargetProcessGeneration:  true,
				RequireExactExecutableInventory: true, AbsentRequiresEmptyInventory: true,
				RequireExactHealthTarget: true, RequireExactStopResult: true,
			},
		},
	})
}
