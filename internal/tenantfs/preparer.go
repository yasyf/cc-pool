package tenantfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
)

// ErrPreparationConflict means FuseKit did not prove one fully converged tenant revision.
var ErrPreparationConflict = errors.New("tenantfs: preparation proof mismatch")

// Preparer converges selected accounts through FuseKit outside account claims.
type Preparer struct {
	runtime PreparationRuntime
}

// NewPreparer constructs the product's on-demand convergence client.
func NewPreparer(runtime PreparationRuntime) (*Preparer, error) {
	if runtime == nil {
		return nil, errors.New("tenantfs: preparer runtime is required")
	}
	return &Preparer{runtime: runtime}, nil
}

// Prepare converges one account and returns its exact FuseKit activation proof.
func (p *Preparer) Prepare(ctx context.Context, account Account) (catalogproto.TenantPreparationProof, error) {
	tenantID, err := account.TenantID()
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	request, err := p.request(ctx, account)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	response, err := p.runtime.PrepareTenant(ctx, catalogproto.TenantID(tenantID), request)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("tenantfs: prepare tenant %q: %w", tenantID, err)
	}
	if response.Protocol != catalogproto.Version || response.Code != catalogproto.ErrorCodeOk ||
		response.Message != "" || response.Proof == nil ||
		!matchingPreparation(*response.Proof, catalogproto.TenantID(tenantID), account, request) {
		return catalogproto.TenantPreparationProof{}, ErrPreparationConflict
	}
	return *response.Proof, nil
}

// Validate confirms that a retained proof belongs to the current activation and account generation.
func (p *Preparer) Validate(ctx context.Context, account Account, proof catalogproto.TenantPreparationProof) error {
	tenantID, err := account.TenantID()
	if err != nil {
		return err
	}
	request, err := p.request(ctx, account)
	if err != nil {
		return err
	}
	if !matchingPreparation(proof, catalogproto.TenantID(tenantID), account, request) {
		return ErrPreparationConflict
	}
	return nil
}

func (p *Preparer) request(ctx context.Context, account Account) (catalogproto.PrepareTenantRequest, error) {
	health, err := p.runtime.RuntimeHealth(ctx)
	if err != nil {
		return catalogproto.PrepareTenantRequest{}, fmt.Errorf("tenantfs: observe FuseKit runtime activation: %w", err)
	}
	if health.Protocol != mountproto.Version || health.Code != mountproto.ErrorCodeOk ||
		health.Message != "" || health.RuntimeBuild != version.String() ||
		health.RuntimeProtocol != mountproto.RuntimeProtocolVersion || health.RuntimePID <= 0 ||
		health.ProcessGeneration == "" || health.ActivationGeneration == "" ||
		health.State != mountproto.RuntimeStateHealthy || health.Draining || health.Busy || !health.Ready ||
		health.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		health.ReadinessStep != mountproto.ReadinessStepPublished ||
		health.NativePhase != mountproto.NativePhaseDisabled || health.NativeMount != nil ||
		health.BrokerPhase != mountproto.BrokerPhaseLive {
		return catalogproto.PrepareTenantRequest{}, ErrPreparationConflict
	}
	return catalogproto.PrepareTenantRequest{
		Protocol:             catalogproto.Version,
		Generation:           account.Generation,
		Presentation:         catalogproto.PresentationKindFileProvider,
		ActivationGeneration: health.ActivationGeneration,
	}, nil
}

func matchingPreparation(
	proof catalogproto.TenantPreparationProof,
	tenantID catalogproto.TenantID,
	account Account,
	request catalogproto.PrepareTenantRequest,
) bool {
	domainID, err := catalogproto.DeriveDomainID(
		OwnerID,
		catalogproto.PresentationInstanceID(account.InstanceID),
	)
	if err != nil {
		return false
	}
	catalogProof := proof.Catalog
	presentation := proof.Presentation
	fileProvider := presentation.FileProvider
	return catalogProof.Tenant == tenantID && catalogProof.Generation == request.Generation &&
		catalogProof.Requested != 0 && catalogProof.Desired == catalogProof.Requested &&
		catalogProof.Observed == catalogProof.Requested && catalogProof.Verified == catalogProof.Requested &&
		catalogProof.Applied == catalogProof.Requested &&
		proof.SourceAuthority == catalogproto.SourceAuthorityID(ClaudeAuthorityID) &&
		proof.SourceRevision != 0 && proof.CatalogRevision == catalogProof.Requested &&
		proof.ChangeID != "" && proof.OperationID != "" &&
		presentation.Kind == catalogproto.PresentationKindFileProvider && presentation.Mount == nil &&
		fileProvider != nil && fileProvider.TenantID == tenantID && fileProvider.DomainID == domainID &&
		fileProvider.Generation == request.Generation && fileProvider.ActivationGeneration == request.ActivationGeneration &&
		exactAbsolutePath(fileProvider.PublicPath)
}

// FileProviderPublicPath returns the OS-observed root from one validated preparation proof.
func FileProviderPublicPath(proof catalogproto.TenantPreparationProof) (string, error) {
	if proof.Presentation.Kind != catalogproto.PresentationKindFileProvider || proof.Presentation.Mount != nil ||
		proof.Presentation.FileProvider == nil {
		return "", ErrPreparationConflict
	}
	publicPath := proof.Presentation.FileProvider.PublicPath
	if err := ValidateFileProviderPublicPath(publicPath); err != nil {
		return "", err
	}
	return publicPath, nil
}

// ValidateFileProviderPublicPath rejects anything except one exact absolute OS path.
func ValidateFileProviderPublicPath(value string) error {
	if !exactAbsolutePath(value) {
		return ErrPreparationConflict
	}
	return nil
}
