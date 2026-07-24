package tenantfs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/holder"
)

// ErrPreparationConflict means FuseKit did not prove one fully converged tenant revision.
var ErrPreparationConflict = errors.New("tenantfs: preparation proof mismatch")

// PreparationRuntime is cc-pool's exact holder-local convergence surface.
type PreparationRuntime interface {
	Readiness(context.Context) (holder.LocalRuntimeReadiness, error)
	PrepareTenant(context.Context, catalog.TenantID, holder.LocalPreparationRequest) (catalogproto.TenantPreparationProof, error)
}

// Preparer converges selected accounts through FuseKit outside account claims.
type Preparer struct {
	runtime PreparationRuntime
}

// PreparationLease binds one launch reservation to provisional File Provider readiness.
type PreparationLease struct {
	ID        string
	ExpiresAt time.Time
}

// NewPreparer constructs the product's on-demand convergence client.
func NewPreparer(runtime PreparationRuntime) (*Preparer, error) {
	if runtime == nil {
		return nil, errors.New("tenantfs: preparer runtime is required")
	}
	return &Preparer{runtime: runtime}, nil
}

// Prepare converges one account and returns its exact FuseKit activation proof.
func (p *Preparer) Prepare(
	ctx context.Context,
	account Account,
	lease PreparationLease,
) (catalogproto.TenantPreparationProof, error) {
	tenantID, err := account.TenantID()
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	request, err := p.request(account, lease)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	proof, err := p.runtime.PrepareTenant(ctx, tenantID, request)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("tenantfs: prepare tenant %q: %w", tenantID, err)
	}
	readiness, err := p.runtime.Readiness(ctx)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("tenantfs: observe holder readiness: %w", err)
	}
	if !matchingPreparation(proof, catalogproto.TenantID(tenantID), account, request, readiness) {
		return catalogproto.TenantPreparationProof{}, ErrPreparationConflict
	}
	return proof, nil
}

// Validate confirms that a retained proof belongs to the current activation and account generation.
func (p *Preparer) Validate(
	ctx context.Context,
	account Account,
	lease PreparationLease,
	proof catalogproto.TenantPreparationProof,
) error {
	tenantID, err := account.TenantID()
	if err != nil {
		return err
	}
	request, err := p.request(account, lease)
	if err != nil {
		return err
	}
	readiness, err := p.runtime.Readiness(ctx)
	if err != nil {
		return fmt.Errorf("tenantfs: observe holder readiness: %w", err)
	}
	if !matchingPreparation(proof, catalogproto.TenantID(tenantID), account, request, readiness) {
		return ErrPreparationConflict
	}
	return nil
}

func (p *Preparer) request(
	account Account,
	lease PreparationLease,
) (holder.LocalPreparationRequest, error) {
	if err := validatePreparationLease(lease); err != nil {
		return holder.LocalPreparationRequest{}, err
	}
	tenantID, err := account.TenantID()
	if err != nil {
		return holder.LocalPreparationRequest{}, err
	}
	critical, err := newCriticalReadinessPolicy(tenantID)
	if err != nil {
		return holder.LocalPreparationRequest{}, fmt.Errorf("tenantfs: build critical readiness policy: %w", err)
	}
	return holder.LocalPreparationRequest{
		Generation:      catalog.Generation(account.Generation),
		Presentation:    catalog.PresentationFileProvider,
		CriticalObjects: critical.Objects,
		LeaseID:         lease.ID,
		LeaseExpiresAt:  lease.ExpiresAt.UTC(),
	}, nil
}

func validatePreparationLease(lease PreparationLease) error {
	raw, err := hex.DecodeString(lease.ID)
	if err != nil || len(raw) != 16 || lease.ID != strings.ToLower(lease.ID) ||
		lease.ExpiresAt.IsZero() || lease.ExpiresAt.UnixNano() <= 0 {
		return errors.New("tenantfs: preparation lease is invalid")
	}
	return nil
}

func matchingPreparation(
	proof catalogproto.TenantPreparationProof,
	tenantID catalogproto.TenantID,
	account Account,
	request holder.LocalPreparationRequest,
	readiness holder.LocalRuntimeReadiness,
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
	return readiness.RuntimeBuild == version.String() && readiness.ActivationGeneration != "" &&
		catalogProof.Tenant == tenantID && catalogProof.Generation == uint64(request.Generation) &&
		catalogProof.Requested != 0 && catalogProof.Desired == catalogProof.Requested &&
		catalogProof.Observed == catalogProof.Requested && catalogProof.Verified == catalogProof.Requested &&
		catalogProof.Applied == catalogProof.Requested &&
		proof.SourceAuthority == catalogproto.SourceAuthorityID(ClaudeAuthorityID) &&
		proof.SourceRevision != 0 && proof.CatalogRevision == catalogProof.Requested &&
		proof.ChangeID != "" && proof.OperationID != "" &&
		presentation.Kind == catalogproto.PresentationKindFileProvider && presentation.Mount == nil &&
		fileProvider != nil && fileProvider.TenantID == tenantID && fileProvider.DomainID == domainID &&
		fileProvider.Generation == uint64(request.Generation) && fileProvider.ActivationGeneration == readiness.ActivationGeneration &&
		fileProvider.PresentationInstanceID == catalogproto.PresentationInstanceID(account.InstanceID) &&
		fileProvider.RootID != "" && exactAbsolutePath(fileProvider.PublicPath) && matchingCriticalReadiness(proof, request)
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
