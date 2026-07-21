package tenantfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalogproto"
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
	request := catalogproto.PrepareTenantRequest{
		Protocol:   catalogproto.Version,
		Generation: account.Generation,
	}
	response, err := p.runtime.PrepareTenant(ctx, catalogproto.TenantID(tenantID), request)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("tenantfs: prepare tenant %q: %w", tenantID, err)
	}
	if response.Protocol != catalogproto.Version || response.Code != catalogproto.ErrorCodeOk ||
		response.Message != "" || response.Proof == nil ||
		!matchingPreparation(*response.Proof, catalogproto.TenantID(tenantID), request) {
		return catalogproto.TenantPreparationProof{}, ErrPreparationConflict
	}
	return *response.Proof, nil
}

// Validate confirms that a retained proof belongs to the current account generation.
func (p *Preparer) Validate(account Account, proof catalogproto.TenantPreparationProof) error {
	tenantID, err := account.TenantID()
	if err != nil {
		return err
	}
	request := catalogproto.PrepareTenantRequest{
		Protocol:   catalogproto.Version,
		Generation: account.Generation,
	}
	if !matchingPreparation(proof, catalogproto.TenantID(tenantID), request) {
		return ErrPreparationConflict
	}
	return nil
}

func matchingPreparation(
	proof catalogproto.TenantPreparationProof,
	tenantID catalogproto.TenantID,
	request catalogproto.PrepareTenantRequest,
) bool {
	catalogProof := proof.Catalog
	return catalogProof.Tenant == tenantID && catalogProof.Generation == request.Generation &&
		catalogProof.Requested != 0 && catalogProof.Desired == catalogProof.Requested &&
		catalogProof.Observed == catalogProof.Requested && catalogProof.Verified == catalogProof.Requested &&
		catalogProof.Applied == catalogProof.Requested &&
		proof.SourceAuthority == catalogproto.SourceAuthorityID(ClaudeAuthorityID) &&
		proof.SourceRevision != 0 && proof.CatalogRevision == catalogProof.Requested &&
		proof.ChangeID != "" && proof.OperationID != ""
}
