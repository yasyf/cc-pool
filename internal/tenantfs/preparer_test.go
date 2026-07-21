package tenantfs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalogproto"
)

type recordingPreparationRuntime struct {
	tenant   catalogproto.TenantID
	request  catalogproto.PrepareTenantRequest
	response catalogproto.PrepareTenantResponse
	err      error
	called   int
}

func (r *recordingPreparationRuntime) PrepareTenant(
	_ context.Context,
	tenantID catalogproto.TenantID,
	request catalogproto.PrepareTenantRequest,
) (catalogproto.PrepareTenantResponse, error) {
	r.called++
	r.tenant = tenantID
	r.request = request
	if r.err != nil {
		return catalogproto.PrepareTenantResponse{}, r.err
	}
	if r.response.Proof == nil {
		proof := exactPreparationProof(tenantID, request)
		return catalogproto.PrepareTenantResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
		}, nil
	}
	return r.response, nil
}

func TestPreparerRequestsOnlyTenantGeneration(t *testing.T) {
	account := preparationAccount(t)
	runtime := &recordingPreparationRuntime{}
	preparer, err := NewPreparer(runtime)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := preparer.Prepare(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, _ := account.TenantID()
	if runtime.called != 1 || runtime.tenant != catalogproto.TenantID(tenantID) ||
		runtime.request != (catalogproto.PrepareTenantRequest{
			Protocol: catalogproto.Version, Generation: account.Generation,
		}) {
		t.Fatalf("prepare call = tenant %q request %+v", runtime.tenant, runtime.request)
	}
	if !matchingPreparation(proof, runtime.tenant, runtime.request) {
		t.Fatalf("proof = %+v", proof)
	}
}

func TestPreparerRejectsForeignAuthorityProof(t *testing.T) {
	account := preparationAccount(t)
	tenantID, _ := account.TenantID()
	request := catalogproto.PrepareTenantRequest{
		Protocol: catalogproto.Version, Generation: account.Generation,
	}
	proof := exactPreparationProof(catalogproto.TenantID(tenantID), request)
	proof.SourceAuthority = "foreign"
	runtime := &recordingPreparationRuntime{response: catalogproto.PrepareTenantResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
	}}
	preparer, err := NewPreparer(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preparer.Prepare(t.Context(), account); !errors.Is(err, ErrPreparationConflict) {
		t.Fatalf("Prepare error = %v, want ErrPreparationConflict", err)
	}
}

func preparationAccount(t *testing.T) Account {
	t.Helper()
	root := t.TempDir()
	return Account{
		InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
		PresentationRoot: filepath.Join(root, "presentation"),
		BackingRoot:      filepath.Join(root, "backing"),
	}
}

func exactPreparationProof(
	tenantID catalogproto.TenantID,
	request catalogproto.PrepareTenantRequest,
) catalogproto.TenantPreparationProof {
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: tenantID, Generation: request.Generation,
			Requested: 11, Desired: 11, Observed: 11, Verified: 11, Applied: 11,
		},
		SourceAuthority: catalogproto.SourceAuthorityID(ClaudeAuthorityID),
		SourceRevision:  17,
		CatalogRevision: 11,
		ChangeID:        "11111111111111111111111111111111",
		OperationID:     "22222222222222222222222222222222",
	}
}
