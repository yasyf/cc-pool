package tenantfs

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/holder"
)

type recordingPreparationRuntime struct {
	readiness    holder.LocalRuntimeReadiness
	readinessErr error
	tenant       catalog.TenantID
	request      holder.LocalPreparationRequest
	proof        *catalogproto.TenantPreparationProof
	err          error
	called       int
}

func (r *recordingPreparationRuntime) Readiness(context.Context) (holder.LocalRuntimeReadiness, error) {
	if r.readinessErr != nil {
		return holder.LocalRuntimeReadiness{}, r.readinessErr
	}
	if r.readiness.ActivationGeneration == "" {
		return exactRuntimeReadiness(), nil
	}
	return r.readiness, nil
}

func (r *recordingPreparationRuntime) PrepareTenant(
	_ context.Context,
	tenantID catalog.TenantID,
	request holder.LocalPreparationRequest,
) (catalogproto.TenantPreparationProof, error) {
	r.called++
	r.tenant = tenantID
	r.request = request
	if r.err != nil {
		return catalogproto.TenantPreparationProof{}, r.err
	}
	if r.proof == nil {
		return exactPreparationProof(
			catalogproto.TenantID(tenantID), request, "activation-7",
			"0123456789abcdef0123456789abcdef",
		), nil
	}
	return *r.proof, nil
}

func TestPreparerFencesFileProviderPreparationToObservedActivation(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	runtime := &recordingPreparationRuntime{}
	preparer, err := NewPreparer(runtime)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := preparer.Prepare(t.Context(), account, lease)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, _ := account.TenantID()
	critical, err := newCriticalReadinessPolicy(tenantID)
	if err != nil {
		t.Fatal(err)
	}
	want := holder.LocalPreparationRequest{
		Generation: catalog.Generation(account.Generation), Presentation: catalog.PresentationFileProvider,
		CriticalObjects: critical.Objects, LeaseID: lease.ID, LeaseExpiresAt: lease.ExpiresAt,
	}
	if runtime.called != 1 || runtime.tenant != tenantID || !reflect.DeepEqual(runtime.request, want) {
		t.Fatalf("prepare call = tenant %q request %+v", runtime.tenant, runtime.request)
	}
	if !matchingPreparation(
		proof, catalogproto.TenantID(runtime.tenant), account, runtime.request, exactRuntimeReadiness(),
	) {
		t.Fatalf("proof = %+v", proof)
	}
}

func TestPreparerRejectsForeignAuthorityProof(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	tenantID, _ := account.TenantID()
	request := exactPreparationRequest(account, lease)
	proof := exactPreparationProof(catalogproto.TenantID(tenantID), request, "activation-7", account.InstanceID)
	proof.SourceAuthority = "foreign"
	runtime := &recordingPreparationRuntime{proof: &proof}
	preparer, err := NewPreparer(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preparer.Prepare(t.Context(), account, lease); !errors.Is(err, ErrPreparationConflict) {
		t.Fatalf("Prepare error = %v, want ErrPreparationConflict", err)
	}
}

func TestPreparerRejectsUnavailableAndStaleActivation(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	unavailable := errors.New("holder unavailable")
	request := exactPreparationRequest(account, lease)
	tenantID, _ := account.TenantID()
	stale := exactPreparationProof(catalogproto.TenantID(tenantID), request, "stale-activation", account.InstanceID)
	for name, runtime := range map[string]*recordingPreparationRuntime{
		"unavailable":      {readinessErr: unavailable},
		"stale activation": {proof: &stale},
	} {
		t.Run(name, func(t *testing.T) {
			preparer, err := NewPreparer(runtime)
			if err != nil {
				t.Fatal(err)
			}
			_, err = preparer.Prepare(t.Context(), account, lease)
			if name == "unavailable" {
				if !errors.Is(err, unavailable) {
					t.Fatalf("Prepare error = %v, want unavailable", err)
				}
				return
			}
			if !errors.Is(err, ErrPreparationConflict) {
				t.Fatalf("Prepare error = %v, want ErrPreparationConflict", err)
			}
		})
	}
}

func TestPreparerValidateRejectsActivationRollover(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	runtime := &recordingPreparationRuntime{}
	preparer, err := NewPreparer(runtime)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := preparer.Prepare(t.Context(), account, lease)
	if err != nil {
		t.Fatal(err)
	}
	runtime.readiness = exactRuntimeReadiness()
	runtime.readiness.ActivationGeneration = "activation-8"
	if err := preparer.Validate(t.Context(), account, lease, proof); !errors.Is(err, ErrPreparationConflict) {
		t.Fatalf("Validate error = %v, want ErrPreparationConflict", err)
	}
}

func preparationAccount(t *testing.T) Account {
	t.Helper()
	root := t.TempDir()
	return Account{
		InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
		BackingRoot: filepath.Join(root, "backing"), FileProviderDisplayName: "acct-07",
	}
}

func exactPreparationRequest(account Account, lease PreparationLease) holder.LocalPreparationRequest {
	tenantID, err := account.TenantID()
	if err != nil {
		panic(err)
	}
	critical, err := newCriticalReadinessPolicy(tenantID)
	if err != nil {
		panic(err)
	}
	return holder.LocalPreparationRequest{
		Generation: catalog.Generation(account.Generation), Presentation: catalog.PresentationFileProvider,
		CriticalObjects: critical.Objects, LeaseID: lease.ID, LeaseExpiresAt: lease.ExpiresAt,
	}
}

func exactPreparationProof(
	tenantID catalogproto.TenantID,
	request holder.LocalPreparationRequest,
	activationGeneration, instanceID string,
) catalogproto.TenantPreparationProof {
	domainID, err := catalogproto.DeriveDomainID(OwnerID, catalogproto.PresentationInstanceID(instanceID))
	if err != nil {
		panic(err)
	}
	policyDigest, err := criticalReadinessPolicyDigest(request.CriticalObjects)
	if err != nil {
		panic(err)
	}
	proof := catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: tenantID, Generation: uint64(request.Generation),
			Requested: 11, Desired: 11, Observed: 11, Verified: 11, Applied: 11,
		},
		SourceAuthority: catalogproto.SourceAuthorityID(ClaudeAuthorityID), SourceRevision: 17,
		CatalogRevision: 11, ChangeID: "11111111111111111111111111111111",
		OperationID: "22222222222222222222222222222222", SourcePublication: "55555555555555555555555555555555",
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				TenantID: tenantID, DomainID: domainID, Generation: uint64(request.Generation),
				PublicPath:             filepath.Join("/Users/test/Library/CloudStorage", string(domainID)),
				ActivationGeneration:   activationGeneration,
				PresentationInstanceID: catalogproto.PresentationInstanceID(instanceID),
				RootID:                 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
		},
	}
	objects := make([]catalogproto.ResolvedCriticalObjectProof, len(request.CriticalObjects))
	for index, object := range request.CriticalObjects {
		objectID := "11111111111111111111111111111111"
		hash := "3333333333333333333333333333333333333333333333333333333333333333"
		if index == 1 {
			objectID = "22222222222222222222222222222222"
			hash = "4444444444444444444444444444444444444444444444444444444444444444"
		}
		objects[index] = catalogproto.ResolvedCriticalObjectProof{
			LogicalID: object.LogicalID, Role: object.Role, ObjectID: catalogproto.ObjectID(objectID),
			ObjectRevision: uint64(21 + index), ContentRevision: uint64(31 + index),
			Size: uint64(41 + index), Hash: hash,
		}
	}
	proof.CriticalReadiness = &catalogproto.CriticalReadinessProof{
		PolicyDigest: policyDigest, CatalogHead: proof.CatalogRevision, SourceRevision: proof.SourceRevision,
		TenantGeneration: proof.Catalog.Generation, DomainID: domainID,
		PresentationInstanceID: catalogproto.PresentationInstanceID(instanceID),
		RootID:                 proof.Presentation.FileProvider.RootID, ActivationGeneration: activationGeneration, Objects: objects,
	}
	digest, ok := criticalReadinessResolutionDigest(proof)
	if !ok {
		panic("test preparation proof has no critical resolution digest")
	}
	proof.CriticalReadiness.ResolutionDigest = digest
	proof.CriticalReadiness.Lease = catalogproto.FileProviderLeaseReceipt{
		LeaseID: request.LeaseID, TenantID: tenantID, DomainID: domainID,
		Generation: uint64(request.Generation), RootID: proof.Presentation.FileProvider.RootID,
		PresentationInstanceID: catalogproto.PresentationInstanceID(instanceID),
		State:                  catalogproto.FileProviderLeaseStateProvisional, PolicyDigest: policyDigest,
		ResolutionDigest: digest, CatalogHead: proof.CatalogRevision,
		SourceAuthority: proof.SourceAuthority, SourcePublication: proof.SourcePublication,
		SourceRevision: proof.SourceRevision, ActivationGeneration: activationGeneration,
		ExpiresUnixNano: uint64(request.LeaseExpiresAt.UnixNano()),
	}
	return proof
}

func testPreparationLease() PreparationLease {
	return PreparationLease{
		ID: "abababababababababababababababab", ExpiresAt: time.Unix(1_800_000_000, 123).UTC(),
	}
}

func exactRuntimeReadiness() holder.LocalRuntimeReadiness {
	return holder.LocalRuntimeReadiness{
		RuntimeBuild: version.String(), ProcessGeneration: catalog.ProcessGeneration{1},
		ActivationGeneration: "activation-7",
	}
}
