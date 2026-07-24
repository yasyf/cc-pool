package tenantfs

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
)

type recordingPreparationRuntime struct {
	health    mountproto.RuntimeHealthResponse
	healthErr error
	tenant    catalogproto.TenantID
	request   catalogproto.PrepareTenantRequest
	response  catalogproto.PrepareTenantResponse
	err       error
	called    int
}

func (r *recordingPreparationRuntime) RuntimeHealth(context.Context) (mountproto.RuntimeHealthResponse, error) {
	if r.healthErr != nil {
		return mountproto.RuntimeHealthResponse{}, r.healthErr
	}
	if r.health.ActivationGeneration == "" {
		return exactRuntimeHealth(), nil
	}
	return r.health, nil
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
		proof := exactPreparationProof(tenantID, request, "0123456789abcdef0123456789abcdef")
		return catalogproto.PrepareTenantResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
		}, nil
	}
	return r.response, nil
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
	if runtime.called != 1 || runtime.tenant != catalogproto.TenantID(tenantID) ||
		!reflect.DeepEqual(runtime.request, catalogproto.PrepareTenantRequest{
			Protocol: catalogproto.Version, Generation: account.Generation,
			Presentation:         catalogproto.PresentationKindFileProvider,
			ActivationGeneration: "activation-7",
			CriticalPolicyDigest: critical.Digest,
			CriticalObjects:      critical.Objects,
			LeaseID:              lease.ID,
			LeaseExpiresUnixNano: uint64(lease.ExpiresAt.UnixNano()),
		}) {
		t.Fatalf("prepare call = tenant %q request %+v", runtime.tenant, runtime.request)
	}
	if !matchingPreparation(proof, runtime.tenant, account, runtime.request) {
		t.Fatalf("proof = %+v", proof)
	}
}

func TestPreparerRejectsForeignAuthorityProof(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	tenantID, _ := account.TenantID()
	request := exactPreparationRequest(account, "activation-7", lease)
	proof := exactPreparationProof(catalogproto.TenantID(tenantID), request, account.InstanceID)
	proof.SourceAuthority = "foreign"
	runtime := &recordingPreparationRuntime{response: catalogproto.PrepareTenantResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
	}}
	preparer, err := NewPreparer(runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preparer.Prepare(t.Context(), account, lease); !errors.Is(err, ErrPreparationConflict) {
		t.Fatalf("Prepare error = %v, want ErrPreparationConflict", err)
	}
}

func TestPreparerRejectsDrainingHealthAndStaleActivationProof(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	for name, runtime := range map[string]*recordingPreparationRuntime{
		"starting": {health: func() mountproto.RuntimeHealthResponse {
			health := exactRuntimeHealth()
			health.State = mountproto.RuntimeStateDegraded
			health.Busy = true
			health.Ready = false
			health.ReadinessPhase = mountproto.ReadinessPhaseStarting
			health.ReadinessStep = mountproto.ReadinessStepBroker
			health.BrokerPhase = mountproto.BrokerPhaseStarting
			return health
		}()},
		"draining": {health: func() mountproto.RuntimeHealthResponse {
			health := exactRuntimeHealth()
			health.Draining = true
			return health
		}()},
		"stale activation": {response: func() catalogproto.PrepareTenantResponse {
			tenantID, _ := account.TenantID()
			request := exactPreparationRequest(account, "stale-activation", lease)
			proof := exactPreparationProof(catalogproto.TenantID(tenantID), request, account.InstanceID)
			return catalogproto.PrepareTenantResponse{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
			}
		}()},
	} {
		t.Run(name, func(t *testing.T) {
			preparer, err := NewPreparer(runtime)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := preparer.Prepare(t.Context(), account, lease); !errors.Is(err, ErrPreparationConflict) {
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
	runtime.health = exactRuntimeHealth()
	runtime.health.ActivationGeneration = "activation-8"
	if err := preparer.Validate(t.Context(), account, lease, proof); !errors.Is(err, ErrPreparationConflict) {
		t.Fatalf("Validate error = %v, want ErrPreparationConflict", err)
	}
}

func preparationAccount(t *testing.T) Account {
	t.Helper()
	root := t.TempDir()
	return Account{
		InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
		BackingRoot: filepath.Join(root, "backing"),
	}
}

func exactPreparationRequest(
	account Account,
	activationGeneration string,
	lease PreparationLease,
) catalogproto.PrepareTenantRequest {
	tenantID, err := account.TenantID()
	if err != nil {
		panic(err)
	}
	critical, err := newCriticalReadinessPolicy(tenantID)
	if err != nil {
		panic(err)
	}
	return catalogproto.PrepareTenantRequest{
		Protocol: catalogproto.Version, Generation: account.Generation,
		Presentation: catalogproto.PresentationKindFileProvider, ActivationGeneration: activationGeneration,
		CriticalPolicyDigest: critical.Digest, CriticalObjects: critical.Objects,
		LeaseID: lease.ID, LeaseExpiresUnixNano: uint64(lease.ExpiresAt.UnixNano()),
	}
}

func exactPreparationProof(
	tenantID catalogproto.TenantID,
	request catalogproto.PrepareTenantRequest,
	instanceID string,
) catalogproto.TenantPreparationProof {
	domainID, err := catalogproto.DeriveDomainID(OwnerID, catalogproto.PresentationInstanceID(instanceID))
	if err != nil {
		panic(err)
	}
	proof := catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: tenantID, Generation: request.Generation,
			Requested: 11, Desired: 11, Observed: 11, Verified: 11, Applied: 11,
		},
		SourceAuthority:   catalogproto.SourceAuthorityID(ClaudeAuthorityID),
		SourceRevision:    17,
		CatalogRevision:   11,
		ChangeID:          "11111111111111111111111111111111",
		OperationID:       "22222222222222222222222222222222",
		SourcePublication: "55555555555555555555555555555555",
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider,
			FileProvider: &catalogproto.FileProviderPresentationProof{
				TenantID: tenantID, DomainID: domainID, Generation: request.Generation,
				PublicPath:             filepath.Join("/Users/test/Library/CloudStorage", string(domainID)),
				ActivationGeneration:   request.ActivationGeneration,
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
		PolicyDigest: request.CriticalPolicyDigest, CatalogHead: proof.CatalogRevision,
		SourceRevision: proof.SourceRevision, TenantGeneration: proof.Catalog.Generation,
		DomainID: domainID, PresentationInstanceID: catalogproto.PresentationInstanceID(instanceID),
		RootID: proof.Presentation.FileProvider.RootID, ActivationGeneration: request.ActivationGeneration,
		Objects: objects,
	}
	digest, ok := criticalReadinessResolutionDigest(proof)
	if !ok {
		panic("test preparation proof has no critical resolution digest")
	}
	proof.CriticalReadiness.ResolutionDigest = digest
	proof.CriticalReadiness.Lease = catalogproto.FileProviderLeaseReceipt{
		LeaseID: request.LeaseID, TenantID: tenantID, DomainID: domainID,
		Generation: request.Generation, RootID: proof.Presentation.FileProvider.RootID,
		PresentationInstanceID: catalogproto.PresentationInstanceID(instanceID),
		State:                  catalogproto.FileProviderLeaseStateProvisional,
		PolicyDigest:           request.CriticalPolicyDigest, ResolutionDigest: digest,
		CatalogHead: proof.CatalogRevision, SourceAuthority: proof.SourceAuthority,
		SourcePublication: proof.SourcePublication, SourceRevision: proof.SourceRevision,
		ActivationGeneration: request.ActivationGeneration,
		ExpiresUnixNano:      request.LeaseExpiresUnixNano,
	}
	return proof
}

func testPreparationLease() PreparationLease {
	return PreparationLease{
		ID:        "abababababababababababababababab",
		ExpiresAt: time.Unix(1_800_000_000, 123).UTC(),
	}
}

func exactRuntimeHealth() mountproto.RuntimeHealthResponse {
	return mountproto.RuntimeHealthResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		RuntimeBuild: version.String(), RuntimeProtocol: mountproto.RuntimeProtocolVersion,
		RuntimePID: 42, ProcessGeneration: "process-7", ActivationGeneration: "activation-7",
		State: mountproto.RuntimeStateHealthy, Ready: true,
		ReadinessPhase: mountproto.ReadinessPhaseReady, ReadinessStep: mountproto.ReadinessStepPublished,
		NativePhase: mountproto.NativePhaseDisabled, BrokerPhase: mountproto.BrokerPhaseLive,
	}
}
