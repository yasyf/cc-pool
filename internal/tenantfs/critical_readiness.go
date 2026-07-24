package tenantfs

import (
	"encoding/hex"
	"math"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/holder"
)

type criticalReadinessPolicy struct {
	Digest  string
	Objects []catalog.CriticalObjectRequirement
}

func newCriticalReadinessPolicy(tenantID catalog.TenantID) (criticalReadinessPolicy, error) {
	objects := []catalog.CriticalObjectRequirement{
		{LogicalID: string(syntheticLogical(tenantID, criticalRoleClaudeJSON)), Role: criticalRoleClaudeJSON},
		{LogicalID: string(syntheticLogical(tenantID, criticalRoleSettings)), Role: criticalRoleSettings},
	}
	digest, err := criticalReadinessPolicyDigest(objects)
	if err != nil {
		return criticalReadinessPolicy{}, err
	}
	return criticalReadinessPolicy{Digest: digest, Objects: objects}, nil
}

func criticalReadinessPolicyDigest(objects []catalog.CriticalObjectRequirement) (string, error) {
	digest, err := catalog.CriticalObjectPolicyDigest(objects)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

func matchingCriticalReadiness(
	proof catalogproto.TenantPreparationProof,
	request holder.LocalPreparationRequest,
) bool {
	readiness := proof.CriticalReadiness
	fileProvider := proof.Presentation.FileProvider
	lease := catalogproto.FileProviderLeaseReceipt{}
	if readiness != nil {
		lease = readiness.Lease
	}
	policyDigest, err := criticalReadinessPolicyDigest(request.CriticalObjects)
	if err != nil || readiness == nil || fileProvider == nil || readiness.PolicyDigest != policyDigest ||
		readiness.CatalogHead != proof.CatalogRevision || readiness.SourceRevision != proof.SourceRevision ||
		readiness.TenantGeneration != proof.Catalog.Generation || readiness.DomainID != fileProvider.DomainID ||
		readiness.PresentationInstanceID != fileProvider.PresentationInstanceID || readiness.RootID != fileProvider.RootID ||
		readiness.ActivationGeneration != fileProvider.ActivationGeneration ||
		lease.LeaseID != request.LeaseID || lease.ExpiresUnixNano != uint64(request.LeaseExpiresAt.UnixNano()) ||
		lease.State != catalogproto.FileProviderLeaseStateProvisional || lease.SessionID != "" || lease.ProcessIdentity != "" ||
		lease.TenantID != proof.Catalog.Tenant || lease.DomainID != readiness.DomainID ||
		lease.Generation != readiness.TenantGeneration || lease.RootID != readiness.RootID ||
		lease.PresentationInstanceID != readiness.PresentationInstanceID || lease.PolicyDigest != readiness.PolicyDigest ||
		lease.ResolutionDigest != readiness.ResolutionDigest || lease.CatalogHead != readiness.CatalogHead ||
		lease.SourceAuthority != proof.SourceAuthority || lease.SourcePublication != proof.SourcePublication ||
		lease.SourceRevision != readiness.SourceRevision || lease.ActivationGeneration != readiness.ActivationGeneration ||
		len(readiness.Objects) != len(request.CriticalObjects) {
		return false
	}
	for index, object := range readiness.Objects {
		expected := request.CriticalObjects[index]
		if object.LogicalID != expected.LogicalID || object.Role != expected.Role {
			return false
		}
	}
	resolutionDigest, ok := criticalReadinessResolutionDigest(proof)
	return ok && resolutionDigest == readiness.ResolutionDigest
}

func criticalReadinessResolutionDigest(proof catalogproto.TenantPreparationProof) (string, bool) {
	readiness := proof.CriticalReadiness
	if readiness == nil {
		return "", false
	}
	publicationRaw, err := hex.DecodeString(string(proof.SourcePublication))
	if err != nil || len(publicationRaw) != len(causal.OperationID{}) {
		return "", false
	}
	var publication causal.OperationID
	copy(publication[:], publicationRaw)
	resolution := catalog.CriticalObjectResolution{
		Authority: causal.SourceAuthorityID(proof.SourceAuthority), Publication: publication,
		Tenant: catalog.TenantID(proof.Catalog.Tenant), Generation: catalog.Generation(proof.Catalog.Generation),
		Head: catalog.Revision(readiness.CatalogHead), Objects: make([]catalog.ResolvedCriticalObject, len(readiness.Objects)),
	}
	for index, object := range readiness.Objects {
		if object.Size > math.MaxInt64 {
			return "", false
		}
		objectID, err := catalog.ParseObjectID(string(object.ObjectID))
		if err != nil {
			return "", false
		}
		hashRaw, err := hex.DecodeString(object.Hash)
		if err != nil || len(hashRaw) != len(catalog.ContentHash{}) {
			return "", false
		}
		var hash catalog.ContentHash
		copy(hash[:], hashRaw)
		resolution.Objects[index] = catalog.ResolvedCriticalObject{
			LogicalID: object.LogicalID, Role: object.Role, ObjectID: objectID,
			ObjectRevision: catalog.Revision(object.ObjectRevision), ContentRevision: catalog.Revision(object.ContentRevision),
			Size: int64(object.Size), Hash: hash,
		}
	}
	digest, err := catalog.CriticalObjectResolutionDigest(resolution)
	if err != nil {
		return "", false
	}
	return hex.EncodeToString(digest[:]), true
}
