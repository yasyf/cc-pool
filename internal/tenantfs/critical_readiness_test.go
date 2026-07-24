package tenantfs

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestCriticalReadinessPolicyUsesStableSyntheticLogicalIDs(t *testing.T) {
	account := preparationAccount(t)
	tenantID, err := account.TenantID()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := newCriticalReadinessPolicy(tenantID)
	if err != nil {
		t.Fatal(err)
	}
	want := []catalog.CriticalObjectRequirement{
		{LogicalID: string(syntheticLogical(tenantID, criticalRoleClaudeJSON)), Role: criticalRoleClaudeJSON},
		{LogicalID: string(syntheticLogical(tenantID, criticalRoleSettings)), Role: criticalRoleSettings},
	}
	if !reflect.DeepEqual(policy.Objects, want) || policy.Digest == "" {
		t.Fatalf("critical policy = %+v digest %q, want %+v", policy.Objects, policy.Digest, want)
	}
}

func TestCriticalReadinessPolicyDigestSeparatesSubsetsAndSupersets(t *testing.T) {
	account := preparationAccount(t)
	tenantID, err := account.TenantID()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := newCriticalReadinessPolicy(tenantID)
	if err != nil {
		t.Fatal(err)
	}
	subset, err := criticalReadinessPolicyDigest(policy.Objects[:1])
	if err != nil {
		t.Fatal(err)
	}
	superset, err := criticalReadinessPolicyDigest([]catalog.CriticalObjectRequirement{
		policy.Objects[0],
		{LogicalID: string(syntheticLogical(tenantID, "plans")), Role: "plans"},
		policy.Objects[1],
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy.Digest == subset || policy.Digest == superset || subset == superset {
		t.Fatalf("critical policy digest aliased: exact=%s subset=%s superset=%s", policy.Digest, subset, superset)
	}
}

func TestPreparerRejectsCriticalReadinessDrift(t *testing.T) {
	account := preparationAccount(t)
	lease := testPreparationLease()
	tenantID, err := account.TenantID()
	if err != nil {
		t.Fatal(err)
	}
	request := exactPreparationRequest(account, lease)
	mutations := map[string]func(*catalogproto.TenantPreparationProof){
		"policy digest": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.PolicyDigest = strings.Repeat("f", 64)
		},
		"policy subset": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.Objects = proof.CriticalReadiness.Objects[:1]
		},
		"opaque object": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.Objects[0].ObjectID = "99999999999999999999999999999999"
		},
		"content size": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.Objects[0].Size++
		},
		"catalog head": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.CatalogHead++
		},
		"source publication": func(proof *catalogproto.TenantPreparationProof) {
			proof.SourcePublication = "66666666666666666666666666666666"
		},
		"lease identity": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.Lease.LeaseID = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
		},
		"lease expiry": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.Lease.ExpiresUnixNano++
		},
		"committed lease": func(proof *catalogproto.TenantPreparationProof) {
			proof.CriticalReadiness.Lease.State = catalogproto.FileProviderLeaseStateCommitted
			proof.CriticalReadiness.Lease.SessionID = "session-7"
			proof.CriticalReadiness.Lease.ProcessIdentity = "process-7"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			proof := exactPreparationProof(catalogproto.TenantID(tenantID), request, "activation-7", account.InstanceID)
			mutate(&proof)
			runtime := &recordingPreparationRuntime{proof: &proof}
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

func TestPreparerRejectsInvalidPreparationLease(t *testing.T) {
	account := preparationAccount(t)
	preparer, err := NewPreparer(&recordingPreparationRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	for name, lease := range map[string]PreparationLease{
		"malformed ID": {ID: "not-a-lease", ExpiresAt: testPreparationLease().ExpiresAt},
		"uppercase ID": {ID: "ABABABABABABABABABABABABABABABAB", ExpiresAt: testPreparationLease().ExpiresAt},
		"zero expiry":  {ID: testPreparationLease().ID},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := preparer.Prepare(t.Context(), account, lease); err == nil {
				t.Fatal("Prepare succeeded with invalid preparation lease")
			}
		})
	}
}
