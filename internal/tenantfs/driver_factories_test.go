package tenantfs

import (
	"testing"

	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
)

func TestClaudeSourceAuthorityDeclarationAndDriverIdentityAreExact(t *testing.T) {
	policy := testClaudePolicy()
	declaration, err := ClaudeSourceAuthorityDeclaration(policy)
	if err != nil {
		t.Fatal(err)
	}
	if declaration.Authority != ClaudeAuthorityID ||
		declaration.DriverID != ClaudePhysicalDriverID ||
		declaration.DeclarationDigest == ([32]byte{}) {
		t.Fatalf("Claude declaration = %+v", declaration)
	}
	if _, err := NewClaudeDriverFactories(policy); err != nil {
		t.Fatal(err)
	}
	identity := sourceauthority.SourceTaskIdentity{
		Owner: SourceAuthorityFleetOwner, FleetGeneration: 7,
		Authority: causal.SourceAuthorityID(declaration.Authority), AuthorityGeneration: 7,
		DriverID: declaration.DriverID, DeclarationDigest: declaration.DeclarationDigest,
	}
	resolved, err := resolveClaudePhysicalPolicy(policy, declaration, identity)
	if err != nil || resolved == nil {
		t.Fatalf("resolve exact Claude driver = %v, %v", resolved, err)
	}
	identity.DriverID = "foreign"
	if _, err := resolveClaudePhysicalPolicy(policy, declaration, identity); err == nil {
		t.Fatal("Claude driver accepted a foreign DriverID")
	}
}
