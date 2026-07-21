package tenantfs

import (
	"encoding/json"
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
		len(declaration.DriverConfig) == 0 ||
		declaration.DeclarationDigest == ([32]byte{}) {
		t.Fatalf("Claude declaration = %+v", declaration)
	}
	var config claudePhysicalDriverConfig
	if err := json.Unmarshal(declaration.DriverConfig, &config); err != nil ||
		config.Schema != 1 || config.ClaudeDir != policy.ClaudeDir ||
		config.ClaudeJSONPath != policy.ClaudeJSONPath {
		t.Fatalf("Claude driver config = %+v, %v", config, err)
	}
	if _, err := NewClaudeDriverFactories(policy); err != nil {
		t.Fatal(err)
	}
	identity := sourceauthority.SourceTaskIdentity{
		Owner: SourceAuthorityFleetOwner, FleetGeneration: SourceAuthorityFleetGeneration,
		Authority:           causal.SourceAuthorityID(declaration.Authority),
		AuthorityGeneration: SourceAuthorityFleetGeneration,
		DriverID:            declaration.DriverID, DriverConfig: append([]byte(nil), declaration.DriverConfig...),
		DeclarationDigest: declaration.DeclarationDigest,
	}
	resolved, err := resolveClaudePhysicalPolicy(policy, declaration, identity)
	if err != nil || resolved == nil {
		t.Fatalf("resolve exact Claude driver = %v, %v", resolved, err)
	}
	identity.DriverID = "foreign"
	if _, err := resolveClaudePhysicalPolicy(policy, declaration, identity); err == nil {
		t.Fatal("Claude driver accepted a foreign DriverID")
	}
	identity.DriverID = declaration.DriverID
	identity.DriverConfig = []byte(`{"schema":1}`)
	if _, err := resolveClaudePhysicalPolicy(policy, declaration, identity); err == nil {
		t.Fatal("Claude driver accepted foreign configuration bytes")
	}
}
