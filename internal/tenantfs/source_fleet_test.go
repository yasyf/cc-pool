package tenantfs

import (
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestClaudeSourceFleetPublicationIsExactV1Topology(t *testing.T) {
	publication, expected, err := claudeSourceFleetPublication(testClaudePolicy())
	if err != nil {
		t.Fatal(err)
	}
	if publication.ExpectedGeneration != 0 ||
		publication.Generation != SourceAuthorityFleetGeneration ||
		len(publication.Declarations) != 1 || len(publication.Declarations[0].DriverConfig) == 0 {
		t.Fatalf("Claude source fleet publication = %+v", publication)
	}
	declaration := publication.Declarations[0]
	if declaration.Authority != ClaudeAuthorityID || declaration.DriverID != ClaudePhysicalDriverID {
		t.Fatalf("Claude source declaration = %+v", declaration)
	}
	if expected.Owner != SourceAuthorityFleetOwner || expected.Generation != SourceAuthorityFleetGeneration ||
		expected.AuthorityCount != 1 || expected.AuthoritiesDigest == ([32]byte{}) ||
		expected.DeclarationsDigest == ([32]byte{}) {
		t.Fatalf("Claude source fleet state = %+v", expected)
	}
	if SourceAuthorityFleetGeneration != causal.Generation(1) {
		t.Fatalf("source fleet generation = %d, want hard-cut v1 generation", SourceAuthorityFleetGeneration)
	}
}

func TestClaudeSourceFleetPublicationRequiresLocalController(t *testing.T) {
	if err := PublishClaudeSourceFleet(t.Context(), nil, testClaudePolicy()); err == nil {
		t.Fatal("PublishClaudeSourceFleet accepted a nil local controller")
	}
}
