package tenantfs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/holder"
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

func TestPublishClaudeSourceFleetRepublishesNothingOnRestart(t *testing.T) {
	controller := newCatalogSourceFleet(t)

	if err := PublishClaudeSourceFleet(t.Context(), controller, testClaudePolicy()); err != nil {
		t.Fatalf("first publication: %v", err)
	}
	controller.acknowledgeStagedFleetAsRuntimeStart(t)
	published, _, err := controller.DesiredSourceFleet(t.Context())
	if err != nil || published == nil {
		t.Fatalf("published fleet = %+v, %v", published, err)
	}

	if err := PublishClaudeSourceFleet(t.Context(), controller, testClaudePolicy()); err != nil {
		t.Fatalf("restart publication: %v", err)
	}
	if controller.publications != 1 {
		t.Fatalf("catalog publications = %d, want only the first", controller.publications)
	}
	current, _, err := controller.DesiredSourceFleet(t.Context())
	if err != nil || current == nil || *current != *published {
		t.Fatalf("fleet after restart = %+v, %v, want %+v", current, err, published)
	}
}

func TestPublishClaudeSourceFleetSurfacesADivergedStoredTopology(t *testing.T) {
	controller := newCatalogSourceFleet(t)
	stored := testClaudePolicy()
	stored.ClaudeDir = "/Users/test/.claude-v0"
	if err := PublishClaudeSourceFleet(t.Context(), controller, stored); err != nil {
		t.Fatalf("stored publication: %v", err)
	}
	controller.acknowledgeStagedFleetAsRuntimeStart(t)

	err := PublishClaudeSourceFleet(t.Context(), controller, testClaudePolicy())
	if !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("diverged publication = %v, want %v", err, catalog.ErrGenerationMismatch)
	}
	current, _, readErr := controller.DesiredSourceFleet(t.Context())
	if readErr != nil || current == nil {
		t.Fatalf("fleet after divergence = %+v, %v", current, readErr)
	}
	if want := claudeFleetState(t, stored, SourceAuthorityFleetGeneration); *current != want {
		t.Fatalf("fleet after divergence = %+v, want the stored topology left at %+v", *current, want)
	}
}

func TestPublishClaudeSourceFleetSettlesBehindARaceWinner(t *testing.T) {
	controller := newCatalogSourceFleet(t)
	if err := PublishClaudeSourceFleet(t.Context(), controller, testClaudePolicy()); err != nil {
		t.Fatalf("winning publication: %v", err)
	}
	controller.acknowledgeStagedFleetAsRuntimeStart(t)

	loser := &staleReadSourceFleet{catalogSourceFleet: controller}
	if err := PublishClaudeSourceFleet(t.Context(), loser, testClaudePolicy()); err != nil {
		t.Fatalf("losing publication: %v", err)
	}
	if loser.reads != 2 {
		t.Fatalf("desired fleet reads = %d, want the stale read and one re-read", loser.reads)
	}
	if controller.publications != 2 {
		t.Fatalf("catalog publications = %d, want the winner and the lost race only", controller.publications)
	}
	current, _, err := controller.DesiredSourceFleet(t.Context())
	if err != nil || current == nil {
		t.Fatalf("fleet after race = %+v, %v", current, err)
	}
	if want := claudeFleetState(t, testClaudePolicy(), SourceAuthorityFleetGeneration); *current != want {
		t.Fatalf("fleet after race = %+v, want %+v", *current, want)
	}
}

func claudeFleetState(
	t *testing.T,
	policy ClaudeAuthorityPolicy,
	generation causal.Generation,
) catalog.DesiredSourceAuthorityFleetState {
	t.Helper()
	declaration, err := ClaudeSourceAuthorityDeclaration(policy)
	if err != nil {
		t.Fatal(err)
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(
		[]causal.SourceAuthorityID{declaration.Authority},
	)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(
		[]catalog.SourceAuthorityDeclaration{declaration},
	)
	if err != nil {
		t.Fatal(err)
	}
	return catalog.DesiredSourceAuthorityFleetState{
		Owner: SourceAuthorityFleetOwner, Generation: generation, AuthorityCount: 1,
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	}
}

type staleReadSourceFleet struct {
	*catalogSourceFleet
	reads int
}

func (c *staleReadSourceFleet) DesiredSourceFleet(
	ctx context.Context,
) (*catalog.DesiredSourceAuthorityFleetState, []catalog.SourceAuthorityDeclaration, error) {
	c.reads++
	if c.reads == 1 {
		return nil, nil, nil
	}
	return c.catalogSourceFleet.DesiredSourceFleet(ctx)
}

type catalogSourceFleet struct {
	catalog      *catalog.Catalog
	publications int
}

func newCatalogSourceFleet(t *testing.T) *catalogSourceFleet {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return &catalogSourceFleet{catalog: store}
}

func (c *catalogSourceFleet) acknowledgeStagedFleetAsRuntimeStart(t *testing.T) {
	t.Helper()
	status, err := c.catalog.SourceAuthorityFleetHead(t.Context(), SourceAuthorityFleetOwner)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending == nil {
		t.Fatalf("fleet head = %+v, want a pending stage to acknowledge", status)
	}
	pending := status.Pending
	if _, err := c.catalog.AcknowledgeSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetAcknowledgement{
			Owner:              SourceAuthorityFleetOwner,
			ExpectedGeneration: pending.ExpectedGeneration,
			Generation:         pending.Generation,
			AuthorityCount:     pending.AuthorityCount,
			AuthoritiesDigest:  pending.AuthoritiesDigest,
			DeclarationsDigest: pending.DeclarationsDigest,
			StageDigest:        pending.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
}

func (c *catalogSourceFleet) DesiredSourceFleet(
	ctx context.Context,
) (*catalog.DesiredSourceAuthorityFleetState, []catalog.SourceAuthorityDeclaration, error) {
	page, err := c.catalog.DesiredSourceFleetPage(ctx, catalog.DesiredSourceFleetPageRequest{
		Owner: SourceAuthorityFleetOwner, Limit: catalog.TopologyPageLimit,
	})
	if errors.Is(err, catalog.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return &page.State, page.Declarations, nil
}

func (c *catalogSourceFleet) PublishSourceFleet(
	ctx context.Context,
	publication holder.LocalSourceFleetPublication,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	c.publications++
	return c.catalog.PublishDesiredSourceFleet(ctx, catalog.PublishDesiredSourceFleetRequest{
		Owner:              SourceAuthorityFleetOwner,
		ExpectedGeneration: publication.ExpectedGeneration,
		Generation:         publication.Generation,
		Declarations:       publication.Declarations,
	})
}
