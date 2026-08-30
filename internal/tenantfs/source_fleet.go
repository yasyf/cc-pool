package tenantfs

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/holder"
)

// ClaudeSourceFleetController is the local controller surface cc-pool's source
// fleet publication drives; *holder.LocalTenantController satisfies it.
type ClaudeSourceFleetController interface {
	DesiredSourceFleet(
		ctx context.Context,
	) (*catalog.DesiredSourceAuthorityFleetState, []catalog.SourceAuthorityDeclaration, error)
	PublishSourceFleet(
		ctx context.Context,
		publication holder.LocalSourceFleetPublication,
	) (catalog.DesiredSourceAuthorityFleetState, error)
}

// PublishClaudeSourceFleet publishes cc-pool's complete v1 topology directly
// through the fixed signed runtime's pinned local controller. It publishes
// nothing when the stored fleet already carries that exact topology, advances
// a diverged stored fleet from the generation it read, and retries once when
// a concurrent start publishes between its read and its publication.
func PublishClaudeSourceFleet(
	ctx context.Context,
	controller ClaudeSourceFleetController,
	policy ClaudeAuthorityPolicy,
) error {
	if controller == nil {
		return errors.New("tenantfs: local tenant controller is required")
	}
	err := publishClaudeSourceFleet(ctx, controller, policy)
	if errors.Is(err, catalog.ErrGenerationMismatch) {
		return publishClaudeSourceFleet(ctx, controller, policy)
	}
	return err
}

func publishClaudeSourceFleet(
	ctx context.Context,
	controller ClaudeSourceFleetController,
	policy ClaudeAuthorityPolicy,
) error {
	current, _, err := controller.DesiredSourceFleet(ctx)
	if err != nil {
		return err
	}
	var stored causal.Generation
	if current != nil {
		stored = current.Generation
	}
	publication, expected, err := claudeSourceFleetPublication(policy, stored)
	if err != nil {
		return err
	}
	if current != nil && current.Owner == expected.Owner &&
		current.AuthorityCount == expected.AuthorityCount &&
		current.AuthoritiesDigest == expected.AuthoritiesDigest &&
		current.DeclarationsDigest == expected.DeclarationsDigest {
		return nil
	}
	state, err := controller.PublishSourceFleet(ctx, publication)
	if err != nil {
		return err
	}
	if state != expected {
		return errors.New("tenantfs: published source fleet state differs from exact v1 topology")
	}
	return nil
}

func claudeSourceFleetPublication(
	policy ClaudeAuthorityPolicy,
	stored causal.Generation,
) (holder.LocalSourceFleetPublication, catalog.DesiredSourceAuthorityFleetState, error) {
	declaration, err := ClaudeSourceAuthorityDeclaration(policy)
	if err != nil {
		return holder.LocalSourceFleetPublication{}, catalog.DesiredSourceAuthorityFleetState{}, err
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(
		[]causal.SourceAuthorityID{declaration.Authority},
	)
	if err != nil {
		return holder.LocalSourceFleetPublication{}, catalog.DesiredSourceAuthorityFleetState{}, err
	}
	declarations := []catalog.SourceAuthorityDeclaration{declaration}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		return holder.LocalSourceFleetPublication{}, catalog.DesiredSourceAuthorityFleetState{}, err
	}
	publication := holder.LocalSourceFleetPublication{
		ExpectedGeneration: stored,
		Generation:         stored + 1,
		Declarations:       declarations,
	}
	expected := catalog.DesiredSourceAuthorityFleetState{
		Owner:              SourceAuthorityFleetOwner,
		Generation:         stored + 1,
		AuthorityCount:     1,
		AuthoritiesDigest:  authoritiesDigest,
		DeclarationsDigest: declarationsDigest,
	}
	return publication, expected, nil
}
