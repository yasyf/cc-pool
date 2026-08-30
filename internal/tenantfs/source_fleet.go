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
// through the fixed signed runtime's pinned local controller, and publishes
// nothing when the stored fleet already carries that exact topology.
func PublishClaudeSourceFleet(
	ctx context.Context,
	controller ClaudeSourceFleetController,
	policy ClaudeAuthorityPolicy,
) error {
	if controller == nil {
		return errors.New("tenantfs: local tenant controller is required")
	}
	publication, expected, err := claudeSourceFleetPublication(policy)
	if err != nil {
		return err
	}
	current, _, err := controller.DesiredSourceFleet(ctx)
	if err != nil {
		return err
	}
	if current != nil && *current == expected {
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
		ExpectedGeneration: 0,
		Generation:         SourceAuthorityFleetGeneration,
		Declarations:       declarations,
	}
	expected := catalog.DesiredSourceAuthorityFleetState{
		Owner:              SourceAuthorityFleetOwner,
		Generation:         SourceAuthorityFleetGeneration,
		AuthorityCount:     1,
		AuthoritiesDigest:  authoritiesDigest,
		DeclarationsDigest: declarationsDigest,
	}
	return publication, expected, nil
}
