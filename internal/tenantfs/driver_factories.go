package tenantfs

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/sourceauthority"
)

const (
	// SourceAuthorityFleetOwner is cc-pool's exact FuseKit topology owner.
	SourceAuthorityFleetOwner catalog.SourceAuthorityFleetOwnerID = "com.yasyf.cc-pool"
	// ClaudePhysicalDriverID is the durable driver identity for Claude projections.
	ClaudePhysicalDriverID = "com.yasyf.cc-pool.config"
)

// ClaudeSourceAuthorityDeclaration returns cc-pool's complete v1 source declaration.
func ClaudeSourceAuthorityDeclaration(
	policy ClaudeAuthorityPolicy,
) (catalog.SourceAuthorityDeclaration, error) {
	digest, err := policy.DeclarationDigest()
	if err != nil {
		return catalog.SourceAuthorityDeclaration{}, err
	}
	declaration := catalog.SourceAuthorityDeclaration{
		Authority: ClaudeAuthorityID, DriverID: ClaudePhysicalDriverID,
		DeclarationDigest: digest,
	}
	return declaration, nil
}

// NewClaudeDriverFactories returns the immutable holder registry for cc-pool.
func NewClaudeDriverFactories(policy ClaudeAuthorityPolicy) (holder.DriverFactories, error) {
	declaration, err := ClaudeSourceAuthorityDeclaration(policy)
	if err != nil {
		return holder.DriverFactories{}, err
	}
	return holder.NewDriverFactories(map[string]holder.DriverFactory{
		ClaudePhysicalDriverID: {
			Physical: func(
				_ context.Context,
				identity sourceauthority.SourceTaskIdentity,
			) (sourceauthority.AuthorityPolicy, error) {
				return resolveClaudePhysicalPolicy(policy, declaration, identity)
			},
		},
	})
}

func resolveClaudePhysicalPolicy(
	policy ClaudeAuthorityPolicy,
	declaration catalog.SourceAuthorityDeclaration,
	identity sourceauthority.SourceTaskIdentity,
) (sourceauthority.AuthorityPolicy, error) {
	if identity.Owner != SourceAuthorityFleetOwner ||
		identity.FleetGeneration == 0 ||
		identity.AuthorityGeneration != identity.FleetGeneration ||
		identity.Authority != causal.SourceAuthorityID(declaration.Authority) ||
		identity.DriverID != declaration.DriverID ||
		identity.DeclarationDigest != declaration.DeclarationDigest {
		return nil, errors.New("tenantfs: source task identity differs from compiled Claude driver")
	}
	return policy, nil
}
