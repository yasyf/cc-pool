package tenantfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/holder"
	"github.com/yasyf/fusekit/sourceauthority"
)

const (
	// SourceAuthorityFleetOwner is cc-pool's exact FuseKit topology owner.
	SourceAuthorityFleetOwner catalog.SourceAuthorityFleetOwnerID = "com.yasyf.cc-pool"
	// SourceAuthorityFleetGeneration is cc-pool's hard-cut v1 source topology.
	SourceAuthorityFleetGeneration causal.Generation = 1
	// ClaudePhysicalDriverID is the durable driver identity for Claude projections.
	ClaudePhysicalDriverID = "com.yasyf.cc-pool.config"
)

type claudePhysicalDriverConfig struct {
	Schema         uint16 `json:"schema"`
	ClaudeDir      string `json:"claude_dir"`
	ClaudeJSONPath string `json:"claude_json_path"`
}

// ClaudeSourceAuthorityDeclaration returns cc-pool's complete v1 source declaration.
func ClaudeSourceAuthorityDeclaration(
	policy ClaudeAuthorityPolicy,
) (catalog.SourceAuthorityDeclaration, error) {
	digest, err := policy.DeclarationDigest()
	if err != nil {
		return catalog.SourceAuthorityDeclaration{}, err
	}
	driverConfig, err := json.Marshal(claudePhysicalDriverConfig{
		Schema: 1, ClaudeDir: policy.ClaudeDir, ClaudeJSONPath: policy.ClaudeJSONPath,
	})
	if err != nil {
		return catalog.SourceAuthorityDeclaration{}, err
	}
	declaration := catalog.SourceAuthorityDeclaration{
		Authority: ClaudeAuthorityID, DriverID: ClaudePhysicalDriverID,
		DriverConfig: driverConfig, DeclarationDigest: digest,
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
		identity.FleetGeneration != SourceAuthorityFleetGeneration ||
		identity.AuthorityGeneration != identity.FleetGeneration ||
		identity.Authority != causal.SourceAuthorityID(declaration.Authority) ||
		identity.DriverID != declaration.DriverID ||
		!bytes.Equal(identity.DriverConfig, declaration.DriverConfig) ||
		identity.DeclarationDigest != declaration.DeclarationDigest {
		return nil, errors.New("tenantfs: source task identity differs from compiled Claude driver")
	}
	return policy, nil
}
