package tenantfs

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"slices"

	"github.com/yasyf/cc-pool/internal/overlay"
)

const claudeAuthorityDeclarationDomain = "cc-pool.claude-authority.declaration.v1"

// DeclarationDigest returns cc-pool's complete versioned Claude policy identity.
// Every semantic algorithm revision must advance its corresponding declaration
// field before release.
func (p ClaudeAuthorityPolicy) DeclarationDigest() ([32]byte, error) {
	if err := p.validate(); err != nil {
		return [32]byte{}, err
	}
	digest := sha256.New()
	writeDeclarationField(digest, claudeAuthorityDeclarationDomain)
	writeDeclarationField(digest, "authority", string(ClaudeAuthorityID))
	writeDeclarationField(digest, "product-config-revision", "1")
	writeDeclarationField(digest, "root-policy-revision", "1")
	writeDeclarationField(digest, "snapshot-policy-revision", "1")
	writeDeclarationField(digest, "delta-policy-revision", "1")
	writeDeclarationField(digest, "materializer-revision", "1")
	writeDeclarationField(digest, "mutation-policy-revision", "1")
	writeDeclarationField(digest, "claude-json-merge-revision", "1")
	writeDeclarationField(digest, "settings-injection-revision", "1")
	writeDeclarationField(digest, "claude-dir", p.ClaudeDir)
	writeDeclarationField(digest, "claude-json-path", p.ClaudeJSONPath)
	writeDeclarationField(digest, "canonical-root", string(canonicalClaudeJSONRoot))
	writeDeclarationField(digest, "shared-root", string(sharedClaudeRoot))
	writeDeclarationField(digest, "private-root-prefix", privateRootPrefix)
	writeDeclarationField(digest, "claude-json-name", claudeJSONFile)
	writeDeclarationField(digest, "settings-name", settingsFile)
	writeDeclarationField(digest, "affected-config-key", string(affectedClaudeConfig))
	writeDeclarationSet(digest, "excluded-entries", trueMapKeys(overlay.ExcludedEntries))
	writeDeclarationSet(digest, "shared-entries", trueMapKeys(overlay.SharedEntries))
	writeDeclarationSet(digest, "skip-entries", trueMapKeys(overlay.SkipEntries))
	writeDeclarationSet(digest, "skip-prefixes", overlay.SkipPrefixes)
	writeDeclarationSet(digest, "private-patterns", overlay.PrivatePatterns)
	writeDeclarationSet(digest, "private-source-prefixes", overlay.PrivateSourcePrefixes)
	writeDeclarationSet(digest, "private-staging-prefixes", overlay.PrivateStagingPrefixes)
	writeDeclarationSet(digest, "private-lock-prefixes", overlay.PrivateLockPrefixes)
	writeDeclarationSet(digest, "claude-json-private-keys", trueMapKeys(overlay.ClaudeJSONPrivateKeys))
	writeDeclarationSet(
		digest,
		"claude-json-shared-project-keys",
		trueMapKeys(overlay.ClaudeJSONSharedProjectKeys),
	)
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func writeDeclarationSet(target hash.Hash, name string, values []string) {
	ordered := append([]string(nil), values...)
	slices.Sort(ordered)
	writeDeclarationField(target, name)
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(len(ordered)))
	_, _ = target.Write(count[:])
	for _, value := range ordered {
		writeDeclarationField(target, value)
	}
}

func writeDeclarationField(target hash.Hash, values ...string) {
	for _, value := range values {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = target.Write(length[:])
		_, _ = target.Write([]byte(value))
	}
}

func trueMapKeys(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for key, enabled := range values {
		if enabled {
			result = append(result, key)
		}
	}
	return result
}
