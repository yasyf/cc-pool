package pool

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// SeedOutcome describes what seedClaudeJSON did for an account dir.
type SeedOutcome string

const (
	// SeedCopied means ~/.claude.json was copied in with oauthAccount stripped.
	SeedCopied SeedOutcome = "copied"
	// SeedNoSource means no ~/.claude.json exists; claude onboards fresh.
	SeedNoSource SeedOutcome = "no-source"
	// SeedKeptExisting means the account already holds logged-in state (a prior add
	// logged in but was not finalized); left untouched.
	SeedKeptExisting SeedOutcome = "kept-existing"
)

// seedClaudeJSON seeds an account's private .claude.json from srcPath (plain
// claude's ~/.claude.json) verbatim except the top-level oauthAccount identity,
// which is stripped (`claude /login` writes the account's own). It strips ONLY
// overlay.OAuthAccountKey, not the full overlay.ClaudeJSONPrivateKeys blacklist
// that mergeClaudeJSON and the fuse view honor, so projects and userID carry
// over. Written to the provider's private root, not a fuse mount that may be
// down in a CLI process.
func seedClaudeJSON(prov fkoverlay.Provider, accountDir, srcPath string) (SeedOutcome, error) {
	dst := filepath.Join(prov.PrivateRoot(accountDir), ".claude.json")

	if existing, err := os.ReadFile(dst); err == nil { //nolint:gosec // G304: dst is the account's own .claude.json under the cc-pool-managed config dir
		var cur map[string]json.RawMessage
		if json.Unmarshal(existing, &cur) == nil {
			if _, ok := cur[overlay.OAuthAccountKey]; ok {
				return SeedKeptExisting, nil
			}
		}
		// Unparseable or a pre-login onboarding stub: overwrite below.
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read existing %s: %w", dst, err)
	}

	src, err := os.ReadFile(srcPath) //nolint:gosec // G304: srcPath is the user's own ~/.claude.json resolved by cc-pool
	if os.IsNotExist(err) {
		return SeedNoSource, nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", srcPath, err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(src, &top); err != nil {
		return "", fmt.Errorf("parse %s: %w", srcPath, err)
	}
	delete(top, overlay.OAuthAccountKey)
	out, err := json.Marshal(top)
	if err != nil {
		return "", fmt.Errorf("encode seeded config: %w", err)
	}

	if err := overlay.WriteAtomic0600(dst, out); err != nil {
		return "", fmt.Errorf("install seeded config: %w", err)
	}
	return SeedCopied, nil
}
