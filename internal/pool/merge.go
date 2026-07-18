package pool

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// MergeOutcome describes what mergeClaudeJSON did for an account dir.
type MergeOutcome string

const (
	// MergeApplied means base keys were merged in and the account file rewritten.
	MergeApplied MergeOutcome = "applied"
	// MergeUnchanged means the account file already held every shareable base key;
	// nothing was written.
	MergeUnchanged MergeOutcome = "unchanged"
	// MergeNoBase means no ~/.claude.json exists to propagate.
	MergeNoBase MergeOutcome = "no-base"
	// MergeRecreated means a missing account file was recreated as
	// base-minus-blacklist, minting a skeleton projects entry per base project
	// carrying only overlay.ClaudeJSONSharedProjectKeys.
	MergeRecreated MergeOutcome = "recreated"
	// MergeSkippedOverlay means the account's recorded overlay kind is not symlink;
	// the fuse arm serves its own merged view.
	MergeSkippedOverlay MergeOutcome = "skipped-overlay"
)

// mergeClaudeJSON propagates ~/.claude.json's shareable top-level keys (outside
// overlay.ClaudeJSONPrivateKeys, base wins) plus per-project
// overlay.ClaudeJSONSharedProjectKeys into an account's private .claude.json.
// A concurrent live session can rewrite from memory and clobber merged values;
// the next semantic-content event (or daemon-owned launch reconciliation) reapplies
// the shared projection.
func mergeClaudeJSON(prov fkoverlay.Provider, accountDir, srcPath string) (MergeOutcome, error) {
	// Writing into a live mirror lands in the wrong root.
	if overlay.Mounted(accountDir) {
		return "", fmt.Errorf("%s is a live mountpoint; refusing to merge through a mirror", accountDir)
	}

	base, err := os.ReadFile(srcPath) //nolint:gosec // G304: srcPath is the user's own ~/.claude.json resolved by cc-pool, not external input
	if os.IsNotExist(err) {
		return MergeNoBase, nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s: %w", srcPath, err)
	}

	dst := filepath.Join(prov.PrivateRoot(accountDir), ".claude.json")
	recreate := false
	private, err := os.ReadFile(dst) //nolint:gosec // G304: dst is the account's own .claude.json under the cc-pool-managed config dir
	if os.IsNotExist(err) {
		// A stranded copy in the fuse private backing dir is an interrupted
		// conversion; minting a fresh file here would win HealStrandedPrivate's
		// last-write-wins and discard the real identity, so point at doctor.
		stranded := filepath.Join(fkoverlay.FusePrivateRoot(accountDir), ".claude.json")
		if stranded != dst {
			if _, serr := os.Lstat(stranded); serr == nil {
				return "", fmt.Errorf("%s is missing but a copy is stranded at %s (interrupted overlay conversion); run `ccp doctor`", dst, stranded)
			} else if !os.IsNotExist(serr) {
				// An unprobeable stranded path must not fall through to
				// recreate — the same collision the guard prevents.
				return "", fmt.Errorf("stat %q: %w", stranded, serr)
			}
		}
		recreate = true
		private = []byte("{}")
	} else if err != nil {
		return "", fmt.Errorf("read %s: %w", dst, err)
	}

	// Stricter than seeding: the file may hold login identity, so an unparseable
	// file errors rather than being replaced.
	merged, changed, err := overlay.MergeClaudeJSON(private, base)
	if err != nil {
		return "", fmt.Errorf("merge %s into %s: %w", srcPath, dst, err)
	}
	if !changed && !recreate {
		return MergeUnchanged, nil
	}
	if err := overlay.WriteAtomic0600(dst, merged); err != nil {
		return "", fmt.Errorf("install merged config: %w", err)
	}
	if recreate {
		return MergeRecreated, nil
	}
	return MergeApplied, nil
}

// MergeBaseClaudeJSON applies the shareable-settings projection for an account,
// gating on the RECORDED overlay backend (non-symlink →
// MergeSkippedOverlay). The gate keys on the row, never on a resolved provider's
// backend, so no build variant can silently un-gate a fuse account.
func (m *Manager) MergeBaseClaudeJSON(a store.Account) (MergeOutcome, error) {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return "", fmt.Errorf("merge base settings into acct-%02d: parse stored backend: %w", a.ID, err)
	}
	if backend != fkoverlay.BackendSymlink {
		return MergeSkippedOverlay, nil
	}
	prov, err := m.overlayFor(fkoverlay.BackendSymlink)
	if err != nil {
		return "", fmt.Errorf("merge base settings into acct-%02d: resolve symlink provider: %w", a.ID, err)
	}
	out, err := mergeClaudeJSON(prov, a.ConfigDir, ClaudeJSONPath())
	if err != nil {
		return "", fmt.Errorf("merge base settings into acct-%02d: %w", a.ID, err)
	}
	return out, nil
}
