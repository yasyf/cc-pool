package pool

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
)

// MergeOutcome describes what mergeClaudeJSON did for an account dir.
type MergeOutcome string

const (
	// MergeApplied means base keys were merged in and the account file rewritten.
	MergeApplied MergeOutcome = "applied"
	// MergeUnchanged means the account file already held every shareable base key;
	// nothing was written, so a live session's rewriter was never raced.
	MergeUnchanged MergeOutcome = "unchanged"
	// MergeNoBase means no ~/.claude.json exists; there is nothing to propagate.
	MergeNoBase MergeOutcome = "no-base"
	// MergeRecreated means the account file was missing and was recreated as
	// base-minus-blacklist — no onboarding wizard, no inherited identity. The
	// recreated file does mint a skeleton projects entry per base project
	// carrying only overlay.ClaudeJSONSharedProjectKeys (desired: trust, MCP
	// approvals, and local-scope MCP server definitions carry over, history and
	// session state never do).
	MergeRecreated MergeOutcome = "recreated"
	// MergeSkippedOverlay means the account's recorded overlay kind is not symlink;
	// the fuse arm owns its own merged view, so the launch merge stays out.
	MergeSkippedOverlay MergeOutcome = "skipped-overlay"
)

// mergeClaudeJSON propagates ~/.claude.json's shareable top-level keys
// (everything outside overlay.ClaudeJSONPrivateKeys, base wins) plus the
// per-project overlay.ClaudeJSONSharedProjectKeys into an account's private
// .claude.json at launch time. An unchanged merge skips the write entirely. The merge is only guaranteed visible to the session being
// launched: a concurrently live session on the same account rewrites the file
// from memory and later clobbers merged values — an accepted limitation, with
// no machinery against it. A launch merging mid-conversion is the same
// pre-existing accepted race as SyncOverlay re-laying links during a
// conversion; the daemon gates its own selects, CLI-only launches were always
// outside that fence.
func mergeClaudeJSON(prov overlay.Provider, accountDir, srcPath string) (MergeOutcome, error) {
	// A live mirror over a dir whose row says symlink is the same anomaly
	// convertToFuse refuses: writing "into" the mirror lands in the wrong root.
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
		// A copy stranded in the fuse private backing dir is the fingerprint
		// of an interrupted conversion. Minting a fresh file here would make
		// the freshly-written (empty) copy the newer one, so when
		// HealStrandedPrivate moves the stranded copy back its last-write-wins
		// resolution would keep this mint and discard the real stranded
		// identity. Point at doctor instead so the heal restores the stranded
		// copy intact.
		stranded := filepath.Join(overlay.FusePrivateRoot(accountDir), ".claude.json")
		if stranded != dst {
			if _, serr := os.Lstat(stranded); serr == nil {
				return "", fmt.Errorf("%s is missing but a copy is stranded at %s (interrupted overlay conversion); run `ccp doctor`", dst, stranded)
			} else if !os.IsNotExist(serr) {
				// An unprobeable stranded path must not fall through to
				// recreate: minting a file here is the exact collision the
				// guard exists to prevent.
				return "", fmt.Errorf("stat %q: %w", stranded, serr)
			}
		}
		recreate = true
		private = []byte("{}")
	} else if err != nil {
		return "", fmt.Errorf("read %s: %w", dst, err)
	}

	// Stricter than seeding: at launch the file may hold login identity, so an
	// unparseable file is never replaced (MergeClaudeJSON errors on it).
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

// MergeBaseClaudeJSON runs the launch-time shareable-settings merge for an
// account. It gates on the RECORDED overlay kind: any non-symlink kind is
// MergeSkippedOverlay, because the fuse arm serves its own live merged view —
// the gate keys on the row, never on a resolved provider's kind, so no build
// variant can silently un-gate a fuse account. The daemon's fuse fallback and
// `ccp migrate`'s row flip keep the recorded kind truthful.
func (m *Manager) MergeBaseClaudeJSON(a store.Account) (MergeOutcome, error) {
	if overlay.Kind(a.OverlayKind) != overlay.KindSymlink {
		return MergeSkippedOverlay, nil
	}
	out, err := mergeClaudeJSON(m.overlayFor(overlay.KindSymlink), a.ConfigDir, ClaudeJSONPath())
	if err != nil {
		return "", fmt.Errorf("merge base settings into acct-%02d: %w", a.ID, err)
	}
	return out, nil
}
