package overlay

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/yasyf/fusekit/state"
)

// OAuthAccountKey is the top-level .claude.json key holding an account's
// login identity.
const OAuthAccountKey = "oauthAccount"

// OnboardingCompletedKey is the top-level .claude.json flag claude checks to
// skip the first-run onboarding wizard.
const OnboardingCompletedKey = "hasCompletedOnboarding"

// PluginUsageKey is Claude's account-local plugin telemetry. It must never
// propagate between the canonical config and pooled accounts.
const PluginUsageKey = "pluginUsage"

// ClaudeJSONPrivateKeys are the top-level .claude.json keys that never cross
// between base ~/.claude.json and an account's private copy in either
// direction (the ClaudeJSONSharedProjectKeys carve-out inside "projects"
// excepted); every other key propagates.
var ClaudeJSONPrivateKeys = map[string]bool{
	OAuthAccountKey:  true,
	"userID":         true,
	"anonymousId":    true,
	"projects":       true,
	"firstStartTime": true,
	"numStartups":    true,
	PluginUsageKey:   true,
}

// ClaudeJSONSharedProjectKeys are the keys inside each projects["<path>"]
// entry that propagate both directions — the "projects" carve-out from
// ClaudeJSONPrivateKeys. They are project/machine properties, not account:
// unshared, every account re-asks trust dialogs and local-scope mcp servers
// are invisible to pooled sessions.
var ClaudeJSONSharedProjectKeys = map[string]bool{
	"hasTrustDialogAccepted":                  true,
	"hasClaudeMdExternalIncludesApproved":     true,
	"hasClaudeMdExternalIncludesWarningShown": true,
	"mcpServers":             true,
	"enabledMcpjsonServers":  true,
	"disabledMcpjsonServers": true,
}

// MergeClaudeJSON overlays base's shareable top-level keys onto private and
// returns the merged document: base wins outside ClaudeJSONPrivateKeys,
// inside "projects" only ClaudeJSONSharedProjectKeys cross. changed gates
// callers' rewrites; base values are normalized before comparison so a
// pretty-printed base reports unchanged. Output is key-sorted json.Marshal —
// deterministic bytes, load-bearing for the fuse merged view's Getattr/Read
// coherence. Non-object input errors rather than replacing an unparseable file.
func MergeClaudeJSON(private, base []byte) (merged []byte, changed bool, err error) {
	if base == nil {
		return private, false, nil
	}
	priv, err := parseObject(private, "private claude.json")
	if err != nil {
		return nil, false, err
	}
	top, err := parseObject(base, "base claude.json")
	if err != nil {
		return nil, false, err
	}
	for k, v := range top {
		if ClaudeJSONPrivateKeys[k] {
			continue
		}
		nv, err := normalizeValue(v)
		if err != nil {
			return nil, false, fmt.Errorf("normalize base claude.json key %q: %w", k, err)
		}
		if cur, ok := priv[k]; ok && bytes.Equal(cur, nv) {
			continue
		}
		priv[k] = nv
		changed = true
	}
	projChanged, err := overlaySharedProjectKeys(priv, top)
	if err != nil {
		return nil, false, fmt.Errorf("merge shared project keys: %w", err)
	}
	changed = changed || projChanged
	merged, err = json.Marshal(priv)
	if err != nil {
		return nil, false, fmt.Errorf("encode merged claude.json: %w", err)
	}
	return merged, changed, nil
}

// SplitClaudeJSON returns base with payload's shareable top-level keys
// overlaid: ClaudeJSONPrivateKeys never cross, base-only keys are retained,
// absence from payload deletes nothing, and inside "projects" the
// ClaudeJSONSharedProjectKeys cross back. Non-object input errors rather than
// clobbering an unparseable base.
func SplitClaudeJSON(payload, base []byte) ([]byte, error) {
	top, err := parseObject(payload, "claude.json payload")
	if err != nil {
		return nil, err
	}
	out, err := parseObject(base, "base claude.json")
	if err != nil {
		return nil, err
	}
	for k, v := range top {
		if ClaudeJSONPrivateKeys[k] {
			continue
		}
		out[k] = v
	}
	if _, err := overlaySharedProjectKeys(out, top); err != nil {
		return nil, fmt.Errorf("split shared project keys: %w", err)
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode split claude.json: %w", err)
	}
	return b, nil
}

// overlaySharedProjectKeys copies src's ClaudeJSONSharedProjectKeys onto
// dst's per-project entries. Unchanged entries pass through unparsed: a
// gratuitous re-encode would defeat MergeClaudeJSON's changed gate and
// writeThroughBase's bytes.Equal short-circuit, bumping base's mtime every
// cycle.
func overlaySharedProjectKeys(dst, src map[string]json.RawMessage) (changed bool, err error) {
	srcRaw, ok := src["projects"]
	if !ok {
		return false, nil
	}
	srcProj, err := parseObject(srcRaw, "source projects")
	if err != nil {
		return false, err
	}
	dstProj := map[string]json.RawMessage{}
	if dstRaw, ok := dst["projects"]; ok {
		dstProj, err = parseObject(dstRaw, "destination projects")
		if err != nil {
			return false, err
		}
	}
	for path, srcEntryRaw := range srcProj {
		srcEntry, err := parseObject(srcEntryRaw, fmt.Sprintf("source project %q", path))
		if err != nil {
			return false, err
		}
		shared := map[string]json.RawMessage{}
		for k, v := range srcEntry {
			if !ClaudeJSONSharedProjectKeys[k] {
				continue
			}
			nv, err := normalizeValue(v)
			if err != nil {
				return false, fmt.Errorf("normalize shared key %q of project %q: %w", k, path, err)
			}
			shared[k] = nv
		}
		if len(shared) == 0 {
			continue
		}
		dstEntry := map[string]json.RawMessage{}
		if dstEntryRaw, ok := dstProj[path]; ok {
			dstEntry, err = parseObject(dstEntryRaw, fmt.Sprintf("destination project %q", path))
			if err != nil {
				return false, err
			}
		}
		entryChanged := false
		for k, nv := range shared {
			if cur, ok := dstEntry[k]; ok && bytes.Equal(cur, nv) {
				continue
			}
			dstEntry[k] = nv
			entryChanged = true
		}
		if !entryChanged {
			continue
		}
		b, err := json.Marshal(dstEntry)
		if err != nil {
			return false, fmt.Errorf("encode project %q: %w", path, err)
		}
		dstProj[path] = b
		changed = true
	}
	if !changed {
		return false, nil
	}
	b, err := json.Marshal(dstProj)
	if err != nil {
		return false, fmt.Errorf("encode projects: %w", err)
	}
	dst["projects"] = b
	return true, nil
}

// parseObject decodes b's top-level keys, rejecting non-objects:
// json.Unmarshal accepts a bare `null` and leaves the map nil.
func parseObject(b []byte, what string) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", what, err)
	}
	if m == nil {
		return nil, fmt.Errorf("parse %s: not a JSON object", what)
	}
	return m, nil
}

// normalizeValue re-encodes v to the exact bytes json.Marshal emits for an
// embedded RawMessage: compacted then HTML-escaped — json.Compact alone
// leaves <, >, & raw, breaking the equality probes for values carrying them.
func normalizeValue(v json.RawMessage) (json.RawMessage, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, v); err != nil {
		return nil, err
	}
	var escaped bytes.Buffer
	json.HTMLEscape(&escaped, compact.Bytes())
	return escaped.Bytes(), nil
}

// WriteAtomic0600 writes data to dst via temp+rename with mode 0600, creating
// dst's directory if missing, so a concurrent reader never sees a partial file.
func WriteAtomic0600(dst string, data []byte) error {
	return state.AtomicWrite(dst, data, 0o600)
}
