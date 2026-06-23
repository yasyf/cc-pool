package overlay

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/yasyf/fusekit/state"
)

// OAuthAccountKey is the top-level .claude.json key holding an account's
// per-account login identity. It is the one key pool's add-time seeding strips
// (seedClaudeJSON deliberately copies projects/userID for add-time continuity)
// and is always in ClaudeJSONPrivateKeys, so it never crosses between the base
// and an account in either merge direction.
const OAuthAccountKey = "oauthAccount"

// ClaudeJSONPrivateKeys are the top-level .claude.json keys that never
// propagate between the base ~/.claude.json and an account's private copy, in
// either direction. Every other key — including ones claude invents later —
// propagates automatically. Key names are best-effort: an absent key simply
// never matches. Contrast with seedClaudeJSON, which strips ONLY
// OAuthAccountKey at add time so a new account inherits projects/userID.
//
//   - oauthAccount:   per-account login identity, written by that account's
//     own `claude /login`.
//   - userID:         per-account telemetry identity derived at first start.
//   - anonymousId:    per-account anonymous analytics identity.
//   - projects:       per-account project state (history, allowed tools);
//     sharing it would commingle session state across accounts — except the
//     per-project trust/approval keys and local-scope MCP server definitions in
//     ClaudeJSONSharedProjectKeys, which overlaySharedProjectKeys carves out and
//     shares in both directions.
//   - firstStartTime: per-account startup record.
//   - numStartups:    per-account startup counter, bumped every launch.
var ClaudeJSONPrivateKeys = map[string]bool{
	OAuthAccountKey:  true,
	"userID":         true,
	"anonymousId":    true,
	"projects":       true,
	"firstStartTime": true,
	"numStartups":    true,
}

// ClaudeJSONSharedProjectKeys are the keys inside each projects["<path>"]
// object that DO propagate between the base ~/.claude.json and an account's
// private copy, in both directions — the per-project exception to the
// "projects" entry in ClaudeJSONPrivateKeys. They record trust/approval
// decisions, MCP server enablement, and the project's local-scope MCP server
// definitions, which are properties of the project (and the single user's
// machine), not of the account answering the prompt; without sharing them
// every pool account re-asks every dialog and a `claude mcp add --scope local`
// server is invisible to pooled sessions. Everything else inside a project
// entry (history, allowed tools, session state) stays private.
//
//   - mcpServers: local-scope per-project server definitions written by
//     `claude mcp add --scope local`. Unlike enabledMcpjsonServers (which only
//     records approval of .mcp.json servers, themselves shared via the project
//     tree), this carries the full server config, so sharing it is what gives a
//     pooled session the same project MCP servers as plain claude.
var ClaudeJSONSharedProjectKeys = map[string]bool{
	"hasTrustDialogAccepted":                  true,
	"hasClaudeMdExternalIncludesApproved":     true,
	"hasClaudeMdExternalIncludesWarningShown": true,
	"mcpServers":             true,
	"enabledMcpjsonServers":  true,
	"disabledMcpjsonServers": true,
}

// MergeClaudeJSON overlays base's shareable top-level keys onto private and
// returns the merged document. Base wins on every key not in
// ClaudeJSONPrivateKeys; private-only keys survive; base's blacklisted keys
// never appear — except inside "projects", where the per-project
// ClaudeJSONSharedProjectKeys cross too (base wins per key, entries minted for
// projects the account never opened); every other per-project key (history,
// allowed tools, session state) stays private. changed reports whether any
// key actually differed, so callers can skip rewriting an already-merged
// file. Base values are normalized to json.Marshal's encoding before both the
// comparison and storage, so a pretty-printed base (claude's own writer)
// reports unchanged once merged — the output bytes are unaffected,
// json.Marshal re-encodes RawMessage anyway. A nil base returns private
// verbatim. The output is json.Marshal of a map — key-sorted, hence
// deterministic bytes for identical inputs (load-bearing for the fuse merged
// view's Getattr/Read coherence). Unparseable or non-object private or base
// is an error; the caller must never replace a file it could not parse.
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

// SplitClaudeJSON returns new base bytes with payload's shareable top-level
// keys overlaid onto base: every key not in ClaudeJSONPrivateKeys is copied
// from payload, blacklisted keys are never copied (base's own oauthAccount
// and startup counters stay verbatim), base-only keys are retained, and no
// deletions propagate — a key absent from payload is left alone. Inside
// "projects" the per-project ClaudeJSONSharedProjectKeys cross back to base
// (payload wins per key, skeleton entries minted for base-unknown projects);
// payload's history/allowed tools/session state never do, and base's projects
// value keeps its original bytes when no shared key differed — load-bearing
// for writeThroughBase's whole-file bytes.Equal short-circuit. Unparseable or
// non-object payload or base is an error: never clobber a base you cannot
// parse.
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

// overlaySharedProjectKeys copies src's ClaudeJSONSharedProjectKeys from each
// projects["<path>"] entry onto dst's (src wins; the entry — and dst's
// projects object — are created when absent, but an empty projects object is
// never minted). No other per-project key is copied and no deletion
// propagates: src lacking "projects" entirely is a no-op. Values are
// normalized to json.Marshal's encoding before the equality probe, mirroring
// the top-level merge loop. dst is mutated only after the full walk succeeds
// and only when some shared key actually differed; otherwise dst["projects"]
// keeps its original RawMessage bytes — entry key order included — and within
// a rewritten projects object only changed entries are re-marshaled, so a dst
// entry src shares nothing into passes through unparsed (even a non-object
// one). Both no-rewrite guarantees are load-bearing: MergeClaudeJSON's
// changed contract gates the symlink launch rewrite, and SplitClaudeJSON
// feeds writeThroughBase's whole-file bytes.Equal short-circuit — a
// gratuitous re-encode would bump base's mtime every cycle and thrash every
// mount's merge cache. Errors: non-object projects on either side, a
// non-object src entry, or a non-object dst entry src shares into (entry
// errors name the project path).
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

// parseObject decodes b's top-level keys to raw values, rejecting any document
// that is not a JSON object: json.Unmarshal accepts a bare `null` and leaves
// the map nil, which would turn the merge loops into silent no-ops or nil-map
// panics.
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

// normalizeValue re-encodes one raw JSON value to the exact bytes json.Marshal
// emits for an embedded RawMessage: compacted, then HTML-escaped (Marshal's
// encoder compacts WITH escaping; json.Compact alone leaves <, >, & raw and
// would keep the comparison failing for values carrying them). Merged output
// values always hold this form, so base values must be normalized identically
// before the equality probe or a pretty-printed base makes every merge report
// changed. A Compact failure on bytes json.Unmarshal already accepted cannot
// happen short of a programmer error; it is returned, never silently passed
// through.
func normalizeValue(v json.RawMessage) (json.RawMessage, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, v); err != nil {
		return nil, err
	}
	var escaped bytes.Buffer
	json.HTMLEscape(&escaped, compact.Bytes())
	return escaped.Bytes(), nil
}

// WriteAtomic0600 writes data to dst via temp+rename in dst's directory, so a
// concurrent reader never sees a partial file. Creates the directory if
// missing. Thin 0600 wrapper over fusekit/state.AtomicWrite — the one place the
// temp+rename mirror lives now.
func WriteAtomic0600(dst string, data []byte) error {
	return state.AtomicWrite(dst, data, 0o600)
}
