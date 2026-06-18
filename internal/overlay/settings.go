package overlay

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// plansDirectoryKey is the settings.json key Claude Code reads to decide where
// plan-mode plan files are written and reported. There is no env-var lever; this
// key (absolute paths only, no ~ expansion) is the sole way to steer a pooled
// session's plans at the canonical ~/.claude/plans. The fuse mirror injects it
// into the bytes it serves so a pooled `claude` writes and reports the shared
// path, without ever mutating the real base settings.json.
const plansDirectoryKey = "plansDirectory"

// injectPlansDirectory returns the settings.json bytes to serve through the
// mirror: base with plansDirectory set to plansDir when base lacks the key, or
// base UNCHANGED when it already carries any plansDirectory (a user-set value is
// never overridden). When injected, the result is json.Marshal of a map —
// key-sorted, hence deterministic bytes for identical inputs (load-bearing for
// the fuse merged view's Getattr/Read coherence). An unparseable or non-object
// base is an error: the mirror must never serve garbage in place of the user's
// settings.
func injectPlansDirectory(base []byte, plansDir string) (served []byte, err error) {
	top, err := parseObject(base, "settings.json")
	if err != nil {
		return nil, err
	}
	if _, ok := top[plansDirectoryKey]; ok {
		return base, nil
	}
	v, err := json.Marshal(plansDir)
	if err != nil {
		return nil, fmt.Errorf("encode %s value: %w", plansDirectoryKey, err)
	}
	top[plansDirectoryKey] = v
	served, err = json.Marshal(top)
	if err != nil {
		return nil, fmt.Errorf("encode served settings.json: %w", err)
	}
	return served, nil
}

// stripInjectedPlansDirectory returns new base bytes with plansDirectory removed
// IFF its value equals the value we inject (plansDir) — the stateless inverse of
// injectPlansDirectory. A user-set plansDirectory with any other value is left
// untouched, and the one harmless edge (a user who coincidentally set our exact
// path) is stripped, then re-injected identically on the next read. The result
// is json.Marshal of a map (key-sorted, deterministic). An unparseable committed
// document is an error: never clobber a base you cannot parse.
func stripInjectedPlansDirectory(committed []byte, plansDir string) (newBase []byte, err error) {
	top, err := parseObject(committed, "settings.json")
	if err != nil {
		return nil, err
	}
	cur, ok := top[plansDirectoryKey]
	if !ok {
		return committed, nil
	}
	want, err := json.Marshal(plansDir)
	if err != nil {
		return nil, fmt.Errorf("encode %s value: %w", plansDirectoryKey, err)
	}
	nv, err := normalizeValue(cur)
	if err != nil {
		return nil, fmt.Errorf("normalize %s value: %w", plansDirectoryKey, err)
	}
	if !bytes.Equal(nv, want) {
		return committed, nil
	}
	delete(top, plansDirectoryKey)
	newBase, err = json.Marshal(top)
	if err != nil {
		return nil, fmt.Errorf("encode stripped settings.json: %w", err)
	}
	return newBase, nil
}
