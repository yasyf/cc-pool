package overlay

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// plansDirectoryKey is the settings.json key Claude Code reads for the plans
// directory (absolute paths only, no ~ expansion).
const plansDirectoryKey = "plansDirectory"

// injectPlansDirectory returns the settings.json bytes to serve: base with
// plansDirectory=plansDir injected when base lacks the key, or base unchanged
// when it already sets one (a user value is never overridden). Injected output is
// key-sorted json.Marshal — deterministic bytes, load-bearing for catalog
// content revisions and File Provider content versions.
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

// stripInjectedPlansDirectory removes plansDirectory from committed IFF its value
// equals plansDir — the stateless inverse of injectPlansDirectory; any other user
// value is left untouched. An unparseable document errors (never clobber a base
// you cannot parse).
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

// StripInjectedPlansDirectory removes cc-pool's exact synthetic plans path before source persistence.
func StripInjectedPlansDirectory(committed []byte, plansDir string) ([]byte, error) {
	return stripInjectedPlansDirectory(committed, plansDir)
}

// InjectPlansDirectory adds cc-pool's canonical plans path when settings do not override it.
func InjectPlansDirectory(base []byte, plansDir string) ([]byte, error) {
	return injectPlansDirectory(base, plansDir)
}
