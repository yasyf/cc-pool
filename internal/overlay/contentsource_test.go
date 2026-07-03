package overlay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/content"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

type csFixture struct {
	src      *PoolContentSource
	domain   string
	baseCJ   string // ~/.claude.json
	baseSet  string // ~/.claude/settings.json
	privCJ   string // <privateRoot>/.claude.json
	plansDir string // ~/.claude/plans
}

func newCSFixture(t *testing.T) csFixture {
	t.Helper()
	root := t.TempDir()
	claudeDir := filepath.Join(root, ".claude")
	if err := os.MkdirAll(claudeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	baseCJ := filepath.Join(root, ".claude.json")
	domain := filepath.Join(root, "acct-01")
	priv := fkoverlay.FusePrivateRoot(domain)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	return csFixture{
		src:      NewPoolContentSource(claudeDir, baseCJ),
		domain:   domain,
		baseCJ:   baseCJ,
		baseSet:  filepath.Join(claudeDir, "settings.json"),
		privCJ:   filepath.Join(priv, ".claude.json"),
		plansDir: filepath.Join(claudeDir, "plans"),
	}
}

func writeJSON(t *testing.T, path string, v map[string]any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", b, err)
	}
	return m
}

func TestPoolContentSourceManifest(t *testing.T) {
	f := newCSFixture(t)
	entries, err := f.src.Manifest(f.domain)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	byName := map[string]content.Entry{}
	for _, e := range entries {
		byName[e.Name] = e
	}
	if e := byName["plans"]; e.Kind != content.EntrySymlink || e.Target != f.plansDir {
		t.Errorf("plans entry = %+v, want symlink → %s", e, f.plansDir)
	}
	for _, name := range []string{"daemon", "ide", "backups"} {
		if e := byName[name]; e.Kind != content.EntryPrivate {
			t.Errorf("%s entry = %+v, want EntryPrivate", name, e)
		}
	}
	cj := byName[".claude.json"]
	if cj.Kind != content.EntrySynth || !cj.Private {
		t.Errorf(".claude.json entry = %+v, want private synth", cj)
	}
	if len(cj.Freshness) != 2 || cj.Freshness[0] != f.privCJ || cj.Freshness[1] != f.baseCJ {
		t.Errorf(".claude.json freshness = %v, want [%s %s]", cj.Freshness, f.privCJ, f.baseCJ)
	}
	set := byName["settings.json"]
	if set.Kind != content.EntrySynth || set.Private {
		t.Errorf("settings.json entry = %+v, want shared synth", set)
	}
	if len(set.Freshness) != 1 || set.Freshness[0] != f.baseSet {
		t.Errorf("settings.json freshness = %v, want [%s]", set.Freshness, f.baseSet)
	}
}

func TestPoolContentSourceReadSynthClaudeJSON(t *testing.T) {
	f := newCSFixture(t)
	writeJSON(t, f.privCJ, map[string]any{"oauthAccount": "acct", "foo": 1})
	writeJSON(t, f.baseCJ, map[string]any{"oauthAccount": "base", "bar": 2})
	got, err := f.src.ReadSynth(f.domain, ".claude.json")
	if err != nil {
		t.Fatalf("ReadSynth: %v", err)
	}
	m := readJSON(t, got)
	if m["oauthAccount"] != "acct" {
		t.Errorf("merged oauthAccount = %v, want the private value \"acct\" (base's blacklisted key must not cross)", m["oauthAccount"])
	}
	if m["foo"] != float64(1) {
		t.Errorf("merged foo = %v, want the private 1", m["foo"])
	}
	if m["bar"] != float64(2) {
		t.Errorf("merged bar = %v, want the base shareable 2", m["bar"])
	}
}

func TestPoolContentSourceReadSynthMissingPrivateErrors(t *testing.T) {
	f := newCSFixture(t)
	// Seeded fuse accounts always have a private .claude.json; never fabricate from base alone.
	writeJSON(t, f.baseCJ, map[string]any{"bar": 2})
	if _, err := f.src.ReadSynth(f.domain, ".claude.json"); err == nil {
		t.Fatal("ReadSynth(.claude.json) with no private file = nil error, want a failure")
	}
}

func TestPoolContentSourceReadSynthMissingBaseServesPrivate(t *testing.T) {
	f := newCSFixture(t)
	writeJSON(t, f.privCJ, map[string]any{"oauthAccount": "acct", "foo": 1})
	// Missing base (onboarding): serve the private copy verbatim, never EIO.
	got, err := f.src.ReadSynth(f.domain, ".claude.json")
	if err != nil {
		t.Fatalf("ReadSynth: %v", err)
	}
	if m := readJSON(t, got); m["foo"] != float64(1) {
		t.Errorf("served foo = %v, want the private 1", m["foo"])
	}
}

func TestPoolContentSourceReadSynthSettingsInjects(t *testing.T) {
	f := newCSFixture(t)
	writeJSON(t, f.baseSet, map[string]any{"theme": "dark"})
	got, err := f.src.ReadSynth(f.domain, "settings.json")
	if err != nil {
		t.Fatalf("ReadSynth: %v", err)
	}
	m := readJSON(t, got)
	if m["plansDirectory"] != f.plansDir {
		t.Errorf("served plansDirectory = %v, want %s injected", m["plansDirectory"], f.plansDir)
	}
	if m["theme"] != "dark" {
		t.Errorf("served theme = %v, want the base value preserved", m["theme"])
	}
}

func TestPoolContentSourceWriteThroughClaudeJSONSplits(t *testing.T) {
	f := newCSFixture(t)
	writeJSON(t, f.baseCJ, map[string]any{"oauthAccount": "base", "bar": 1})
	payload, _ := json.Marshal(map[string]any{"oauthAccount": "acct", "bar": 2, "newShared": 3})
	if err := f.src.WriteThrough(f.domain, ".claude.json", payload); err != nil {
		t.Fatalf("WriteThrough: %v", err)
	}
	b, err := os.ReadFile(f.baseCJ)
	if err != nil {
		t.Fatal(err)
	}
	m := readJSON(t, b)
	if m["oauthAccount"] != "base" {
		t.Errorf("base oauthAccount = %v, want \"base\" unchanged (blacklisted key must not cross back)", m["oauthAccount"])
	}
	if m["bar"] != float64(2) || m["newShared"] != float64(3) {
		t.Errorf("base after split = %v, want bar=2 newShared=3", m)
	}
}

func TestPoolContentSourceWriteThroughSettingsStrips(t *testing.T) {
	f := newCSFixture(t)
	// Base already carries the injected key, as if a served view was committed back.
	writeJSON(t, f.baseSet, map[string]any{"theme": "dark", "plansDirectory": f.plansDir})
	payload, _ := json.Marshal(map[string]any{"theme": "dark", "plansDirectory": f.plansDir})
	if err := f.src.WriteThrough(f.domain, "settings.json", payload); err != nil {
		t.Fatalf("WriteThrough: %v", err)
	}
	b, err := os.ReadFile(f.baseSet)
	if err != nil {
		t.Fatal(err)
	}
	if m := readJSON(t, b); m["plansDirectory"] != nil {
		t.Errorf("base settings still carries plansDirectory = %v, want it stripped", m["plansDirectory"])
	}
}

func TestPoolContentSourceWriteThroughMissingBaseIsNoop(t *testing.T) {
	f := newCSFixture(t)
	payload, _ := json.Marshal(map[string]any{"bar": 2})
	if err := f.src.WriteThrough(f.domain, ".claude.json", payload); err != nil {
		t.Fatalf("WriteThrough on missing base = %v, want a silent no-op", err)
	}
	if _, err := os.Stat(f.baseCJ); !os.IsNotExist(err) {
		t.Errorf("base ~/.claude.json was created (err=%v); cc-pool must not mint it", err)
	}
}

func TestPoolContentSourceClassify(t *testing.T) {
	f := newCSFixture(t)
	cases := map[string]content.EntryKind{
		".claude.json":              content.EntrySynth,
		"settings.json":             content.EntrySynth,
		"plans":                     content.EntrySymlink,
		"daemon":                    content.EntryPrivate,
		".credentials.json":         content.EntryPrivate,
		"mcp-needs-auth-cache.json": content.EntryPrivate,
		"remote-settings.json":      content.EntryPrivate,
		".claude.json.tmp.abcd":     content.EntryPrivate,
		// CARDINAL gap class: dot-anchored PrivateEntry misses these, but the
		// holder's bare-HasPrefix PrivatePrefixes claims them — they must never
		// be symlinked into base (the symlink would win over the private redirect).
		".credentials.json~":         content.EntryPrivate,
		".claude.json-old":           content.EntryPrivate,
		"remote-settings.json_bak":   content.EntryPrivate,
		"mcp-needs-auth-cache.json2": content.EntryPrivate,
		// Case variants: the default APFS base resolves names case-insensitively,
		// so these ARE plain claude's live credential file / backups dir.
		".Credentials.json": content.EntryPrivate,
		"Backups":           content.EntryPrivate,
		// Near-miss, genuinely different file: stays a shared carve-out.
		"mcp-needs-auth.json": content.EntrySymlink,
		// Bulk-I/O names are carve-out symlinks now, agreeing with Manifest.
		"history.jsonl": content.EntrySymlink,
		"projects":      content.EntrySymlink,
		"statsig":       content.EntrySymlink,
		// Silly-rename / AppleDouble / OS litter and the probe stay passthrough.
		"._sidecar":           "",
		".fuse_hidden0000abc": "",
		".nfs.20051234":       "",
		".DS_Store":           "",
		ProbeFileName:         "",
	}
	for name, want := range cases {
		if got := f.src.Classify(name); got != want {
			t.Errorf("Classify(%q) = %q, want %q", name, got, want)
		}
	}
}

// manifestByName indexes a manifest slice and records duplicate emissions so a
// name emitted twice (e.g. once forced, once carved) is caught.
func manifestByName(t *testing.T, entries []content.Entry) (map[string]content.Entry, map[string]int) {
	t.Helper()
	byName := map[string]content.Entry{}
	count := map[string]int{}
	for _, e := range entries {
		byName[e.Name] = e
		count[e.Name]++
	}
	return byName, count
}

func TestPoolContentSourceManifestCarveOut(t *testing.T) {
	f := newCSFixture(t)
	base := f.src.claudeDir

	// Bulk-I/O entries that MUST become live symlinks into base (the regression).
	sharedDirs := []string{"projects", "statsig", "todos", "session-env", "shell-snapshots"}
	sharedFiles := []string{"history.jsonl"}
	// CARDINAL negative: identity/credentials/excluded names that must NEVER be
	// symlinked into base — the pool must never see plain claude's identity.
	privateDirs := []string{"daemon"}
	privateFiles := []string{
		".credentials.json", "mcp-needs-auth-cache.json", ".claude.json.tmp.abcd", "remote-settings.json",
		// Gap-class family siblings: dot-anchored PrivateEntry misses these but the
		// holder's bare-HasPrefix PrivatePrefixes private-routes them; emitting them
		// as symlinks would put them in the forbidden both-symlinked-and-private
		// state, where the symlink wins and exposes plain claude's file.
		".credentials.json~", ".claude.json-old", "remote-settings.json_bak", "mcp-needs-auth-cache.json2",
		// Case variant: on the default case-insensitive APFS base this resolves to
		// plain claude's live file family.
		".Last-Update-Result.json",
	}
	// Silly-rename / AppleDouble / OS litter that must not appear in the manifest at all.
	litter := []string{".fuse_hidden0000abc", ".nfs.20051234", "._sidecar", ".DS_Store"}

	for _, n := range append(append([]string{}, sharedDirs...), privateDirs...) {
		if err := os.MkdirAll(filepath.Join(base, n), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	files := append(append(append([]string{}, sharedFiles...), privateFiles...), litter...)
	for _, n := range files {
		if err := os.WriteFile(filepath.Join(base, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// settings.json + .claude.json living inside base: must stay synth-only, never
	// duplicated as a symlink/passthrough carve-out.
	writeJSON(t, f.baseSet, map[string]any{"theme": "dark"})
	if err := os.WriteFile(filepath.Join(base, ".claude.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// plans exists physically in base: the forced SharedEntries emission and the
	// carve-out scan must dedup to exactly one entry (the manifest is a wire
	// contract; Tree consumers enumerate it). Forced-when-absent is pinned by
	// TestPoolContentSourceManifest, whose base is empty.
	if err := os.MkdirAll(f.plansDir, 0o700); err != nil {
		t.Fatal(err)
	}

	entries, err := f.src.Manifest(f.domain)
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	byName, count := manifestByName(t, entries)

	// Bulk I/O → live symlink into base, agreeing with Classify.
	for _, name := range append(append([]string{}, sharedDirs...), sharedFiles...) {
		e := byName[name]
		wantTarget := filepath.Join(base, name)
		if e.Kind != content.EntrySymlink || e.Target != wantTarget {
			t.Errorf("%s entry = %+v, want symlink → %s", name, e, wantTarget)
		}
		if got := f.src.Classify(name); got != content.EntrySymlink {
			t.Errorf("Classify(%q) = %q, want EntrySymlink (must agree with Manifest)", name, got)
		}
	}

	// plans: forced SharedEntries entry, emitted exactly once despite also being
	// present in base (the carve-out scan must not re-emit it).
	if e := byName["plans"]; e.Kind != content.EntrySymlink || e.Target != f.plansDir {
		t.Errorf("plans entry = %+v, want symlink → %s (forced SharedEntries)", e, f.plansDir)
	}
	if count["plans"] != 1 {
		t.Errorf("plans emitted %d times, want exactly 1 (forced + carved must dedup)", count["plans"])
	}

	// CARDINAL: excluded dir is a private empty dir; the identity/credentials
	// files are never emitted — and neither is EVER an EntrySymlink.
	if e := byName["daemon"]; e.Kind != content.EntryPrivate {
		t.Errorf("daemon entry = %+v, want EntryPrivate", e)
	}
	for _, name := range privateFiles {
		if e, ok := byName[name]; ok {
			t.Errorf("%s emitted as %+v; a private name must never appear in the manifest", name, e)
		}
	}
	for _, name := range append(append([]string{}, privateDirs...), privateFiles...) {
		if byName[name].Kind == content.EntrySymlink {
			t.Errorf("CARDINAL VIOLATION: %s emitted as EntrySymlink into base", name)
		}
		if got := f.src.Classify(name); got == content.EntrySymlink {
			t.Errorf("CARDINAL VIOLATION: Classify(%q) = EntrySymlink", name)
		}
	}

	// Litter: never emitted at all.
	for _, name := range litter {
		if e, ok := byName[name]; ok {
			t.Errorf("%s emitted as %+v; silly-rename/AppleDouble litter must be skipped", name, e)
		}
	}

	// The two synth documents stay synth-only, exactly once each — no duplicate
	// symlink/passthrough entry even though .claude.json/settings.json exist in base.
	for _, name := range []string{claudeJSONName, settingsName} {
		if e := byName[name]; e.Kind != content.EntrySynth {
			t.Errorf("%s entry = %+v, want EntrySynth", name, e)
		}
		if count[name] != 1 {
			t.Errorf("%s emitted %d times, want exactly 1 (synth-only, no carve-out duplicate)", name, count[name])
		}
	}
}

func TestPoolContentSourceManifestUnreadableBaseErrors(t *testing.T) {
	f := newCSFixture(t)
	// Base missing → Manifest must fail loud (the holder Build fails, the daemon heals);
	// it must never silently drop the carve-out snapshot.
	if err := os.RemoveAll(f.src.claudeDir); err != nil {
		t.Fatal(err)
	}
	if _, err := f.src.Manifest(f.domain); err == nil {
		t.Fatal("Manifest with missing base = nil error, want a failure (fail loud)")
	}
}

func TestPoolContentSourceHealthErrors(t *testing.T) {
	f := newCSFixture(t)
	if err := os.WriteFile(f.baseSet, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.src.ReadSynth(f.domain, "settings.json"); err != nil {
		t.Fatalf("ReadSynth should fall back to raw bytes, not error: %v", err)
	}
	if f.src.HealthErrors() == nil {
		t.Fatal("HealthErrors() = nil after a parse failure, want a recorded error")
	}
	writeJSON(t, f.baseSet, map[string]any{"theme": "dark"})
	if _, err := f.src.ReadSynth(f.domain, "settings.json"); err != nil {
		t.Fatalf("ReadSynth after fix: %v", err)
	}
	if err := f.src.HealthErrors(); err != nil {
		t.Errorf("HealthErrors() = %v after a successful re-read, want nil (cleared)", err)
	}
}
