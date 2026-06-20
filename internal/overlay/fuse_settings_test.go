//go:build fuse && cgo && darwin

package overlay

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// newSettingsMirror builds a mirrorFS over a home-shaped temp tree — home/.claude
// as the mirrored root with home/.claude/settings.json as the base the view
// injects into — without mounting anything (the method-level pattern, cf.
// newClaudeJSONMirror). It returns the mirror, the home dir, and the injected
// plansDirectory value the view is wired to (filepath.Join(root, "plans")), so a
// test can assert the served bytes carry exactly that path. An empty base means
// "settings.json absent".
func newSettingsMirror(t *testing.T, base string) (fs *mirrorFS, home, plansDir string) {
	t.Helper()
	home = t.TempDir()
	root := filepath.Join(home, ".claude")
	for _, d := range []string{root, filepath.Join(home, "acct.private")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if base != "" {
		if err := os.WriteFile(filepath.Join(root, "settings.json"), []byte(base), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fs = newMirrorFS(root, filepath.Join(home, "acct.private"), filepath.Join(home, ".claude.json"))
	return fs, home, filepath.Join(root, "plans")
}

// commitSettings rehearses claude's atomic save of settings.json through the
// mirror: write a tmp file into the base dir (settings.json backs onto root, not
// the private dir), fs.Rename it onto /settings.json — the path that schedules
// the base strip write-through — then drain the off-handler worker so the caller
// can assert base deterministically.
func commitSettings(t *testing.T, fs *mirrorFS, home, payload string) {
	t.Helper()
	root := filepath.Join(home, ".claude")
	if err := os.WriteFile(filepath.Join(root, "settings.json.tmp.ab12cd34"), []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	if st := fs.Rename("/settings.json.tmp.ab12cd34", settingsJSONFusePath); st != 0 {
		t.Fatalf("rename commit = %d, want 0", st)
	}
	if !fs.settings.flushWithin(5 * time.Second) {
		t.Fatal("settings write-through did not drain within 5s")
	}
}

// readSettingsSnapshot opens /settings.json read-only through the mirror, returns
// the Getattr.Size of the synthetic handle and exactly the bytes one full read
// serves, and releases the handle. The two are returned together so the caller
// can assert the stat/read coherence litmus (Getattr.Size == len(Read)).
func readSettingsSnapshot(t *testing.T, fs *mirrorFS) (size int64, served []byte) {
	t.Helper()
	st, fh := fs.Open(settingsJSONFusePath, syscall.O_RDONLY)
	if st != 0 {
		t.Fatalf("open = %d, want 0", st)
	}
	if !settingsFh(fh) {
		t.Fatalf("fh = %d, want a settings synthetic handle (>= settingsFhBase)", fh)
	}
	defer func() {
		if st := fs.Release(settingsJSONFusePath, fh); st != 0 {
			t.Fatalf("release = %d, want 0", st)
		}
	}()
	var stat fuse.Stat_t
	if st := fs.Getattr(settingsJSONFusePath, &stat, fh); st != 0 {
		t.Fatalf("getattr(fh) = %d, want 0", st)
	}
	buf := make([]byte, stat.Size+64)
	n := fs.Read(settingsJSONFusePath, buf, 0, fh)
	if n < 0 {
		t.Fatalf("read = %d, want >= 0", n)
	}
	if eof := fs.Read(settingsJSONFusePath, buf, int64(n), fh); eof != 0 {
		t.Fatalf("read past end = %d, want 0 (EOF)", eof)
	}
	return stat.Size, buf[:n]
}

// TestMirrorSettingsInjectsPlansDirectoryStatLitmus is the settings stat-then-read
// litmus: a RDONLY open of /settings.json yields a synthetic handle whose
// Getattr.Size equals exactly the bytes Read returns (a size/read mismatch
// truncates the NFS client), and those bytes carry the injected plansDirectory
// pointing at the absolute <root>/plans the view is wired to — the path a pooled
// claude must write and report instead of its per-account $CONFIG_DIR/plans.
func TestMirrorSettingsInjectsPlansDirectoryStatLitmus(t *testing.T) {
	fs, _, plansDir := newSettingsMirror(t, `{"theme":"dark"}`)

	size, served := readSettingsSnapshot(t, fs)
	if int64(len(served)) != size {
		t.Fatalf("Getattr.Size = %d but Read returned %d bytes — stat/read incoherent", size, len(served))
	}

	got := raw(t, served)
	if _, ok := got["plansDirectory"]; !ok {
		t.Fatalf("served settings.json missing injected plansDirectory: %s", served)
	}
	if string(got["plansDirectory"]) != `"`+plansDir+`"` {
		t.Fatalf("served plansDirectory = %s, want the injected absolute %q", got["plansDirectory"], plansDir)
	}
	// The base's own key survives the injection unchanged.
	if string(got["theme"]) != `"dark"` {
		t.Fatalf("served theme = %s, want base's \"dark\" preserved", got["theme"])
	}

	// Path-based Getattr (no handle) reports the same served size.
	var pstat fuse.Stat_t
	if st := fs.Getattr(settingsJSONFusePath, &pstat, ^uint64(0)); st != 0 {
		t.Fatalf("getattr(path) = %d, want 0", st)
	}
	if pstat.Size != size {
		t.Fatalf("path Getattr.Size = %d, handle Getattr.Size = %d — must agree", pstat.Size, size)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorSettingsPreservesUserPlansDirectory pins the respect-user-override
// half: a base that already sets plansDirectory=/custom is served BYTE-FOR-BYTE
// unchanged — the view never overrides a user's value, never appends its own
// <root>/.claude/plans, and the served length equals the base length.
func TestMirrorSettingsPreservesUserPlansDirectory(t *testing.T) {
	const base = `{"plansDirectory":"/custom","theme":"dark"}`
	fs, _, _ := newSettingsMirror(t, base)

	size, served := readSettingsSnapshot(t, fs)
	if int64(len(served)) != size {
		t.Fatalf("Getattr.Size = %d but Read returned %d bytes — stat/read incoherent", size, len(served))
	}
	if !bytes.Equal(served, []byte(base)) {
		t.Fatalf("served settings.json = %q, want the user base unchanged %q", served, base)
	}
	if len(served) != len(base) {
		t.Fatalf("served length = %d, want base length %d (no injection over a user value)", len(served), len(base))
	}
	if bytes.Contains(served, []byte("/.claude/plans")) {
		t.Fatalf("served settings.json injected our plans path over the user's value: %s", served)
	}
	got := raw(t, served)
	if string(got["plansDirectory"]) != `"/custom"` {
		t.Fatalf("served plansDirectory = %s, want the user's \"/custom\"", got["plansDirectory"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorSettingsCommitStripsInjectedPlansDirectory: a claude-style tmp+rename
// commit of the served bytes (which carry the injected plansDirectory) writes
// through to base with plansDirectory STRIPPED — the real ~/.claude/settings.json
// stays pristine — while every other committed change persists.
func TestMirrorSettingsCommitStripsInjectedPlansDirectory(t *testing.T) {
	fs, home, plansDir := newSettingsMirror(t, `{"theme":"dark"}`)
	basePath := filepath.Join(home, ".claude", "settings.json")

	// Commit a payload shaped exactly like the served bytes claude would have
	// read: the injected plansDirectory plus a user-made edit (theme flip).
	committed := `{"plansDirectory":"` + plansDir + `","theme":"light"}`
	commitSettings(t, fs, home, committed)

	onDisk := mustReadFile(t, basePath)
	got := raw(t, onDisk)
	if _, ok := got["plansDirectory"]; ok {
		t.Fatalf("on-disk base still carries the injected plansDirectory: %s", onDisk)
	}
	if bytes.Contains(onDisk, []byte("plansDirectory")) {
		t.Fatalf("on-disk base mentions plansDirectory at all: %s", onDisk)
	}
	// The user's edit reached base.
	if string(got["theme"]) != `"light"` {
		t.Fatalf("on-disk base theme = %s, want the committed \"light\" persisted", got["theme"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorSettingsCommitKeepsUserPlansDirectory: when the user set their OWN
// plansDirectory=/custom, a commit must leave it on disk untouched — the strip is
// value-gated (it removes the key only when its value equals the value we inject),
// so a foreign value survives the write-through verbatim.
func TestMirrorSettingsCommitKeepsUserPlansDirectory(t *testing.T) {
	fs, home, _ := newSettingsMirror(t, `{"plansDirectory":"/custom","theme":"dark"}`)
	basePath := filepath.Join(home, ".claude", "settings.json")

	// The served bytes equal base (override preserved), so a commit re-writes the
	// same /custom plus a user edit.
	committed := `{"plansDirectory":"/custom","theme":"light"}`
	commitSettings(t, fs, home, committed)

	onDisk := mustReadFile(t, basePath)
	got := raw(t, onDisk)
	if string(got["plansDirectory"]) != `"/custom"` {
		t.Fatalf("on-disk base plansDirectory = %s, want the user's \"/custom\" untouched", got["plansDirectory"])
	}
	if string(got["theme"]) != `"light"` {
		t.Fatalf("on-disk base theme = %s, want the committed \"light\" persisted", got["theme"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestFuseSettingsBaseUnchangedThroughRealMount is the plain-claude safety proof
// at the real filesystem level: a fuse-t mirror over a temp ~/.claude serves the
// injected plansDirectory through the mount, yet a read + teardown leaves the
// on-disk base settings.json BYTE-IDENTICAL. It needs a real fuse-t mount (like
// TestFuseMirrorRoundTrip) and skips cleanly when one is unavailable.
func TestFuseSettingsBaseUnchangedThroughRealMount(t *testing.T) {
	base := t.TempDir()
	mnt := t.TempDir()
	const original = `{"theme":"dark"}`
	settingsPath := filepath.Join(base, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(original), 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}

	p := &FuseProvider{}
	if err := p.Setup(base, mnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	teardown := func() { _ = p.Teardown(base, mnt) }
	defer teardown()

	// Read through the mount: the served bytes carry the injected plansDirectory
	// pointing at the mounted base's <base>/plans.
	served, err := os.ReadFile(filepath.Join(mnt, "settings.json")) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
	if err != nil {
		t.Fatalf("read settings.json through mount: %v", err)
	}
	got := raw(t, served)
	wantPlans := `"` + filepath.Join(base, "plans") + `"`
	if string(got["plansDirectory"]) != wantPlans {
		t.Fatalf("served plansDirectory = %s, want the injected %s", got["plansDirectory"], wantPlans)
	}
	if string(got["theme"]) != `"dark"` {
		t.Fatalf("served theme = %s, want base's \"dark\"", got["theme"])
	}

	// The on-disk base must be byte-identical: the injection lives only in the
	// served bytes; a pure read must never mutate the real file.
	onDisk := mustReadFile(t, settingsPath)
	if !bytes.Equal(onDisk, []byte(original)) {
		t.Fatalf("base settings.json mutated by a read: got %q, want %q", onDisk, original)
	}

	// Teardown drains any scheduled strip; the base must STILL be byte-identical
	// (a read scheduled no write-through, and even a spurious strip is a no-op
	// because nothing injected was committed).
	teardown()
	onDisk = mustReadFile(t, settingsPath)
	if !bytes.Equal(onDisk, []byte(original)) {
		t.Fatalf("base settings.json mutated by read+teardown: got %q, want %q", onDisk, original)
	}
}
