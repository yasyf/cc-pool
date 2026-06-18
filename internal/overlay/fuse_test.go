//go:build fuse && cgo && darwin

package overlay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// TestFuseMountOptionsByteIdentical pins the cc-pool mount-option WIRING:
// buildMirrorConfig must hand fusekit a flat -o slice byte-identical to the one
// the pre-extraction inline Setup passed to cgofuse at v0.28.1 — volname,
// noattrcache (forced on darwin by MountOptions.Build), nobrowse, namedattr,
// then rwsize=1048576 carried via MountOptions.Extra. A drift here (a reordered
// flag, a dropped rwsize, a missing noattrcache) silently changes fuse-t mount
// behavior, so the exact slice is the contract. fusekit's options_test pins
// Build in isolation; this pins that cc-pool feeds it the right fields.
func TestFuseMountOptionsByteIdentical(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(t.TempDir(), "acct-01")
	cfg := buildMirrorConfig(base, dir)
	// buildMirrorConfig records the mirror in the process-global registry; this
	// test never mounts, so drop it so a later test's dir reuse can't collide.
	t.Cleanup(func() {
		mirrorMu.Lock()
		delete(mirrors, dir)
		mirrorMu.Unlock()
	})
	want := []string{
		"-o", "volname=cc-pool-acct-01",
		"-o", "noattrcache",
		"-o", "nobrowse",
		"-o", "namedattr",
		"-o", "rwsize=1048576",
	}
	if !slices.Equal(cfg.Options, want) {
		t.Fatalf("mount options = %q, want the byte-identical v0.28.1 string %q", cfg.Options, want)
	}
}

// TestFuseMirrorRoundTrip mounts a passthrough mirror via fuse-t and verifies
// reads and writes pass straight through to the backing dir (no copy-up). It
// requires fuse-t installed and may trip the one-time "Network Volumes" grant;
// it fails loudly so R-FUSE-T can be confirmed.
func TestFuseMirrorRoundTrip(t *testing.T) {
	base := t.TempDir()
	mnt := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "hello.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}

	p := &FuseProvider{}
	if err := p.Setup(base, mnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	defer p.Teardown(base, mnt)

	// Read through the mount.
	got, err := os.ReadFile(filepath.Join(mnt, "hello.txt"))
	if err != nil {
		t.Fatalf("read through mount: %v", err)
	}
	if string(got) != "hi" {
		t.Fatalf("read = %q, want hi", got)
	}

	// Write through the mount must land in base (shared, no copy-up).
	if err := os.WriteFile(filepath.Join(mnt, "written.txt"), []byte("pass"), 0o644); err != nil {
		t.Fatalf("write through mount: %v", err)
	}
	back, err := os.ReadFile(filepath.Join(base, "written.txt"))
	if err != nil {
		t.Fatalf("write did not pass through to base: %v", err)
	}
	if string(back) != "pass" {
		t.Fatalf("backing file = %q, want pass", back)
	}

	// A new entry created directly in base appears live through the mount.
	if err := os.Mkdir(filepath.Join(base, "newdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(mnt, "newdir")); err != nil {
		t.Fatalf("new base entry not visible through mount: %v", err)
	}

	// Writing .claude.json through the mount lands in the private backing dir,
	// never in base (per-account identity must not pollute the shared base).
	if err := os.WriteFile(filepath.Join(mnt, ".claude.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write .claude.json through mount: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, ".claude.json")); !os.IsNotExist(err) {
		t.Fatalf(".claude.json leaked into base")
	}
	if _, err := os.Stat(filepath.Join(FusePrivateRoot(mnt), ".claude.json")); err != nil {
		t.Fatalf(".claude.json not in private backing dir: %v", err)
	}

	// Merged read through the mount: the base SIBLING ~/.claude.json's
	// shareable keys overlay the account's private file while the private
	// identity wins. This pins fuse-t's read-open mode — read opens must
	// arrive O_RDONLY for the synthetic merged handle to engage (the biggest
	// fuse risk in the merged-view design).
	sibling := filepath.Join(filepath.Dir(base), ".claude.json")
	if err := os.WriteFile(sibling, []byte(`{"theme":"light","sharedKey":true,"oauthAccount":{"accountUuid":"base-own"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	privFile := filepath.Join(FusePrivateRoot(mnt), ".claude.json")
	if err := os.WriteFile(privFile, []byte(`{"theme":"dark","oauthAccount":{"accountUuid":"acct-own"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	merged, err := os.ReadFile(filepath.Join(mnt, ".claude.json"))
	if err != nil {
		t.Fatalf("merged read through mount: %v", err)
	}
	mgot := raw(t, merged)
	if string(mgot["theme"]) != `"light"` {
		t.Fatalf("merged theme = %s, want base's \"light\"", mgot["theme"])
	}
	if string(mgot["sharedKey"]) != `true` {
		t.Fatalf("base-only shareable key missing from merged read: %s", merged)
	}
	if string(mgot["oauthAccount"]) != `{"accountUuid":"acct-own"}` {
		t.Fatalf("merged oauthAccount = %s, want the account's own", mgot["oauthAccount"])
	}

	// Claude-style atomic save through the mount: WriteFile(tmp) + Rename.
	// The commit lands in the private file verbatim and its shareable keys
	// write through to the base sibling, which keeps its own oauthAccount.
	committed := `{"theme":"solarized","sharedKey":true,"oauthAccount":{"accountUuid":"acct-own"}}`
	tmp := filepath.Join(mnt, ".claude.json.tmp.cd34ef56")
	if err := os.WriteFile(tmp, []byte(committed), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(mnt, ".claude.json")); err != nil {
		t.Fatalf("claude-style commit through mount: %v", err)
	}
	pb, err := os.ReadFile(privFile)
	if err != nil {
		t.Fatalf("read private file after commit: %v", err)
	}
	if string(pb) != committed {
		t.Fatalf("private file after commit = %q, want the full payload %q", pb, committed)
	}
	// The commit's base write-through now runs off the fuse handler; drain it
	// before asserting the base sibling.
	mirrorMu.Lock()
	mfs := mirrors[mnt]
	mirrorMu.Unlock()
	if mfs == nil || !mfs.cj.flushWithin(5*time.Second) {
		t.Fatal("base write-through did not drain after the commit")
	}
	sb, err := os.ReadFile(sibling)
	if err != nil {
		t.Fatalf("read base sibling after commit: %v", err)
	}
	sgot := raw(t, sb)
	if string(sgot["theme"]) != `"solarized"` {
		t.Fatalf("base sibling theme = %s, want write-through \"solarized\"", sgot["theme"])
	}
	if string(sgot["oauthAccount"]) != `{"accountUuid":"base-own"}` {
		t.Fatalf("base sibling oauthAccount = %s, want its own untouched", sgot["oauthAccount"])
	}
}

// TestFuseMirrorCarvesBulkAsSymlinks pins the bulk-carve: shared top-level
// entries present through the mount as LIVE SYMLINKS into base, so the kernel
// resolves them outside the mount and claude's bulk transcript/history writes
// bypass fuse-t's NFS layer entirely. settings.json is the ONE shared file that
// is NOT carved — it is a virtual merged-view file (injected plansDirectory), so
// it presents as a regular file carrying the injected content, asserted here so
// the regression sentinel fails loudly if the carve-out exclusion is dropped. It
// needs a real fuse-t mount and skips (like TestFuseMirrorRoundTrip) when one is
// unavailable.
func TestFuseMirrorCarvesBulkAsSymlinks(t *testing.T) {
	base := t.TempDir()
	mnt := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o755); err != nil {
		t.Fatal(err)
	}
	// history is a shared top-level FILE that IS carved as a symlink (claude's
	// bulk transcript I/O); settings.json is the merged-view exception.
	if err := os.WriteFile(filepath.Join(base, "history"), []byte("h0"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte(`{"theme":"dark"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	p := &FuseProvider{}
	if err := p.Setup(base, mnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	defer p.Teardown(base, mnt)

	// A shared top-level DIR presents as a symlink to its absolute base path.
	fi, err := os.Lstat(filepath.Join(mnt, "projects"))
	if err != nil {
		t.Fatalf("lstat projects through mount: %v", err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("projects mode = %v, want a symlink (bulk must be carved out of the mount)", fi.Mode())
	}
	if got, err := os.Readlink(filepath.Join(mnt, "projects")); err != nil || got != filepath.Join(base, "projects") {
		t.Fatalf("readlink projects = %q (err %v), want %q", got, err, filepath.Join(base, "projects"))
	}

	// A shared top-level FILE presents as a symlink too.
	hfi, err := os.Lstat(filepath.Join(mnt, "history"))
	if err != nil {
		t.Fatalf("lstat history through mount: %v", err)
	}
	if hfi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("history mode = %v, want a symlink (a shared file must be carved out of the mount)", hfi.Mode())
	}
	if got, err := os.Readlink(filepath.Join(mnt, "history")); err != nil || got != filepath.Join(base, "history") {
		t.Fatalf("readlink history = %q (err %v), want %q", got, err, filepath.Join(base, "history"))
	}

	// settings.json is the merged-view exception: it is NOT a symlink but a
	// regular file whose served content carries the injected plansDirectory
	// (pointing at <base>/plans). Were it carved like the others, the injection
	// would never run — this is the regression sentinel for that exclusion.
	sfi, err := os.Lstat(filepath.Join(mnt, "settings.json"))
	if err != nil {
		t.Fatalf("lstat settings.json through mount: %v", err)
	}
	if sfi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("settings.json mode = %v, want a regular file (the merged-view exception, never a carved symlink)", sfi.Mode())
	}
	if !sfi.Mode().IsRegular() {
		t.Fatalf("settings.json mode = %v, want a regular file", sfi.Mode())
	}
	served, err := os.ReadFile(filepath.Join(mnt, "settings.json"))
	if err != nil {
		t.Fatalf("read settings.json through mount: %v", err)
	}
	sgot := raw(t, served)
	wantPlans := `"` + filepath.Join(base, "plans") + `"`
	if string(sgot["plansDirectory"]) != wantPlans {
		t.Fatalf("served settings.json plansDirectory = %s, want the injected %s — carve-out exclusion regressed", sgot["plansDirectory"], wantPlans)
	}
	if string(sgot["theme"]) != `"dark"` {
		t.Fatalf("served settings.json theme = %s, want base's \"dark\" preserved", sgot["theme"])
	}

	// A multi-MB write through the carved symlink lands directly in base — the
	// kernel resolves mnt/projects outside the mount, so the bytes never touch
	// fuse-t. Read them back from base to prove it.
	big := make([]byte, 4<<20)
	for i := range big {
		big[i] = byte(i*31 + 7)
	}
	if err := os.WriteFile(filepath.Join(mnt, "projects", "big.bin"), big, 0o644); err != nil {
		t.Fatalf("write multi-MB file through carved symlink: %v", err)
	}
	back, err := os.ReadFile(filepath.Join(base, "projects", "big.bin"))
	if err != nil {
		t.Fatalf("bulk write did not land in base: %v", err)
	}
	if !bytes.Equal(back, big) {
		t.Fatalf("base big.bin = %d bytes, want the %d written through the mount", len(back), len(big))
	}
}

// TestMirrorRealRedirectsLocalEntries pins the path-mapping table without
// needing a live mount: every PrivateEntry top component (and its subtree)
// must back onto privateRoot; everything else onto root.
func TestMirrorRealRedirectsLocalEntries(t *testing.T) {
	fs := newMirrorFS("/base", "/priv", "/.claude.json")
	cases := map[string]string{
		"/.claude.json":                      "/priv/.claude.json",
		"/.claude.json.tmp.ab12cd34":         "/priv/.claude.json.tmp.ab12cd34",
		"/.credentials.json":                 "/priv/.credentials.json",
		"/.credentials.json.lock":            "/priv/.credentials.json.lock",
		"/remote-settings.json":              "/priv/remote-settings.json",
		"/remote-settings.json.tmp.ab12cd34": "/priv/remote-settings.json.tmp.ab12cd34",
		"/backups":                           "/priv/backups",
		"/backups/x.bak":                     "/priv/backups/x.bak",
		"/daemon/roster.json":                "/priv/daemon/roster.json",
		"/ide/lock":                          "/priv/ide/lock",
		"/projects/p.json":                   "/base/projects/p.json",
		"/settings.json":                     "/base/settings.json",
		"/":                                  "/base",
	}
	for in, want := range cases {
		if got := fs.real(in); got != want {
			t.Errorf("real(%q) = %q, want %q", in, got, want)
		}
	}
}

// newClaudeJSONMirror builds a mirrorFS over a home-shaped temp tree —
// home/.claude as the mirrored root, home/.claude.json as the base sibling,
// home/acct.private as the private backing dir — without mounting anything
// (the existing method-level pattern). Empty private/base mean "file absent".
func newClaudeJSONMirror(t *testing.T, private, base string) (*mirrorFS, string) {
	t.Helper()
	home := t.TempDir()
	for _, d := range []string{filepath.Join(home, ".claude"), filepath.Join(home, "acct.private")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if base != "" {
		if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(base), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if private != "" {
		if err := os.WriteFile(filepath.Join(home, "acct.private", ".claude.json"), []byte(private), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fs := newMirrorFS(filepath.Join(home, ".claude"), filepath.Join(home, "acct.private"), filepath.Join(home, ".claude.json"))
	return fs, home
}

// commitClaudeJSON rehearses claude's atomic save through the mirror: write a
// tmp file into the private backing dir, then fs.Rename it onto /.claude.json
// — the path that triggers the base write-through.
func commitClaudeJSON(t *testing.T, fs *mirrorFS, home, payload string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(home, "acct.private", ".claude.json.tmp.ab12cd34"), []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	if st := fs.Rename("/.claude.json.tmp.ab12cd34", "/.claude.json"); st != 0 {
		t.Fatalf("rename commit = %d, want 0", st)
	}
	// Rename now schedules the base write-through off-handler; drain it so the
	// caller can assert base/healthErr deterministically.
	if !fs.cj.flushWithin(5 * time.Second) {
		t.Fatal("write-through did not drain within 5s")
	}
}

// TestMirrorClaudeJSONMergedReadStatLitmus is the stat-then-read litmus: a
// RDONLY open of /.claude.json yields a synthetic handle whose Getattr.Size
// equals exactly the bytes Read returns, and those bytes are the merge of
// base's shareable keys over the private file.
func TestMirrorClaudeJSONMergedReadStatLitmus(t *testing.T) {
	fs, _ := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_RDONLY)
	if st != 0 {
		t.Fatalf("open = %d, want 0", st)
	}
	if !syntheticFh(fh) {
		t.Fatalf("fh = %d, want a synthetic handle (>= 1<<62)", fh)
	}
	var stat fuse.Stat_t
	if st := fs.Getattr(claudeJSONFusePath, &stat, fh); st != 0 {
		t.Fatalf("getattr(fh) = %d, want 0", st)
	}
	if stat.Mode&0o777 != 0o600 {
		t.Errorf("mode = %o, want the private file's 600", stat.Mode&0o777)
	}
	buf := make([]byte, stat.Size+64)
	n := fs.Read(claudeJSONFusePath, buf, 0, fh)
	if n <= 0 {
		t.Fatalf("read = %d, want > 0", n)
	}
	if int64(n) != stat.Size {
		t.Fatalf("Getattr.Size = %d but Read returned %d bytes — stat/read incoherent", stat.Size, n)
	}
	if eof := fs.Read(claudeJSONFusePath, buf, int64(n), fh); eof != 0 {
		t.Fatalf("read past end = %d, want 0 (EOF)", eof)
	}
	got := raw(t, buf[:n])
	if string(got["theme"]) != `"light"` {
		t.Errorf("theme = %s, want base's \"light\"", got["theme"])
	}
	if string(got["claudeInChromeDefaultEnabled"]) != `true` {
		t.Errorf("base-only shareable key missing: %s", buf[:n])
	}
	if string(got["oauthAccount"]) != `{"accountUuid":"acct-own"}` {
		t.Errorf("oauthAccount = %s, want the account's own", got["oauthAccount"])
	}

	// Path-based Getattr (no handle) reports the same merged size.
	var pstat fuse.Stat_t
	if st := fs.Getattr(claudeJSONFusePath, &pstat, ^uint64(0)); st != 0 {
		t.Fatalf("getattr(path) = %d, want 0", st)
	}
	if pstat.Size != stat.Size {
		t.Fatalf("path Getattr.Size = %d, handle Getattr.Size = %d — must agree", pstat.Size, stat.Size)
	}
	if st := fs.Release(claudeJSONFusePath, fh); st != 0 {
		t.Fatalf("release = %d, want 0", st)
	}
}

// TestMirrorClaudeJSONIdentityReadIgnoresBaseIdentity pins the migrate
// interplay contract behind convertToFuse's post-mount identity verification
// (pool/convert.go): a readIdentity-shaped parse of the merged /.claude.json
// must see the PRIVATE file's oauthAccount even when the base sibling carries
// a different one — base identity must never leak through the merged read.
// Method-level on purpose: it runs without a fuse-t mount, unlike the pool
// package's live-mount interplay test.
func TestMirrorClaudeJSONIdentityReadIgnoresBaseIdentity(t *testing.T) {
	const (
		acctIdentity = `{"theme":"dark","oauthAccount":{"accountUuid":"u-1","emailAddress":"a@example.com"}}`
		foreignBase  = `{"theme":"light","sharedKey":true,"oauthAccount":{"accountUuid":"u-IMPOSTOR","emailAddress":"x@example.com"}}`
	)
	fs, _ := newClaudeJSONMirror(t, acctIdentity, foreignBase)
	merged := readMergedClaudeJSON(t, fs)
	if bytes.Contains(merged, []byte("u-IMPOSTOR")) {
		t.Fatalf("base identity leaked into the merged read:\n%s", merged)
	}
	got := raw(t, merged)
	if string(got["sharedKey"]) != `true` || string(got["theme"]) != `"light"` {
		t.Fatalf("merged view not live (base shareable keys missing): %s", merged)
	}
	// The exact parse pool's readIdentity performs on this view.
	var oauth struct {
		AccountUUID  string `json:"accountUuid"`
		EmailAddress string `json:"emailAddress"`
	}
	if err := json.Unmarshal(got["oauthAccount"], &oauth); err != nil {
		t.Fatalf("parse oauthAccount: %v", err)
	}
	if oauth.AccountUUID != "u-1" || oauth.EmailAddress != "a@example.com" {
		t.Fatalf("identity through merged read = %+v, want the private file's u-1/a@example.com", oauth)
	}
}

// TestMirrorClaudeJSONMissingPrivateENOENT pins onboarding semantics: with no
// private file the read path is plain ENOENT even when base has content — a
// view is never fabricated from base alone.
func TestMirrorClaudeJSONMissingPrivateENOENT(t *testing.T) {
	fs, _ := newClaudeJSONMirror(t, "", mergeBase)
	st, _ := fs.Open(claudeJSONFusePath, syscall.O_RDONLY)
	if st != -int(syscall.ENOENT) {
		t.Fatalf("open = %d, want -ENOENT", st)
	}
	var stat fuse.Stat_t
	if st := fs.Getattr(claudeJSONFusePath, &stat, ^uint64(0)); st != -int(syscall.ENOENT) {
		t.Fatalf("getattr = %d, want -ENOENT", st)
	}
}

// TestMirrorClaudeJSONRenameWriteThrough: a claude-style tmp+rename commit
// lands the full payload in the private file AND writes the shareable keys
// through to base, which keeps its own oauthAccount and counters verbatim.
func TestMirrorClaudeJSONRenameWriteThrough(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	committed := `{"theme":"solarized","newSetting":42,"oauthAccount":{"accountUuid":"acct-own"},"numStartups":8}`
	commitClaudeJSON(t, fs, home, committed)

	priv, err := os.ReadFile(filepath.Join(home, "acct.private", ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(priv) != committed {
		t.Fatalf("private file = %q, want the full committed payload %q (migrate depends on it)", priv, committed)
	}
	baseBytes, err := os.ReadFile(filepath.Join(home, ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	got := raw(t, baseBytes)
	if string(got["theme"]) != `"solarized"` || string(got["newSetting"]) != `42` {
		t.Errorf("shareable keys did not reach base: theme=%s newSetting=%s", got["theme"], got["newSetting"])
	}
	if string(got["oauthAccount"]) != `{"accountUuid":"base-own"}` {
		t.Errorf("base oauthAccount = %s, want its own untouched", got["oauthAccount"])
	}
	if string(got["numStartups"]) != `9999` {
		t.Errorf("base numStartups = %s, want its own 9999", got["numStartups"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONRenameWriteThroughSharedProjectKeys: a claude-style
// commit whose project entries mix an approval key with session history writes
// ONLY the approval key through to base — base's matching entry keeps its own
// history, a base-unknown project is minted with the approval key alone (no
// history at all), and the private file still holds the full committed payload
// verbatim (migrate depends on it).
func TestMirrorClaudeJSONRenameWriteThroughSharedProjectKeys(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	committed := `{"theme":"dark","oauthAccount":{"accountUuid":"acct-own"},"projects":{"/base":{"history":["acct-h1","acct-h2"],"hasClaudeMdExternalIncludesApproved":true},"/fresh":{"history":["acct-h3"],"hasClaudeMdExternalIncludesApproved":true}}}`
	commitClaudeJSON(t, fs, home, committed)

	priv := mustReadFile(t, filepath.Join(home, "acct.private", ".claude.json"))
	if string(priv) != committed {
		t.Fatalf("private file = %q, want the full committed payload %q (migrate depends on it)", priv, committed)
	}
	base := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	proj := raw(t, base["projects"])
	entry := raw(t, proj["/base"])
	if string(entry["hasClaudeMdExternalIncludesApproved"]) != `true` {
		t.Errorf("base /base approval key = %s, want true written through", entry["hasClaudeMdExternalIncludesApproved"])
	}
	if string(entry["history"]) != `["theirs"]` {
		t.Errorf("base /base history = %s, want its own [\"theirs\"] — account history must never cross", entry["history"])
	}
	if len(entry) != 2 {
		t.Errorf("base /base entry = %s, want exactly its own history + the approval key", proj["/base"])
	}
	fresh := raw(t, proj["/fresh"])
	if string(fresh["hasClaudeMdExternalIncludesApproved"]) != `true` {
		t.Errorf("minted /fresh approval key = %s, want true", fresh["hasClaudeMdExternalIncludesApproved"])
	}
	if h, ok := fresh["history"]; ok {
		t.Errorf("minted /fresh entry carries history %s — private session state leaked into base", h)
	}
	if len(fresh) != 1 {
		t.Errorf("minted /fresh entry = %s, want the approval key alone", proj["/fresh"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONWriteThroughLocalMcpServers: a commit that adds a
// project's local-scope mcpServers (what `claude mcp add --scope local` writes)
// pushes the full server definitions through to base while the account's
// session history stays private — the write-through half of plain-claude parity
// for per-project local servers.
func TestMirrorClaudeJSONWriteThroughLocalMcpServers(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	committed := `{"theme":"dark","oauthAccount":{"accountUuid":"acct-own"},"projects":{"/base":{"history":["acct-h1"],"mcpServers":{"XcodeBuildMCP":{"type":"stdio","command":"npx","args":["-y","xcodebuildmcp@latest","mcp"]}}}}}`
	commitClaudeJSON(t, fs, home, committed)

	base := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	proj := raw(t, base["projects"])
	entry := raw(t, proj["/base"])
	if got, want := string(entry["mcpServers"]), `{"XcodeBuildMCP":{"type":"stdio","command":"npx","args":["-y","xcodebuildmcp@latest","mcp"]}}`; got != want {
		t.Errorf("base /base mcpServers = %s, want the committed local server written through %s", got, want)
	}
	if string(entry["history"]) != `["theirs"]` {
		t.Errorf("base /base history = %s, want its own [\"theirs\"] — account history must never cross", entry["history"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONWriteThroughSkipsMissingBase: with no base ~/.claude.json
// a commit must not mint one — cc-pool must not pre-empt vanilla claude's own
// onboarding.
func TestMirrorClaudeJSONWriteThroughSkipsMissingBase(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, "")
	commitClaudeJSON(t, fs, home, `{"theme":"solarized"}`)
	if _, err := os.Lstat(filepath.Join(home, ".claude.json")); !os.IsNotExist(err) {
		t.Fatalf("base file was created (err %v); write-through must skip a missing base", err)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil (skip is not a failure)", err)
	}
}

// TestMirrorClaudeJSONWriteThroughErrStickyAndClears: a failing write-through
// (read-only base dir) must not fail the rename, goes sticky for Health, and
// clears on the next successful write-through.
func TestMirrorClaudeJSONWriteThroughErrStickyAndClears(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	if err := os.Chmod(home, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(home, 0o700) })

	commitClaudeJSON(t, fs, home, `{"theme":"solarized"}`) // fatals unless rename returns 0
	if err := fs.healthErr(); err == nil {
		t.Fatal("healthErr = nil, want the sticky write-through failure")
	}
	baseBytes, err := os.ReadFile(filepath.Join(home, ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(baseBytes) != mergeBase {
		t.Fatalf("base changed despite the failed write-through:\n%s", baseBytes)
	}

	if err := os.Chmod(home, 0o700); err != nil {
		t.Fatal(err)
	}
	commitClaudeJSON(t, fs, home, `{"theme":"zenburn"}`)
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil after a successful write-through", err)
	}
	got := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	if string(got["theme"]) != `"zenburn"` {
		t.Fatalf("base theme = %s, want \"zenburn\" after recovery", got["theme"])
	}
}

// TestMirrorClaudeJSONNoopWriteThroughSkipsRewrite: a commit whose shareable
// keys already match base must not rewrite the base file — rewriting identical
// bytes bumps base's mtime, which invalidates every mount's merge cache and
// widens the vanilla-claude last-writer window for nothing. Pinned via mtime:
// base is backdated after the first commit, so any rewrite by the second
// commit — which differs only in private state (history grows, numStartups
// bumps: exactly what every claude session commits) — would move ModTime
// forward. Base's project entry is reordered to non-json.Marshal key order
// between the commits, so a gratuitous projects re-encode (which would sort
// it) cannot hide behind byte-identical output: it would defeat the
// bytes.Equal short-circuit and move the mtime.
func TestMirrorClaudeJSONNoopWriteThroughSkipsRewrite(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	basePath := filepath.Join(home, ".claude.json")
	commitClaudeJSON(t, fs, home, `{"theme":"solarized","oauthAccount":{"accountUuid":"acct-own"},"numStartups":7,"projects":{"/base":{"history":["mine"],"hasTrustDialogAccepted":true}}}`)

	const (
		sorted    = `"/base":{"hasTrustDialogAccepted":true,"history":["theirs"]}`
		reordered = `"/base":{"history":["theirs"],"hasTrustDialogAccepted":true}`
	)
	canon := mustReadFile(t, basePath)
	if !bytes.Contains(canon, []byte(sorted)) {
		t.Fatalf("first commit did not write the trust key into base's project entry:\n%s", canon)
	}
	want := bytes.Replace(canon, []byte(sorted), []byte(reordered), 1)
	if err := os.WriteFile(basePath, want, 0o600); err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.Chtimes(basePath, old, old); err != nil {
		t.Fatal(err)
	}
	commitClaudeJSON(t, fs, home, `{"theme":"solarized","oauthAccount":{"accountUuid":"acct-own"},"numStartups":8,"projects":{"/base":{"history":["mine","more"],"hasTrustDialogAccepted":true}}}`)
	fi, err := os.Stat(basePath)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.ModTime().Equal(old) {
		t.Fatalf("no-op commit rewrote base: ModTime = %v, want untouched %v", fi.ModTime(), old)
	}
	if got := mustReadFile(t, basePath); !bytes.Equal(got, want) {
		t.Fatalf("no-op commit changed base bytes:\n%s\nwant:\n%s", got, want)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil (a skipped no-op cycle is a success)", err)
	}
}

// TestMirrorClaudeJSONNoopWriteThroughClearsWriteErr pins the success
// semantics of the skip: a commit whose shareable keys already match base is a
// SUCCESSFUL write-through cycle even though nothing was written, so it clears
// a sticky write error like any other success.
func TestMirrorClaudeJSONNoopWriteThroughClearsWriteErr(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	commitClaudeJSON(t, fs, home, `{"theme":"solarized","numStartups":7}`)
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil after the first commit", err)
	}
	if err := os.Chmod(home, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(home, 0o700) })
	commitClaudeJSON(t, fs, home, `{"theme":"zenburn","numStartups":7}`)
	if err := fs.healthErr(); err == nil {
		t.Fatal("healthErr = nil, want the sticky write-through failure")
	}
	if err := os.Chmod(home, 0o700); err != nil {
		t.Fatal(err)
	}
	// Shareable state back to exactly what base holds: the write-through skips
	// the rewrite yet must still clear the sticky error.
	commitClaudeJSON(t, fs, home, `{"theme":"solarized","numStartups":8}`)
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil — a skipped no-op write-through is a success", err)
	}
}

// TestMirrorClaudeJSONWriteHandleRelease: a write open of /.claude.json is a
// real fd (Getattr on it is the raw private file, not the merged view), and
// its Release runs the same write-through as a rename commit.
func TestMirrorClaudeJSONWriteHandleRelease(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_WRONLY|syscall.O_TRUNC)
	if st != 0 {
		t.Fatalf("open(WRONLY) = %d, want 0", st)
	}
	if syntheticFh(fh) {
		t.Fatalf("write open returned a synthetic handle %d, want a real fd", fh)
	}
	payload := `{"theme":"in-place","oauthAccount":{"accountUuid":"acct-own"}}`
	if n := fs.Write(claudeJSONFusePath, []byte(payload), 0, fh); n != len(payload) {
		t.Fatalf("write = %d, want %d", n, len(payload))
	}
	var stat fuse.Stat_t
	if st := fs.Getattr(claudeJSONFusePath, &stat, fh); st != 0 {
		t.Fatalf("getattr(write fh) = %d, want 0", st)
	}
	if stat.Size != int64(len(payload)) {
		t.Fatalf("write-handle Getattr.Size = %d, want the raw private size %d (no merged override)", stat.Size, len(payload))
	}
	if st := fs.Release(claudeJSONFusePath, fh); st != 0 {
		t.Fatalf("release = %d, want 0", st)
	}
	if !fs.cj.flushWithin(5 * time.Second) {
		t.Fatal("release write-through did not drain within 5s")
	}
	got := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	if string(got["theme"]) != `"in-place"` {
		t.Errorf("base theme = %s, want \"in-place\" after release write-through", got["theme"])
	}
	if string(got["oauthAccount"]) != `{"accountUuid":"base-own"}` {
		t.Errorf("base oauthAccount = %s, want its own untouched", got["oauthAccount"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONMtimeMax: the path-based Getattr's Mtim is the max of
// private and base — base-driven changes must bump mtime or the NFS client
// serves stale data pages.
func TestMirrorClaudeJSONMtimeMax(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	privPath := filepath.Join(home, "acct.private", ".claude.json")
	basePath := filepath.Join(home, ".claude.json")
	old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	newer := time.Now().Truncate(time.Second)

	for _, tc := range []struct {
		name         string
		privT, baseT time.Time
	}{
		{"base newer wins", old, newer},
		{"private newer wins", newer, old},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.Chtimes(privPath, tc.privT, tc.privT); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(basePath, tc.baseT, tc.baseT); err != nil {
				t.Fatal(err)
			}
			var stat fuse.Stat_t
			if st := fs.Getattr(claudeJSONFusePath, &stat, ^uint64(0)); st != 0 {
				t.Fatalf("getattr = %d, want 0", st)
			}
			if stat.Mtim.Sec != newer.Unix() {
				t.Fatalf("Mtim.Sec = %d, want max(private, base) = %d", stat.Mtim.Sec, newer.Unix())
			}
		})
	}
}

// TestMirrorClaudeJSONSyntheticHandleGuards: Truncate on a synthetic handle is
// refused without touching the private file, Fsync is a no-op success, and a
// released handle is EBADF — none may reach a bogus kernel fd.
func TestMirrorClaudeJSONSyntheticHandleGuards(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_RDONLY)
	if st != 0 {
		t.Fatalf("open = %d, want 0", st)
	}
	if st := fs.Truncate(claudeJSONFusePath, 0, fh); st != -int(syscall.EINVAL) {
		t.Fatalf("truncate(synthetic fh) = %d, want -EINVAL", st)
	}
	if got := mustReadFile(t, filepath.Join(home, "acct.private", ".claude.json")); string(got) != mergePrivate {
		t.Fatalf("refused truncate modified the private file:\n%s", got)
	}
	if st := fs.Write(claudeJSONFusePath, []byte(`{"evil":1}`), 0, fh); st != -int(syscall.EBADF) {
		t.Fatalf("write(synthetic fh) = %d, want -EBADF", st)
	}
	if got := mustReadFile(t, filepath.Join(home, "acct.private", ".claude.json")); string(got) != mergePrivate {
		t.Fatalf("refused write modified the private file:\n%s", got)
	}
	if st := fs.Fsync(claudeJSONFusePath, false, fh); st != 0 {
		t.Fatalf("fsync(synthetic fh) = %d, want 0", st)
	}
	if st := fs.Release(claudeJSONFusePath, fh); st != 0 {
		t.Fatalf("release = %d, want 0", st)
	}
	buf := make([]byte, 16)
	if st := fs.Read(claudeJSONFusePath, buf, 0, fh); st != -int(syscall.EBADF) {
		t.Fatalf("read after release = %d, want -EBADF", st)
	}
	var stat fuse.Stat_t
	if st := fs.Getattr(claudeJSONFusePath, &stat, fh); st != -int(syscall.EBADF) {
		t.Fatalf("getattr after release = %d, want -EBADF", st)
	}
}

// TestMirrorClaudeJSONCorruptBaseServesRawPrivate: an unparseable base falls
// back to the raw private bytes with a sticky error — the session must never
// see EIO on its state file.
func TestMirrorClaudeJSONCorruptBaseServesRawPrivate(t *testing.T) {
	fs, _ := newClaudeJSONMirror(t, mergePrivate, `{not json`)
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_RDONLY)
	if st != 0 {
		t.Fatalf("open = %d, want 0 (never EIO on corruption)", st)
	}
	buf := make([]byte, len(mergePrivate)+64)
	n := fs.Read(claudeJSONFusePath, buf, 0, fh)
	if string(buf[:n]) != mergePrivate {
		t.Fatalf("read = %q, want the raw private bytes", buf[:n])
	}
	if err := fs.healthErr(); err == nil {
		t.Fatal("healthErr = nil, want a sticky read error for the corrupt base")
	}
}

// TestMirrorClaudeJSONCorruptPrivateServesRaw: an unparseable PRIVATE file
// falls back to its own raw bytes with a sticky read error — claude's recovery
// must be able to read whatever is in its state file; never EIO.
func TestMirrorClaudeJSONCorruptPrivateServesRaw(t *testing.T) {
	const corrupt = `{not json`
	fs, _ := newClaudeJSONMirror(t, corrupt, mergeBase)
	if got := readMergedClaudeJSON(t, fs); string(got) != corrupt {
		t.Fatalf("read = %q, want the raw private bytes %q", got, corrupt)
	}
	if err := fs.healthErr(); err == nil {
		t.Fatal("healthErr = nil, want a sticky read error for the corrupt private file")
	}
}

// TestMirrorClaudeJSONReadErrClearsOnBaseFix: the read error is sticky only
// until the fault is gone — once the corrupt base is fixed, the next merged
// read alone must clear it; no claude commit (write-through) required.
func TestMirrorClaudeJSONReadErrClearsOnBaseFix(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, `{not json`)
	readMergedClaudeJSON(t, fs)
	if err := fs.healthErr(); err == nil {
		t.Fatal("healthErr = nil, want a read error for the corrupt base")
	}
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(mergeBase), 0o600); err != nil {
		t.Fatal(err)
	}
	got := raw(t, readMergedClaudeJSON(t, fs))
	if string(got["theme"]) != `"light"` {
		t.Fatalf("merged theme after fix = %s, want base's \"light\"", got["theme"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil once the fixed base merges cleanly", err)
	}
}

// TestMirrorClaudeJSONWriteErrSurvivesReadRecovery: read and write-through
// failures are independent domains — a merged read succeeding must not clear a
// write-through failure; only a successful write-through may.
func TestMirrorClaudeJSONWriteErrSurvivesReadRecovery(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, `{not json`)
	// Committing against the unparseable base fails the write-through (never
	// clobber a base you cannot parse) and records the write error.
	commitClaudeJSON(t, fs, home, `{"theme":"solarized"}`)
	if err := fs.healthErr(); err == nil {
		t.Fatal("healthErr = nil, want the write-through failure for the corrupt base")
	}
	// Fixing base lets the merged read succeed, which clears only the read
	// domain — the write-through failure must persist.
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(mergeBase), 0o600); err != nil {
		t.Fatal(err)
	}
	readMergedClaudeJSON(t, fs)
	err := fs.healthErr()
	if err == nil {
		t.Fatal("healthErr = nil after a successful read, want the write-through failure to persist")
	}
	if !strings.Contains(err.Error(), "write-through") {
		t.Fatalf("healthErr = %v, want the write-through failure", err)
	}
	// A successful write-through is the only thing that clears it.
	commitClaudeJSON(t, fs, home, `{"theme":"zenburn"}`)
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil after a successful write-through", err)
	}
}

// TestMirrorClaudeJSONCleanWriteHandleSkipsWriteThrough: a write-capable open
// that closes WITHOUT writing must not write through — right after a
// symlink→fuse conversion the private file's shareable keys can be staler than
// base, and a no-op open/close must not push them over it.
func TestMirrorClaudeJSONCleanWriteHandleSkipsWriteThrough(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_WRONLY)
	if st != 0 {
		t.Fatalf("open(WRONLY) = %d, want 0", st)
	}
	if st := fs.Release(claudeJSONFusePath, fh); st != 0 {
		t.Fatalf("release = %d, want 0", st)
	}
	if got := mustReadFile(t, filepath.Join(home, ".claude.json")); string(got) != mergeBase {
		t.Fatalf("base rewritten by a write handle that never wrote:\n%s", got)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONTruncateHandleMarksDirty: an fd Truncate counts as a
// mutation — a truncate-only commit must still write through on Release.
func TestMirrorClaudeJSONTruncateHandleMarksDirty(t *testing.T) {
	const valid = `{"theme":"truncated","oauthAccount":{"accountUuid":"acct-own"}}`
	fs, home := newClaudeJSONMirror(t, valid+`garbage-tail`, mergeBase)
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_WRONLY)
	if st != 0 {
		t.Fatalf("open(WRONLY) = %d, want 0", st)
	}
	if st := fs.Truncate(claudeJSONFusePath, int64(len(valid)), fh); st != 0 {
		t.Fatalf("truncate(fh) = %d, want 0", st)
	}
	if st := fs.Release(claudeJSONFusePath, fh); st != 0 {
		t.Fatalf("release = %d, want 0", st)
	}
	if !fs.cj.flushWithin(5 * time.Second) {
		t.Fatal("truncate-release write-through did not drain within 5s")
	}
	got := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	if string(got["theme"]) != `"truncated"` {
		t.Fatalf("base theme = %s, want \"truncated\" after a truncate-only release write-through", got["theme"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONFailedRenameSkipsWriteThrough: a rename that fails (the
// tmp file does not exist) returns the rename's own status and must not run
// the write-through — nothing was committed.
func TestMirrorClaudeJSONFailedRenameSkipsWriteThrough(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	if st := fs.Rename("/.claude.json.tmp.missing", claudeJSONFusePath); st != -int(syscall.ENOENT) {
		t.Fatalf("rename of a missing tmp = %d, want -ENOENT", st)
	}
	if got := mustReadFile(t, filepath.Join(home, ".claude.json")); string(got) != mergeBase {
		t.Fatalf("base rewritten by a failed rename:\n%s", got)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil after a failed rename", err)
	}
}

// TestMirrorClaudeJSONReleaseDoesNotBlockOnWriteThrough is the root-cause
// regression: a /.claude.json commit must return at fuse-op speed even while a
// base write-through is mid-cycle. Holding writeThroughMu parks the background
// worker; the Rename handler must still return promptly (it only schedules) and
// must NOT have touched base yet. Releasing the lock then lets the drained
// write-through reach base. This pins that the process-global base lock never
// sits on a fuse handler goroutine — the stall that wedged the mount's NFS
// server ("nfs server … not responding").
func TestMirrorClaudeJSONReleaseDoesNotBlockOnWriteThrough(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	committed := `{"theme":"unblocked","oauthAccount":{"accountUuid":"acct-own"}}`
	if err := os.WriteFile(filepath.Join(home, "acct.private", ".claude.json.tmp.ab12cd34"), []byte(committed), 0o600); err != nil {
		t.Fatal(err)
	}

	writeThroughMu.Lock() // freeze any write-through cycle mid-flight
	rename := make(chan int, 1)
	go func() { rename <- fs.Rename("/.claude.json.tmp.ab12cd34", "/.claude.json") }()
	select {
	case st := <-rename:
		if st != 0 {
			writeThroughMu.Unlock()
			t.Fatalf("rename commit = %d, want 0", st)
		}
	case <-time.After(2 * time.Second):
		writeThroughMu.Unlock()
		t.Fatal("Rename blocked while writeThroughMu was held — write-through must not run on the fuse handler")
	}
	// The worker is parked on the held lock, so base must still be untouched.
	if got := mustReadFile(t, filepath.Join(home, ".claude.json")); string(got) != mergeBase {
		writeThroughMu.Unlock()
		t.Fatalf("base written while the worker was blocked on the lock:\n%s", got)
	}
	writeThroughMu.Unlock()

	if !fs.cj.flushWithin(5 * time.Second) {
		t.Fatal("write-through did not drain after releasing the lock")
	}
	got := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	if string(got["theme"]) != `"unblocked"` {
		t.Fatalf("base theme = %s, want \"unblocked\" once the write-through drained", got["theme"])
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONCoalescedCommits fires many commits back-to-back without
// draining between them. However the schedules and the worker interleave, the
// drained base must reflect the LAST committed shareable keys — coalescing must
// never drop the final write.
func TestMirrorClaudeJSONCoalescedCommits(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	var last string
	for i := 0; i < 20; i++ {
		last = fmt.Sprintf("theme-%d", i)
		payload := fmt.Sprintf(`{"theme":%q,"oauthAccount":{"accountUuid":"acct-own"}}`, last)
		if err := os.WriteFile(filepath.Join(home, "acct.private", ".claude.json.tmp.ab12cd34"), []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if st := fs.Rename("/.claude.json.tmp.ab12cd34", "/.claude.json"); st != 0 {
			t.Fatalf("rename commit %d = %d, want 0", i, st)
		}
	}
	if !fs.cj.flushWithin(5 * time.Second) {
		t.Fatal("write-through did not drain")
	}
	got := raw(t, mustReadFile(t, filepath.Join(home, ".claude.json")))
	if string(got["theme"]) != `"`+last+`"` {
		t.Fatalf("base theme = %s, want the last commit's %q", got["theme"], last)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}

// TestMirrorClaudeJSONFlushWithinTimesOut: flushWithin must return false when a
// write-through cannot drain in time, so a bounded teardown never blocks
// forever on a stuck base write.
func TestMirrorClaudeJSONFlushWithinTimesOut(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	if err := os.WriteFile(filepath.Join(home, "acct.private", ".claude.json.tmp.ab12cd34"), []byte(`{"theme":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	writeThroughMu.Lock()
	if st := fs.Rename("/.claude.json.tmp.ab12cd34", "/.claude.json"); st != 0 {
		writeThroughMu.Unlock()
		t.Fatalf("rename = %d, want 0", st)
	}
	drained := fs.cj.flushWithin(100 * time.Millisecond)
	writeThroughMu.Unlock()
	if drained {
		t.Fatal("flushWithin reported drained while the write-through was blocked")
	}
	// Let the now-unblocked worker finish before the temp dir is cleaned up.
	fs.cj.flushWithin(5 * time.Second)
}

// TestFuseProviderHealthJoinsMirrorErrors pins the Health glue between the
// package mirrors registry and the mirror's sticky errors: a registered (but
// dead) mount's write-through failure must surface through FuseProvider.Health
// joined with the liveness error, and an unregistered dir reports only the
// liveness error — never another mount's sticky state. No live mount needed:
// the mirrorFS is registered by hand, the way buildMirrorConfig would.
func TestFuseProviderHealthJoinsMirrorErrors(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, `{not json`)
	base := filepath.Join(home, ".claude")
	// Liveness compares a base entry through the mountpoint; seed one so an
	// unmounted dir is deterministically "not live" (an empty base is vacuously
	// live).
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Committing over the unparseable base fails the write-through and leaves
	// the sticky error Health must surface.
	commitClaudeJSON(t, fs, home, `{"theme":"solarized"}`)
	if err := fs.healthErr(); err == nil {
		t.Fatal("precondition: healthErr = nil, want a sticky write-through failure")
	}

	accountDir := t.TempDir()
	mirrorMu.Lock()
	mirrors[accountDir] = fs
	mirrorMu.Unlock()
	t.Cleanup(func() {
		mirrorMu.Lock()
		delete(mirrors, accountDir)
		mirrorMu.Unlock()
	})

	p := &FuseProvider{}
	err := p.Health(base, accountDir)
	if err == nil {
		t.Fatal("Health = nil, want liveness and write-through failures joined")
	}
	if !strings.Contains(err.Error(), "not live") {
		t.Errorf("Health = %v, want the liveness error joined in", err)
	}
	if !strings.Contains(err.Error(), "write-through") {
		t.Errorf("Health = %v, want the mirror's sticky write-through failure joined in", err)
	}

	err = p.Health(base, t.TempDir())
	if err == nil {
		t.Fatal("Health(unregistered) = nil, want the liveness error")
	}
	if !strings.Contains(err.Error(), "not live") {
		t.Errorf("Health(unregistered) = %v, want the liveness error", err)
	}
	if strings.Contains(err.Error(), "write-through") {
		t.Errorf("Health(unregistered) = %v, leaked another mount's sticky write-through state", err)
	}
}

// readMergedClaudeJSON opens /.claude.json read-only through the mirror,
// returns what one full read serves (the merged document, or the raw private
// fallback on corruption), and releases the handle.
func readMergedClaudeJSON(t *testing.T, fs *mirrorFS) []byte {
	t.Helper()
	st, fh := fs.Open(claudeJSONFusePath, syscall.O_RDONLY)
	if st != 0 {
		t.Fatalf("open = %d, want 0 (never EIO on corruption)", st)
	}
	defer fs.Release(claudeJSONFusePath, fh)
	buf := make([]byte, 1<<16)
	n := fs.Read(claudeJSONFusePath, buf, 0, fh)
	if n < 0 {
		t.Fatalf("read = %d, want >= 0", n)
	}
	return buf[:n]
}

// mustReadFile reads a file or fails the test.
func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// readdirEntries runs Readdir at path and returns the names it fills, asserting
// every entry carries a nil stat — the mirror's contract: on fuse-t's FUSE2
// backend a filler stat never reaches the readdir reply, so nil is both cheap
// and correct, and the path-based Getattr stays authoritative for /.claude.json.
func readdirEntries(t *testing.T, fs *mirrorFS, path string) []string {
	t.Helper()
	var names []string
	st := fs.Readdir(path, func(name string, stat *fuse.Stat_t, ofst int64) bool {
		if stat != nil {
			t.Errorf("Readdir(%q) filled %q with a non-nil stat; entries must be nil-stat", path, name)
		}
		names = append(names, name)
		return true
	}, 0, ^uint64(0))
	if st != 0 {
		t.Fatalf("Readdir(%q) = %d, want 0", path, st)
	}
	return names
}

// mirrorTree builds a mirrorFS over a fresh home-shaped temp tree (base =
// home/.claude, private backing = home/acct.private, base sibling =
// home/.claude.json) and returns it with the base and private paths.
func mirrorTree(t *testing.T) (fs *mirrorFS, base, priv string) {
	t.Helper()
	home := t.TempDir()
	base = filepath.Join(home, ".claude")
	priv = filepath.Join(home, "acct.private")
	for _, d := range []string{base, priv} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return newMirrorFS(base, priv, filepath.Join(home, ".claude.json")), base, priv
}

func mustTouch(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

// TestMirrorReaddirRootMergesPrivateEntries pins the root listing contract:
// base entries appear, privateRoot's PrivateEntry names are merged in, a name
// present in both is listed once (the `seen` dedup), a non-private privateRoot
// stray is never listed, and the virtual .ccp-probe is shadowed by name.
func TestMirrorReaddirRootMergesPrivateEntries(t *testing.T) {
	fs, base, priv := mirrorTree(t)
	// base (mirrored root)
	mustTouch(t, filepath.Join(base, "settings.json"))
	mustMkdir(t, filepath.Join(base, "projects"))
	mustMkdir(t, filepath.Join(base, "daemon"))      // also in priv → dedup
	mustTouch(t, filepath.Join(base, ProbeFileName)) // shadowed virtual probe → never listed
	// private backing
	mustTouch(t, filepath.Join(priv, ".claude.json")) // PrivateEntry → merged in
	mustMkdir(t, filepath.Join(priv, "daemon"))       // collides with base → one entry
	mustTouch(t, filepath.Join(priv, "stray.txt"))    // not a PrivateEntry → excluded

	got := readdirEntries(t, fs, "/")

	for _, w := range []string{".", "..", "settings.json", "projects", "daemon", ".claude.json"} {
		if !slices.Contains(got, w) {
			t.Errorf("Readdir(/) missing %q; got %v", w, got)
		}
	}
	for _, w := range []string{ProbeFileName, "stray.txt"} {
		if slices.Contains(got, w) {
			t.Errorf("Readdir(/) should not list %q; got %v", w, got)
		}
	}
	if n := slices.Index(got, "daemon"); n < 0 || slices.Contains(got[n+1:], "daemon") {
		t.Errorf("daemon must be listed exactly once (seen-dedup); got %v", got)
	}
}

// TestMirrorReaddirSubdirOmitsPrivateMerge pins the path=="/" guard: the
// privateRoot merge happens only at the root, never inside a subdirectory.
func TestMirrorReaddirSubdirOmitsPrivateMerge(t *testing.T) {
	fs, base, priv := mirrorTree(t)
	mustMkdir(t, filepath.Join(base, "projects"))
	mustTouch(t, filepath.Join(base, "projects", "p.json"))
	mustTouch(t, filepath.Join(priv, ".claude.json")) // must not leak into a subdir listing

	got := readdirEntries(t, fs, "/projects")
	if !slices.Contains(got, "p.json") {
		t.Errorf("Readdir(/projects) missing p.json; got %v", got)
	}
	if slices.Contains(got, ".claude.json") {
		t.Errorf("Readdir(/projects) leaked private .claude.json; got %v", got)
	}
}

// TestFuseAttrCacheNoTornRead pins the load-bearing reason noattrcache stays on
// (fuse.go): plain `claude` edits ~/.claude.json externally (varying its size)
// while a pooled session reads the merged view through the mount via fresh opens,
// and every read must be a COMPLETE document — never truncated/torn. noattrcache
// makes the NFS client revalidate every read, so this holds; dropping it makes
// the client clamp reads to a stale cached size and serve a torn /.claude.json
// (close-to-open does NOT save it — measured), which would corrupt a session's
// state. Needs a real fuse-t mount; skips otherwise.
func TestFuseAttrCacheNoTornRead(t *testing.T) {
	base := t.TempDir()
	mnt := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	sibling := filepath.Join(filepath.Dir(base), ".claude.json")
	mkBase := func(pad int) []byte {
		return []byte(`{"theme":"light","sharedKey":"` + strings.Repeat("x", pad) + `","oauthAccount":{"accountUuid":"base"}}`)
	}
	if err := os.WriteFile(sibling, mkBase(1), 0o600); err != nil {
		t.Fatal(err)
	}
	p := &FuseProvider{}
	if err := p.Setup(base, mnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	defer p.Teardown(base, mnt)
	if err := os.WriteFile(filepath.Join(FusePrivateRoot(mnt), ".claude.json"), []byte(`{"theme":"dark","oauthAccount":{"accountUuid":"acct"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cj := filepath.Join(mnt, ".claude.json")
	if _, err := os.ReadFile(cj); err != nil { // warm the attr cache
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for i := 1; time.Now().Before(deadline); i++ {
		if err := os.WriteFile(sibling, mkBase(i%400+1), 0o600); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(cj)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !json.Valid(b) {
			t.Fatalf("TORN READ at i=%d: %d bytes not valid JSON: %q", i, len(b), b)
		}
	}
}

// TestFuseAttrCacheBaseToAccountCTO pins base->account propagation of the merged
// /.claude.json view: an external edit to the base ~/.claude.json must reach a
// pooled session reading through the mount on a fresh open, and reads must never
// be short/torn. It needs a real fuse-t mount and skips when one is unavailable.
func TestFuseAttrCacheBaseToAccountCTO(t *testing.T) {
	base := t.TempDir()
	mnt := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	sibling := filepath.Join(filepath.Dir(base), ".claude.json")
	if err := os.WriteFile(sibling, []byte(`{"theme":"light","sharedKey":"v1","oauthAccount":{"accountUuid":"base"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	p := &FuseProvider{}
	if err := p.Setup(base, mnt); err != nil {
		t.Skipf("fuse-t mount unavailable (acceptable; symlink is the default): %v", err)
	}
	defer p.Teardown(base, mnt)
	if err := os.WriteFile(filepath.Join(FusePrivateRoot(mnt), ".claude.json"), []byte(`{"theme":"dark","oauthAccount":{"accountUuid":"acct"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cjPath := filepath.Join(mnt, ".claude.json")
	readVal := func() string {
		b, err := os.ReadFile(cjPath)
		if err != nil {
			t.Fatalf("read .claude.json through mount: %v", err)
		}
		return strings.Trim(string(raw(t, b)["sharedKey"]), `"`)
	}
	// assertNoTear: at a stable point, the stat size must equal the bytes a full
	// read returns (a merged-view size/read mismatch truncates the client).
	assertNoTear := func() {
		fi, err := os.Stat(cjPath)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(cjPath)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() != int64(len(b)) {
			t.Fatalf("short/torn read: stat size %d != %d bytes read", fi.Size(), len(b))
		}
	}

	// Warm: the merged view reflects base's v1 (and caches its attrs).
	if got := readVal(); got != "v1" {
		t.Fatalf("warm merged sharedKey = %q, want v1", got)
	}
	assertNoTear()

	// External edit to base — a longer value so size AND mtime move.
	if err := os.WriteFile(sibling, []byte(`{"theme":"light","sharedKey":"v2-after-external-edit","oauthAccount":{"accountUuid":"base"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	// Close-to-open: a fresh open+read must observe v2. If dropping noattrcache
	// broke base->account propagation, this never converges.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if strings.HasPrefix(readVal(), "v2") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("external base edit not visible through the mount within 15s — close-to-open propagation regressed")
		}
		time.Sleep(200 * time.Millisecond)
	}
	assertNoTear()
}

// TestMirrorSharedLinkSyntheticStatNoSyscall pins the syscall-free carve-out
// presentation: after snapshotShared, a shared top-level entry's Getattr and
// Readlink serve a precomputed synthetic S_IFLNK stat (size = len(target)) and
// the absolute base target WITHOUT stat-ing the target — proven by deleting the
// targets after the snapshot and asserting the calls still succeed (a dangling
// symlink, exactly as a real on-disk symlink to a deleted file would). Distinct
// synthetic inodes keep the NFS client from aliasing two carve-outs.
func TestMirrorSharedLinkSyntheticStatNoSyscall(t *testing.T) {
	fs, base, _ := mirrorTree(t)
	mustMkdir(t, filepath.Join(base, "projects"))
	// history is a shared top-level FILE that IS carved as a symlink;
	// settings.json is deliberately NOT — it is the merged-view exception, guarded
	// below — so the file half of this litmus uses history.
	mustTouch(t, filepath.Join(base, "history"))
	mustTouch(t, filepath.Join(base, "settings.json"))
	fs.snapshotShared()

	// Remove the targets AFTER the snapshot: a syscall-free getattr/readlink must
	// still serve the synthetic presentation; an Lstat here would fail ENOENT.
	if err := os.RemoveAll(filepath.Join(base, "projects")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(base, "history")); err != nil {
		t.Fatal(err)
	}

	inos := map[uint64]string{}
	for _, name := range []string{"projects", "history"} {
		path := "/" + name
		target := filepath.Join(base, name)
		var stat fuse.Stat_t
		if st := fs.Getattr(path, &stat, ^uint64(0)); st != 0 {
			t.Fatalf("Getattr(%q) = %d, want 0 (synthetic, no syscall on a deleted target)", path, st)
		}
		if stat.Mode != fuse.S_IFLNK|0o777 {
			t.Errorf("Getattr(%q).Mode = %#o, want %#o", path, stat.Mode, fuse.S_IFLNK|0o777)
		}
		if stat.Size != int64(len(target)) {
			t.Errorf("Getattr(%q).Size = %d, want len(target) = %d", path, stat.Size, len(target))
		}
		if stat.Ino < sharedLinkInoBase {
			t.Errorf("Getattr(%q).Ino = %d, want a synthetic ino >= %d", path, stat.Ino, sharedLinkInoBase)
		}
		if prev, dup := inos[stat.Ino]; dup {
			t.Errorf("Getattr(%q).Ino = %d collides with %q", path, stat.Ino, prev)
		}
		inos[stat.Ino] = name
		if st, got := fs.Readlink(path); st != 0 || got != target {
			t.Fatalf("Readlink(%q) = (%d, %q), want (0, %q)", path, st, got, target)
		}
	}

	// A non-carved name (a private entry) is never presented as a symlink.
	if _, ok := fs.sharedEntryFor(claudeJSONFusePath); ok {
		t.Errorf("/.claude.json must not be a carve-out symlink")
	}
	// settings.json was present in base at snapshot time, yet it is the
	// merged-view exception (injected plansDirectory) — it must be excluded from
	// the carve-out so its intercepts run, never presented as a symlink.
	if _, ok := fs.sharedEntryFor(settingsJSONFusePath); ok {
		t.Errorf("/settings.json must not be a carve-out symlink (it is the merged-view exception)")
	}
}

// TestMirrorClaudeJSONMergedConcurrentSingleFlight pins the single-flight
// refactor: many goroutines racing merged() on one freshly-invalidated key must
// all return bytes identical to a serial merge, with no data race (run -race)
// and no torn result, and the cache must stay coherent afterward. The
// single-flight wrapper collapses the concurrent recompute herd; this guards
// that collapsing it never corrupts or tears the shared result.
func TestMirrorClaudeJSONMergedConcurrentSingleFlight(t *testing.T) {
	fs, home := newClaudeJSONMirror(t, mergePrivate, mergeBase)
	privPath := filepath.Join(home, "acct.private", ".claude.json")
	basePath := filepath.Join(home, ".claude.json")

	// Prime the cache for the initial key, then invalidate it by rewriting the
	// private file so the concurrent callers all miss and contend the merge.
	if _, st := fs.cj.merged(); st != 0 {
		t.Fatalf("warm merged = %d, want 0", st)
	}
	newPriv := `{"theme":"contended","oauthAccount":{"accountUuid":"acct-own"},"numStartups":3}`
	if err := os.WriteFile(privPath, []byte(newPriv), 0o600); err != nil {
		t.Fatal(err)
	}
	want, _, err := MergeClaudeJSON([]byte(newPriv), mustReadFile(t, basePath))
	if err != nil {
		t.Fatal(err)
	}

	const n = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([][]byte, n)
	codes := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], codes[i] = fs.cj.merged()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < n; i++ {
		if codes[i] != 0 {
			t.Fatalf("goroutine %d merged status = %d, want 0", i, codes[i])
		}
		if !bytes.Equal(results[i], want) {
			t.Fatalf("goroutine %d merged bytes diverged from the serial merge:\n got %s\nwant %s", i, results[i], want)
		}
	}
	// The cache stays coherent: a subsequent serial read still serves the bytes.
	got, st := fs.cj.merged()
	if st != 0 || !bytes.Equal(got, want) {
		t.Fatalf("post-contention merged = (%d, %s), want (0, %s)", st, got, want)
	}
	if err := fs.healthErr(); err != nil {
		t.Fatalf("healthErr = %v, want nil", err)
	}
}
