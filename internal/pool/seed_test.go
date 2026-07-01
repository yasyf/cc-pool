package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

type privateRootProvider struct{ fkoverlay.SymlinkProvider }

func (*privateRootProvider) PrivateRoot(accountDir string) string { return accountDir + ".private" }

const seedSrc = `{
	"hasCompletedOnboarding": true,
	"lastOnboardingVersion": "1.0.10",
	"oauthAccount": {"accountUuid": "u-1", "emailAddress": "me@example.com"},
	"mcpServers": {"semble": {"command": "uvx", "args": ["semble-mcp"]}},
	"projects": {"/Users/x/code": {"allowedTools": ["Bash(go test:*)"], "history": ["héllo ✓"]}},
	"numStartups": 42,
	"userID": "deadbeef"
}`

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return m
}

func TestSeedClaudeJSON(t *testing.T) {
	prov := newSymlinkProvider()

	writeSrc := func(t *testing.T, content string) string {
		t.Helper()
		src := filepath.Join(t.TempDir(), ".claude.json")
		if err := os.WriteFile(src, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return src
	}

	t.Run("copies and strips only oauthAccount", func(t *testing.T) {
		acct := t.TempDir()
		out, err := seedClaudeJSON(prov, acct, writeSrc(t, seedSrc))
		if err != nil {
			t.Fatal(err)
		}
		if out != SeedCopied {
			t.Fatalf("outcome = %q, want %q", out, SeedCopied)
		}
		got := decode(t, readFile(t, filepath.Join(acct, ".claude.json")))
		if _, ok := got["oauthAccount"]; ok {
			t.Fatal("oauthAccount survived the strip")
		}
		want := decode(t, []byte(seedSrc))
		delete(want, "oauthAccount")
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seeded content diverged beyond the oauthAccount strip:\ngot  %v\nwant %v", got, want)
		}
		fi, err := os.Stat(filepath.Join(acct, ".claude.json"))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Errorf("mode = %v, want 0600", fi.Mode().Perm())
		}
	})

	t.Run("source without oauthAccount copies verbatim", func(t *testing.T) {
		acct := t.TempDir()
		out, err := seedClaudeJSON(prov, acct, writeSrc(t, `{"hasCompletedOnboarding": true}`))
		if err != nil || out != SeedCopied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		got := decode(t, readFile(t, filepath.Join(acct, ".claude.json")))
		if got["hasCompletedOnboarding"] != true {
			t.Fatalf("content lost: %v", got)
		}
	})

	t.Run("missing source skips with no file", func(t *testing.T) {
		acct := t.TempDir()
		out, err := seedClaudeJSON(prov, acct, filepath.Join(t.TempDir(), "nope.json"))
		if err != nil || out != SeedNoSource {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if _, err := os.Stat(filepath.Join(acct, ".claude.json")); !os.IsNotExist(err) {
			t.Fatal("no destination file should be created")
		}
	})

	t.Run("corrupt source fails the add", func(t *testing.T) {
		acct := t.TempDir()
		if _, err := seedClaudeJSON(prov, acct, writeSrc(t, `{not json`)); err == nil {
			t.Fatal("corrupt ~/.claude.json must be an error, not silently skipped")
		}
	})

	t.Run("pre-login stub is overwritten", func(t *testing.T) {
		acct := t.TempDir()
		stub := `{"firstStartTime": "2026-06-06T07:57:05.707Z", "userID": "fresh"}`
		if err := os.WriteFile(filepath.Join(acct, ".claude.json"), []byte(stub), 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := seedClaudeJSON(prov, acct, writeSrc(t, seedSrc))
		if err != nil || out != SeedCopied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		got := decode(t, readFile(t, filepath.Join(acct, ".claude.json")))
		if got["hasCompletedOnboarding"] != true {
			t.Fatal("stub was not overwritten by the seed")
		}
	})

	t.Run("logged-in destination is kept byte-identical", func(t *testing.T) {
		acct := t.TempDir()
		existing := `{"oauthAccount": {"accountUuid": "other"}, "hasCompletedOnboarding": true}`
		dst := filepath.Join(acct, ".claude.json")
		if err := os.WriteFile(dst, []byte(existing), 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := seedClaudeJSON(prov, acct, writeSrc(t, seedSrc))
		if err != nil || out != SeedKeptExisting {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if got := string(readFile(t, dst)); got != existing {
			t.Fatalf("logged-in state was modified: %q", got)
		}
	})

	t.Run("symlink destination is replaced, target untouched", func(t *testing.T) {
		acct := t.TempDir()
		canary := filepath.Join(t.TempDir(), "canary.json")
		if err := os.WriteFile(canary, []byte(`{"canary": true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(canary, filepath.Join(acct, ".claude.json")); err != nil {
			t.Fatal(err)
		}
		out, err := seedClaudeJSON(prov, acct, writeSrc(t, seedSrc))
		if err != nil || out != SeedCopied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if fi, err := os.Lstat(filepath.Join(acct, ".claude.json")); err != nil || fi.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("destination still a symlink (err=%v)", err)
		}
		if got := string(readFile(t, canary)); got != `{"canary": true}` {
			t.Fatalf("seed wrote through the symlink into the target: %q", got)
		}
	})

	t.Run("fuse-shaped provider seeds the private root", func(t *testing.T) {
		acct := filepath.Join(t.TempDir(), "acct-01")
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		out, err := seedClaudeJSON(&privateRootProvider{}, acct, writeSrc(t, seedSrc))
		if err != nil || out != SeedCopied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if _, err := os.Stat(filepath.Join(acct+".private", ".claude.json")); err != nil {
			t.Fatalf("seed not in private root: %v", err)
		}
		if _, err := os.Stat(filepath.Join(acct, ".claude.json")); !os.IsNotExist(err) {
			t.Fatal("seed must not land in the mountpoint dir")
		}
	})
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
	if err != nil {
		t.Fatal(err)
	}
	return b
}
