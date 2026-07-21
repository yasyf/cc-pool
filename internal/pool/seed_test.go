package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSeedClaudeJSONStripsIdentityAndNeverWritesThroughSymlink(t *testing.T) {
	source := filepath.Join(t.TempDir(), ".claude.json")
	if err := os.WriteFile(source, []byte(`{"oauthAccount":{"accountUuid":"main"},"pluginUsage":{"x":1},"userID":"kept"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	backing := t.TempDir()
	canary := filepath.Join(t.TempDir(), "canary")
	if err := os.WriteFile(canary, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(canary, privateClaudeJSONPath(backing)); err != nil {
		t.Fatal(err)
	}
	outcome, err := seedClaudeJSON(backing, source)
	if err != nil || outcome != SeedCopied {
		t.Fatalf("seed = %q, %v", outcome, err)
	}
	if content, _ := os.ReadFile(canary); string(content) != "untouched" {
		t.Fatalf("canary changed: %q", content)
	}
	content, err := os.ReadFile(privateClaudeJSONPath(backing))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	if document["oauthAccount"] != nil || document["pluginUsage"] != nil || string(document["userID"]) != `"kept"` || string(document["hasCompletedOnboarding"]) != "true" {
		t.Fatalf("seeded document = %s", content)
	}
	if info, err := os.Lstat(privateClaudeJSONPath(backing)); err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("seeded file = %v, %v", info, err)
	}
}

func TestSeedClaudeJSONKeepsCompletedLogin(t *testing.T) {
	backing := t.TempDir()
	existing := []byte(`{"oauthAccount":{"accountUuid":"pooled"}}`)
	if err := os.WriteFile(privateClaudeJSONPath(backing), existing, 0o600); err != nil {
		t.Fatal(err)
	}
	outcome, err := seedClaudeJSON(backing, filepath.Join(t.TempDir(), "missing"))
	if err != nil || outcome != SeedKeptExisting {
		t.Fatalf("seed = %q, %v", outcome, err)
	}
	content, _ := os.ReadFile(privateClaudeJSONPath(backing))
	if string(content) != string(existing) {
		t.Fatalf("existing login changed: %s", content)
	}
}
