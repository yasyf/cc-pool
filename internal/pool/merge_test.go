package pool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// Fixture values are compact inside (json.Marshal compacts RawMessage values),
// so blacklisted keys can be asserted byte-identical across a merge.
const (
	mergeAcctJSON = `{
		"hasCompletedOnboarding": true,
		"theme": "dark",
		"oauthAccount": {"accountUuid":"acct-own"},
		"userID": "acct-user",
		"anonymousId": "acct-anon",
		"projects": {"/acct":{"history":["mine"]}},
		"firstStartTime": "2026-01-01T00:00:00Z",
		"numStartups": 7,
		"acctOnly": "survives"
	}`
	mergeBaseJSON = `{
		"theme": "light",
		"claudeInChromeDefaultEnabled": true,
		"oauthAccount": {"accountUuid":"base-own"},
		"userID": "base-user",
		"anonymousId": "base-anon",
		"projects": {"/base":{"history":["theirs"]}},
		"firstStartTime": "2020-01-01T00:00:00Z",
		"numStartups": 9999
	}`
)

func TestMergeClaudeJSON(t *testing.T) {
	prov := newSymlinkProvider()

	writeSrc := func(t *testing.T, content string) string {
		t.Helper()
		src := filepath.Join(t.TempDir(), ".claude.json")
		if err := os.WriteFile(src, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return src
	}
	writeDst := func(t *testing.T, acct, content string) string {
		t.Helper()
		dst := filepath.Join(acct, ".claude.json")
		if err := os.WriteFile(dst, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return dst
	}

	t.Run("propagates base-only keys at 0600", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, `{"hasCompletedOnboarding": true}`)
		out, err := mergeClaudeJSON(prov, acct, writeSrc(t, `{"claudeInChromeDefaultEnabled": true}`))
		if err != nil || out != MergeApplied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		got := decode(t, readFile(t, dst))
		if got["claudeInChromeDefaultEnabled"] != true || got["hasCompletedOnboarding"] != true {
			t.Fatalf("merged content wrong: %v", got)
		}
		fi, err := os.Stat(dst)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Errorf("mode = %v, want 0600", fi.Mode().Perm())
		}
	})

	t.Run("base wins on a differing shareable key", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, `{"theme": "dark"}`)
		out, err := mergeClaudeJSON(prov, acct, writeSrc(t, `{"theme": "light"}`))
		if err != nil || out != MergeApplied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if got := decode(t, readFile(t, dst)); got["theme"] != "light" {
			t.Fatalf("base did not win: %v", got)
		}
	})

	t.Run("all blacklisted keys survive byte-identical", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, mergeAcctJSON)
		out, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON))
		if err != nil || out != MergeApplied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		got := rawTop(t, readFile(t, dst))
		orig := rawTop(t, []byte(mergeAcctJSON))
		for k := range overlay.ClaudeJSONPrivateKeys {
			if string(got[k]) != string(orig[k]) {
				t.Errorf("blacklisted %q changed: %s → %s", k, orig[k], got[k])
			}
		}
		if string(got["theme"]) != `"light"` {
			t.Errorf("shareable key not merged: %s", got["theme"])
		}
	})

	t.Run("account-only keys survive", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, mergeAcctJSON)
		if _, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON)); err != nil {
			t.Fatal(err)
		}
		if got := decode(t, readFile(t, dst)); got["acctOnly"] != "survives" {
			t.Fatalf("account-only key lost: %v", got)
		}
	})

	t.Run("missing base is a no-op", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, mergeAcctJSON)
		out, err := mergeClaudeJSON(prov, acct, filepath.Join(t.TempDir(), "nope.json"))
		if err != nil || out != MergeNoBase {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if got := string(readFile(t, dst)); got != mergeAcctJSON {
			t.Fatalf("dst modified on a no-base merge: %q", got)
		}
	})

	t.Run("missing account file is recreated as base-minus-blacklist", func(t *testing.T) {
		acct := t.TempDir()
		out, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON))
		if err != nil || out != MergeRecreated {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		got := decode(t, readFile(t, filepath.Join(acct, ".claude.json")))
		want := decode(t, []byte(mergeBaseJSON))
		for k := range overlay.ClaudeJSONPrivateKeys {
			delete(want, k)
			if v, ok := got[k]; ok {
				t.Errorf("blacklisted %q leaked into the recreated file: %v", k, v)
			}
		}
		if got["theme"] != "light" || got["claudeInChromeDefaultEnabled"] != true {
			t.Fatalf("recreated content = %v, want %v", got, want)
		}
	})

	t.Run("missing account file with a stranded fuse copy errors", func(t *testing.T) {
		acct := filepath.Join(t.TempDir(), "acct-01")
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		priv := fkoverlay.FusePrivateRoot(acct)
		if err := os.MkdirAll(priv, 0o700); err != nil {
			t.Fatal(err)
		}
		stranded := filepath.Join(priv, ".claude.json")
		if err := os.WriteFile(stranded, []byte(mergeAcctJSON), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON))
		if err == nil || !strings.Contains(err.Error(), "ccp doctor") {
			t.Fatalf("err = %v, want the stranded-copy error pointing at ccp doctor", err)
		}
		if _, err := os.Stat(filepath.Join(acct, ".claude.json")); !os.IsNotExist(err) {
			t.Fatal("a file was minted over the stranded copy's restore target")
		}
		if got := string(readFile(t, stranded)); got != mergeAcctJSON {
			t.Fatalf("stranded copy modified: %q", got)
		}
	})

	t.Run("unprobeable stranded copy is a hard error, not a recreate", func(t *testing.T) {
		acct := filepath.Join(t.TempDir(), "acct-01")
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		priv := fkoverlay.FusePrivateRoot(acct)
		if err := os.MkdirAll(priv, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(priv, 0o700) }) //nolint:gosec // G302: restoring a test dir to traversable perms in cleanup
		_, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON))
		if !errors.Is(err, os.ErrPermission) {
			t.Fatalf("err = %v, want a wrapped permission error from the stranded-copy probe", err)
		}
		if _, err := os.Lstat(filepath.Join(acct, ".claude.json")); !os.IsNotExist(err) {
			t.Fatal("a file was minted despite the unprobeable stranded-copy path")
		}
	})

	t.Run("malformed base errors, dst untouched", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, mergeAcctJSON)
		if _, err := mergeClaudeJSON(prov, acct, writeSrc(t, `{not json`)); err == nil {
			t.Fatal("malformed base must error")
		}
		if got := string(readFile(t, dst)); got != mergeAcctJSON {
			t.Fatalf("dst modified despite malformed base: %q", got)
		}
	})

	t.Run("malformed account file errors, never clobbered", func(t *testing.T) {
		acct := t.TempDir()
		dst := writeDst(t, acct, `{half a login identity`)
		if _, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON)); err == nil {
			t.Fatal("malformed account file must error, not be replaced")
		}
		if got := string(readFile(t, dst)); got != `{half a login identity` {
			t.Fatalf("unparseable account file was clobbered: %q", got)
		}
	})

	t.Run("null account file errors, never clobbered", func(t *testing.T) {
		// A bare `null` parses but is not an object; before the nil-map guard
		// this panicked inside MergeClaudeJSON instead of erroring.
		acct := t.TempDir()
		dst := writeDst(t, acct, `null`)
		if _, err := mergeClaudeJSON(prov, acct, writeSrc(t, mergeBaseJSON)); err == nil {
			t.Fatal("null account file must error, not be replaced")
		}
		if got := string(readFile(t, dst)); got != `null` {
			t.Fatalf("null account file was clobbered: %q", got)
		}
	})

	t.Run("pretty-printed base reports unchanged on the second merge", func(t *testing.T) {
		// Production base files are pretty-printed (claude's own writer); the
		// multi-line composite value is what made the pre-normalization compare
		// fail forever and rewrite the account file on every launch.
		acct := t.TempDir()
		writeDst(t, acct, `{"theme": "dark"}`)
		src := writeSrc(t, `{
  "theme": "light",
  "customApiKeyResponses": {
    "approved": ["k1", "k2"]
  }
}`)
		out, err := mergeClaudeJSON(prov, acct, src)
		if err != nil || out != MergeApplied {
			t.Fatalf("first merge: outcome = %q err = %v, want %q", out, err, MergeApplied)
		}
		out, err = mergeClaudeJSON(prov, acct, src)
		if err != nil || out != MergeUnchanged {
			t.Fatalf("second merge: outcome = %q err = %v, want %q", out, err, MergeUnchanged)
		}
	})

	t.Run("unchanged merge provably skips the write", func(t *testing.T) {
		acct := t.TempDir()
		writeDst(t, acct, `{"theme": "light", "acctOnly": 1}`)
		if err := os.Chmod(acct, 0o500); err != nil { //nolint:gosec // G302: deliberately makes the dir read-only to exercise the write-failure path
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(acct, 0o700) }) //nolint:gosec // G302: restoring a test dir to traversable perms in cleanup
		// Any write attempt fails in a read-only dir, so MergeUnchanged with no
		// error proves the write was skipped, not survived.
		out, err := mergeClaudeJSON(prov, acct, writeSrc(t, `{"theme": "light"}`))
		if err != nil || out != MergeUnchanged {
			t.Fatalf("outcome = %q err = %v, want unchanged with no write", out, err)
		}
	})

	t.Run("mounted account dir is refused", func(t *testing.T) {
		// /dev is always a devfs mountpoint on macOS (same trick as the
		// overlay Mounted test); the specific wording pins the guard itself,
		// not a downstream write failure.
		_, err := mergeClaudeJSON(prov, "/dev", writeSrc(t, mergeBaseJSON))
		if err == nil || !strings.Contains(err.Error(), "live mountpoint") {
			t.Fatalf("err = %v, want the live-mountpoint refusal", err)
		}
	})

	t.Run("stale symlink dst is replaced, target untouched", func(t *testing.T) {
		acct := t.TempDir()
		canary := filepath.Join(t.TempDir(), "canary.json")
		if err := os.WriteFile(canary, []byte(`{"canary": true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(canary, filepath.Join(acct, ".claude.json")); err != nil {
			t.Fatal(err)
		}
		out, err := mergeClaudeJSON(prov, acct, writeSrc(t, `{"newKey": 1}`))
		if err != nil || out != MergeApplied {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if fi, err := os.Lstat(filepath.Join(acct, ".claude.json")); err != nil || fi.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("destination still a symlink (err=%v)", err)
		}
		if got := string(readFile(t, canary)); got != `{"canary": true}` {
			t.Fatalf("merge wrote through the symlink into the target: %q", got)
		}
	})

	t.Run("fuse-shaped provider writes the private root", func(t *testing.T) {
		acct := filepath.Join(t.TempDir(), "acct-01")
		if err := os.MkdirAll(acct, 0o700); err != nil {
			t.Fatal(err)
		}
		out, err := mergeClaudeJSON(&privateRootProvider{}, acct, writeSrc(t, mergeBaseJSON))
		if err != nil || out != MergeRecreated {
			t.Fatalf("outcome = %q err = %v", out, err)
		}
		if _, err := os.Stat(filepath.Join(acct+".private", ".claude.json")); err != nil {
			t.Fatalf("merge not in private root: %v", err)
		}
		if _, err := os.Stat(filepath.Join(acct, ".claude.json")); !os.IsNotExist(err) {
			t.Fatal("merge must not land in the mountpoint dir")
		}
	})

	t.Run("base project approval lands once, then pins unchanged on re-run", func(t *testing.T) {
		// The pretty-printed src is the production shape (claude's own writer);
		// a second MergeApplied here is the launch-flap bug, not a tie-break.
		acct := t.TempDir()
		dst := writeDst(t, acct, `{"theme": "light", "projects": {"/p":{"history":["mine"]}}}`)
		src := writeSrc(t, `{
  "theme": "light",
  "projects": {
    "/p": {
      "history": ["theirs"],
      "hasTrustDialogAccepted": true
    }
  }
}`)
		out, err := mergeClaudeJSON(prov, acct, src)
		if err != nil || out != MergeApplied {
			t.Fatalf("first merge: outcome = %q err = %v, want %q", out, err, MergeApplied)
		}
		proj := rawTop(t, rawTop(t, readFile(t, dst))["projects"])
		entry := rawTop(t, proj["/p"])
		if string(entry["hasTrustDialogAccepted"]) != `true` {
			t.Errorf("approval did not reach the account: %s", proj["/p"])
		}
		if string(entry["history"]) != `["mine"]` {
			t.Errorf("base project history clobbered the account's own: %s", entry["history"])
		}
		out, err = mergeClaudeJSON(prov, acct, src)
		if err != nil || out != MergeUnchanged {
			t.Fatalf("second merge: outcome = %q err = %v, want %q", out, err, MergeUnchanged)
		}
	})

	t.Run("recreated account file carries skeleton project entries, shared keys only", func(t *testing.T) {
		acct := t.TempDir()
		src := writeSrc(t, `{"theme": "light", "projects": {"/p":{"history":["theirs"],"lastSessionId":"s","hasTrustDialogAccepted":true,"enabledMcpjsonServers":["srv"]}}}`)
		out, err := mergeClaudeJSON(prov, acct, src)
		if err != nil || out != MergeRecreated {
			t.Fatalf("outcome = %q err = %v, want %q", out, err, MergeRecreated)
		}
		proj := rawTop(t, rawTop(t, readFile(t, filepath.Join(acct, ".claude.json")))["projects"])
		// Byte-exact skeleton (json.Marshal key-sorts): the two shared keys and
		// nothing else — base history and session state never land.
		if string(proj["/p"]) != `{"enabledMcpjsonServers":["srv"],"hasTrustDialogAccepted":true}` {
			t.Fatalf(`recreated projects["/p"] = %s, want the shared keys only`, proj["/p"])
		}
	})
}

// TestMergeBaseClaudeJSON pins the Manager-level gate: only accounts whose
// RECORDED kind is symlink merge (the fuse arm serves its own merged view),
// and the provider resolves through the injectable OverlayFor seam — a bare
// OverlayProviderFor would ignore the fake and fail the resolved-kinds
// assertion.
func TestMergeBaseClaudeJSON(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{"theme": "light"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openTestStore(t)
	symDir, fuseDir := filepath.Join(home, "acct-01"), filepath.Join(home, "acct-02")
	for _, a := range []store.Account{
		{ID: 1, ConfigDir: symDir, KeychainService: "svc", KeychainAccount: "u", OverlayKind: "symlink"},
		{ID: 2, ConfigDir: fuseDir, KeychainService: "svc", KeychainAccount: "u", OverlayKind: "nfs"},
	} {
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(a.ConfigDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(a.ConfigDir, ".claude.json"), []byte(`{"theme": "dark"}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var resolved []fkoverlay.Backend
	m := &Manager{Store: st, OverlayFor: func(kind fkoverlay.Backend) (fkoverlay.Provider, error) {
		resolved = append(resolved, kind)
		return newSymlinkProvider(), nil
	}}

	fuseAcct, err := st.GetAccount(2)
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.MergeBaseClaudeJSON(fuseAcct)
	if err != nil || out != MergeSkippedOverlay {
		t.Fatalf("fuse account: outcome = %q err = %v, want %q", out, err, MergeSkippedOverlay)
	}
	if got := string(readFile(t, filepath.Join(fuseDir, ".claude.json"))); got != `{"theme": "dark"}` {
		t.Fatalf("fuse account's private file touched by the skipped merge: %q", got)
	}
	if len(resolved) != 0 {
		t.Fatalf("the gate must precede provider resolution; resolved %v", resolved)
	}

	symAcct, err := st.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	out, err = m.MergeBaseClaudeJSON(symAcct)
	if err != nil || out != MergeApplied {
		t.Fatalf("symlink account: outcome = %q err = %v, want %q", out, err, MergeApplied)
	}
	if got := decode(t, readFile(t, filepath.Join(symDir, ".claude.json"))); got["theme"] != "light" {
		t.Fatalf("base setting did not reach the symlink account: %v", got)
	}
	if len(resolved) != 1 || resolved[0] != fkoverlay.BackendSymlink {
		t.Fatalf("provider not resolved through the OverlayFor seam: %v", resolved)
	}

	// Errors from the merge layer are wrapped once with the account id.
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := m.MergeBaseClaudeJSON(symAcct); err == nil || !strings.Contains(err.Error(), "acct-01") {
		t.Fatalf("err = %v, want a wrap naming acct-01", err)
	}
}

// rawTop decodes top-level keys to raw JSON values for byte-exact comparison.
func rawTop(t *testing.T, b []byte) map[string]json.RawMessage {
	t.Helper()
	var rm map[string]json.RawMessage
	if err := json.Unmarshal(b, &rm); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return rm
}
