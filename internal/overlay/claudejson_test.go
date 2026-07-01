package overlay

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// Fixture values are deliberately compact: json.Marshal compacts json.RawMessage,
// so only compact inputs can be asserted byte-identical across a merge or split.
const (
	mergePrivate = `{
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
	mergeBase = `{
		"hasCompletedOnboarding": true,
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

// Project-key fixtures mix shared approval keys with private session state, so
// assertions prove the carve-out splits a single entry, not whole entries.
const (
	projPrivate = `{
		"theme": "dark",
		"projects": {"/acct":{"history":["mine"],"allowedTools":["Bash"],"hasTrustDialogAccepted":false}}
	}`
	projBase = `{
		"theme": "dark",
		"projects": {
			"/acct":{"history":["theirs"],"lastSessionId":"base-sess","hasTrustDialogAccepted":true},
			"/other":{"history":["h"],"allowedTools":["X"],"lastSessionId":"s","hasTrustDialogAccepted":true,"enabledMcpjsonServers":["srv"]}
		}
	}`
)

// raw decodes b into top-level raw messages for byte-exact per-key comparison.
func raw(t *testing.T, b []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
	return m
}

func TestMergeClaudeJSON(t *testing.T) {
	cases := map[string]struct {
		private, base string
		wantChanged   bool
		want          map[string]string // key → expected raw JSON value
		absent        []string
		wantErr       bool
	}{
		"base wins on shareable keys": {
			private: mergePrivate, base: mergeBase, wantChanged: true,
			want: map[string]string{"theme": `"light"`, "claudeInChromeDefaultEnabled": `true`},
		},
		"private blacklisted keys survive byte-identical": {
			private: mergePrivate, base: mergeBase, wantChanged: true,
			want: map[string]string{
				"oauthAccount":   `{"accountUuid":"acct-own"}`,
				"userID":         `"acct-user"`,
				"anonymousId":    `"acct-anon"`,
				"projects":       `{"/acct":{"history":["mine"]}}`,
				"firstStartTime": `"2026-01-01T00:00:00Z"`,
				"numStartups":    `7`,
			},
		},
		"base oauthAccount never leaks into a private file lacking one": {
			private: `{"theme": "dark"}`, base: mergeBase, wantChanged: true,
			want:   map[string]string{"theme": `"light"`},
			absent: []string{"oauthAccount", "userID", "anonymousId", "projects", "firstStartTime", "numStartups"},
		},
		"private-only key shows through": {
			private: mergePrivate, base: mergeBase, wantChanged: true,
			want: map[string]string{"acctOnly": `"survives"`},
		},
		"identical shareable keys report unchanged": {
			private: `{"theme": "dark", "numStartups": 7}`, base: `{"theme": "dark", "numStartups": 9999}`,
			wantChanged: false,
			want:        map[string]string{"theme": `"dark"`, "numStartups": `7`},
		},
		"unparseable private errors": {private: `{not json`, base: mergeBase, wantErr: true},
		"unparseable base errors":    {private: mergePrivate, base: `{not json`, wantErr: true},
		"null private errors":        {private: `null`, base: mergeBase, wantErr: true},
		"null base errors":           {private: mergePrivate, base: `null`, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			merged, changed, err := MergeClaudeJSON([]byte(tc.private), []byte(tc.base))
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tc.wantChanged)
			}
			got := raw(t, merged)
			for k, want := range tc.want {
				if string(got[k]) != want {
					t.Errorf("merged[%q] = %s, want %s", k, got[k], want)
				}
			}
			for _, k := range tc.absent {
				if v, ok := got[k]; ok {
					t.Errorf("merged[%q] = %s, want absent", k, v)
				}
			}
		})
	}
}

// TestMergeClaudeJSONNilBase pins the degenerate contract: no base means the
// private bytes come back verbatim — formatting included — with no change.
func TestMergeClaudeJSONNilBase(t *testing.T) {
	private := []byte("{ \"theme\":   \"dark\" }\n")
	merged, changed, err := MergeClaudeJSON(private, nil)
	if err != nil || changed {
		t.Fatalf("changed=%v err=%v, want false,nil", changed, err)
	}
	if !bytes.Equal(merged, private) {
		t.Fatalf("merged = %q, want private verbatim %q", merged, private)
	}
}

// TestMergeClaudeJSONIdempotentAgainstPrettyBase pins the no-write short-circuit
// against a pretty-printed base (claude's own writer) while merged output holds
// compact values: re-merging the output against the same indented base must report
// changed=false with byte-identical output, otherwise every launch rewrites the
// account file and the MergeUnchanged race guard is dead code. The fixture carries a
// multi-line composite value and a string with raw <, & (json.Marshal escapes,
// json.Compact does not).
func TestMergeClaudeJSONIdempotentAgainstPrettyBase(t *testing.T) {
	prettyBase := []byte(`{
  "theme": "light",
  "customApiKeyResponses": {
    "approved": ["k1", "k2"],
    "rejected": []
  },
  "cachedChangelog": "<h1>v1 & v2</h1>",
  "oauthAccount": {
    "accountUuid": "base-own"
  }
}`)
	first, changed, err := MergeClaudeJSON([]byte(mergePrivate), prettyBase)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first merge: changed = false, want true (base carries new shareable keys)")
	}
	second, changed, err := MergeClaudeJSON(first, prettyBase)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("second merge against the same pretty base: changed = true, want false")
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("second merge diverged from the first:\n%s\n%s", first, second)
	}
}

// TestMergeClaudeJSONDeterministic pins byte-determinism: two merges of the same
// inputs must be byte-equal (json.Marshal key-sorts maps). The fuse merged view's
// Getattr size and Read depend on it.
func TestMergeClaudeJSONDeterministic(t *testing.T) {
	a, _, err := MergeClaudeJSON([]byte(mergePrivate), []byte(mergeBase))
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := MergeClaudeJSON([]byte(mergePrivate), []byte(mergeBase))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("two merges diverged:\n%s\n%s", a, b)
	}
}

// assertProjects checks doc's projects object: wantProj pins per-project raw values
// byte-exactly, absent pins per-project keys that must not exist, projectsAbsent
// pins that no projects object was minted.
func assertProjects(t *testing.T, doc []byte, wantProj map[string]map[string]string, absent map[string][]string, projectsAbsent bool) {
	t.Helper()
	projRaw, ok := raw(t, doc)["projects"]
	if projectsAbsent {
		if ok {
			t.Errorf("projects = %s, want absent", projRaw)
		}
		return
	}
	if !ok {
		t.Fatalf("doc carries no projects: %s", doc)
	}
	proj := raw(t, projRaw)
	for path, wantKeys := range wantProj {
		entryRaw, ok := proj[path]
		if !ok {
			t.Fatalf("project %q missing: %s", path, projRaw)
		}
		entry := raw(t, entryRaw)
		for k, want := range wantKeys {
			if string(entry[k]) != want {
				t.Errorf("project %q key %q = %s, want %s", path, k, entry[k], want)
			}
		}
	}
	for path, keys := range absent {
		entryRaw, ok := proj[path]
		if !ok {
			t.Fatalf("project %q missing: %s", path, projRaw)
		}
		entry := raw(t, entryRaw)
		for _, k := range keys {
			if v, ok := entry[k]; ok {
				t.Errorf("project %q key %q = %s, want absent", path, k, v)
			}
		}
	}
}

func TestMergeClaudeJSONSharedProjectKeys(t *testing.T) {
	cases := map[string]struct {
		private, base  string
		wantChanged    bool
		wantProj       map[string]map[string]string // path → key → expected raw JSON value
		absentInProj   map[string][]string          // path → keys that must not appear
		projectsAbsent bool
		wantErr        string // non-empty: substring the error must carry
	}{
		"base approval overrides into an existing entry, own session state survives": {
			private: projPrivate, base: projBase, wantChanged: true,
			wantProj: map[string]map[string]string{
				"/acct": {"hasTrustDialogAccepted": `true`, "history": `["mine"]`, "allowedTools": `["Bash"]`},
			},
			absentInProj: map[string][]string{"/acct": {"lastSessionId"}},
		},
		"entry minted for a never-opened project carries shared keys only": {
			private: projPrivate, base: projBase, wantChanged: true,
			wantProj: map[string]map[string]string{
				"/other": {"hasTrustDialogAccepted": `true`, "enabledMcpjsonServers": `["srv"]`},
			},
			absentInProj: map[string][]string{"/other": {"history", "allowedTools", "lastSessionId"}},
		},
		"projects object minted when private lacks it": {
			private: `{"theme": "dark"}`, base: projBase, wantChanged: true,
			wantProj: map[string]map[string]string{
				"/acct":  {"hasTrustDialogAccepted": `true`},
				"/other": {"hasTrustDialogAccepted": `true`, "enabledMcpjsonServers": `["srv"]`},
			},
			absentInProj: map[string][]string{
				"/acct":  {"history", "lastSessionId"},
				"/other": {"history", "allowedTools", "lastSessionId"},
			},
		},
		"no shared keys anywhere mints nothing and reports unchanged": {
			private: `{"theme": "dark"}`, base: `{"theme": "dark", "projects": {"/p":{"history":["h"]}}}`,
			wantChanged: false, projectsAbsent: true,
		},
		"base empty MCP list overrides a non-empty private one (the account's next commit writes its own back)": {
			private:     `{"projects": {"/p":{"enabledMcpjsonServers":["a","b"]}}}`,
			base:        `{"projects": {"/p":{"enabledMcpjsonServers":[]}}}`,
			wantChanged: true,
			wantProj:    map[string]map[string]string{"/p": {"enabledMcpjsonServers": `[]`}},
		},
		"local-scope mcpServers crosses into an existing entry, private session state survives": {
			private:     `{"projects": {"/p":{"history":["mine"],"allowedTools":["Bash"]}}}`,
			base:        `{"projects": {"/p":{"history":["theirs"],"mcpServers":{"XcodeBuildMCP":{"type":"stdio","command":"npx","args":["-y","xcodebuildmcp@latest","mcp"]}}}}}`,
			wantChanged: true,
			wantProj: map[string]map[string]string{
				"/p": {
					"mcpServers":   `{"XcodeBuildMCP":{"type":"stdio","command":"npx","args":["-y","xcodebuildmcp@latest","mcp"]}}`,
					"history":      `["mine"]`,
					"allowedTools": `["Bash"]`,
				},
			},
		},
		"local-scope mcpServers minted for a never-opened project, no session state leaks": {
			private:     `{"theme":"dark"}`,
			base:        `{"projects": {"/new":{"history":["h"],"mcpServers":{"srv":{"type":"http","url":"https://example/mcp"}}}}}`,
			wantChanged: true,
			wantProj: map[string]map[string]string{
				"/new": {"mcpServers": `{"srv":{"type":"http","url":"https://example/mcp"}}`},
			},
			absentInProj: map[string][]string{"/new": {"history"}},
		},
		"numeric base projects errors":    {private: projPrivate, base: `{"projects": 5}`, wantErr: "merge shared project keys"},
		"null base projects errors":       {private: projPrivate, base: `{"projects": null}`, wantErr: "merge shared project keys"},
		"numeric private projects errors": {private: `{"projects": 5}`, base: projBase, wantErr: "merge shared project keys"},
		"null private projects errors":    {private: `{"projects": null}`, base: projBase, wantErr: "merge shared project keys"},
		"non-object base project entry errors naming the path": {
			private: projPrivate, base: `{"projects": {"/bad": 42}}`, wantErr: `source project "/bad"`,
		},
		"non-object private entry the base shares into errors naming the path": {
			private: `{"projects": {"/p": "str"}}`, base: `{"projects": {"/p":{"hasTrustDialogAccepted":true}}}`,
			wantErr: `destination project "/p"`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			merged, changed, err := MergeClaudeJSON([]byte(tc.private), []byte(tc.base))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one carrying %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tc.wantChanged)
			}
			assertProjects(t, merged, tc.wantProj, tc.absentInProj, tc.projectsAbsent)
		})
	}
}

// TestMergeClaudeJSONSharedProjectKeysIdempotent extends the pretty-base no-flap
// pin to the project carve-out: re-merging the output against the same indented
// base must report changed=false with byte-identical output, otherwise every launch
// rewrites the account file even after approvals landed.
func TestMergeClaudeJSONSharedProjectKeysIdempotent(t *testing.T) {
	prettyBase := []byte(`{
  "theme": "dark",
  "projects": {
    "/acct": {
      "history": ["theirs"],
      "hasTrustDialogAccepted": true,
      "enabledMcpjsonServers": ["srv-a", "srv-b"]
    }
  }
}`)
	first, changed, err := MergeClaudeJSON([]byte(projPrivate), prettyBase)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first merge: changed = false, want true (base carries new approvals)")
	}
	second, changed, err := MergeClaudeJSON(first, prettyBase)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("second merge against the same pretty base: changed = true, want false")
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("second merge diverged from the first:\n%s\n%s", first, second)
	}
}

// TestMergeClaudeJSONSharedProjectKeysDeterministic pins byte-determinism
// through the project walk: map iteration order must not leak into the
// output (json.Marshal key-sorts every level the walk re-encodes).
func TestMergeClaudeJSONSharedProjectKeysDeterministic(t *testing.T) {
	a, _, err := MergeClaudeJSON([]byte(projPrivate), []byte(projBase))
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := MergeClaudeJSON([]byte(projPrivate), []byte(projBase))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("two merges diverged:\n%s\n%s", a, b)
	}
}

// TestMergeClaudeJSONSharedProjectKeysLazyDstParse pins lazy destination
// parsing: a structurally corrupt (non-object) private entry at a path the
// base shares nothing into passes through untouched instead of erroring —
// the carve-out only ever inspects entries it actually writes.
func TestMergeClaudeJSONSharedProjectKeysLazyDstParse(t *testing.T) {
	private := `{"projects": {"/weird": 7, "/p": {"history":["mine"]}}}`
	base := `{"projects": {"/p": {"hasTrustDialogAccepted":true}, "/weird": {"history":["h"]}}}`
	merged, changed, err := MergeClaudeJSON([]byte(private), []byte(base))
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("changed = false, want true (approval crossed into /p)")
	}
	proj := raw(t, raw(t, merged)["projects"])
	if string(proj["/weird"]) != `7` {
		t.Errorf(`projects["/weird"] = %s, want the original 7 passed through`, proj["/weird"])
	}
	entry := raw(t, proj["/p"])
	if string(entry["hasTrustDialogAccepted"]) != `true` || string(entry["history"]) != `["mine"]` {
		t.Errorf(`projects["/p"] = %s, want the approval beside the original history`, proj["/p"])
	}
}

func TestSplitClaudeJSON(t *testing.T) {
	// payload is what claude committed through a pooled session: shareable changes
	// plus per-account state that must never reach base.
	payload := `{
		"theme": "solarized",
		"newSetting": 42,
		"oauthAccount": {"accountUuid":"acct-own"},
		"projects": {"/acct":{"history":["mine"]}},
		"numStartups": 7,
		"userID": "acct-user",
		"anonymousId": "acct-anon",
		"firstStartTime": "2026-01-01T00:00:00Z"
	}`
	cases := map[string]struct {
		payload, base string
		want          map[string]string
		wantErr       bool
	}{
		"shareable payload keys overlay base": {
			payload: payload, base: mergeBase,
			want: map[string]string{"theme": `"solarized"`, "newSetting": `42`},
		},
		"blacklisted payload keys never copied": {
			payload: payload, base: mergeBase,
			want: map[string]string{
				"oauthAccount":   `{"accountUuid":"base-own"}`,
				"projects":       `{"/base":{"history":["theirs"]}}`,
				"numStartups":    `9999`,
				"userID":         `"base-user"`,
				"anonymousId":    `"base-anon"`,
				"firstStartTime": `"2020-01-01T00:00:00Z"`,
			},
		},
		"base-only keys retained, deletions never propagate": {
			payload: `{"theme": "solarized"}`, base: `{"theme": "light", "keepMe": true}`,
			want: map[string]string{"theme": `"solarized"`, "keepMe": `true`},
		},
		"unparseable payload errors": {payload: `{not json`, base: mergeBase, wantErr: true},
		"unparseable base errors":    {payload: payload, base: `{not json`, wantErr: true},
		"nil base errors":            {payload: payload, base: "", wantErr: true},
		"null payload errors":        {payload: `null`, base: mergeBase, wantErr: true},
		"null base errors":           {payload: payload, base: `null`, wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var base []byte
			if tc.base != "" {
				base = []byte(tc.base)
			}
			out, err := SplitClaudeJSON([]byte(tc.payload), base)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := raw(t, out)
			for k, want := range tc.want {
				if string(got[k]) != want {
					t.Errorf("split[%q] = %s, want %s", k, got[k], want)
				}
			}
		})
	}
}

func TestSplitClaudeJSONSharedProjectKeys(t *testing.T) {
	cases := map[string]struct {
		payload, base string
		wantProj      map[string]map[string]string
		absentInProj  map[string][]string
		wantByteEqual bool // out must be byte-identical to the base fixture
		wantErr       string
	}{
		"payload approval reaches base's entry, base history byte-identical": {
			payload:      `{"projects": {"/b":{"history":["mine"],"allowedTools":["Bash"],"hasTrustDialogAccepted":true}}}`,
			base:         `{"projects": {"/b":{"history":["theirs"]}}}`,
			wantProj:     map[string]map[string]string{"/b": {"hasTrustDialogAccepted": `true`, "history": `["theirs"]`}},
			absentInProj: map[string][]string{"/b": {"allowedTools"}},
		},
		"skeleton entry minted in base for a project base never saw": {
			payload: `{"projects": {"/new":{"history":["mine"],"allowedTools":["Bash"],"lastSessionId":"s","hasTrustDialogAccepted":true}}}`,
			base:    `{"projects": {"/b":{"history":["theirs"]}}}`,
			wantProj: map[string]map[string]string{
				"/new": {"hasTrustDialogAccepted": `true`},
				"/b":   {"history": `["theirs"]`},
			},
			absentInProj: map[string][]string{"/new": {"history", "allowedTools", "lastSessionId"}},
		},
		"base lacking projects gains one skeleton entry": {
			payload:      `{"projects": {"/new":{"history":["mine"],"hasTrustDialogAccepted":true}}}`,
			base:         `{"theme": "light"}`,
			wantProj:     map[string]map[string]string{"/new": {"hasTrustDialogAccepted": `true`}},
			absentInProj: map[string][]string{"/new": {"history"}},
		},
		// The byte-equal fixtures are json.Marshal form (top-level keys sorted,
		// compact) with entry keys deliberately UNSORTED: byte-equality across the
		// split proves the projects RawMessage passed through untouched (a re-marshal
		// would sort the keys), which feeds writeThroughBase's bytes.Equal
		// short-circuit.
		"payload without projects leaves base byte-identical": {
			payload:       `{"theme":"light"}`,
			base:          `{"projects":{"/b":{"zKey":1,"aKey":2}},"theme":"light"}`,
			wantByteEqual: true,
		},
		"no shared-key diff leaves base byte-identical": {
			payload:       `{"projects":{"/b":{"hasTrustDialogAccepted":true,"history":["x"]}},"theme":"light"}`,
			base:          `{"projects":{"/b":{"zKey":1,"aKey":2,"hasTrustDialogAccepted":true}},"theme":"light"}`,
			wantByteEqual: true,
		},
		"shared key absent from payload entry is never deleted from base": {
			payload:  `{"projects": {"/b":{"enabledMcpjsonServers":["s"]}}}`,
			base:     `{"projects": {"/b":{"hasTrustDialogAccepted":true}}}`,
			wantProj: map[string]map[string]string{"/b": {"hasTrustDialogAccepted": `true`, "enabledMcpjsonServers": `["s"]`}},
		},
		"local-scope mcpServers writes through to base, base session history retained": {
			payload:      `{"projects": {"/b":{"history":["mine"],"allowedTools":["Bash"],"mcpServers":{"XcodeBuildMCP":{"type":"stdio","command":"npx"}}}}}`,
			base:         `{"projects": {"/b":{"history":["theirs"]}}}`,
			wantProj:     map[string]map[string]string{"/b": {"mcpServers": `{"XcodeBuildMCP":{"type":"stdio","command":"npx"}}`, "history": `["theirs"]`}},
			absentInProj: map[string][]string{"/b": {"allowedTools"}},
		},
		"numeric payload projects errors": {payload: `{"projects": 5}`, base: `{"theme":"light"}`, wantErr: "split shared project keys"},
		"null payload projects errors":    {payload: `{"projects": null}`, base: `{"theme":"light"}`, wantErr: "split shared project keys"},
		"numeric base projects errors": {
			payload: `{"projects": {"/p":{"hasTrustDialogAccepted":true}}}`, base: `{"projects": 5}`,
			wantErr: "split shared project keys",
		},
		"null base projects errors": {
			payload: `{"projects": {"/p":{"hasTrustDialogAccepted":true}}}`, base: `{"projects": null}`,
			wantErr: "split shared project keys",
		},
		"non-object payload entry errors naming the path": {
			payload: `{"projects": {"/bad": 42}}`, base: `{"theme":"light"}`, wantErr: `source project "/bad"`,
		},
		"non-object base entry the payload shares into errors naming the path": {
			payload: `{"projects": {"/p":{"hasTrustDialogAccepted":true}}}`, base: `{"projects": {"/p": []}}`,
			wantErr: `destination project "/p"`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := SplitClaudeJSON([]byte(tc.payload), []byte(tc.base))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one carrying %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantByteEqual {
				if !bytes.Equal(out, []byte(tc.base)) {
					t.Fatalf("split rewrote a base it had no shared-key diff against:\n%s\n%s", tc.base, out)
				}
				return
			}
			assertProjects(t, out, tc.wantProj, tc.absentInProj, false)
		})
	}
}

// TestMergeSplitRoundTrip rehearses the fuse write-through cycle: merge base over
// private, change a shareable key and accept a project's trust dialog, split the
// commit back. The new base must carry both changes while every other blacklisted
// key stays byte-identical, and inside projects only the shared approval crosses:
// base's own entry survives and the session's history never lands.
func TestMergeSplitRoundTrip(t *testing.T) {
	merged, _, err := MergeClaudeJSON([]byte(mergePrivate), []byte(mergeBase))
	if err != nil {
		t.Fatal(err)
	}
	doc := raw(t, merged)
	doc["theme"] = json.RawMessage(`"solarized"`)
	doc["projects"] = json.RawMessage(`{"/acct":{"history":["mine"],"hasTrustDialogAccepted":true}}`)
	committed, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}

	newBase, err := SplitClaudeJSON(committed, []byte(mergeBase))
	if err != nil {
		t.Fatal(err)
	}
	got, orig := raw(t, newBase), raw(t, []byte(mergeBase))
	if string(got["theme"]) != `"solarized"` {
		t.Errorf("shareable change lost: theme = %s", got["theme"])
	}
	for k := range ClaudeJSONPrivateKeys {
		if k == "projects" {
			continue
		}
		if !bytes.Equal(got[k], orig[k]) {
			t.Errorf("base[%q] changed across the round trip: %s → %s", k, orig[k], got[k])
		}
	}
	proj := raw(t, got["projects"])
	if string(proj["/base"]) != `{"history":["theirs"]}` {
		t.Errorf("base's own project entry rewritten: %s", proj["/base"])
	}
	if string(proj["/acct"]) != `{"hasTrustDialogAccepted":true}` {
		t.Errorf("skeleton entry = %s, want the shared approval and nothing else", proj["/acct"])
	}
}
