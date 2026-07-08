package pool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestWriteIdentityRoundTrip pins the WriteIdentity → AccountIdentity inverse on
// both the symlink and the fuse private-root path math: the injected
// oauthAccount survives verbatim (compared after unmarshal) and every sibling
// key the seed carried is left untouched.
func TestWriteIdentityRoundTrip(t *testing.T) {
	const seed = `{"numStartups":5,"userID":"uid-x","projects":{"/p":{"hasTrustDialogAccepted":true}},"oauthAccount":{"accountUuid":"OLD-uuid","emailAddress":"old@x"}}`
	newOAuth := json.RawMessage(`{"accountUuid":"uuid-123","emailAddress":"pooled@example.com","organizationUuid":"org-9","nested":{"k":1},"extra":true}`)

	for _, backend := range []fkoverlay.Backend{fkoverlay.BackendSymlink, fkoverlay.BackendNFS} {
		t.Run(string(backend), func(t *testing.T) {
			dir := t.TempDir()
			priv := dir
			if backend != fkoverlay.BackendSymlink {
				priv = fkoverlay.FusePrivateRoot(dir)
			}
			path := filepath.Join(priv, ".claude.json")
			if err := os.MkdirAll(priv, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
				t.Fatal(err)
			}

			if err := WriteIdentity(backend, dir, newOAuth); err != nil {
				t.Fatalf("WriteIdentity: %v", err)
			}

			// The established reader is the oracle for correct placement.
			id, err := AccountIdentity(backend, dir)
			if err != nil {
				t.Fatalf("AccountIdentity: %v", err)
			}
			if id.AccountUUID != "uuid-123" || id.EmailAddress != "pooled@example.com" {
				t.Fatalf("AccountIdentity = %+v, want uuid-123 / pooled@example.com", id)
			}

			raw, err := os.ReadFile(path) //nolint:gosec // G304: path is a test-owned file under t.TempDir(), not external input
			if err != nil {
				t.Fatal(err)
			}
			var top map[string]json.RawMessage
			if err := json.Unmarshal(raw, &top); err != nil {
				t.Fatalf("reparse written doc: %v", err)
			}

			// oauthAccount preserved verbatim, compared after unmarshal.
			var gotOAuth, wantOAuth any
			if err := json.Unmarshal(top["oauthAccount"], &gotOAuth); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(newOAuth, &wantOAuth); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotOAuth, wantOAuth) {
				t.Fatalf("oauthAccount = %s, want %s", top["oauthAccount"], newOAuth)
			}

			// Every non-identity sibling key is byte-for-byte unchanged.
			var seedMap, gotMap map[string]any
			if err := json.Unmarshal([]byte(seed), &seedMap); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &gotMap); err != nil {
				t.Fatal(err)
			}
			delete(seedMap, "oauthAccount")
			delete(gotMap, "oauthAccount")
			if !reflect.DeepEqual(seedMap, gotMap) {
				t.Fatalf("sibling keys changed:\n got  %v\n want %v", gotMap, seedMap)
			}
		})
	}
}

// TestWriteIdentityFailsLoud pins the fail-fast contract: a missing or
// unparseable document errors and no fresh document is ever minted in its place.
func TestWriteIdentityFailsLoud(t *testing.T) {
	oauth := json.RawMessage(`{"accountUuid":"u","emailAddress":"e@x"}`)
	cases := map[string]struct {
		create  bool
		seed    string
		oauth   json.RawMessage
		wantIs  error
		wantErr string
	}{
		"missing file":               {create: false, wantIs: os.ErrNotExist},
		"unparseable file":           {create: true, seed: "not json", wantErr: "parse"},
		"null document":              {create: true, seed: "null", wantErr: "not a JSON object"},
		"array document":             {create: true, seed: "[1,2,3]", wantErr: "parse"},
		"invalid oauthAccount input": {create: true, seed: `{"keep":true}`, oauth: json.RawMessage("{bad"), wantErr: "encode"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, ".claude.json")
			if tc.create {
				if err := os.WriteFile(path, []byte(tc.seed), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			in := oauth
			if tc.oauth != nil {
				in = tc.oauth
			}
			err := WriteIdentity(fkoverlay.BackendSymlink, dir, in)
			if err == nil {
				t.Fatal("WriteIdentity succeeded, want failure")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("error = %v, want errors.Is %v", err, tc.wantIs)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want to contain %q", err, tc.wantErr)
			}
			if !tc.create {
				if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
					t.Fatalf("a fresh .claude.json was minted (stat err = %v); WriteIdentity must never seed one", statErr)
				}
			} else {
				got, readErr := os.ReadFile(path) //nolint:gosec // G304: path is a test-owned file under t.TempDir(), not external input
				if readErr != nil {
					t.Fatal(readErr)
				}
				if string(got) != tc.seed {
					t.Fatalf("failed WriteIdentity mutated the file: %q, want untouched seed %q", got, tc.seed)
				}
			}
		})
	}
}
