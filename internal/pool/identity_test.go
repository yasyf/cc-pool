package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestAccountIdentityPathMath pins AccountIdentity as pure path math off the recorded backend — no provider resolution.
func TestAccountIdentityPathMath(t *testing.T) {
	const right = `{"oauthAccount":{"accountUuid":"u-right","emailAddress":"r@example.com"}}`
	const decoy = `{"oauthAccount":{"accountUuid":"u-decoy","emailAddress":"d@example.com"}}`

	tests := []struct {
		name     string
		backend  fkoverlay.Backend
		privSide bool
	}{
		{name: "nfs reads the private backing dir", backend: fkoverlay.BackendNFS, privSide: true},
		{name: "fskit reads the private backing dir", backend: fkoverlay.BackendFSKit, privSide: true},
		{name: "fileprovider reads the private backing dir, never through the domain", backend: fkoverlay.BackendFileProvider, privSide: true},
		{name: "symlink reads the account dir", backend: fkoverlay.BackendSymlink},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "acct-01")
			priv := fkoverlay.FusePrivateRoot(dir)
			for _, d := range []string{dir, priv} {
				if err := os.MkdirAll(d, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			inDir, inPriv := right, decoy
			if tc.privSide {
				inDir, inPriv = decoy, right
			}
			if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(inDir), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(inPriv), 0o600); err != nil {
				t.Fatal(err)
			}
			id, err := AccountIdentity(tc.backend, dir)
			if err != nil {
				t.Fatal(err)
			}
			if id.AccountUUID != "u-right" {
				t.Errorf("AccountIdentity(%q) read %q, want u-right", tc.backend, id.AccountUUID)
			}
		})
	}
}

func TestReadIdentity(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), ".claude.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("happy path parses fields, ignoring unknown keys", func(t *testing.T) {
		raw := `{"accountUuid":"u-1","emailAddress":"me@example.com","organizationUuid":"org-1"}`
		id, err := readIdentity(write(t, `{"oauthAccount": `+raw+`, "numStartups": 3}`))
		if err != nil {
			t.Fatal(err)
		}
		if id.AccountUUID != "u-1" || id.EmailAddress != "me@example.com" {
			t.Errorf("parsed fields = %+v", id)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := readIdentity(filepath.Join(t.TempDir(), "nope.json"))
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want ErrNoIdentity", err)
		}
	})

	t.Run("missing oauthAccount key", func(t *testing.T) {
		_, err := readIdentity(write(t, `{"hasCompletedOnboarding": true}`))
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want ErrNoIdentity", err)
		}
	})

	t.Run("empty accountUuid", func(t *testing.T) {
		_, err := readIdentity(write(t, `{"oauthAccount": {"accountUuid": "", "emailAddress": "x@y.z"}}`))
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want ErrNoIdentity", err)
		}
	})

	t.Run("corrupt document is a real error", func(t *testing.T) {
		_, err := readIdentity(write(t, `{not json`))
		if err == nil || errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want a parse error distinct from ErrNoIdentity", err)
		}
	})

	t.Run("corrupt oauthAccount value is a real error", func(t *testing.T) {
		_, err := readIdentity(write(t, `{"oauthAccount": [1, 2]}`))
		if err == nil || errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want a parse error distinct from ErrNoIdentity", err)
		}
	})
}
