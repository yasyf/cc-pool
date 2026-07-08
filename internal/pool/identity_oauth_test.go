package pool

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// TestAccountOAuth pins the raw oauthAccount pass-through: the bytes a
// registry publish carries are exactly the bytes claude wrote, and every
// unusable shape stays ErrNoIdentity.
func TestAccountOAuth(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	t.Run("returns the verbatim oauthAccount and parsed identity", func(t *testing.T) {
		const oauth = `{"accountUuid":"u-1","emailAddress":"a@b.c","organizationUuid":"org-9"}`
		dir := write(t, `{"oauthAccount": `+oauth+`, "numStartups": 3}`)
		raw, id, err := AccountOAuth(fkoverlay.BackendSymlink, dir)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != oauth {
			t.Errorf("raw = %s, want the verbatim object %s", raw, oauth)
		}
		if id.AccountUUID != "u-1" || id.EmailAddress != "a@b.c" {
			t.Errorf("identity = %+v, want u-1 / a@b.c", id)
		}
	})

	t.Run("fuse backend reads the private backing root", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "acct-01")
		priv := fkoverlay.FusePrivateRoot(dir)
		for _, d := range []string{dir, priv} {
			if err := os.MkdirAll(d, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, ".claude.json"),
			[]byte(`{"oauthAccount":{"accountUuid":"u-decoy"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(priv, ".claude.json"),
			[]byte(`{"oauthAccount":{"accountUuid":"u-right"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		_, id, err := AccountOAuth(fkoverlay.BackendNFS, dir)
		if err != nil {
			t.Fatal(err)
		}
		if id.AccountUUID != "u-right" {
			t.Errorf("read %q, want u-right (the private root, never the account dir)", id.AccountUUID)
		}
	})

	t.Run("missing file is ErrNoIdentity", func(t *testing.T) {
		_, _, err := AccountOAuth(fkoverlay.BackendSymlink, t.TempDir())
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want ErrNoIdentity", err)
		}
	})

	t.Run("missing oauthAccount key is ErrNoIdentity", func(t *testing.T) {
		dir := write(t, `{"hasCompletedOnboarding": true}`)
		_, _, err := AccountOAuth(fkoverlay.BackendSymlink, dir)
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want ErrNoIdentity", err)
		}
	})

	t.Run("empty accountUuid is ErrNoIdentity", func(t *testing.T) {
		dir := write(t, `{"oauthAccount": {"accountUuid": "", "emailAddress": "x@y.z"}}`)
		_, _, err := AccountOAuth(fkoverlay.BackendSymlink, dir)
		if !errors.Is(err, ErrNoIdentity) {
			t.Fatalf("err = %v, want ErrNoIdentity", err)
		}
	})
}
