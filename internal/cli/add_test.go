package cli

import (
	"os"
	"path/filepath"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func TestDefaultLabel(t *testing.T) {
	withIdentity := func(t *testing.T, oauthJSON string) string {
		t.Helper()
		dir := t.TempDir()
		if oauthJSON != "" {
			body := `{"oauthAccount": ` + oauthJSON + `}`
			if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}

	t.Run("explicit label wins over the account email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me@example.com"}`)
		if got := defaultLabel("work", fkoverlay.BackendSymlink, dir); got != "work" {
			t.Errorf("defaultLabel = %q, want the explicit %q", got, "work")
		}
	})

	t.Run("empty label prefills a name derived from an org email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me@example.com"}`)
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "Example" {
			t.Errorf("defaultLabel = %q, want %q", got, "Example")
		}
	})

	t.Run("empty label prefills the local part of a consumer email", func(t *testing.T) {
		dir := withIdentity(t, `{"accountUuid": "u-1", "emailAddress": "me@gmail.com"}`)
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "me" {
			t.Errorf("defaultLabel = %q, want %q", got, "me")
		}
	})

	t.Run("unreadable identity stays empty", func(t *testing.T) {
		dir := withIdentity(t, "")
		if got := defaultLabel("", fkoverlay.BackendSymlink, dir); got != "" {
			t.Errorf("defaultLabel = %q, want empty", got)
		}
	})
}
