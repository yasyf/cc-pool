package pool

import (
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountproto"
)

func TestTenantAccountSeparatesPreservedBackingFromPresentation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	account := store.Account{
		ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
		ConfigDir: filepath.Join(home, "Library", "CloudStorage", "acct-18"),
	}
	tenant := TenantAccount(account)
	backing := filepath.Join(home, ".cc-pool", "fusekit", "backing", "acct-18")
	if tenant.BackingRoot != backing {
		t.Fatalf("backing root = %q, want %q", tenant.BackingRoot, backing)
	}
	if tenant.FileProviderDisplayName != "acct-18" {
		t.Fatalf("File Provider display name = %q", tenant.FileProviderDisplayName)
	}
	definition, err := tenant.Spec()
	if err != nil {
		t.Fatal(err)
	}
	if definition.BackingRoot != backing || definition.Generation != 7 ||
		definition.FileProviderPresentationInstanceID != account.InstanceID || definition.FileProviderDisplayName != "acct-18" {
		t.Fatalf("definition = %+v", definition)
	}
	if len(definition.Presentations) != 1 || definition.Presentations[0] != mountproto.PresentationFileProvider {
		t.Fatalf("definition presentations = %v", definition.Presentations)
	}
}
