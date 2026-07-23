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
	presentation := filepath.Join(home, ".cc-pool", "accounts", "acct-18")
	account := store.Account{
		ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
		ConfigDir: presentation,
	}
	tenant := TenantAccount(account)
	backing := filepath.Join(home, ".cc-pool", "fusekit", "backing", "acct-18")
	if tenant.BackingRoot != backing {
		t.Fatalf("backing root = %q, want %q", tenant.BackingRoot, backing)
	}
	if tenant.PresentationRoot != presentation {
		t.Fatalf("presentation root = %q", tenant.PresentationRoot)
	}
	if tenant.FileProviderDisplayName != "acct-18" {
		t.Fatalf("File Provider display name = %q", tenant.FileProviderDisplayName)
	}
	if len(tenant.Presentations) != 1 || tenant.Presentations[0] != mountproto.PresentationFileProvider {
		t.Fatalf("presentations = %v", tenant.Presentations)
	}
	definition, err := tenant.Definition()
	if err != nil {
		t.Fatal(err)
	}
	if definition.BackingRoot != backing || definition.PresentationRoot != presentation || definition.Generation != 7 ||
		definition.FileProviderAccountID != account.InstanceID || definition.FileProviderDisplayName != "acct-18" {
		t.Fatalf("definition = %+v", definition)
	}
	if len(definition.Presentations) != 1 || definition.Presentations[0] != mountproto.PresentationFileProvider {
		t.Fatalf("definition presentations = %v", definition.Presentations)
	}
}
