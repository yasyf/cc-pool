package pool

import (
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/testhome"
	"github.com/yasyf/fusekit/catalog"
)

func TestTenantAccountSeparatesPreservedBackingFromPresentation(t *testing.T) {
	home := t.TempDir()
	testhome.Sandbox(t, home)
	account := store.Account{
		ID: 18, InstanceID: "0123456789abcdef0123456789abcdef", Generation: 7,
	}
	account.ConfigDir, _ = AccountConfigDir(account.InstanceID)
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
	if definition.Backing.Root != backing || definition.Generation != 7 ||
		!definition.FileProvider.Enabled ||
		definition.FileProvider.PresentationInstanceID != account.InstanceID ||
		definition.FileProvider.DisplayName != "acct-18" {
		t.Fatalf("definition = %+v", definition)
	}
	if definition.Traits.Presentations != catalog.PresentFileProvider {
		t.Fatalf("definition presentations = %v", definition.Traits.Presentations)
	}
}
