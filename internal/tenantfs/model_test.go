package tenantfs

import (
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

const testInstanceID = "0123456789abcdef0123456789abcdef"

func TestAccountSpecUsesImmutablePathFreeIdentity(t *testing.T) {
	root := t.TempDir()
	account := Account{
		InstanceID: testInstanceID, Generation: 7,
		BackingRoot:             filepath.Join(root, "backing", "account"),
		FileProviderDisplayName: "acct-18",
	}
	tenantID, err := account.TenantID()
	if err != nil || tenantID != "account-"+testInstanceID {
		t.Fatalf("TenantID = %q, %v", tenantID, err)
	}
	spec, err := account.Spec()
	if err != nil {
		t.Fatalf("Spec: %v", err)
	}
	if spec.OwnerID != tenant.OwnerID(OwnerID) || spec.ID != tenantID ||
		spec.Content.ID != string(ClaudeAuthorityID) || spec.Generation != 7 ||
		spec.Traits.Access != tenant.ReadWrite || spec.Traits.CaseSensitivity != catalog.CaseInsensitive ||
		spec.Traits.Presentations != catalog.PresentFileProvider || spec.Mount.PresentationRoot != "" ||
		!spec.FileProvider.Enabled || spec.FileProvider.PresentationInstanceID != testInstanceID ||
		spec.FileProvider.DisplayName != "acct-18" {
		t.Fatalf("Spec = %+v", spec)
	}
	replacement := account
	replacement.Generation++
	replacementSpec, err := replacement.Spec()
	if err != nil {
		t.Fatalf("replacement Spec: %v", err)
	}
	if replacementSpec.Generation != 8 || replacementSpec.FileProvider != spec.FileProvider ||
		replacementSpec.ID != spec.ID || replacementSpec.OwnerID != spec.OwnerID {
		t.Fatalf("replacement spec changed File Provider identity: %+v", replacementSpec)
	}
}

func TestAccountSpecRequiresExactFileProviderMetadata(t *testing.T) {
	root := t.TempDir()
	account := Account{
		InstanceID: testInstanceID, Generation: 1,
		BackingRoot: filepath.Join(root, "backing"),
	}
	if _, err := account.Spec(); err == nil {
		t.Fatal("Spec accepted an empty File Provider display name")
	}
	account.FileProviderDisplayName = "acct-18\x00suffix"
	if _, err := account.Spec(); err == nil {
		t.Fatal("Spec accepted a File Provider display name containing NUL")
	}
}
