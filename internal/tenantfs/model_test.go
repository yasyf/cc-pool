package tenantfs

import (
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/mountproto"
)

const testInstanceID = "0123456789abcdef0123456789abcdef"

func TestAccountDefinitionUsesImmutablePathFreeIdentity(t *testing.T) {
	root := t.TempDir()
	account := Account{
		InstanceID: testInstanceID, Generation: 7,
		BackingRoot:             filepath.Join(root, "backing", "account"),
		FileProviderDisplayName: "acct-18",
	}
	tenant, err := account.TenantID()
	if err != nil || tenant != "account-"+testInstanceID {
		t.Fatalf("TenantID = %q, %v", tenant, err)
	}
	definition, err := account.Definition()
	if err != nil {
		t.Fatalf("Definition: %v", err)
	}
	if definition.ContentSourceID != string(ClaudeAuthorityID) || definition.Generation != 7 ||
		definition.AccessMode != mountproto.AccessModeReadWrite || definition.CasePolicy != mountproto.CasePolicyInsensitive ||
		definition.FileProviderPresentationInstanceID != testInstanceID || definition.FileProviderDisplayName != "acct-18" {
		t.Fatalf("Definition = %+v", definition)
	}
	if len(definition.Presentations) != 1 || definition.Presentations[0] != mountproto.PresentationFileProvider {
		t.Fatalf("sorted presentations = %v", definition.Presentations)
	}
	if err := mountproto.Validate(definition); err != nil {
		t.Fatalf("definition does not satisfy exact FuseKit wire contract: %v", err)
	}
	replacement := account
	replacement.Generation++
	replacementDefinition, err := replacement.Definition()
	if err != nil {
		t.Fatalf("replacement Definition: %v", err)
	}
	if replacementDefinition.Generation != 8 ||
		replacementDefinition.FileProviderPresentationInstanceID != definition.FileProviderPresentationInstanceID ||
		replacementDefinition.FileProviderDisplayName != definition.FileProviderDisplayName {
		t.Fatalf("replacement definition changed File Provider identity: %+v", replacementDefinition)
	}
	if err := mountproto.Validate(mountproto.ReplaceTenantRequest{
		Protocol: mountproto.Version, ExpectedGeneration: definition.Generation,
		Definition: replacementDefinition,
	}); err != nil {
		t.Fatalf("replacement does not satisfy exact FuseKit wire contract: %v", err)
	}
}

func TestAccountDefinitionRequiresExactFileProviderMetadata(t *testing.T) {
	root := t.TempDir()
	account := Account{
		InstanceID: testInstanceID, Generation: 1,
		BackingRoot: filepath.Join(root, "backing"),
	}
	if _, err := account.Definition(); err == nil {
		t.Fatal("Definition accepted an empty File Provider display name")
	}
	account.FileProviderDisplayName = "acct-18\x00suffix"
	if _, err := account.Definition(); err == nil {
		t.Fatal("Definition accepted a File Provider display name containing NUL")
	}
}
