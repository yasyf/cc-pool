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
		PresentationRoot:        filepath.Join(root, "mount", "account"),
		BackingRoot:             filepath.Join(root, "backing", "account"),
		FileProviderDisplayName: "acct-18",
		Presentations: []mountproto.Presentation{
			mountproto.PresentationFileProvider, mountproto.PresentationMount,
		},
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
		definition.FileProviderAccountID != testInstanceID || definition.FileProviderDisplayName != "acct-18" {
		t.Fatalf("Definition = %+v", definition)
	}
	if len(definition.Presentations) != 2 || definition.Presentations[0] != mountproto.PresentationMount ||
		definition.Presentations[1] != mountproto.PresentationFileProvider {
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
		replacementDefinition.FileProviderAccountID != definition.FileProviderAccountID ||
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

func TestAccountDefinitionRequiresExactFileProviderMetadataParity(t *testing.T) {
	root := t.TempDir()
	account := Account{
		InstanceID: testInstanceID, Generation: 1,
		PresentationRoot: filepath.Join(root, "mount"), BackingRoot: filepath.Join(root, "backing"),
		Presentations: []mountproto.Presentation{mountproto.PresentationFileProvider},
	}
	if _, err := account.Definition(); err == nil {
		t.Fatal("Definition accepted File Provider presentation without display metadata")
	}
	account.Presentations = []mountproto.Presentation{mountproto.PresentationMount}
	account.FileProviderDisplayName = "acct-18"
	if _, err := account.Definition(); err == nil {
		t.Fatal("Definition accepted File Provider metadata without presentation")
	}
}
