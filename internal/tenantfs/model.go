// Package tenantfs binds cc-pool account policy to FuseKit's tenant APIs.
package tenantfs

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
)

const (
	// OwnerID is cc-pool's stable FuseKit owner identity.
	OwnerID catalogproto.OwnerID = "com.yasyf.cc-pool"
)

// Account identifies one immutable account tenant generation.
type Account struct {
	InstanceID              string
	Generation              uint64
	PresentationRoot        string
	BackingRoot             string
	FileProviderDisplayName string
	Presentations           []mountproto.Presentation
}

// TenantID returns the stable path-free tenant identity for an account instance.
func (a Account) TenantID() (catalog.TenantID, error) {
	if !validAccountInstanceID(a.InstanceID) {
		return "", errors.New("tenantfs: account instance id is not 32 lowercase hexadecimal characters")
	}
	return catalog.NewTenantID("account-" + a.InstanceID)
}

// Definition returns the exact durable FuseKit tenant definition.
func (a Account) Definition() (mountproto.TenantDefinition, error) {
	if _, err := a.TenantID(); err != nil {
		return mountproto.TenantDefinition{}, err
	}
	if a.Generation == 0 {
		return mountproto.TenantDefinition{}, errors.New("tenantfs: account generation is zero")
	}
	if !exactAbsolutePath(a.PresentationRoot) || !exactAbsolutePath(a.BackingRoot) {
		return mountproto.TenantDefinition{}, errors.New("tenantfs: tenant roots must be clean absolute paths")
	}
	if len(a.Presentations) == 0 || hasDuplicate(a.Presentations) {
		return mountproto.TenantDefinition{}, errors.New("tenantfs: presentations are empty or duplicated")
	}
	presentations := make([]mountproto.Presentation, 0, len(a.Presentations))
	mount := false
	fileProvider := false
	for _, presentation := range a.Presentations {
		switch presentation {
		case mountproto.PresentationMount:
			mount = true
		case mountproto.PresentationFileProvider:
			fileProvider = true
		default:
			return mountproto.TenantDefinition{}, fmt.Errorf("tenantfs: unknown presentation %q", presentation)
		}
	}
	if mount {
		presentations = append(presentations, mountproto.PresentationMount)
	}
	if fileProvider {
		presentations = append(presentations, mountproto.PresentationFileProvider)
	}
	if fileProvider != (a.FileProviderDisplayName != "") || strings.ContainsRune(a.FileProviderDisplayName, 0) {
		return mountproto.TenantDefinition{}, errors.New("tenantfs: File Provider metadata does not match presentation set")
	}
	definition := mountproto.TenantDefinition{
		PresentationRoot: a.PresentationRoot,
		BackingRoot:      a.BackingRoot,
		ContentSourceID:  string(ClaudeAuthorityID),
		AccessMode:       mountproto.AccessModeReadWrite,
		CasePolicy:       mountproto.CasePolicyInsensitive,
		Presentations:    presentations,
		Generation:       a.Generation,
	}
	if fileProvider {
		definition.FileProviderAccountID = a.InstanceID
		definition.FileProviderDisplayName = a.FileProviderDisplayName
	}
	return definition, nil
}

func validAccountInstanceID(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func exactAbsolutePath(value string) bool {
	return value != "" && filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, 0)
}

func hasDuplicate[T comparable](values []T) bool {
	seen := make(map[T]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}
