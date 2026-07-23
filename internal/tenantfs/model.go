// Package tenantfs binds cc-pool account policy to FuseKit's tenant APIs.
package tenantfs

import (
	"errors"
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
	BackingRoot             string
	FileProviderDisplayName string
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
	if !exactAbsolutePath(a.BackingRoot) {
		return mountproto.TenantDefinition{}, errors.New("tenantfs: backing root must be a clean absolute path")
	}
	if a.FileProviderDisplayName == "" || strings.ContainsRune(a.FileProviderDisplayName, 0) {
		return mountproto.TenantDefinition{}, errors.New("tenantfs: File Provider display name is invalid")
	}
	definition := mountproto.TenantDefinition{
		BackingRoot:                        a.BackingRoot,
		ContentSourceID:                    string(ClaudeAuthorityID),
		AccessMode:                         mountproto.AccessModeReadWrite,
		CasePolicy:                         mountproto.CasePolicyInsensitive,
		Presentations:                      []mountproto.Presentation{mountproto.PresentationFileProvider},
		Generation:                         a.Generation,
		FileProviderPresentationInstanceID: a.InstanceID,
		FileProviderDisplayName:            a.FileProviderDisplayName,
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
