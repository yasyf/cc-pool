// Package tenantfs binds cc-pool account policy to FuseKit's tenant APIs.
package tenantfs

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/tenant"
)

const (
	// OwnerID is cc-pool's stable FuseKit owner identity.
	OwnerID catalogproto.OwnerID = "com.yasyf.cc-pool"
)

const accountTenantPrefix = "account-"

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
	return catalog.NewTenantID(accountTenantPrefix + a.InstanceID)
}

// Spec returns the exact durable File Provider-only FuseKit tenant.
func (a Account) Spec() (tenant.TenantSpec, error) {
	id, err := a.TenantID()
	if err != nil {
		return tenant.TenantSpec{}, err
	}
	if a.Generation == 0 {
		return tenant.TenantSpec{}, errors.New("tenantfs: account generation is zero")
	}
	if !exactAbsolutePath(a.BackingRoot) {
		return tenant.TenantSpec{}, errors.New("tenantfs: backing root must be a clean absolute path")
	}
	if a.FileProviderDisplayName == "" || strings.ContainsRune(a.FileProviderDisplayName, 0) {
		return tenant.TenantSpec{}, errors.New("tenantfs: File Provider display name is invalid")
	}
	return tenant.TenantSpec{
		OwnerID: tenant.OwnerID(OwnerID), ID: id,
		Backing: tenant.BackingSpec{Root: a.BackingRoot},
		Content: tenant.ContentSource{ID: string(ClaudeAuthorityID)},
		Traits: tenant.TenantTraits{
			Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseInsensitive,
			Presentations: catalog.PresentFileProvider,
		},
		FileProvider: tenant.FileProviderSpec{
			Enabled: true, PresentationInstanceID: a.InstanceID,
			DisplayName: a.FileProviderDisplayName,
		},
		Generation: catalog.Generation(a.Generation),
	}, nil
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
