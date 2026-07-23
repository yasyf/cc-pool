package pool

import (
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/tenantfs"
)

// TenantAccount maps product account identity into the one FuseKit tenant contract.
func TenantAccount(account store.Account) tenantfs.Account {
	return tenantfs.Account{
		InstanceID: account.InstanceID, Generation: account.Generation,
		BackingRoot:             AccountBackingDir(account.ID),
		FileProviderDisplayName: AccountDirName(account.ID),
	}
}
