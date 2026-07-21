package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/syncservice"
)

// syncHooksEnabled reports whether the host-sync lifecycle hooks are on; the
// hooks in this file are inert without the syncMetaKey flag.
func syncHooksEnabled(m *pool.Manager) (bool, error) {
	v, ok, err := m.Store.GetMeta(syncMetaKey)
	if err != nil {
		return false, fmt.Errorf("read %s meta: %w", syncMetaKey, err)
	}
	return ok && v == "1", nil
}

// syncRecordLabel mirrors a local rename into the shared registry; the label
// is display-only, so failures warn instead of failing the rename.
func syncRecordLabel(cmd *cobra.Command, m *pool.Manager, a store.Account, label string) {
	on, err := syncHooksEnabled(m)
	if err != nil {
		warn(cmd.ErrOrStderr(), "check host-sync state: %v", err)
		return
	}
	if !on {
		return
	}
	uuid := a.AccountUUID
	if uuid == "" {
		return
	}
	if !syncEnsureDaemon(cmd.Context()) {
		warn(cmd.ErrOrStderr(), "renamed locally, but the daemon is unavailable; run `ccp service install` to converge host sync")
		return
	}
	cl := syncservice.NewClient(syncservice.Socket(pool.SyncSocketPath()))
	defer func() { _ = cl.Close() }()
	if _, err := cl.Sync(cmd.Context(), ""); err != nil {
		warn(cmd.ErrOrStderr(), "renamed locally, but couldn't record it in the sync registry: %v — a peer converge may revert it", err)
	}
}
