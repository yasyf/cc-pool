package pool

import (
	"os"
	"path/filepath"
	"testing"

	fkoverlay "github.com/yasyf/fusekit/overlay"

	"github.com/yasyf/cc-pool/internal/store"
)

type recordingOverlay struct {
	stubOverlay
	synced bool
}

func (r *recordingOverlay) Sync(_, _ string) error { r.synced = true; return nil }

// TestSyncOverlayRemovedRow pins the remove-vs-poll race guard (ccn 4db70ca0):
// a row deleted after the poll's account-list read must not be re-created by
// SyncOverlay; a live row still syncs.
func TestSyncOverlayRemovedRow(t *testing.T) {
	tests := map[string]struct {
		insert   bool
		wantSync bool
	}{
		"removed row skips sync":  {insert: false, wantSync: false},
		"live row syncs normally": {insert: true, wantSync: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			st := openTestStore(t)
			dir := filepath.Join(t.TempDir(), "acct-07")
			a := store.Account{ID: 7, ConfigDir: dir, OverlayKind: "symlink"}
			if tc.insert {
				if err := st.UpsertAccount(a); err != nil {
					t.Fatal(err)
				}
			}
			prov := &recordingOverlay{stubOverlay: stubOverlay{backend: fkoverlay.BackendSymlink}}
			m := &Manager{Store: st, OverlayFor: func(fkoverlay.Backend) (fkoverlay.Provider, error) { return prov, nil }}
			if err := m.SyncOverlay(a); err != nil {
				t.Fatalf("SyncOverlay = %v, want nil", err)
			}
			if prov.synced != tc.wantSync {
				t.Fatalf("synced = %v, want %v", prov.synced, tc.wantSync)
			}
			if !tc.insert {
				if _, err := os.Stat(dir); !os.IsNotExist(err) {
					t.Fatalf("config dir exists after a removed-row sync (stat err = %v)", err)
				}
			}
		})
	}
}
