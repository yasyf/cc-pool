package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/cregistry"
)

// seedSyncRegistry applies mut to the on-disk shared registry under its flock.
func seedSyncRegistry(t *testing.T, mut func(reg hostsync.Registry)) {
	t.Helper()
	err := syncRegistryFile().Update(context.Background(), func(reg hostsync.Registry) error {
		mut(reg)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func loadSyncRegistry(t *testing.T) hostsync.Registry {
	t.Helper()
	reg, err := syncRegistryFile().Load()
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func mustNoSyncDir(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(pool.SyncDir()); !os.IsNotExist(err) {
		t.Fatalf("sync dir %s exists (stat err %v); disabled hooks must generate zero registry traffic", pool.SyncDir(), err)
	}
}

// TestSyncPublishAccount pins the `ccp add`/`ccp login` publish hook: the full
// AccountValue lands, the row is uuid-tagged strictly after the publish,
// tombstones are overridden, and a disabled pool sees zero registry traffic.
func TestSyncPublishAccount(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("publishes the full value, tags the row, stamps, and nudges", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		calls := stubSynckitdRun(t)
		writeMeshState(t, `{"self": "me@host-a", "hosts": ["me@host-b"]}`)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		c := testCred("at-1", future)
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", c)
		hash := hostsync.CredentialHash(c)
		if err := m.Store.SetChainHashes(a.ID, hash, "h-parent"); err != nil {
			t.Fatal(err)
		}
		cmd, _ := syncCmdBuf(t)

		if err := syncPublishAccount(cmd, m, a.ID); err != nil {
			t.Fatalf("syncPublishAccount: %v", err)
		}

		entry, ok := loadSyncRegistry(t)["u-1"]
		if !ok || !entry.Present() {
			t.Fatalf("entry = %+v, want present", entry)
		}
		v := entry.Value
		if v.UUID != "u-1" || v.Email != "e@x.y" || v.Label != "Work" {
			t.Errorf("value = %+v, want u-1/e@x.y/Work", v)
		}
		var oauth map[string]any
		if err := json.Unmarshal(v.OAuthAccount, &oauth); err != nil {
			t.Fatalf("parse published oauthAccount: %v", err)
		}
		if oauth["extra"] != "keep-me-1" {
			t.Errorf("oauthAccount = %s, want the verbatim object incl. extra keys", v.OAuthAccount)
		}
		if v.Chain.ExpiresAt != future || v.Chain.Hash != hash {
			t.Errorf("chain = %+v, want expiry %d hash %s", v.Chain, future, hash)
		}
		if v.Chain.Holder != "me@host-a" {
			t.Errorf("holder = %q, want the mesh self", v.Chain.Holder)
		}
		if v.Chain.ParentHash != "h-parent" {
			t.Errorf("parentHash = %q, want the stored lineage h-parent", v.Chain.ParentHash)
		}
		if v.Chain.RotatedAt <= 0 {
			t.Errorf("rotatedAt = %d, want set", v.Chain.RotatedAt)
		}
		row, err := m.Store.GetAccount(a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.AccountUUID != "u-1" {
			t.Errorf("row uuid = %q, want u-1", row.AccountUUID)
		}
		if _, err := os.Stat(filepath.Join(pool.SyncStampsDir(), "u-1", "stamp")); err != nil {
			t.Errorf("stamp not touched: %v", err)
		}
		path, err := hostsync.ManifestPath()
		if err != nil {
			t.Fatal(err)
		}
		if len(*calls) != 1 || (*calls)[0][0] != "register" || (*calls)[0][1] != path {
			t.Errorf("synckitd calls = %v, want one register of %s", *calls, path)
		}
	})

	t.Run("publish lands before the row is uuid-tagged", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		c := testCred("at-1", future)
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", c)
		tagged := 0
		p := &syncPublisher{
			svc:      lifecycleSyncService(m),
			self:     "host-a",
			readCred: m.ReadCredential,
			setUUID: func(_ int, uuid string) error {
				reg := loadSyncRegistry(t)
				if !reg[uuid].Present() {
					t.Error("row uuid-tagged before the registry publish landed")
				}
				tagged++
				return nil
			},
			now: time.Now,
		}
		if err := p.Publish(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		if tagged != 1 {
			t.Fatalf("setUUID calls = %d, want 1", tagged)
		}
	})

	t.Run("re-add overrides a peer tombstone", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		stubSynckitdRun(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		seedSyncRegistry(t, func(reg hostsync.Registry) {
			reg.Add("u-1", hostsync.AccountValue{UUID: "u-1"}, cregistry.Micros(1000))
			reg.Remove("u-1", cregistry.Micros(2000))
		})
		cmd, _ := syncCmdBuf(t)

		if err := syncPublishAccount(cmd, m, a.ID); err != nil {
			t.Fatal(err)
		}
		if entry := loadSyncRegistry(t)["u-1"]; !entry.Present() {
			t.Fatalf("entry = %+v, want the tombstone overridden by the explicit re-add", entry)
		}
	})

	t.Run("preserves a live peer lease", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		stubSynckitdRun(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		lease := &hostsync.Lease{Host: "me@host-b", Until: future}
		seedSyncRegistry(t, func(reg hostsync.Registry) {
			reg.Add("u-1", hostsync.AccountValue{UUID: "u-1", Lease: lease}, cregistry.Micros(1000))
		})
		cmd, _ := syncCmdBuf(t)

		if err := syncPublishAccount(cmd, m, a.ID); err != nil {
			t.Fatal(err)
		}
		got := loadSyncRegistry(t)["u-1"].Value.Lease
		if got == nil || got.Host != "me@host-b" || got.Until != future {
			t.Fatalf("lease = %+v, want the peer lease preserved", got)
		}
	})

	t.Run("drifted cred_hash becomes the chain parent", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		stubSynckitdRun(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		// claude rotated the chain since cc-pool last wrote: the stored hash is
		// the live chain's parent — the writeCred resolution.
		if err := m.Store.SetChainHashes(a.ID, "h-old", "h-old-parent"); err != nil {
			t.Fatal(err)
		}
		cmd, _ := syncCmdBuf(t)

		if err := syncPublishAccount(cmd, m, a.ID); err != nil {
			t.Fatal(err)
		}
		if got := loadSyncRegistry(t)["u-1"].Value.Chain.ParentHash; got != "h-old" {
			t.Errorf("parentHash = %q, want the drifted stored hash h-old", got)
		}
	})

	t.Run("disabled: zero registry traffic", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		calls := stubSynckitdRun(t)
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		cmd, _ := syncCmdBuf(t)

		if err := syncPublishAccount(cmd, m, a.ID); err != nil {
			t.Fatalf("disabled publish must be a silent no-op, got %v", err)
		}
		mustNoSyncDir(t)
		row, err := m.Store.GetAccount(a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.AccountUUID != "" {
			t.Errorf("row uuid = %q, want untagged", row.AccountUUID)
		}
		if len(*calls) != 0 {
			t.Errorf("synckitd calls = %v, want none", *calls)
		}
	})
}

// TestSyncRecordRemoval pins the `ccp remove` hook: the tombstone is recorded
// from pre-teardown state (row uuid or on-disk identity), a recording failure
// aborts, and a disabled pool sees zero registry traffic.
func TestSyncRecordRemoval(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("tombstones a tagged row", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		if err := m.Store.SetAccountUUID(a.ID, "u-1"); err != nil {
			t.Fatal(err)
		}
		seedSyncRegistry(t, func(reg hostsync.Registry) {
			reg.Add("u-1", hostsync.AccountValue{UUID: "u-1"}, cregistry.Micros(1000))
		})
		cmd, _ := syncCmdBuf(t)

		if err := syncRecordRemoval(cmd, m, a.ID); err != nil {
			t.Fatalf("syncRecordRemoval: %v", err)
		}
		if entry := loadSyncRegistry(t)["u-1"]; entry.Present() {
			t.Fatalf("entry = %+v, want tombstoned", entry)
		}
		// The hook only records; the caller owns the local teardown.
		if _, err := m.Store.GetAccount(a.ID); err != nil {
			t.Errorf("local row gone after the record step: %v", err)
		}
	})

	t.Run("resolves the uuid from disk when the row is untagged", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		cmd, _ := syncCmdBuf(t)

		if err := syncRecordRemoval(cmd, m, a.ID); err != nil {
			t.Fatal(err)
		}
		entry, ok := loadSyncRegistry(t)["u-1"]
		if !ok || entry.Present() || entry.Removed == 0 {
			t.Fatalf("entry = %+v, want a tombstone recorded under the on-disk identity", entry)
		}
	})

	t.Run("no identity: removes locally only", func(t *testing.T) {
		m, _ := syncTestEnv(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := store.Account{ID: 1, ConfigDir: t.TempDir(), OverlayKind: "symlink", KeychainService: "svc-1", KeychainAccount: "u"}
		if err := m.Store.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		cmd, out := syncCmdBuf(t)

		if err := syncRecordRemoval(cmd, m, a.ID); err != nil {
			t.Fatalf("want nil (local-only removal), got %v", err)
		}
		mustNoSyncDir(t)
		if !strings.Contains(stripANSI(out.String()), "removing locally only") {
			t.Errorf("output %q missing the local-only warning", out.String())
		}
	})

	t.Run("recording failure aborts the removal", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		if err := m.Store.SetAccountUUID(a.ID, "u-1"); err != nil {
			t.Fatal(err)
		}
		// A file where the sync dir belongs makes every registry write fail.
		if err := os.WriteFile(pool.SyncDir(), []byte("wedge"), 0o600); err != nil {
			t.Fatal(err)
		}
		cmd, _ := syncCmdBuf(t)

		if err := syncRecordRemoval(cmd, m, a.ID); err == nil {
			t.Fatal("want an error aborting the removal when the tombstone cannot be recorded")
		}
	})

	t.Run("disabled: zero registry traffic", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Work", testCred("at-1", future))
		cmd, _ := syncCmdBuf(t)

		if err := syncRecordRemoval(cmd, m, a.ID); err != nil {
			t.Fatal(err)
		}
		mustNoSyncDir(t)
	})
}

// TestSyncRecordLabelHook pins the `ccp rename` hook: a rename lands in the
// registry (through runRename end to end), an unknown registry account warns
// without failing the rename, and a disabled pool sees zero registry traffic.
func TestSyncRecordLabelHook(t *testing.T) {
	future := time.Now().Add(time.Hour).UnixMilli()

	t.Run("runRename records the label", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Old", testCred("at-1", future))
		if err := m.Store.SetAccountUUID(a.ID, "u-1"); err != nil {
			t.Fatal(err)
		}
		seedSyncRegistry(t, func(reg hostsync.Registry) {
			reg.Add("u-1", hostsync.AccountValue{UUID: "u-1", Label: "Old"}, cregistry.Micros(1000))
		})
		cmd, _ := syncCmdBuf(t)

		if err := runRename(cmd, m, []string{"1", "New"}, renameOptions{}); err != nil {
			t.Fatalf("runRename: %v", err)
		}
		entry := loadSyncRegistry(t)["u-1"]
		if !entry.Present() || entry.Value.Label != "New" {
			t.Fatalf("entry = %+v, want present with label New", entry)
		}
		if _, err := os.Stat(filepath.Join(pool.SyncStampsDir(), "u-1", "stamp")); err != nil {
			t.Errorf("stamp not touched: %v", err)
		}
	})

	t.Run("unknown registry account warns, rename stands", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
			t.Fatal(err)
		}
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Old", testCred("at-1", future))
		if err := m.Store.SetAccountUUID(a.ID, "u-1"); err != nil {
			t.Fatal(err)
		}
		cmd, out := syncCmdBuf(t)

		if err := runRename(cmd, m, []string{"1", "New"}, renameOptions{}); err != nil {
			t.Fatalf("a registry miss must not fail the rename: %v", err)
		}
		row, err := m.Store.GetAccount(a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.Label != "New" {
			t.Errorf("label = %q, want the local rename kept", row.Label)
		}
		if !strings.Contains(stripANSI(out.String()), "sync registry") {
			t.Errorf("output %q missing the registry warning", out.String())
		}
	})

	t.Run("disabled: zero registry traffic", func(t *testing.T) {
		m, fk := syncTestEnv(t)
		a := addSyncTestAccount(t, m, fk, 1, "u-1", "e@x.y", "Old", testCred("at-1", future))
		cmd, _ := syncCmdBuf(t)

		syncRecordLabel(cmd, m, a, "New")
		mustNoSyncDir(t)
	})
}
