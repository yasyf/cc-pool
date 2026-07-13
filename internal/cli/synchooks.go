package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/synckit/hostregistry"
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

// lifecycleSyncService builds the flock-backed registry service the CLI hooks
// mutate — plain file ops, no daemon dependency.
func lifecycleSyncService(m *pool.Manager) *hostsync.Service {
	rf := syncRegistryFile()
	return &hostsync.Service{
		M:        m,
		Registry: &rf,
		StampDir: pool.SyncStampsDir(),
	}
}

// syncHookSelf resolves this host's holder identity for a lifecycle publish —
// the same resolution `ccp sync enable` applies (syncSelf).
func syncHookSelf(w io.Writer) string {
	mesh, err := hostregistry.Mesh.Load()
	if err != nil {
		warn(w, "mesh state unreadable: %v", err)
		mesh = &hostregistry.Registry{}
	}
	return syncSelf(w, mesh)
}

// syncNudge best-effort asks synckitd to re-watch a new account's stamp dir; a
// failure warns — the reconcile tick still propagates the change.
func syncNudge(ctx context.Context, w io.Writer) {
	path, err := hostsync.ManifestPath()
	if err != nil {
		warn(w, "resolve synckit manifest path: %v", err)
		return
	}
	if err := synckitdRun(ctx, "register", path); err != nil {
		warn(w, "synckitd register nudge failed (%v); peers hear about the change on the next reconcile tick", err)
	}
}

// accountSyncUUID resolves a's registry key: the tagged row uuid, else the
// on-disk identity.
func accountSyncUUID(a store.Account) string {
	if a.AccountUUID != "" {
		return a.AccountUUID
	}
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return ""
	}
	id, err := pool.AccountIdentity(backend, a.ConfigDir)
	if err != nil {
		return ""
	}
	return id.AccountUUID
}

// syncPublisher publishes one account's full AccountValue and uuid-tags its
// row; fields are seams so tests pin the publish-before-tag order.
type syncPublisher struct {
	svc      *hostsync.Service
	self     string
	readCred func(store.Account) (*creds.Credential, creds.Source, error)
	setUUID  func(id int, uuid string) error
	now      func() time.Time
}

func newSyncPublisher(m *pool.Manager, self string) *syncPublisher {
	return &syncPublisher{
		svc:      lifecycleSyncService(m),
		self:     self,
		readCred: m.ReadCredential,
		setUUID:  m.Store.SetAccountUUID,
		now:      time.Now,
	}
}

// Publish force-publishes a — the explicit add/relogin intent that overrides
// tombstones — then uuid-tags the row. The publish runs strictly first so a
// concurrent teardown's post-claim re-check always sees the re-add — see ccn 10bf17d.
func (p *syncPublisher) Publish(ctx context.Context, a store.Account) error {
	backend, err := fkoverlay.Parse(a.OverlayKind)
	if err != nil {
		return fmt.Errorf("parse acct-%02d overlay backend: %w", a.ID, err)
	}
	raw, ident, err := pool.AccountOAuth(backend, a.ConfigDir)
	if err != nil {
		return fmt.Errorf("read acct-%02d identity: %w", a.ID, err)
	}
	cred, _, err := p.readCred(a)
	if err != nil {
		return fmt.Errorf("read acct-%02d credential: %w", a.ID, err)
	}
	v := hostsync.AccountValue{
		UUID:         ident.AccountUUID,
		Email:        ident.EmailAddress,
		Label:        a.Label,
		OAuthAccount: raw,
		// TODO(phase-3): AccessHash — build the v2 stamp {Origin: p.self,
		// ExpiresAt, Hash: creds.AccessHash(cred), RotatedAt} and delete
		// currentLease (schema v2 has no holder/lease).
		Chain: hostsync.ChainStamp{
			ExpiresAt: cred.ClaudeAiOauth.ExpiresAt,
			Hash:      hostsync.CredentialHash(cred),
			Holder:    p.self,
			RotatedAt: p.now().UnixMilli(),
		},
		Lease: currentLease(p.svc, ident.AccountUUID),
	}
	if err := p.svc.PublishAccount(ctx, v); err != nil {
		return fmt.Errorf("publish acct-%02d: %w", a.ID, err)
	}
	if a.AccountUUID != ident.AccountUUID {
		if err := p.setUUID(a.ID, ident.AccountUUID); err != nil {
			return fmt.Errorf("tag acct-%02d with uuid %s: %w", a.ID, ident.AccountUUID, err)
		}
	}
	return nil
}

// currentLease preserves a live registry lease across a publish — the publish
// rewrites the whole value and must not clear a peer's lease. A load failure
// reads as no lease; PublishAccount itself fails loud on real corruption.
func currentLease(svc *hostsync.Service, uuid string) *hostsync.Lease {
	reg, err := svc.Registry.Load()
	if err != nil {
		return nil
	}
	e, ok := reg[uuid]
	if !ok || !e.Present() {
		return nil
	}
	return e.Value.Lease
}

// syncPublishAccount publishes account id to the shared registry when host
// sync is enabled — the explicit re-add intent of `ccp add` and `ccp login`.
// Callers own failure reporting: the local operation already succeeded.
func syncPublishAccount(cmd *cobra.Command, m *pool.Manager, id int) error {
	return syncPublishAccountIO(cmd.Context(), cmd.ErrOrStderr(), m, id)
}

// syncPublishAccountIO is the cmd-free publish; errw carries the
// self-resolution and nudge warnings.
func syncPublishAccountIO(ctx context.Context, errw io.Writer, m *pool.Manager, id int) error {
	on, err := syncHooksEnabled(m)
	if err != nil || !on {
		return err
	}
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	p := newSyncPublisher(m, syncHookSelf(errw))
	if err := p.Publish(ctx, a); err != nil {
		return err
	}
	syncNudge(ctx, errw)
	return nil
}

// syncRecordRemoval tombstones account id pool-wide, BEFORE the local teardown
// (the identity is still readable; a peer converging mid-removal sees the
// tombstone). A recording failure aborts the removal — see ccn 10bf17d.
func syncRecordRemoval(cmd *cobra.Command, m *pool.Manager, id int) error {
	on, err := syncHooksEnabled(m)
	if err != nil || !on {
		return err
	}
	a, err := m.Store.GetAccount(id)
	if err != nil {
		return err
	}
	uuid := accountSyncUUID(a)
	if uuid == "" {
		warn(cmd.ErrOrStderr(), "acct-%02d has no sync identity; removing locally only — peer hosts keep their copy", id)
		return nil
	}
	if err := lifecycleSyncService(m).RecordRemoval(cmd.Context(), uuid); err != nil {
		return fmt.Errorf("record pool-wide removal of acct-%02d: %w", id, err)
	}
	return nil
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
	uuid := accountSyncUUID(a)
	if uuid == "" {
		return
	}
	if err := lifecycleSyncService(m).RecordLabel(cmd.Context(), uuid, label); err != nil {
		warn(cmd.ErrOrStderr(), "renamed locally, but couldn't record it in the sync registry: %v — a peer converge may revert it", err)
	}
}
