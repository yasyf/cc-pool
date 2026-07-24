package hostsync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/syncservice"
)

// ErrSyncDisabled is returned by every Consumer method while host sync is off.
var ErrSyncDisabled = errors.New("hostsync: sync is disabled")

// Consumer answers synckitd's typed RPC contract from the secretless registry;
// every method is gated by Enabled so a disabled pool fails loud.
type Consumer struct {
	// S is the host-sync service backing every method.
	S *Service
	// Enabled reports whether sync is on, consulted per call so enable needs no
	// restart; nil means always on.
	Enabled func() (bool, error)
}

// NewConsumer builds the SyncConsumer over svc, gated on enabled (nil ⇒ always on).
func NewConsumer(svc *Service, enabled func() (bool, error)) *Consumer {
	return &Consumer{S: svc, Enabled: enabled}
}

// gate returns nil when sync is enabled, ErrSyncDisabled otherwise.
func (c *Consumer) gate() error {
	if c.Enabled == nil {
		return nil
	}
	on, err := c.Enabled()
	if err != nil {
		return fmt.Errorf("hostsync: check sync enabled: %w", err)
	}
	if !on {
		return ErrSyncDisabled
	}
	return nil
}

// Capabilities advertises only Synckit's exact sync contract.
func (c *Consumer) Capabilities(_ context.Context) (syncservice.Capabilities, error) {
	if err := c.gate(); err != nil {
		return syncservice.Capabilities{}, err
	}
	return syncservice.DefaultCapabilities(SyncServiceID), nil
}

// List reports one WatchItem per registry account, tombstones included so
// their fingerprints keep terminating removal echoes; sorted by uuid.
func (c *Consumer) List(ctx context.Context) ([]syncservice.WatchItem, error) {
	if err := c.gate(); err != nil {
		return nil, err
	}
	reg, err := c.S.Registry.Load()
	if err != nil {
		return nil, fmt.Errorf("hostsync: load registry: %w", err)
	}
	items := make([]syncservice.WatchItem, 0, len(reg))
	for uuid, entry := range reg {
		busy, reason := c.listBusy(ctx, uuid)
		items = append(items, syncservice.WatchItem{
			ID:          uuid,
			WatchDirs:   []string{filepath.Join(c.S.StampDir, uuid)},
			Fingerprint: Fingerprint(entry),
			Busy:        busy,
			BusyReason:  reason,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

// listBusy fails OPEN — a nil Sessions seam or a Busy error reads not busy —
// because List is advisory watch metadata (teardown fails closed instead).
func (c *Consumer) listBusy(ctx context.Context, uuid string) (bool, string) {
	if c.S.Sessions == nil {
		return false, ""
	}
	busy, reason, err := c.S.Sessions.Busy(ctx, uuid)
	if err != nil {
		c.S.logf("hostsync: List busy check %s: %v", uuid, err)
		return false, ""
	}
	return busy, reason
}

// Reconcile runs one converge pass against origin and reports what it changed.
func (c *Consumer) Reconcile(ctx context.Context, origin string) (syncservice.ReconcileResult, error) {
	if err := c.gate(); err != nil {
		return syncservice.ReconcileResult{}, err
	}
	return c.S.Converge(ctx, origin)
}

// Export returns the immutable canonical snapshot at the product's exact local revision.
func (c *Consumer) Export(ctx context.Context, request syncservice.ExportRequest) (syncservice.ChangeEnvelope, error) {
	if err := c.gate(); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if err := request.Validate(); err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if request.ServiceID != SyncServiceID || request.SchemaFingerprint != SyncSchemaFingerprint {
		return syncservice.ChangeEnvelope{}, errors.New("hostsync: export service schema mismatch")
	}
	var state RegistryState
	err := c.S.Registry.WithLock(ctx, func() error {
		var err error
		state, err = c.S.Registry.LoadState()
		return err
	})
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	since, err := request.SinceRevision.Uint64()
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	if since > state.Revision {
		return syncservice.ChangeEnvelope{}, fmt.Errorf(
			"hostsync: export revision %d precedes acknowledged revision %d",
			state.Revision, since,
		)
	}
	credentials := make(map[string]CredentialEnvelope)
	if c.S.CredentialSnapshot != nil {
		credentials, err = c.S.CredentialSnapshot(ctx, state.Snapshot)
		if err != nil {
			return syncservice.ChangeEnvelope{}, err
		}
	}
	payload, err := encodeSyncSnapshot(syncSnapshot{
		Registry: state.Snapshot, Credentials: credentials,
	})
	if err != nil {
		return syncservice.ChangeEnvelope{}, err
	}
	return syncservice.NewExportedChange(
		SyncServiceID,
		SyncSchemaFingerprint,
		syncservice.ChangeSnapshot,
		syncservice.NewRevision(0),
		syncservice.NewRevision(state.Revision),
		payload,
	)
}

// Apply merges one delivery-bound snapshot, persists one local revision only
// for effective CRDT change, then reconciles product state before acknowledging.
func (c *Consumer) Apply(ctx context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
	if err := c.gate(); err != nil {
		return syncservice.ApplyResult{}, err
	}
	if err := change.Validate(true); err != nil {
		return syncservice.ApplyResult{}, err
	}
	if change.ServiceID != SyncServiceID || change.SchemaFingerprint != SyncSchemaFingerprint {
		return syncservice.ApplyResult{}, errors.New("hostsync: apply service schema mismatch")
	}
	if change.Kind != syncservice.ChangeSnapshot {
		return syncservice.ApplyResult{NeedSnapshot: true}, nil
	}
	snapshot, err := decodeSyncSnapshot(change.Payload)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	credentials, err := validateAppliedCredentials(snapshot, change.Origin)
	if err != nil {
		return syncservice.ApplyResult{}, err
	}
	if err := c.S.Registry.Update(ctx, func(local Registry) error {
		merged := cregistry.Merge(local, snapshot.Registry)
		clear(local)
		for id, entry := range merged {
			local[id] = entry
		}
		return nil
	}); err != nil {
		return syncservice.ApplyResult{}, fmt.Errorf("hostsync: persist applied snapshot: %w", err)
	}
	if _, err := c.S.Converge(withAppliedCredentials(ctx, credentials), change.Origin); err != nil {
		return syncservice.ApplyResult{}, err
	}
	return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
}

// Consumer satisfies the synckit sync contract.
var _ syncservice.SyncConsumer = (*Consumer)(nil)
