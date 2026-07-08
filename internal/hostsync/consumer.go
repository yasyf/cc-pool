package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
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

// Capabilities advertises the sync contract plus the ccp.fetch_credential method.
func (c *Consumer) Capabilities(_ context.Context) (syncservice.Capabilities, error) {
	if err := c.gate(); err != nil {
		return syncservice.Capabilities{}, err
	}
	caps := syncservice.DefaultCapabilities("cc-pool")
	caps.Methods = append(caps.Methods, MethodFetchCredential)
	return caps, nil
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

// Sync runs the same converge pass as Reconcile; the two contract methods coincide.
func (c *Consumer) Sync(ctx context.Context, origin string) (syncservice.SyncResult, error) {
	if err := c.gate(); err != nil {
		return syncservice.SyncResult{}, err
	}
	res, err := c.S.Converge(ctx, origin)
	if err != nil {
		return syncservice.SyncResult{}, err
	}
	return syncservice.SyncResult{Converged: res.Converged, SkippedBusy: res.SkippedBusy}, nil
}

// GetState returns the registry's raw JSON — secretless — read verbatim so the
// int64 stamps survive byte-exact; a not-yet-created registry answers empty.
func (c *Consumer) GetState(_ context.Context) (syncservice.RawRegistry, error) {
	if err := c.gate(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(c.S.Registry.Path)
	if errors.Is(err, fs.ErrNotExist) {
		empty, mErr := json.Marshal(cregistry.New[AccountValue]())
		if mErr != nil {
			return nil, fmt.Errorf("hostsync: marshal empty registry: %w", mErr)
		}
		return empty, nil
	}
	if err != nil {
		return nil, fmt.Errorf("hostsync: read registry %s: %w", c.S.Registry.Path, err)
	}
	return data, nil
}

// Consumer satisfies the synckit sync contract.
var _ syncservice.SyncConsumer = (*Consumer)(nil)
