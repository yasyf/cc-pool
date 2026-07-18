package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

const (
	leaseProbeTimeout = 5 * time.Second
	leaseAcquireBound = 5 * time.Second
)

// leaseRoot resolves the fleet lease dir; tests replace it with a temp root.
var leaseRoot = lease.DefaultRoot

func boundedStat(dir string) error {
	done := make(chan error, 1)
	go func() {
		_, err := os.Stat(dir)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(leaseProbeTimeout):
		return fmt.Errorf("did not answer a stat within %s", leaseProbeTimeout)
	}
}

func probeLeasedDir(configDir string, fuseRow bool) error {
	if err := boundedStat(configDir); err != nil {
		return fmt.Errorf("%s is not answering (dead or absent mount?): %w — run `ccp doctor`", configDir, err)
	}
	switch err := overlay.DeepProbeWithin(configDir); {
	case err == nil:
		return nil
	case errors.Is(err, overlay.ErrProbeMissing):
		if fuseRow {
			return fmt.Errorf("%s is a fuse account but its mount serves no probe file (unmounted or stale mount?): %w — run `ccp doctor`", configDir, err)
		}
		return nil
	default:
		return fmt.Errorf("%s did not answer a full read (wedged mirror?): %w — run `ccp doctor`", configDir, err)
	}
}

func isFuseRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	return err == nil && b.IsFuse()
}

func isFPRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	return err == nil && b == fkoverlay.BackendFileProvider
}

func acquireLease(owner, key string) (*lease.Handle, error) {
	root, err := leaseRoot()
	if err != nil {
		return nil, fmt.Errorf("resolve the session lease root: %w", err)
	}
	h, err := lease.Acquire(root, key, owner)
	if err != nil {
		return nil, fmt.Errorf("take a session lease on %s: %w", key, err)
	}
	return h, nil
}

func acquireSessionLease(a store.Account) (*lease.Handle, error) {
	return acquireLease(pool.HolderOwner, pool.SessionLeaseDir(a))
}

func probeSessionLease(a store.Account) error {
	return probeLeasedDir(a.ConfigDir, isFuseRow(a.OverlayKind))
}

func acquireAndProbeSessionLease(a store.Account) (*lease.Handle, error) {
	return acquireAndProbe(pool.SessionLeaseDir(a), a.ConfigDir, isFuseRow(a.OverlayKind))
}

func acquireAndProbeSessionLeaseContext(ctx context.Context, a store.Account) (*lease.Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	const bound = leaseAcquireBound + leaseProbeTimeout + overlay.DeepProbeBound
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < bound {
		return nil, context.DeadlineExceeded
	}
	h, err := acquireAndProbeSessionLease(a)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

func acquireAndProbePendingLease(p *pool.PendingAdd) (*lease.Handle, error) {
	return acquireAndProbe(pool.SessionLeaseDirFor(p.Index, p.ConfigDir, string(p.OverlayKind)), p.ConfigDir, p.OverlayKind.IsFuse())
}

func acquireAndProbe(key, probeDir string, fuseRow bool) (*lease.Handle, error) {
	h, err := acquireLease(pool.HolderOwner, key)
	if err != nil {
		return nil, err
	}
	if err := probeLeasedDir(probeDir, fuseRow); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

func keepLeaseAlive(h *lease.Handle) { runtime.KeepAlive(h) }
