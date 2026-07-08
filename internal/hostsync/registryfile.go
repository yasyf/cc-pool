package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/yasyf/fusekit/state"
	"github.com/yasyf/synckit/cregistry"
)

// registryPerm is the mode of the secretless registry file. 0600 by intent —
// the registry carries no tokens, but the pool's whole state dir is private.
const registryPerm = 0o600

// RegistryFile is the on-disk home of the account Registry plus the flock that
// serializes read-modify-write cycles across processes. Both fields are plain
// paths, so the zero-plus-paths value is safe to copy.
type RegistryFile struct {
	// Path is the secretless registry.json.
	Path string
	// LockPath is the advisory-lock file guarding Update cycles.
	LockPath string
}

// Load reads the registry off disk. A missing file yields a fresh empty
// registry — the first-run case, not an error. A file that exists but does not
// parse is a loud error: the registry is never silently reset, since that would
// resurrect tombstoned accounts and lose removals.
//
// It decodes into the typed registry (never map[string]any) so the int64 Micros
// stamps survive byte-exact — a float64 detour would corrupt stamps past 2^53.
func (rf RegistryFile) Load() (Registry, error) {
	data, err := os.ReadFile(rf.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return cregistry.New[AccountValue](), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read registry %s: %w", rf.Path, err)
	}
	reg := cregistry.New[AccountValue]()
	if err := json.Unmarshal(data, &reg); err != nil {
		return nil, fmt.Errorf("parse registry %s: %w", rf.Path, err)
	}
	return reg, nil
}

// Save writes the registry atomically (temp + rename via fusekit state). It is a
// no-op when the marshaled bytes equal what is already on disk: the file — its
// bytes AND its inode/mtime — is left untouched, so a converge pass that changes
// nothing does not churn the watched file and re-trigger the mesh.
func (rf RegistryFile) Save(reg Registry) error {
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal registry: %w", err)
	}
	existing, err := os.ReadFile(rf.Path)
	switch {
	case err == nil:
		if bytes.Equal(existing, data) {
			return nil
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("read registry %s: %w", rf.Path, err)
	}
	if err := state.AtomicWrite(rf.Path, data, registryPerm); err != nil {
		return fmt.Errorf("write registry %s: %w", rf.Path, err)
	}
	return nil
}

// WithLock runs fn while holding the exclusive registry flock, releasing it on
// return. Its signature matches converge.Reconcile's lock parameter, so a
// RegistryFile doubles as the reconcile serializer.
func (rf RegistryFile) WithLock(ctx context.Context, fn func() error) error {
	h, err := flockAcquire(ctx, rf.LockPath)
	if err != nil {
		return fmt.Errorf("acquire registry lock: %w", err)
	}
	defer h.release()
	return fn()
}

// Update is the atomic read-modify-write cycle: under the flock it loads the
// registry, applies fn, and saves. fn mutates the passed registry in place
// (cregistry Add/Remove); an error from fn aborts before any write.
func (rf RegistryFile) Update(ctx context.Context, fn func(Registry) error) error {
	return rf.WithLock(ctx, func() error {
		reg, err := rf.Load()
		if err != nil {
			return err
		}
		if err := fn(reg); err != nil {
			return err
		}
		return rf.Save(reg)
	})
}
