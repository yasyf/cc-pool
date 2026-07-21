package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/synckit/cregistry"
)

// registryPerm keeps the registry file private, like the rest of the state dir.
const registryPerm = 0o600

// RegistryFile is the on-disk account Registry plus the flock that serializes
// read-modify-write cycles across processes.
type RegistryFile struct {
	// Path is the secretless registry.json.
	Path string
	// LockPath is the advisory-lock file guarding Update cycles.
	LockPath string
}

// NewRegistryFile is the registry layout under dir — registry.json beside its
// registry.lock — shared by the daemon and the ccp sync CLI.
func NewRegistryFile(dir string) *RegistryFile {
	return &RegistryFile{
		Path:     filepath.Join(dir, "registry.json"),
		LockPath: filepath.Join(dir, "registry.lock"),
	}
}

// Load reads the registry: a missing file is a fresh empty registry, a
// malformed one a loud error — never a silent reset. It decodes into the typed
// registry so the int64 Micros stamps survive byte-exact.
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

// Save writes the registry atomically, as a no-op when the marshaled bytes
// match disk — a pass that changes nothing never churns the watched file.
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
	if err := overlay.WriteAtomic0600(rf.Path, data); err != nil {
		return fmt.Errorf("write registry %s: %w", rf.Path, err)
	}
	return nil
}

// WithLock runs fn under the exclusive registry flock; the signature matches
// converge.Reconcile's lock parameter.
func (rf RegistryFile) WithLock(ctx context.Context, fn func() error) error {
	h, err := (proc.FileLockSpec{
		Path:     rf.LockPath,
		Mode:     proc.FileLockExclusive,
		Deadline: 30 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire registry lock: %w", err)
	}
	defer func() { _ = h.Close() }()
	return fn()
}

// Update is the atomic read-modify-write cycle: load, apply fn in place, save,
// all under the flock; an error from fn aborts before any write.
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
