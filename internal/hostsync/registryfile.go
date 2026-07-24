package hostsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/synckit/cregistry"
)

// RegistryFile is the on-disk account Registry plus the flock that serializes
// read-modify-write cycles across processes.
type RegistryFile struct {
	// Path is the secretless registry.json.
	Path string
	// LockPath is the advisory-lock file guarding Update cycles.
	LockPath string
}

// RegistryState is the exact v1 persisted host-sync state.
type RegistryState struct {
	Revision uint64   `json:"revision"`
	Snapshot Registry `json:"snapshot"`
	Digest   string   `json:"digest"`
}

// NewRegistryFile is the registry layout under dir — registry.json beside its
// registry.lock — shared by the daemon and the ccp sync CLI.
func NewRegistryFile(dir string) *RegistryFile {
	return &RegistryFile{
		Path:     filepath.Join(dir, "registry.json"),
		LockPath: filepath.Join(dir, "registry.lock"),
	}
}

// LoadState reads the exact v1 state. A missing file is the immutable initial
// revision; every malformed, legacy, or digest-mismatched file fails closed.
func (rf RegistryFile) LoadState() (RegistryState, error) {
	data, err := os.ReadFile(rf.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return initialRegistryState()
	}
	if err != nil {
		return RegistryState{}, fmt.Errorf("read registry %s: %w", rf.Path, err)
	}
	var state RegistryState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return RegistryState{}, fmt.Errorf("parse registry %s: %w", rf.Path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RegistryState{}, fmt.Errorf("parse registry %s: trailing data", rf.Path)
	}
	if state.Revision == 0 || state.Snapshot == nil {
		return RegistryState{}, fmt.Errorf("parse registry %s: invalid v1 state", rf.Path)
	}
	_, digest, err := canonicalRegistry(state.Snapshot)
	if err != nil {
		return RegistryState{}, fmt.Errorf("parse registry %s: %w", rf.Path, err)
	}
	if state.Digest != digest {
		return RegistryState{}, fmt.Errorf("parse registry %s: snapshot digest mismatch", rf.Path)
	}
	return state, nil
}

// Load returns the typed CRDT snapshot from the exact v1 state.
func (rf RegistryFile) Load() (Registry, error) {
	state, err := rf.LoadState()
	if err != nil {
		return nil, err
	}
	return state.Snapshot, nil
}

// Save atomically advances the product revision only when the canonical CRDT
// snapshot changes. Identical state is a byte-for-byte no-op.
func (rf RegistryFile) Save(reg Registry) error {
	current, err := rf.LoadState()
	if err != nil {
		return err
	}
	_, digest, err := canonicalRegistry(reg)
	if err != nil {
		return err
	}
	if digest == current.Digest {
		return nil
	}
	if current.Revision == math.MaxUint64 {
		return errors.New("hostsync: registry revision exhausted")
	}
	state := RegistryState{Revision: current.Revision + 1, Snapshot: reg, Digest: digest}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal registry state: %w", err)
	}
	if err := overlay.WriteAtomic0600(rf.Path, append(data, '\n')); err != nil {
		return fmt.Errorf("write registry %s: %w", rf.Path, err)
	}
	return nil
}

func initialRegistryState() (RegistryState, error) {
	snapshot := cregistry.New[AccountValue]()
	_, digest, err := canonicalRegistry(snapshot)
	return RegistryState{Revision: 1, Snapshot: snapshot, Digest: digest}, err
}

func canonicalRegistry(reg Registry) ([]byte, string, error) {
	if reg == nil {
		return nil, "", errors.New("hostsync: registry snapshot is nil")
	}
	payload, err := json.Marshal(reg)
	if err != nil {
		return nil, "", fmt.Errorf("marshal registry snapshot: %w", err)
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
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
