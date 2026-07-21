// Package credstest provides in-memory test doubles for the pool's credential
// seam: Fake resolves accounts to an in-memory Keychain plus the real on-disk
// file store, and FaultStore injects per-op failures into any creds.Store.
package credstest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/supervise"
)

type fileTaskRunner struct{}

func (fileTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	if !creds.IsFileWorkerInvocation(task.Args) {
		return errors.New("unexpected credential test worker task")
	}
	if task.Stdin == nil || task.Stdout == nil {
		return io.ErrUnexpectedEOF
	}
	return creds.RunFileWorker(ctx, task.Stdin, task.Stdout)
}

// FileStore returns a plaintext credential store backed by the in-process
// worker adapter used only by tests.
func FileStore(configDir string) creds.FileStore {
	return creds.FileStore{
		ConfigDir: configDir, Runner: fileTaskRunner{}, WorkerExecutable: "test-worker",
	}
}

// Faults selects the per-op errors a FaultStore injects; nil fields pass
// through to the wrapped Store.
type Faults struct {
	Read, Write, Delete error
}

// FaultStore wraps a Store, failing selected ops with injected errors, so
// tests can simulate one backend breaking mid-cycle (e.g. an unsearchable
// Keychain or a partial rollback).
type FaultStore struct {
	creds.Store
	Faults Faults
}

// Read fails with Faults.Read when set.
func (s FaultStore) Read(ctx context.Context) (*creds.Credential, error) {
	if s.Faults.Read != nil {
		return nil, s.Faults.Read
	}
	return s.Store.Read(ctx)
}

// Write fails with Faults.Write when set.
func (s FaultStore) Write(ctx context.Context, cred *creds.Credential) error {
	if s.Faults.Write != nil {
		return s.Faults.Write
	}
	return s.Store.Write(ctx, cred)
}

// Delete fails with Faults.Delete when set.
func (s FaultStore) Delete(ctx context.Context) error {
	if s.Faults.Delete != nil {
		return s.Faults.Delete
	}
	return s.Store.Delete(ctx)
}

// Fake is an in-memory credential seam (it implements pool.Credentials).
// Keychain items live in an internally locked map so any race the detector
// reports is in the code under test; the file backend is the real on-disk
// creds.FileStore under the account's ConfigDir, which tests point at a temp
// dir. Keychain ops from code under test are recorded; seeding via Put/Remove
// is not.
type Fake struct {
	// KeychainFaults and FileFaults are injected into every store Fake hands
	// out. Set them before use — they are read without the lock.
	KeychainFaults Faults
	FileFaults     Faults

	mu      sync.Mutex
	items   map[string]*creds.Credential
	touched []string // service of every keychain read/write/delete/discover, in order
	deleted []string // service of every keychain delete, in order
	writes  int
}

// NewFake returns an empty Fake.
func NewFake() *Fake { return &Fake{items: map[string]*creds.Credential{}} }

func key(service, account string) string { return service + "\x00" + account }

// Put seeds a keychain item, bypassing op recording.
func (f *Fake) Put(service, account string, cred *creds.Credential) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *cred
	f.items[key(service, account)] = &cp
}

// Remove unseeds a keychain item, bypassing op recording.
func (f *Fake) Remove(service, account string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, key(service, account))
}

// Get returns the stored keychain item and whether it exists, bypassing op
// recording.
func (f *Fake) Get(service, account string) (*creds.Credential, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.items[key(service, account)]
	if !ok {
		return nil, false
	}
	cp := *c
	return &cp, true
}

// Store returns a's store for the backend src names, wrapped with the
// configured faults.
func (f *Fake) Store(a store.Account, src creds.Source) creds.Store {
	if src == creds.SourceFile {
		return FaultStore{Store: FileStore(a.ConfigDir), Faults: f.FileFaults}
	}
	return FaultStore{
		Store:  keychainItem{f: f, service: a.KeychainService, account: a.KeychainAccount},
		Faults: f.KeychainFaults,
	}
}

// Stores returns a's stores in the production resolution order: Keychain
// first, then the file.
func (f *Fake) Stores(a store.Account) []creds.Store {
	return []creds.Store{f.Store(a, creds.SourceKeychain), f.Store(a, creds.SourceFile)}
}

// Discover mirrors creds.DiscoverAccount over the in-memory items.
func (f *Fake) Discover(_ context.Context, service string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touched = append(f.touched, service)
	prefix := service + "\x00"
	for k := range f.items {
		if strings.HasPrefix(k, prefix) {
			return strings.TrimPrefix(k, prefix), nil
		}
	}
	return "", creds.ErrNotFound
}

// TouchedServices returns the service of every recorded keychain op, in order.
func (f *Fake) TouchedServices() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.touched...)
}

// DeletedServices returns the service of every recorded keychain delete, in
// order.
func (f *Fake) DeletedServices() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

// WriteCount returns how many keychain writes the code under test performed.
func (f *Fake) WriteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes
}

// keychainItem is the Fake's in-memory creds.Store for one service/account.
type keychainItem struct {
	f       *Fake
	service string
	account string
}

func (k keychainItem) Source() creds.Source { return creds.SourceKeychain }

func (k keychainItem) Read(context.Context) (*creds.Credential, error) {
	k.f.mu.Lock()
	defer k.f.mu.Unlock()
	k.f.touched = append(k.f.touched, k.service)
	c, ok := k.f.items[key(k.service, k.account)]
	if !ok {
		return nil, creds.ErrNotFound
	}
	cp := *c
	return &cp, nil
}

func (k keychainItem) Write(_ context.Context, cred *creds.Credential) error {
	k.f.mu.Lock()
	defer k.f.mu.Unlock()
	k.f.touched = append(k.f.touched, k.service)
	k.f.writes++
	cp := *cred
	k.f.items[key(k.service, k.account)] = &cp
	return nil
}

func (k keychainItem) Delete(context.Context) error {
	k.f.mu.Lock()
	defer k.f.mu.Unlock()
	k.f.touched = append(k.f.touched, k.service)
	k.f.deleted = append(k.f.deleted, k.service)
	delete(k.f.items, key(k.service, k.account))
	return nil
}

func (k keychainItem) String() string { return fmt.Sprintf("fake keychain item %q", k.service) }
