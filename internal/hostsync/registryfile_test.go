package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/yasyf/synckit/cregistry"
)

func tempRegistry(t *testing.T) RegistryFile {
	t.Helper()
	dir := t.TempDir()
	return RegistryFile{
		Path:     filepath.Join(dir, "registry.json"),
		LockPath: filepath.Join(dir, "registry.lock"),
	}
}

func inode(t *testing.T, fi os.FileInfo) uint64 {
	t.Helper()
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("FileInfo.Sys() is %T, want *syscall.Stat_t", fi.Sys())
	}
	return st.Ino
}

func TestLoadMissingFileIsEmpty(t *testing.T) {
	rf := tempRegistry(t)
	state, err := rf.LoadState()
	if err != nil {
		t.Fatalf("LoadState of missing file: %v", err)
	}
	if state.Revision != 1 || state.Digest == "" {
		t.Fatalf("initial state = %+v, want revision 1 with digest", state)
	}
	if state.Snapshot == nil {
		t.Fatal("Load returned a nil registry; want an empty non-nil one")
	}
	if len(state.Snapshot) != 0 {
		t.Fatalf("empty registry has %d entries", len(state.Snapshot))
	}
}

func TestLoadCorruptFileIsLoud(t *testing.T) {
	rf := tempRegistry(t)
	if err := os.WriteFile(rf.Path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("seed corrupt file: %v", err)
	}
	reg, err := rf.Load()
	if err == nil {
		t.Fatal("Load of corrupt file returned nil error; want a loud parse error")
	}
	if reg != nil {
		t.Fatalf("Load of corrupt file returned a registry: %v", reg)
	}
}

func TestLoadRejectsLegacyRawRegistry(t *testing.T) {
	rf := tempRegistry(t)
	if err := os.WriteFile(rf.Path, []byte(`{"u":{"added_at":1,"value":{"uuid":"u"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := rf.LoadState(); err == nil {
		t.Fatal("LoadState accepted the removed raw-registry schema")
	}
}

func TestLoadSaveRoundTripInt64Stamps(t *testing.T) {
	rf := tempRegistry(t)

	// MaxInt64-1 is not exactly representable as float64, so a decode that
	// detoured through float64 would corrupt it — the bug this pins against.
	const big = int64(math.MaxInt64) - 1
	val := AccountValue{
		UUID:         "u1",
		Email:        "e@x.com",
		Label:        "label",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1"}`),
		Chain:        ChainStamp{Origin: "H", ExpiresAt: big, Hash: "h", RotatedAt: big - 1},
	}
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", val, cregistry.Micros(big-3))
	reg.Remove("u1", cregistry.Micros(big-4)) // still Present (add > remove); exercises the remove stamp

	if err := rf.Save(reg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	state, err := rf.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if state.Revision != 2 || state.Digest == "" {
		t.Fatalf("saved state = %+v, want revision 2 with digest", state)
	}

	// The stamp must be rendered as an exact integer literal on disk — direct
	// proof no float64 rounding occurred.
	raw, err := os.ReadFile(rf.Path)
	if err != nil {
		t.Fatalf("read back file: %v", err)
	}
	if want := []byte("9223372036854775806"); !bytes.Contains(raw, want) {
		t.Errorf("big stamp not rendered as exact int64 %s in:\n%s", want, raw)
	}

	got, err := rf.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	e := got["u1"]
	checks := []struct {
		name      string
		got, want int64
	}{
		{"Added", int64(e.Added), big - 3},
		{"Removed", int64(e.Removed), big - 4},
		{"Chain.ExpiresAt", e.Value.Chain.ExpiresAt, big},
		{"Chain.RotatedAt", e.Value.Chain.RotatedAt, big - 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d (int64 corrupted)", c.name, c.got, c.want)
		}
	}
	if e.Value.UUID != "u1" || e.Value.Chain.Hash != "h" || e.Value.Chain.Origin != "H" {
		t.Errorf("scalar fields not preserved: %+v", e.Value)
	}

	// The value survives semantically. RawMessage is compared via the fingerprint
	// (which compacts) because Save indents the on-disk form, so a raw byte
	// compare of the re-parsed RawMessage would differ only in whitespace.
	if before, after := Fingerprint(reg["u1"]), Fingerprint(got["u1"]); before != after {
		t.Errorf("fingerprint changed across Save/Load: before %q, after %q", before, after)
	}
}

func TestSaveNoopWhenEqual(t *testing.T) {
	rf := tempRegistry(t)
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", AccountValue{UUID: "u1", OAuthAccount: json.RawMessage(`{}`)}, cregistry.Micros(1))

	if err := rf.Save(reg); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	fi0, err := os.Stat(rf.Path)
	if err != nil {
		t.Fatalf("stat after first Save: %v", err)
	}
	ino0 := inode(t, fi0)

	// Re-saving identical content must not touch the file at all: same inode,
	// same mtime (a rename would change both).
	if err := rf.Save(reg); err != nil {
		t.Fatalf("no-op Save: %v", err)
	}
	fi1, err := os.Stat(rf.Path)
	if err != nil {
		t.Fatalf("stat after no-op Save: %v", err)
	}
	if inode(t, fi1) != ino0 {
		t.Errorf("no-op Save rewrote the file: inode %d -> %d", ino0, inode(t, fi1))
	}
	if !fi1.ModTime().Equal(fi0.ModTime()) {
		t.Errorf("no-op Save changed mtime: %v -> %v", fi0.ModTime(), fi1.ModTime())
	}
	state, err := rf.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 2 {
		t.Fatalf("no-op Save advanced revision to %d, want 2", state.Revision)
	}

	// Positive control: a real content change must rewrite (new inode).
	reg.Add("u2", AccountValue{UUID: "u2", OAuthAccount: json.RawMessage(`{}`)}, cregistry.Micros(1))
	if err := rf.Save(reg); err != nil {
		t.Fatalf("changed Save: %v", err)
	}
	fi2, err := os.Stat(rf.Path)
	if err != nil {
		t.Fatalf("stat after changed Save: %v", err)
	}
	if inode(t, fi2) == ino0 {
		t.Error("changed Save did not rewrite the file (inode unchanged)")
	}
	state, err = rf.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 3 {
		t.Fatalf("changed Save revision = %d, want 3", state.Revision)
	}
}

func TestLoadRejectsDigestMismatch(t *testing.T) {
	rf := tempRegistry(t)
	reg := cregistry.New[AccountValue]()
	reg.Add("u", AccountValue{UUID: "u", OAuthAccount: json.RawMessage(`{}`)}, 1)
	if err := rf.Save(reg); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(rf.Path)
	if err != nil {
		t.Fatal(err)
	}
	var state RegistryState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	state.Digest = "0000000000000000000000000000000000000000000000000000000000000000"
	raw, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rf.Path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := rf.LoadState(); err == nil {
		t.Fatal("LoadState accepted a digest-mismatched snapshot")
	}
}

func TestWithLockSerializes(t *testing.T) {
	rf := tempRegistry(t)
	ctx := context.Background()

	const goroutines, iters = 2, 30
	var (
		counter int   // plain int: correct only if WithLock truly serializes (also -race guarded)
		inside  int32 // overlap detector
		wg      sync.WaitGroup
	)
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				err := rf.WithLock(ctx, func() error {
					if atomic.AddInt32(&inside, 1) != 1 {
						return errors.New("overlapping critical sections: WithLock did not serialize")
					}
					counter++
					atomic.AddInt32(&inside, -1)
					return nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if want := goroutines * iters; counter != want {
		t.Fatalf("counter = %d, want %d (lost updates ⇒ not serialized)", counter, want)
	}
}
