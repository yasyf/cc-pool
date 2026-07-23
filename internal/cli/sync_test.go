package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// syncTestEnv isolates HOME and XDG_CONFIG_HOME and returns a manager over a
// temp store with an in-memory credential fake.
func syncTestEnv(t *testing.T) *pool.Manager {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &pool.Manager{Store: st}
}

// putSynckitdOnPath makes the real LookPath find a fake synckitd binary.
func putSynckitdOnPath(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "synckitd"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil { //nolint:gosec // test fixture must be executable for LookPath
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
}

// stubSynckitdRun records synckitd invocations instead of exec'ing.
func stubSynckitdRun(t *testing.T) *[][]string {
	t.Helper()
	var calls [][]string
	orig := synckitdRun
	synckitdRun = func(_ context.Context, args ...string) error {
		calls = append(calls, args)
		return nil
	}
	t.Cleanup(func() { synckitdRun = orig })
	return &calls
}

func stubSyncConverge(t *testing.T) *int {
	t.Helper()
	var calls int
	origConverge := syncConverge
	origEnsure := syncEnsureDaemon
	syncEnsureDaemon = func(context.Context) bool { return true }
	syncConverge = func(ctx context.Context, out io.Writer, ensure func(context.Context) bool, _ string) error {
		if !ensure(ctx) {
			return fmt.Errorf("daemon unavailable")
		}
		calls++
		success(out, "Converged 1 item(s), 0 deferred busy.")
		return nil
	}
	t.Cleanup(func() {
		syncConverge = origConverge
		syncEnsureDaemon = origEnsure
	})
	return &calls
}

func writeMeshState(t *testing.T, self string, hosts []string) {
	t.Helper()
	ctx := context.Background()
	if err := hostregistry.Mesh.InitializeState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := hostregistry.Mesh.Update(ctx, func(reg *hostregistry.Registry) error {
		reg.Self = self
		reg.Hosts = append([]string{}, hosts...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func syncCmdBuf(t *testing.T) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetContext(context.Background())
	return cmd, &out
}

func TestEnableWritesValidManifest(t *testing.T) {
	m := syncTestEnv(t)
	putSynckitdOnPath(t)
	calls := stubSynckitdRun(t)
	converges := stubSyncConverge(t)

	cmd, _ := syncCmdBuf(t)
	if err := runSyncEnable(cmd, m); err != nil {
		t.Fatal(err)
	}

	path, err := hostsync.ManifestPath()
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("manifest perms = %o, want 600", perm)
	}

	// Round-trip through the REAL synckit loader: Load validates, so schema
	// drift in the written manifest fails here.
	lm, err := manifest.Load(path)
	if err != nil {
		t.Fatalf("synckit manifest.Load rejected our manifest: %v", err)
	}
	if lm.Name != "cc-pool" || lm.Binary != "cc-pool" || lm.Brew != "yasyf/tap/cc-pool" {
		t.Fatalf("manifest identity = %q/%q/%q", lm.Name, lm.Binary, lm.Brew)
	}
	if lm.Watch.Debounce != codec.Duration(syncWatchDebounce) {
		t.Fatalf("watch spec = %+v", lm.Watch)
	}
	if lm.Service.Transport != "socket" || lm.Service.Sock != pool.SyncSocketPath() {
		t.Fatalf("service spec = %+v, want socket at %s", lm.Service, pool.SyncSocketPath())
	}
	if want := []string{"sync", "rpc-serve"}; !equalStrings(lm.Service.ServeArgs, want) {
		t.Fatalf("serve args = %v, want %v", lm.Service.ServeArgs, want)
	}
	if lm.Helper != nil {
		t.Fatalf("manifest must not carry a helper block: %+v", lm)
	}

	if v, ok, err := m.Store.GetMeta(syncMetaKey); err != nil || !ok || v != "1" {
		t.Fatalf("sync_enabled meta = %q ok=%v err=%v, want 1", v, ok, err)
	}
	if len(*calls) != 1 || !equalStrings((*calls)[0], []string{"register", path}) {
		t.Fatalf("synckitd calls = %v, want one register of %s", *calls, path)
	}
	if *converges != 1 {
		t.Fatalf("daemon converge calls = %d, want 1", *converges)
	}
}

func TestEnableRefusesWithoutSynckitd(t *testing.T) {
	m := syncTestEnv(t)
	t.Setenv("PATH", t.TempDir())

	cmd, _ := syncCmdBuf(t)
	err := runSyncEnable(cmd, m)
	if err == nil {
		t.Fatal("want error when synckitd is not on PATH")
	}
	if !strings.Contains(err.Error(), "brew install yasyf/tap/synckit") {
		t.Fatalf("error %q missing the install hint", err)
	}
	if _, ok, err := m.Store.GetMeta(syncMetaKey); err != nil || ok {
		t.Fatalf("sync_enabled meta set despite refusal (ok=%v err=%v)", ok, err)
	}
	path, err := hostsync.ManifestPath()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("manifest written despite refusal: %v", err)
	}
}

func TestEnableRequestsDaemonConvergence(t *testing.T) {
	m := syncTestEnv(t)
	putSynckitdOnPath(t)
	stubSynckitdRun(t)
	converges := stubSyncConverge(t)
	writeMeshState(t, "me@hosta", nil)

	cmd, out := syncCmdBuf(t)
	if err := runSyncEnable(cmd, m); err != nil {
		t.Fatal(err)
	}
	if *converges != 1 {
		t.Fatalf("daemon converge calls = %d, want 1", *converges)
	}
	if got := stripANSI(out.String()); !strings.Contains(got, "Converged 1 item") {
		t.Fatalf("output %q missing daemon convergence", got)
	}
}

func TestDisableClearsMetaAndRemovesManifest(t *testing.T) {
	m := syncTestEnv(t)
	putSynckitdOnPath(t)
	calls := stubSynckitdRun(t)
	stubSyncConverge(t)
	rf := hostsync.NewRegistryFile(pool.SyncDir())
	if err := rf.Save(hostsync.Registry{}); err != nil {
		t.Fatal(err)
	}

	cmd, _ := syncCmdBuf(t)
	if err := runSyncEnable(cmd, m); err != nil {
		t.Fatal(err)
	}
	if err := runSyncDisable(cmd, m); err != nil {
		t.Fatal(err)
	}

	if v, ok, err := m.Store.GetMeta(syncMetaKey); err != nil || !ok || v != "0" {
		t.Fatalf("sync_enabled meta = %q ok=%v err=%v, want 0", v, ok, err)
	}
	path, err := hostsync.ManifestPath()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("manifest still present after disable: %v", err)
	}
	last := (*calls)[len(*calls)-1]
	if !equalStrings(last, []string{"unregister", "cc-pool"}) {
		t.Fatalf("last synckitd call = %v, want unregister cc-pool", last)
	}
	// Registry state (and any tombstones) must survive a disable.
	if _, err := os.Stat(pool.SyncDir()); err != nil {
		t.Fatalf("sync dir removed by disable: %v", err)
	}
}

func TestRPCServeBridgesPersistentSessionByteExact(t *testing.T) {
	base, err := os.MkdirTemp("", "ccp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	sock := filepath.Join(base, "s.sock")

	ln, err := rpc.Listen(t.Context(), sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = io.Copy(conn, conn)
	}()
	t.Cleanup(func() { cancel(); <-done })

	t.Run("bridges one persistent session byte-exact", func(t *testing.T) {
		payload := []byte{0, 1, 2, 0xff, '\n', '{', '}', 0}
		in := bytes.NewReader(payload)
		var out bytes.Buffer
		if err := runSyncRPCServe(ctx, in, &out, func(context.Context) bool { return true }, sock); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), payload) {
			t.Fatalf("bridged bytes = %v, want %v", out.Bytes(), payload)
		}
	})

	t.Run("refuses when the daemon cannot start", func(t *testing.T) {
		var out bytes.Buffer
		err := runSyncRPCServe(ctx, strings.NewReader(""), &out, func(context.Context) bool { return false }, sock)
		if err == nil {
			t.Fatal("want error when ensure fails")
		}
		if out.Len() != 0 {
			t.Fatalf("stdout written despite refusal: %q", out.String())
		}
	})
}

func TestSyncConvergeReportsResult(t *testing.T) {
	base, err := os.MkdirTemp("", "ccp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	sock := filepath.Join(base, "c.sock")

	ln, err := rpc.Listen(t.Context(), sock)
	if err != nil {
		t.Fatal(err)
	}
	d := rpc.NewDispatcher()
	d.Register(syncservice.MethodSync, func(context.Context, map[string]any) (any, error) {
		return syncservice.SyncResult{Converged: 3, SkippedBusy: 1}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rpc.NewServer(d).Serve(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	var out bytes.Buffer
	if err := runSyncConverge(ctx, &out, func(context.Context) bool { return true }, sock); err != nil {
		t.Fatal(err)
	}
	if got := stripANSI(out.String()); !strings.Contains(got, "Converged 3 item(s), 1 deferred busy.") {
		t.Fatalf("output %q missing the converge result", got)
	}

	if err := runSyncConverge(ctx, &out, func(context.Context) bool { return false }, sock); err == nil {
		t.Fatal("want error when ensure fails")
	}
}

func TestStatusRendersMesh(t *testing.T) {
	m := syncTestEnv(t)
	writeMeshState(t, "me@hosta", []string{"you@hostb"})
	if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
		t.Fatal(err)
	}

	rf := hostsync.NewRegistryFile(pool.SyncDir())
	if err := rf.Update(context.Background(), func(reg hostsync.Registry) error {
		reg.Add("u-1", hostsync.AccountValue{
			UUID:  "u-1",
			Label: "Work",
			Chain: hostsync.ChainStamp{ExpiresAt: 1700000000000, Hash: "h1", Origin: "hosta"},
		}, cregistry.UnixMicros(time.Now()))
		reg.Remove("u-9", cregistry.UnixMicros(time.Now()))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	reg, err := rf.Load()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := rpc.Listen(t.Context(), pool.SyncSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	d := rpc.NewDispatcher()
	d.Register(syncservice.MethodCapabilities, func(context.Context, map[string]any) (any, error) {
		return syncservice.Capabilities{
			Name:    "cc-pool",
			Methods: []string{syncservice.MethodCapabilities, syncservice.MethodGetState},
		}, nil
	})
	d.Register(syncservice.MethodGetState, func(context.Context, map[string]any) (any, error) {
		return json.RawMessage(raw), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rpc.NewServer(d).Serve(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	admitCLITestAccount(t, m.Store, store.Account{
		ID: 1, ConfigDir: t.TempDir(), Label: "Work", AccountUUID: "u-1",
		KeychainService: "svc-1", KeychainAccount: "u",
	})

	cmd, out := syncCmdBuf(t)
	if err := runSyncStatus(cmd, m); err != nil {
		t.Fatal(err)
	}
	got := stripANSI(out.String())
	for _, want := range []string{
		"Host sync: enabled",
		"Mesh self: me@hosta",
		"Mesh peers: you@hostb",
		"Sync socket healthy",
		"u-1",
		"Work",
		"2023-11-14T22:13:20Z", // 1700000000000 ms
		"ORIGIN",               // the current registry table columns
		"LOCAL",
		"hosta",   // u-1's chain origin
		"removed", // the u-9 tombstone row
		"present",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("status output missing %q:\n%s", want, got)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
