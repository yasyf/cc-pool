package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
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
func syncTestEnv(t *testing.T) (*pool.Manager, *credstest.Fake) {
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
	fk := credstest.NewFake()
	return &pool.Manager{Store: st, Creds: fk}, fk
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

func writeMeshState(t *testing.T, payload string) {
	t.Helper()
	dir, err := hostregistry.Mesh.Dir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(payload), 0o600); err != nil {
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

// addSyncTestAccount creates a symlink-backed row with a .claude.json identity
// and a keychain credential in the fake.
func addSyncTestAccount(t *testing.T, m *pool.Manager, fk *credstest.Fake, id int, uuid, email, label string, cred *creds.Credential) store.Account {
	t.Helper()
	dir := t.TempDir()
	body := fmt.Sprintf(`{"oauthAccount": {"accountUuid": %q, "emailAddress": %q, "extra": "keep-me-%d"}}`, uuid, email, id)
	if err := os.WriteFile(filepath.Join(dir, ".claude.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	a := store.Account{
		ID: id, ConfigDir: dir, Label: label, OverlayKind: "symlink",
		KeychainService: fmt.Sprintf("svc-%d", id), KeychainAccount: "u",
	}
	if err := m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	if cred != nil {
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
	}
	return a
}

func testCred(access string, expiresAt int64) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken: access, RefreshToken: "rt-" + access, ExpiresAt: expiresAt,
	}}
}

func TestEnableWritesValidManifest(t *testing.T) {
	m, _ := syncTestEnv(t)
	putSynckitdOnPath(t)
	calls := stubSynckitdRun(t)

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
	if lm.Watch.Backend != "fsnotify" || lm.Watch.Debounce != codec.Duration(syncWatchDebounce) {
		t.Fatalf("watch spec = %+v", lm.Watch)
	}
	if lm.Service.Transport != "socket" || lm.Service.Sock != pool.SyncSocketPath() {
		t.Fatalf("service spec = %+v, want socket at %s", lm.Service, pool.SyncSocketPath())
	}
	if want := []string{"sync", "rpc-serve"}; !equalStrings(lm.Service.ServeArgs, want) {
		t.Fatalf("serve args = %v, want %v", lm.Service.ServeArgs, want)
	}
	if lm.Launchd != nil || lm.Helper != nil {
		t.Fatalf("manifest must not carry launchd/helper blocks: %+v", lm)
	}

	if v, ok, err := m.Store.GetMeta(syncMetaKey); err != nil || !ok || v != "1" {
		t.Fatalf("sync_enabled meta = %q ok=%v err=%v, want 1", v, ok, err)
	}
	if len(*calls) != 1 || !equalStrings((*calls)[0], []string{"register", path}) {
		t.Fatalf("synckitd calls = %v, want one register of %s", *calls, path)
	}
}

func TestEnableRefusesWithoutSynckitd(t *testing.T) {
	m, _ := syncTestEnv(t)
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

func TestEnableBackfillsAndScanPublishes(t *testing.T) {
	m, fk := syncTestEnv(t)
	putSynckitdOnPath(t)
	stubSynckitdRun(t)
	writeMeshState(t, `{"self":"me@hosta","hosts":[]}`)

	cred1 := testCred("at-1", 1893456000000)
	addSyncTestAccount(t, m, fk, 1, "u-1", "one@example.com", "Work", cred1)
	cred2 := testCred("at-2", 1893456000000)
	addSyncTestAccount(t, m, fk, 2, "u-2", "two@example.com", "Old", cred2)

	// A peer already removed u-2; enable must NOT resurrect the tombstone.
	rf := syncRegistryFile()
	if err := rf.Update(context.Background(), func(reg hostsync.Registry) error {
		reg.Remove("u-2", cregistry.UnixMicros(time.Now()))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	cmd, out := syncCmdBuf(t)
	if err := runSyncEnable(cmd, m); err != nil {
		t.Fatal(err)
	}

	for id, want := range map[int]string{1: "u-1", 2: "u-2"} {
		a, err := m.Store.GetAccount(id)
		if err != nil {
			t.Fatal(err)
		}
		if a.AccountUUID != want {
			t.Fatalf("acct-%02d uuid = %q, want %q", id, a.AccountUUID, want)
		}
	}

	reg, err := rf.Load()
	if err != nil {
		t.Fatal(err)
	}
	e1, ok := reg["u-1"]
	if !ok || !e1.Present() {
		t.Fatalf("u-1 not present in registry: %+v", reg)
	}
	v := e1.Value
	if v.Email != "one@example.com" || v.Label != "Work" {
		t.Fatalf("u-1 value = %+v", v)
	}
	if !bytes.Contains(v.OAuthAccount, []byte("keep-me-1")) {
		t.Fatalf("u-1 oauthAccount not passed through verbatim: %s", v.OAuthAccount)
	}
	if v.Chain.Hash != creds.AccessHash(cred1) {
		t.Fatalf("u-1 chain hash = %q, want AccessHash(cred1)", v.Chain.Hash)
	}
	if v.Chain.ExpiresAt != 1893456000000 || v.Chain.Origin != "me@hosta" {
		t.Fatalf("u-1 chain = %+v", v.Chain)
	}
	if v.Chain.RotatedAt <= 0 {
		t.Fatalf("u-1 chain rotatedAt = %d, want > 0", v.Chain.RotatedAt)
	}

	if e2 := reg["u-2"]; e2.Present() {
		t.Fatalf("u-2 tombstone resurrected by enable: %+v", e2)
	}

	if _, err := os.Stat(filepath.Join(pool.SyncStampsDir(), "u-1", "stamp")); err != nil {
		t.Fatalf("u-1 stamp missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(pool.SyncStampsDir(), "u-2")); !os.IsNotExist(err) {
		t.Fatalf("u-2 stamp dir created for an unchanged tombstone: %v", err)
	}

	if got := stripANSI(out.String()); !strings.Contains(got, "Published 1 account") {
		t.Fatalf("output %q missing the publish count", got)
	}
}

func TestDisableClearsMetaAndRemovesManifest(t *testing.T) {
	m, _ := syncTestEnv(t)
	putSynckitdOnPath(t)
	calls := stubSynckitdRun(t)

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

func TestRPCServeBridgesFrames(t *testing.T) {
	base, err := os.MkdirTemp("", "ccp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	sock := filepath.Join(base, "s.sock")

	ln, err := rpc.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	d := rpc.NewDispatcher()
	// 2^53+1: any float64 round-trip corrupts it to ...992, so byte-exact
	// bridging is the only way this digit string survives.
	d.Register(syncservice.MethodGetState, func(context.Context, map[string]any) (any, error) {
		return json.RawMessage(`{"u1":{"added_at":9007199254740993,"value":null}}`), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rpc.Serve(ctx, ln, d); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	t.Run("bridges one frame byte-exact", func(t *testing.T) {
		in := strings.NewReader(`{"method":"svc.get_state","params":{}}` + "\n")
		var out bytes.Buffer
		if err := runSyncRPCServe(ctx, in, &out, func(context.Context) bool { return true }, sock); err != nil {
			t.Fatal(err)
		}
		got := out.String()
		if !strings.Contains(got, "9007199254740993") {
			t.Fatalf("int64 stamp corrupted in transit: %q", got)
		}
		if !strings.Contains(got, `"ok":true`) {
			t.Fatalf("response not ok: %q", got)
		}
		if strings.Count(got, "\n") != 1 || !strings.HasSuffix(got, "\n") {
			t.Fatalf("want exactly one newline-terminated frame, got %q", got)
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

	ln, err := rpc.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	d := rpc.NewDispatcher()
	d.Register(syncservice.MethodSync, func(context.Context, map[string]any) (any, error) {
		return syncservice.SyncResult{Converged: 3, SkippedBusy: 1}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rpc.Serve(ctx, ln, d); close(done) }()
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
	m, _ := syncTestEnv(t)
	writeMeshState(t, `{"self":"me@hosta","hosts":["you@hostb"]}`)
	if err := m.Store.SetMeta(syncMetaKey, "1"); err != nil {
		t.Fatal(err)
	}

	rf := syncRegistryFile()
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

	// A credential only in the plaintext file store must draw the fallback warning.
	dir := t.TempDir()
	if err := creds.WriteFileCredential(dir, testCred("at-f", 1893456000000)); err != nil {
		t.Fatal(err)
	}
	if err := m.Store.UpsertAccount(store.Account{
		ID: 1, ConfigDir: dir, Label: "Work", OverlayKind: "symlink",
		KeychainService: "svc-1", KeychainAccount: "u",
	}); err != nil {
		t.Fatal(err)
	}

	cmd, out := syncCmdBuf(t)
	if err := runSyncStatus(cmd, m); err != nil {
		t.Fatal(err)
	}
	got := stripANSI(out.String())
	for _, want := range []string{
		"Host sync: enabled",
		"Mesh self: me@hosta",
		"Mesh peers: you@hostb",
		"not answering", // no daemon, no sync socket
		"u-1",
		"Work",
		"2023-11-14T22:13:20Z", // 1700000000000 ms
		"ORIGIN",               // the v2 registry table columns
		"LOCAL",
		"hosta",   // u-1's chain origin
		"removed", // the u-9 tombstone row
		"plaintext file store",
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
