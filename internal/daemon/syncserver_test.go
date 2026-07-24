package daemon

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func hasMethod(ms []string, want string) bool {
	for _, m := range ms {
		if m == want {
			return true
		}
	}
	return false
}

type blockingSyncConsumer struct {
	syncservice.SyncConsumer
	apply func(context.Context, syncservice.ChangeEnvelope) (syncservice.ApplyResult, error)
}

func (c blockingSyncConsumer) Apply(
	ctx context.Context,
	change syncservice.ChangeEnvelope,
) (syncservice.ApplyResult, error) {
	return c.apply(ctx, change)
}

// TestSyncSocketServesConsumer stands up the real second socket and round-trips
// the contract capability method over a unix client, pins the 0600 socket mode,
// and proves removed credential-fetch methods are not registered.
func TestSyncSocketServesConsumer(t *testing.T) {
	// macOS caps sun_path at 104 bytes; t.TempDir paths overflow it.
	sockDir, err := os.MkdirTemp("/tmp", "ccp-sync")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "sync.sock")

	regDir := t.TempDir()
	rf := hostsync.RegistryFile{Path: filepath.Join(regDir, "registry.json"), LockPath: filepath.Join(regDir, "registry.lock")}
	svc := &hostsync.Service{Registry: &rf, StampDir: filepath.Join(regDir, "stamps")}
	consumer := hostsync.NewConsumer(svc, func() (bool, error) { return true, nil })

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var intake drain.Intake
	t.Cleanup(func() { cancel(); wg.Wait() })
	if _, err := serveSyncSocket(ctx, &wg, &intake, sock, consumer, log.New(io.Discard, "", 0)); err != nil {
		t.Fatalf("serveSyncSocket: %v", err)
	}

	if fi, err := os.Stat(sock); err != nil {
		t.Fatalf("stat socket: %v", err)
	} else if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket mode = %o, want 0600", perm)
	}

	client := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = client.Close() }()
	caps, err := client.Capabilities(ctx)
	if err != nil {
		t.Fatalf("Capabilities RPC: %v", err)
	}
	if caps.Name != "cc-pool" {
		t.Errorf("Capabilities Name = %q, want cc-pool", caps.Name)
	}
	tx := syncservice.Socket(sock)
	defer func() { _ = tx.Close() }()
	for _, method := range []string{"ccp.fetch_credential", "ccp.fetch_stripped_credential"} {
		if hasMethod(caps.Methods, method) {
			t.Errorf("Capabilities Methods %v retain removed %s", caps.Methods, method)
		}
		removed, err := tx.Do(ctx, &rpc.Request{Method: method})
		if err != nil {
			t.Fatalf("removed %s Do: %v", method, err)
		}
		if removed.OK || !strings.Contains(removed.Error, "unknown method") {
			t.Errorf("removed %s response = %+v", method, removed)
		}
		if payload := string(removed.Result); payload != "" && payload != "null" {
			t.Errorf("removed %s carried result payload: %s", method, payload)
		}
	}
}

// TestSyncSocketDrainsInFlightHandler pins the second socket into the daemon's
// settle-before-cancel order: Deactivate refuses new dials, an admitted handler
// keeps Drain blocked with a live context, and its store read finishes before
// teardown closes the store.
func TestSyncSocketDrainsInFlightHandler(t *testing.T) {
	sockDir, err := os.MkdirTemp("/tmp", "ccp-sync-drain")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "sync.sock")
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}

	regDir := t.TempDir()
	rf := hostsync.RegistryFile{Path: filepath.Join(regDir, "registry.json"), LockPath: filepath.Join(regDir, "registry.lock")}
	svc := &hostsync.Service{Registry: &rf, StampDir: filepath.Join(regDir, "stamps")}
	consumer := hostsync.NewConsumer(svc, func() (bool, error) { return true, nil })
	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	ctxErr := make(chan error, 1)
	storeErr := make(chan error, 1)
	blocking := blockingSyncConsumer{SyncConsumer: consumer}
	blocking.apply = func(ctx context.Context, change syncservice.ChangeEnvelope) (syncservice.ApplyResult, error) {
		close(entered)
		<-release
		ctxErr <- ctx.Err()
		_, _, err := st.GetMeta("sync-drain-probe")
		storeErr <- err
		return syncservice.ApplyResult{AckedRevision: change.SourceRevision}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var intake drain.Intake
	ln, err := serveSyncSocket(ctx, &wg, &intake, sock, blocking, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		wg.Wait()
	})

	client := syncservice.NewClient(syncservice.Socket(sock))
	defer func() { _ = client.Close() }()
	change, err := syncservice.NewExportedChange(
		hostsync.SyncServiceID, hostsync.SyncSchemaFingerprint, syncservice.ChangeSnapshot,
		syncservice.NewRevision(0), syncservice.NewRevision(1), []byte(`{}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err = syncservice.BindDelivery(change, "remote-host")
	if err != nil {
		t.Fatal(err)
	}
	type callResult struct {
		result syncservice.ApplyResult
		err    error
	}
	called := make(chan callResult, 1)
	go func() {
		result, err := client.Apply(context.Background(), change)
		called <- callResult{result: result, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sync handler never entered")
	}

	deactivated := make(chan struct{})
	drained := make(chan error, 1)
	go func() {
		intake.Close()
		err := ln.Close()
		close(deactivated)
		if settleErr := intake.Settle(ctx); settleErr != nil {
			err = errors.Join(err, settleErr)
		}
		cancel()
		drained <- err
	}()
	<-deactivated
	if conn, err := net.DialTimeout("unix", sock, 100*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("post-drain sync dial succeeded")
	}
	select {
	case err := <-drained:
		t.Fatalf("Drain returned before the in-flight sync handler settled: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case <-ctx.Done():
		t.Fatalf("sync handler context canceled before settle: %v", ctx.Err())
	default:
	}

	close(release)
	released = true
	if err := <-ctxErr; err != nil {
		t.Fatalf("sync handler context canceled before store access: %v", err)
	}
	if err := <-storeErr; err != nil {
		t.Fatalf("sync handler store access raced teardown: %v", err)
	}
	result := <-called
	if result.err != nil || result.result.AckedRevision != change.SourceRevision {
		t.Fatalf("in-flight sync response: result=%+v err=%v", result.result, result.err)
	}
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store after sync settle: %v", err)
	}
}

// TestSyncEnabledMeta pins that syncEnabled reflects the store's sync_enabled meta
// and needs no restart: unset or "0" is off, "1" is on.
func TestSyncEnabledMeta(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pool-v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &Server{cl: newClaims(), m: &pool.Manager{Store: st}}

	if on, err := s.syncEnabled(); err != nil || on {
		t.Fatalf("syncEnabled with no meta = %v (err %v), want false", on, err)
	}
	if err := st.SetMeta(metaSyncEnabled, "1"); err != nil {
		t.Fatal(err)
	}
	if on, err := s.syncEnabled(); err != nil || !on {
		t.Fatalf("syncEnabled after set 1 = %v (err %v), want true", on, err)
	}
	if err := st.SetMeta(metaSyncEnabled, "0"); err != nil {
		t.Fatal(err)
	}
	if on, err := s.syncEnabled(); err != nil || on {
		t.Fatalf("syncEnabled after set 0 = %v (err %v), want false", on, err)
	}
}
