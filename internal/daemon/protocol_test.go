package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	dkdaemon "github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
)

func TestControlPlaneEpochIsHardReset(t *testing.T) {
	if BusinessBuild != "cc-pool.rpc.v1" {
		t.Fatalf("business build = %q", BusinessBuild)
	}
	if SnapshotVersion != 1 {
		t.Fatalf("snapshot version = %d", SnapshotVersion)
	}
}

func TestServerRejectsLFProtocol(t *testing.T) {
	s, _ := newTestServer(t)
	socket := serveHandlerOnSocket(t, s)
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("{\"op\":\"status\"}\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err == nil {
		t.Fatalf("LF client received a business response: %+v", response)
	}
}

func TestClientRejectsMismatchedBuildBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		build string
	}{
		{name: "older", build: "0.0.1"},
		{name: "newer", build: "9999.9999.9999"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			socket := serveBuildTestServer(t, test.build, &calls)
			client := &Client{
				socket: socket, build: version.String(),
				sessions: make(map[*clientSession]struct{}),
			}
			t.Cleanup(func() { _ = client.Close() })
			_, err := client.StatusContext(t.Context())
			var callErr *CallError
			if !errors.As(err, &callErr) || callErr.Outcome != wire.PreSendFailure ||
				!errors.Is(err, ErrDaemonBuildMismatch) {
				t.Fatalf("mismatched build err = %v, want pre-send ErrDaemonBuildMismatch", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("mismatched build dispatched %d handlers", calls.Load())
			}
			client.mu.Lock()
			current, sessions := client.current, len(client.sessions)
			client.mu.Unlock()
			if current != nil || sessions != 0 {
				t.Fatalf("mismatched build retained current=%p sessions=%d", current, sessions)
			}
		})
	}
}

func TestClientAdmitsExactBuild(t *testing.T) {
	var calls atomic.Int64
	socket := serveBuildTestServer(t, version.String(), &calls)
	client := &Client{
		socket: socket, build: version.String(),
		sessions: make(map[*clientSession]struct{}),
	}
	response, err := client.StatusContext(t.Context())
	if err != nil || !response.OK {
		t.Fatalf("exact build status = %+v, %v", response, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("exact build dispatched %d handlers, want 1", calls.Load())
	}
	client.mu.Lock()
	current, sessions := client.current, len(client.sessions)
	client.mu.Unlock()
	if current == nil || sessions != 1 {
		t.Fatalf("exact build retained current=%p sessions=%d, want one live session", current, sessions)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	current, sessions = client.current, len(client.sessions)
	client.mu.Unlock()
	if current != nil || sessions != 0 {
		t.Fatalf("closed exact client retained current=%p sessions=%d", current, sessions)
	}
}

func TestServerRejectsOldClientBuildBeforeBusinessDispatch(t *testing.T) {
	var calls atomic.Int64
	socket := serveBuildTestServer(t, version.String(), &calls)
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	client, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), Build: "0.0.1", Ladder: ladder,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Call(t.Context(), wire.Op(OpStatus), "", []byte(`{"op":"status"}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != wire.Rejected || result.Response.Reason != wire.ErrBuildMismatch.Error() {
		t.Fatalf("old client result = %+v, want pre-dispatch build rejection", result)
	}
	if calls.Load() != 0 {
		t.Fatalf("old client dispatched %d business handlers", calls.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientReusesPersistentSession(t *testing.T) {
	s, _ := newTestServer(t)
	client := &Client{socket: serveHandlerOnSocket(t, s), sessions: make(map[*clientSession]struct{})}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.StatusContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	first := client.current
	client.mu.Unlock()
	if _, err := client.StatusContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	second := client.current
	client.mu.Unlock()
	if first == nil || first != second {
		t.Fatalf("persistent session changed: first=%p second=%p", first, second)
	}
}

func TestClientMultiplexesConcurrentCallsOnOneSession(t *testing.T) {
	s, _ := newTestServer(t)
	client := &Client{socket: serveHandlerOnSocket(t, s), sessions: make(map[*clientSession]struct{})}
	t.Cleanup(func() { _ = client.Close() })
	const callers = 16
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.StatusContext(t.Context())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	client.mu.Lock()
	generation := client.generation
	sessions := len(client.sessions)
	client.mu.Unlock()
	if generation != 1 || sessions != 1 {
		t.Fatalf("client sessions = generation %d, live %d; want one multiplexed session", generation, sessions)
	}
}

func TestClientSelectRequiresReachableDaemon(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ccp-missing-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	client := &Client{socket: filepath.Join(dir, "missing.sock")}
	_, err = client.Select(t.Context(), nil, store.ProcessIdentity{}, "/project", false, nil)
	if !errors.Is(err, ErrDaemonUnavailable) {
		t.Fatalf("Select without daemon = %v, want ErrDaemonUnavailable", err)
	}
}

func TestStatusContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := (&Client{socket: filepath.Join(t.TempDir(), "missing.sock")}).StatusContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StatusContext err = %v, want context cancellation", err)
	}
}

func TestCommitSelectionPreCanceledDoesNotDial(t *testing.T) {
	f, err := os.CreateTemp("/tmp", "ccp-commit-cancel-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	socket := f.Name()
	_ = f.Close()
	_ = os.Remove(socket)
	t.Cleanup(func() { _ = os.Remove(socket) })
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = (&Client{socket: socket}).CommitSelection(ctx, "token")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitSelection err = %v, want context cancellation", err)
	}
	unix := ln.(*net.UnixListener)
	_ = unix.SetDeadline(time.Now().Add(25 * time.Millisecond))
	conn, acceptErr := unix.Accept()
	if conn != nil {
		_ = conn.Close()
	}
	var netErr net.Error
	if !errors.As(acceptErr, &netErr) || !netErr.Timeout() {
		t.Fatalf("pre-canceled commit dialed: accept err=%v", acceptErr)
	}
}

type buildTestLifecycle struct{ build string }

func (l buildTestLifecycle) Health(context.Context) (dkdaemon.Health, error) {
	return dkdaemon.Health{
		Build: l.build, Protocol: int(wire.ProtocolVersion), PID: os.Getpid(),
		State: dkdaemon.StateHealthy,
	}, nil
}

func (buildTestLifecycle) Shutdown(context.Context) error { return nil }
func (buildTestLifecycle) Handoff(context.Context) error  { return nil }

type buildTestProtectedClassifier struct{}

func (buildTestProtectedClassifier) Validate() error { return nil }
func (buildTestProtectedClassifier) Classify(context.Context, wire.Peer) (bool, error) {
	return true, nil
}
func (buildTestProtectedClassifier) AuthorizeLifecycleBuild(string, string) bool { return true }

func serveBuildTestServer(
	t *testing.T,
	build string,
	calls *atomic.Int64,
) string {
	t.Helper()
	socketDir, err := os.MkdirTemp("/tmp", "ccp-build-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	server := &wire.Server{
		Build: build, LifecycleBuild: build, Ladder: ladder,
		MaxSessions: 2, ReservedProtectedSessions: 1,
		ProtectedSessionClassifier: buildTestProtectedClassifier{},
	}
	server.RegisterConcurrent(wire.Op(OpStatus), func(context.Context, wire.Request) (any, error) {
		calls.Add(1)
		return Response{OK: true, Version: build}, nil
	})
	server.RegisterLifecycle(buildTestLifecycle{build: build})
	intake := &drain.Intake{}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener, func() error { return nil }, intake.Admit, intake.AdmitLifecycle)
	}()
	t.Cleanup(func() {
		intake.Close()
		_ = server.CloseIntake()
		_ = intake.Settle(context.Background())
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("serve build test socket: %v", err)
		}
	})
	return socket
}
