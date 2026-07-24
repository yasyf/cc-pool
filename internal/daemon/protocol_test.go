package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/forecast"
	"github.com/yasyf/cc-pool/internal/score"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

func TestControlPlaneEpochIsHardReset(t *testing.T) {
	const prefix = "com.yasyf.cc-pool.control/"
	const suffix = "/v1"
	digest := strings.TrimSuffix(strings.TrimPrefix(WireBuild, prefix), suffix)
	if !strings.HasPrefix(WireBuild, prefix) || !strings.HasSuffix(WireBuild, suffix) || len(digest) != 64 {
		t.Fatalf("wire build = %q, want generated v1 schema identity", WireBuild)
	}
	for _, character := range digest {
		if !strings.ContainsRune("0123456789abcdef", character) {
			t.Fatalf("wire build = %q, want lowercase SHA-256 digest", WireBuild)
		}
	}
	if SnapshotVersion != 1 {
		t.Fatalf("snapshot version = %d", SnapshotVersion)
	}
	if DaemonHealthSchema != 1 {
		t.Fatalf("daemon health schema = %d", DaemonHealthSchema)
	}
}

func TestScoreComponentsWireProjectionIsExact(t *testing.T) {
	want := score.Components{
		Eff5: 1, Eff7: 2, RawRemaining5h: 3, RawRemaining7d: 4,
		Remaining5h: 5, Remaining7d: 6, SessionPenalty: 7,
		RateLimitPenalty: 8, NeedsLoginPenalty: 9, CredentialQuarantinePenalty: 10,
		StalePenalty: 11, Barrier5h: 12, Barrier7d: 13, RunwayPenalty: 14,
	}
	wireComponents := ScoreComponentsFromDomain(want)
	if got := ScoreComponentsToDomain(wireComponents); got != want {
		t.Fatalf("score component round trip = %+v, want %+v", got, want)
	}
}

func TestPoolMoodWireProjectionIsExact(t *testing.T) {
	for domain, want := range map[forecast.Mood]PoolMood{
		forecast.MoodChill: PoolMoodChill, forecast.MoodEasy: PoolMoodEasy,
		forecast.MoodUneasy: PoolMoodUneasy, forecast.MoodWorried: PoolMoodWorried,
		forecast.MoodAlarmed: PoolMoodAlarmed, forecast.MoodPanic: PoolMoodPanic,
	} {
		if got := poolMoodFromForecast(domain); got != want {
			t.Fatalf("pool mood %q = %q, want %q", domain, got, want)
		}
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

func TestServerRejectsOldClientBuildBeforeSession(t *testing.T) {
	var calls atomic.Int64
	socket := serveBuildTestServer(t, version.String(), &calls)
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	client, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: "0.0.1", Role: trust.UnprotectedRole, Ladder: ladder,
	})
	if !errors.Is(err, wire.ErrBuildMismatch) {
		t.Fatalf("old client handshake = %v, want build mismatch", err)
	}
	if client != nil {
		t.Fatal("old client build retained a session")
	}
	if calls.Load() != 0 {
		t.Fatalf("old client dispatched %d business handlers", calls.Load())
	}
}

type ordinaryTestProtectedClassifier struct{}

func (ordinaryTestProtectedClassifier) Validate() error { return nil }
func (ordinaryTestProtectedClassifier) Classify(context.Context, wire.Peer) (bool, error) {
	return false, nil
}
func (ordinaryTestProtectedClassifier) AuthorizeLifecycleBuild(string, string) bool { return true }

func TestRemovedLifecycleOperationsAreAbsent(t *testing.T) {
	socketDir, err := os.MkdirTemp("/tmp", "ccp-lifecycle-boundary-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "daemon.sock")
	server := &wire.Server{
		WireBuild: WireBuild, MaxSessions: 8,
	}
	startTestWireRuntime(t, socket, version.String(), server, ordinaryTestProtectedClassifier{}, nil)
	client, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: WireBuild, Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	for _, operation := range []wire.Op{
		"daemon.lifecycle.health", "daemon.lifecycle.shutdown", "daemon.lifecycle.handoff",
	} {
		t.Run(string(operation), func(t *testing.T) {
			result, callErr := client.Call(t.Context(), operation, "", []byte(`{}`))
			if callErr != nil {
				t.Fatal(callErr)
			}
			if result.Outcome != wire.Delivered || !strings.Contains(result.Response.Err, "wire: unknown op") {
				t.Fatalf("removed lifecycle %s = %+v, want unknown operation", operation, result)
			}
		})
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
	ladder, err := operationLadder()
	if err != nil {
		t.Fatal(err)
	}
	server := &wire.Server{
		WireBuild: build, Ladder: ladder, MaxSessions: 8,
	}
	for _, op := range []Op{OpStatus} {
		server.Register(wire.HandlerSpec{
			Op: wire.Op(op), Concurrent: true,
			Handler: func(context.Context, wire.Request) (any, error) {
				calls.Add(1)
				return Response{OK: true, Version: build}, nil
			},
		})
	}
	startTestWireRuntime(t, socket, build, server, buildTestProtectedClassifier{}, []wire.ObservationRoute{
		testHealthObservation(build, nil),
	})
	return socket
}
