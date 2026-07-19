package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/daemonkit/wire"
)

func TestServerRejectsLFProtocol(t *testing.T) {
	s, _ := newTestServer(t)
	called := make(chan struct{}, 1)
	s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus {
		called <- struct{}{}
		return FPBridgeStatus{Verdict: FPBridgeServing}
	}
	socket := serveHandlerOnSocket(t, s)
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("{\"op\":\"fpbridgecheck\"}\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err == nil {
		t.Fatalf("LF client received a business response: %+v", response)
	}
	select {
	case <-called:
		t.Fatal("LF request reached a business handler")
	default:
	}
}

func TestServerRejectsMismatchedBuildBeforeDispatch(t *testing.T) {
	s, _ := newTestServer(t)
	called := make(chan struct{}, 1)
	s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus {
		called <- struct{}{}
		return FPBridgeStatus{Verdict: FPBridgeServing}
	}
	client := &Client{
		socket:   serveHandlerOnSocket(t, s),
		build:    "0.0.0-wrong",
		sessions: make(map[*clientSession]struct{}),
	}
	t.Cleanup(func() { _ = client.Close() })
	_, err := client.FPBridgeCheck()
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Outcome != wire.Rejected {
		t.Fatalf("mismatched build err = %v, want rejected CallError", err)
	}
	select {
	case <-called:
		t.Fatal("mismatched build reached a business handler")
	default:
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

func TestEnsureRunningAcceptsExactLifecycle(t *testing.T) {
	s, _ := newTestServer(t)
	client := &Client{socket: serveHandlerOnSocket(t, s), sessions: make(map[*clientSession]struct{})}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if !client.EnsureRunning(ctx, time.Second) {
		t.Fatal("EnsureRunning rejected an exact live daemon")
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

func TestHolderStatusWire(t *testing.T) {
	b, err := json.Marshal(Response{OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "holder") {
		t.Fatalf("empty response leaked a holder key: %s", b)
	}
	in := Response{OK: true, Holder: &HolderStatus{Version: "9.9.9", Mounts: 2}}
	b, err = json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Response
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.Holder, out.Holder) {
		t.Fatalf("holder did not round-trip: %+v != %+v", out.Holder, in.Holder)
	}
}

func TestHandleStatusCarriesHolderState(t *testing.T) {
	s, _ := newTestServer(t)
	resp := s.handleStatus(t.Context())
	if !resp.OK || resp.Holder == nil {
		t.Fatalf("status = %+v, want a holder view", resp)
	}
	if resp.Holder.Version != "" || resp.Holder.Mounts != 0 {
		t.Fatalf("zero-cache holder = %+v, want the unreachable shape", resp.Holder)
	}
	s.holder.mu.Lock()
	s.holder.healthy = true
	s.holder.version = "0.0.9-old"
	s.holder.mounts = map[string]bool{"/a": true, "/b": false}
	s.holder.mu.Unlock()
	resp = s.handleStatus(t.Context())
	want := &HolderStatus{Version: "0.0.9-old", Mounts: 1}
	if !reflect.DeepEqual(resp.Holder, want) {
		t.Fatalf("holder = %+v, want %+v", resp.Holder, want)
	}
}
