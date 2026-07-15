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
	"testing"
	"time"
)

func TestServerRejectsIncompatibleProtocol(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	s := &Server{}
	go s.handle(t.Context(), server)
	if err := json.NewEncoder(client).Encode(Request{Proto: ProtocolVersion - 1, Op: OpHealth}); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(client).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK || resp.Proto != ProtocolVersion || !strings.Contains(resp.Error, "unsupported protocol") {
		t.Fatalf("protocol rejection = %+v", resp)
	}
}

func TestClientRejectsIncompatibleProtocol(t *testing.T) {
	f, err := os.CreateTemp("/tmp", "ccp-proto-*.sock")
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
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var req Request
		_ = json.NewDecoder(conn).Decode(&req)
		_ = json.NewEncoder(conn).Encode(Response{Proto: ProtocolVersion - 1, OK: true})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = (&Client{socket: socket}).HealthContext(ctx)
	if !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("HealthContext err = %v, want protocol mismatch", err)
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

func TestCommitSelectionRequiresFullConfirmationWindowBeforeDial(t *testing.T) {
	f, err := os.CreateTemp("/tmp", "ccp-commit-window-*.sock")
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = (&Client{socket: socket}).CommitSelection(ctx, "token")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CommitSelection err = %v, want deadline before send", err)
	}
	unix := ln.(*net.UnixListener)
	_ = unix.SetDeadline(time.Now().Add(25 * time.Millisecond))
	conn, acceptErr := unix.Accept()
	if conn != nil {
		_ = conn.Close()
	}
	var netErr net.Error
	if !errors.As(acceptErr, &netErr) || !netErr.Timeout() {
		t.Fatalf("commit dialed without a full confirmation window: accept err=%v", acceptErr)
	}
}

func TestHolderStatusWireAdditive(t *testing.T) {
	b, err := json.Marshal(Response{OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "holder") {
		t.Fatalf("empty response leaked a holder key: %s", b)
	}

	var old Response
	if err := json.Unmarshal([]byte(`{"proto":1,"ok":true}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Holder != nil {
		t.Fatalf("old-shape response decoded a phantom holder: %+v", old.Holder)
	}

	in := Response{OK: true, Holder: &HolderStatus{Version: "9.9.9", Mounts: 2, TCCError: "grant pending"}}
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
	s.holder.tccErr = "grant pending"
	s.holder.mu.Unlock()

	resp = s.handleStatus(t.Context())
	want := &HolderStatus{Version: "0.0.9-old", Mounts: 1, TCCError: "grant pending"}
	if !reflect.DeepEqual(resp.Holder, want) {
		t.Fatalf("holder = %+v, want %+v", resp.Holder, want)
	}
}
