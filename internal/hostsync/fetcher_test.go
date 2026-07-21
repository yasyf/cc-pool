package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/syncservice"
)

// fakeStateGetter serves a canned get_state reply and records Close; it has no
// write method, mirroring the fetcher's read-only loop guard.
type fakeStateGetter struct {
	raw    syncservice.RawRegistry
	err    error
	block  bool // block until ctx is done, then return ctx.Err (a wedged peer)
	closed bool
}

func (g *fakeStateGetter) GetState(ctx context.Context) (syncservice.RawRegistry, error) {
	if g.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return g.raw, g.err
}

func (g *fakeStateGetter) Close() error {
	g.closed = true
	return nil
}

// TestSSHFetcherRoundTripInt64Stamps proves Fetch decodes svc.get_state's bytes
// into the TYPED registry with its int64 CRDT stamps and chain expiry intact — a
// float64 detour would corrupt values past 2^53 — and closes the client when done.
func TestSSHFetcherRoundTripInt64Stamps(t *testing.T) {
	// MaxInt64-3 is not exactly representable as a float64, so a decode that
	// detoured through float64 would corrupt it — the bug this pins against.
	const big = int64(math.MaxInt64) - 3
	reg := cregistry.New[AccountValue]()
	val := AccountValue{
		UUID:         "u1",
		Email:        "e@x.com",
		Label:        "work",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1"}`),
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: big, Hash: "h", RotatedAt: big - 1},
	}
	reg.Add("u1", val, cregistry.Micros(big-4))

	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}
	getter := &fakeStateGetter{raw: syncservice.RawRegistry(body)}
	fetcher := newSSHFetcher(func(string) stateGetter { return getter })

	got, err := fetcher.Fetch(context.Background(), "hostA")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	e, ok := got["u1"]
	if !ok {
		t.Fatal("fetched registry missing u1")
	}
	checks := []struct {
		name      string
		got, want int64
	}{
		{"Added", int64(e.Added), big - 4},
		{"Chain.ExpiresAt", e.Value.Chain.ExpiresAt, big},
		{"Chain.RotatedAt", e.Value.Chain.RotatedAt, big - 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d (int64 corrupted through decode)", c.name, c.got, c.want)
		}
	}
	if e.Value.Chain.Origin != "hostA" || e.Value.Label != "work" {
		t.Errorf("scalar fields not preserved: %+v", e.Value)
	}
	if !getter.closed {
		t.Fatal("fetcher did not close the client")
	}
}

// TestSSHFetcherWrapsPeerError proves a peer that fails the read (or serves
// unparseable bytes) surfaces a wrapped error naming the peer, so converge can skip
// it — never a nil registry treated as empty.
func TestSSHFetcherWrapsPeerError(t *testing.T) {
	t.Run("read error", func(t *testing.T) {
		getter := &fakeStateGetter{err: errors.New("connection refused")}
		fetcher := newSSHFetcher(func(string) stateGetter { return getter })
		reg, err := fetcher.Fetch(context.Background(), "you@desktop")
		if reg != nil {
			t.Fatalf("registry = %v, want nil on error", reg)
		}
		if err == nil || !strings.Contains(err.Error(), "get_state from you@desktop") {
			t.Fatalf("err = %v, want it to name the peer", err)
		}
		if !getter.closed {
			t.Fatal("client not closed on the error path")
		}
	})

	t.Run("unparseable registry", func(t *testing.T) {
		getter := &fakeStateGetter{raw: syncservice.RawRegistry(`{not json`)}
		fetcher := newSSHFetcher(func(string) stateGetter { return getter })
		reg, err := fetcher.Fetch(context.Background(), "peerA")
		if reg != nil {
			t.Fatalf("registry = %v, want nil on parse error", reg)
		}
		if err == nil || !strings.Contains(err.Error(), "parse registry from peerA") {
			t.Fatalf("err = %v, want a parse error naming the peer", err)
		}
	})
}

// TestSSHFetcherDeadline proves a wedged peer makes Fetch fail at getStateTimeout
// with a context.DeadlineExceeded error naming the peer, so it never hangs a pass.
func TestSSHFetcherDeadline(t *testing.T) {
	prev := getStateTimeout
	getStateTimeout = 25 * time.Millisecond
	t.Cleanup(func() { getStateTimeout = prev })

	getter := &fakeStateGetter{block: true}
	fetcher := newSSHFetcher(func(string) stateGetter { return getter })

	start := time.Now()
	_, err := fetcher.Fetch(context.Background(), "you@desktop")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Fetch took %v, want failure at the ~25ms deadline", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Fetch error = %v, want context.DeadlineExceeded", err)
	}
	if want := "get_state from you@desktop"; !strings.Contains(err.Error(), want) {
		t.Fatalf("Fetch error %q does not name the operation and peer", err)
	}
	if !getter.closed {
		t.Fatal("client not closed on the deadline path")
	}
}

// TestExecPeerCommandGate pins the sim-only exec: gate: only with envExecPeer set
// does an exec: peer name a local shell command. Unset (production), an exec:
// peer is never a shell command, so PeerTransport dials it as an ssh host —
// never sh -c — and a registry-injected "exec:<cmd>" can't reach a shell.
func TestExecPeerCommandGate(t *testing.T) {
	t.Run("disabled in production", func(t *testing.T) {
		t.Setenv(envExecPeer, "")
		if cmd, ok := execPeerCommand("exec:touch /tmp/pwned"); ok {
			t.Fatalf("execPeerCommand = (%q, true) with the gate unset; want no shell command", cmd)
		}
	})
	t.Run("enabled for the sim", func(t *testing.T) {
		t.Setenv(envExecPeer, "1")
		cmd, ok := execPeerCommand("exec:touch /tmp/x")
		if !ok || cmd != "touch /tmp/x" {
			t.Fatalf(`execPeerCommand = (%q, %v), want ("touch /tmp/x", true)`, cmd, ok)
		}
	})
	t.Run("non-exec peer is never a shell command", func(t *testing.T) {
		t.Setenv(envExecPeer, "1")
		if cmd, ok := execPeerCommand("you@desktop"); ok {
			t.Fatalf("execPeerCommand = (%q, true) for an ssh host; want false", cmd)
		}
	})
}

// TestPeerTransportExecServesRegistry pins cc-pool's exec: peer convention
// through the real dial: a local `sh -c` command serves the registry through
// syncservice's real framing, int64 stamps intact.
func TestPeerTransportExecServesRegistry(t *testing.T) {
	t.Setenv(envExecPeer, "1") // exec: peers are sim-only; enable for this test
	const big = int64(math.MaxInt64) - 5
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", AccountValue{
		UUID:         "u1",
		Email:        "e@x.com",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1"}`),
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: big, Hash: "h", RotatedAt: big - 1},
	}, cregistry.Micros(big-2))
	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}

	stateFile := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(stateFile, body, 0o600); err != nil {
		t.Fatalf("write registry file: %v", err)
	}
	script := "env " + testRPCStateEnv + "=" + strconv.Quote(stateFile) + " " + strconv.Quote(os.Args[0])

	fetcher := NewSSHFetcher()
	got, err := fetcher.Fetch(context.Background(), execPeerPrefix+script)
	if err != nil {
		t.Fatalf("Fetch over exec: peer: %v", err)
	}
	e, ok := got["u1"]
	if !ok {
		t.Fatal("exec: peer registry missing u1")
	}
	if int64(e.Added) != big-2 || e.Value.Chain.ExpiresAt != big {
		t.Fatalf("int64 stamps corrupted through the real bridge: added=%d expiry=%d, want %d/%d",
			int64(e.Added), e.Value.Chain.ExpiresAt, big-2, big)
	}
	if e.Value.Chain.Origin != "hostA" {
		t.Fatalf("value not round-tripped through the exec: bridge: %+v", e.Value)
	}
}
