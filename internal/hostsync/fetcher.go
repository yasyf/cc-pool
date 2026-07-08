package hostsync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/syncservice"
)

// RemoteServeCmd is the command a peer runs to serve its account registry over
// the ssh-stdio bridge, streaming the registry JSON byte-exact.
const RemoteServeCmd = "cc-pool sync rpc-serve"

// execPeerPrefix marks a peer served by a local shell command instead of ssh —
// cc-pool's own convention, for the two-host sim harness.
const execPeerPrefix = "exec:"

// getStateTimeout bounds one peer registry read; a slow peer is treated as
// down for the pass. A var so tests shrink it.
var getStateTimeout = 15 * time.Second

// stateGetter is the read-only slice of syncservice.Client the fetcher
// consumes; its lack of a write method is the structural never-mutate-a-peer guard.
type stateGetter interface {
	GetState(ctx context.Context) (syncservice.RawRegistry, error)
	Close() error
}

// PeerTransport opens a transport to peer — `exec:<cmd>` runs `sh -c <cmd>`
// locally, anything else is ssh-stdio driving RemoteServeCmd; the shared dialer
// for the registry fetch and the credential pull.
func PeerTransport(peer string) syncservice.Transport {
	if cmd, ok := strings.CutPrefix(peer, execPeerPrefix); ok {
		return syncservice.Stdio("sh", "-c", cmd)
	}
	return syncservice.SSHStdio(peer, RemoteServeCmd)
}

// SSHFetcher reads a peer's registry read-only for the pull-merge; a per-peer
// failure skips that peer, never aborting the pass.
type SSHFetcher struct {
	// dial opens a typed sync client to peer; tests inject a fake.
	dial func(peer string) stateGetter
}

// NewSSHFetcher builds the fetcher that dials each peer via PeerTransport.
func NewSSHFetcher() SSHFetcher {
	return newSSHFetcher(func(peer string) stateGetter {
		return syncservice.NewClient(PeerTransport(peer))
	})
}

// newSSHFetcher builds the fetcher over an injected dial for tests.
func newSSHFetcher(dial func(peer string) stateGetter) SSHFetcher {
	return SSHFetcher{dial: dial}
}

// Fetch returns peer's registry without modifying it, decoded into the typed
// Registry so the int64 stamps survive byte-exact; a failure is wrapped with
// the peer name and skips that peer for the pass.
func (f SSHFetcher) Fetch(ctx context.Context, peer string) (cregistry.Registry[AccountValue], error) {
	ctx, cancel := context.WithTimeout(ctx, getStateTimeout)
	defer cancel()
	c := f.dial(peer)
	defer func() { _ = c.Close() }()
	raw, err := c.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("get_state from %s: %w", peer, err)
	}
	reg := cregistry.New[AccountValue]()
	if err := json.Unmarshal(raw, &reg); err != nil {
		return nil, fmt.Errorf("parse registry from %s: %w", peer, err)
	}
	return reg, nil
}

// SSHFetcher satisfies converge.Fetcher for the account registry.
var _ converge.Fetcher[AccountValue] = SSHFetcher{}
