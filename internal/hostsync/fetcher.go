package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/synckit/converge"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/syncservice"
)

// RemoteServeCmd is the command a peer runs to serve its account registry over
// the ssh-stdio bridge, streaming the registry JSON byte-exact.
const RemoteServeCmd = "cc-pool sync rpc-serve"

// execPeerPrefix marks a peer served by a local shell command instead of ssh —
// cc-pool's own convention, enabled only for the two-host sim harness.
const execPeerPrefix = "exec:"

// envExecPeer gates the exec: transport (sim-only). Unset in production, so an
// `exec:<cmd>` peer is treated as an ssh hostname, never `sh -c`; any non-empty
// value enables it.
const envExecPeer = "CCP_SYNC_EXEC_PEER"

// getStateTimeout bounds one peer registry read; a slow peer is treated as
// down for the pass. A var so tests shrink it.
var getStateTimeout = 15 * time.Second

// stateGetter is the read-only slice of syncservice.Client the fetcher
// consumes; its lack of a write method is the structural never-mutate-a-peer guard.
type stateGetter interface {
	GetState(ctx context.Context) (syncservice.RawRegistry, error)
	Close() error
}

// PeerTransport opens a transport to peer — an `exec:<cmd>` peer runs
// `sh -c <cmd>` locally ONLY when envExecPeer is set (sim harness); otherwise,
// and for every other peer, it is ssh-stdio driving RemoteServeCmd. The shared
// dialer for the registry fetch and the credential pull.
func PeerTransport(workers *supervise.Pool, peer string) syncservice.Transport {
	if cmd, ok := execPeerCommand(peer); ok {
		return syncservice.Stdio(workers, "sh", "-c", cmd)
	}
	return syncservice.SSHStdio(workers, peer, RemoteServeCmd)
}

// execPeerCommand reports the local shell command an exec: peer names, but only
// when the sim-only exec: transport is enabled. In production (envExecPeer
// unset) it always reports false, so a registry-injected `exec:<cmd>` never
// reaches a shell.
func execPeerCommand(peer string) (string, bool) {
	if os.Getenv(envExecPeer) == "" {
		return "", false
	}
	return strings.CutPrefix(peer, execPeerPrefix)
}

// SSHFetcher reads a peer's registry read-only for the pull-merge; a per-peer
// failure skips that peer, never aborting the pass.
type SSHFetcher struct {
	// dial opens a typed sync client to peer; tests inject a fake.
	dial func(peer string) stateGetter
}

// NewSSHFetcher builds the fetcher that dials each peer via PeerTransport.
func NewSSHFetcher(workers *supervise.Pool) (SSHFetcher, error) {
	if workers == nil {
		return SSHFetcher{}, errors.New("hostsync: SSH fetcher requires disposable workers")
	}
	return newSSHFetcher(func(peer string) stateGetter {
		return syncservice.NewClient(PeerTransport(workers, peer))
	}), nil
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
