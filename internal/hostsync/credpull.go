package hostsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// MethodFetchCredential is the credential fetch RPC — the only path a secret
// ever crosses between hosts; namespaced under ccp. clear of the svc.* contract.
const MethodFetchCredential = "ccp.fetch_credential"

// FetchTimeout bounds each per-peer fetch attempt; a var so tests shrink it.
var FetchTimeout = 15 * time.Second

// ErrNoPeerCredential is the deferred outcome of a pull: no peer offered an
// acceptable credential; the caller retries on a later tick.
var ErrNoPeerCredential = errors.New("no peer offered an acceptable credential")

// errHolderAhead halts a pull when the holder's answer proves the registry
// stale: falling back would install an already-spent chain — see ccn 10bf17d.
var errHolderAhead = fmt.Errorf("holder chain is ahead of the registry (mirror lag): %w", ErrNoPeerCredential)

// CredentialEnvelope is the wire result of MethodFetchCredential: the
// credential blob byte-exact plus the stamp fields the client verifies.
type CredentialEnvelope struct {
	// Credential is the marshaled creds.Credential, byte-exact.
	Credential json.RawMessage `json:"credential"`
	// ExpiresAt is the access-token expiry in Unix epoch milliseconds.
	ExpiresAt int64 `json:"expiresAt"`
	// Hash is CredentialHash of the enclosed credential.
	Hash string `json:"hash"`
}

// AccountLookup resolves an account UUID to its local pool row;
// store.(*Store).GetAccountByUUID satisfies it.
type AccountLookup func(uuid string) (store.Account, bool, error)

// CredentialReader reads an account's credential under its per-account lock;
// it must never refresh or write — the fetch handler is strictly read-only.
type CredentialReader func(ctx context.Context, a store.Account) (*creds.Credential, error)

// NewFetchCredentialHandler returns the daemon-side MethodFetchCredential
// handler; it never writes — serving a fetch must not disturb the chain.
func NewFetchCredentialHandler(lookup AccountLookup, read CredentialReader) rpc.Handler {
	return func(ctx context.Context, params map[string]any) (any, error) {
		uuid, _ := params["uuid"].(string)
		if uuid == "" {
			return nil, fmt.Errorf("%s: missing uuid param", MethodFetchCredential)
		}
		a, ok, err := lookup(uuid)
		if err != nil {
			return nil, fmt.Errorf("resolve account %s: %w", uuid, err)
		}
		if !ok {
			return nil, fmt.Errorf("%s: unknown account uuid %s", MethodFetchCredential, uuid)
		}
		cred, err := read(ctx, a)
		if err != nil {
			return nil, fmt.Errorf("read acct-%d credential: %w", a.ID, err)
		}
		blob, err := cred.Marshal()
		if err != nil {
			return nil, err
		}
		return CredentialEnvelope{
			Credential: blob,
			ExpiresAt:  cred.ClaudeAiOauth.ExpiresAt,
			Hash:       CredentialHash(cred),
		}, nil
	}
}

// DialTransport opens a syncservice transport to a peer for one fetch attempt;
// FetchCredential closes it when the attempt ends.
type DialTransport func(peer string) syncservice.Transport

// FetchCredential pulls uuid's credential — holder first, then peers — and
// accepts only an envelope that recomputes to chain.Hash and beats the local
// credential by lineage or strictly-later expiry; all peers failing is the
// deferred outcome ErrNoPeerCredential.
func FetchCredential(ctx context.Context, dial DialTransport, uuid string, chain ChainStamp, localExpiresAt int64, localHash string, peers []string) (*creds.Credential, error) {
	var attempts []string
	for _, peer := range fetchOrder(chain.Holder, peers) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cred, err := fetchFromPeer(ctx, dial, peer, uuid, chain, localExpiresAt, localHash, peer == chain.Holder)
		if err != nil {
			if errors.Is(err, errHolderAhead) {
				return nil, fmt.Errorf("fetch credential for %s: %w", uuid, err)
			}
			attempts = append(attempts, fmt.Sprintf("%s: %v", peer, err))
			continue
		}
		return cred, nil
	}
	if len(attempts) == 0 {
		return nil, fmt.Errorf("fetch credential for %s: no candidate peers: %w", uuid, ErrNoPeerCredential)
	}
	return nil, fmt.Errorf("fetch credential for %s: %w (%s)", uuid, ErrNoPeerCredential, strings.Join(attempts, "; "))
}

// fetchOrder returns the peers to try in order: the holder first, then the
// remaining peers, with empties and duplicates dropped.
func fetchOrder(holder string, peers []string) []string {
	order := make([]string, 0, len(peers)+1)
	seen := map[string]bool{"": true}
	for _, p := range append([]string{holder}, peers...) {
		if seen[p] {
			continue
		}
		seen[p] = true
		order = append(order, p)
	}
	return order
}

// fetchFromPeer runs one bounded attempt against peer: the advertised stamp
// fields gate cheaply, then the parsed credential is re-verified by recomputation.
func fetchFromPeer(ctx context.Context, dial DialTransport, peer, uuid string, chain ChainStamp, localExpiresAt int64, localHash string, isHolder bool) (*creds.Credential, error) {
	ctx, cancel := context.WithTimeout(ctx, FetchTimeout)
	defer cancel()
	tx := dial(peer)
	defer func() { _ = tx.Close() }()

	resp, err := tx.Do(ctx, &rpc.Request{Method: MethodFetchCredential, Params: map[string]any{"uuid": uuid}})
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", MethodFetchCredential, err)
	}
	if !resp.OK {
		return nil, fmt.Errorf("call %s: %s", MethodFetchCredential, resp.Error)
	}
	var env CredentialEnvelope
	if err := json.Unmarshal(resp.Result, &env); err != nil {
		return nil, fmt.Errorf("decode credential envelope: %w", err)
	}
	if env.Hash != chain.Hash {
		if isHolder && holderAhead(env, chain) {
			return nil, errHolderAhead
		}
		return nil, errors.New("advertised hash does not match the registry chain (stale peer registry)")
	}
	childOfLocal := chain.ParentHash != "" && chain.ParentHash == localHash
	if env.ExpiresAt <= localExpiresAt && !childOfLocal {
		return nil, fmt.Errorf("advertised expiry %d is not strictly later than local %d", env.ExpiresAt, localExpiresAt)
	}
	var cred creds.Credential
	if err := json.Unmarshal(env.Credential, &cred); err != nil {
		return nil, fmt.Errorf("parse credential: %w", err)
	}
	if CredentialHash(&cred) != chain.Hash {
		return nil, errors.New("credential does not hash to the registry chain")
	}
	if cred.ClaudeAiOauth.ExpiresAt <= localExpiresAt && !childOfLocal {
		return nil, fmt.Errorf("credential expiry %d is not strictly later than local %d", cred.ClaudeAiOauth.ExpiresAt, localExpiresAt)
	}
	return &cred, nil
}

// holderAhead reports a self-consistent holder answer strictly fresher than
// the registry chain; an older or corrupt answer still falls back — see ccn 10bf17d.
func holderAhead(env CredentialEnvelope, chain ChainStamp) bool {
	var cred creds.Credential
	if err := json.Unmarshal(env.Credential, &cred); err != nil {
		return false
	}
	return CredentialHash(&cred) == env.Hash && cred.ClaudeAiOauth.ExpiresAt > chain.ExpiresAt
}
