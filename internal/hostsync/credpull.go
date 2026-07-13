package hostsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// MethodFetchCredential is the credential fetch RPC — the only path a
// credential ever crosses between hosts, always stripped of its refresh
// token; namespaced under ccp. clear of the svc.* contract.
const MethodFetchCredential = "ccp.fetch_credential" //nolint:gosec // G101: a JSON-RPC method name, not a credential

// FetchTimeout bounds each per-peer fetch attempt; a var so tests shrink it.
var FetchTimeout = 15 * time.Second

// ErrNoPeerCredential is the deferred outcome of a pull: no peer offered an
// acceptable credential; the caller retries on a later tick.
var ErrNoPeerCredential = errors.New("no peer offered an acceptable credential")

// CredentialEnvelope is the wire result of MethodFetchCredential: the
// stripped credential blob plus the stamp fields the client verifies.
type CredentialEnvelope struct {
	// Credential is the marshaled stripped creds.Credential.
	Credential json.RawMessage `json:"credential"`
	// ExpiresAt is the access-token expiry in Unix epoch milliseconds.
	ExpiresAt int64 `json:"expiresAt"`
	// Hash is creds.AccessHash of the enclosed credential.
	Hash string `json:"hash"`
}

// AccountLookup resolves an account UUID to its local pool row;
// store.(*Store).GetAccountByUUID satisfies it.
type AccountLookup func(uuid string) (store.Account, bool, error)

// CredentialReader reads an account's credential under its per-account lock;
// it must never refresh or write — the fetch handler is strictly read-only.
type CredentialReader func(ctx context.Context, a store.Account) (*creds.Credential, error)

// NewFetchCredentialHandler returns the daemon-side MethodFetchCredential
// handler; it never writes, and it serves cred.Strip() — the refresh token
// never leaves the origin process.
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
		stripped := cred.Strip()
		blob, err := stripped.Marshal()
		if err != nil {
			return nil, err
		}
		// The secret boundary, asserted on the exact wire bytes.
		if rt := cred.ClaudeAiOauth.RefreshToken; rt != "" && bytes.Contains(blob, []byte(rt)) {
			return nil, fmt.Errorf("%s: refusing to serve acct-%d: stripped envelope still carries the refresh token", MethodFetchCredential, a.ID)
		}
		return CredentialEnvelope{
			Credential: blob,
			ExpiresAt:  stripped.ClaudeAiOauth.ExpiresAt,
			Hash:       creds.AccessHash(stripped),
		}, nil
	}
}

// DialTransport opens a syncservice transport to a peer for one fetch attempt;
// FetchCredential closes it when the attempt ends.
type DialTransport func(peer string) syncservice.Transport

// FetchCredential pulls uuid's stripped credential — origin first, then the
// relay peers — and accepts only a valid synced envelope strictly fresher than
// the local credential: the origin is authoritative for its own chain (a
// self-consistent answer ahead of the registry stamp is mirror lag, not
// corruption), while a relay must match the registry hash exactly. All peers
// failing is the deferred outcome ErrNoPeerCredential.
func FetchCredential(ctx context.Context, dial DialTransport, uuid string, chain ChainStamp, localExpiresAt int64, peers []string) (*creds.Credential, error) {
	var attempts []string
	for _, peer := range fetchOrder(chain.Origin, peers) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cred, err := fetchFromPeer(ctx, dial, peer, uuid, chain, localExpiresAt, peer == chain.Origin)
		if err != nil {
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

// fetchOrder returns the peers to try in order: the origin first, then the
// remaining peers, with empties and duplicates dropped.
func fetchOrder(origin string, peers []string) []string {
	order := make([]string, 0, len(peers)+1)
	seen := map[string]bool{"": true}
	for _, p := range append([]string{origin}, peers...) {
		if seen[p] {
			continue
		}
		seen[p] = true
		order = append(order, p)
	}
	return order
}

// fetchFromPeer runs one bounded attempt against peer, verifying the parsed
// credential by recomputation: it must be a valid stripped synced credential,
// hash-consistent with its envelope, registry-matching unless peer is the
// origin, and strictly fresher than the local credential.
func fetchFromPeer(ctx context.Context, dial DialTransport, peer, uuid string, chain ChainStamp, localExpiresAt int64, isOrigin bool) (*creds.Credential, error) {
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
	var cred creds.Credential
	if err := json.Unmarshal(env.Credential, &cred); err != nil {
		return nil, fmt.Errorf("parse credential: %w", err)
	}
	switch {
	case cred.HasRefreshToken():
		// A downrev peer serving unstripped envelopes: never installable.
		return nil, pool.ErrEnvelopeCarriesSecret
	case !cred.Synced():
		return nil, pool.ErrEnvelopeNoAccessToken
	}
	if creds.AccessHash(&cred) != env.Hash {
		return nil, errors.New("credential does not hash to the advertised envelope hash")
	}
	if !isOrigin && env.Hash != chain.Hash {
		return nil, errors.New("relay answer does not match the registry chain (stale peer registry)")
	}
	if cred.ClaudeAiOauth.ExpiresAt <= localExpiresAt {
		return nil, fmt.Errorf("credential expiry %d is not strictly later than local %d", cred.ClaudeAiOauth.ExpiresAt, localExpiresAt)
	}
	return &cred, nil
}
