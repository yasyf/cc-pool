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
// token; namespaced under ccp. clear of the svc.* contract. Its name is
// deliberately distinct from the removed "ccp.fetch_credential", which served
// the full refresh-token-bearing blob. The fresh v1 protocol never registers
// that method, so a stale caller receives unknown-method without a secret.
const MethodFetchCredential = "ccp.fetch_stripped_credential" //nolint:gosec // G101: a JSON-RPC method name, not a credential

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

// CredentialReader reads an account without taking a mutation lane; it must
// never refresh or write — the fetch handler is strictly read-only.
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
// failing is the deferred outcome ErrNoPeerCredential; per-peer rejection
// sentinels (pool.ErrEnvelopeCarriesSecret, pool.ErrEnvelopeNoAccessToken)
// stay errors.Is-reachable so the materializer can pick the non-destructive
// rollback.
func FetchCredential(ctx context.Context, dial DialTransport, uuid string, chain ChainStamp, localExpiresAt int64, peers []string) (*creds.Credential, error) {
	var attempts []error
	for _, peer := range fetchOrder(chain.Origin, peers) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cred, err := fetchFromPeer(ctx, dial, peer, uuid, chain, localExpiresAt, peer == chain.Origin)
		if err != nil {
			attempts = append(attempts, fmt.Errorf("%s: %w", peer, err))
			continue
		}
		return cred, nil
	}
	if len(attempts) == 0 {
		return nil, fmt.Errorf("fetch credential for %s: no candidate peers: %w", uuid, ErrNoPeerCredential)
	}
	return nil, fmt.Errorf("fetch credential for %s: %w: %w", uuid, ErrNoPeerCredential, errors.Join(attempts...))
}

// fetchOrder returns the dial targets in order. peers is the TRUSTED configured
// mesh; origin rides the synced registry and is attacker-controllable, so it may
// only PRIORITIZE a peer that is already a mesh member — it is never itself a new
// dial target. A non-member origin (a registry-injected "exec:<cmd>", or any host
// not in the configured mesh) is dropped, so a synced value can never introduce a
// peer to dial. Empties, duplicates, and implausible identities are dropped.
func fetchOrder(origin string, peers []string) []string {
	trusted := make(map[string]bool, len(peers))
	for _, p := range peers {
		if plausiblePeer(p) {
			trusted[p] = true
		}
	}
	order := make([]string, 0, len(peers))
	seen := map[string]bool{}
	if trusted[origin] {
		order = append(order, origin)
		seen[origin] = true
	}
	for _, p := range peers {
		if seen[p] || !trusted[p] {
			continue
		}
		seen[p] = true
		order = append(order, p)
	}
	return order
}

// plausiblePeer rejects a peer/origin string that can't name a real dial target:
// empty, or carrying a NUL or line break (a registry field is one line, and no
// ssh target or sim exec: peer contains these). A cheap trust-boundary guard atop
// the trusted-set intersection.
func plausiblePeer(s string) bool {
	return s != "" && !strings.ContainsAny(s, "\x00\r\n")
}

// fetchFromPeer runs one bounded attempt against peer, verifying the parsed
// credential by recomputation: it must be a valid stripped synced credential,
// hash- and expiry-consistent with its envelope, registry-matching (hash and
// expiry both) unless peer is the origin, and strictly fresher than the local
// credential.
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
	if env.ExpiresAt != cred.ClaudeAiOauth.ExpiresAt {
		return nil, fmt.Errorf("envelope expiry %d disagrees with the credential's %d", env.ExpiresAt, cred.ClaudeAiOauth.ExpiresAt)
	}
	if !isOrigin && env.Hash != chain.Hash {
		return nil, errors.New("relay answer does not match the registry chain (stale peer registry)")
	}
	// The origin publishes hash and expiry as one stamp; a relay must present
	// exactly that expiry, so a tampered stamp (fresh expiry beside an old
	// hash) stays uninstallable.
	if !isOrigin && cred.ClaudeAiOauth.ExpiresAt != chain.ExpiresAt {
		return nil, fmt.Errorf("relay expiry %d does not match the registry chain's %d", cred.ClaudeAiOauth.ExpiresAt, chain.ExpiresAt)
	}
	if cred.ClaudeAiOauth.ExpiresAt <= localExpiresAt {
		return nil, fmt.Errorf("credential expiry %d is not strictly later than local %d", cred.ClaudeAiOauth.ExpiresAt, localExpiresAt)
	}
	return &cred, nil
}
