// Package hostsync makes cc-pool a synckit consumer: it models the pool as a
// secretless convergent registry (a cregistry keyed by account UUID) so the
// pool becomes cluster-wide. This file defines the registry value type and its
// content digests; registryfile.go persists the registry under a flock.
package hostsync

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/synckit/cregistry"
)

// Registry is cc-pool's convergent account registry: a cregistry keyed by
// account UUID whose values are AccountValue. It carries no secrets — only
// metadata and a chain stamp — so it is safe to persist and ship over the mesh.
type Registry = cregistry.Registry[AccountValue]

// ChainStamp is the secretless descriptor of an account's OAuth chain: enough
// to decide freshness and holdership without ever carrying a token. Every field
// is serialized (no omitempty) so the cregistry equal-add tiebreak, which
// orders by canonical JSON bytes, always has the full value to compare.
type ChainStamp struct {
	// ExpiresAt is the access-token expiry in Unix epoch milliseconds. It is
	// server-issued, so it is the skew-immune comparator for "which chain is
	// fresher" — strictly-later wins.
	ExpiresAt int64 `json:"expiresAt"`
	// Hash is CredentialHash of the chain: its token-pair identity, used to
	// verify a pulled credential matches the advertised chain.
	Hash string `json:"hash"`
	// Holder is the host allowed to preemptively refresh this chain.
	Holder string `json:"holder"`
	// RotatedAt is the Unix epoch millisecond wall time of the last rotation
	// the holder published; a stale RotatedAt is one signal a holder is dead.
	RotatedAt int64 `json:"rotatedAt"`
}

// Lease is a time-boxed refresh claim held by one host while it runs a live
// session on the account. All fields are serialized (no omitempty) for the same
// canonical-JSON tiebreak reason as ChainStamp.
type Lease struct {
	// Host is the leaseholder.
	Host string `json:"host"`
	// Until is the lease expiry in Unix epoch milliseconds.
	Until int64 `json:"until"`
}

// AccountValue is the per-account registry payload. It must capture its full
// identity in JSON: cregistry orders equal-add values by canonical JSON bytes,
// so every field carries an explicit tag and NONE uses omitempty — an omitted
// field would make two distinct values marshal alike and weaken the tiebreak.
type AccountValue struct {
	// UUID is the stable account identity and the registry key.
	UUID string `json:"uuid"`
	// Email is the account's login email (display + de-dup aid).
	Email string `json:"email"`
	// Label is the user-assigned account label; last-write-wins across hosts.
	Label string `json:"label"`
	// OAuthAccount is Claude's opaque oauthAccount object, injected verbatim
	// into a materialized account's private .claude.json. Carried as raw JSON
	// so its bytes pass through untouched.
	OAuthAccount json.RawMessage `json:"oauthAccount"`
	// Chain is the secretless chain stamp (freshness, holder, rotation).
	Chain ChainStamp `json:"chain"`
	// Lease is the current refresh lease, or nil when the chain is unleased.
	Lease *Lease `json:"lease"`
}

// CredentialHash is a stable content digest of an OAuth chain's identity: the
// SHA-256 over the length-prefixed refresh token followed by the access token.
// It covers ONLY that token pair — not expiresAt, scopes, subscription type, or
// any wrapper struct — so it is invariant under credential marshal/unmarshal
// round-trips (field ordering can never change it) and changes exactly when the
// chain rotates. Length-prefixing keeps the two fields unambiguous, so no two
// distinct token pairs can collide by concatenation.
func CredentialHash(c *creds.Credential) string {
	h := sha256.New()
	hashField(h, c.ClaudeAiOauth.RefreshToken)
	hashField(h, c.ClaudeAiOauth.AccessToken)
	return hex.EncodeToString(h.Sum(nil))
}

// hashField writes a length-prefixed string into h so field boundaries are
// unambiguous. sha256's Write never errors.
func hashField(h hash.Hash, s string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(s)))
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(s))
}

// Fingerprint is an apply-stable digest of a registry entry: the SHA-256 over
// the entry's canonical JSON — its add/remove stamps plus the canonical value
// bytes. Because cregistry stores the winning value and stamps verbatim,
// applying a peer's change and recomputing reproduces the identical
// fingerprint; that is the property the synckit watch engine relies on to
// terminate change echoes. AccountValue is JSON-serializable by construction,
// so a marshal error is impossible; it panics loudly rather than fabricate a
// digest, mirroring cregistry's own canonical() contract.
func Fingerprint(e cregistry.Entry[AccountValue]) string {
	b, err := json.Marshal(e)
	if err != nil {
		panic("hostsync: registry entry is not JSON-serializable: " + err.Error())
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
