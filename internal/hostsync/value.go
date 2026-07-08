// Package hostsync makes cc-pool a synckit consumer: the pool is a secretless
// convergent registry (a cregistry keyed by account UUID) shared cluster-wide.
package hostsync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/synckit/cregistry"
)

// Registry is cc-pool's convergent account registry: a cregistry keyed by
// account UUID, carrying metadata and chain stamps but never secrets.
type Registry = cregistry.Registry[AccountValue]

// ChainStamp describes an account's OAuth chain — freshness and holdership —
// without ever carrying a token. No omitempty, per the AccountValue tiebreak rule.
type ChainStamp struct {
	// ExpiresAt is the access-token expiry in Unix epoch milliseconds.
	ExpiresAt int64 `json:"expiresAt"`
	// Hash is CredentialHash of the chain's token pair.
	Hash string `json:"hash"`
	// Holder is the host allowed to preemptively refresh this chain.
	Holder string `json:"holder"`
	// ParentHash is CredentialHash of the spent parent; empty when unknown.
	ParentHash string `json:"parentHash"`
	// RotatedAt is the Unix-millis wall time of the holder's last published rotation.
	RotatedAt int64 `json:"rotatedAt"`
}

// Lease is a time-boxed refresh claim held by one host while it runs a live
// session on the account. No omitempty, per the AccountValue tiebreak rule.
type Lease struct {
	// Host is the leaseholder.
	Host string `json:"host"`
	// Until is the lease expiry in Unix epoch milliseconds.
	Until int64 `json:"until"`
}

// AccountValue is the per-account registry payload. cregistry breaks equal-add
// ties by canonical JSON bytes, so every field is tagged and none uses omitempty.
type AccountValue struct {
	// UUID is the stable account identity and the registry key.
	UUID string `json:"uuid"`
	// Email is the account's login email (display + de-dup aid).
	Email string `json:"email"`
	// Label is the user-assigned account label; last-write-wins across hosts.
	Label string `json:"label"`
	// OAuthAccount is Claude's opaque oauthAccount object, passed through byte-exact.
	OAuthAccount json.RawMessage `json:"oauthAccount"`
	// Chain is the secretless chain stamp (freshness, holder, rotation).
	Chain ChainStamp `json:"chain"`
	// Lease is the current refresh lease, or nil when the chain is unleased.
	Lease *Lease `json:"lease"`
}

// CredentialHash is creds.CredentialHash, re-exported for hostsync callers.
func CredentialHash(c *creds.Credential) string { return creds.CredentialHash(c) }

// Fingerprint is an apply-stable digest of a registry entry — SHA-256 over its
// canonical JSON — so applying a peer's change reproduces the identical digest;
// the synckit watch engine relies on that to terminate change echoes.
func Fingerprint(e cregistry.Entry[AccountValue]) string {
	b, err := json.Marshal(e)
	if err != nil {
		panic("hostsync: registry entry is not JSON-serializable: " + err.Error())
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
