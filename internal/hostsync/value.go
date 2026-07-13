// Package hostsync makes cc-pool a synckit consumer: the pool is a secretless
// convergent registry (a cregistry keyed by account UUID) shared cluster-wide.
package hostsync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/yasyf/synckit/cregistry"
)

// Registry is cc-pool's convergent account registry: a cregistry keyed by
// account UUID, carrying metadata and chain stamps but never secrets.
type Registry = cregistry.Registry[AccountValue]

// ErrRegistrySchema rejects a registry written by a pre-origin (schema v1)
// cc-pool, whose holder/lease/parentHash semantics no host enforces anymore.
var ErrRegistrySchema = errors.New("pre-origin registry — upgrade both hosts and delete ~/.cc-pool/sync/registry.json (see runbook)")

// ChainStamp describes an account's OAuth chain — its origin host and
// access-token identity — without ever carrying a token. No omitempty, per
// the AccountValue tiebreak rule.
type ChainStamp struct {
	// Origin is the host whose login minted this chain; only it ever refreshes.
	Origin string `json:"origin"`
	// ExpiresAt is the access-token expiry in Unix epoch milliseconds.
	ExpiresAt int64 `json:"expiresAt"`
	// Hash is creds.AccessHash of the chain's access token — owned and
	// stripped forms of the same chain hash identically.
	Hash string `json:"hash"`
	// RotatedAt is the Unix-millis wall time of the origin's last published
	// rotation; observability only, never a freshness input.
	RotatedAt int64 `json:"rotatedAt"`
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
	// Chain is the secretless chain stamp (origin, freshness, access-token identity).
	Chain ChainStamp `json:"chain"`
}

// UnmarshalJSON fails fast on unknown fields, so a schema-v1 registry (its
// holder/lease/parentHash keys carried semantics no v2 host enforces)
// surfaces as ErrRegistrySchema instead of being silently half-read.
func (v *AccountValue) UnmarshalJSON(b []byte) error {
	type plain AccountValue // methodless alias: avoids UnmarshalJSON recursion
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var p plain
	if err := dec.Decode(&p); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return fmt.Errorf("%w: %s", ErrRegistrySchema, err)
		}
		return err
	}
	*v = AccountValue(p)
	return nil
}

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
