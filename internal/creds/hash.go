package creds

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
)

// CredentialHash digests the chain's token pair (length-prefixed SHA-256):
// invariant under marshal round-trips, changed exactly when the chain rotates.
func CredentialHash(c *Credential) string {
	h := sha256.New()
	hashField(h, c.ClaudeAiOauth.RefreshToken)
	hashField(h, c.ClaudeAiOauth.AccessToken)
	return hex.EncodeToString(h.Sum(nil))
}

// AccessHash digests the canonical Marshal of Strip() (length-prefixed
// SHA-256): the identity of a stripped (synced) blob. An owned blob and its
// stripped form hash identically, while every field but the refresh token is
// authenticated — token, expiry, and metadata alike.
func AccessHash(c *Credential) string {
	b, err := json.Marshal(c.Strip())
	if err != nil {
		panic("creds: marshal stripped credential: " + err.Error()) // plain struct; cannot fail
	}
	h := sha256.New()
	hashField(h, string(b))
	return hex.EncodeToString(h.Sum(nil))
}

// hashField writes s length-prefixed into h so field boundaries are unambiguous.
func hashField(h hash.Hash, s string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(s)))
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(s))
}
