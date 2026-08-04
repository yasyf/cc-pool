package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// OwnerRecord is one daemon generation's opaque owner identity: the canonical
// bytes every owner_record column stores and every fenced CAS compares. Bytes
// are minted once per generation and never decoded once stored; a foreign
// generation's bytes — any prior era's included — are claimed by echoing them
// into the epoch CAS, never by interpreting them.
type OwnerRecord []byte

const ownerRecordMaxBytes = 1 << 10

type ownerIdentity struct {
	V       int    `json:"v"`
	PID     int    `json:"pid"`
	Started int64  `json:"started"`
	Nonce   string `json:"nonce"`
}

// MintOwnerRecord derives a fresh generation identity. The nonce is the
// identity; PID and Started are diagnostic only.
func MintOwnerRecord(now time.Time) (OwnerRecord, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("mint owner record: %w", err)
	}
	encoded, err := json.Marshal(ownerIdentity{
		V:       2,
		PID:     os.Getpid(),
		Started: now.UnixNano(),
		Nonce:   hex.EncodeToString(nonce[:]),
	})
	if err != nil {
		return nil, fmt.Errorf("mint owner record: %w", err)
	}
	return encoded, nil
}

// Validate rejects an absent or oversized owner identity.
func (o OwnerRecord) Validate() error {
	if len(o) == 0 {
		return errors.New("credential operation owner record is required")
	}
	if len(o) > ownerRecordMaxBytes {
		return errors.New("credential operation owner record exceeds its limit")
	}
	return nil
}
