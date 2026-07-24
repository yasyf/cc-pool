package hostsync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/yasyf/synckit/cregistry"
)

func TestSyncSchemaFingerprintBindsExactDeclaration(t *testing.T) {
	digest := sha256.Sum256([]byte(syncSchemaIdentity + "\x00" + syncSchemaDeclaration))
	if got := hex.EncodeToString(digest[:]); got != SyncSchemaFingerprint {
		t.Fatalf("schema fingerprint = %q, want %q", got, SyncSchemaFingerprint)
	}
}

func TestRegistrySnapshotRoundTripPreservesInt64(t *testing.T) {
	const big = int64(math.MaxInt64) - 3
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", AccountValue{
		UUID: "u1", Email: "e@x.com", Label: "work",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1"}`),
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: big, Hash: "h", RotatedAt: big - 1},
	}, cregistry.Micros(big-4))

	payload, err := encodeRegistrySnapshot(reg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRegistrySnapshot(payload)
	if err != nil {
		t.Fatal(err)
	}
	entry := got["u1"]
	if int64(entry.Added) != big-4 || entry.Value.Chain.ExpiresAt != big ||
		entry.Value.Chain.RotatedAt != big-1 || entry.Value.Chain.Origin != "hostA" {
		t.Fatalf("snapshot corrupted exact integer or scalar fields: %+v", entry)
	}
}

func TestRegistrySnapshotRejectsNonCanonicalOrUnknownPayload(t *testing.T) {
	for name, payload := range map[string][]byte{
		"empty":         nil,
		"whitespace":    []byte(" {}"),
		"trailing":      []byte("{}{}"),
		"unknown field": []byte(`{"u":{"added_at":1,"value":{"uuid":"u","email":"","label":"","oauthAccount":{},"chain":{"origin":"","expiresAt":0,"hash":"","rotatedAt":0},"extra":true}}}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeRegistrySnapshot(payload); err == nil {
				t.Fatalf("decodeRegistrySnapshot(%q) succeeded", payload)
			}
		})
	}
}

func TestRegistrySnapshotIsSecretless(t *testing.T) {
	reg := cregistry.New[AccountValue]()
	reg.Add("u", AccountValue{
		UUID: "u", OAuthAccount: json.RawMessage(`{"accountUuid":"u"}`),
		Chain: ChainStamp{Origin: "host", Hash: "chain-hash"},
	}, 1)
	payload, err := encodeRegistrySnapshot(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"accessToken", "refreshToken", "ACCESS-TOKEN", "REFRESH-TOKEN"} {
		if strings.Contains(string(payload), secret) {
			t.Fatalf("snapshot leaked %q: %s", secret, payload)
		}
	}
}
