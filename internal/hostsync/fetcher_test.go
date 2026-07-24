package hostsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/cregistry"
)

func TestSyncSchemaFingerprintBindsExactDeclaration(t *testing.T) {
	digest := sha256.Sum256([]byte(syncSchemaIdentity + "\x00" + syncSchemaDeclaration))
	if got := hex.EncodeToString(digest[:]); got != SyncSchemaFingerprint {
		t.Fatalf("schema fingerprint = %q, want %q", got, SyncSchemaFingerprint)
	}
}

func TestSyncSnapshotRoundTripPreservesExactState(t *testing.T) {
	const big = int64(math.MaxInt64) - 3
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "access"
	credential.ClaudeAiOauth.ExpiresAt = big
	blob, err := credential.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", AccountValue{
		UUID: "u1", Email: "e@x.com", Label: "work",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1"}`),
		Chain: ChainStamp{
			Origin: "hostA", ExpiresAt: big, Hash: creds.AccessHash(credential), RotatedAt: big - 1,
		},
	}, cregistry.Micros(big-4))
	payload, err := encodeSyncSnapshot(syncSnapshot{
		Registry: reg,
		Credentials: map[string]CredentialEnvelope{
			"u1": {Credential: blob, ExpiresAt: big, Hash: creds.AccessHash(credential)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeSyncSnapshot(payload)
	if err != nil {
		t.Fatal(err)
	}
	entry := got.Registry["u1"]
	if int64(entry.Added) != big-4 || entry.Value.Chain.ExpiresAt != big ||
		entry.Value.Chain.RotatedAt != big-1 || entry.Value.Chain.Origin != "hostA" {
		t.Fatalf("snapshot corrupted exact integer or scalar fields: %+v", entry)
	}
	if got.Credentials["u1"].Hash != creds.AccessHash(credential) {
		t.Fatalf("credential envelope = %+v", got.Credentials["u1"])
	}
}

func TestSyncSnapshotRejectsNonCanonicalOrUnknownPayload(t *testing.T) {
	for name, payload := range map[string][]byte{
		"empty":         nil,
		"whitespace":    []byte(` {"registry":{},"credentials":{}}`),
		"trailing":      []byte(`{"registry":{},"credentials":{}}{}`),
		"missing map":   []byte(`{"registry":{}}`),
		"unknown field": []byte(`{"registry":{},"credentials":{},"extra":true}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSyncSnapshot(payload); err == nil {
				t.Fatalf("decodeSyncSnapshot(%q) succeeded", payload)
			}
		})
	}
}

func TestBuildCredentialSnapshotStripsRefreshTokenAndBindsChain(t *testing.T) {
	owned := &creds.Credential{}
	owned.ClaudeAiOauth.AccessToken = "ACCESS-ONLY-WIRE"
	owned.ClaudeAiOauth.RefreshToken = "REFRESH-MUST-NOT-LEAVE"
	owned.ClaudeAiOauth.ExpiresAt = 5_000
	registry := cregistry.New[AccountValue]()
	registry.Add("u1", AccountValue{
		UUID:  "u1",
		Chain: ChainStamp{Origin: "self", ExpiresAt: owned.ClaudeAiOauth.ExpiresAt, Hash: creds.AccessHash(owned)},
	}, 1)
	registry.Add("u2", AccountValue{UUID: "u2", Chain: ChainStamp{Origin: "other", Hash: "foreign"}}, 1)
	credentials, err := BuildCredentialSnapshot(
		t.Context(), registry, "self",
		func(uuid string) (store.Account, bool, error) {
			return store.Account{ID: 1, AccountUUID: uuid}, uuid == "u1", nil
		},
		func(context.Context, store.Account) (*creds.Credential, error) { return owned, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials) != 1 {
		t.Fatalf("credential snapshot = %+v", credentials)
	}
	envelope := credentials["u1"]
	if strings.Contains(string(envelope.Credential), owned.ClaudeAiOauth.RefreshToken) {
		t.Fatalf("credential snapshot leaked refresh token: %s", envelope.Credential)
	}
	credential, err := decodeCredentialEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if credential.HasRefreshToken() || creds.AccessHash(credential) != registry["u1"].Value.Chain.Hash {
		t.Fatalf("decoded credential = %+v", credential)
	}
}

func TestAppliedCredentialsAreDeliveryScopedAndComplete(t *testing.T) {
	credential := &creds.Credential{}
	credential.ClaudeAiOauth.AccessToken = "access"
	credential.ClaudeAiOauth.ExpiresAt = 5_000
	blob, err := credential.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	chain := ChainStamp{Origin: "source", ExpiresAt: 5_000, Hash: creds.AccessHash(credential)}
	registry := cregistry.New[AccountValue]()
	registry.Add("u1", AccountValue{UUID: "u1", Chain: chain}, 1)
	snapshot := syncSnapshot{Registry: registry, Credentials: map[string]CredentialEnvelope{
		"u1": {Credential: blob, ExpiresAt: 5_000, Hash: chain.Hash},
	}}
	resolved, err := validateAppliedCredentials(snapshot, "source")
	if err != nil {
		t.Fatal(err)
	}
	ctx := withAppliedCredentials(t.Context(), resolved)
	got, err := ResolveAppliedCredential(ctx, "u1", chain)
	if err != nil || creds.AccessHash(got) != chain.Hash {
		t.Fatalf("ResolveAppliedCredential = %+v, %v", got, err)
	}
	if _, err := ResolveAppliedCredential(context.Background(), "u1", chain); !errors.Is(err, ErrCredentialMaterialUnavailable) {
		t.Fatalf("resolver escaped Apply scope: %v", err)
	}
	missing := syncSnapshot{Registry: registry, Credentials: map[string]CredentialEnvelope{}}
	if _, err := validateAppliedCredentials(missing, "source"); err == nil {
		t.Fatal("accepted an origin-owned chain without delivery material")
	}
	if _, err := validateAppliedCredentials(snapshot, "other"); err == nil {
		t.Fatal("accepted credential material from a non-owner delivery")
	}
}
