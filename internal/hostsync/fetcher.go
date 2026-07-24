package hostsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
)

const (
	// SyncServiceID is cc-pool's exact Synckit service identity.
	SyncServiceID = "cc-pool"
	// SyncSchemaFingerprint binds the v1 CRDT and delivery-only credential schema.
	SyncSchemaFingerprint = "88ba2072464b3cc9b9f1670d2f64b0f9cf57f9e3dda8a8527bff34b5e5b192a6"

	syncSchemaIdentity    = "com.yasyf.cc-pool.hostsync.registry.v1"
	syncSchemaDeclaration = "{registry:map<string,{added_at:int64-micros,removed_at?:int64-micros," +
		"value:{uuid:string,email:string,label:string,oauthAccount:json," +
		"chain:{origin:string,expiresAt:int64-millis,hash:string,rotatedAt:int64-millis}}}>," +
		"credentials:map<string,{credential:json,expiresAt:int64-millis,hash:string}>}"
)

// CredentialEnvelope is one canonical access-only credential bound to its chain.
type CredentialEnvelope struct {
	Credential json.RawMessage `json:"credential"`
	ExpiresAt  int64           `json:"expiresAt"`
	Hash       string          `json:"hash"`
}

type syncSnapshot struct {
	Registry    Registry                      `json:"registry"`
	Credentials map[string]CredentialEnvelope `json:"credentials"`
}

// CredentialSnapshotSource builds delivery-only material for one immutable registry view.
type CredentialSnapshotSource func(context.Context, Registry) (map[string]CredentialEnvelope, error)

// AccountLookup resolves an account UUID to its local pool row.
type AccountLookup func(string) (store.Account, bool, error)

// CredentialReader reads one account without refreshing or writing it.
type CredentialReader func(context.Context, store.Account) (*creds.Credential, error)

// CredentialResolver returns delivery-scoped material for one exact chain.
type CredentialResolver func(context.Context, string, ChainStamp) (*creds.Credential, error)

type appliedCredentialsKey struct{}

// ErrCredentialMaterialUnavailable means the current delivery did not carry
// the exact access-only material needed to converge one chain.
var ErrCredentialMaterialUnavailable = errors.New("hostsync: credential material unavailable in current delivery")

// BuildCredentialSnapshot exports only credentials owned by this delivery
// origin, stripped of their refresh tokens and bound to the registry chain.
func BuildCredentialSnapshot(
	ctx context.Context,
	registry Registry,
	self string,
	lookup AccountLookup,
	read CredentialReader,
) (map[string]CredentialEnvelope, error) {
	if self == "" || lookup == nil || read == nil {
		return nil, errors.New("hostsync: credential snapshot source is incomplete")
	}
	credentials := make(map[string]CredentialEnvelope)
	for uuid, entry := range registry {
		if !entry.Present() || entry.Value.UUID != uuid || entry.Value.Chain.Origin != self {
			continue
		}
		if entry.Value.Chain == (ChainStamp{}) {
			continue
		}
		account, ok, err := lookup(uuid)
		if err != nil {
			return nil, fmt.Errorf("hostsync: resolve owned account %s: %w", uuid, err)
		}
		if !ok {
			return nil, fmt.Errorf("hostsync: owned account %s is absent", uuid)
		}
		credential, err := read(ctx, account)
		if err != nil {
			return nil, fmt.Errorf("hostsync: read owned account %s credential: %w", uuid, err)
		}
		if !credential.HasRefreshToken() {
			return nil, fmt.Errorf("hostsync: owned account %s has no refresh token", uuid)
		}
		stripped := credential.Strip()
		blob, err := stripped.Marshal()
		if err != nil {
			return nil, fmt.Errorf("hostsync: marshal owned account %s credential: %w", uuid, err)
		}
		envelope := CredentialEnvelope{
			Credential: blob,
			ExpiresAt:  stripped.ClaudeAiOauth.ExpiresAt,
			Hash:       creds.AccessHash(stripped),
		}
		if _, err := decodeCredentialEnvelope(envelope); err != nil {
			return nil, fmt.Errorf("hostsync: encode owned account %s credential: %w", uuid, err)
		}
		if envelope.ExpiresAt != entry.Value.Chain.ExpiresAt || envelope.Hash != entry.Value.Chain.Hash {
			return nil, fmt.Errorf("hostsync: owned account %s credential does not match registry chain", uuid)
		}
		credentials[uuid] = envelope
	}
	return credentials, nil
}

func encodeSyncSnapshot(snapshot syncSnapshot) ([]byte, error) {
	if snapshot.Registry == nil || snapshot.Credentials == nil {
		return nil, errors.New("hostsync: sync snapshot maps are required")
	}
	return json.Marshal(snapshot)
}

func decodeSyncSnapshot(payload []byte) (syncSnapshot, error) {
	if len(payload) == 0 {
		return syncSnapshot{}, errors.New("hostsync: sync snapshot is empty")
	}
	var snapshot syncSnapshot
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return syncSnapshot{}, fmt.Errorf("hostsync: decode sync snapshot: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return syncSnapshot{}, errors.New("hostsync: sync snapshot has trailing data")
	}
	if snapshot.Registry == nil || snapshot.Credentials == nil {
		return syncSnapshot{}, errors.New("hostsync: sync snapshot maps are required")
	}
	canonical, err := encodeSyncSnapshot(snapshot)
	if err != nil {
		return syncSnapshot{}, err
	}
	if !bytes.Equal(payload, canonical) {
		return syncSnapshot{}, errors.New("hostsync: sync snapshot is not canonical")
	}
	return snapshot, nil
}

func validateAppliedCredentials(snapshot syncSnapshot, origin string) (map[string]*creds.Credential, error) {
	resolved := make(map[string]*creds.Credential, len(snapshot.Credentials))
	for uuid, envelope := range snapshot.Credentials {
		entry, ok := snapshot.Registry[uuid]
		if !ok || !entry.Present() || entry.Value.UUID != uuid || entry.Value.Chain.Origin != origin {
			return nil, fmt.Errorf("hostsync: credential material for %s is not owned by delivery origin", uuid)
		}
		credential, err := decodeCredentialEnvelope(envelope)
		if err != nil {
			return nil, fmt.Errorf("hostsync: credential material for %s: %w", uuid, err)
		}
		if envelope.ExpiresAt != entry.Value.Chain.ExpiresAt || envelope.Hash != entry.Value.Chain.Hash {
			return nil, fmt.Errorf("hostsync: credential material for %s does not match registry chain", uuid)
		}
		resolved[uuid] = credential
	}
	for uuid, entry := range snapshot.Registry {
		if !entry.Present() || entry.Value.Chain == (ChainStamp{}) || entry.Value.Chain.Origin != origin {
			continue
		}
		if _, ok := resolved[uuid]; !ok {
			return nil, fmt.Errorf("hostsync: credential material for owned chain %s is missing", uuid)
		}
	}
	return resolved, nil
}

func decodeCredentialEnvelope(envelope CredentialEnvelope) (*creds.Credential, error) {
	if len(envelope.Credential) == 0 {
		return nil, errors.New("credential is empty")
	}
	var credential creds.Credential
	decoder := json.NewDecoder(bytes.NewReader(envelope.Credential))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&credential); err != nil {
		return nil, fmt.Errorf("decode credential: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("credential has trailing data")
	}
	canonical, err := credential.Marshal()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, envelope.Credential) {
		return nil, errors.New("credential is not canonical")
	}
	if credential.HasRefreshToken() {
		return nil, pool.ErrEnvelopeCarriesSecret
	}
	if !credential.Synced() {
		return nil, pool.ErrEnvelopeNoAccessToken
	}
	if credential.ClaudeAiOauth.ExpiresAt != envelope.ExpiresAt || creds.AccessHash(&credential) != envelope.Hash {
		return nil, errors.New("credential envelope metadata mismatch")
	}
	return &credential, nil
}

func withAppliedCredentials(ctx context.Context, credentials map[string]*creds.Credential) context.Context {
	return context.WithValue(ctx, appliedCredentialsKey{}, credentials)
}

// ResolveAppliedCredential returns the access-only credential carried by this Apply call.
func ResolveAppliedCredential(ctx context.Context, uuid string, chain ChainStamp) (*creds.Credential, error) {
	credentials, _ := ctx.Value(appliedCredentialsKey{}).(map[string]*creds.Credential)
	credential := credentials[uuid]
	if credential == nil {
		return nil, ErrCredentialMaterialUnavailable
	}
	if credential.ClaudeAiOauth.ExpiresAt != chain.ExpiresAt || creds.AccessHash(credential) != chain.Hash {
		return nil, errors.New("hostsync: applied credential does not match requested chain")
	}
	copy := *credential
	return &copy, nil
}

func syncSchemaDigest() string {
	digest := sha256.Sum256([]byte(syncSchemaIdentity + "\x00" + syncSchemaDeclaration))
	return hex.EncodeToString(digest[:])
}
