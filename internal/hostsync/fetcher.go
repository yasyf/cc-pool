package hostsync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	// SyncServiceID is cc-pool's exact Synckit service identity.
	SyncServiceID = "cc-pool"
	// SyncSchemaFingerprint binds the v1 secretless CRDT snapshot schema.
	SyncSchemaFingerprint = "24e1ddc9cc2c5116e4fa16df7f01fef9f0b125d791e3d6c4c6fd4b57b5e77a41"

	syncSchemaIdentity    = "com.yasyf.cc-pool.hostsync.registry.v1"
	syncSchemaDeclaration = "map<string,{added_at:int64-micros,removed_at?:int64-micros," +
		"value:{uuid:string,email:string,label:string,oauthAccount:json," +
		"chain:{origin:string,expiresAt:int64-millis,hash:string,rotatedAt:int64-millis}}}>"
)

func encodeRegistrySnapshot(reg Registry) ([]byte, error) {
	payload, _, err := canonicalRegistry(reg)
	return payload, err
}

func decodeRegistrySnapshot(payload []byte) (Registry, error) {
	if len(payload) == 0 {
		return nil, errors.New("hostsync: registry snapshot is empty")
	}
	reg := Registry{}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reg); err != nil {
		return nil, fmt.Errorf("hostsync: decode registry snapshot: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("hostsync: registry snapshot has trailing data")
	}
	canonical, err := encodeRegistrySnapshot(reg)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(payload, canonical) {
		return nil, errors.New("hostsync: registry snapshot is not canonical")
	}
	return reg, nil
}
