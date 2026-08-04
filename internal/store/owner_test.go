package store

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

// TestOwnerRecordMarshalsAsAJSONString pins the era separation §B rests on: an
// OwnerRecord in a raw-carried JSON field (the credential-lock journal's
// worker) must render as a JSON string, never an object, so no v2 identity can
// ever be byte-equal to a v0.20 proc.Record object.
func TestOwnerRecordMarshalsAsAJSONString(t *testing.T) {
	minted, err := MintOwnerRecord(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for name, owner := range map[string]OwnerRecord{
		"minted":                  minted,
		"golden-v0.20.9-contents": OwnerRecord(upgradeGoldenOwnerA),
	} {
		marshaled, err := json.Marshal(owner)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(marshaled, []byte(`"`)) {
			t.Fatalf("%s owner record marshals as %s, want a JSON string", name, marshaled)
		}
	}
}
