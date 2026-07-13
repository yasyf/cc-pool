package hostsync

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/synckit/cregistry"
)

func cred(access, refresh string) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{AccessToken: access, RefreshToken: refresh}}
}

func TestFingerprintApplyStable(t *testing.T) {
	val := AccountValue{
		UUID:         "u1",
		Email:        "a@example.com",
		Label:        "work",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1","emailAddress":"a@example.com"}`),
		Chain:        ChainStamp{Origin: "hostA", ExpiresAt: 1_720_000_000_000, Hash: "chainhash", RotatedAt: 1_719_000_000_000},
	}
	reg := cregistry.New[AccountValue]()
	reg.Add("u1", val, cregistry.Micros(42))
	before := Fingerprint(reg["u1"])

	// Wire path: a peer serializes the registry and we apply it. The
	// fingerprint must survive the marshal/unmarshal round-trip.
	blob, err := json.Marshal(reg)
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}
	applied := cregistry.New[AccountValue]()
	if err := json.Unmarshal(blob, &applied); err != nil {
		t.Fatalf("unmarshal registry: %v", err)
	}
	if after := Fingerprint(applied["u1"]); after != before {
		t.Errorf("fingerprint changed across wire round-trip: before %q, after %q", before, after)
	}

	// Merge of two converged replicas must reproduce the same entry, hence the
	// same fingerprint — the echo-termination invariant.
	merged := cregistry.Merge(reg, applied)
	if got := Fingerprint(merged["u1"]); got != before {
		t.Errorf("fingerprint changed after Merge: got %q, want %q", got, before)
	}

	// A genuine change to the value must move the fingerprint.
	changed := val
	changed.Chain.ExpiresAt++
	other := cregistry.New[AccountValue]()
	other.Add("u1", changed, cregistry.Micros(42))
	if got := Fingerprint(other["u1"]); got == before {
		t.Errorf("fingerprint unchanged after value edit: %q", got)
	}
}

func TestValueCanonicalJSONCarriesAllFields(t *testing.T) {
	val := AccountValue{
		UUID:         "u",
		Email:        "e",
		Label:        "l",
		OAuthAccount: json.RawMessage(`{"raw":1}`),
		Chain:        ChainStamp{Origin: "o", ExpiresAt: 1, Hash: "h", RotatedAt: 2},
	}
	blob, err := json.Marshal(val)
	if err != nil {
		t.Fatalf("marshal AccountValue: %v", err)
	}
	js := string(blob)

	// Every serialized field of the value and its nested structs must be named
	// in the JSON, and none may use omitempty: the cregistry equal-add tiebreak
	// orders values by canonical JSON bytes, so an omitted field would let two
	// distinct values marshal identically and weaken the ordering.
	for _, typ := range []reflect.Type{
		reflect.TypeOf(AccountValue{}),
		reflect.TypeOf(ChainStamp{}),
	} {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			tag := f.Tag.Get("json")
			if tag == "" || tag == "-" {
				t.Fatalf("%s.%s has no json tag (must be serialized)", typ.Name(), f.Name)
			}
			parts := strings.Split(tag, ",")
			name := parts[0]
			if name == "" {
				t.Fatalf("%s.%s has an empty json name", typ.Name(), f.Name)
			}
			for _, opt := range parts[1:] {
				if opt == "omitempty" {
					t.Errorf("%s.%s uses omitempty (breaks canonical-JSON tiebreak)", typ.Name(), f.Name)
				}
			}
			if !strings.Contains(js, `"`+name+`"`) {
				t.Errorf("%s.%s (json %q) missing from marshaled value: %s", typ.Name(), f.Name, name, js)
			}
		}
	}
}

// v1AccountJSON is a schema-v1 AccountValue as the shipped holder/lease design
// wrote it — the exact shape the v2 gate must refuse.
const v1AccountJSON = `{
	"uuid": "u1",
	"email": "a@x.com",
	"label": "work",
	"oauthAccount": {"accountUuid": "u1"},
	"chain": {
		"expiresAt": 1720000000000,
		"hash": "h1",
		"holder": "host-a",
		"parentHash": "h0",
		"rotatedAt": 1719000000000
	},
	"lease": {"host": "host-b", "until": 1720000500000}
}`

// TestSchemaGateRejectsV1Registry pins the fail-fast schema gate: v1 payloads
// (holder/lease/parentHash keys) fail with ErrRegistrySchema — at the value,
// through the registry file, and for a lease-only leftover — while a valid v2
// payload round-trips and a syntax error stays a plain parse error.
func TestSchemaGateRejectsV1Registry(t *testing.T) {
	t.Run("v1 value fails with the sentinel", func(t *testing.T) {
		var v AccountValue
		err := json.Unmarshal([]byte(v1AccountJSON), &v)
		if !errors.Is(err, ErrRegistrySchema) {
			t.Fatalf("err = %v, want errors.Is ErrRegistrySchema", err)
		}
		for _, want := range []string{"upgrade both hosts", "registry.json", "runbook"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q missing remedy fragment %q", err, want)
			}
		}
	})

	t.Run("v1 lease-only value fails too", func(t *testing.T) {
		var v AccountValue
		err := json.Unmarshal([]byte(`{"uuid":"u1","email":"e","label":"l","oauthAccount":null,"chain":{"origin":"o","expiresAt":1,"hash":"h","rotatedAt":2},"lease":null}`), &v)
		if !errors.Is(err, ErrRegistrySchema) {
			t.Fatalf("err = %v, want errors.Is ErrRegistrySchema", err)
		}
	})

	t.Run("v1 registry file fails Load loud", func(t *testing.T) {
		rf := tempRegistry(t)
		regJSON := `{"u1": {"added": 100, "removed": 0, "value": ` + v1AccountJSON + `}}`
		if err := os.WriteFile(rf.Path, []byte(regJSON), 0o600); err != nil {
			t.Fatal(err)
		}
		reg, err := rf.Load()
		if !errors.Is(err, ErrRegistrySchema) {
			t.Fatalf("Load err = %v, want errors.Is ErrRegistrySchema", err)
		}
		if reg != nil {
			t.Fatalf("Load returned a registry from a v1 file: %v", reg)
		}
	})

	t.Run("v2 value round-trips", func(t *testing.T) {
		want := AccountValue{
			UUID:         "u1",
			Email:        "a@x.com",
			Label:        "work",
			OAuthAccount: json.RawMessage(`{"accountUuid":"u1"}`),
			Chain:        ChainStamp{Origin: "host-a", ExpiresAt: 1_720_000_000_000, Hash: "h1", RotatedAt: 1_719_000_000_000},
		}
		blob, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got AccountValue
		if err := json.Unmarshal(blob, &got); err != nil {
			t.Fatalf("v2 round-trip failed the gate: %v", err)
		}
		if got.UUID != want.UUID || got.Chain != want.Chain || string(got.OAuthAccount) != string(want.OAuthAccount) {
			t.Fatalf("round-trip = %+v, want %+v", got, want)
		}
	})

	t.Run("syntax error is not the schema sentinel", func(t *testing.T) {
		var v AccountValue
		err := json.Unmarshal([]byte(`{not json`), &v)
		if err == nil {
			t.Fatal("corrupt value parsed")
		}
		if errors.Is(err, ErrRegistrySchema) {
			t.Fatalf("syntax error misclassified as a schema error: %v", err)
		}
	})
}
