package hostsync

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/synckit/cregistry"
)

func cred(access, refresh string) *creds.Credential {
	return &creds.Credential{ClaudeAiOauth: creds.OAuth{AccessToken: access, RefreshToken: refresh}}
}

func TestCredentialHashStable(t *testing.T) {
	base := &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken:      "access-AAA",
		RefreshToken:     "refresh-BBB",
		ExpiresAt:        1000,
		Scopes:           []string{"user:inference"},
		SubscriptionType: "max",
	}}
	want := CredentialHash(base)

	// A JSON marshal/unmarshal round-trip is a different construction path for
	// the same tokens; the hash must not change.
	blob, err := base.Marshal()
	if err != nil {
		t.Fatalf("marshal base: %v", err)
	}
	var roundtrip creds.Credential
	if err := json.Unmarshal(blob, &roundtrip); err != nil {
		t.Fatalf("unmarshal base: %v", err)
	}
	if got := CredentialHash(&roundtrip); got != want {
		t.Errorf("round-trip hash = %q, want %q", got, want)
	}

	// The hash covers ONLY the token pair: differing non-token fields must not
	// perturb it.
	sameTokens := &creds.Credential{ClaudeAiOauth: creds.OAuth{
		AccessToken:      "access-AAA",
		RefreshToken:     "refresh-BBB",
		ExpiresAt:        9_999_999,
		Scopes:           nil,
		SubscriptionType: "pro",
		RateLimitTier:    "tier-2",
		ClientID:         "client-xyz",
	}}
	if got := CredentialHash(sameTokens); got != want {
		t.Errorf("non-token fields changed the hash: got %q, want %q", got, want)
	}

	differs := []struct {
		name string
		c    *creds.Credential
	}{
		{"different refresh token", cred("access-AAA", "refresh-CCC")},
		{"different access token", cred("access-ZZZ", "refresh-BBB")},
		{"tokens swapped between fields", cred("refresh-BBB", "access-AAA")},
	}
	for _, tc := range differs {
		if got := CredentialHash(tc.c); got == want {
			t.Errorf("%s: hash collided with base %q", tc.name, want)
		}
	}

	// Length-prefixing must prevent the classic boundary collision: without it,
	// (refresh="ab",access="c") and (refresh="a",access="bc") both concatenate
	// to "abc" and hash alike.
	ab := CredentialHash(cred("c", "ab"))
	a := CredentialHash(cred("bc", "a"))
	if ab == a {
		t.Errorf("length-prefix boundary collision: %q == %q", ab, a)
	}
}

func TestFingerprintApplyStable(t *testing.T) {
	val := AccountValue{
		UUID:         "u1",
		Email:        "a@example.com",
		Label:        "work",
		OAuthAccount: json.RawMessage(`{"accountUuid":"u1","emailAddress":"a@example.com"}`),
		Chain:        ChainStamp{ExpiresAt: 1_720_000_000_000, Hash: "chainhash", Holder: "hostA", RotatedAt: 1_719_000_000_000},
		Lease:        &Lease{Host: "hostB", Until: 1_720_000_500_000},
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
		Chain:        ChainStamp{ExpiresAt: 1, Hash: "h", Holder: "ho", RotatedAt: 2},
		Lease:        &Lease{Host: "le", Until: 3},
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
		reflect.TypeOf(Lease{}),
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
