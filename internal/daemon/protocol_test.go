package daemon

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestHolderStatusWireAdditive pins the holder field's compatibility contract
// (ProtocolVersion stays 1): a response without it marshals with no holder
// key, old-shape bytes decode with Holder nil, and a populated field
// round-trips.
func TestHolderStatusWireAdditive(t *testing.T) {
	b, err := json.Marshal(Response{OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "holder") {
		t.Fatalf("empty response leaked a holder key: %s", b)
	}

	var old Response
	if err := json.Unmarshal([]byte(`{"proto":1,"ok":true}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Holder != nil {
		t.Fatalf("old-shape response decoded a phantom holder: %+v", old.Holder)
	}

	in := Response{OK: true, Holder: &HolderStatus{Version: "9.9.9", Mounts: 2, Skewed: true, TCCError: "grant pending"}}
	b, err = json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Response
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in.Holder, out.Holder) {
		t.Fatalf("holder did not round-trip: %+v != %+v", out.Holder, in.Holder)
	}
}

// TestHandleStatusCarriesHolderState pins the population: status carries the
// daemon's cached holder view, with Skewed carried from the supervisor's
// last-tick verdict (setSkewed, published off the supervise goroutine where
// IsSkew is safe), and Mounts counting live mirrors only.
func TestHandleStatusCarriesHolderState(t *testing.T) {
	s, _ := newTestServer(t)

	// The zero cache (holder never reached): present, version empty, no skew.
	resp := s.handleStatus(t.Context())
	if !resp.OK || resp.Holder == nil {
		t.Fatalf("status = %+v, want a holder view", resp)
	}
	if resp.Holder.Version != "" || resp.Holder.Skewed || resp.Holder.Mounts != 0 {
		t.Fatalf("zero-cache holder = %+v, want the unreachable shape", resp.Holder)
	}

	s.holder.mu.Lock()
	s.holder.healthy = true
	s.holder.version = "0.0.9-old"
	s.holder.mounts = map[string]bool{"/a": true, "/b": false}
	s.holder.tccErr = "grant pending"
	// Skewed is no longer derived in wireStatus; it is the supervisor's last-tick
	// IsSkew verdict published via setSkewed. A foreign-version holder a tick
	// would flag — set it here so the status carry-through assertion holds.
	s.holder.skewed = true
	s.holder.mu.Unlock()

	resp = s.handleStatus(t.Context())
	want := &HolderStatus{Version: "0.0.9-old", Mounts: 1, Skewed: true, TCCError: "grant pending"}
	if !reflect.DeepEqual(resp.Holder, want) {
		t.Fatalf("holder = %+v, want %+v", resp.Holder, want)
	}
}
