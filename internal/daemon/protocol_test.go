package daemon

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

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

	in := Response{OK: true, Holder: &HolderStatus{Version: "9.9.9", Mounts: 2}}
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

func TestHandleStatusCarriesHolderState(t *testing.T) {
	s, _ := newTestServer(t)

	resp := s.handleStatus(t.Context())
	if !resp.OK || resp.Holder == nil {
		t.Fatalf("status = %+v, want a holder view", resp)
	}
	if resp.Holder.Version != "" || resp.Holder.Mounts != 0 {
		t.Fatalf("zero-cache holder = %+v, want the unreachable shape", resp.Holder)
	}

	s.holder.mu.Lock()
	s.holder.healthy = true
	s.holder.version = "0.0.9-old"
	s.holder.mounts = map[string]bool{"/a": true, "/b": false}
	s.holder.mu.Unlock()

	resp = s.handleStatus(t.Context())
	want := &HolderStatus{Version: "0.0.9-old", Mounts: 1}
	if !reflect.DeepEqual(resp.Holder, want) {
		t.Fatalf("holder = %+v, want %+v", resp.Holder, want)
	}
}
