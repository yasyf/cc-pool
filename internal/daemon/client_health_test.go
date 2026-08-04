package daemon

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit"
)

func TestDecodeDaemonHealthDetailRequiresExactIdentity(t *testing.T) {
	const build = "cc-pool-test"
	healthy := HealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: build,
		State: RuntimeStateHealthy, Ready: true,
	}
	detail := func(edit func(*HealthResponse)) daemonkit.Health {
		response := healthy
		if edit != nil {
			edit(&response)
		}
		payload, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		return daemonkit.Health{Detail: payload}
	}
	if _, err := decodeDaemonHealthDetail(detail(nil)); err != nil {
		t.Fatalf("healthy runtime detail: %v", err)
	}
	if _, err := decodeDaemonHealthDetail(daemonkit.Health{}); err == nil ||
		!strings.Contains(err.Error(), "no product detail") {
		t.Fatalf("empty detail = %v, want a no-detail refusal", err)
	}
	for _, test := range []struct {
		name string
		edit func(*HealthResponse)
	}{
		{name: "schema", edit: func(h *HealthResponse) { h.Schema++ }},
		{name: "runtime build", edit: func(h *HealthResponse) { h.RuntimeBuild = "" }},
		{name: "unknown state", edit: func(h *HealthResponse) { h.State = "future" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeDaemonHealthDetail(detail(test.edit)); err == nil ||
				!strings.Contains(err.Error(), "identity is not exact") {
				t.Fatalf("%s = %v, want an identity refusal", test.name, err)
			}
		})
	}
}

func TestValidateDaemonHealthRequiresExactReadyBuild(t *testing.T) {
	const build = "cc-pool-test"
	healthy := HealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: build,
		State: RuntimeStateHealthy, Ready: true,
	}
	if err := validateDaemonHealth(healthy, build); err != nil {
		t.Fatalf("healthy runtime response: %v", err)
	}
	mismatched := healthy
	mismatched.RuntimeBuild = "other"
	if err := validateDaemonHealth(mismatched, build); err == nil ||
		!strings.Contains(err.Error(), "daemon build mismatch") {
		t.Fatalf("build mismatch = %v, want ErrDaemonBuildMismatch", err)
	}
	for _, test := range []struct {
		name string
		edit func(*HealthResponse)
	}{
		{name: "degraded", edit: func(h *HealthResponse) { h.State = RuntimeStateDegraded }},
		{name: "draining", edit: func(h *HealthResponse) { h.Draining = true }},
		{name: "busy", edit: func(h *HealthResponse) { h.Busy = true }},
		{name: "not ready", edit: func(h *HealthResponse) { h.Ready = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := healthy
			test.edit(&response)
			if err := validateDaemonHealth(response, build); err == nil ||
				!strings.Contains(err.Error(), "not ready") {
				t.Fatalf("%s = %v, want a not-ready refusal", test.name, err)
			}
		})
	}
}
