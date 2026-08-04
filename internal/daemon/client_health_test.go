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

// TestDecodeDaemonHealthDetailDerivesLifecycleFromPhase drives the real
// client pipeline over stale detail: the decoded bytes replay the daemon's
// steady-state report — Ready, not Draining — while the live Phase says
// otherwise, and the Phase must win all the way through validation.
func TestDecodeDaemonHealthDetailDerivesLifecycleFromPhase(t *testing.T) {
	const build = "cc-pool-test"
	steady := HealthResponse{
		Schema: DaemonHealthSchema, RuntimeBuild: build,
		State: RuntimeStateHealthy, Ready: true,
	}
	payload, err := json.Marshal(steady)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name         string
		phase        daemonkit.Phase
		wantReady    bool
		wantDraining bool
		wantValid    bool
	}{
		{"ready", daemonkit.PhaseReady, true, false, true},
		{"draining", daemonkit.PhaseDraining, false, true, false},
		{"starting", daemonkit.PhaseStarting, false, false, false},
		{"failed", daemonkit.PhaseFailed, false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := decodeDaemonHealthDetail(daemonkit.Health{Phase: tt.phase, Detail: payload})
			if err != nil {
				t.Fatal(err)
			}
			if response.Ready != tt.wantReady || response.Draining != tt.wantDraining {
				t.Fatalf("decoded ready=%t draining=%t, want ready=%t draining=%t",
					response.Ready, response.Draining, tt.wantReady, tt.wantDraining)
			}
			err = validateDaemonHealth(*response, build)
			if tt.wantValid && err != nil {
				t.Fatalf("ready phase refused: %v", err)
			}
			if !tt.wantValid && err == nil {
				t.Fatal("stale steady-state detail passed readiness against a non-ready phase")
			}
		})
	}
}

// TestHandleRepublishesLiveHealthDetail pins the count-freshness half: every
// business dispatch republishes the product detail, so the counts a health
// read observes are as of the last op, not the startup snapshot.
func TestHandleRepublishesLiveHealthDetail(t *testing.T) {
	s, _, account := newAccountMutationTestServer(t, true)
	var published [][]byte
	s.report = func(detail []byte) {
		published = append(published, append([]byte(nil), detail...))
	}
	lastClaims := func(t *testing.T) int {
		t.Helper()
		if _, err := s.Handle(t.Context(), daemonkit.Request{Op: "no-such-op", Body: []byte("{}")}); err == nil {
			t.Fatal("unknown op dispatched")
		}
		if len(published) == 0 {
			t.Fatal("business dispatch republished no health detail")
		}
		var detail HealthResponse
		if err := json.Unmarshal(published[len(published)-1], &detail); err != nil {
			t.Fatal(err)
		}
		return detail.ExclusiveClaims
	}
	if !s.cl.ownExclusive(account.ID) {
		t.Fatal("exclusive claim refused")
	}
	if got := lastClaims(t); got != 1 {
		t.Fatalf("republished exclusive claims = %d, want the live claim", got)
	}
	s.cl.releaseExclusive(account.ID)
	if got := lastClaims(t); got != 0 {
		t.Fatalf("republished exclusive claims after release = %d, want 0", got)
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
