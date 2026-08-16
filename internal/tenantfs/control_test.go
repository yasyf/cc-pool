package tenantfs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/holder"
)

type recordingBusiness struct {
	deadline time.Time
	stated   bool
	body     []byte
}

func (r *recordingBusiness) Call(ctx context.Context, _ string, _ []byte) (daemonkit.Reply, error) {
	r.deadline, r.stated = ctx.Deadline()
	return daemonkit.Reply{Body: r.body}, nil
}

func (r *recordingBusiness) Close(context.Context) error { return nil }

func admittedReadiness() holder.LocalRuntimeReadiness {
	return holder.LocalRuntimeReadiness{
		RuntimeBuild:         version.String(),
		ActivationGeneration: "activation-1",
		ProcessGeneration:    catalog.ProcessGeneration{1},
	}
}

func admittedReadinessLane(t *testing.T) (*ControlClient, *recordingBusiness) {
	t.Helper()
	body, err := json.Marshal(readinessResponse{
		controlHeader: controlHeader{Protocol: controlProtocol, Code: ControlErrorOK},
		Readiness:     admittedReadiness(),
	})
	if err != nil {
		t.Fatalf("encode the admitted readiness reply: %v", err)
	}
	business := &recordingBusiness{body: body}
	return &ControlClient{business: business}, business
}

func TestTenantLaneCallSpendsTheCallersOwnStatedDeadline(t *testing.T) {
	client, business := admittedReadinessLane(t)
	stated, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	readiness, err := client.Readiness(stated)
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if readiness != admittedReadiness() {
		t.Fatalf("Readiness = %+v, want the admitted publication", readiness)
	}
	want, ok := stated.Deadline()
	if !ok {
		t.Fatal("the caller's own context carries no deadline")
	}
	if !business.stated || !business.deadline.Equal(want) {
		t.Fatalf(
			"lane call deadline = %v (stated=%t), want the caller's own %v",
			business.deadline, business.stated, want,
		)
	}
}

func TestTenantLaneCallStatesItsOwnBudgetForADeadlinelessCaller(t *testing.T) {
	client, business := admittedReadinessLane(t)
	before := time.Now()
	if _, err := client.Readiness(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	after := time.Now()
	if !business.stated {
		t.Fatal("lane call carried no deadline; daemonkit refuses a deadline-less Call")
	}
	if business.deadline.Before(before.Add(controlOperationTimeout)) ||
		business.deadline.After(after.Add(controlOperationTimeout)) {
		t.Fatalf(
			"lane call deadline = %v, want the lane's own %v budget stated at the call",
			business.deadline, controlOperationTimeout,
		)
	}
}

func TestControlRemoteErrorCarriesItsCodeAsTheSentinelAndStatesTheFactOnce(t *testing.T) {
	tests := []struct {
		name       string
		code       ControlErrorCode
		message    string
		wantAbsent bool
	}{
		{"absent", ControlErrorNotFound, catalog.ErrNotFound.Error(), true},
		{"unclassified", ControlErrorFailed, "retire tenant: disk is full", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &ControlRemoteError{Code: tt.code, Message: tt.message}
			if got := errors.Is(err, catalog.ErrNotFound); got != tt.wantAbsent {
				t.Fatalf("errors.Is(%v, catalog.ErrNotFound) = %t, want %t", err, got, tt.wantAbsent)
			}
			if got := strings.Count(err.Error(), tt.message); got != 1 {
				t.Fatalf("%q states %q %d times, want 1", err, tt.message, got)
			}
		})
	}
}
