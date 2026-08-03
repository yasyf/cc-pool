package holderbridge

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/holder"
)

func TestNewRuntimeSuppliesExactRuntimeDeadlines(t *testing.T) {
	want := errors.New("stop after config capture")
	trust := RuntimeTrust("group.com.yasyf.cc-pool")
	var got holder.Config
	runtime, err := newRuntime(
		t.Context(), RuntimeSpec{Trust: trust},
		func(_ context.Context, config holder.Config) (*holder.Runtime, error) {
			got = config
			return nil, want
		},
	)
	if runtime != nil || !errors.Is(err, want) {
		t.Fatalf("runtime/error = %#v/%v, want literal nil and %v", runtime, err, want)
	}
	if got.NativeReadinessTimeout != NativeReadinessTimeout ||
		got.CatalogReadinessTimeout != CatalogReadinessTimeout ||
		got.CatalogOperationTimeout != CatalogOperationTimeout ||
		got.ShutdownTimeout != RuntimeShutdownTimeout {
		t.Fatalf(
			"runtime timeouts = native %s, catalog readiness %s, operation %s, shutdown %s",
			got.NativeReadinessTimeout,
			got.CatalogReadinessTimeout, got.CatalogOperationTimeout, got.ShutdownTimeout,
		)
	}
	if NativeReadinessTimeout <= 0 ||
		CatalogReadinessTimeout <= 0 || CatalogOperationTimeout <= 0 || RuntimeShutdownTimeout <= 0 {
		t.Fatal("cc-pool runtime deadlines must be explicit and positive")
	}
	if got.RuntimeBuild != got.Plan.BuildID() {
		t.Fatalf("holder config build = %q, plan build = %q", got.RuntimeBuild, got.Plan.BuildID())
	}
	if !reflect.DeepEqual(got.Trust, trust) {
		t.Fatalf("holder trust = %#v", got.Trust)
	}
}

func TestNewRuntimePreservesValidationErrorWithoutTypedNil(t *testing.T) {
	want := errors.New("holder: positive catalog hard operation timeout is required")
	runtime, err := newRuntime(
		t.Context(), RuntimeSpec{},
		func(context.Context, holder.Config) (*holder.Runtime, error) { return nil, want },
	)
	if runtime != nil {
		t.Fatalf("runtime = %#v, want literal nil interface", runtime)
	}
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want original validation error %v", err, want)
	}
}
