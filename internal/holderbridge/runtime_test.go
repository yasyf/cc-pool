package holderbridge

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/holder"
)

func TestNewEmbeddedRuntimeSuppliesCatalogOperationDeadline(t *testing.T) {
	want := errors.New("stop after config capture")
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "stop-processes.db")}
	var got holder.Config
	runtime, err := newEmbeddedRuntime(
		t.Context(), EmbeddedRuntimeSpec{StopRole: StopRoleID, StopControlStore: store},
		func(_ context.Context, config holder.Config) (*holder.Runtime, error) {
			got = config
			return nil, want
		},
	)
	if runtime != nil || !errors.Is(err, want) {
		t.Fatalf("runtime/error = %#v/%v, want literal nil and %v", runtime, err, want)
	}
	if got.CatalogOperationTimeout != 30*time.Second {
		t.Fatalf("catalog operation timeout = %s, want 30s", got.CatalogOperationTimeout)
	}
	if got.RuntimeBuild != got.Plan.BuildID() {
		t.Fatalf("holder config build = %q, plan build = %q", got.RuntimeBuild, got.Plan.BuildID())
	}
	if got.StopRole != StopRoleID || got.StopControlStore != store {
		t.Fatalf("holder stop authority = %q/%T", got.StopRole, got.StopControlStore)
	}
	if got.NativeStdout != os.Stdout || got.NativeStderr != os.Stderr {
		t.Fatalf("native output = %#v/%#v, want process stdout/stderr", got.NativeStdout, got.NativeStderr)
	}
}

func TestConstructEmbeddedRuntimePreservesValidationErrorWithoutTypedNil(t *testing.T) {
	want := errors.New("holder: positive catalog hard operation timeout is required")
	runtime, err := constructEmbeddedRuntime(
		t.Context(), holder.Config{},
		func(context.Context, holder.Config) (*holder.Runtime, error) { return nil, want },
	)
	if runtime != nil {
		t.Fatalf("runtime = %#v, want literal nil interface", runtime)
	}
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want original validation error %v", err, want)
	}
}
