package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

type recordingOverlay struct {
	stubOverlay
	reconciled bool
	notified   bool
}

func (r *recordingOverlay) Reconcile(ctx context.Context, _, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.reconciled = true
	return nil
}

func (r *recordingOverlay) NotifyContent(context.Context, string) error {
	r.notified = true
	return nil
}

func TestReconcileOverlayIsExplicitLifecycleOnly(t *testing.T) {
	a := store.Account{ID: 7, ConfigDir: "/pool/acct-07", OverlayKind: "fileprovider"}
	prov := &recordingOverlay{stubOverlay: stubOverlay{backend: fkoverlay.BackendFileProvider}}
	m := &Manager{OverlayFor: func(fkoverlay.Backend) (fkoverlay.Provider, error) { return prov, nil }}

	if err := m.ReconcileOverlay(t.Context(), a); err != nil {
		t.Fatalf("ReconcileOverlay = %v, want nil", err)
	}
	if !prov.reconciled {
		t.Fatal("provider Reconcile was not called")
	}
	if prov.notified {
		t.Fatal("ReconcileOverlay called NotifyContent; lifecycle repair must not impersonate a content commit")
	}
}

func TestReconcileOverlayHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prov := &recordingOverlay{stubOverlay: stubOverlay{backend: fkoverlay.BackendSymlink}}
	m := &Manager{OverlayFor: func(fkoverlay.Backend) (fkoverlay.Provider, error) { return prov, nil }}

	err := m.ReconcileOverlay(ctx, store.Account{ID: 7, ConfigDir: "/pool/acct-07", OverlayKind: "symlink"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReconcileOverlay = %v, want context.Canceled", err)
	}
	if prov.reconciled {
		t.Fatal("provider Reconcile ran with an already-canceled context")
	}
}
