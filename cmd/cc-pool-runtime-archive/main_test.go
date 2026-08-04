//go:build darwin && cgo

package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
)

type stubLane struct {
	ended  chan struct{}
	result error

	mu     sync.Mutex
	once   sync.Once
	closes int
}

func newStubLane() *stubLane { return &stubLane{ended: make(chan struct{})} }

func (l *stubLane) end() { l.once.Do(func() { close(l.ended) }) }

func (l *stubLane) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.ended:
		return l.result
	}
}

func (l *stubLane) Close(ctx context.Context) error {
	l.mu.Lock()
	l.closes++
	l.mu.Unlock()
	l.end()
	return l.Wait(ctx)
}

func (l *stubLane) closeCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closes
}

type stubRuntime struct {
	once    sync.Once
	stopped chan struct{}
}

func newStubRuntime() *stubRuntime { return &stubRuntime{stopped: make(chan struct{})} }

func (r *stubRuntime) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-r.stopped:
	}
	return nil
}

func (r *stubRuntime) WaitReady(context.Context) error { return nil }

func (r *stubRuntime) Close(context.Context) error {
	r.once.Do(func() { close(r.stopped) })
	return nil
}

func (r *stubRuntime) Wait(context.Context) error { return nil }

func useEmbeddedLanes(t *testing.T, lanes ...embeddedLane) {
	t.Helper()
	oldLanes, oldSettled := embeddedLanes, embeddedSettled
	embeddedLanes, embeddedSettled = func() []embeddedLane { return lanes }, make(chan struct{})
	t.Cleanup(func() { embeddedLanes, embeddedSettled = oldLanes, oldSettled })
}

func useEmbeddedHolder(t *testing.T) {
	t.Helper()
	oldHolder, oldSettled := embeddedHolder, embeddedSettled
	embeddedHolder, embeddedSettled = &holderbridge.Process{}, make(chan struct{})
	if err := embeddedHolder.Start(t.Context(), func(context.Context) (holderbridge.ProcessRuntime, error) {
		return newStubRuntime(), nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := embeddedHolder.Close(context.Background()); err != nil {
			t.Errorf("holder teardown = %v", err)
		}
		embeddedHolder, embeddedSettled = oldHolder, oldSettled
	})
}

func TestSettleEmbeddedEndsTheAppWhenEitherHalfEnds(t *testing.T) {
	tests := []struct {
		name      string
		endTenant bool
	}{
		{"the tenant lane ends alone", true},
		{"the FuseKit runtime ends alone", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terminal := errors.New("embedded lane serve failed")
			tenantLane, runtimeLane := newStubLane(), newStubLane()
			ended, survivor := runtimeLane, tenantLane
			if tt.endTenant {
				ended, survivor = tenantLane, runtimeLane
			}
			ended.result = terminal
			useEmbeddedLanes(t, tenantLane, runtimeLane)
			go superviseEmbedded(context.Background())
			ended.end()

			settlement := make(chan error, 1)
			go func() { settlement <- settleEmbedded(context.Background()) }()
			select {
			case err := <-settlement:
				if !errors.Is(err, terminal) {
					t.Fatalf("settlement = %v, want the ended lane's own terminal result", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the wait never returned after one half of the publication ended")
			}
			if survivor.closeCount() == 0 {
				t.Fatal("the surviving half kept serving after the other one ended")
			}
		})
	}
}

func TestCCPoolFuseKitWaitReportsATenantLaneThatEndedBeforeShutdown(t *testing.T) {
	tenantLane, runtimeLane := newStubLane(), newStubLane()
	useEmbeddedLanes(t, tenantLane, runtimeLane)
	go superviseEmbedded(context.Background())
	tenantLane.end()

	status := make(chan int32, 1)
	go func() { status <- int32(CCPoolFuseKitWait()) }()
	select {
	case got := <-status:
		if got != int32(operationFailed) {
			t.Fatalf("wait status = %d, want %d for a tenant lane that ended on its own", got, operationFailed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CCPoolFuseKitWait never returned after the tenant lane ended")
	}
	if runtimeLane.closeCount() == 0 {
		t.Fatal("the FuseKit runtime kept serving after the tenant lane ended")
	}
}

func TestCCPoolFuseKitReadyRefusesAnEndedEmbeddedLane(t *testing.T) {
	useEmbeddedHolder(t)
	if status := CCPoolFuseKitReady(); status != 0 {
		t.Fatalf("readiness = %d for a whole publication", status)
	}
	close(embeddedSettled)
	if status := CCPoolFuseKitReady(); status != operationFailed {
		t.Fatalf("readiness = %d after an embedded lane ended, want %d", status, operationFailed)
	}
}
