//go:build darwin && cgo

// Package main exports the signed FuseKit runtime archive entry points.
package main

/*
#include <stdint.h>
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit/holder"
)

const (
	notChild        C.int32_t = -1
	operationFailed C.int32_t = 1
)

// embeddedLane is one half of the app's publication as supervision and
// settlement read it: FuseKit's own runtime process, or the tenant lane serving
// beside it.
type embeddedLane interface {
	Wait(context.Context) error
	Close(context.Context) error
}

var (
	embeddedHolder         = &holderbridge.Process{}
	embeddedTenants        = &tenantfs.Lane{}
	embeddedHolderStopping atomic.Bool
	embeddedSettled        = make(chan struct{})
	embeddedLanes          = func() []embeddedLane { return []embeddedLane{embeddedTenants, embeddedHolder} }
)

//export CCPoolFuseKitDispatchChild
func CCPoolFuseKitDispatchChild() C.int32_t {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	drivers, err := claudeDriverFactories()
	if err != nil {
		return operationStatus("source driver registry", err)
	}
	recognized, err := holder.RunChild(ctx, os.Args[1:], holder.ChildConfig{
		Stdout: os.Stdout, Drivers: drivers,
	})
	if !recognized {
		return notChild
	}
	return operationStatus("child", err)
}

//export CCPoolFuseKitStart
func CCPoolFuseKitStart(appGroupIdentifier *C.char) C.int32_t {
	if appGroupIdentifier == nil {
		return operationStatus("runtime start", errors.New("signed app group identifier is required"))
	}
	requiredAppGroup := C.GoString(appGroupIdentifier)
	ctx, cancel := context.WithTimeout(
		context.Background(), holderbridge.ReadinessContract().StartupTimeout(),
	)
	defer cancel()
	return operationStatus("runtime start", startHolder(ctx, requiredAppGroup))
}

func startHolder(ctx context.Context, requiredAppGroup string) error {
	var runtime *holder.Runtime
	if err := embeddedHolder.Start(ctx, func(ctx context.Context) (holderbridge.ProcessRuntime, error) {
		constructed, err := newHolderRuntime(ctx, requiredAppGroup)
		runtime = constructed
		return constructed, err
	}); err != nil {
		return err
	}
	err := tenantfs.PublishClaudeSourceFleet(
		ctx, runtime.LocalTenantController(), claudeAuthorityPolicy(),
	)
	if err == nil {
		err = embeddedTenants.Start(ctx, runtime.LocalTenantController())
	}
	if err == nil {
		go superviseEmbedded(context.WithoutCancel(ctx))
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), holderbridge.ReadinessContract().SettlementTimeout(),
	)
	defer cancel()
	return errors.Join(
		fmt.Errorf("publish exact source fleet: %w", err),
		embeddedHolder.Close(cleanupCtx),
	)
}

//export CCPoolFuseKitReady
func CCPoolFuseKitReady() C.int32_t {
	ctx, cancel := context.WithTimeout(
		context.Background(), holderbridge.ReadinessContract().StartupTimeout(),
	)
	defer cancel()
	return operationStatus("runtime readiness", readyEmbedded(ctx))
}

//export CCPoolFuseKitWait
func CCPoolFuseKitWait() C.int32_t {
	err := settleEmbedded(context.Background())
	if err == nil && !embeddedHolderStopping.Load() {
		err = errors.New("runtime exited before shutdown")
	}
	return operationStatus("runtime terminal settlement", err)
}

//export CCPoolFuseKitStop
func CCPoolFuseKitStop() C.int32_t {
	embeddedHolderStopping.Store(true)
	ctx, cancel := context.WithTimeout(
		context.Background(), holderbridge.RuntimeShutdownTimeout,
	)
	defer cancel()
	return operationStatus("runtime shutdown", closeEmbedded(ctx))
}

// superviseEmbedded closes embeddedSettled the moment either half of the
// publication ends. The FuseKit runtime and the tenant lane serving beside it
// are halves of one thing: a runtime no tenant can reach, or a tenant lane over
// a runtime that is gone, is a husk that still answers the socket, so launchd
// sees nothing to restart.
func superviseEmbedded(ctx context.Context) {
	lanes := embeddedLanes()
	ended := make(chan struct{}, len(lanes))
	for _, lane := range lanes {
		go func() {
			_ = lane.Wait(ctx)
			ended <- struct{}{}
		}()
	}
	<-ended
	close(embeddedSettled)
}

// readyEmbedded proves both halves of the publication: FuseKit's own committed
// readiness, and a tenant lane still serving the source fleet startHolder
// published through it.
func readyEmbedded(ctx context.Context) error {
	if err := embeddedHolder.Ready(ctx); err != nil {
		return err
	}
	select {
	case <-embeddedSettled:
		return errors.New("an embedded lane ended before readiness")
	default:
		return nil
	}
}

// settleEmbedded returns once either half of the publication ends and drains
// the other, so a lane that exited on its own terminates the app instead of
// leaving it serving half a runtime. Close replays each half's own terminal
// result, so the wait reports what actually ended.
func settleEmbedded(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-embeddedSettled:
	}
	shutdown, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), holderbridge.RuntimeShutdownTimeout,
	)
	defer cancel()
	return closeEmbedded(shutdown)
}

func closeEmbedded(ctx context.Context) error {
	var settlement error
	for _, lane := range embeddedLanes() {
		settlement = errors.Join(settlement, lane.Close(ctx))
	}
	return settlement
}

func newHolderRuntime(ctx context.Context, requiredAppGroup string) (*holder.Runtime, error) {
	plan, err := holderbridge.NewRuntimePlan(
		pool.WidgetAppPath(), pool.FuseKitRuntimeDir(), version.String(), requiredAppGroup,
	)
	if err != nil {
		return nil, err
	}
	drivers, err := claudeDriverFactories()
	if err != nil {
		return nil, err
	}
	return holderbridge.NewRuntime(ctx, holderbridge.RuntimeSpec{
		Plan:  plan,
		Trust: holderbridge.RuntimeTrust(requiredAppGroup),
		Owner: tenantfs.SourceAuthorityFleetOwner, Drivers: drivers,
		CatalogAuthorizer: tenantfs.NewCatalogAuthorizer(),
		Authorizer:        tenantfs.NewMountAuthorizer(),
	})
}

func claudeDriverFactories() (holder.DriverFactories, error) {
	return tenantfs.NewClaudeDriverFactories(claudeAuthorityPolicy())
}

func claudeAuthorityPolicy() tenantfs.ClaudeAuthorityPolicy {
	return tenantfs.ClaudeAuthorityPolicy{
		ClaudeDir: pool.ClaudeDir(), ClaudeJSONPath: pool.ClaudeJSONPath(),
	}
}

func operationStatus(operation string, err error) C.int32_t {
	if err == nil {
		return 0
	}
	_, _ = fmt.Fprintf(os.Stderr, "CCPoolStatus: FuseKit %s failed: %v\n", operation, err)
	return operationFailed
}

func main() {}
