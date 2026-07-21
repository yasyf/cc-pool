//go:build darwin && cgo && fuse

// Package main exports the signed holder runtime archive entry points.
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
	"time"

	"github.com/yasyf/cc-pool/internal/holderbridge"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/tenantfs"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/fusekit/holder"
)

const (
	notChild        C.int32_t = -1
	operationFailed C.int32_t = 1
	startTimeout              = 60 * time.Second
	readyTimeout              = 5 * time.Second
	shutdownTimeout           = 30 * time.Second
)

var (
	embeddedHolder         daemon.EmbeddedProcess
	embeddedHolderStopping atomic.Bool
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
func CCPoolFuseKitStart() C.int32_t {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	return operationStatus("holder start", startHolder(ctx))
}

func startHolder(ctx context.Context) error {
	if err := embeddedHolder.Start(ctx, newHolderRuntime); err != nil {
		return err
	}
	err := tenantfs.PublishClaudeSourceFleet(
		ctx, pool.FuseKitSocketPath(), claudeAuthorityPolicy(),
	)
	if err == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	return errors.Join(
		fmt.Errorf("publish exact source fleet: %w", err),
		embeddedHolder.Close(cleanupCtx),
	)
}

//export CCPoolFuseKitReady
func CCPoolFuseKitReady() C.int32_t {
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	return operationStatus("holder readiness", embeddedHolder.Ready(ctx))
}

//export CCPoolFuseKitWait
func CCPoolFuseKitWait() C.int32_t {
	err := embeddedHolder.Wait(context.Background())
	if err == nil && !embeddedHolderStopping.Load() {
		err = errors.New("holder runtime exited before shutdown")
	}
	return operationStatus("holder terminal settlement", err)
}

//export CCPoolFuseKitStop
func CCPoolFuseKitStop() C.int32_t {
	embeddedHolderStopping.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return operationStatus("holder shutdown", embeddedHolder.Close(ctx))
}

func newHolderRuntime(ctx context.Context) (daemon.EmbeddedRuntime, error) {
	toolRunner, err := holderbridge.NewToolRunner(ctx)
	if err != nil {
		return nil, err
	}
	plan, planErr := holderbridge.NewRuntimePlan(
		toolRunner, pool.WidgetAppPath(), pool.FuseKitRuntimeDir(),
		pool.FuseKitPresentationRoot(), version.String(),
	)
	err = errors.Join(planErr, toolRunner.Close(ctx))
	if err != nil {
		return nil, err
	}
	drivers, err := claudeDriverFactories()
	if err != nil {
		return nil, err
	}
	return holderbridge.NewEmbeddedRuntime(ctx, holderbridge.EmbeddedRuntimeSpec{
		Plan: plan, Build: version.String(),
		Owner: tenantfs.SourceAuthorityFleetOwner, Drivers: drivers,
		CatalogAuthorizer: tenantfs.NewCatalogAuthorizer(),
		Authorizer:        tenantfs.NewMountAuthorizer(),
		ShutdownTimeout:   shutdownTimeout,
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
