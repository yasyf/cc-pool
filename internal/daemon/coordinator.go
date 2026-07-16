package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

type dirtyCause uint8

const (
	dirtyCanonical dirtyCause = 1 << iota
	dirtySettings
	dirtyStructure
	dirtyHeartbeat
	dirtyStartup
	dirtyApp
	dirtyRetry
)

type dirtyQueue struct {
	mu      sync.Mutex
	pending dirtyCause
	ready   chan struct{}
}

func newDirtyQueue() *dirtyQueue { return &dirtyQueue{ready: make(chan struct{}, 1)} }

func (q *dirtyQueue) mark(cause dirtyCause) {
	q.mu.Lock()
	q.pending |= cause
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *dirtyQueue) take(ctx context.Context) (dirtyCause, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case <-q.ready:
	}
	q.mu.Lock()
	cause := q.pending
	q.pending = 0
	q.mu.Unlock()
	return cause, true
}

type desiredOverlay struct {
	stamps overlay.SemanticStamps
}

type appBuildResult struct {
	attempted bool
	stamp     string
	err       error
}

type overlayCoordinator struct {
	server         *Server
	dirty          *dirtyQueue
	appBuild       func(context.Context) (string, error)
	watch          func(context.Context, overlay.SemanticInputPaths, func(dirtyCause)) error
	watchRetryBase time.Duration

	mu          sync.RWMutex
	desired     desiredOverlay
	initialized bool
	retryMu     sync.Mutex
	retry       *time.Timer
	retryStep   int
}

var errContentApplyDeferred = errors.New("content apply deferred by an account claim")

var runSemanticWatcher = watchSemanticInputs

const (
	semanticWatcherRetryBase = time.Second
	semanticWatcherRetryMax  = 30 * time.Second
)

func newOverlayCoordinator(s *Server) *overlayCoordinator {
	return &overlayCoordinator{
		server: s,
		dirty:  newDirtyQueue(),
		appBuild: func(ctx context.Context) (string, error) {
			return fileproviderd.NewAppClient(pool.FPControlSocketPath()).Health(ctx)
		},
		watch:          runSemanticWatcher,
		watchRetryBase: semanticWatcherRetryBase,
	}
}

func (c *overlayCoordinator) initialize() error {
	stamps, _, err := c.server.contentSource.RefreshSemanticStamps()
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.desired = desiredOverlay{stamps: stamps}
	c.initialized = true
	c.mu.Unlock()
	return nil
}

func (c *overlayCoordinator) mark(cause dirtyCause) { c.dirty.mark(cause) }

func (c *overlayCoordinator) start(ctx context.Context) {
	c.server.wg.Add(2)
	go func() {
		defer c.server.wg.Done()
		delay := c.watchRetryBase
		if delay <= 0 {
			delay = semanticWatcherRetryBase
		}
		for ctx.Err() == nil {
			err := c.watch(ctx, c.inputPaths(), c.mark)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				c.server.log.Printf("content event watcher stopped; retrying in %s: %v", delay, err)
			} else {
				c.server.log.Printf("content event watcher stopped unexpectedly; retrying in %s", delay)
			}
			c.mark(dirtyStartup)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay = min(delay*2, semanticWatcherRetryMax)
		}
	}()
	go func() {
		defer c.server.wg.Done()
		c.run(ctx)
	}()
	c.mark(dirtyStartup)
}

func (c *overlayCoordinator) inputPaths() overlay.SemanticInputPaths {
	inputs := c.server.contentSource.InputPaths()
	inputs.AppBuild = filepath.Join(pool.WidgetAppPath(), "Contents", "Info.plist")
	inputs.AppParent = filepath.Dir(pool.WidgetAppPath())
	return inputs
}

func (c *overlayCoordinator) run(ctx context.Context) {
	defer c.stopRetry()
	for {
		cause, ok := c.dirty.take(ctx)
		if !ok {
			return
		}
		c.converge(ctx, cause)
	}
}

func (c *overlayCoordinator) converge(ctx context.Context, cause dirtyCause) {
	if cause&dirtyApp != 0 && c.server.reconcileStaleFPApp(ctx) {
		if c.server.fpAppWanted() {
			c.server.ensureFPAppAsync(ctx)
		}
		c.scheduleRetry(ctx)
		return
	}
	stamps, changes, err := c.server.contentSource.RefreshSemanticStamps()
	if err != nil {
		c.server.log.Printf("refresh semantic content stamps: %v", err)
		c.scheduleRetry(ctx)
		return
	}
	c.mu.Lock()
	c.desired = desiredOverlay{stamps: stamps}
	c.initialized = true
	c.mu.Unlock()
	semanticChanged := changes.Canonical || changes.Settings || changes.Structure
	if !semanticChanged && cause&(dirtyHeartbeat|dirtyStartup|dirtyApp|dirtyRetry) == 0 {
		return
	}
	accounts, err := c.server.m.Store.ListAccounts()
	if err != nil {
		c.server.log.Printf("content coordinator: list accounts: %v", err)
		c.scheduleRetry(ctx)
		return
	}
	snapshot := c.server.heartbeatFor().view()
	var appBuild appBuildResult
	failed := false
	for _, account := range accounts {
		if ctx.Err() != nil {
			return
		}
		if snapshot.sessionCount(account.ConfigDir) == 0 {
			continue
		}
		if err := c.applyAccount(ctx, account, false, &appBuild); err != nil {
			c.server.log.Printf("acct-%02d content apply: %v", account.ID, err)
			failed = true
		}
	}
	if failed {
		c.scheduleRetry(ctx)
	} else {
		c.resetRetry()
	}
}

func (c *overlayCoordinator) catchUp(ctx context.Context, account store.Account) error {
	var appBuild appBuildResult
	return c.applyAccount(ctx, account, true, &appBuild)
}

func (c *overlayCoordinator) current() (desiredOverlay, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.desired, c.initialized
}

func (c *overlayCoordinator) applyAccount(ctx context.Context, listed store.Account, allowIdle bool, appBuild *appBuildResult) error {
	if !allowIdle && c.server.heartbeatFor().view().sessionCount(listed.ConfigDir) == 0 {
		return nil
	}
	claimed := c.server.cl.hold(listed.ID)
	if allowIdle && !claimed {
		claimed = c.server.cl.holdContext(ctx, listed.ID)
	}
	if !claimed {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errContentApplyDeferred
	}
	defer c.server.cl.disownHold(listed.ID)
	fresh, err := c.server.m.Store.GetAccount(listed.ID)
	if errors.Is(err, store.ErrAccountNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("re-read row: %w", err)
	}
	if !allowIdle && c.server.heartbeatFor().view().sessionCount(fresh.ConfigDir) == 0 {
		return nil
	}
	desired, ok := c.current()
	if !ok {
		return errors.New("semantic content stamps are not initialized")
	}
	backend, err := fkoverlay.Parse(fresh.OverlayKind)
	if err != nil {
		return fmt.Errorf("parse stored backend: %w", err)
	}
	wantApp := ""
	if backend == fkoverlay.BackendFileProvider {
		if !appBuild.attempted {
			appBuild.attempted = true
			stampCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			appBuild.stamp, appBuild.err = c.appBuild(stampCtx)
			cancel()
		}
		if appBuild.err != nil {
			return fmt.Errorf("read companion app build: %w", appBuild.err)
		}
		wantApp = appBuild.stamp
	}
	applied, present, err := c.server.m.Store.GetOverlayApplied(fresh.ID)
	if err != nil {
		return err
	}
	want := store.OverlayApplied{
		AccountID: fresh.ID, Backend: fresh.OverlayKind,
		CanonicalStamp: desired.stamps.Canonical,
		SettingsStamp:  desired.stamps.Settings,
		StructureStamp: desired.stamps.Structure,
		AppStamp:       wantApp,
	}
	if backend == fkoverlay.BackendSymlink &&
		(!present || applied.Backend != want.Backend || applied.CanonicalStamp != want.CanonicalStamp) {
		if _, err := c.server.m.MergeBaseClaudeJSON(fresh); err != nil {
			return fmt.Errorf("merge canonical config: %w", err)
		}
	}
	generationMatches := present && sameAppliedGeneration(applied, want)
	var provider fkoverlay.Provider
	if generationMatches {
		if !allowIdle {
			return nil
		}
		provider = c.server.overlayFor(backend)
		if provider == nil {
			return fmt.Errorf("resolve provider for %s", backend)
		}
		return checkOrReconcile(ctx, provider, fresh.ConfigDir)
	}
	if backend != fkoverlay.BackendFileProvider && present &&
		applied.Backend == want.Backend && applied.StructureStamp == want.StructureStamp {
		if allowIdle {
			provider = c.server.overlayFor(backend)
			if provider == nil {
				return fmt.Errorf("resolve provider for %s", backend)
			}
			if err := checkOrReconcile(ctx, provider, fresh.ConfigDir); err != nil {
				return err
			}
		}
		want.AppliedAt = time.Now()
		return c.recordApplied(want)
	}
	provider = c.server.overlayFor(backend)
	if provider == nil {
		return fmt.Errorf("resolve provider for %s", backend)
	}
	if backend == fkoverlay.BackendFileProvider {
		if err := checkOrReconcile(ctx, provider, fresh.ConfigDir); err != nil {
			return err
		}
		notifier, ok := provider.(fkoverlay.ContentNotifier)
		if !ok {
			return errors.New("file provider does not implement ContentNotifier")
		}
		if err := notifier.NotifyContent(ctx, fresh.ConfigDir); err != nil {
			return fmt.Errorf("notify content: %w", err)
		}
	} else {
		if err := provider.Reconcile(ctx, pool.ClaudeDir(), fresh.ConfigDir); err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}
		if err := provider.Check(ctx, pool.ClaudeDir(), fresh.ConfigDir); err != nil {
			return fmt.Errorf("check applied overlay: %w", err)
		}
	}
	want.AppliedAt = time.Now()
	return c.recordApplied(want)
}

func (c *overlayCoordinator) recordApplied(applied store.OverlayApplied) error {
	if err := c.server.m.Store.SetOverlayApplied(applied); err != nil {
		if errors.Is(err, store.ErrAccountNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func checkOrReconcile(ctx context.Context, provider fkoverlay.Provider, dir string) error {
	base := pool.ClaudeDir()
	if err := provider.Check(ctx, base, dir); err == nil {
		return nil
	}
	if err := provider.Reconcile(ctx, base, dir); err != nil {
		return fmt.Errorf("reconcile failed check: %w", err)
	}
	if err := provider.Check(ctx, base, dir); err != nil {
		return fmt.Errorf("check reconciled overlay: %w", err)
	}
	return nil
}

func (c *overlayCoordinator) scheduleRetry(ctx context.Context) {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	if c.retry != nil {
		return
	}
	delay := 5 * time.Second * time.Duration(1<<min(c.retryStep, 4))
	c.retryStep++
	c.retry = time.AfterFunc(delay, func() {
		c.retryMu.Lock()
		c.retry = nil
		c.retryMu.Unlock()
		if ctx.Err() == nil {
			c.mark(dirtyRetry)
		}
	})
}

func (c *overlayCoordinator) resetRetry() {
	c.retryMu.Lock()
	if c.retry != nil {
		c.retry.Stop()
		c.retry = nil
	}
	c.retryStep = 0
	c.retryMu.Unlock()
}

func (c *overlayCoordinator) stopRetry() {
	c.retryMu.Lock()
	if c.retry != nil {
		c.retry.Stop()
		c.retry = nil
	}
	c.retryMu.Unlock()
}

func sameAppliedGeneration(got, want store.OverlayApplied) bool {
	return got.Backend == want.Backend &&
		got.CanonicalStamp == want.CanonicalStamp &&
		got.SettingsStamp == want.SettingsStamp &&
		got.StructureStamp == want.StructureStamp &&
		got.AppStamp == want.AppStamp
}

func (s *Server) catchUpOverlay(ctx context.Context, account store.Account) error {
	if s.overlayCoordinator == nil {
		return nil
	}
	return s.overlayCoordinator.catchUp(ctx, account)
}
