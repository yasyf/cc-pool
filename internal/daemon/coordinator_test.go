package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	ccoverlay "github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

func newCoordinatorTestSource(t *testing.T, s *Server) *overlayCoordinator {
	t.Helper()
	root := t.TempDir()
	claudeDir := filepath.Join(root, ".claude")
	if err := os.MkdirAll(claudeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	baseClaudeJSON := filepath.Join(root, ".claude.json")
	if err := os.WriteFile(baseClaudeJSON, []byte(`{"theme":"dark"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(claudeDir, "settings.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.contentSource = ccoverlay.NewPoolContentSource(claudeDir, baseClaudeJSON, filepath.Join(root, "stamps"))
	c := newOverlayCoordinator(s)
	if err := c.initialize(); err != nil {
		t.Fatal(err)
	}
	s.overlayCoordinator = c
	return c
}

func desiredApplied(t *testing.T, c *overlayCoordinator, account store.Account) store.OverlayApplied {
	t.Helper()
	desired, ok := c.current()
	if !ok {
		t.Fatal("coordinator is not initialized")
	}
	return store.OverlayApplied{
		AccountID:      account.ID,
		Backend:        account.OverlayKind,
		CanonicalStamp: desired.stamps.Canonical,
		SettingsStamp:  desired.stamps.Settings,
		StructureStamp: desired.stamps.Structure,
	}
}

func newSymlinkMergeCoordinator(t *testing.T) (*Server, *overlayCoordinator, store.Account, string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"theme":"light"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pool.ClaudeDir(), "settings.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s, dirs := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(account.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(account.ConfigDir, ".claude.json")
	if err := os.WriteFile(private, []byte(`{"theme":"dark"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.contentSource = ccoverlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath(), filepath.Join(home, "stamps"))
	c := newOverlayCoordinator(s)
	if err := c.initialize(); err != nil {
		t.Fatal(err)
	}
	s.overlayCoordinator = c
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}
	return s, c, account, private
}

func TestDirtyQueueCoalescesCauses(t *testing.T) {
	q := newDirtyQueue()
	q.mark(dirtyCanonical)
	q.mark(dirtySettings)
	q.mark(dirtyHeartbeat)
	cause, ok := q.take(t.Context())
	if !ok {
		t.Fatal("take failed")
	}
	want := dirtyCanonical | dirtySettings | dirtyHeartbeat
	if cause != want {
		t.Fatalf("cause = %08b, want %08b", cause, want)
	}
	if len(q.ready) != 0 {
		t.Fatalf("ready contains %d signals after one coalesced take", len(q.ready))
	}
}

func TestCoordinatorDirectContentDriftPersistsWithoutProviderIO(t *testing.T) {
	s, dirs := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendNFS)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFuseProv{}
	s.m.OverlayFor = func(_ fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	applied := desiredApplied(t, c, account)
	applied.CanonicalStamp = "old-canonical"
	if err := s.m.Store.SetOverlayApplied(applied); err != nil {
		t.Fatal(err)
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}

	var appBuild appBuildResult
	if err := c.applyAccount(t.Context(), account, false, &appBuild); err != nil {
		t.Fatal(err)
	}
	if fake.reconcileCount() != 0 || fake.checkCount() != 0 {
		t.Fatalf("canonical-only drift touched provider: reconcile=%d check=%d", fake.reconcileCount(), fake.checkCount())
	}
	got, present, err := s.m.Store.GetOverlayApplied(account.ID)
	if err != nil || !present {
		t.Fatalf("GetOverlayApplied = (%+v, %v, %v)", got, present, err)
	}
	want := desiredApplied(t, c, account)
	if !sameAppliedGeneration(got, want) {
		t.Fatalf("applied generation = %+v, want %+v", got, want)
	}
}

func TestCoordinatorSymlinkCanonicalDriftMergesBeforeStampingApplied(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"theme":"light"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pool.ClaudeDir(), "settings.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s, dirs := newTestServer(t)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(account.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(account.ConfigDir, ".claude.json"), []byte(`{"theme":"dark"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.contentSource = ccoverlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath(), filepath.Join(home, "stamps"))
	c := newOverlayCoordinator(s)
	if err := c.initialize(); err != nil {
		t.Fatal(err)
	}
	s.overlayCoordinator = c
	applied := desiredApplied(t, c, account)
	applied.CanonicalStamp = "old-canonical"
	if err := s.m.Store.SetOverlayApplied(applied); err != nil {
		t.Fatal(err)
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}

	var appBuild appBuildResult
	if err := c.applyAccount(t.Context(), account, false, &appBuild); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(account.ConfigDir, ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	var merged map[string]any
	if err := json.Unmarshal(payload, &merged); err != nil {
		t.Fatal(err)
	}
	if merged["theme"] != "light" {
		t.Fatalf("symlink canonical config = %v, want base theme applied", merged)
	}
	got, present, err := s.m.Store.GetOverlayApplied(account.ID)
	if err != nil || !present || got.CanonicalStamp != desiredApplied(t, c, account).CanonicalStamp {
		t.Fatalf("applied canonical stamp = (%+v, %v, %v)", got, present, err)
	}
}

func TestCoordinatorSelectionChecksSameGenerationAndRepairsDrift(t *testing.T) {
	s, _ := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendNFS)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFuseProv{checkErr: errors.New("shared link missing")}
	fake.reconcileFn = func(_, _ string) error {
		fake.mu.Lock()
		fake.checkErr = nil
		fake.mu.Unlock()
		return nil
	}
	s.m.OverlayFor = func(_ fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	if err := s.m.Store.SetOverlayApplied(desiredApplied(t, c, account)); err != nil {
		t.Fatal(err)
	}

	if err := c.catchUp(t.Context(), account); err != nil {
		t.Fatal(err)
	}
	if fake.reconcileCount() != 1 || fake.checkCount() != 2 {
		t.Fatalf("selection drift calls = reconcile %d, check %d; want 1, 2", fake.reconcileCount(), fake.checkCount())
	}
}

func TestCoordinatorSelectionRepairsGenerationDriftOnce(t *testing.T) {
	s, _ := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendNFS)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFuseProv{checkErr: errors.New("shared link missing")}
	fake.reconcileFn = func(_, _ string) error {
		fake.mu.Lock()
		fake.checkErr = nil
		fake.mu.Unlock()
		return nil
	}
	s.m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	applied := desiredApplied(t, c, account)
	applied.StructureStamp = "old-structure"
	if err := s.m.Store.SetOverlayApplied(applied); err != nil {
		t.Fatal(err)
	}
	if err := c.catchUp(t.Context(), account); err != nil {
		t.Fatal(err)
	}
	if fake.reconcileCount() != 1 || fake.checkCount() != 1 {
		t.Fatalf("generation repair calls = reconcile %d, check %d; want 1, 1", fake.reconcileCount(), fake.checkCount())
	}
}

func TestCoordinatorSymlinkMergeCoversFirstGenerationAndBackendChange(t *testing.T) {
	for _, tc := range []struct {
		name       string
		seedLedger bool
	}{
		{name: "absent applied generation"},
		{name: "fuse to symlink backend change", seedLedger: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, account, private := newSymlinkMergeCoordinator(t)
			if tc.seedLedger {
				applied := desiredApplied(t, c, account)
				applied.Backend = string(fkoverlay.BackendNFS)
				if err := s.m.Store.SetOverlayApplied(applied); err != nil {
					t.Fatal(err)
				}
			}
			var appBuild appBuildResult
			if err := c.applyAccount(t.Context(), account, false, &appBuild); err != nil {
				t.Fatal(err)
			}
			payload, err := os.ReadFile(private) //nolint:gosec // private is a test-owned temp path.
			if err != nil {
				t.Fatal(err)
			}
			var merged map[string]any
			if err := json.Unmarshal(payload, &merged); err != nil {
				t.Fatal(err)
			}
			if merged["theme"] != "light" {
				t.Fatalf("canonical merge did not run: %v", merged)
			}
			got, present, err := s.m.Store.GetOverlayApplied(account.ID)
			if err != nil || !present || !sameAppliedGeneration(got, desiredApplied(t, c, account)) {
				t.Fatalf("applied generation = (%+v, %v, %v)", got, present, err)
			}
		})
	}
}

func TestCoordinatorSymlinkMergeFailureDoesNotStampApplied(t *testing.T) {
	s, c, account, private := newSymlinkMergeCoordinator(t)
	if err := os.WriteFile(private, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	var appBuild appBuildResult
	if err := c.applyAccount(t.Context(), account, false, &appBuild); err == nil {
		t.Fatal("malformed private config unexpectedly applied")
	}
	if got, present, err := s.m.Store.GetOverlayApplied(account.ID); err != nil || present {
		t.Fatalf("failed merge stamped applied = (%+v, %v, %v)", got, present, err)
	}
}

func TestCoordinatorSelectionRepairsFileProviderBeforeNotify(t *testing.T) {
	s, _ := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendFileProvider)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFPProv{checkErr: errors.New("domain missing")}
	fake.reconcileFn = func() {
		fake.mu.Lock()
		fake.checkErr = nil
		fake.mu.Unlock()
	}
	s.m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	c.appBuild = func(context.Context) (string, error) { return "build-1", nil }
	if err := c.catchUp(t.Context(), account); err != nil {
		t.Fatal(err)
	}
	checks, reconciles, _, _ := fake.counts()
	if checks != 2 || reconciles != 1 || fake.notifyCount() != 1 {
		t.Fatalf("FP selection calls = check %d reconcile %d notify %d; want 2, 1, 1", checks, reconciles, fake.notifyCount())
	}
}

func TestCoordinatorFailedApplyKeepsPriorGeneration(t *testing.T) {
	s, dirs := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendNFS)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFuseProv{reconcileErr: errors.New("reconcile failed")}
	s.m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	applied := desiredApplied(t, c, account)
	applied.StructureStamp = "prior-structure"
	if err := s.m.Store.SetOverlayApplied(applied); err != nil {
		t.Fatal(err)
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	s.heartbeatFor().refresh(t.Context(), 0)
	var appBuild appBuildResult
	if err := c.applyAccount(t.Context(), account, false, &appBuild); err == nil {
		t.Fatal("failed reconcile unexpectedly succeeded")
	}
	got, present, err := s.m.Store.GetOverlayApplied(account.ID)
	if err != nil || !present || got.StructureStamp != "prior-structure" {
		t.Fatalf("failed apply changed ledger = (%+v, %v, %v)", got, present, err)
	}
}

func TestCoordinatorSelectionWaitUsesLatestDesiredGeneration(t *testing.T) {
	s, _ := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendNFS)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFuseProv{}
	s.m.OverlayFor = func(fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	initial := desiredApplied(t, c, account)
	if err := s.m.Store.SetOverlayApplied(initial); err != nil {
		t.Fatal(err)
	}
	if !s.cl.hold(account.ID) {
		t.Fatal("could not hold poll claim")
	}
	done := make(chan error, 1)
	go func() { done <- c.catchUp(t.Context(), account) }()
	time.Sleep(20 * time.Millisecond)
	c.mu.Lock()
	desired := c.desired
	desired.stamps.Canonical = "generation-2"
	c.desired = desired
	c.mu.Unlock()
	s.cl.disownHold(account.ID)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, present, err := s.m.Store.GetOverlayApplied(account.ID)
	if err != nil || !present || got.CanonicalStamp != "generation-2" {
		t.Fatalf("selection persisted stale generation = (%+v, %v, %v)", got, present, err)
	}
}

func TestCoordinatorStructureDriftReconcilesAndChecksDirectProvider(t *testing.T) {
	s, dirs := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendNFS)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFuseProv{}
	s.m.OverlayFor = func(_ fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	applied := desiredApplied(t, c, account)
	applied.StructureStamp = "old-structure"
	if err := s.m.Store.SetOverlayApplied(applied); err != nil {
		t.Fatal(err)
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}

	var appBuild appBuildResult
	if err := c.applyAccount(t.Context(), account, false, &appBuild); err != nil {
		t.Fatal(err)
	}
	if fake.reconcileCount() != 1 || fake.checkCount() != 1 {
		t.Fatalf("structure drift calls = reconcile %d, check %d; want 1, 1", fake.reconcileCount(), fake.checkCount())
	}
}

func TestCoordinatorAppDirtyNotifiesActiveFileProvider(t *testing.T) {
	s, dirs := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendFileProvider)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeFPProv{}
	s.m.OverlayFor = func(_ fkoverlay.Backend) (fkoverlay.Provider, error) { return fake, nil }
	c := newCoordinatorTestSource(t, s)
	c.appBuild = func(context.Context) (string, error) { return "build-2", nil }
	applied := desiredApplied(t, c, account)
	applied.AppStamp = "build-1"
	if err := s.m.Store.SetOverlayApplied(applied); err != nil {
		t.Fatal(err)
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}
	if delta := s.heartbeatFor().refresh(t.Context(), 0); !delta.success {
		t.Fatal("heartbeat failed")
	}

	c.converge(t.Context(), dirtyApp)
	if fake.notifyCount() != 1 {
		t.Fatalf("NotifyContent calls = %d, want 1", fake.notifyCount())
	}
	if _, reconciles, _, _ := fake.counts(); reconciles != 0 {
		t.Fatalf("app drift called Reconcile %d times; want NotifyContent only", reconciles)
	}
	got, present, err := s.m.Store.GetOverlayApplied(account.ID)
	if err != nil || !present || got.AppStamp != "build-2" {
		t.Fatalf("applied app stamp = (%+v, %v, %v), want build-2", got, present, err)
	}
}

func TestCoordinatorWatchesExactAppBuildAndBundleParent(t *testing.T) {
	s, _ := newTestServer(t)
	c := newCoordinatorTestSource(t, s)
	inputs := c.inputPaths()
	wantBuild := filepath.Join(pool.WidgetAppPath(), "Contents", "Info.plist")
	if inputs.AppBuild != wantBuild {
		t.Fatalf("AppBuild = %q, want %q", inputs.AppBuild, wantBuild)
	}
	if inputs.AppParent != filepath.Dir(pool.WidgetAppPath()) {
		t.Fatalf("AppParent = %q, want %q", inputs.AppParent, filepath.Dir(pool.WidgetAppPath()))
	}
	if inputs.Canonical == "" || inputs.CanonicalParent == "" || inputs.Settings == "" || inputs.ClaudeDir == "" {
		t.Fatalf("semantic watcher inputs contain an empty source path: %+v", inputs)
	}
}

func TestCoordinatorSemanticRefreshFailureSchedulesRetry(t *testing.T) {
	s, _ := newTestServer(t)
	c := newCoordinatorTestSource(t, s)
	t.Cleanup(c.stopRetry)
	if err := os.WriteFile(c.inputPaths().Canonical, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	c.converge(t.Context(), dirtyCanonical)
	c.retryMu.Lock()
	retryScheduled := c.retry != nil
	c.retryMu.Unlock()
	if !retryScheduled {
		t.Fatal("semantic refresh failure did not schedule a bounded retry")
	}
}

func TestCoordinatorListFailureSchedulesRetry(t *testing.T) {
	s, _ := newTestServer(t)
	c := newCoordinatorTestSource(t, s)
	t.Cleanup(c.stopRetry)
	if err := s.m.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.converge(t.Context(), dirtyStartup)
	c.retryMu.Lock()
	retryScheduled := c.retry != nil
	c.retryMu.Unlock()
	if !retryScheduled {
		t.Fatal("account list failure did not schedule a bounded retry")
	}
}

func TestCoordinatorCachesFailedAppBuildPerConverge(t *testing.T) {
	s, dirs := newTestServer(t)
	setRowKind(t, s, 1, fkoverlay.BackendFileProvider)
	setRowKind(t, s, 2, fkoverlay.BackendFileProvider)
	c := newCoordinatorTestSource(t, s)
	t.Cleanup(c.stopRetry)
	calls := 0
	c.appBuild = func(context.Context) (string, error) {
		calls++
		return "", errors.New("app unavailable")
	}
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{
			{PID: 4242, ConfigDir: dirs[1]},
			{PID: 4243, ConfigDir: dirs[2]},
		}, nil
	}
	s.heartbeatFor().refresh(t.Context(), 0)
	c.converge(t.Context(), dirtyStartup)
	if calls != 1 {
		t.Fatalf("failed app Check calls = %d for two accounts, want 1", calls)
	}
}

func TestCoordinatorRestartsWatcherAfterFailure(t *testing.T) {
	s, _ := newTestServer(t)
	c := newCoordinatorTestSource(t, s)
	c.watchRetryBase = 5 * time.Millisecond
	second := make(chan struct{})
	calls := 0
	c.watch = func(ctx context.Context, _ ccoverlay.SemanticInputPaths, _ func(dirtyCause)) error {
		calls++
		if calls == 1 {
			return errors.New("kqueue reconcile raced replacement")
		}
		close(second)
		<-ctx.Done()
		return nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	c.start(ctx)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("watcher was not restarted")
	}
	cancel()
	s.wg.Wait()
	if calls != 2 {
		t.Fatalf("watch calls = %d, want 2", calls)
	}
}

func TestCoordinatorRecordAppliedIgnoresRemovalRace(t *testing.T) {
	s, _ := newTestServer(t)
	c := newCoordinatorTestSource(t, s)
	account, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	applied := desiredApplied(t, c, account)
	if err := s.m.Store.DeleteAccount(account.ID); err != nil {
		t.Fatal(err)
	}
	if err := c.recordApplied(applied); err != nil {
		t.Fatalf("removed account race = %v, want success", err)
	}
}
