package pool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	lockCrashPhaseEnv = "CCPOOL_LOCK_CRASH_PHASE"
	lockReadyPathEnv  = "CCPOOL_LOCK_READY_PATH"
)

func TestCredentialLockCrashHelper(t *testing.T) {
	phase := os.Getenv(lockCrashPhaseEnv)
	if phase == "" {
		t.Skip("credential lock crash helper")
	}
	configDir := AccountDir(1)
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	credentialLockFailpoint = func(checkpoint string) {
		if checkpoint != phase {
			return
		}
		if readyPath := os.Getenv(lockReadyPathEnv); readyPath != "" {
			if readyPath != filepath.Join(os.Getenv("HOME"), "lock-ready") {
				panic("credential lock ready path escaped the test home")
			}
			// #nosec G703 -- readyPath is exactly the fixed filename in the test HOME.
			if err := os.WriteFile(readyPath, []byte(checkpoint), 0o600); err != nil {
				panic(err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
		os.Exit(77)
	}
	lease, err := acquireCredentialRefreshLocks(t.Context(), 1, configDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("credential lock checkpoint %q was not reached", phase)
}

func TestCredentialLockRecoversEveryCrashTransition(t *testing.T) {
	checkpoints := []struct {
		name string
		kill bool
	}{
		{name: "journal-intended"},
		{name: "stage-created-0"},
		{name: "stage-prepared-0"},
		{name: "target-published-0"},
		{name: "target-acquired-0"},
		{name: "stage-created-1"},
		{name: "stage-prepared-1"},
		{name: "target-published-1"},
		{name: "target-acquired-1"},
		{name: "locks-acquired", kill: true},
		{name: "release-intended-1"},
		{name: "marker-removed-1"},
		{name: "target-removed-1"},
		{name: "target-released-1"},
		{name: "release-intended-0"},
		{name: "marker-removed-0"},
		{name: "target-removed-0"},
		{name: "target-released-0"},
		{name: "journal-removed"},
	}
	for _, checkpoint := range checkpoints {
		t.Run(checkpoint.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			// #nosec G204 -- executable is the current Go test binary returned by os.Executable.
			command := exec.Command(executable, "-test.run=^TestCredentialLockCrashHelper$")
			command.Env = append(os.Environ(), lockCrashPhaseEnv+"="+checkpoint.name)
			if checkpoint.kill {
				readyPath := filepath.Join(home, "lock-ready")
				command.Env = append(command.Env, lockReadyPathEnv+"="+readyPath)
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				waitForCredentialLockHelper(t, readyPath)
				if err := command.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := command.Wait(); err == nil {
					t.Fatal("credential lock helper survived SIGKILL")
				}
			} else {
				err := command.Run()
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != 77 {
					t.Fatalf("credential lock helper exit = %v", err)
				}
			}

			configDir := AccountDir(1)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			lease, err := acquireCredentialRefreshLocks(ctx, 1, configDir)
			if err != nil {
				t.Fatalf("recover after %s: %v", checkpoint.name, err)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatalf("release after %s: %v", checkpoint.name, err)
			}
			assertCredentialLockResidueGone(t, 1, configDir)
		})
	}
}

func TestCredentialLockRecoversAbandonedSameWorkerJournal(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	configDir := AccountDir(1)
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireCredentialRefreshLocks(t.Context(), 1, configDir)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := credentialRefreshLockPaths(configDir)
	if err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(paths[1], "injected-release-failure")
	if err := os.WriteFile(foreign, []byte("block rmdir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(t.Context()); err == nil {
		t.Fatal("release unexpectedly removed a non-empty exact lock")
	}
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	recovered, err := acquireCredentialRefreshLocks(ctx, 1, configDir)
	if err != nil {
		t.Fatalf("recover same-worker abandoned journal: %v", err)
	}
	if err := recovered.Release(ctx); err != nil {
		t.Fatal(err)
	}
	assertCredentialLockResidueGone(t, 1, configDir)
}

func TestCredentialLockReleaseOutlivesCallerCancellation(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	configDir := AccountDir(1)
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireCredentialRefreshLocks(t.Context(), 1, configDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	assertCredentialLockResidueGone(t, 1, configDir)
}

func TestCredentialLockNeverDeletesUnmarkedAmbiguousTarget(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	configDir := AccountDir(1)
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	paths, err := credentialRefreshLockPaths(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(paths[0], 0o700); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := credentialLockFingerprintForPath(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	worker, err := currentCredentialLockWorker()
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := newCredentialLockNonce()
	if err != nil {
		t.Fatal(err)
	}
	journal := credentialLockJournal{
		Schema: credentialLockJournalSchema, AccountID: 1, Nonce: nonce, Worker: worker,
		Targets: []credentialLockTarget{
			{
				Path: paths[0], Stage: credentialLockStagePath(paths[0], nonce, 0),
				Phase: credentialLockAcquired, Fingerprint: fingerprint,
			},
			{
				Path: paths[1], Stage: credentialLockStagePath(paths[1], nonce, 1),
				Phase: credentialLockIntended,
			},
		},
	}
	payload, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	journalPath := credentialLockJournalPath(1)
	if err := os.MkdirAll(filepath.Dir(journalPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journalPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := acquireCredentialRefreshLocks(ctx, 1, configDir); err == nil ||
		!strings.Contains(err.Error(), "owner marker") {
		t.Fatalf("ambiguous recovery error = %v", err)
	}
	if info, err := os.Lstat(paths[0]); err != nil || !info.IsDir() {
		t.Fatalf("ambiguous Claude lock was deleted: %+v, %v", info, err)
	}
}

func waitForCredentialLockHelper(t *testing.T, readyPath string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyPath); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("credential lock helper did not reach its kill checkpoint")
}

func assertCredentialLockResidueGone(t *testing.T, accountID int, configDir string) {
	t.Helper()
	paths, err := credentialRefreshLockPaths(configDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("credential lock remained at %s: %v", path, err)
		}
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".cc-pool-lock-v1-") {
				t.Fatalf("credential lock stage remained at %s", filepath.Join(filepath.Dir(path), entry.Name()))
			}
		}
	}
	if _, err := os.Lstat(credentialLockJournalPath(accountID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential lock journal remained: %v", err)
	}
}
