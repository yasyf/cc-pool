package daemon

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// stubFPAppSeams swaps the three fp.app seams for the test's duration. A nil
// spawn defaults to a no-op success.
func stubFPAppSeams(t *testing.T, available func() bool, spawn func(context.Context) error, domains func() ([]int, error)) {
	t.Helper()
	if spawn == nil {
		spawn = func(context.Context) error { return nil }
	}
	oa, osp, od := fpAppAvailable, fpAppSpawn, fpCloudStorageDomains
	fpAppAvailable, fpAppSpawn, fpCloudStorageDomains = available, spawn, domains
	t.Cleanup(func() { fpAppAvailable, fpAppSpawn, fpCloudStorageDomains = oa, osp, od })
}

// runRow drives the named maintainer row exactly as runTable would — gate, then
// run — so a test exercises the real registered gate+run, not a hand-rolled call.
func runRow(t *testing.T, s *Server, table []maintainer, name string) {
	t.Helper()
	for _, m := range table {
		if m.name != name {
			continue
		}
		if m.gate == nil || m.gate(s) {
			m.run(s, t.Context(), s.newTick(t.Context()))
		}
		return
	}
	t.Fatalf("no row %q in table", name)
}

// markFPRow flips an account to the File Provider backend.
func markFPRow(t *testing.T, s *Server, id int) {
	t.Helper()
	if err := s.m.Store.SetAccountOverlayKind(id, string(fkoverlay.BackendFileProvider)); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureFPAppSpawnsWhenWanted(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, s *Server, spawns *atomic.Int32)
	}{
		{"fp row, app down", func(t *testing.T, s *Server, spawns *atomic.Int32) {
			markFPRow(t, s, 1)
			stubFPAppSeams(t, func() bool { return false },
				func(context.Context) error { spawns.Add(1); return nil },
				func() ([]int, error) { return nil, nil })
		}},
		{"artifact only, app down", func(t *testing.T, _ *Server, spawns *atomic.Int32) {
			// No FP rows; a lone orphan CloudStorage artifact still warrants the app
			// (the incident shape, and the reap needs a live app to confirm against).
			stubFPAppSeams(t, func() bool { return false },
				func(context.Context) error { spawns.Add(1); return nil },
				func() ([]int, error) { return []int{13}, nil })
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			s, _ := newTestServer(t)
			s.fpSynth = alwaysNonEmpty
			var spawns atomic.Int32
			tc.setup(t, s, &spawns)

			runRow(t, s, healTable, "fp.app.ensure")
			s.wg.Wait()
			if got := spawns.Load(); got != 1 {
				t.Fatalf("first ensure spawned %d times, want 1", got)
			}

			// A second ensure within the fp.app backoff does not spawn again.
			runRow(t, s, healTable, "fp.app.ensure")
			s.wg.Wait()
			if got := spawns.Load(); got != 1 {
				t.Fatalf("ensure within backoff spawned %d times, want 1 (backoff must fence the relaunch)", got)
			}
		})
	}
}

func TestEnsureFPAppNegatives(t *testing.T) {
	tests := []struct {
		name      string
		available func() bool
		domains   func() ([]int, error)
		prep      func(t *testing.T, s *Server)
	}{
		{
			name:      "no rows and no artifacts",
			available: func() bool { return false },
			domains:   func() ([]int, error) { return nil, nil },
			prep:      func(_ *testing.T, _ *Server) {},
		},
		{
			name:      "app already serving the socket",
			available: func() bool { return true },
			domains:   func() ([]int, error) { return nil, nil },
			prep:      func(t *testing.T, s *Server) { markFPRow(t, s, 1) },
		},
		{
			name:      "consent pending",
			available: func() bool { return false },
			domains:   func() ([]int, error) { return nil, nil },
			prep:      func(t *testing.T, s *Server) { markFPRow(t, s, 1); s.fpConsentPending.Store(true) },
		},
		{
			name:      "backoff not due",
			available: func() bool { return false },
			domains:   func() ([]int, error) { return nil, nil },
			prep:      func(t *testing.T, s *Server) { markFPRow(t, s, 1); s.bookFPAppEnsure(time.Now()) },
		},
		{
			name:      "file provider not wired",
			available: func() bool { return false },
			domains:   func() ([]int, error) { return []int{13}, nil },
			prep:      func(t *testing.T, s *Server) { s.fpSynth = nil; markFPRow(t, s, 1) },
		},
		{
			name:      "list rows error never guesses",
			available: func() bool { return false },
			domains:   func() ([]int, error) { return []int{13}, nil },
			prep:      func(_ *testing.T, s *Server) { _ = s.m.Store.Close() },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			s, _ := newTestServer(t)
			s.fpSynth = alwaysNonEmpty
			var spawns atomic.Int32
			stubFPAppSeams(t, tc.available,
				func(context.Context) error { spawns.Add(1); return nil },
				tc.domains)
			tc.prep(t, s)

			runRow(t, s, healTable, "fp.app.ensure")
			s.wg.Wait()
			if got := spawns.Load(); got != 0 {
				t.Fatalf("%s spawned %d times, want 0", tc.name, got)
			}
		})
	}
}

// TestEnsureFPAppNonBlocking pins that the heal tick never stalls on the ~30s
// spawn: with a spawn that parks, ensureFPAppAsync returns immediately and the
// tracked goroutine finishes once released.
func TestEnsureFPAppNonBlocking(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	s.fpSynth = alwaysNonEmpty
	markFPRow(t, s, 1)

	release := make(chan struct{})
	var spawned atomic.Bool
	stubFPAppSeams(t, func() bool { return false }, func(context.Context) error {
		spawned.Store(true)
		<-release
		return nil
	}, func() ([]int, error) { return nil, nil })

	done := make(chan struct{})
	go func() {
		s.ensureFPAppAsync(t.Context())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("ensureFPAppAsync blocked on the spawn; the heal tick must not stall")
	}
	close(release)
	s.wg.Wait()
}

// TestEnsureFPAppSpawnFailureLogged pins that a failed launch does not panic and
// the backoff still holds a second attempt off.
func TestEnsureFPAppSpawnFailureLogged(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, _ := newTestServer(t)
	s.fpSynth = alwaysNonEmpty
	markFPRow(t, s, 1)

	var spawns atomic.Int32
	stubFPAppSeams(t, func() bool { return false }, func(context.Context) error {
		spawns.Add(1)
		return errors.New("open -g failed")
	}, func() ([]int, error) { return nil, nil })

	runRow(t, s, healTable, "fp.app.ensure")
	s.wg.Wait()
	runRow(t, s, healTable, "fp.app.ensure")
	s.wg.Wait()
	if got := spawns.Load(); got != 1 {
		t.Fatalf("a failed launch retried within the backoff (%d spawns), want 1", got)
	}
}
