package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/fileproviderd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

// fakeFPProv is an in-package fake of the File Provider overlay provider at the
// Provider seam (the daemon drives providers, never the control wire; fusekit's
// overlay/fileprovider_fake_test.go fakes the wire for its own provider tests).
type fakeFPProv struct {
	mu        sync.Mutex
	healths   int
	setups    int
	syncs     int
	teardowns int
	healthErr error
	setupErr  error
	syncErr   error
}

func (f *fakeFPProv) Backend() fkoverlay.Backend { return fkoverlay.BackendFileProvider }

func (f *fakeFPProv) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }

func (f *fakeFPProv) Health(_, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healths++
	return f.healthErr
}

func (f *fakeFPProv) Setup(_, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setups++
	return f.setupErr
}

func (f *fakeFPProv) Sync(_, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return f.syncErr
}

func (f *fakeFPProv) Teardown(_, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.teardowns++
	return nil
}

func (f *fakeFPProv) counts() (healths, setups, syncs, teardowns int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healths, f.setups, f.syncs, f.teardowns
}

// newFPServer builds a test server whose acct-1 is a fileprovider row served by
// an injected fakeFPProv; symlink resolution stays real so the ErrCannotControl
// retreat can run the genuine conversion.
func newFPServer(t *testing.T) (*Server, map[int]string, *fakeFPProv) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeFPProv{}
	s.m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
		switch backend {
		case fkoverlay.BackendFileProvider:
			return fake, nil
		case fkoverlay.BackendSymlink:
			return &fkoverlay.SymlinkProvider{Spec: s.m.OverlaySpec()}, nil
		default:
			return nil, fmt.Errorf("unexpected backend %q", backend)
		}
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	a.OverlayKind = string(fkoverlay.BackendFileProvider)
	if err := s.m.Store.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	return s, dirs, fake
}

func TestFPBackedRow(t *testing.T) {
	cases := map[string]bool{
		"fileprovider": true,
		"symlink":      false,
		"nfs":          false,
		"fskit":        false,
		"":             false,
		"garbage":      false,
	}
	for kind, want := range cases {
		if got := fpBackedRow(kind); got != want {
			t.Errorf("fpBackedRow(%q) = %v, want %v", kind, got, want)
		}
	}
}

// shortHome pins HOME to a short /tmp dir so the ~/.cc-pool bridge sockets stay
// under the 104-byte sun_path limit (t.TempDir paths blow it).
func shortHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-fp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	return home
}

// TestFPBridgeSharesSourceAndSerializesWrites pins the two-servers-one-Source
// architecture: startContentBridge and startFPBridge serve the SAME
// PoolContentSource, so concurrent claude.json write-throughs arriving on both
// sockets serialize under the package-level write-through mutex — every
// writer's shareable key survives into base (a lost update would drop one) —
// and a second bind on the live FP socket is refused by proc.SingleEntrant.
func TestFPBridgeSharesSourceAndSerializesWrites(t *testing.T) {
	shortHome(t)
	if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pool.ClaudeJSONPath(), []byte(`{"seed":"0"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	s := &Server{
		log:           log.New(io.Discard, "", 0),
		contentSource: overlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath()),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startContentBridge(ctx)
	s.startFPBridge(ctx)
	holderCl := content.NewBridgeClient(pool.BridgeSocketPath())
	fpCl := content.NewBridgeClient(pool.FPBridgeSocketPath())
	if !holderCl.Available() || !fpCl.Available() {
		t.Fatalf("bridges not up: holder=%v fp=%v", holderCl.Available(), fpCl.Available())
	}

	const writers = 10
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := holderCl
			if i%2 == 1 {
				cl = fpCl
			}
			payload := fmt.Appendf(nil, `{"k%02d":"v%02d"}`, i, i)
			if err := cl.Write(ctx, "acct-01", ".claude.json", payload); err != nil {
				errs <- fmt.Errorf("writer %d: %w", i, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	raw, err := os.ReadFile(pool.ClaudeJSONPath())
	if err != nil {
		t.Fatal(err)
	}
	var base map[string]string
	if err := json.Unmarshal(raw, &base); err != nil {
		t.Fatalf("base claude.json corrupted by concurrent write-throughs: %v\n%s", err, raw)
	}
	if base["seed"] != "0" {
		t.Fatalf("seed key lost: %s", raw)
	}
	for i := range writers {
		k, v := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)
		if base[k] != v {
			t.Errorf("write-through %s lost (want %q, got %q): unserialized read-modify-write across the two bridges\n%s", k, v, base[k], raw)
		}
	}

	// A second server on the live FP socket must be refused, not silently bound
	// over the live peer.
	dup := &content.BridgeServer{Socket: pool.FPBridgeSocketPath(), Source: s.contentSource, Version: "test", Log: log.New(io.Discard, "", 0)}
	if err := dup.Run(ctx); err == nil || !strings.Contains(err.Error(), "already serves") {
		t.Fatalf("duplicate FP bridge Run = %v, want a refusal naming the live peer", err)
	}

	cancel()
	s.wg.Wait()
}

// TestStartFPBridgeBindsUnconditionally pins the dropped container gate: the FP
// socket now lives in cc-pool's own state dir, so startFPBridge creates the dir
// and binds without any group-container fixture, exactly as the holder bridge
// binds bridge.sock.
func TestStartFPBridgeBindsUnconditionally(t *testing.T) {
	shortHome(t)
	s := &Server{
		log:           log.New(io.Discard, "", 0),
		contentSource: overlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath()),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.startFPBridge(ctx)

	if !content.NewBridgeClient(pool.FPBridgeSocketPath()).Available() {
		t.Fatalf("FP bridge not up after startFPBridge with no group container")
	}
	cancel()
	s.wg.Wait()
}

// syncBuffer is a concurrency-safe log sink: the retry goroutine writes it while
// the test polls it, so reads and writes must not race.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitFor polls cond up to timeout, failing the test if it never holds.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStartFPBridgeRetriesAfterBindFailure pins the self-heal: a live peer
// holding the socket makes the first BridgeServer.Run refuse (SingleEntrant sees
// it and returns "already serves"), so serveFPBridge logs the failure and
// retries; once the peer releases the socket, the retry binds within the backoff
// and the FP bridge comes up without a daemon restart.
func TestStartFPBridgeRetriesAfterBindFailure(t *testing.T) {
	shortHome(t)
	sock := pool.FPBridgeSocketPath()

	// A second BridgeServer stands in for a stale peer holding the socket: it
	// accepts (so Evict's dial connects) and holds the flock, exactly as a live
	// daemon would, so the first bind is deterministically refused.
	blockerCtx, stopBlocker := context.WithCancel(context.Background())
	defer stopBlocker()
	blocker := &content.BridgeServer{
		Socket:  sock,
		Source:  overlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath()),
		Version: "blocker",
		Log:     log.New(io.Discard, "", 0),
	}
	blockerDone := make(chan struct{})
	go func() { defer close(blockerDone); _ = blocker.Run(blockerCtx) }()
	waitFor(t, 2*time.Second, "blocker to bind", content.NewBridgeClient(sock).Available)

	buf := &syncBuffer{}
	s := &Server{
		log:             log.New(buf, "", 0),
		contentSource:   overlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath()),
		fpBridgeBackoff: 25 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.startFPBridge(ctx) // first Run refused by the live peer; the loop retries

	// Confirm the first bind actually failed and the loop is retrying BEFORE
	// releasing the socket, else the retry path is never exercised.
	waitFor(t, 2*time.Second, "the initial bind failure to be logged", func() bool {
		return strings.Contains(buf.String(), "retrying")
	})

	// Release the socket; only the daemon's retry can bind it now (the peer is
	// gone), so a live socket proves the retry re-bound within the backoff.
	stopBlocker()
	<-blockerDone
	waitFor(t, 2*time.Second, "the retry to rebind the socket", content.NewBridgeClient(sock).Available)

	cancel()
	s.wg.Wait()
}

// TestReconcileFileProviderErrorDispatch pins the FP error dispatch:
// ErrCannotControl is the ONLY permanent retreat; every transient control
// sentinel leaves the row on fileprovider for the next cycle, converting
// nothing.
func TestReconcileFileProviderErrorDispatch(t *testing.T) {
	cases := []struct {
		name       string
		healthErr  error
		setupErr   error
		want       fpOutcome
		wantKind   string
		wantSetups int
	}{
		{
			name: "healthy row needs no setup", want: fpHealthy,
			wantKind: "fileprovider", wantSetups: 0,
		},
		{
			name: "health failure repaired by idempotent setup", healthErr: errors.New("bridge symlink drifted"),
			want: fpRepaired, wantKind: "fileprovider", wantSetups: 1,
		},
		{
			name: "cannot-control retreats to symlink", healthErr: errors.New("no domain"),
			setupErr: fmt.Errorf("file provider setup: %w", fileproviderd.ErrCannotControl),
			want:     fpRetreated, wantKind: "symlink", wantSetups: 1,
		},
		{
			name: "app unavailable retries", healthErr: errors.New("app down"),
			setupErr: fmt.Errorf("file provider setup: %w", fileproviderd.ErrAppUnavailable),
			want:     fpRetry, wantKind: "fileprovider", wantSetups: 1,
		},
		{
			name: "register failed retries", healthErr: errors.New("no domain"),
			setupErr: fmt.Errorf("file provider setup: %w", fileproviderd.ErrRegisterFailed),
			want:     fpRetry, wantKind: "fileprovider", wantSetups: 1,
		},
		{
			name: "busy retries", healthErr: errors.New("no domain"),
			setupErr: fmt.Errorf("file provider setup: %w", fileproviderd.ErrBusy),
			want:     fpRetry, wantKind: "fileprovider", wantSetups: 1,
		},
		{
			name: "no-domain retries", healthErr: errors.New("no domain"),
			setupErr: fmt.Errorf("file provider setup: %w", fileproviderd.ErrNoDomain),
			want:     fpRetry, wantKind: "fileprovider", wantSetups: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, dirs, fake := newFPServer(t)
			fake.healthErr, fake.setupErr = tc.healthErr, tc.setupErr
			a, err := s.m.Store.GetAccount(1)
			if err != nil {
				t.Fatal(err)
			}

			got := s.reconcileFileProvider(t.Context(), a)

			if got != tc.want {
				t.Fatalf("outcome = %d, want %d", got, tc.want)
			}
			if kind := kindOf(t, s, 1); kind != tc.wantKind {
				t.Fatalf("overlay kind = %q, want %q", kind, tc.wantKind)
			}
			healths, setups, _, teardowns := fake.counts()
			if healths != 1 {
				t.Fatalf("healths = %d, want exactly 1", healths)
			}
			if setups != tc.wantSetups {
				t.Fatalf("setups = %d, want %d", setups, tc.wantSetups)
			}
			switch tc.want {
			case fpRetreated:
				// The retreat ran the real conversion: symlinks laid, claim released.
				if _, err := os.Lstat(filepath.Join(dirs[1], "plans")); err != nil {
					t.Fatalf("retreat did not lay the symlink overlay: %v", err)
				}
				if s.isConverting(1) {
					t.Fatal("retreat leaked its converting claim")
				}
			default:
				if teardowns != 0 {
					t.Fatalf("teardowns = %d on a non-retreat outcome, want 0", teardowns)
				}
				if !s.tryReserve(1) {
					t.Fatal("account not reservable after a non-retreat outcome")
				}
			}
		})
	}
}

// TestReconcileFileProviderRetreatGates pins that even the permanent
// ErrCannotControl verdict never converts under a live session or a pending
// select reservation — it defers instead, leaving the row on fileprovider.
func TestReconcileFileProviderRetreatGates(t *testing.T) {
	cannotControl := fmt.Errorf("file provider setup: %w", fileproviderd.ErrCannotControl)

	t.Run("live session defers", func(t *testing.T) {
		s, dirs, fake := newFPServer(t)
		fake.healthErr, fake.setupErr = errors.New("no domain"), cannotControl
		s.scanSessions = func(context.Context) ([]procscan.Session, error) {
			return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
		}
		a, err := s.m.Store.GetAccount(1)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.reconcileFileProvider(t.Context(), a); got != fpDeferred {
			t.Fatalf("outcome = %d, want fpDeferred", got)
		}
		if kind := kindOf(t, s, 1); kind != "fileprovider" {
			t.Fatalf("row converted under a live session: kind = %q", kind)
		}
		if s.isConverting(1) {
			t.Fatal("deferred retreat leaked its converting claim")
		}
	})

	t.Run("pending select reservation defers", func(t *testing.T) {
		s, _, fake := newFPServer(t)
		fake.healthErr, fake.setupErr = errors.New("no domain"), cannotControl
		if !s.tryReserve(1) {
			t.Fatal("could not reserve acct-1")
		}
		a, err := s.m.Store.GetAccount(1)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.reconcileFileProvider(t.Context(), a); got != fpDeferred {
			t.Fatalf("outcome = %d, want fpDeferred", got)
		}
		if kind := kindOf(t, s, 1); kind != "fileprovider" {
			t.Fatalf("row converted under a reservation: kind = %q", kind)
		}
	})
}

// TestReconcileAccountRoutesFileProviderRow pins the reconcile routing split:
// an FP row goes to reconcileFileProvider (its private store stays put — the
// symlink arm's HealStrandedPrivate would drag it through the domain bridge
// symlink), while a symlink row still heals stranded private files.
func TestReconcileAccountRoutesFileProviderRow(t *testing.T) {
	s, dirs, fake := newFPServer(t)

	priv1 := fkoverlay.FusePrivateRoot(dirs[1])
	if err := os.MkdirAll(priv1, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv1, ".claude.json"), []byte("fp-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	a1, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	s.reconcileAccount(t.Context(), a1)
	if healths, _, _, _ := fake.counts(); healths != 1 {
		t.Fatalf("FP row not routed to reconcileFileProvider: healths = %d", healths)
	}
	if _, err := os.Stat(filepath.Join(priv1, ".claude.json")); err != nil {
		t.Fatalf("FP private store drained by the symlink heal arm: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(dirs[1], ".claude.json")); !os.IsNotExist(err) {
		t.Fatalf("private file surfaced in the FP account dir: %v", err)
	}

	// Contrast: the same stranded shape under a symlink row IS healed.
	priv2 := fkoverlay.FusePrivateRoot(dirs[2])
	if err := os.MkdirAll(priv2, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv2, ".claude.json"), []byte("stranded"), 0o600); err != nil {
		t.Fatal(err)
	}
	a2, err := s.m.Store.GetAccount(2)
	if err != nil {
		t.Fatal(err)
	}
	s.reconcileAccount(t.Context(), a2)
	got, err := os.ReadFile(filepath.Join(dirs[2], ".claude.json"))
	if err != nil || string(got) != "stranded" {
		t.Fatalf("symlink row's stranded identity not healed: %q err=%v", got, err)
	}
}

// newFPPollServer builds a minimal poll server around one fileprovider account
// whose credential is seeded (adoption succeeds) unless seedCred is false.
func newFPPollServer(t *testing.T, seedCred bool) (*Server, store.Account, *fakeFPProv) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct"),
		OverlayKind:     string(fkoverlay.BackendFileProvider),
		KeychainService: "svc", KeychainAccount: "user",
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	fk := credstest.NewFake()
	if seedCred {
		cred := &creds.Credential{}
		cred.ClaudeAiOauth.AccessToken = "at-0"
		cred.ClaudeAiOauth.RefreshToken = "rt-0"
		cred.ClaudeAiOauth.ExpiresAt = time.Now().Add(time.Hour).UnixMilli()
		fk.Put(a.KeychainService, a.KeychainAccount, cred)
	}
	fake := &fakeFPProv{}
	s := &Server{
		m:               &pool.Manager{Store: st, OAuth: &fakeOAuth{currentRT: "rt-0"}, Creds: fk, LockDir: t.TempDir()},
		snapshot:        filepath.Join(t.TempDir(), "status.json"),
		log:             log.New(io.Discard, "", 0),
		scanSessions:    func(context.Context) ([]procscan.Session, error) { return nil, nil },
		reservations:    map[int]time.Time{},
		converting:      map[int]bool{},
		rlStreak:        map[int]int{},
		authStreak:      map[int]int{},
		lastAuthAttempt: map[int]time.Time{},
	}
	s.m.OverlayFor = func(backend fkoverlay.Backend) (fkoverlay.Provider, error) {
		if backend != fkoverlay.BackendFileProvider {
			return nil, fmt.Errorf("unexpected backend %q", backend)
		}
		return fake, nil
	}
	return s, a, fake
}

// pollOnceAccount drives one pollAccount iteration under its claim, the way
// pollOnce does.
func pollOnceAccount(t *testing.T, s *Server, a store.Account) {
	t.Helper()
	if !s.beginPoll(a.ID) {
		t.Fatalf("acct-%02d poll claim refused", a.ID)
	}
	s.pollAccount(t.Context(), nil, 0, a)
}

// TestPollAccountFileProviderSyncAfterAdoption pins the post-adoption nudge: an
// idle FP row whose rotated token was adopted gets a second provider Sync so
// the domain re-enumerates the rotated .claude.json; a reserved (busy) row and
// a failed adoption get only the routine overlay sync.
func TestPollAccountFileProviderSyncAfterAdoption(t *testing.T) {
	t.Run("idle adoption syncs the domain", func(t *testing.T) {
		s, a, fake := newFPPollServer(t, true)
		pollOnceAccount(t, s, a)
		if _, _, syncs, _ := fake.counts(); syncs != 2 {
			t.Fatalf("syncs = %d, want 2 (overlay sync + post-adoption)", syncs)
		}
	})

	t.Run("reserved account skips adoption and the extra sync", func(t *testing.T) {
		s, a, fake := newFPPollServer(t, true)
		if !s.tryReserve(a.ID) {
			t.Fatal("could not reserve")
		}
		pollOnceAccount(t, s, a)
		if _, _, syncs, _ := fake.counts(); syncs != 1 {
			t.Fatalf("syncs = %d, want 1 (no adoption on a reserved account)", syncs)
		}
	})

	t.Run("failed adoption skips the extra sync", func(t *testing.T) {
		s, a, fake := newFPPollServer(t, false) // no credential: adoption fails
		pollOnceAccount(t, s, a)
		if _, _, syncs, _ := fake.counts(); syncs != 1 {
			t.Fatalf("syncs = %d, want 1 (nothing adopted, nothing to re-enumerate)", syncs)
		}
	})
}

// TestPollAccountSyncFailureRoutesFPToReconcile pins the poll escalation
// branch: an FP row's overlay-sync failure goes to reconcileFileProvider (never
// healFuse), and a healthy domain means no Setup and no row change.
func TestPollAccountSyncFailureRoutesFPToReconcile(t *testing.T) {
	s, a, fake := newFPPollServer(t, false)
	fake.syncErr = errors.New("bridge symlink drifted")

	pollOnceAccount(t, s, a)

	healths, setups, _, _ := fake.counts()
	if healths != 1 {
		t.Fatalf("healths = %d, want 1 (sync failure must reconcile the domain)", healths)
	}
	if setups != 0 {
		t.Fatalf("setups = %d, want 0 (healthy domain needs no re-register)", setups)
	}
	if kind := kindOf(t, s, a.ID); kind != "fileprovider" {
		t.Fatalf("row changed to %q on a transient sync failure", kind)
	}
}
