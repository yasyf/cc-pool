package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

type fakeLeaseCleanup struct {
	stops int
}

func (f *fakeLeaseCleanup) Stop() error {
	f.stops++
	return nil
}

func TestCommitSelectionWithLeaseStopsNewAgentOnCommitFailure(t *testing.T) {
	agent := &fakeLeaseCleanup{}
	swapVar(t, &spawnLeaseAgent, func(store.Account) (leaseAgentCleanup, error) { return agent, nil })
	want := errors.New("persist selection")
	selection := &selectionTxn{
		acct:   store.Account{ID: 1, Label: "work@example.com"},
		commit: func(context.Context) error { return want },
		abort:  func() {},
	}
	err := commitSelectionWithLease(context.Background(), selection)
	if !errors.Is(err, want) || agent.stops != 1 {
		t.Fatalf("err=%v stops=%d, want commit error and one stop", err, agent.stops)
	}
}

func TestAbortDaemonSelectionOutlivesCallerCancellation(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("unix", pool.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	reqCh := make(chan daemon.Request, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var req daemon.Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			return
		}
		reqCh <- req
		_ = json.NewEncoder(conn).Encode(daemon.Response{Proto: daemon.ProtocolVersion, OK: true})
	}()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	abortDaemonSelection(ctx, daemon.NewClient(), "reservation-token")
	select {
	case req := <-reqCh:
		if req.Op != daemon.OpSelectAbort || req.ReservationToken != "reservation-token" {
			t.Fatalf("abort request = %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abort request was not sent after caller cancellation")
	}
}

type contextBlockingOverlay struct {
	started chan struct{}
}

func (f *contextBlockingOverlay) Backend() fkoverlay.Backend                  { return fkoverlay.BackendSymlink }
func (f *contextBlockingOverlay) PrivateRoot(dir string) string               { return dir }
func (f *contextBlockingOverlay) Check(context.Context, string, string) error { return nil }
func (f *contextBlockingOverlay) Teardown(context.Context, string, string) (string, error) {
	return "", nil
}

func (f *contextBlockingOverlay) Reconcile(ctx context.Context, _, _ string) error {
	close(f.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestResolveSelectionBoundsOverlayReconcileByLaunchContext(t *testing.T) {
	st := openTestStore(t)
	id := 1
	a := store.Account{ID: id, ConfigDir: filepath.Join(t.TempDir(), "acct-01"), OverlayKind: string(fkoverlay.BackendSymlink)}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	provider := &contextBlockingOverlay{started: make(chan struct{})}
	m := &pool.Manager{Store: st, OverlayFor: func(fkoverlay.Backend) (fkoverlay.Provider, error) { return provider, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())

	_, err := resolveSelectionTxn(ctx, cmd, m, selectReq{account: &id, noDaemon: true})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("resolveSelectionTxn err = %v, want launch deadline", err)
	}
	select {
	case <-provider.started:
	default:
		t.Fatal("context-aware overlay sync was not called")
	}
}

type countingOverlay struct {
	reconciles int
}

func (f *countingOverlay) Backend() fkoverlay.Backend    { return fkoverlay.BackendSymlink }
func (f *countingOverlay) PrivateRoot(dir string) string { return dir }
func (f *countingOverlay) Reconcile(context.Context, string, string) error {
	f.reconciles++
	return nil
}
func (f *countingOverlay) Check(context.Context, string, string) error { return nil }
func (f *countingOverlay) Teardown(context.Context, string, string) (string, error) {
	return "", nil
}

func TestPrepareDaemonSelectionSkipsLocalReconcile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	st := openTestStore(t)
	a := store.Account{
		ID: 1, ConfigDir: filepath.Join(t.TempDir(), "acct-01"), OverlayKind: string(fkoverlay.BackendSymlink),
		KeychainService: "missing", KeychainAccount: "missing",
	}
	if err := os.MkdirAll(a.ConfigDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{"theme":"shared"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	privatePath := filepath.Join(a.ConfigDir, ".claude.json")
	private := []byte(`{"oauthAccount":{"accountUuid":"private"}}`)
	if err := os.WriteFile(privatePath, private, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertAccount(a); err != nil {
		t.Fatal(err)
	}
	provider := &countingOverlay{}
	m := &pool.Manager{
		Store: st, Creds: credstest.NewFake(), LockDir: t.TempDir(),
		OverlayFor: func(fkoverlay.Backend) (fkoverlay.Provider, error) { return provider, nil },
	}
	cmd := &cobra.Command{}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	dir, err := prepareAccount(t.Context(), cmd, m, a, false)
	if err != nil {
		t.Fatalf("prepareAccount(daemon-prepared) = %v", err)
	}
	if dir != a.ConfigDir {
		t.Fatalf("dir = %q, want %q", dir, a.ConfigDir)
	}
	if provider.reconciles != 0 {
		t.Fatalf("local Reconcile calls = %d, want 0 after daemon catch-up", provider.reconciles)
	}
	got, err := os.ReadFile(privatePath) //nolint:gosec // G304: test-owned path under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, private) {
		t.Fatalf("daemon-prepared launch rewrote private config: got %s, want %s", got, private)
	}
}

func selectTestManager(t *testing.T) *pool.Manager {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.UpsertAccount(store.Account{
		ID: 5, ConfigDir: filepath.Join(t.TempDir(), "acct"), Label: "work@example.com",
		KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
	}); err != nil {
		t.Fatal(err)
	}
	return &pool.Manager{Store: st}
}

func TestSelectionLine(t *testing.T) {
	m := selectTestManager(t)
	id := 5
	missing := 999

	cases := map[string]struct {
		resp daemon.Response
		want string
	}{
		"named, no usage":         {daemon.Response{SelectedID: &id}, "Selected work@example.com"},
		"named sticky, no usage":  {daemon.Response{SelectedID: &id, Sticky: true}, "Reusing work@example.com (pinned)"},
		"nil id degrades":         {daemon.Response{}, "Selected account"},
		"unknown id degrades":     {daemon.Response{SelectedID: &missing}, "Selected account"},
		"named with usage":        {daemon.Response{SelectedID: &id, HasUsage: true, Remaining5h: 96, Remaining7d: 27}, "Selected work@example.com · 5h 4% used · 7d 73% used"},
		"named sticky with usage": {daemon.Response{SelectedID: &id, Sticky: true, HasUsage: true, Remaining5h: 96, Remaining7d: 27}, "Reusing work@example.com (pinned) · 5h 4% used · 7d 73% used"},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			if got := stripANSI(daemonSelectionLine(m, &tc.resp)); got != tc.want {
				t.Errorf("daemonSelectionLine = %q, want %q", got, tc.want)
			}
		})
	}
}

// NoneAvailable must win over a co-set Error so --wait reaches the wait loop.
func TestDaemonSelectOutcome(t *testing.T) {
	cases := map[string]struct {
		resp daemon.Response
		wait bool
		want selectOutcome
	}{
		"picked":                        {daemon.Response{OK: true, Dir: "/d"}, false, outcomePicked},
		"picked ignores wait":           {daemon.Response{OK: true, Dir: "/d"}, true, outcomePicked},
		"fallback pick accepted":        {daemon.Response{OK: true, Dir: "/d", ExhaustedFallback: true}, false, outcomePicked},
		"fallback pick waits with wait": {daemon.Response{OK: true, Dir: "/d", ExhaustedFallback: true}, true, outcomeWait},
		"none available, no wait":       {daemon.Response{NoneAvailable: true, Error: "no account is currently available"}, false, outcomeFail},
		"none available, wait":          {daemon.Response{NoneAvailable: true, Error: "no account is currently available"}, true, outcomeWait},
		"real error":                    {daemon.Response{Error: "boom"}, false, outcomeError},
		"real error not masked by wait": {daemon.Response{Error: "boom"}, true, outcomeError},
		"empty reply, wait":             {daemon.Response{}, true, outcomeWait},
		"empty reply, no wait":          {daemon.Response{}, false, outcomeFail},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			if got := daemonSelectOutcome(&tc.resp, tc.wait); got != tc.want {
				t.Errorf("daemonSelectOutcome = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWarnExhaustedFallback(t *testing.T) {
	run := func(extraEnabled bool) string {
		var stderr bytes.Buffer
		cmd := &cobra.Command{}
		cmd.SetErr(&stderr)
		warnExhaustedFallback(cmd, "work@example.com", extraEnabled, time.Now().Add(20*time.Minute))
		return stripANSI(stderr.String())
	}
	if got := run(true); !strings.Contains(got, "WILL bill extra-usage credits") || !strings.Contains(got, "resets at") {
		t.Errorf("overage warning wrong: %q", got)
	}
	if got := run(false); !strings.Contains(got, "rate-limited until") {
		t.Errorf("rate-limit warning wrong: %q", got)
	}
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	warnExhaustedFallback(cmd, "x", true, time.Time{})
	if got := stripANSI(stderr.String()); strings.Contains(got, "resets at") {
		t.Errorf("unknown reset must omit the reset clause: %q", got)
	}
}

// exhaustedPoolManager builds an all-exhausted pool where acct-2 is the
// least-bad fallback (emptier 7d, overage enabled).
func exhaustedPoolManager(t *testing.T) *pool.Manager {
	t.Helper()
	t.Setenv("HOME", t.TempDir()) // ReconcileOverlay resolves ~/.claude from HOME
	t.Setenv("USER", "user")
	st := openTestStore(t)
	now := time.Now()
	for id, util7 := range map[int]float64{1: 90, 2: 10} {
		if err := st.UpsertAccount(store.Account{
			ID: id, ConfigDir: filepath.Join(t.TempDir(), "acct"), Label: "work@example.com",
			KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test", OverlayKind: "symlink",
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Util7d: util7,
			Resets5h: now.Add(20 * time.Minute), ExtraEnabled: id == 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// An empty seam fake makes any preflight refresh a harmless needs-login miss.
	return &pool.Manager{Store: st, Creds: credstest.NewFake(), LockDir: t.TempDir()}
}

// Pins the billing warning at its call site: removing warnExhaustedFallback fails this.
func TestResolveSelectionWarnsOnExhaustedFallback(t *testing.T) {
	m := exhaustedPoolManager(t)
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	cmd.SetContext(context.Background())

	_, dir, _, err := resolveSelection(cmd, m, selectReq{noDaemon: true, cwd: "/proj"})
	if err != nil || dir == "" {
		t.Fatalf("fallback selection must succeed: dir=%q err=%v", dir, err)
	}
	out := stripANSI(stderr.String())
	if !strings.Contains(out, "WILL bill extra-usage credits") {
		t.Fatalf("billing warning missing from stderr: %q", out)
	}
	if !strings.Contains(out, "resets at") {
		t.Fatalf("warning must name the recovery time: %q", out)
	}
}

// The bypass notice is loud only when a held pin was actually bypassed.
func TestWarnPinHeld(t *testing.T) {
	m := exhaustedPoolManager(t)
	held, selected := 2, 1
	cases := map[string]struct {
		held, selected *int
		want           string // "" = stderr must stay clean
	}{
		"no held pin":          {nil, &selected, ""},
		"pick is the held pin": {&held, &held, ""},
		"bypassed":             {&held, &selected, "manual pin to"},
		"bypassed, nil pick":   {&held, nil, "manual pin to"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var stderr bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetErr(&stderr)
			warnPinHeld(cmd, m, tc.held, tc.selected)
			out := stripANSI(stderr.String())
			if tc.want == "" {
				if out != "" {
					t.Fatalf("expected silence, got %q", out)
				}
				return
			}
			if !strings.Contains(out, tc.want) || !strings.Contains(out, "pin kept") {
				t.Fatalf("notice malformed: %q", out)
			}
		})
	}
}

// Pins the bypass notice at its call site: removing warnPinHeld fails this.
func TestResolveSelectionWarnsOnHeldManualPin(t *testing.T) {
	m := exhaustedPoolManager(t)
	now := time.Now()
	// Heal acct-1 so selection bypasses the pin on still-pegged acct-2.
	if err := m.Store.InsertUsageSample(store.UsageSample{
		AccountID: 1, TS: now.Add(time.Second), Util5h: 10, Util7d: 10,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Store.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	cmd.SetContext(context.Background())
	_, dir, _, err := resolveSelection(cmd, m, selectReq{noDaemon: true, cwd: "/proj"})
	if err != nil || dir == "" {
		t.Fatalf("selection must succeed: dir=%q err=%v", dir, err)
	}
	out := stripANSI(stderr.String())
	if !strings.Contains(out, "manual pin to") || !strings.Contains(out, "pin kept") {
		t.Fatalf("bypass notice missing from stderr: %q", out)
	}
	st, ok, _ := m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 2 || !st.Manual {
		t.Fatalf("manual pin lost on bypass: %+v ok=%v", st, ok)
	}

	// Negative: a pin on the least-bad fallback pick is honored in effect — no notice.
	m2 := exhaustedPoolManager(t)
	if err := m2.Store.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	var stderr2 bytes.Buffer
	cmd2 := &cobra.Command{}
	cmd2.SetErr(&stderr2)
	cmd2.SetContext(context.Background())
	if _, _, _, err := resolveSelection(cmd2, m2, selectReq{noDaemon: true, cwd: "/proj"}); err != nil {
		t.Fatal(err)
	}
	if out := stripANSI(stderr2.String()); strings.Contains(out, "manual pin to") {
		t.Fatalf("honored-in-effect pin must not warn: %q", out)
	}
}

// --wait over an all-exhausted pool waits instead of accepting the fallback pick.
func TestResolveSelectionWaitRefusesExhaustedFallback(t *testing.T) {
	m := exhaustedPoolManager(t)
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled: selection must stop before scoring
	cmd.SetContext(ctx)

	_, _, _, err := resolveSelection(cmd, m, selectReq{noDaemon: true, wait: true, cwd: "/proj"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait must block until cancelled, got %v", err)
	}
	out := stripANSI(stderr.String())
	if strings.Contains(out, "WILL bill") {
		t.Fatalf("--wait must never accept (or warn about) a billing fallback: %q", out)
	}
}

// TestWarnPreflight pins prepareAccount's non-fatal warn copy: needs-login and
// unrefreshable get their operator guidance (naming `ccp login <id>`), any
// other error passes through verbatim.
func TestWarnPreflight(t *testing.T) {
	a := store.Account{
		ID: 7, ConfigDir: t.TempDir(), Label: "work@example.com",
		KeychainService: "svc-warn-preflight", KeychainAccount: "user",
	}
	m := &pool.Manager{Creds: credstest.NewFake(), LockDir: t.TempDir()}
	_, _, absentErr := m.EnsureFreshToken(context.Background(), a, pool.RefreshLeadTime, true)
	if !errors.Is(absentErr, pool.ErrNeedsLogin) || !errors.Is(absentErr, creds.ErrNotFound) {
		t.Fatalf("absent credential error = %v, want ErrNeedsLogin and creds.ErrNotFound", absentErr)
	}
	opaque := errors.New("preflight refresh: dial tcp: connection refused")
	cases := map[string]struct {
		err  error
		want []string
	}{
		"needs-login names the login command": {
			err:  pool.ErrNeedsLogin,
			want: []string{"needs to log in again", "ccp login 7"},
		},
		"absent credential names the login command": {
			err:  absentErr,
			want: []string{"needs to log in again", "ccp login 7"},
		},
		"unrefreshable names the origin and the local login": {
			err:  pool.ErrUnrefreshable,
			want: []string{"synced copy it can't refresh", "the origin rotates it", "ccp login 7"},
		},
		"other errors pass through verbatim": {
			err:  opaque,
			want: []string{opaque.Error()},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var stderr bytes.Buffer
			warnPreflight(&stderr, a, tc.err)
			out := stripANSI(stderr.String())
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("warn = %q, want it to contain %q", out, want)
				}
			}
			if errors.Is(tc.err, pool.ErrUnrefreshable) && strings.Contains(out, "needs to log in again") {
				t.Errorf("unrefreshable must not use the needs-login copy: %q", out)
			}
		})
	}
}

// Pins the settings merge on both no-daemon arms: removing mergeLaunchSettings
// in prepareAccount fails both.
func TestResolveSelectionMergesBaseSettings(t *testing.T) {
	cases := map[string]func(id int) selectReq{
		"forced arm": func(id int) selectReq { return selectReq{noDaemon: true, account: &id, cwd: "/proj"} },
		"live arm":   func(int) selectReq { return selectReq{noDaemon: true, cwd: "/proj"} },
	}
	for name, mkReq := range cases {
		t.Run(name, func(t *testing.T) {
			m := exhaustedPoolManager(t) // sets HOME; accounts are symlink-kind
			marker := []byte(`{"mergeMarker": "yes"}`)
			//nolint:gosec // G703: HOME is the test's own t.TempDir(), not external input
			if err := os.WriteFile(filepath.Join(os.Getenv("HOME"), ".claude.json"), marker, 0o600); err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetErr(&stderr)
			cmd.SetContext(context.Background())

			_, dir, _, err := resolveSelection(cmd, m, mkReq(1))
			if err != nil || dir == "" {
				t.Fatalf("selection must succeed: dir=%q err=%v", dir, err)
			}
			b, err := os.ReadFile(filepath.Join(dir, ".claude.json")) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
			if err != nil {
				t.Fatalf("account .claude.json missing after launch merge: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("merged file unparseable: %v", err)
			}
			if got["mergeMarker"] != "yes" {
				t.Fatalf("base marker did not reach the account file: %v", got)
			}
		})
	}
}

// The daemon's content coordinator applies the semantic generation before it
// returns a pick; the client must not repeat that work after every selection.
func TestResolveSelectionDaemonPickDoesNotRepeatBaseMerge(t *testing.T) {
	// Short HOME under /tmp: macOS caps sun_path at 104 bytes; t.TempDir's /var/folders path exceeds it.
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{"mergeMarker": "yes"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	st := openTestStore(t)
	id := 1
	dir := filepath.Join(home, "acct-01")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	privatePath := filepath.Join(dir, ".claude.json")
	private := []byte(`{"oauthAccount":{"accountUuid":"private"}}`)
	if err := os.WriteFile(privatePath, private, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertAccount(store.Account{
		ID: id, ConfigDir: dir, Label: "work@example.com",
		KeychainService: "svc", KeychainAccount: "u", OverlayKind: "symlink",
	}); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", pool.SocketPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req daemon.Request
			_ = json.NewDecoder(conn).Decode(&req)
			resp := daemon.Response{Proto: daemon.ProtocolVersion, OK: true, Version: version.String()}
			if req.Op == daemon.OpSelect {
				resp.SelectedID = &id
				resp.Dir = dir
				resp.ReservationToken = "test-reservation"
			}
			_ = json.NewEncoder(conn).Encode(resp)
			_ = conn.Close()
		}
	}()

	m := &pool.Manager{Store: st}
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	cmd.SetContext(context.Background())

	_, gotDir, _, err := resolveSelection(cmd, m, selectReq{cwd: "/proj"})
	if err != nil || gotDir != dir {
		t.Fatalf("daemon pick must succeed: dir=%q err=%v (stderr=%q)", gotDir, err, stripANSI(stderr.String()))
	}
	got, err := os.ReadFile(privatePath) //nolint:gosec // G304: test-owned path under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, private) {
		t.Fatalf("daemon pick repeated the base merge: got %s, want %s", got, private)
	}
}

// A mount-layer problem must never surface as "exhausted or rate-limited".
func TestResolveSelectionMountsNotReadyError(t *testing.T) {
	cases := map[string]struct {
		resp    daemon.Response
		wantErr error
		notErr  error
	}{
		"mounts not ready": {
			daemon.Response{OK: false, NoneAvailable: true, MountsNotReady: true, Error: pool.ErrMountsNotReady.Error()},
			pool.ErrMountsNotReady, pool.ErrNoneAvailable,
		},
		"plain none available": {
			daemon.Response{OK: false, NoneAvailable: true, Error: pool.ErrNoneAvailable.Error()},
			pool.ErrNoneAvailable, pool.ErrMountsNotReady,
		},
	}
	for name, tc := range cases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			home, err := os.MkdirTemp("/tmp", "ccp-home")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)

			st := openTestStore(t)
			if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("unix", pool.SocketPath())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ln.Close() })
			selectResp := tc.resp
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					var req daemon.Request
					_ = json.NewDecoder(conn).Decode(&req)
					resp := daemon.Response{Proto: daemon.ProtocolVersion, OK: true, Version: version.String()}
					if req.Op == daemon.OpSelect {
						resp = selectResp
						resp.Proto = daemon.ProtocolVersion
						resp.Version = version.String()
					}
					_ = json.NewEncoder(conn).Encode(resp)
					_ = conn.Close()
				}
			}()

			m := &pool.Manager{Store: st}
			cmd := &cobra.Command{}
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetContext(context.Background())

			_, _, _, err = resolveSelection(cmd, m, selectReq{cwd: "/proj"})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("resolveSelection err = %v, want errors.Is %v", err, tc.wantErr)
			}
			if errors.Is(err, tc.notErr) {
				t.Fatalf("err must not match the other sentinel %v; err = %v", tc.notErr, err)
			}
		})
	}
}

func TestValidateDaemonSelection(t *testing.T) {
	st := openTestStore(t)
	a := store.Account{
		ID: 5, ConfigDir: filepath.Join(t.TempDir(), "acct-05"), Label: "work@example.com",
		KeychainService: "svc-5", KeychainAccount: "u-5", OverlayKind: "symlink",
	}
	b := store.Account{
		ID: 6, ConfigDir: filepath.Join(t.TempDir(), "acct-06"), Label: "other@example.com",
		KeychainService: "svc-6", KeychainAccount: "u-6", OverlayKind: "symlink",
	}
	for _, acct := range []store.Account{a, b} {
		if err := st.UpsertAccount(acct); err != nil {
			t.Fatal(err)
		}
	}
	m := &pool.Manager{Store: st}
	zero, unknown := 0, 999
	cases := []struct {
		name      string
		resp      daemon.Response
		forced    *store.Account
		wantError []string
		wantID    int
	}{
		{
			name:      "nil selected id",
			resp:      daemon.Response{Dir: "/returned/nil"},
			wantError: []string{"id <nil>", "expected dir \"<unknown>\"", "returned dir \"/returned/nil\""},
		},
		{
			name:      "empty selected id",
			resp:      daemon.Response{SelectedID: &zero, Dir: "/returned/empty"},
			wantError: []string{"id 0", "expected dir \"<unknown>\"", "returned dir \"/returned/empty\""},
		},
		{
			name:      "unknown selected id",
			resp:      daemon.Response{SelectedID: &unknown, Dir: "/returned/unknown"},
			wantError: []string{"id 999", "expected dir \"<unknown>\"", "returned dir \"/returned/unknown\""},
		},
		{
			name:      "wrong dir",
			resp:      daemon.Response{SelectedID: &a.ID, Dir: "/wrong/dir"},
			wantError: []string{"id 5", "expected dir \"" + a.ConfigDir + "\"", "returned dir \"/wrong/dir\""},
		},
		{
			name:      "trailing slash alias fails",
			resp:      daemon.Response{SelectedID: &a.ID, Dir: a.ConfigDir + "/"},
			wantError: []string{"id 5", "expected dir \"" + a.ConfigDir + "\"", "returned dir \"" + a.ConfigDir + "/\""},
		},
		{
			name:      "dot segment alias fails",
			resp:      daemon.Response{SelectedID: &a.ID, Dir: a.ConfigDir + "/./"},
			wantError: []string{"id 5", "expected dir \"" + a.ConfigDir + "\"", "returned dir \"" + a.ConfigDir + "/./\""},
		},
		{
			name:      "forced account mismatch",
			resp:      daemon.Response{SelectedID: &b.ID, Dir: b.ConfigDir},
			forced:    &a,
			wantError: []string{"id 6", "forced account 5", "expected dir \"" + a.ConfigDir + "\"", "returned dir \"" + b.ConfigDir + "\""},
		},
		{
			name:   "exact automatic match succeeds",
			resp:   daemon.Response{SelectedID: &a.ID, Dir: a.ConfigDir},
			wantID: a.ID,
		},
		{
			name:   "exact forced match succeeds",
			resp:   daemon.Response{SelectedID: &a.ID, Dir: a.ConfigDir},
			forced: &a,
			wantID: a.ID,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateDaemonSelection(m, &tc.resp, tc.forced)
			if len(tc.wantError) > 0 {
				if err == nil {
					t.Fatalf("validateDaemonSelection = %+v, nil; want error", got)
				}
				for _, want := range tc.wantError {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("validateDaemonSelection: %v", err)
			}
			if got.ID != tc.wantID || got.ConfigDir != a.ConfigDir {
				t.Errorf("validated account = %+v, want id=%d dir=%q", got, tc.wantID, a.ConfigDir)
			}
		})
	}
}

// announceLine stays silent on non-TTY stdout (as under $(ccp select)).
func TestAnnounceLineSilentWhenNotTTY(t *testing.T) {
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)

	announceLine(cmd, "Selected work@example.com")

	if stderr.Len() != 0 {
		t.Errorf("expected no stderr output when stdout is not a TTY, got %q", stderr.String())
	}
}
