package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/creds/credstest"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
)

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

	reqCh := make(chan daemon.Request, 1)
	startDaemonTestServer(t, "", func(_ context.Context, _ daemon.Op, req daemon.Request) daemon.Response {
		reqCh <- req
		return daemon.Response{OK: true}
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cl := daemon.NewClient()
	defer func() { _ = cl.Close() }()
	abortDaemonSelection(ctx, cl, "reservation-token")
	select {
	case req := <-reqCh:
		if req.Op != daemon.OpSelectAbort || req.ReservationToken != "reservation-token" {
			t.Fatalf("abort request = %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abort request was not sent after caller cancellation")
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
	account, err := st.GetAccount(id)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	startDaemonTestServer(t, "", func(_ context.Context, op daemon.Op, _ daemon.Request) daemon.Response {
		resp := daemon.Response{OK: true, Version: version.String()}
		if op == daemon.OpSelect {
			resp.SelectedID = &id
			resp.Dir = dir
			resp.AccountInstanceID = account.InstanceID
			resp.AccountGeneration = account.Generation
		}
		return resp
	})

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
			selectResp := tc.resp
			startDaemonTestServer(t, "", func(_ context.Context, op daemon.Op, _ daemon.Request) daemon.Response {
				resp := daemon.Response{OK: true, Version: version.String()}
				if op == daemon.OpSelect {
					resp = selectResp
					resp.Version = version.String()
				}
				return resp
			})

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

func TestResolveSelectionRejectsDaemonBuildSkewWithoutLocalFallback(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(pool.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}

	startDaemonTestServer(t, "incompatible-build", nil)

	m := selectTestManager(t)
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	_, _, _, err = resolveSelection(cmd, m, selectReq{cwd: "/project"})
	if err == nil || !strings.Contains(err.Error(), "require exact daemon version") {
		t.Fatalf("resolveSelection with daemon build skew = %v", err)
	}
	if sessions, listErr := m.Store.ListActiveSessions(); listErr != nil || len(sessions) != 0 {
		t.Fatalf("build-skewed selection fell back locally: sessions=%+v err=%v", sessions, listErr)
	}
	if _, ok, stickyErr := m.Store.GetSticky("/project"); stickyErr != nil || ok {
		t.Fatalf("build-skewed selection wrote local sticky state: ok=%v err=%v", ok, stickyErr)
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
	a, _ = st.GetAccount(a.ID)
	b, _ = st.GetAccount(b.ID)
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
			name: "exact automatic match succeeds",
			resp: daemon.Response{
				SelectedID: &a.ID, Dir: a.ConfigDir, AccountInstanceID: a.InstanceID,
				AccountGeneration: a.Generation,
			},
			wantID: a.ID,
		},
		{
			name: "exact forced match succeeds",
			resp: daemon.Response{
				SelectedID: &a.ID, Dir: a.ConfigDir, AccountInstanceID: a.InstanceID,
				AccountGeneration: a.Generation,
			},
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
