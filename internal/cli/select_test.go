package cli

import (
	"bytes"
	"context"
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
	admitCLITestAccount(t, st, store.Account{
		ID: 5, ConfigDir: filepath.Join(t.TempDir(), "acct"), Label: "work@example.com",
		KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
	})
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

func TestMetadataSelectionOutput(t *testing.T) {
	if got := metadataSelectionOutput(7); got != "acct-07\tprepared=false" {
		t.Fatalf("metadata selection output = %q", got)
	}
}

// NoneAvailable must win over a co-set Error so --wait reaches the wait loop.
func TestDaemonSelectOutcome(t *testing.T) {
	id := 1
	cases := map[string]struct {
		resp daemon.Response
		wait bool
		want selectOutcome
	}{
		"metadata pick":                 {daemon.Response{OK: true, SelectedID: &id}, false, outcomePicked},
		"prepared pick ignores wait":    {daemon.Response{OK: true, SelectedID: &id, Prepared: true, Dir: "/d"}, true, outcomePicked},
		"fallback pick accepted":        {daemon.Response{OK: true, SelectedID: &id, ExhaustedFallback: true}, false, outcomePicked},
		"fallback pick waits with wait": {daemon.Response{OK: true, SelectedID: &id, ExhaustedFallback: true}, true, outcomeWait},
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
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USER", "user")
	st := openTestStore(t)
	now := time.Now()
	for _, fixture := range []struct {
		id    int
		util7 float64
	}{{id: 1, util7: 90}, {id: 2, util7: 10}} {
		id, util7 := fixture.id, fixture.util7
		admitCLITestAccount(t, st, store.Account{
			ID: id, ConfigDir: filepath.Join(t.TempDir(), "acct"), Label: "work@example.com",
			KeychainService: "ccp-test-missing", KeychainAccount: "ccp-test",
		})
		if err := st.InsertUsageSample(store.UsageSample{
			AccountID: id, TS: now, Util5h: 100, Util7d: util7,
			Resets5h: now.Add(20 * time.Minute), ExtraEnabled: id == 2,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// An empty seam fake makes any preflight refresh a harmless needs-login miss.
	return &pool.Manager{Store: st, Creds: credstest.NewFake()}
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
	admitCLITestAccount(t, st, store.Account{
		ID: id, ConfigDir: dir, Label: "work@example.com",
		KeychainService: "svc", KeychainAccount: "u",
	})
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
	if err != nil || gotDir != "" {
		t.Fatalf("metadata-only daemon pick must have no path: dir=%q err=%v (stderr=%q)", gotDir, err, stripANSI(stderr.String()))
	}
	got, err := os.ReadFile(privatePath) //nolint:gosec // G304: test-owned path under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, private) {
		t.Fatalf("daemon pick repeated the base merge: got %s, want %s", got, private)
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
	if err == nil || !strings.Contains(err.Error(), "daemon runtime build is not exact") {
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
	t.Setenv("HOME", t.TempDir())
	st := openTestStore(t)
	a := store.Account{
		ID: 5, ConfigDir: filepath.Join(t.TempDir(), "acct-05"), Label: "work@example.com",
		KeychainService: "svc-5", KeychainAccount: "u-5",
	}
	b := store.Account{
		ID: 6, ConfigDir: filepath.Join(t.TempDir(), "acct-06"), Label: "other@example.com",
		KeychainService: "svc-6", KeychainAccount: "u-6",
	}
	for _, acct := range []store.Account{a, b} {
		admitCLITestAccount(t, st, acct)
	}
	a, _ = st.GetAccount(a.ID)
	b, _ = st.GetAccount(b.ID)
	presentationA := testFileProviderConfigDir(a.ID)
	presentationB := testFileProviderConfigDir(b.ID)
	m := &pool.Manager{Store: st}
	zero, unknown := 0, 999
	cases := []struct {
		name      string
		resp      daemon.Response
		forced    *store.Account
		launch    bool
		wantError []string
		wantID    int
	}{
		{
			name:      "nil selected id",
			resp:      daemon.Response{Dir: "/returned/nil"},
			wantError: []string{"id <nil>", "returned dir \"/returned/nil\""},
		},
		{
			name:      "empty selected id",
			resp:      daemon.Response{SelectedID: &zero, Dir: "/returned/empty"},
			wantError: []string{"id 0", "returned dir \"/returned/empty\""},
		},
		{
			name:      "unknown selected id",
			resp:      daemon.Response{SelectedID: &unknown, Dir: "/returned/unknown"},
			wantError: []string{"id 999", "returned dir \"/returned/unknown\""},
		},
		{
			name:      "relative public path fails",
			resp:      daemon.Response{SelectedID: &a.ID, Prepared: true, Dir: "relative/path"},
			launch:    true,
			wantError: []string{"id 5", "invalid File Provider path", "relative/path"},
		},
		{
			name:      "trailing slash alias fails",
			resp:      daemon.Response{SelectedID: &a.ID, Prepared: true, Dir: presentationA + "/"},
			launch:    true,
			wantError: []string{"id 5", "invalid File Provider path", presentationA + "/"},
		},
		{
			name:      "dot segment alias fails",
			resp:      daemon.Response{SelectedID: &a.ID, Prepared: true, Dir: presentationA + "/./"},
			launch:    true,
			wantError: []string{"id 5", "invalid File Provider path", presentationA + "/./"},
		},
		{
			name:      "forced account mismatch",
			resp:      daemon.Response{SelectedID: &b.ID, Prepared: true, Dir: presentationB},
			forced:    &a,
			launch:    true,
			wantError: []string{"id 6", "forced account 5", "returned dir \"" + presentationB + "\""},
		},
		{
			name: "OS public path succeeds without static-path equality",
			resp: daemon.Response{
				SelectedID: &a.ID, Prepared: true, Dir: "/Users/test/Library/CloudStorage/account-5", AccountInstanceID: a.InstanceID,
				AccountGeneration: a.Generation,
			},
			launch: true,
			wantID: a.ID,
		},
		{
			name: "exact forced match succeeds",
			resp: daemon.Response{
				SelectedID: &a.ID, Prepared: true, Dir: presentationA, AccountInstanceID: a.InstanceID,
				AccountGeneration: a.Generation,
			},
			forced: &a,
			launch: true,
			wantID: a.ID,
		},
		{
			name: "metadata-only succeeds without a path",
			resp: daemon.Response{
				SelectedID: &a.ID, AccountInstanceID: a.InstanceID, AccountGeneration: a.Generation,
			},
			wantID: a.ID,
		},
		{
			name: "metadata-only rejects a runnable path",
			resp: daemon.Response{
				SelectedID: &a.ID, Dir: presentationA, AccountInstanceID: a.InstanceID, AccountGeneration: a.Generation,
			},
			wantError: []string{"inspection", "runnable File Provider path"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateDaemonSelection(m, &tc.resp, tc.forced, tc.launch)
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
