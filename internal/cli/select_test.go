package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/daemon"
	"github.com/yasyf/cc-pool/internal/keychain"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/cc-pool/internal/version"
)

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

// TestSelectionLine pins the wording of the shared selection diagnostic and its
// degraded fallbacks. The daemon's SelectedID resolves to the account label; a
// sticky pick is labelled "Reusing … (pinned)"; an unknown/absent id degrades to
// a generic "account"; and a sampled pick (HasUsage) gets its raw 5h/7d usage
// appended (100−remaining), while an unsampled one stays usage-free (no
// fabricated 0%). ANSI is stripped so the assertions hold regardless of TTY.
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

// TestDaemonSelectOutcome pins the daemon-reply dispatch, in particular that a
// none-available reply with --wait reaches the wait loop: the daemon sets BOTH
// NoneAvailable and Error, and the old Error-first match made --wait dead code.
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

// TestWarnExhaustedFallback pins the billing warning wording: credits when
// overage is enabled, rate-limit otherwise, reset time when known.
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

// exhaustedPoolManager builds a Manager over a temp store whose two accounts
// are both 5h-pegged with pending resets and fresh samples — the all-exhausted
// state — with acct-2 the least-bad (emptier 7d, overage enabled). The fake
// keychain is empty, so any preflight refresh is a harmless needs-login miss.
func exhaustedPoolManager(t *testing.T) *pool.Manager {
	t.Helper()
	t.Setenv("HOME", t.TempDir()) // SyncOverlay resolves ~/.claude from HOME
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
	return &pool.Manager{Store: st, Keychain: emptyKeychain{}, LockDir: t.TempDir()}
}

// emptyKeychain satisfies pool.CredentialStore with no items, turning the
// preflight refresh into the needs-login warning path.
type emptyKeychain struct{}

func (emptyKeychain) Read(string, string) (*keychain.Credential, error) {
	return nil, keychain.ErrNotFound
}
func (emptyKeychain) Write(string, string, *keychain.Credential) error { return nil }
func (emptyKeychain) Delete(string, string) error                      { return nil }
func (emptyKeychain) Discover(string) (string, error)                  { return "", keychain.ErrNotFound }

// TestResolveSelectionWarnsOnExhaustedFallback pins the warning at the
// integration point: a live all-exhausted selection must emit the billing
// warning on stderr and still hand back the least-bad dir. Deleting the
// warnExhaustedFallback call site fails this test.
func TestResolveSelectionWarnsOnExhaustedFallback(t *testing.T) {
	m := exhaustedPoolManager(t)
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	cmd.SetContext(context.Background())

	dir, _, err := resolveSelection(cmd, m, selectReq{noDaemon: true, cwd: "/proj"})
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

// TestWarnPinHeld pins the bypass notice's gating: silent without a held pin
// and when the pick IS the held account (the pin was honored in effect), loud
// — with the pin-kept reassurance — when an explicit pin was bypassed.
func TestWarnPinHeld(t *testing.T) {
	m := exhaustedPoolManager(t) // any manager with account rows for names
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

// TestResolveSelectionWarnsOnHeldManualPin pins the notice at the integration
// point: a live selection over a dir manually pinned to an exhausted account
// must pick the healthy one AND say the pin was bypassed. Deleting the
// warnPinHeld call site fails this test.
func TestResolveSelectionWarnsOnHeldManualPin(t *testing.T) {
	m := exhaustedPoolManager(t)
	now := time.Now()
	// Heal acct-1 so the pool is no longer all-exhausted; pin to acct-2, which
	// stays pegged (unusable for stickiness).
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
	dir, _, err := resolveSelection(cmd, m, selectReq{noDaemon: true, cwd: "/proj"})
	if err != nil || dir == "" {
		t.Fatalf("selection must succeed: dir=%q err=%v", dir, err)
	}
	out := stripANSI(stderr.String())
	if !strings.Contains(out, "manual pin to") || !strings.Contains(out, "pin kept") {
		t.Fatalf("bypass notice missing from stderr: %q", out)
	}
	// The pin must survive the bypass untouched.
	st, ok, _ := m.Store.GetSticky("/proj")
	if !ok || st.AccountID != 2 || !st.Manual {
		t.Fatalf("manual pin lost on bypass: %+v ok=%v", st, ok)
	}

	// Negative: when the fallback pick IS the held account (all exhausted,
	// pin on the least-bad), the pin was honored in effect — no notice.
	m2 := exhaustedPoolManager(t)
	if err := m2.Store.PinManual("/proj", 2, now); err != nil {
		t.Fatal(err)
	}
	var stderr2 bytes.Buffer
	cmd2 := &cobra.Command{}
	cmd2.SetErr(&stderr2)
	cmd2.SetContext(context.Background())
	if _, _, err := resolveSelection(cmd2, m2, selectReq{noDaemon: true, cwd: "/proj"}); err != nil {
		t.Fatal(err)
	}
	if out := stripANSI(stderr2.String()); strings.Contains(out, "manual pin to") {
		t.Fatalf("honored-in-effect pin must not warn: %q", out)
	}
}

// TestResolveSelectionWaitRefusesExhaustedFallback pins --wait's contract:
// over an all-exhausted pool it must wait (here: until the context cancels),
// never hand back the exhausted pick that a non-wait call accepts.
func TestResolveSelectionWaitRefusesExhaustedFallback(t *testing.T) {
	m := exhaustedPoolManager(t)
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled: the wait loop must exit on its first sleep
	cmd.SetContext(ctx)

	_, _, err := resolveSelection(cmd, m, selectReq{noDaemon: true, wait: true, cwd: "/proj"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait must block until cancelled, got %v", err)
	}
	out := stripANSI(stderr.String())
	if !strings.Contains(out, "soonest reset at") {
		t.Fatalf("wait message missing from stderr: %q", out)
	}
	if strings.Contains(out, "WILL bill") {
		t.Fatalf("--wait must never accept (or warn about) a billing fallback: %q", out)
	}
}

// TestResolveSelectionMergesBaseSettings pins the launch-time settings merge at
// its integration points: both the forced arm and the live no-daemon arm of
// resolveSelection must land a shareable ~/.claude.json key in the selected
// account's private .claude.json. Deleting the mergeLaunchSettings call in
// prepareAccount fails both arms.
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

			dir, _, err := resolveSelection(cmd, m, mkReq(1))
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

// TestResolveSelectionDaemonPickMergesBaseSettings pins the daemon fast path's
// launch-settings hook at its integration point: an outcomePicked reply must
// trigger the shared-settings merge for the picked account before the dir is
// handed back — the merge runs client-side, after the daemon replies. The
// daemon is faked on pool.SocketPath() under an isolated HOME (the
// status_test.go fixture pattern): EnsureRunning's ping is satisfied by the
// live listener, daemonAt by an OK health reply at version.String(). Deleting
// the mergeDaemonPick call in resolveSelection fails this test.
func TestResolveSelectionDaemonPickMergesBaseSettings(t *testing.T) {
	// Short HOME under /tmp: macOS caps sun_path at 104 bytes, and t.TempDir's
	// /var/folders path plus the test name exceeds it.
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

	gotDir, _, err := resolveSelection(cmd, m, selectReq{cwd: "/proj"})
	if err != nil || gotDir != dir {
		t.Fatalf("daemon pick must succeed: dir=%q err=%v (stderr=%q)", gotDir, err, stripANSI(stderr.String()))
	}
	var got map[string]any
	if err := json.Unmarshal(readSelectTestFile(t, filepath.Join(dir, ".claude.json")), &got); err != nil || got["mergeMarker"] != "yes" {
		t.Fatalf("daemon-pick merge did not land the base marker (err=%v): %v", err, got)
	}
}

// TestResolveSelectionMountsNotReadyError pins the honest-error split at the
// daemon fast path: a none-available reply tagged MountsNotReady (every account
// has capacity but no mirror is mounted/healthy — the holder is mid-replacement)
// must surface ErrMountsNotReady, while a plain none-available reply keeps
// ErrNoneAvailable. The two sentinels are distinct so the user is never told
// "exhausted or rate-limited" for a mount-layer problem.
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
			// Short HOME under /tmp: macOS caps the unix socket path at 104 bytes.
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

			_, _, err = resolveSelection(cmd, m, selectReq{cwd: "/proj"})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("resolveSelection err = %v, want errors.Is %v", err, tc.wantErr)
			}
			if errors.Is(err, tc.notErr) {
				t.Fatalf("err must not match the other sentinel %v; err = %v", tc.notErr, err)
			}
		})
	}
}

// TestMergeDaemonPick pins the daemon-pick merge hook's degradation contract:
// a nil or unknown SelectedID warns and skips (a daemon hiccup must not block
// the launch), while a valid id merges the base's shareable settings.
func TestMergeDaemonPick(t *testing.T) {
	known, unknown := 5, 999
	cases := map[string]struct {
		id       *int
		wantWarn bool
		wantFile bool
	}{
		"nil id warns and skips":     {nil, true, false},
		"unknown id warns and skips": {&unknown, true, false},
		"valid id merges":            {&known, false, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(`{"mergeMarker": "yes"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			st := openTestStore(t)
			dir := filepath.Join(home, "acct-05")
			if err := st.UpsertAccount(store.Account{
				ID: known, ConfigDir: dir, Label: "work@example.com",
				KeychainService: "svc", KeychainAccount: "u", OverlayKind: "symlink",
			}); err != nil {
				t.Fatal(err)
			}
			m := &pool.Manager{Store: st}
			var stderr bytes.Buffer
			cmd := &cobra.Command{}
			cmd.SetErr(&stderr)

			mergeDaemonPick(cmd, m, tc.id)

			out := stripANSI(stderr.String())
			if tc.wantWarn && !strings.Contains(out, "shared-settings merge") {
				t.Fatalf("expected a skip warning, got %q", out)
			}
			if !tc.wantWarn && out != "" {
				t.Fatalf("expected silence, got %q", out)
			}
			_, err := os.Stat(filepath.Join(dir, ".claude.json"))
			if tc.wantFile {
				if err != nil {
					t.Fatalf("merge did not write the account file: %v", err)
				}
				var got map[string]any
				if err := json.Unmarshal(readSelectTestFile(t, filepath.Join(dir, ".claude.json")), &got); err != nil || got["mergeMarker"] != "yes" {
					t.Fatalf("marker missing from merged file (err=%v): %v", err, got)
				}
				return
			}
			if !os.IsNotExist(err) {
				t.Fatalf("no account file should be written on a skipped merge (err=%v)", err)
			}
		})
	}
}

// readSelectTestFile reads a file or fails the test.
func readSelectTestFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is a cc-pool-managed/test-owned file, not external input
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAnnounceLineSilentWhenNotTTY is the core of the noise fix: when stdout is
// not an interactive terminal (as in tests, and under $(ccp select)), the success
// line is suppressed entirely — only the dir reaches stdout elsewhere.
func TestAnnounceLineSilentWhenNotTTY(t *testing.T) {
	var stderr bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetErr(&stderr)

	announceLine(cmd, "Selected work@example.com")

	if stderr.Len() != 0 {
		t.Errorf("expected no stderr output when stdout is not a TTY, got %q", stderr.String())
	}
}
