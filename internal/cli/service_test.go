package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/version"
)

// swapVar swaps a package-level seam for the test's lifetime.
func swapVar[T any](t *testing.T, target *T, val T) {
	t.Helper()
	old := *target
	*target = val
	t.Cleanup(func() { *target = old })
}

// tempHome isolates HOME under a short /tmp path (macOS caps sun_path at 104
// bytes; t.TempDir's /var/folders path overflows it once socket names append).
func tempHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "ccp-home")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	return home
}

// seedAccounts writes account rows into the pool db under the current HOME.
func seedAccounts(t *testing.T, accts ...store.Account) {
	t.Helper()
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(pool.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, a := range accts {
		if a.KeychainService == "" {
			a.KeychainService = "ccp-test-missing"
		}
		if a.KeychainAccount == "" {
			a.KeychainAccount = "ccp-test"
		}
		if err := st.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
	}
}

// stubStopDaemon replaces the daemon-stop seam, recording whether it ran. The
// real one drives launchctl/brew, which tests must never touch.
func stubStopDaemon(t *testing.T) *bool {
	t.Helper()
	called := false
	swapVar(t, &stopDaemon, func(_ *cobra.Command) error {
		called = true
		return nil
	})
	return &called
}

// uninstallCmd builds a throwaway command with captured stdout/stderr.
func uninstallCmd() (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	cmd := &cobra.Command{}
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	return cmd, &out, &errOut
}

// fakeHolder is a minimal mount-holder socket speaking the mountd JSON
// protocol: it records the ops it serves and answers from fixed fixtures.
type fakeHolder struct {
	t  *testing.T
	ln net.Listener

	mu  sync.Mutex
	ops []string

	version         string
	mounts          []mountd.MountInfo // list response
	shutdownFailed  []mountd.MountInfo // shutdown response (dirs that failed)
	closeOnShutdown bool               // release the socket after acking shutdown
	failHealth      bool               // answer health ops with OK:false
}

// startFakeHolder serves the mount-holder socket under the current HOME.
func startFakeHolder(t *testing.T, fh *fakeHolder) *fakeHolder {
	t.Helper()
	fh.t = t
	if err := pool.EnsureStateDir(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", pool.MountsSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	fh.ln = ln
	t.Cleanup(func() { _ = ln.Close() })
	go fh.serve()
	return fh
}

func (f *fakeHolder) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		var req mountd.Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			_ = conn.Close() // Available() probes dial-and-close; not an op
			continue
		}
		f.mu.Lock()
		f.ops = append(f.ops, string(req.Op))
		f.mu.Unlock()
		resp := mountd.Response{Proto: mountd.MountProtoVersion, OK: true, Version: f.version}
		switch req.Op {
		case mountd.OpHealth:
			if f.failHealth {
				resp.OK = false
				resp.Error = "health check failed"
			}
		case mountd.OpList:
			resp.Mounts = f.mounts
		case mountd.OpShutdown:
			resp.Mounts = f.shutdownFailed
		}
		_ = json.NewEncoder(conn).Encode(resp)
		_ = conn.Close()
		if req.Op == mountd.OpShutdown && f.closeOnShutdown {
			_ = f.ln.Close()
			return
		}
	}
}

func (f *fakeHolder) sawOp(op mountd.Op) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.ops {
		if o == string(op) {
			return true
		}
	}
	return false
}

// TestUninstallSessionGate pins the gate-before-destruction contract: live
// sessions on dirs the uninstall would destroy refuse it (fuse rows always;
// every row under --purge), the refusal names accounts and pids, --force
// bypasses the scan entirely, and a failed scan aborts rather than proceeding
// blind.
func TestUninstallSessionGate(t *testing.T) {
	type tc struct {
		purge, force bool
		sessions     []procscan.Session
		scanErr      error
		wantErr      []string // substrings; empty means the gate passes
		notInErr     []string
		wantStop     bool // the daemon-stop step was reached
	}
	home := func(t *testing.T) (fuseDir, symDir string) {
		h := tempHome(t)
		fuseDir = filepath.Join(h, ".cc-pool", "accounts", "acct-01")
		symDir = filepath.Join(h, ".cc-pool", "accounts", "acct-02")
		seedAccounts(t,
			store.Account{ID: 1, ConfigDir: fuseDir, OverlayKind: string(overlay.KindFuse)},
			store.Account{ID: 2, ConfigDir: symDir, OverlayKind: string(overlay.KindSymlink)},
		)
		return fuseDir, symDir
	}
	cases := map[string]struct {
		build func(fuseDir, symDir string) tc
	}{
		"fuse sessions block a plain uninstall, listing pids": {
			build: func(fuseDir, _ string) tc {
				return tc{
					sessions: []procscan.Session{{PID: 101, ConfigDir: fuseDir}, {PID: 102, ConfigDir: fuseDir}},
					wantErr:  []string{"acct-01 (pid 101, 102)", "close them or pass --force"},
					notInErr: []string{"acct-02"},
				}
			},
		},
		"symlink sessions do not block a plain uninstall": {
			build: func(_, symDir string) tc {
				return tc{
					sessions: []procscan.Session{{PID: 201, ConfigDir: symDir}},
					wantStop: true,
				}
			},
		},
		"purge blocks on sessions of any kind": {
			build: func(_, symDir string) tc {
				return tc{
					purge:    true,
					sessions: []procscan.Session{{PID: 201, ConfigDir: symDir}},
					wantErr:  []string{"acct-02 (pid 201)"},
				}
			},
		},
		// purge=false here even though the gate-per-kind question is about
		// --purge: a passing purge run would reach m.Remove and the Keychain,
		// which tests never touch. The empty-ConfigDir match is kind-agnostic.
		"plain-claude sessions (no config dir) never block": {
			build: func(_, _ string) tc {
				return tc{
					sessions: []procscan.Session{{PID: 300, ConfigDir: ""}},
					wantStop: true,
				}
			},
		},
		"force bypasses the gate with live fuse sessions": {
			build: func(fuseDir, _ string) tc {
				return tc{
					force:    true,
					sessions: []procscan.Session{{PID: 101, ConfigDir: fuseDir}},
					wantStop: true,
				}
			},
		},
		"a failed scan aborts": {
			build: func(_, _ string) tc {
				return tc{
					scanErr: errors.New("ps exploded"),
					wantErr: []string{"cannot verify no live sessions", "ps exploded", "--force"},
				}
			},
		},
		"force skips even a failing scan": {
			build: func(_, _ string) tc {
				return tc{
					force:    true,
					scanErr:  errors.New("ps exploded"),
					wantStop: true,
				}
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			fuseDir, symDir := home(t)
			tc := c.build(fuseDir, symDir)
			scanned := false
			swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) {
				scanned = true
				return tc.sessions, tc.scanErr
			})
			swapVar(t, &dirMounted, func(string) bool { return false })
			stopped := stubStopDaemon(t)
			// No fake holder: the socket is absent, so the holder leg is a
			// silent skip in every case here.
			cmd, _, _ := uninstallCmd()
			err := runServiceUninstall(cmd, tc.purge, tc.force)

			if len(tc.wantErr) > 0 {
				if err == nil {
					t.Fatal("uninstall proceeded; want gate refusal")
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q missing %q", err, want)
					}
				}
				for _, bad := range tc.notInErr {
					if strings.Contains(err.Error(), bad) {
						t.Errorf("error %q must not name %q", err, bad)
					}
				}
				if *stopped {
					t.Error("the daemon was stopped despite the gate refusing")
				}
				return
			}
			if err != nil {
				t.Fatalf("uninstall: %v", err)
			}
			if *stopped != tc.wantStop {
				t.Errorf("daemon stop reached = %v, want %v", *stopped, tc.wantStop)
			}
			if tc.force && scanned {
				t.Error("--force must skip the session scan entirely")
			}
		})
	}
}

// TestUninstallShutsDownHolder: a reachable holder gets the shutdown op, its
// failed dirs are reported, and the success line lands once the socket dies.
func TestUninstallShutsDownHolder(t *testing.T) {
	tempHome(t)
	swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) { return nil, nil })
	swapVar(t, &dirMounted, func(string) bool { return false })
	swapVar(t, &holderGoneWait, 2*time.Second)
	stubStopDaemon(t)
	fh := startFakeHolder(t, &fakeHolder{
		version:         version.String(),
		shutdownFailed:  []mountd.MountInfo{{Dir: "/tmp/stuck-dir"}},
		closeOnShutdown: true,
	})

	cmd, out, errOut := uninstallCmd()
	if err := runServiceUninstall(cmd, false, false); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if !fh.sawOp(mountd.OpShutdown) {
		t.Error("the holder never received a shutdown op")
	}
	if got := stripANSI(errOut.String()); !strings.Contains(got, "couldn't unmount /tmp/stuck-dir") {
		t.Errorf("failed dir not reported:\n%s", got)
	}
	if got := stripANSI(out.String()); !strings.Contains(got, "Stopped the mount holder.") {
		t.Errorf("missing holder-stopped line:\n%s", got)
	}
}

// TestUninstallEscalatesToKillOnWedgedHolder: a holder that acks shutdown but
// keeps its socket gets the loud socket-peer kill, after which the success
// line still lands once the socket dies.
func TestUninstallEscalatesToKillOnWedgedHolder(t *testing.T) {
	tempHome(t)
	swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) { return nil, nil })
	swapVar(t, &dirMounted, func(string) bool { return false })
	swapVar(t, &holderGoneWait, 300*time.Millisecond)
	stubStopDaemon(t)
	fh := startFakeHolder(t, &fakeHolder{version: version.String()}) // never releases on its own
	killed := false
	swapVar(t, &killHolderPeer, func(socket string) (int, error) {
		if socket != pool.MountsSocketPath() {
			return 0, fmt.Errorf("kill aimed at %q, want the holder socket", socket)
		}
		killed = true
		_ = fh.ln.Close() // the "kill" releases the socket
		return 4242, nil
	})

	cmd, out, errOut := uninstallCmd()
	if err := runServiceUninstall(cmd, false, false); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if !killed {
		t.Fatal("wedged holder was never killed")
	}
	if got := stripANSI(errOut.String()); !strings.Contains(got, "killing the process") {
		t.Errorf("kill escalation must be loud:\n%s", got)
	}
	if got := stripANSI(out.String()); !strings.Contains(got, "Stopped the mount holder.") {
		t.Errorf("missing holder-stopped line after the kill:\n%s", got)
	}
}

// TestUninstallSurvivorMount pins the post-shutdown verification: an account
// dir still mounted hard-aborts a purge (nothing is deleted) and exits
// nonzero without one. --force vouches only for the session gate — the
// survivor verify is unconditional, so a forced purge through a live mirror
// still aborts with all state (rows included) intact.
func TestUninstallSurvivorMount(t *testing.T) {
	cases := map[string]struct {
		purge, force bool
		wantErr      string
	}{
		"purge hard-aborts":                   {purge: true, wantErr: "refusing to purge"},
		"plain uninstall is nonzero":          {purge: false, wantErr: "still mounted"},
		"force purge still hard-aborts":       {purge: true, force: true, wantErr: "refusing to purge"},
		"force plain uninstall stays nonzero": {purge: false, force: true, wantErr: "still mounted"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := tempHome(t)
			fuseDir := filepath.Join(home, ".cc-pool", "accounts", "acct-01")
			seedAccounts(t, store.Account{ID: 1, ConfigDir: fuseDir, OverlayKind: string(overlay.KindFuse)})
			if err := os.MkdirAll(fuseDir, 0o700); err != nil {
				t.Fatal(err)
			}
			swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) { return nil, nil })
			swapVar(t, &dirMounted, func(dir string) bool { return dir == fuseDir })
			stubStopDaemon(t)

			cmd, _, errOut := uninstallCmd()
			err := runServiceUninstall(cmd, tc.purge, tc.force)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
			if tc.purge && !strings.Contains(err.Error(), "acct-01") {
				t.Errorf("purge abort must name the mounted account, got %v", err)
			}
			if got := stripANSI(errOut.String()); !strings.Contains(got, "still a live mountpoint") {
				t.Errorf("survivor not warned:\n%s", got)
			}
			if _, serr := os.Stat(pool.DBPath()); serr != nil {
				t.Errorf("pool state must survive an aborted run: %v", serr)
			}
			// The abort must land before purgeAll's per-account m.Remove: the
			// row (and its dir) survive untouched.
			st, serr := store.Open(pool.DBPath())
			if serr != nil {
				t.Fatalf("reopen store: %v", serr)
			}
			defer func() { _ = st.Close() }()
			accts, lerr := st.ListAccounts()
			if lerr != nil {
				t.Fatalf("list accounts: %v", lerr)
			}
			if len(accts) != 1 || accts[0].ID != 1 {
				t.Errorf("account rows after aborted run = %+v, want acct-01 intact", accts)
			}
			if _, serr := os.Stat(fuseDir); serr != nil {
				t.Errorf("account dir must survive an aborted run: %v", serr)
			}
		})
	}
}

// TestStopDaemonServiceBrewStopFailureIsFatal pins the uninstall safety fix: a
// failed `brew services stop` aborts the run instead of warning and claiming
// success — everything downstream (the holder sweep, the purge) is only safe
// once the daemon is actually down, since a live one respawns the holder and
// remounts fuse rows on its next supervision tick.
func TestStopDaemonServiceBrewStopFailureIsFatal(t *testing.T) {
	swapVar(t, &brewManaged, func() bool { return true })
	swapVar(t, &brewStop, func() error { return errors.New("brew exploded") })

	cmd, out, _ := uninstallCmd()
	err := stopDaemonService(cmd)
	if err == nil || !strings.Contains(err.Error(), "brew exploded") {
		t.Fatalf("error = %v, want the brew failure surfaced", err)
	}
	if !strings.Contains(err.Error(), "respawn the mount holder") {
		t.Errorf("error %q must explain why a live daemon is unsafe", err)
	}
	if got := stripANSI(out.String()); strings.Contains(got, "Stopped the daemon.") {
		t.Errorf("must not claim success after a failed stop:\n%s", got)
	}
}

// TestPurgeAllDefensiveMountRecheck: even with the account rows gone (nothing
// for the caller's row-based verification to see), a mounted dir under
// ~/.cc-pool/accounts aborts purgeAll immediately before the RemoveAll — the
// belt-and-braces guard on the catastrophic delete-into-~/.claude path.
func TestPurgeAllDefensiveMountRecheck(t *testing.T) {
	tempHome(t)
	carcass := filepath.Join(pool.AccountsDir(), "acct-99")
	if err := os.MkdirAll(carcass, 0o700); err != nil {
		t.Fatal(err)
	}
	swapVar(t, &dirMounted, func(dir string) bool { return dir == carcass })

	cmd, _, _ := uninstallCmd()
	err := purgeAll(cmd)
	if err == nil || !strings.Contains(err.Error(), "refusing to purge") || !strings.Contains(err.Error(), carcass) {
		t.Fatalf("error = %v, want refusal naming %s", err, carcass)
	}
	if _, serr := os.Stat(pool.StateDir()); serr != nil {
		t.Fatalf("state dir must survive the aborted purge: %v", serr)
	}
}

// TestPurgeAllRemovesStateWhenClean: with nothing mounted, purgeAll removes
// the state dir and says so. Zero accounts on purpose — row removal goes
// through the Keychain, which tests never touch.
func TestPurgeAllRemovesStateWhenClean(t *testing.T) {
	tempHome(t)
	if err := pool.EnsureAccountsDir(); err != nil {
		t.Fatal(err)
	}
	swapVar(t, &dirMounted, func(string) bool { return false })

	cmd, out, _ := uninstallCmd()
	if err := purgeAll(cmd); err != nil {
		t.Fatalf("purgeAll: %v", err)
	}
	if _, err := os.Stat(pool.StateDir()); !os.IsNotExist(err) {
		t.Errorf("state dir still exists (err=%v)", err)
	}
	if got := stripANSI(out.String()); !strings.Contains(got, "Purged all pool state") {
		t.Errorf("missing purge confirmation:\n%s", got)
	}
}

// TestHolderStatusLine pins the `ccp service status` holder line: silent only
// when there is no holder AND no fuse rows, "not running" when fuse rows need
// one, and a running line with mount count plus a skew warning when the
// holder's build differs.
func TestHolderStatusLine(t *testing.T) {
	cases := map[string]struct {
		holder   *fakeHolder // nil = no socket
		fuseRows int
		want     []string
		empty    bool
	}{
		"absent with no fuse rows says nothing": {
			holder: nil, fuseRows: 0, empty: true,
		},
		"absent with fuse rows is reported": {
			holder: nil, fuseRows: 2, want: []string{"Mount holder: not running"},
		},
		"running at the current version": {
			holder:   &fakeHolder{version: version.String(), mounts: []mountd.MountInfo{{Dir: "/a"}, {Dir: "/b"}}},
			fuseRows: 2,
			want:     []string{"Mount holder: running (" + version.String() + ", 2 mounts)"},
		},
		"running with zero fuse rows still shows (orphan visibility)": {
			holder:   &fakeHolder{version: version.String()},
			fuseRows: 0,
			want:     []string{"Mount holder: running (" + version.String() + ", 0 mounts)"},
		},
		"skewed holder warns": {
			holder:   &fakeHolder{version: "0.0.1-old", mounts: []mountd.MountInfo{{Dir: "/a"}}},
			fuseRows: 1,
			want:     []string{"running (0.0.1-old, 1 mount)", "version skew — will be replaced when idle"},
		},
		"live socket failing health is not responding": {
			holder:   &fakeHolder{version: version.String(), failHealth: true},
			fuseRows: 1,
			want:     []string{"Mount holder: not responding"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tempHome(t)
			if tc.holder != nil {
				startFakeHolder(t, tc.holder)
			}
			got := holderStatusLine(mountd.NewClient(pool.MountsSocketPath()), tc.fuseRows)
			if tc.empty {
				if got != "" {
					t.Fatalf("want no line, got %q", got)
				}
				return
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("line %q missing %q", got, want)
				}
			}
		})
	}
}

// TestUninstallHelpMentionsGateAndPurge keeps the command help honest about
// the new semantics.
func TestUninstallHelpMentionsGateAndPurge(t *testing.T) {
	cmd := newServiceUninstallCmd()
	for _, want := range []string{"mount", "live claude sessions", "--force", "~/.claude is\nnever touched"} {
		if !strings.Contains(cmd.Short+"\n"+cmd.Long, want) {
			t.Errorf("uninstall help missing %q", want)
		}
	}
	for _, flag := range []string{"purge", "force"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Errorf("uninstall lost the --%s flag", flag)
		}
	}
}
