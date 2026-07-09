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
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/mountd"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"github.com/yasyf/fusekit/version"
)

func ptr[T any](v T) *T { return &v }

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

// stubStopDaemon stubs the daemon-stop seam so tests never drive the real
// launchctl/brew.
func stubStopDaemon(t *testing.T) *bool {
	t.Helper()
	called := false
	swapVar(t, &stopDaemon, func(_ *cobra.Command) error {
		called = true
		return nil
	})
	return &called
}

func uninstallCmd() (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	cmd := &cobra.Command{}
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	return cmd, &out, &errOut
}

type fakeHolder struct {
	t  *testing.T
	ln net.Listener

	mu  sync.Mutex
	ops []string

	version       string
	mounts        []mountd.MountInfo
	reclaimFailed []mountd.MountInfo
	failHealth    bool
}

func startFakeHolder(t *testing.T, fh *fakeHolder) *fakeHolder {
	t.Helper()
	fh.t = t
	socket := mountd.DefaultHolderSocket()
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", socket)
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
		case mountd.OpReclaim:
			resp.Mounts = f.reclaimFailed
		}
		_ = json.NewEncoder(conn).Encode(resp)
		_ = conn.Close()
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

// TestUninstallSessionGate pins the uninstall session gate: fuse rows always
// gate, every row under --purge, --force skips the scan, a failed scan aborts.
func TestUninstallSessionGate(t *testing.T) {
	type tc struct {
		purge, force bool
		sessions     []procscan.Session
		scanErr      error
		wantErr      []string
		notInErr     []string
		wantStop     bool
	}
	home := func(t *testing.T) (fuseDir, symDir string) {
		h := tempHome(t)
		fuseDir = filepath.Join(h, ".cc-pool", "accounts", "acct-01")
		symDir = filepath.Join(h, ".cc-pool", "accounts", "acct-02")
		seedAccounts(t,
			store.Account{ID: 1, ConfigDir: fuseDir, OverlayKind: string(fkoverlay.BackendNFS)},
			store.Account{ID: 2, ConfigDir: symDir, OverlayKind: string(fkoverlay.BackendSymlink)},
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
		// purge=false: a passing purge would reach m.Remove and the Keychain,
		// which tests never touch.
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
			// No fake holder: the absent socket makes the holder leg a silent skip.
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

// TestUninstallReclaimsHolderMounts pins that uninstall reclaims cc-pool's own
// mounts but never shuts down the shared multi-tenant holder.
func TestUninstallReclaimsHolderMounts(t *testing.T) {
	tempHome(t)
	swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) { return nil, nil })
	swapVar(t, &dirMounted, func(string) bool { return false })
	stubStopDaemon(t)
	fh := startFakeHolder(t, &fakeHolder{
		version:       version.String(),
		reclaimFailed: []mountd.MountInfo{{Dir: "/tmp/stuck-dir"}},
	})

	cmd, out, errOut := uninstallCmd()
	if err := runServiceUninstall(cmd, false, false); err != nil {
		t.Fatalf("uninstall: %v", err)
	}
	if !fh.sawOp(mountd.OpReclaim) {
		t.Error("the holder never received a reclaim op")
	}
	if fh.sawOp(mountd.OpShutdown) {
		t.Error("uninstall must never shut down the shared multi-tenant holder")
	}
	if got := stripANSI(errOut.String()); !strings.Contains(got, "couldn't unmount /tmp/stuck-dir") {
		t.Errorf("failed dir not reported:\n%s", got)
	}
	if got := stripANSI(out.String()); !strings.Contains(got, "Released cc-pool's mounts from the shared holder.") {
		t.Errorf("missing holder-released line:\n%s", got)
	}
}

// TestUninstallSurvivorMount pins the unconditional post-stop survivor verify:
// a still-mounted account dir hard-aborts a purge and exits nonzero otherwise;
// --force vouches only for the session gate.
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
			seedAccounts(t, store.Account{ID: 1, ConfigDir: fuseDir, OverlayKind: string(fkoverlay.BackendNFS)})
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

// TestStopDaemonServiceBrewStopFailureIsFatal: a failed `brew services stop`
// aborts the uninstall — teardown is only safe once the daemon is down, since
// a live one respawns the holder and remounts fuse rows on its next heal tick.
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

// TestPurgeAllDefensiveMountRecheck: a mounted dir with no account row still
// aborts purgeAll before its RemoveAll — the guard on the catastrophic
// delete-into-~/.claude path.
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

// TestPurgeAllRemovesStateWhenClean seeds zero accounts on purpose — row
// removal goes through the Keychain, which tests never touch.
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

// TestUninstallSurvivorMuxRoot pins the mux-root survivor check: account dirs are
// bridge symlinks into ~/.cc-pool/mnt, so a still-mounted shared root is invisible
// to mountedAccounts — the uninstall must treat the mounted mux root itself as a
// survivor (else purgeAll's RemoveAll walks the live mirror into ~/.claude).
func TestUninstallSurvivorMuxRoot(t *testing.T) {
	cases := map[string]struct {
		purge   bool
		wantErr string
	}{
		"plain uninstall is nonzero": {purge: false, wantErr: "still mounted"},
		"purge hard-aborts":          {purge: true, wantErr: "refusing to purge"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			home := tempHome(t)
			// A fuse account whose dir is a bridge symlink — never a mountpoint itself.
			acctDir := filepath.Join(home, ".cc-pool", "accounts", "acct-01")
			seedAccounts(t, store.Account{ID: 1, ConfigDir: acctDir, OverlayKind: string(fkoverlay.BackendNFS)})
			swapVar(t, &scanSessions, func(context.Context) ([]procscan.Session, error) { return nil, nil })
			// Only the shared mux root reads mounted; the bridge-symlink account dir does not.
			swapVar(t, &dirMounted, func(dir string) bool { return dir == pool.MuxRootDir() })
			stubStopDaemon(t)

			cmd, _, errOut := uninstallCmd()
			err := runServiceUninstall(cmd, tc.purge, false)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
			}
			if tc.purge && !strings.Contains(err.Error(), pool.MuxRootDir()) {
				t.Errorf("purge abort must name the live mux root, got %v", err)
			}
			if got := stripANSI(errOut.String()); !strings.Contains(got, "still a live mountpoint") {
				t.Errorf("mux-root survivor not warned:\n%s", got)
			}
			if _, serr := os.Stat(pool.DBPath()); serr != nil {
				t.Errorf("pool state must survive an aborted run: %v", serr)
			}
		})
	}
}

// TestPurgeAllRefusesLiveMuxRoot pins the second guard on the catastrophic
// delete-into-~/.claude path: purgeAll refuses its RemoveAll while the shared mux
// root (outside accounts/, so mountedStateDirs never sees it) is a live mountpoint.
func TestPurgeAllRefusesLiveMuxRoot(t *testing.T) {
	tempHome(t)
	if err := pool.EnsureAccountsDir(); err != nil {
		t.Fatal(err)
	}
	swapVar(t, &dirMounted, func(dir string) bool { return dir == pool.MuxRootDir() })

	cmd, _, _ := uninstallCmd()
	err := purgeAll(cmd)
	if err == nil || !strings.Contains(err.Error(), "refusing to purge") || !strings.Contains(err.Error(), pool.MuxRootDir()) {
		t.Fatalf("error = %v, want a refusal naming the mux root", err)
	}
	if _, serr := os.Stat(pool.StateDir()); serr != nil {
		t.Fatalf("state dir must survive the aborted purge: %v", serr)
	}
}

// TestHolderStatusLine pins the `ccp service status` holder line: silent when
// there is no holder and no fuse rows, "not running" when fuse rows need one,
// a running line with mount count otherwise.
func TestHolderStatusLine(t *testing.T) {
	cases := map[string]struct {
		holder   *fakeHolder
		fuseRows int
		want     []string
		notWant  []string
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
		"holder version is reported as-is, never as skew": {
			// The holder is a separate product (the fusekit-holder cask), so its
			// version never reads as skew.
			holder:   &fakeHolder{version: "0.0.1-old", mounts: []mountd.MountInfo{{Dir: "/a"}}},
			fuseRows: 1,
			want:     []string{"running (0.0.1-old, 1 mount)"},
			notWant:  []string{"version skew", "will be replaced"},
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
			got := holderStatusLine(holderClient(), tc.fuseRows)
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
			for _, notWant := range tc.notWant {
				if strings.Contains(got, notWant) {
					t.Errorf("line %q must not contain %q", got, notWant)
				}
			}
		})
	}
}

// TestUninstallHelpMentionsGateAndPurge keeps the command help honest about
// the gate and purge semantics.
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
