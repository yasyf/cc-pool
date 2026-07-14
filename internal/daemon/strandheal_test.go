package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/fusekit/fileproviderd"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
)

const strandIdentityJSON = `{"oauthAccount":{"accountUuid":"u-1","emailAddress":"a@example.com"}}`

// fakeStrandFP is a File Provider provider fake for the strand-heal tests. It
// models the domain on disk the way the real provider does: Teardown removes the
// account-dir bridge symlink and deregisters; DomainRoot is the zero-spawn
// registration check (ErrNoDomain when unregistered); RemoveDomain deregisters
// WITHOUT touching the dir. It records teardowns and removes for assertions.
type fakeStrandFP struct {
	registered map[string]bool
	teardowns  []string
	removes    []string
}

func newFakeStrandFP(registered ...string) *fakeStrandFP {
	f := &fakeStrandFP{registered: map[string]bool{}}
	for _, r := range registered {
		f.registered[r] = true
	}
	return f
}

func (f *fakeStrandFP) Backend() fkoverlay.Backend    { return fkoverlay.BackendFileProvider }
func (f *fakeStrandFP) PrivateRoot(dir string) string { return fkoverlay.FusePrivateRoot(dir) }
func (f *fakeStrandFP) Health(_, _ string) error      { return nil }
func (f *fakeStrandFP) Sync(_, _ string) error        { return nil }
func (f *fakeStrandFP) Setup(_, _ string) error       { return nil }

func (f *fakeStrandFP) Teardown(_, dir string) (string, error) {
	f.teardowns = append(f.teardowns, dir)
	if fi, err := os.Lstat(dir); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		if err := os.Remove(dir); err != nil {
			return "", err
		}
	}
	delete(f.registered, filepath.Base(dir))
	return "", nil
}

func (f *fakeStrandFP) RemoveDomain(dir string) error {
	f.removes = append(f.removes, dir)
	delete(f.registered, filepath.Base(dir))
	return nil
}

func (f *fakeStrandFP) DomainRoot(_ context.Context, dir string) (string, error) {
	if !f.registered[filepath.Base(dir)] {
		return "", fmt.Errorf("state %s: %w", dir, fileproviderd.ErrNoDomain)
	}
	return "/domain/" + filepath.Base(dir), nil
}

func fpAndSymlinkOverlay(fp fkoverlay.Provider, spec fkoverlay.Spec) func(fkoverlay.Backend) (fkoverlay.Provider, error) {
	return func(b fkoverlay.Backend) (fkoverlay.Provider, error) {
		switch b {
		case fkoverlay.BackendFileProvider:
			return fp, nil
		case fkoverlay.BackendSymlink:
			return &fkoverlay.SymlinkProvider{Spec: spec}, nil
		default:
			return nil, fmt.Errorf("unexpected backend %q", b)
		}
	}
}

// TestReconcileAccountConvergesLeakedBridge pins (b): a symlink row whose dir is a
// leaked File Provider domain bridge symlink (a convert that laid the bridge but
// crashed before flipping the row) is retracted, the real dir is recreated, and the
// stranded private files are healed back — all off the startup reconcile, never
// draining THROUGH the live domain.
func TestReconcileAccountConvergesLeakedBridge(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	base := pool.ClaudeDir()
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	s, dirs := newTestServer(t)
	dir := dirs[1]
	priv := fkoverlay.FusePrivateRoot(dir)
	// The crash wreckage: identity stranded in the private root, the dir a File
	// Provider domain bridge symlink, the row still symlink.
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(strandIdentityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	domainRoot := filepath.Join(t.TempDir(), "CCPoolStatus-"+filepath.Base(dir))
	if err := os.Symlink(domainRoot, dir); err != nil {
		t.Fatal(err)
	}

	fp := newFakeStrandFP(filepath.Base(dir))
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())

	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	s.reconcileAccount(t.Context(), a)

	if len(fp.teardowns) == 0 {
		t.Fatal("leaked bridge not retracted: FP Teardown never called")
	}
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("dir not a real directory after converge: fi=%v err=%v", fi, err)
	}
	if got := readFileString(t, filepath.Join(dir, ".claude.json")); got != strandIdentityJSON {
		t.Fatalf("identity not restored to the account dir: %q", got)
	}
	if _, err := os.Readlink(filepath.Join(dir, "projects")); err != nil {
		t.Fatalf("symlink overlay not re-laid after converge: %v", err)
	}
	if _, err := os.Lstat(priv); !os.IsNotExist(err) {
		t.Fatal("emptied private root not removed after heal")
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatalf("row changed during converge: %q", kindOf(t, s, 1))
	}
}

// TestHealStrandedRowsSweepsLeakedDomains pins (e)'s leak sweep: a File Provider
// domain still registered against a symlink row is deregistered (RemoveDomain),
// while a fileprovider row's domain is left alone — the sweep keys on the row type,
// never touching an account that legitimately owns its domain.
func TestHealStrandedRowsSweepsLeakedDomains(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	distinctAccountDirs(t, s, dirs)
	// acct-1 stays symlink (its FP domain is a leak); acct-2 is a legitimate
	// fileprovider row whose domain must be left registered.
	if err := s.m.Store.SetAccountOverlayKind(2, string(fkoverlay.BackendFileProvider)); err != nil {
		t.Fatal(err)
	}
	fp := newFakeStrandFP(filepath.Base(dirs[1]), filepath.Base(dirs[2]))
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
	s.fpSynth = alwaysNonEmpty
	s.fpBridgeReadyFn = func() bool { return true }

	s.healStrandedRows(t.Context(), s.newTick(t.Context()))

	if len(fp.removes) != 1 || fp.removes[0] != dirs[1] {
		t.Fatalf("leak sweep removes = %v, want exactly [%s] (the symlink row's leaked domain)", fp.removes, dirs[1])
	}
	if !fp.registered[filepath.Base(dirs[2])] {
		t.Fatal("fileprovider row's domain was deregistered; the sweep must leave it alone")
	}
	if fp.registered[filepath.Base(dirs[1])] {
		t.Fatal("symlink row's leaked domain not deregistered")
	}
}

// TestHealStrandedRowsDefersUnderHeldLease pins G5: the stranded-row heal is a local
// destructive op fenced under the row's session-lease key, so a live handout holding
// that lease defers the heal — the leaked domain is left registered until the session
// ends, never removed underneath a live `ccp env`.
func TestHealStrandedRowsDefersUnderHeldLease(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	distinctAccountDirs(t, s, dirs)
	fp := newFakeStrandFP(filepath.Base(dirs[1]))
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
	s.fpSynth = alwaysNonEmpty
	s.fpBridgeReadyFn = func() bool { return true }

	// A live `ccp env` holds acct-1's session lease.
	root, err := s.m.LeaseRoot()
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	h, err := lease.Acquire(root, pool.SessionLeaseDir(a), pool.HolderOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Close() }()

	s.healStrandedRows(t.Context(), s.newTick(t.Context()))

	if len(fp.removes) != 0 {
		t.Fatalf("leak sweep ran under a held session lease: removes = %v, want none (deferred)", fp.removes)
	}
	if !fp.registered[filepath.Base(dirs[1])] {
		t.Fatal("the leaked domain was removed under a live handout's lease; the heal must defer")
	}
}

// TestHealStrandedRowsSkipsSweepWhenBridgeDown pins the guard: with the File
// Provider bridge down, the leak sweep never runs (a State probe through a down
// stack is meaningless and could spawn), so no registration is touched.
func TestHealStrandedRowsSkipsSweepWhenBridgeDown(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	distinctAccountDirs(t, s, dirs)
	fp := newFakeStrandFP(filepath.Base(dirs[1]), filepath.Base(dirs[2]))
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
	s.fpSynth = alwaysNonEmpty
	s.fpBridgeReadyFn = func() bool { return false }

	s.healStrandedRows(t.Context(), s.newTick(t.Context()))

	if len(fp.removes) != 0 {
		t.Fatalf("leak sweep ran with the bridge down: removes = %v", fp.removes)
	}
}

// distinctAccountDirs repoints the test accounts onto distinct-basename dirs.
// newTestServer names every account dir "acct", but the File Provider domain
// identifier is the dir basename, so two "acct" dirs would collide into one domain.
func distinctAccountDirs(t *testing.T, s *Server, dirs map[int]string) {
	t.Helper()
	root := t.TempDir()
	for id := range dirs {
		dir := filepath.Join(root, pool.AccountDirName(id))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		a, err := s.m.Store.GetAccount(id)
		if err != nil {
			t.Fatal(err)
		}
		a.ConfigDir = dir
		if err := s.m.Store.UpsertAccount(a); err != nil {
			t.Fatal(err)
		}
		dirs[id] = dir
	}
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is under the test's own t.TempDir()
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// stageLeakedBridge writes the crash wreckage of a symlink→fileprovider convert
// that died after Setup, before the row flip: the account identity stranded in the
// private root, and the account dir replaced by a File Provider domain bridge
// symlink. The store row is left untouched.
func stageLeakedBridge(t *testing.T, dir string) {
	t.Helper()
	priv := fkoverlay.FusePrivateRoot(dir)
	if err := os.MkdirAll(priv, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(priv, ".claude.json"), []byte(strandIdentityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	domainRoot := filepath.Join(t.TempDir(), "CCPoolStatus-"+filepath.Base(dir))
	if err := os.Symlink(domainRoot, dir); err != nil {
		t.Fatal(err)
	}
}

func TestHealStrandedRowsLogsOnlyPendingWork(t *testing.T) {
	cases := []struct {
		name         string
		bridged      bool
		busy         bool
		wantScan     bool
		wantDeferred bool
		wantHealed   bool
	}{
		{name: "healthy busy row is silent", busy: true},
		{name: "bridged busy row defers", bridged: true, busy: true, wantScan: true, wantDeferred: true},
		{name: "bridged idle row heals", bridged: true, wantScan: true, wantHealed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			base := pool.ClaudeDir()
			if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}

			s, dirs := newTestServer(t)
			distinctAccountDirs(t, s, dirs)
			dir := dirs[1]
			fp := newFakeStrandFP()
			if tc.bridged {
				stageLeakedBridge(t, dir)
				fp.registered[filepath.Base(dir)] = true
			}
			s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
			s.fpSynth = alwaysNonEmpty
			s.fpBridgeReadyFn = func() bool { return true }
			scanCalls := 0
			s.scanSessions = func(context.Context) ([]procscan.Session, error) {
				scanCalls++
				if tc.busy {
					return []procscan.Session{{PID: 4242, ConfigDir: dir}}, nil
				}
				return nil, nil
			}
			var logs bytes.Buffer
			s.log = log.New(&logs, "", 0)

			s.healStrandedRows(t.Context(), s.newTick(t.Context()))

			if got := scanCalls > 0; got != tc.wantScan {
				t.Errorf("session scan ran = %v, want %v (calls=%d)", got, tc.wantScan, scanCalls)
			}
			out := logs.String()
			if !tc.wantDeferred && !tc.wantHealed && out != "" {
				t.Fatalf("healthy row logged: %q", out)
			}
			if got := strings.Contains(out, "deferring stranded-bridge heal: 1 live session(s)"); got != tc.wantDeferred {
				t.Errorf("defer log present = %v, want %v; logs=%q", got, tc.wantDeferred, out)
			}
			if got := strings.Contains(out, "restored private files stranded by an interrupted migration"); got != tc.wantHealed {
				t.Errorf("heal log present = %v, want %v; logs=%q", got, tc.wantHealed, out)
			}
			if tc.wantDeferred {
				if len(fp.teardowns) != 0 || len(fp.removes) != 0 {
					t.Fatalf("busy heal took action: teardowns=%v removes=%v", fp.teardowns, fp.removes)
				}
				if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("busy bridge changed: fi=%v err=%v", fi, err)
				}
				if got := readFileString(t, filepath.Join(fkoverlay.FusePrivateRoot(dir), ".claude.json")); got != strandIdentityJSON {
					t.Fatalf("busy heal moved private identity: %q", got)
				}
			}
			if tc.wantHealed {
				if len(fp.teardowns) != 1 || fp.teardowns[0] != dir {
					t.Fatalf("healing teardowns = %v, want [%s]", fp.teardowns, dir)
				}
				if len(fp.removes) != 0 {
					t.Fatalf("healing removed an already-torn-down domain: %v", fp.removes)
				}
				if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
					t.Fatalf("healed dir is not real: fi=%v err=%v", fi, err)
				}
				if got := readFileString(t, filepath.Join(dir, ".claude.json")); got != strandIdentityJSON {
					t.Fatalf("identity not restored: %q", got)
				}
				if _, err := os.Lstat(fkoverlay.FusePrivateRoot(dir)); !os.IsNotExist(err) {
					t.Fatalf("private root survived heal: %v", err)
				}
			}
			if s.cl.held(1) {
				t.Fatal("strand-heal claim remained held")
			}
		})
	}
}

// TestHealStrandedSymlinkRowSkipsRowFlippedToFileProvider pins the stale-snapshot
// race (finding A): the heal listing captured the row as symlink, but a user
// conversion flipped it to fileprovider before the poll claim. The dir is now a
// LEGITIMATE domain bridge symlink, so the re-read under the claim must no-op the
// heal — no Teardown, no RemoveDomain, no private-file move — rather than retract a
// live domain off the stale snapshot.
func TestHealStrandedSymlinkRowSkipsRowFlippedToFileProvider(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	dir := dirs[1]
	priv := fkoverlay.FusePrivateRoot(dir)
	stageLeakedBridge(t, dir)

	fp := newFakeStrandFP(filepath.Base(dir))
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
	s.fpSynth = alwaysNonEmpty
	s.fpBridgeReadyFn = func() bool { return true }

	// The stale snapshot: captured while the row was still symlink.
	stale, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	// The user's conversion won the race: the row is now fileprovider and its dir
	// symlink is a legitimate domain bridge.
	if err := s.m.Store.SetAccountOverlayKind(1, string(fkoverlay.BackendFileProvider)); err != nil {
		t.Fatal(err)
	}

	s.healStrandedSymlinkRow(t.Context(), s.newTick(t.Context()), stale)

	if len(fp.teardowns) != 0 {
		t.Fatalf("stale symlink snapshot retracted a live fileprovider domain: teardowns = %v", fp.teardowns)
	}
	if len(fp.removes) != 0 {
		t.Fatalf("stale symlink snapshot deregistered a live fileprovider domain: removes = %v", fp.removes)
	}
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("legitimate domain bridge symlink destroyed: fi=%v err=%v", fi, err)
	}
	if got := readFileString(t, filepath.Join(priv, ".claude.json")); got != strandIdentityJSON {
		t.Fatalf("private identity dragged off the fileprovider private store: %q", got)
	}
	if kindOf(t, s, 1) != "fileprovider" {
		t.Fatalf("row changed during the skipped heal: %q", kindOf(t, s, 1))
	}
}

// TestHealStrandedRowsSkipsSweepUnderLiveSession pins the live-session gate on the
// leak sweep (finding B): a symlink row with a File Provider domain still
// registered against it (a real-dir leak) and a live claude bound to its dir must
// defer — the sweep must not deregister the domain out from under the session (nor
// can a select landing mid-tick be fenced by beginPoll alone). acct-2 has no leak
// and no session, so the sweep leaving it untouched confirms the skip is the
// session, not a dead sweep.
func TestHealStrandedRowsSkipsSweepUnderLiveSession(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	s, dirs := newTestServer(t)
	distinctAccountDirs(t, s, dirs)
	fp := newFakeStrandFP(filepath.Base(dirs[1])) // only acct-1 has a leaked domain
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
	s.fpSynth = alwaysNonEmpty
	s.fpBridgeReadyFn = func() bool { return true }
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dirs[1]}}, nil
	}

	s.healStrandedRows(t.Context(), s.newTick(t.Context()))

	if len(fp.removes) != 0 {
		t.Fatalf("leak sweep deregistered a domain under a live session: removes = %v", fp.removes)
	}
	if len(fp.teardowns) != 0 {
		t.Fatalf("converge retracted a bridge under a live session: teardowns = %v", fp.teardowns)
	}
	if !fp.registered[filepath.Base(dirs[1])] {
		t.Fatal("acct-1's domain deregistered despite the live session on its dir")
	}
}

// TestReconcileAccountSkipsConvergeUnderLiveSession pins finding B on the startup
// reconcile arm: the crash-window bridge may still back a claude that was running
// when the convert crashed, and the socket serves selects during the startup
// reconcile. With a live session on the bridge the converge must defer — leaving
// the bridge, the stranded identity, and the row untouched — rather than retract
// the domain out from under it.
func TestReconcileAccountSkipsConvergeUnderLiveSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	base := pool.ClaudeDir()
	if err := os.MkdirAll(filepath.Join(base, "projects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	s, dirs := newTestServer(t)
	dir := dirs[1]
	priv := fkoverlay.FusePrivateRoot(dir)
	stageLeakedBridge(t, dir)

	fp := newFakeStrandFP(filepath.Base(dir))
	s.m.OverlayFor = fpAndSymlinkOverlay(fp, s.m.OverlaySpec())
	s.scanSessions = func(context.Context) ([]procscan.Session, error) {
		return []procscan.Session{{PID: 4242, ConfigDir: dir}}, nil
	}

	a, err := s.m.Store.GetAccount(1)
	if err != nil {
		t.Fatal(err)
	}
	s.reconcileAccount(t.Context(), a)

	if len(fp.teardowns) != 0 {
		t.Fatalf("converge retracted the bridge under a live session: teardowns = %v", fp.teardowns)
	}
	if fi, err := os.Lstat(dir); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("bridge symlink destroyed under a live session: fi=%v err=%v", fi, err)
	}
	if got := readFileString(t, filepath.Join(priv, ".claude.json")); got != strandIdentityJSON {
		t.Fatalf("private identity moved out from under a live session: %q", got)
	}
	if kindOf(t, s, 1) != "symlink" {
		t.Fatalf("row changed under the deferred converge: %q", kindOf(t, s, 1))
	}
}
