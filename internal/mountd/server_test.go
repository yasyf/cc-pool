package mountd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/version"
)

// hostCall is one recorded Setup or Teardown invocation.
type hostCall struct{ base, dir string }

// fakeHost is an overlay.Provider whose Setup/Teardown record calls and answer
// from injectable hooks — no real mounts, so the suite runs in pure-Go CI with
// no fuse-t installed. It also models kernel mount state: a successful Setup
// marks dir live, a successful Teardown clears it (a failing Teardown leaves
// it, like a wedged unmount). installLiveSeams points the package seams here.
type fakeHost struct {
	mu        sync.Mutex
	setups    []hostCall
	teardowns []hostCall
	live      map[string]bool
	// setupFn/teardownFn, when non-nil, decide the outcome AFTER the call is
	// recorded. They run outside the fake's lock so they may block — the
	// concurrency tests gate on a channel inside them.
	setupFn    func(base, dir string) error
	teardownFn func(base, dir string) error
}

var _ overlay.Provider = (*fakeHost)(nil)

func (f *fakeHost) Kind() overlay.Kind { return overlay.KindFuse }

func (f *fakeHost) Setup(base, dir string) error {
	f.mu.Lock()
	f.setups = append(f.setups, hostCall{base, dir})
	fn := f.setupFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(base, dir); err != nil {
			return err
		}
	}
	f.setLive(dir, true)
	return nil
}

func (f *fakeHost) Teardown(base, dir string) error {
	f.mu.Lock()
	f.teardowns = append(f.teardowns, hostCall{base, dir})
	fn := f.teardownFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(base, dir); err != nil {
			return err
		}
	}
	f.setLive(dir, false)
	return nil
}

func (f *fakeHost) setLive(dir string, live bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.live == nil {
		f.live = map[string]bool{}
	}
	if live {
		f.live[dir] = true
		return
	}
	delete(f.live, dir)
}

// isLive reports whether the fake currently hosts a live mirror at dir.
func (f *fakeHost) isLive(dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.live[dir]
}

func (f *fakeHost) Sync(base, dir string) error   { return nil }
func (f *fakeHost) Health(base, dir string) error { return nil }
func (f *fakeHost) PrivateRoot(dir string) string { return dir + ".private" }

func (f *fakeHost) calls() (setups, teardowns []hostCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]hostCall(nil), f.setups...), append([]hostCall(nil), f.teardowns...)
}

// shortSockDir returns a fresh dir under /tmp for the holder socket: macOS
// caps sun_path at 104 bytes and t.TempDir() paths exceed it.
func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-mountd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// startServerAt runs a holder on the given socket and waits for it to accept.
// Cleanup cancels Run's ctx and waits for it to exit; done is buffered so
// tests that never read it still let Run finish.
func startServerAt(t *testing.T, fake *fakeHost, socket string) (s *Server, cl *Client, done chan error, cancel context.CancelFunc) {
	t.Helper()
	s = &Server{Socket: socket, Host: fake, Log: log.New(io.Discard, "", 0)}
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		done <- s.Run(ctx)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("holder did not stop on ctx cancel")
		}
	})
	cl = NewClient(socket)
	waitAvailable(t, cl)
	return s, cl, done, cancel
}

func startServer(t *testing.T, fake *fakeHost) (s *Server, cl *Client, done chan error, cancel context.CancelFunc) {
	t.Helper()
	return startServerAt(t, fake, filepath.Join(shortSockDir(t), "m.sock"))
}

func waitAvailable(t *testing.T, cl *Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("holder socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newHandlerServer returns a Server wired for direct dispatch with no socket.
func newHandlerServer(f *fakeHost) *Server {
	s := &Server{Host: f, Log: log.New(io.Discard, "", 0)}
	s.initState()
	return s
}

// fakeMounted overrides the mounted seam for one test, restoring it after.
// Tests using it must not run in parallel (the seam is a package var).
func fakeMounted(t *testing.T, fn func(dir string) bool) {
	t.Helper()
	prev := mounted
	mounted = fn
	t.Cleanup(func() { mounted = prev })
}

// fakeMountAlive overrides the mountAlive seam for one test, restoring it
// after. Same no-parallel rule as fakeMounted.
func fakeMountAlive(t *testing.T, fn func(base, dir string) bool) {
	t.Helper()
	prev := mountAlive
	mountAlive = fn
	t.Cleanup(func() { mountAlive = prev })
}

// registryBases flattens the registry snapshot to dir -> base. The handler
// tables assert row membership through it; epoch and mount-time behavior is
// pinned separately (TestListReportsEpochMountedAt).
func registryBases(s *Server) map[string]string {
	bases := map[string]string{}
	for dir, row := range s.snapshotRegistry() {
		bases[dir] = row.Base
	}
	return bases
}

// installLiveSeams points both kernel-state seams at the fake's live set, so
// over-the-socket tests see the mount state the fake's Setup/Teardown imply.
// Must run BEFORE the server starts: the seams are package vars the handler
// goroutines read, and swapping them mid-serve would race.
func installLiveSeams(t *testing.T, fake *fakeHost) {
	t.Helper()
	fakeMounted(t, fake.isLive)
	fakeMountAlive(t, func(_, dir string) bool { return fake.isLive(dir) })
}

func TestHandleMount(t *testing.T) {
	const (
		base = "/pool/base"
		dir  = "/pool/acct-01"
	)
	tests := []struct {
		name        string
		base, dir   string
		seed        map[string]string // pre-existing registry rows
		inflight    []string          // dirs whose claim is already held
		mountedAt   map[string]bool   // seam: dirs that look like mountpoints
		aliveAt     map[string]bool   // seam: dirs whose mirror shows base's contents
		setupErr    error             // returned by the fake's Setup
		teardownErr error             // returned by the fake's Teardown
		wantOK      bool
		wantClass   string
		wantErr     string // required substring of Error when wantOK is false
		wantSetup   []hostCall
		wantTear    []hostCall
		wantReg     map[string]string
	}{
		{
			name: "fresh mount registers",
			base: base, dir: dir,
			wantOK:    true,
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{dir: base},
		},
		{
			name: "repeat mount of the same LIVE pair is idempotent and skips Setup",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			mountedAt: map[string]bool{dir: true},
			aliveAt:   map[string]bool{dir: true},
			wantOK:    true,
			wantReg:   map[string]string{dir: base},
		},
		{
			name: "registered dir with a different base classifies base-mismatch",
			base: base, dir: dir,
			seed:      map[string]string{dir: "/pool/other"},
			wantOK:    false,
			wantClass: ClassBaseMismatch,
			wantErr:   "already mirrors",
			wantReg:   map[string]string{dir: "/pool/other"},
		},
		{
			// Mount is ensure-mounted: a registered mirror that is no longer a
			// mountpoint (external umount) is torn down and remounted.
			name: "dead mirror (not a mountpoint) is torn down and remounted",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			wantOK:    true,
			wantTear:  []hostCall{{base, dir}},
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{dir: base},
		},
		{
			// Still a mountpoint, but base's contents no longer show through
			// (wedged fuse daemon): same ensure-mounted recovery.
			name: "dead mirror (mountpoint, base not visible) is torn down and remounted",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			mountedAt: map[string]bool{dir: true},
			wantOK:    true,
			wantTear:  []hostCall{{base, dir}},
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{dir: base},
		},
		{
			name: "dead mirror whose teardown wedges classifies wedged, deregisters, never re-Setups",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: fmt.Errorf("%w: %s; refusing to treat it as torn down", overlay.ErrUnmountWedged, dir),
			wantOK:      false,
			wantClass:   ClassWedged,
			wantErr:     "refusing to treat it as torn down",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "dead mirror whose teardown fails plainly classifies mount-failed",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: errors.New("umount: EBUSY"),
			wantOK:      false,
			wantClass:   ClassMountFailed,
			wantErr:     "EBUSY",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "setup failure classifies mount-failed and does not register",
			base: base, dir: dir,
			setupErr:  errors.New("mount_fuset: exec format error"),
			wantOK:    false,
			wantClass: ClassMountFailed,
			wantErr:   "exec format error",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "setup wrapping ErrMountNotLive classifies tcc and does not register",
			base: base, dir: dir,
			setupErr:  fmt.Errorf("%w: %s (grant Network Volumes access once)", overlay.ErrMountNotLive, dir),
			wantOK:    false,
			wantClass: ClassTCC,
			wantErr:   "grant Network Volumes access",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			// A proven-grant timeout must classify mount-timeout — exact-match
			// on wantClass also pins NOT tcc and NOT mount-failed.
			name: "setup wrapping ErrMountTimeout classifies mount-timeout and does not register",
			base: base, dir: dir,
			setupErr:  fmt.Errorf("%w: %s after 8s; the Network Volumes grant is proven — transient fuse-t slowness, retrying", overlay.ErrMountTimeout, dir),
			wantOK:    false,
			wantClass: ClassMountTimeout,
			wantErr:   "transient fuse-t slowness",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "foreign mountpoint is refused before Setup",
			base: base, dir: dir,
			mountedAt: map[string]bool{dir: true},
			wantOK:    false,
			wantClass: ClassForeignMount,
			wantErr:   "unmount it first",
			wantReg:   map[string]string{},
		},
		{
			name: "empty base refused",
			base: "", dir: dir,
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{},
		},
		{
			name: "empty dir refused",
			base: base, dir: "",
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{},
		},
		{
			name: "dir equal to base refused",
			base: base, dir: base,
			wantOK:  false,
			wantErr: "refusing dir == base",
			wantReg: map[string]string{},
		},
		{
			name: "in-flight dir is busy and never reaches the provider",
			base: base, dir: dir,
			inflight:  []string{dir},
			wantOK:    false,
			wantClass: ClassBusy,
			wantErr:   "busy",
			wantReg:   map[string]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{
				setupFn:    func(string, string) error { return tc.setupErr },
				teardownFn: func(string, string) error { return tc.teardownErr },
			}
			s := newHandlerServer(fake)
			for d, b := range tc.seed {
				s.registry[d] = mountRow{Base: b}
			}
			for _, d := range tc.inflight {
				s.inflight[d] = true
			}
			fakeMounted(t, func(d string) bool { return tc.mountedAt[d] })
			fakeMountAlive(t, func(_, d string) bool { return tc.aliveAt[d] })

			resp := s.dispatch(Request{Op: OpMount, Base: tc.base, Dir: tc.dir})

			assertResp(t, resp, tc.wantOK, tc.wantClass, tc.wantErr)
			setups, tears := fake.calls()
			if !reflect.DeepEqual(setups, tc.wantSetup) {
				t.Errorf("Setup calls = %v, want %v", setups, tc.wantSetup)
			}
			if !reflect.DeepEqual(tears, tc.wantTear) {
				t.Errorf("Teardown calls = %v, want %v", tears, tc.wantTear)
			}
			if got := registryBases(s); !reflect.DeepEqual(got, tc.wantReg) {
				t.Errorf("registry = %v, want %v", got, tc.wantReg)
			}
			assertClaimsReleased(t, s, len(tc.inflight))
		})
	}
}

func TestHandleUnmount(t *testing.T) {
	const (
		base = "/pool/base"
		dir  = "/pool/acct-01"
	)
	tests := []struct {
		name        string
		base, dir   string
		seed        map[string]string
		inflight    []string
		mountedAt   map[string]bool
		teardownErr error
		wantOK      bool
		wantClass   string
		wantErr     string
		wantTear    []hostCall
		wantReg     map[string]string
	}{
		{
			name: "registered dir unmounts and deregisters",
			base: base, dir: dir,
			seed:     map[string]string{dir: base},
			wantOK:   true,
			wantTear: []hostCall{{base, dir}},
			wantReg:  map[string]string{},
		},
		{
			name: "registry base wins over the request base",
			base: "/pool/lies", dir: dir,
			seed:     map[string]string{dir: base},
			wantOK:   true,
			wantTear: []hostCall{{base, dir}},
			wantReg:  map[string]string{},
		},
		{
			name: "wedged teardown classifies wedged and STILL deregisters",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: fmt.Errorf("%w: %s; refusing to treat it as torn down", overlay.ErrUnmountWedged, dir),
			wantOK:      false,
			wantClass:   ClassWedged,
			wantErr:     "refusing to treat it as torn down",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "plain teardown failure carries no class and still deregisters",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: errors.New("umount: EBUSY"),
			wantOK:      false,
			wantErr:     "EBUSY",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "unknown unmounted dir is an OK no-op without Teardown",
			base: base, dir: dir,
			wantOK:  true,
			wantReg: map[string]string{},
		},
		{
			name: "carcass: unknown mountpoint is torn down with the request base",
			base: base, dir: dir,
			mountedAt: map[string]bool{dir: true},
			wantOK:    true,
			wantTear:  []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "empty base refused even though the registry could supply it",
			base: "", dir: dir,
			seed:    map[string]string{dir: base},
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{dir: base},
		},
		{
			name: "empty dir refused",
			base: base, dir: "",
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{},
		},
		{
			name: "dir equal to base refused",
			base: base, dir: base,
			wantOK:  false,
			wantErr: "refusing dir == base",
			wantReg: map[string]string{},
		},
		{
			name: "in-flight dir is busy and stays registered",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			inflight:  []string{dir},
			wantOK:    false,
			wantClass: ClassBusy,
			wantErr:   "busy",
			wantReg:   map[string]string{dir: base},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{teardownFn: func(string, string) error { return tc.teardownErr }}
			s := newHandlerServer(fake)
			for d, b := range tc.seed {
				s.registry[d] = mountRow{Base: b}
			}
			for _, d := range tc.inflight {
				s.inflight[d] = true
			}
			fakeMounted(t, func(d string) bool { return tc.mountedAt[d] })

			resp := s.dispatch(Request{Op: OpUnmount, Base: tc.base, Dir: tc.dir})

			assertResp(t, resp, tc.wantOK, tc.wantClass, tc.wantErr)
			if _, tears := fake.calls(); !reflect.DeepEqual(tears, tc.wantTear) {
				t.Errorf("Teardown calls = %v, want %v", tears, tc.wantTear)
			}
			if got := registryBases(s); !reflect.DeepEqual(got, tc.wantReg) {
				t.Errorf("registry = %v, want %v", got, tc.wantReg)
			}
			assertClaimsReleased(t, s, len(tc.inflight))
		})
	}
}

// assertResp checks the OK/ErrClass/Error triple of one response. Failing
// cases must pin an error substring so a wrong-reason failure cannot pass.
func assertResp(t *testing.T, resp Response, wantOK bool, wantClass, wantErr string) {
	t.Helper()
	if resp.OK != wantOK {
		t.Fatalf("OK = %v (error %q), want %v", resp.OK, resp.Error, wantOK)
	}
	if resp.ErrClass != wantClass {
		t.Errorf("ErrClass = %q, want %q", resp.ErrClass, wantClass)
	}
	if wantOK {
		if resp.Error != "" {
			t.Errorf("Error = %q on an OK response", resp.Error)
		}
		return
	}
	if wantErr == "" {
		t.Fatal("test bug: a failing case must pin an error substring")
	}
	if !strings.Contains(resp.Error, wantErr) {
		t.Errorf("Error = %q, want substring %q", resp.Error, wantErr)
	}
}

// assertClaimsReleased verifies a handler returned its in-flight claim; only
// the claims the test itself seeded may remain.
func assertClaimsReleased(t *testing.T, s *Server, seeded int) {
	t.Helper()
	s.mu.Lock()
	held := len(s.inflight)
	s.mu.Unlock()
	if held != seeded {
		t.Errorf("in-flight gate leaked: %d claims held, want %d", held, seeded)
	}
}

func TestHandleList(t *testing.T) {
	t.Run("Live needs BOTH the mountpoint and base visibility, sorted by dir", func(t *testing.T) {
		s := newHandlerServer(&fakeHost{})
		s.registry["/pool/acct-01"] = mountRow{Base: "/pool/base"}
		s.registry["/pool/acct-02"] = mountRow{Base: "/pool/base"}
		s.registry["/pool/acct-03"] = mountRow{Base: "/pool/base"}
		fakeMounted(t, func(dir string) bool {
			return dir == "/pool/acct-01" || dir == "/pool/acct-02"
		})
		// acct-03 satisfies the visibility stat but is NOT a mountpoint: a dead
		// mirror whose underlying dir shadows base's entries. It must read dead
		// — a false Live here would permanently mask a dead mirror from the
		// driver's remount logic.
		fakeMountAlive(t, func(base, dir string) bool {
			return base == "/pool/base" && (dir == "/pool/acct-01" || dir == "/pool/acct-03")
		})
		resp := s.dispatch(Request{Op: OpList})
		if !resp.OK {
			t.Fatalf("list failed: %q", resp.Error)
		}
		want := []MountInfo{
			{Dir: "/pool/acct-01", Base: "/pool/base", Live: true},
			{Dir: "/pool/acct-02", Base: "/pool/base", Live: false},
			{Dir: "/pool/acct-03", Base: "/pool/base", Live: false},
		}
		if !reflect.DeepEqual(resp.Mounts, want) {
			t.Fatalf("list = %+v, want %+v", resp.Mounts, want)
		}
	})
	t.Run("empty registry lists nothing", func(t *testing.T) {
		resp := newHandlerServer(&fakeHost{}).dispatch(Request{Op: OpList})
		if !resp.OK || len(resp.Mounts) != 0 {
			t.Fatalf("list = %+v (ok %v), want empty OK", resp.Mounts, resp.OK)
		}
	})
}

// shrinkLiveProbeTimeout shortens the liveness probe bound for one test,
// restoring it after. Same no-parallel rule as fakeMounted.
func shrinkLiveProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := liveProbeTimeout
	liveProbeTimeout = d
	t.Cleanup(func() { liveProbeTimeout = prev })
}

// releaseProbes closes block (waking any probe goroutine wedged on it) and
// waits for every in-flight liveness probe to drain. Register it AFTER the
// seam fakes: probe goroutines read the seam package vars, so they must be
// gone — observed through the probe table's lock, which gives the
// happens-before edge — before the seam-restoring cleanups (registered
// earlier, run later) write the vars back.
func releaseProbes(t *testing.T, block chan struct{}) {
	t.Helper()
	close(block)
	deadline := time.Now().Add(5 * time.Second)
	for liveProbes.Inflight() > 0 {
		if time.Now().After(deadline) {
			t.Error("in-flight liveness probes never drained")
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHandleListWedgedMirrorBounded pins the bounded liveness probe: fuse-t's
// NFS backend has no soft/timeout knobs, so a wedged mirror's stats block
// forever. One wedged mirror must read Live=false within the probe bound —
// never hang List — while its healthy sibling still reads true; a second List
// joins the still-stuck probe instead of stacking another goroutine per
// refresh. Without the bound, a single wedged mirror would blow the client's
// List deadline and un-vouch EVERY fuse account pool-wide.
func TestHandleListWedgedMirrorBounded(t *testing.T) {
	shrinkLiveProbeTimeout(t, 100*time.Millisecond)
	s := newHandlerServer(&fakeHost{})
	s.registry["/pool/acct-01"] = mountRow{Base: "/pool/base"}
	s.registry["/pool/acct-02"] = mountRow{Base: "/pool/base"}

	block := make(chan struct{})
	var wedgedStats atomic.Int32
	fakeMounted(t, func(string) bool { return true })
	fakeMountAlive(t, func(_, dir string) bool {
		if dir == "/pool/acct-01" {
			wedgedStats.Add(1)
			<-block // the wedged mirror: this stat never returns
		}
		return true
	})
	t.Cleanup(func() { releaseProbes(t, block) })

	want := []MountInfo{
		{Dir: "/pool/acct-01", Base: "/pool/base", Live: false},
		{Dir: "/pool/acct-02", Base: "/pool/base", Live: true},
	}
	start := time.Now()
	resp := s.dispatch(Request{Op: OpList})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("list took %s, want bounded by the probe timeout", elapsed)
	}
	if !resp.OK {
		t.Fatalf("list failed: %q", resp.Error)
	}
	if !reflect.DeepEqual(resp.Mounts, want) {
		t.Fatalf("list = %+v, want %+v", resp.Mounts, want)
	}

	// The second List must join the still-stuck probe — exactly one wedged
	// stat in flight — and still report the wedged entry dead.
	resp = s.dispatch(Request{Op: OpList})
	if !resp.OK || !reflect.DeepEqual(resp.Mounts, want) {
		t.Fatalf("second list = %+v (ok %v), want %+v", resp.Mounts, resp.OK, want)
	}
	if got := wedgedStats.Load(); got != 1 {
		t.Errorf("wedged dir probed %d times, want 1 (joiners must not stack stuck goroutines)", got)
	}
}

// TestHandleMountWedgedRegisteredMirrorRemounted pins the same bound on
// handleMount's idempotency check: a registered mirror whose liveness stats
// wedge reads dead within the bound and takes the designed recovery — the
// provider's bounded forced teardown, then a remount — instead of hanging the
// handler past the op deadline.
func TestHandleMountWedgedRegisteredMirrorRemounted(t *testing.T) {
	shrinkLiveProbeTimeout(t, 100*time.Millisecond)
	fake := &fakeHost{}
	s := newHandlerServer(fake)
	s.registry["/pool/acct-01"] = mountRow{Base: "/pool/base"}

	block := make(chan struct{})
	fakeMounted(t, func(string) bool { return true })
	fakeMountAlive(t, func(string, string) bool { <-block; return true })
	t.Cleanup(func() { releaseProbes(t, block) })

	resp := s.dispatch(Request{Op: OpMount, Base: "/pool/base", Dir: "/pool/acct-01"})
	if !resp.OK {
		t.Fatalf("mount over a wedged registered mirror = %+v, want the teardown+remount recovery", resp)
	}
	setups, tears := fake.calls()
	if !reflect.DeepEqual(tears, []hostCall{{"/pool/base", "/pool/acct-01"}}) {
		t.Errorf("Teardown calls = %v, want the wedged mirror torn down", tears)
	}
	if !reflect.DeepEqual(setups, []hostCall{{"/pool/base", "/pool/acct-01"}}) {
		t.Errorf("Setup calls = %v, want the mirror remounted", setups)
	}
	assertClaimsReleased(t, s, 0)
}

func TestHandleHealthAndProbe(t *testing.T) {
	s := newHandlerServer(&fakeHost{})

	health := s.dispatch(Request{Op: OpHealth})
	if !health.OK || health.Version != version.String() {
		t.Fatalf("health = %+v, want OK with version %q", health, version.String())
	}

	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || resp.FuseOK {
		t.Fatalf("probe with nil Probe = %+v, want OK with FuseOK=false", resp)
	}
	s.Probe = func() bool { return true }
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || !resp.FuseOK {
		t.Fatalf("probe = %+v, want FuseOK=true", resp)
	}
	s.Probe = func() bool { return false }
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || resp.FuseOK {
		t.Fatalf("probe = %+v, want FuseOK=false", resp)
	}
}

// TestServerMountUnmountHappyPath drives the holder end-to-end over a real
// unix socket: mount registers, a repeat mount of the live pair is idempotent
// (no second Setup), list reports the entry live, unmount tears it down, and
// shutdown sweeps clean and exits the server.
func TestServerMountUnmountHappyPath(t *testing.T) {
	fake := &fakeHost{}
	installLiveSeams(t, fake)
	_, cl, done, _ := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dir := filepath.Join(root, "acct-01")

	before := time.Now().Unix()
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("repeat mount should be idempotent OK, got %v", err)
	}
	if setups, _ := fake.calls(); !reflect.DeepEqual(setups, []hostCall{{base, dir}}) {
		t.Fatalf("Setup calls = %v, want exactly one for %s (repeat mount of a live pair must not re-Setup)", setups, dir)
	}

	mounts, err := cl.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(mounts) != 1 {
		t.Fatalf("list = %+v, want one entry", mounts)
	}
	m := mounts[0]
	if m.Dir != dir || m.Base != base || !m.Live {
		t.Fatalf("list entry = %+v, want live %s <- %s", m, dir, base)
	}
	if m.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1 for the holder's first mount of %s", m.Epoch, dir)
	}
	if m.MountedAt < before || m.MountedAt > time.Now().Unix() {
		t.Errorf("MountedAt = %d, want within [%d, now]", m.MountedAt, before)
	}

	if err := cl.Unmount(base, dir); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("Teardown calls = %v, want exactly one for %s", tears, dir)
	}
	if mounts, err := cl.List(); err != nil || len(mounts) != 0 {
		t.Fatalf("list after unmount = %+v (err %v), want empty", mounts, err)
	}

	failed, err := cl.Shutdown()
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("shutdown reported failed dirs %+v, want none", failed)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after OpShutdown")
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after shutdown")
	}
}

func TestShutdownReportsFailedDirs(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "base")
	dirA := filepath.Join(root, "acct-01")
	dirB := filepath.Join(root, "acct-02")

	fake := &fakeHost{teardownFn: func(_, dir string) error {
		if dir == dirA {
			return fmt.Errorf("%w: %s; refusing to treat it as torn down", overlay.ErrUnmountWedged, dir)
		}
		return nil
	}}
	_, cl, done, _ := startServer(t, fake)

	if err := cl.Mount(base, dirA); err != nil {
		t.Fatalf("mount A: %v", err)
	}
	if err := cl.Mount(base, dirB); err != nil {
		t.Fatalf("mount B: %v", err)
	}

	failed, err := cl.Shutdown()
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if want := []MountInfo{{Dir: dirA, Base: base, Live: true}}; !reflect.DeepEqual(failed, want) {
		t.Fatalf("shutdown failed dirs = %+v, want %+v", failed, want)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after OpShutdown")
	}
	// Both dirs swept exactly once; the final post-drain sweep must not retry
	// the wedged dir (its registry row is already gone).
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dirA}, {base, dirB}}) {
		t.Fatalf("Teardown calls = %v, want each dir exactly once in dir order", tears)
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after shutdown")
	}
}

// TestRunCtxCancelSweepsMounts is the SIGTERM-equivalent path:
// signal.NotifyContext wraps the ctx Run is given, so cancelling it exercises
// the same exit: stop accepting, drain, unmount everything, release socket.
func TestRunCtxCancelSweepsMounts(t *testing.T) {
	fake := &fakeHost{}
	_, cl, done, cancel := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dir := filepath.Join(root, "acct-01")
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit on ctx cancel")
	}
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("ctx cancel must sweep mounts down; Teardown calls = %v", tears)
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after ctx-cancel sweep")
	}
}

func TestSecondRunRefusedAgainstLiveHolder(t *testing.T) {
	a, cl, _, _ := startServer(t, &fakeHost{})

	b := &Server{Socket: a.Socket, Host: &fakeHost{}, Log: log.New(io.Discard, "", 0)}
	err := b.Run(context.Background())
	if err == nil {
		t.Fatal("second holder must refuse to start against a live socket")
	}
	if !strings.Contains(err.Error(), "refusing to start") {
		t.Fatalf("refusal error = %q, want it to say it is refusing to start", err)
	}
	if !strings.Contains(err.Error(), version.String()) {
		t.Fatalf("refusal error = %q, want it to name the live holder's version %q", err, version.String())
	}

	// The loser must not have disturbed the winner: socket intact, still serving.
	if ver, herr := cl.Health(); herr != nil || ver != version.String() {
		t.Fatalf("first holder unhealthy after refused start: version %q, err %v", ver, herr)
	}
}

func TestStaleSocketRemovedAndRebound(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")

	// Manufacture a stale socket: bind, keep the file on close, close.
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket); err != nil {
		t.Fatalf("precondition: stale socket file should remain after close: %v", err)
	}

	_, cl, _, _ := startServerAt(t, &fakeHost{}, socket)
	if ver, err := cl.Health(); err != nil || ver != version.String() {
		t.Fatalf("holder over a reclaimed stale socket: version %q, err %v", ver, err)
	}
}

// TestRunRefusedWhileLockHeld pins the flock that closes the start race: a
// holder that cannot take Socket+".lock" must refuse WITHOUT touching the
// socket path — its os.Remove on a believed-stale socket is exactly the
// hazard the lock exists to prevent.
func TestRunRefusedWhileLockHeld(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	// flock contends between two open file descriptions even in one process,
	// so holding it here stands in for a concurrently starting holder that won
	// the lock but has not bound its socket yet.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	s := &Server{Socket: socket, Host: &fakeHost{}, Log: log.New(io.Discard, "", 0)}
	runErr := s.Run(context.Background())
	if runErr == nil || !strings.Contains(runErr.Error(), "refusing to start") {
		t.Fatalf("Run with the holder lock held = %v, want a refusing-to-start error", runErr)
	}
	if _, statErr := os.Stat(socket); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("a losing holder must not create (or have removed) the socket; stat err = %v", statErr)
	}
}

// TestCrashedHolderLockAndSocketReclaimed: a crashed holder leaves both its
// lock file and its socket file behind. The flock died with the process, so a
// fresh holder must reclaim both.
func TestCrashedHolderLockAndSocketReclaimed(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	if err := os.WriteFile(socket+".lock", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	_, cl, _, _ := startServerAt(t, &fakeHost{}, socket)
	if ver, err := cl.Health(); err != nil || ver != version.String() {
		t.Fatalf("holder over a crashed holder's leavings: version %q, err %v", ver, err)
	}
}

func TestRunNilHostRefused(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	s := &Server{Socket: socket}
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cannot host fuse mounts") {
		t.Fatalf("Run with nil Host = %v, want a loud cannot-host refusal", err)
	}
	if _, statErr := os.Stat(socket); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("nil-Host Run must not create the socket; stat err = %v", statErr)
	}
}

func TestConcurrentSameDirMountsSerialize(t *testing.T) {
	fake := &fakeHost{}
	entered := make(chan string, 2)
	release := make(chan struct{})
	fake.setupFn = func(_, dir string) error {
		entered <- dir
		<-release
		return nil
	}
	installLiveSeams(t, fake)
	_, cl, _, _ := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dir := filepath.Join(root, "acct-01")

	first := make(chan error, 1)
	go func() { first <- cl.Mount(base, dir) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first mount never reached Setup")
	}

	// The first mount is parked inside Setup holding the dir's claim, so a
	// second same-dir mount must bounce as busy without reaching the provider.
	err := cl.Mount(base, dir)
	if err == nil {
		t.Fatal("same-dir mount during an in-flight mount must be refused busy")
	}
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("second mount err = %v, want errors.Is ErrBusy", err)
	}
	if !strings.Contains(err.Error(), "busy") {
		t.Fatalf("second mount err = %v, want a busy refusal", err)
	}
	if errors.Is(err, ErrMountFailed) || errors.Is(err, ErrTCCDenied) || errors.Is(err, ErrForeignMount) {
		t.Fatalf("busy must not carry a failure class: %v", err)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first mount: %v", err)
	}
	if setups, _ := fake.calls(); len(setups) != 1 {
		t.Fatalf("Setup ran %d times, want exactly 1 — the busy op must never reach the provider", len(setups))
	}
	// The claim is back: the same mount now lands on the idempotent path.
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount after claim release: %v", err)
	}
	if setups, _ := fake.calls(); len(setups) != 1 {
		t.Fatalf("Setup ran %d times after idempotent re-mount, want still 1", len(setups))
	}
}

func TestConcurrentDifferentDirMountsProceed(t *testing.T) {
	fake := &fakeHost{}
	entered := make(chan string, 2)
	release := make(chan struct{})
	fake.setupFn = func(_, dir string) error {
		entered <- dir
		<-release
		return nil
	}
	_, cl, _, _ := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dirA := filepath.Join(root, "acct-01")
	dirB := filepath.Join(root, "acct-02")

	errs := make(chan error, 2)
	go func() { errs <- cl.Mount(base, dirA) }()
	go func() { errs <- cl.Mount(base, dirB) }()

	// Neither Setup has been released, so seeing both enter proves the two
	// dirs mount concurrently; a serialized holder would never produce the
	// second entry.
	inFlight := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case d := <-entered:
			inFlight[d] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v reached Setup; different dirs must mount concurrently", inFlight)
		}
	}
	if !inFlight[dirA] || !inFlight[dirB] {
		t.Fatalf("in-flight Setups = %v, want both %s and %s", inFlight, dirA, dirB)
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("mount: %v", err)
		}
	}
	mounts, err := cl.List()
	if err != nil || len(mounts) != 2 {
		t.Fatalf("list = %+v (err %v), want both mounts registered", mounts, err)
	}
}

func TestBadRequestsOverTheWire(t *testing.T) {
	_, cl, _, _ := startServer(t, &fakeHost{})

	t.Run("malformed JSON gets an error response, not a hangup", func(t *testing.T) {
		conn, err := net.DialTimeout("unix", cl.Socket, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.WriteString(conn, "{this is not json}\n"); err != nil {
			t.Fatal(err)
		}
		var resp Response
		if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
			t.Fatalf("no response to malformed JSON: %v", err)
		}
		if resp.OK {
			t.Fatal("malformed JSON must not be OK")
		}
		if !strings.Contains(resp.Error, "bad request") {
			t.Errorf("Error = %q, want a bad-request message", resp.Error)
		}
		if resp.Proto != MountProtoVersion {
			t.Errorf("Proto = %d, want %d on every response", resp.Proto, MountProtoVersion)
		}
	})

	t.Run("unknown op reads as not-supported, never as holder failure", func(t *testing.T) {
		resp, err := cl.do(Request{Op: Op("balance-quota")}, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if resp.OK {
			t.Fatal("unknown op must not be OK")
		}
		// Drivers detect not-supported by this exact prefix (see the package
		// compatibility policy), so it is part of the frozen surface.
		if resp.Error != "unknown op: balance-quota" {
			t.Errorf("Error = %q, want %q", resp.Error, "unknown op: balance-quota")
		}
		if resp.ErrClass != "" {
			t.Errorf("unknown op must not carry an error class, got %q", resp.ErrClass)
		}
		if resp.Proto != MountProtoVersion {
			t.Errorf("Proto = %d, want %d on every response", resp.Proto, MountProtoVersion)
		}
	})
}

// listOne dispatches OpList and returns its single entry.
func listOne(t *testing.T, s *Server) MountInfo {
	t.Helper()
	resp := s.dispatch(Request{Op: OpList})
	if !resp.OK || len(resp.Mounts) != 1 {
		t.Fatalf("list = %+v, want OK with one entry", resp)
	}
	return resp.Mounts[0]
}

// TestListReportsEpochMountedAt pins the additive List fields: Epoch starts at
// 1, bumps on remount, and MountedAt stamps the current mount.
func TestListReportsEpochMountedAt(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	installLiveSeams(t, fake)
	s := newHandlerServer(fake)

	before := time.Now().Unix()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir}); !resp.OK {
		t.Fatalf("mount: %+v", resp)
	}
	m := listOne(t, s)
	if !m.Live {
		t.Errorf("list entry = %+v, want live", m)
	}
	if m.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1 for the holder's first mount", m.Epoch)
	}
	if m.MountedAt < before || m.MountedAt > time.Now().Unix() {
		t.Errorf("MountedAt = %d, want within [%d, now]", m.MountedAt, before)
	}
	first := m.MountedAt

	// Kill the mirror so the ensure-mounted path remounts it: the epoch must
	// bump and MountedAt must restamp.
	fake.setLive(dir, false)
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir}); !resp.OK {
		t.Fatalf("remount: %+v", resp)
	}
	m = listOne(t, s)
	if m.Epoch != 2 {
		t.Errorf("Epoch after remount = %d, want 2", m.Epoch)
	}
	if m.MountedAt < first || m.MountedAt > time.Now().Unix() {
		t.Errorf("MountedAt after remount = %d, want within [%d, now]", m.MountedAt, first)
	}
	if !m.Live {
		t.Errorf("list entry after remount = %+v, want live", m)
	}
}
