package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/version"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
)

// fakeHost reports kernel-truth State: a registered dir reads Live=false in List.
type fakeHost struct{}

func (fakeHost) Setup(fusekit.MountSpec) error { return nil }
func (fakeHost) Teardown(_, _ string) error    { return nil }
func (fakeHost) State(base, dir string) (mounted, alive bool) {
	return overlay.Mounted(dir), overlay.MountAlive(base, dir)
}

// TeardownDone satisfies the required TeardownPender capability; this host never
// pends a graceful unmount, so it returns nil.
func (fakeHost) TeardownDone(string) <-chan struct{} { return nil }

// startFakeHolder runs a real mountd.Server on a short /tmp socket (macOS
// caps sun_path at 104 bytes).
func startFakeHolder(t *testing.T) *mountd.Client {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-hold")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	srv := &mountd.Server{
		Socket:   filepath.Join(sockDir, "m.sock"),
		Host:     fakeHost{},
		Version:  version.String(),
		LeaseDir: filepath.Join(sockDir, "leases"),
		Log:      log.New(io.Discard, "", 0),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fake holder did not stop")
		}
	})
	cl := &mountd.Client{Socket: srv.Socket, Owner: pool.HolderOwner}
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("fake holder socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cl
}

func startCannedHolder(t *testing.T, mounts []mountd.MountInfo) string {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-can")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go serveCannedHolder(ln, mounts)
	return socket
}

func serveCannedHolder(ln net.Listener, mounts []mountd.MountInfo) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		var req mountd.Request
		resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: version.String(), Features: mountd.HolderFeatures}
		if err := json.NewDecoder(conn).Decode(&req); err == nil && req.Op == mountd.OpList {
			resp.Mounts = mounts
		}
		_ = json.NewEncoder(conn).Encode(resp)
		_ = conn.Close()
	}
}

func startCapabilityHolder(t *testing.T, mounts []mountd.MountInfo, probeErrClass, probeErr string) string {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-cap")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "m.sock")
	ln, err := net.Listen("unix", socket)
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
			var req mountd.Request
			resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: version.String(), Features: mountd.HolderFeatures}
			if err := json.NewDecoder(conn).Decode(&req); err == nil {
				switch req.Op {
				case mountd.OpList:
					resp.Mounts = mounts
				case mountd.OpProbe:
					if probeErrClass == "" {
						resp.FuseOK = true
					} else {
						resp.ErrClass = probeErrClass
						resp.Error = probeErr
					}
				}
			}
			_ = json.NewEncoder(conn).Encode(resp)
			_ = conn.Close()
		}
	}()
	return socket
}

// TestHolderStateRefresh pins both refresh arms: a dead socket zeroes the
// cache; a live holder stamps version and per-dir kernel liveness.
func TestHolderStateRefresh(t *testing.T) {
	deadDir, err := os.MkdirTemp("/tmp", "ccp-dead")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(deadDir) })

	h := &holderState{healthy: true, version: "x", mounts: map[string]bool{"/pool/acct-01": true}}
	h.refresh(mountd.NewClient(filepath.Join(deadDir, "no.sock")))
	if h.ready("/pool/acct-01") {
		t.Fatal("unreachable holder left a trusted mount entry")
	}
	if ws := h.wireStatus(); ws.Version != "" || ws.Mounts != 0 {
		t.Fatalf("unreachable holder wire view = %+v, want zeroed", ws)
	}

	cl := startFakeHolder(t)
	base, dir := t.TempDir(), t.TempDir()
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("register fake mount: %v", err)
	}
	h.refresh(cl)
	if ws := h.wireStatus(); ws.Version != version.String() {
		t.Fatalf("live holder wire view = %+v, want version %q", ws, version.String())
	}
	if h.ready(dir) {
		t.Fatal("cache vouched for a registered but kernel-dead mount")
	}
	h.mu.Lock()
	live, ok := h.mounts[dir]
	h.mu.Unlock()
	if !ok || live {
		t.Fatalf("mounts[%s] = %v ok=%v, want a present dead entry", dir, live, ok)
	}
}

// startGatedListHolder parks List until release, letting a test land an
// in-place cache update inside refresh's Health→List window.
func startGatedListHolder(t *testing.T, mounts []mountd.MountInfo) (socket string, listEntered <-chan struct{}, release func()) {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-gate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	socket = filepath.Join(sockDir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	entered := make(chan struct{})
	releaseCh := make(chan struct{})
	var enterOnce, relOnce sync.Once
	release = func() { relOnce.Do(func() { close(releaseCh) }) }
	t.Cleanup(release) // never leave the serve goroutine parked
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			var req mountd.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close()
				continue
			}
			resp := mountd.Response{OK: true, Proto: mountd.MountProtoVersion, Version: version.String(), Features: mountd.HolderFeatures}
			if req.Op == mountd.OpList {
				enterOnce.Do(func() { close(entered) })
				<-releaseCh
				resp.Mounts = mounts
			}
			_ = json.NewEncoder(conn).Encode(resp)
			_ = conn.Close()
		}
	}()
	return socket, entered, release
}

// TestHolderStateRefreshDiscardsSnapshotRacedByInPlaceUpdate pins the
// lost-update guard: a mid-refresh in-place update is newer truth.
func TestHolderStateRefreshDiscardsSnapshotRacedByInPlaceUpdate(t *testing.T) {
	t.Run("noteMounted survives a stale pre-mount List", func(t *testing.T) {
		socket, entered, release := startGatedListHolder(t, nil)
		h := &holderState{}
		cl := mountd.NewClient(socket)
		done := make(chan struct{})
		go func() { defer close(done); h.refresh(cl) }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh never reached List")
		}
		h.noteMounted("/pool/acct-01") // mountFuse completes mid-refresh
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not return")
		}
		if !h.ready("/pool/acct-01") {
			t.Fatal("stale pre-mount List clobbered a noteMounted mirror")
		}
		h.mu.Lock()
		stamped := !h.refreshedAt.IsZero()
		h.mu.Unlock()
		if stamped {
			t.Fatal("discarded snapshot stamped refreshedAt, suppressing refreshIfStale")
		}
		h.refresh(cl)
		if h.ready("/pool/acct-01") {
			t.Fatal("unraced refresh did not install the polled (empty) snapshot")
		}
	})

	t.Run("markUnhealthy survives a stale healthy List", func(t *testing.T) {
		socket, entered, release := startGatedListHolder(t, []mountd.MountInfo{{Dir: "/pool/acct-01", Base: "/b", Live: true}})
		h := &holderState{}
		cl := mountd.NewClient(socket)
		done := make(chan struct{})
		go func() { defer close(done); h.refresh(cl) }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh never reached List")
		}
		h.markUnhealthy() // a replace's shutdown lands mid-refresh
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not return")
		}
		if h.ready("/pool/acct-01") {
			t.Fatal("stale pre-shutdown List re-vouched mirrors a replace swept")
		}
		if healthy, _ := h.view(); healthy {
			t.Fatal("stale snapshot resurrected a holder marked unhealthy")
		}
	})
}

// TestHolderStateHeldDead pins held-dead: holder-listed and (not Live or
// deep-wedged); the wedge verdict is the daemon's own, not a holder field.
func TestHolderStateHeldDead(t *testing.T) {
	cases := map[string]struct {
		mounts     []mountd.MountInfo
		wedge      bool
		unhealthy  bool
		wantDead   bool
		wantWedged bool
	}{
		"shallow-live but deep-wedged is the wedge signature": {
			mounts:     []mountd.MountInfo{{Dir: "/pool/acct-01", Base: "/b", Live: true}},
			wedge:      true,
			wantDead:   true,
			wantWedged: true,
		},
		"present and not Live without a wedge verdict is plain dead": {
			// Models an out-of-band `umount -f` or a dead fuse-t worker.
			mounts:   []mountd.MountInfo{{Dir: "/pool/acct-01", Base: "/b", Live: false}},
			wantDead: true,
		},
		"present and live is healthy": {
			mounts: []mountd.MountInfo{{Dir: "/pool/acct-01", Base: "/b", Live: true}},
		},
		"absent dir (TCC-blocked or never mounted) is not held-dead": {
			mounts: []mountd.MountInfo{{Dir: "/pool/other", Base: "/b", Live: false}},
		},
		"unreachable holder never reads held-dead": {
			mounts:    []mountd.MountInfo{{Dir: "/pool/acct-01", Base: "/b", Live: true}},
			wedge:     true,
			unhealthy: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var h holderState
			h.refresh(mountd.NewClient(startCannedHolder(t, tc.mounts)))
			if tc.wedge {
				h.markDeepWedged("/pool/acct-01")
			}
			if tc.unhealthy {
				h.markUnhealthy()
			}
			dead, wedged := h.heldDead("/pool/acct-01")
			if dead != tc.wantDead || wedged != tc.wantWedged {
				t.Fatalf("heldDead = (%v, %v), want (%v, %v)", dead, wedged, tc.wantDead, tc.wantWedged)
			}
		})
	}
}

// TestHolderStateShallowDeadStrikes pins the shallow-dead strike debounce:
// noteMounted, resetShallowDead, and markUnhealthy each reset the count.
func TestHolderStateShallowDeadStrikes(t *testing.T) {
	const dir = "/pool/acct-01"
	var h holderState
	if got := h.recordShallowDead(dir); got != 1 {
		t.Fatalf("first strike = %d, want 1", got)
	}
	if got := h.recordShallowDead(dir); got != 2 {
		t.Fatalf("second strike = %d, want 2", got)
	}
	for _, clear := range []struct {
		name string
		fn   func()
	}{
		{"noteMounted", func() { h.noteMounted(dir) }},
		{"resetShallowDead", func() { h.resetShallowDead(dir) }},
		{"markUnhealthy", func() { h.markUnhealthy() }},
	} {
		h.recordShallowDead(dir)
		clear.fn()
		if got := h.recordShallowDead(dir); got != 1 {
			t.Fatalf("strike after %s = %d, want a fresh 1", clear.name, got)
		}
		h.resetShallowDead(dir)
	}
}

// TestRefreshPrunesDepartedVerdicts pins that refresh drops probe state for
// departed dirs: a verdict cannot outlive its mount.
func TestRefreshPrunesDepartedVerdicts(t *testing.T) {
	const kept, gone = "/pool/acct-01", "/pool/acct-02"
	var h holderState
	for _, dir := range []string{kept, gone} {
		h.markDeepWedged(dir) // seeds deep + lastProbed
		h.recordShallowDead(dir)
	}
	cl := mountd.NewClient(startCannedHolder(t, []mountd.MountInfo{
		{Dir: kept, Base: "/base", Live: false},
	}))
	h.refresh(cl)

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.led.peek(fuseDeepWedgePolicy, gone) != nil {
		t.Error("refresh kept the departed dir's deep verdict")
	}
	if h.led.peek(fuseShallowDeadPolicy, gone) != nil {
		t.Error("refresh kept the departed dir's shallow strike")
	}
	if _, ok := h.lastProbed[gone]; ok {
		t.Error("refresh kept the departed dir's probe clock")
	}
	if h.led.peek(fuseDeepWedgePolicy, kept) == nil {
		t.Error("refresh pruned the still-listed dir's deep verdict")
	}
	if h.led.peek(fuseShallowDeadPolicy, kept) == nil {
		t.Error("refresh pruned the still-listed dir's shallow strike")
	}
	if _, ok := h.lastProbed[kept]; !ok {
		t.Error("refresh pruned the still-listed dir's probe clock")
	}
}

// TestHolderStateNoteMounted pins the fresh-mount fast path: trusted before
// any refresh.
func TestHolderStateNoteMounted(t *testing.T) {
	var h holderState
	if h.ready("/d") {
		t.Fatal("zero cache vouched for a dir")
	}
	h.noteMounted("/d")
	if !h.ready("/d") {
		t.Fatal("fresh mount not trusted before the first refresh")
	}
}

// TestHolderStateRefreshDegraded pins the third refresh arm: Health up but
// List failing keeps the version, reads not-healthy, and fails mounts closed.
func TestHolderStateRefreshDegraded(t *testing.T) {
	const ver = "v1.2.3 (abc1234)"
	var h holderState
	socket := startDegradedHolder(t, ver)

	h.refresh(mountd.NewClient(socket))

	healthy, got := h.view()
	if healthy || got != ver {
		t.Fatalf("view = (%v, %q), want (false, %q) for a degraded holder", healthy, got, ver)
	}
	if h.ready("/pool/acct-01") {
		t.Fatal("a degraded holder vouched for a mount; mounts must fail closed")
	}
}

// TestHolderStateMarkDegraded pins markDegraded: keeps the version, fails
// mounts closed, and bumps gen so a racing snapshot is discarded.
func TestHolderStateMarkDegraded(t *testing.T) {
	h := &holderState{
		healthy: true,
		version: "old",
		mounts:  map[string]bool{"/d": true},
	}
	g := h.gen

	h.markDegraded("v9")

	healthy, ver := h.view()
	if healthy || ver != "v9" {
		t.Fatalf("view = (%v, %q), want (false, v9)", healthy, ver)
	}
	if h.ready("/d") {
		t.Fatal("markDegraded left a vouched mount; mounts must be nil")
	}
	if h.gen == g {
		t.Fatal("markDegraded did not bump gen; a racing snapshot would not be discarded")
	}
}

// TestMarkUnhealthyResetsCache pins that an unreachable holder wipes the cache:
// healthy flips false, the version blanks (the wire "gone" signal), and the mounts
// map and verdict ledgers are dropped so a respawned holder starts clean. Carcass
// recovery is the shared holder's job now, so no consumer-side sweep is fired.
func TestMarkUnhealthyResetsCache(t *testing.T) {
	h := &holderState{healthy: true, version: "v1.0.0", mounts: map[string]bool{"/pool/acct-01": true}}
	h.markUnhealthy()
	if h.healthy || h.version != "" || len(h.mounts) != 0 {
		t.Fatalf("markUnhealthy left cache = {healthy:%v version:%q mounts:%v}, want a wiped cache", h.healthy, h.version, h.mounts)
	}
}
