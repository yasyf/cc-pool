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

	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/version"
)

// startFakeHolder runs a real mountd.Server backed by the daemon's fake fuse
// provider on a short /tmp socket (macOS caps sun_path at 104 bytes),
// returning a client for it.
func startFakeHolder(t *testing.T, fake *fakeFuseProv) *mountd.Client {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "ccp-hold")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	srv := &mountd.Server{
		Socket:  filepath.Join(sockDir, "m.sock"),
		Host:    fake,
		Version: version.String(),
		Log:     log.New(io.Discard, "", 0),
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
	cl := mountd.NewClient(srv.Socket)
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("fake holder socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cl
}

// startCannedHolder serves the mountd wire protocol with canned Health/List
// answers over a short /tmp socket, returning the socket path. Unlike
// startFakeHolder — a real mountd.Server whose List reports kernel liveness,
// false for any test dir — it lets daemon tests dictate per-dir Live without
// real mounts.
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

// serveCannedHolder answers the mountd wire protocol on ln — our version on
// every op, the given List — until the listener closes. Shared by
// startCannedHolder and spawnRecorder (which binds at an exact socket path to
// stand in for a freshly spawned holder).
func serveCannedHolder(ln net.Listener, mounts []mountd.MountInfo) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed: defined exit
		}
		var req mountd.Request
		resp := mountd.Response{OK: true, Version: version.String()}
		if err := json.NewDecoder(conn).Decode(&req); err == nil && req.Op == mountd.OpList {
			resp.Mounts = mounts
		}
		_ = json.NewEncoder(conn).Encode(resp)
		_ = conn.Close()
	}
}

// TestHolderStateRefresh pins both refresh arms: a dead socket marks the cache
// unhealthy and drops every mount entry (selection must stop trusting them —
// THE carcass input), and a live holder stamps its version and per-dir kernel
// liveness from List — truthfully Live=false for a registered mount whose dir
// is not really a mountpoint.
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
	if ws := h.wireStatus(); ws.Version != "" || ws.Mounts != 0 || ws.Skewed {
		t.Fatalf("unreachable holder wire view = %+v, want zeroed", ws)
	}

	cl := startFakeHolder(t, &fakeFuseProv{})
	base, dir := t.TempDir(), t.TempDir()
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("register fake mount: %v", err)
	}
	h.refresh(cl)
	if ws := h.wireStatus(); ws.Version != version.String() || ws.Skewed {
		t.Fatalf("live holder wire view = %+v, want version %q unskewed", ws, version.String())
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

// startGatedListHolder serves the mountd wire protocol with canned answers:
// Health replies immediately; List signals entered, then parks until release
// fires before replying with mounts. It lets a test land an in-place cache
// update inside refresh's Health→List RPC window.
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
				return // listener closed: defined exit
			}
			var req mountd.Request
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close()
				continue
			}
			resp := mountd.Response{OK: true, Version: version.String()}
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
// lost-update guard on THE cache the select path trusts (R2): an in-place
// update (noteMounted, markUnhealthy) landing while a refresh's Health+List
// snapshot is in flight is event truth newer than the snapshot, so the
// snapshot is discarded — never installed over it.
func TestHolderStateRefreshDiscardsSnapshotRacedByInPlaceUpdate(t *testing.T) {
	t.Run("noteMounted survives a stale pre-mount List", func(t *testing.T) {
		socket, entered, release := startGatedListHolder(t, nil) // List: zero mounts
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
		// The discarded snapshot must not have stamped refreshedAt: that would
		// rate-limit-suppress mountReady's refreshIfStale backstop for a floor.
		h.mu.Lock()
		stamped := !h.refreshedAt.IsZero()
		h.mu.Unlock()
		if stamped {
			t.Fatal("discarded snapshot stamped refreshedAt, suppressing refreshIfStale")
		}
		// An unraced refresh then installs polled truth over the noted mount.
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

// TestHolderStateHeldDead pins the held-dead discriminator on the cache: a dir
// is held-dead when it is PRESENT in the holder's last List but either not Live
// (shallow) or marked wedged by the daemon's own deep probe; the second return
// relays that wedge verdict. The List rows come through refresh (one mounts
// entry per registered mount, so a TCC-blocked or never-mounted dir the holder
// never registers is absent and can never read held-dead); the wedge verdict is
// the daemon's own, forced via markDeepWedged (the holder ships no wedge field).
func TestHolderStateHeldDead(t *testing.T) {
	cases := map[string]struct {
		mounts     []mountd.MountInfo
		wedge      bool // force the daemon's deep verdict wedged after refresh
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
			// An out-of-band `umount -f` or a dead fuse-t worker: the holder
			// names the dir but its shallow liveness probe fails.
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

// TestWireStatusCountsWedged pins the WedgedMounts surface: it counts the
// daemon's OWN deep-probe verdicts (not anything from the holder's List); a
// fresh mount clears its dir's verdict in place, and an unreachable holder
// counts zero.
func TestWireStatusCountsWedged(t *testing.T) {
	var h holderState
	h.refresh(mountd.NewClient(startCannedHolder(t, []mountd.MountInfo{
		{Dir: "/pool/a", Base: "/b", Live: true},
		{Dir: "/pool/b", Base: "/b", Live: true},
		{Dir: "/pool/c", Base: "/b", Live: true},
	})))
	h.markDeepWedged("/pool/a")
	h.markDeepWedged("/pool/b")
	if got := h.wireStatus().WedgedMounts; got != 2 {
		t.Fatalf("WedgedMounts = %d, want 2 of 3", got)
	}
	h.noteMounted("/pool/a")
	if got := h.wireStatus().WedgedMounts; got != 1 {
		t.Fatalf("WedgedMounts after a remount of one = %d, want 1", got)
	}
	h.markUnhealthy()
	if got := h.wireStatus().WedgedMounts; got != 0 {
		t.Fatalf("WedgedMounts after markUnhealthy = %d, want 0", got)
	}
}

// TestHolderStateNoteMounted pins the fresh-mount fast path: a successful
// mount is trusted before any refresh, and it clears recorded TCC guidance
// (the grant is per holder process, so one live mount proves it landed).
func TestHolderStateNoteMounted(t *testing.T) {
	var h holderState
	if h.ready("/d") {
		t.Fatal("zero cache vouched for a dir")
	}
	h.recordTCC("grant pending")
	h.noteMounted("/d")
	if !h.ready("/d") {
		t.Fatal("fresh mount not trusted before the first refresh")
	}
	if ws := h.wireStatus(); ws.TCCError != "" {
		t.Fatalf("TCC guidance survived a successful mount: %+v", ws)
	}
}

// TestHolderStateRefreshDegraded pins the third refresh arm: a holder that
// answers Health but whose List fails is DEGRADED, not unreachable — its
// version is kept (so the supervisor can tell a skewed degraded holder from an
// our-version one), its mounts fail closed (nil → ready false), and the bases
// registry survives (the pre-row guard reads it when mounts is nil).
func TestHolderStateRefreshDegraded(t *testing.T) {
	const ver = "v1.2.3 (abc1234)"
	h := &holderState{bases: map[string]string{"/pool/acct-01": "/base"}}
	deg := startDegradedHolder(t, ver, false)

	h.refresh(mountd.NewClient(deg.socket))

	healthy, degraded, got := h.viewState()
	if healthy || !degraded || got != ver {
		t.Fatalf("viewState = (%v, %v, %q), want (false, true, %q)", healthy, degraded, got, ver)
	}
	if h.ready("/pool/acct-01") {
		t.Fatal("a degraded holder vouched for a mount; mounts must fail closed")
	}
	if b := h.carriedBases(); b["/pool/acct-01"] != "/base" {
		t.Fatalf("bases = %v, want the pre-degraded registry preserved", b)
	}
}

// TestHolderStateMarkDegraded pins markDegraded directly: it keeps the version,
// fails the mounts closed, preserves bases, and bumps gen so a racing refresh
// snapshot is discarded.
func TestHolderStateMarkDegraded(t *testing.T) {
	h := &holderState{
		healthy: true,
		version: "old",
		mounts:  map[string]bool{"/d": true},
		bases:   map[string]string{"/d": "/b"},
	}
	g := h.gen

	h.markDegraded("v9")

	healthy, degraded, ver := h.viewState()
	if healthy || !degraded || ver != "v9" {
		t.Fatalf("viewState = (%v, %v, %q), want (false, true, v9)", healthy, degraded, ver)
	}
	if h.ready("/d") {
		t.Fatal("markDegraded left a vouched mount; mounts must be nil")
	}
	if h.carriedBases()["/d"] != "/b" {
		t.Fatal("markDegraded dropped bases; the registry must survive")
	}
	if h.gen == g {
		t.Fatal("markDegraded did not bump gen; a racing snapshot would not be discarded")
	}
}
