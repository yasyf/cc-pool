package mountd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/version"
)

// mounted and mountAlive are seams over the overlay kernel mountpoint checks
// (overlay.Mounted, overlay.MountAlive) so tests can fake mount state without
// real fuse mounts.
var (
	mounted    = overlay.Mounted
	mountAlive = overlay.MountAlive
)

// Server is the running mount holder. It owns a registry of the mounts IT
// established — the fuse provider's internal registry is private to the
// provider — and reports it through list with per-entry kernel liveness.
// Base is deliberately not a field: it arrives per-request, so the holder
// carries no desired state at all.
type Server struct {
	// Socket is the holder's unix socket path.
	Socket string
	// Host is the in-process fuse provider that hosts the mounts. nil means
	// this binary cannot host mounts; Run fails immediately and loudly.
	Host overlay.Provider
	// Probe answers OpProbe with a throwaway in-process capability mount
	// (capability + TCC grant are per-process, so it must run here). nil
	// reports false.
	Probe func() bool
	// Log receives per-op outcomes. nil defaults to stderr.
	Log *log.Logger

	// triggerShutdown cancels Run's context, ending the holder (OpShutdown).
	// It is set in Run before the accept loop starts; the go-statement that
	// spawns each handler establishes the happens-before, so handlers read it
	// without a lock.
	triggerShutdown context.CancelFunc

	// wg tracks connection handlers; Run waits for them to drain before the
	// final unmount-all sweep.
	wg sync.WaitGroup

	mu       sync.Mutex
	registry map[string]mountRow // dir -> the mount this holder established
	inflight map[string]bool     // dir -> a mount/unmount holds the dir mid-I/O
	// epochs is the per-dir (re)mount counter behind mountRow.Epoch. It lives
	// outside the registry so it survives the deregister between a dead
	// mirror's teardown and its remount — monotonic per dir for this holder
	// process's lifetime, never reset or deleted.
	epochs map[string]uint64
}

// mountRow is one registry entry: the base a dir mirrors, which (re)mount of
// the dir this holder is on, and when the current mount was established.
type mountRow struct {
	Base      string
	Epoch     uint64
	MountedAt time.Time
}

// Run binds the holder socket and serves until ctx is cancelled, the process
// is signalled (SIGTERM/SIGINT), or an OpShutdown lands. On the way out it
// stops accepting, drains in-flight handlers, then unmounts everything it
// owns — each teardown individually bounded by the provider's grace timers,
// per-dir outcomes logged.
func (s *Server) Run(ctx context.Context) error {
	if s.Host == nil {
		return errors.New("mountd: this binary cannot host fuse mounts; install the fuse build")
	}
	if s.Log == nil {
		s.Log = log.New(os.Stderr, "[ccp-mountd] ", log.LstdFlags)
	}
	s.initState()

	ln, lock, err := s.listen()
	if err != nil {
		return err
	}
	// The flock on lock is the cross-process guarantee that only this holder
	// may stale-check, remove, bind, or unlink the socket path. It must
	// outlive the listener (Close releases it), so this defer is registered
	// first and runs last.
	defer lock.Close()
	// closeListener unlinks the socket exactly once. *net.UnixListener.Close
	// unlinks the socket file and is NOT idempotent: a second Close (the late
	// deferred one, after a slow teardown) would delete a successor holder's
	// freshly-bound socket. The sync.Once pins the unlink to the first close,
	// at ctx-cancel time. No explicit os.Remove for the same reason.
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = ln.Close() }) }
	defer closeListener()

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// stop cancels ctx, so it doubles as the over-the-socket shutdown trigger
	// (OpShutdown). Set before the accept loop spawns any handler.
	s.triggerShutdown = stop

	s.Log.Printf("mountd %s started; socket=%s", version.String(), s.Socket)

	// Break the accept loop on shutdown.
	go func() {
		<-ctx.Done()
		closeListener()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			// Back off on a transient accept error (e.g. EMFILE) instead of
			// busy-spinning a core until the next shutdown.
			s.Log.Printf("accept: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handle(conn) }()
	}

	s.wg.Wait()
	// Handlers are drained, so every claim is free and this sweep cannot
	// contend. It also catches dirs an OpShutdown sweep reported busy and any
	// mounts that landed after that sweep's snapshot.
	s.unmountAll()
	s.Log.Printf("mountd stopped")
	return nil
}

// initState resets the registry, the in-flight gate, and the epoch counters.
// Run calls it before serving; handler-level tests call it to dispatch without
// a socket.
func (s *Server) initState() {
	s.registry = map[string]mountRow{}
	s.inflight = map[string]bool{}
	s.epochs = map[string]uint64{}
}

// listen binds the unix socket with 0600 perms. Unlike the daemon, the holder
// NEVER evicts a live peer — a live holder hosts mounts that claude sessions
// run on, and replacing it would rip those mounts out from under them. A
// socket file with no live listener behind it is stale: removed and rebound.
//
// An exclusive flock on Socket+".lock" — returned to Run, which holds it for
// the holder's lifetime — makes the stale-check/remove/bind sequence
// single-entrant across processes. Without it, two concurrently starting
// holders both see a dead socket, and the loser's os.Remove can unlink the
// winner's freshly-bound socket; worse, *net.UnixListener.Close unlinks by
// PATH, so the loser would delete the winner's live socket again at its own
// exit. The lock file itself is never removed: unlinking a held lock file
// would let a third holder flock a fresh inode while the old inode's lock is
// still held, reopening the race.
func (s *Server) listen() (net.Listener, *os.File, error) {
	// Only the socket's parent dir is needed here; deriving it from s.Socket
	// keeps tests off the real ~/.cc-pool.
	if err := os.MkdirAll(filepath.Dir(s.Socket), 0o700); err != nil {
		return nil, nil, fmt.Errorf("ensure socket dir: %w", err)
	}
	lock, err := os.OpenFile(s.Socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open holder lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if ver, herr := NewClient(s.Socket).Health(); herr == nil {
			return nil, nil, fmt.Errorf("a mount holder (%s) already serves %s; refusing to start", ver, s.Socket)
		}
		return nil, nil, fmt.Errorf("another mount holder owns %s.lock but does not answer health yet (it may still be starting); refusing to start", s.Socket)
	}
	// Defense in depth: the lock is ours, but never evict a peer that answers
	// health anyway.
	if ver, err := NewClient(s.Socket).Health(); err == nil {
		lock.Close()
		return nil, nil, fmt.Errorf("a mount holder (%s) already serves %s; refusing to start", ver, s.Socket)
	}
	_ = os.Remove(s.Socket) // stale socket: the lock is ours and nothing answered health
	ln, err := net.Listen("unix", s.Socket)
	if err != nil {
		lock.Close()
		return nil, nil, err
	}
	if err := os.Chmod(s.Socket, 0o600); err != nil {
		ln.Close()
		lock.Close()
		return nil, nil, err
	}
	return ln, lock, nil
}

// opDeadline bounds one connection by its op: probe performs a real throwaway
// mount, mount waits out the provider's bounded mount-or-timeout, unmount its
// bounded graceful-then-forced teardown, and shutdown sweeps every mount.
// Each deadline is coupled to its client timeout, which sits ABOVE it (Mount
// 25s/20s, Unmount 17s/15s, Shutdown 65s/60s) so the op deadline is the
// binding bound — a blown client deadline reads ErrHolderUnavailable and
// would mask the holder's real error class.
func opDeadline(op Op) time.Duration {
	switch op {
	case OpProbe, OpMount:
		return 20 * time.Second
	case OpUnmount:
		return 15 * time.Second
	case OpShutdown:
		return 60 * time.Second
	default:
		return 10 * time.Second
	}
}

// handle serves one connection: one request, one response.
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(opDeadline("")))
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResp(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	_ = conn.SetDeadline(time.Now().Add(opDeadline(req.Op)))
	writeResp(conn, s.dispatch(req))
}

func writeResp(conn net.Conn, r Response) {
	r.Proto = MountProtoVersion
	_ = json.NewEncoder(conn).Encode(r)
}

func (s *Server) dispatch(req Request) Response {
	switch req.Op {
	case OpHealth:
		return Response{OK: true, Version: version.String()}
	case OpProbe:
		return s.handleProbe()
	case OpMount:
		return s.handleMount(req)
	case OpUnmount:
		return s.handleUnmount(req)
	case OpList:
		return s.handleList()
	case OpShutdown:
		return s.handleShutdown()
	default:
		return Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

func (s *Server) handleProbe() Response {
	if s.Probe == nil {
		return Response{OK: true, FuseOK: false}
	}
	return Response{OK: true, FuseOK: s.Probe()}
}

// claim takes dir's in-flight gate: concurrent ops on the SAME dir serialize
// (the second gets a busy error) while different dirs proceed concurrently —
// the holder serves the daemon and N CLIs at once, and the provider's Setup
// has its own registry check-then-act window that two same-dir mounts would
// race. The claim — not the mutex — owns the dir across the provider I/O;
// release returns the gate.
func (s *Server) claim(dir string) (release func(), ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[dir] {
		return nil, false
	}
	s.inflight[dir] = true
	return func() {
		s.mu.Lock()
		delete(s.inflight, dir)
		s.mu.Unlock()
	}, true
}

// liveProbeTimeout bounds one kernel liveness probe (the mounted + mountAlive
// stats). fuse-t's NFS backend has no soft/timeout mount options, so a wedged
// mirror — the serving-path fault this error taxonomy was built around —
// blocks those stats indefinitely. An unanswered probe reads dead: the driver
// then routes the dir through the bounded forced-teardown remount path,
// instead of one wedged mirror hanging List (and un-vouching every healthy
// sibling when the client's deadline blows). It must stay under the client's
// 3s List deadline. A var, not a const, so tests can shrink it.
var liveProbeTimeout = 2 * time.Second

// mountState is one bounded probe's verdict: the two kernel-truth halves of
// mirror liveness (the device-id mountpoint check and base's contents showing
// through it).
type mountState struct {
	mounted bool
	alive   bool
}

// liveProbes joins concurrent bounded liveness stats per dir, package-wide:
// the holder's handlers and the client-side RemoteProvider both stat dirs
// that can wedge with their mirror, and a wedged dir must cost at most one
// stuck goroutine no matter how many callers ask.
var liveProbes overlay.StatProbes[mountState]

// probeMount reports dir's kernel mount state — mounted(dir), and base's
// contents visible through it — bounded by liveProbeTimeout (see
// overlay.StatProbes for the join/detach semantics). ok=false means the stats
// did not answer within the bound (a wedged mirror) and the caller must fail
// toward its safe direction: dead for liveness checks, still-mounted for
// foreign-mount refusals and teardown verification.
func probeMount(base, dir string) (st mountState, ok bool) {
	return liveProbes.Do(dir, liveProbeTimeout, func() mountState {
		m := mounted(dir)
		return mountState{mounted: m, alive: m && mountAlive(base, dir)}
	})
}

// liveWithin reports whether dir is a live mirror of base, bounded by
// liveProbeTimeout; a probe that outlives the bound reads dead.
func (s *Server) liveWithin(base, dir string) bool {
	st, ok := probeMount(base, dir)
	return ok && st.mounted && st.alive
}

// registered returns dir's registry row, if any.
func (s *Server) registered(dir string) (row mountRow, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok = s.registry[dir]
	return row, ok
}

// deregister drops dir's registry row.
func (s *Server) deregister(dir string) {
	s.mu.Lock()
	delete(s.registry, dir)
	s.mu.Unlock()
}

func (s *Server) handleMount(req Request) Response {
	if req.Base == "" || req.Dir == "" {
		return Response{OK: false, Error: "mount: base and dir are required"}
	}
	if req.Dir == req.Base {
		return Response{OK: false, Error: fmt.Sprintf("mount: refusing dir == base (%s)", req.Dir)}
	}
	release, ok := s.claim(req.Dir)
	if !ok {
		return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on " + req.Dir}
	}
	defer release()

	if row, ok := s.registered(req.Dir); ok {
		if row.Base != req.Base {
			return Response{
				OK:       false,
				ErrClass: ClassBaseMismatch,
				Error:    fmt.Sprintf("mount: %s already mirrors %s, not %s; unmount it first", req.Dir, row.Base, req.Base),
			}
		}
		// Bounded: a wedged mirror's stats never return, and a wedged probe
		// reads dead — routing into the forced teardown below, the designed
		// recovery — instead of hanging the handler. A shallow-live mirror is
		// idempotently OK here; detecting and healing a partial wedge
		// (shallow-alive, bulk reads hang) is the daemon's job now — it
		// deep-probes the mirror and tears a wedged one down (mountFuse) before
		// it issues this Mount, so a remount RPC only ever lands after the
		// corpse is gone and this path remounts a clean dir.
		if s.liveWithin(req.Base, req.Dir) {
			return Response{OK: true} // idempotent: this exact mount is held and live
		}
		// Mount is ensure-mounted: the registered mirror died while the holder
		// lived (external umount, fuse-t fault). The provider's Setup
		// early-returns on its own stale row, so the corpse must come down
		// before the remount.
		err := s.Host.Teardown(req.Base, req.Dir)
		// Drop the row regardless of outcome, exactly like handleUnmount: the
		// provider dropped its handle, so the row would be a lie.
		s.deregister(req.Dir)
		if err != nil {
			class := ClassMountFailed
			if errors.Is(err, overlay.ErrUnmountWedged) {
				class = ClassWedged
			}
			s.Log.Printf("remount %s: tear down dead mirror: %v", req.Dir, err)
			return Response{OK: false, ErrClass: class, Error: fmt.Sprintf("remount %s: tear down dead mirror: %v", req.Dir, err)}
		}
		s.Log.Printf("remounting dead mirror %s <- %s", req.Dir, req.Base)
		// The corpse is down (Teardown verifies the mountpoint is gone before
		// returning nil), so skip the foreign-mount check and remount.
		return s.setupAndRegister(req.Base, req.Dir)
	}
	// Never stack mounts: a mountpoint with no registry row belongs to
	// someone else (a dead holder's carcass, or not ours at all). Bounded, and
	// fail closed: a carcass can be a wedged mirror whose stat never returns,
	// and an unanswered probe must read as a foreign mountpoint — refusing,
	// never stacking a mount over it or hanging the handler with the dir's
	// claim held (every retry would then read busy forever).
	if st, ok := probeMount(req.Base, req.Dir); !ok || st.mounted {
		return Response{
			OK:       false,
			ErrClass: ClassForeignMount,
			Error:    fmt.Sprintf("mount: %s is already a mountpoint this holder does not own; unmount it first", req.Dir),
		}
	}
	return s.setupAndRegister(req.Base, req.Dir)
}

// setupAndRegister mounts base at dir via the provider and records the mount:
// a fresh registry row with a bumped epoch and the mount time. The caller
// holds dir's in-flight claim.
func (s *Server) setupAndRegister(base, dir string) Response {
	if err := s.Host.Setup(base, dir); err != nil {
		// Ordered: ErrMountTimeout (proven grant, transient) must classify
		// before ErrMountNotLive (the presumed-TCC condition) — mount-timeout
		// is the honest verdict whenever the proven-grant sentinel is present.
		class := ClassMountFailed
		switch {
		case errors.Is(err, overlay.ErrMountTimeout):
			class = ClassMountTimeout
		case errors.Is(err, overlay.ErrMountNotLive):
			class = ClassTCC
		}
		s.Log.Printf("mount %s <- %s: %v", dir, base, err)
		return Response{OK: false, ErrClass: class, Error: err.Error()}
	}
	s.mu.Lock()
	s.epochs[dir]++
	s.registry[dir] = mountRow{Base: base, Epoch: s.epochs[dir], MountedAt: time.Now()}
	s.mu.Unlock()
	s.Log.Printf("mounted %s <- %s", dir, base)
	return Response{OK: true}
}

func (s *Server) handleUnmount(req Request) Response {
	if req.Base == "" || req.Dir == "" {
		return Response{OK: false, Error: "unmount: base and dir are required"}
	}
	if req.Dir == req.Base {
		return Response{OK: false, Error: fmt.Sprintf("unmount: refusing dir == base (%s)", req.Dir)}
	}
	release, ok := s.claim(req.Dir)
	if !ok {
		return Response{OK: false, ErrClass: ClassBusy, Error: "busy: another operation is in flight on " + req.Dir}
	}
	defer release()

	row, ok := s.registered(req.Dir)
	base := row.Base
	if !ok {
		// Bounded, and fail closed: a probe that does not answer (a wedged
		// carcass) must read still-mounted, routing into the provider's
		// bounded forced teardown below — never an OK no-op for a dir that may
		// still be a live mountpoint, and never a hung handler.
		if st, ok := probeMount(req.Base, req.Dir); ok && !st.mounted {
			return Response{OK: true} // not mounted at all: no-op
		}
		// A carcass: a mountpoint with no registry row (a dead holder's
		// leftover). Teardown needs base only for its base==dir refusal, so
		// the request's Base serves.
		base = req.Base
	}
	err := s.Host.Teardown(base, req.Dir)
	// Drop the registry row regardless of outcome: the provider already
	// dropped its handle, so a row for a dir the holder can no longer operate
	// on would be a lie. Honesty about a wedged unmount comes from the error.
	s.deregister(req.Dir)
	if err != nil {
		class := ""
		if errors.Is(err, overlay.ErrUnmountWedged) {
			class = ClassWedged
		}
		s.Log.Printf("unmount %s: %v", req.Dir, err)
		return Response{OK: false, ErrClass: class, Error: err.Error()}
	}
	s.Log.Printf("unmounted %s", req.Dir)
	return Response{OK: true}
}

func (s *Server) handleList() Response {
	// Liveness is kernel truth, and both halves matter: mounted is the
	// device-id mountpoint check (a dead mirror exposes the underlying dir,
	// whose leftover entries can make mountAlive's visibility stat lie) and
	// mountAlive confirms base's contents show through. Both are stat-side
	// I/O the registry lock must not span (snapshotRegistry released it) and
	// either can wedge with its mirror, so the entries are probed in parallel,
	// each bounded by liveProbeTimeout: one wedged mirror reads Live=false —
	// the driver heals it through the bounded forced-teardown remount path —
	// while its healthy siblings keep reporting true within the deadline.
	snap := s.snapshotRegistry()
	dirs := make([]string, 0, len(snap))
	for dir := range snap {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	mounts := make([]MountInfo, len(dirs))
	var wg sync.WaitGroup
	for i, dir := range dirs {
		row := snap[dir]
		mounts[i] = MountInfo{Dir: dir, Base: row.Base, Epoch: row.Epoch}
		if !row.MountedAt.IsZero() {
			mounts[i].MountedAt = row.MountedAt.Unix()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Live is shallow kernel truth only — mountpoint present and base
			// visible. Detecting a partial wedge (shallow-alive, bulk reads
			// hang) is the daemon's job: it deep-probes through the same kernel
			// mount and keeps its own per-dir verdict, so the holder ships no
			// deep verdict at all.
			mounts[i].Live = s.liveWithin(row.Base, dir)
		}()
	}
	wg.Wait()
	return Response{OK: true, Mounts: mounts}
}

// handleShutdown sweeps every owned mount, replies with the dirs that failed
// to come down (empty means clean), then cancels Run's context. Cancelling
// the ctx closes the listener, never this live connection, so the reply
// (written by handle after dispatch returns) still lands.
func (s *Server) handleShutdown() Response {
	failed := s.unmountAll()
	s.triggerShutdown()
	return Response{OK: true, Mounts: failed}
}

// snapshotRegistry copies the registry under the lock so callers can do I/O
// against the entries lock-free.
func (s *Server) snapshotRegistry() map[string]mountRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := make(map[string]mountRow, len(s.registry))
	for dir, row := range s.registry {
		snap[dir] = row
	}
	return snap
}

// unmountAll tears down every registered mount, claiming each dir like a
// normal unmount so the sweep cannot interleave with an in-flight op — a busy
// dir is left to its own handler and reported as failed (Live=true). Each
// Teardown is individually bounded by the provider's grace timers. Returns
// the dirs still mounted afterwards, for the shutdown reply.
func (s *Server) unmountAll() []MountInfo {
	snap := s.snapshotRegistry()
	dirs := make([]string, 0, len(snap))
	for dir := range snap {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	var failed []MountInfo
	for _, dir := range dirs {
		base := snap[dir].Base
		release, ok := s.claim(dir)
		if !ok {
			s.Log.Printf("sweep: %s busy; leaving it to its in-flight op", dir)
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		err := s.Host.Teardown(base, dir)
		s.deregister(dir)
		release()
		if err != nil {
			s.Log.Printf("sweep unmount %s: %v", dir, err)
			failed = append(failed, MountInfo{Dir: dir, Base: base, Live: true})
			continue
		}
		s.Log.Printf("sweep unmounted %s", dir)
	}
	return failed
}
