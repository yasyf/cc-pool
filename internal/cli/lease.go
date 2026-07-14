package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/fusekit/lease"
	fkoverlay "github.com/yasyf/fusekit/overlay"
	"golang.org/x/sys/unix"
)

// leaseProbeTimeout bounds the post-Acquire liveness stat of the leased dir: a
// fully-wedged fuse mirror never answers metadata, so a bounded stat fails the
// launch loudly instead of hanging.
const leaseProbeTimeout = 5 * time.Second

// leaseAcquireBound mirrors the lease package's acquireWait — the Seize-fence wait
// Acquire tolerates before failing HeldError. Kept in sync by hand; the lease pkg
// keeps its own value internal.
const leaseAcquireBound = 5 * time.Second

// leaseReadyMargin is slack over the summed component bounds for scheduler jitter.
const leaseReadyMargin = 3 * time.Second

// leaseReadyTimeout bounds the parent's wait for a detached lease agent to signal
// acquired+probed. It is DERIVED from the agent's FULL sequential worst case —
// Acquire's fence wait (bounded by leaseAcquireBound), then the bounded liveness
// stat and the deep-read probe — plus a margin. With advisory-only slots the child
// never blocks on another agent, so this is a straight component sum, strictly
// LARGER (by the margin) than any wait the child can legitimately perform, so a
// healthy-but-slow init is never killed. A var so tests can shrink it.
var leaseReadyTimeout = leaseAcquireBound + leaseProbeTimeout + overlay.DeepProbeBound + leaseReadyMargin

// slotReadyMarker is the prefix a lease agent writes into its ADVISORY registry slot
// once it has acquired, probed, and begun watching. The slot is an OPTIMIZATION ONLY —
// a best-effort hint that lets a later spawner skip forking a redundant agent — never a
// correctness mechanism. See the advisory-slot design note below (writeAdvisorySlot).
const slotReadyMarker = "ready"

// ErrNoProc means a watched pid does not exist — the shell already gone, or a
// pid reused out from under a lease agent. Never an error condition: the agent
// releases and exits.
var ErrNoProc = errors.New("process not found")

// procWaiter blocks until a watched process exits.
type procWaiter interface {
	Wait() error
	// Drain non-blockingly reports whether the watched exit is ALREADY queued on the
	// installed watch — a zombie leader that passes start-time revalidation but would
	// make Wait return at once. The agent drains it before ok so a knowable death fails.
	Drain() (bool, error)
	Close() error
}

// leaseRoot resolves the fleet lease dir; a var seam so tests point it at a temp
// root, never ~/.fusekit.
var leaseRoot = lease.DefaultRoot

// leaseAgentDir resolves the detached lease-agent registry dir (pidfile slots);
// a var seam so tests point it at a temp dir, never ~/.cc-pool.
var leaseAgentDir = func() (string, error) { return filepath.Join(pool.StateDir(), "lease-agents"), nil }

// pollDrainHook, when non-nil, fires in the poll-fallback arm after the watcher's
// first aliveness check and before the queued-death drain — a test seam to make an
// already-queued leader exit deterministically observable. Nil in production.
var pollDrainHook func(exited <-chan error)

// procStartTime, registerProcExit, and procSessionLeader are the pid-watch seams.
// Their darwin implementations use sysctl KERN_PROC_PID, kqueue NOTE_EXIT, and
// getsid(0); tests fake all three.
var (
	procStartTime     = realProcStartTime
	registerProcExit  = realRegisterProcExit
	procSessionLeader = realSessionLeader
)

// boundedStat is a bounded os.Stat: a fully-wedged mirror never answers, so a
// timeout aborts the launch instead of hanging in the metadata layer.
func boundedStat(dir string) error {
	done := make(chan error, 1)
	go func() {
		_, err := os.Stat(dir)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(leaseProbeTimeout):
		return fmt.Errorf("did not answer a stat within %s", leaseProbeTimeout)
	}
}

// probeLeasedDir is the post-Acquire contract: it proves the dir claude will read
// is not a dead or wedged mirror before anything execs into it. A bounded stat
// catches a full wedge (metadata hangs) and an absent/dead mount; the deep
// sequential-read probe catches the documented PARTIAL wedge (metadata answers but
// bulk reads hang). Missing-probe strictness is ROW-SHAPE-AWARE: a fuse row (mux or
// legacy) MUST serve the holder's probe file, so ErrProbeMissing is FATAL — it means
// an unmounted or stale mount whose bare directory would launch claude into the
// underlying dir; a symlink/File Provider row serves no probe file, so a missing
// probe there is benign.
func probeLeasedDir(configDir string, fuseRow bool) error {
	if err := boundedStat(configDir); err != nil {
		return fmt.Errorf("%s is not answering (dead or absent mount?): %w — run `ccp doctor`", configDir, err)
	}
	switch err := overlay.DeepProbeWithin(configDir); {
	case err == nil:
		return nil
	case errors.Is(err, overlay.ErrProbeMissing):
		if fuseRow {
			return fmt.Errorf("%s is a fuse account but its mount serves no probe file (unmounted or stale mount?): %w — run `ccp doctor`", configDir, err)
		}
		return nil
	default:
		return fmt.Errorf("%s did not answer a full read (wedged mirror?): %w — run `ccp doctor`", configDir, err)
	}
}

// isFuseRow reports whether a stored overlay kind is fuse-backed (mux or legacy).
func isFuseRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	return err == nil && b.IsFuse()
}

// isFPRow reports whether a stored overlay kind is the File Provider backend — the
// only backend whose files can be dataless and need the exec-time materialization guard.
func isFPRow(overlayKind string) bool {
	b, err := fkoverlay.Parse(overlayKind)
	return err == nil && b == fkoverlay.BackendFileProvider
}

// acquireLease takes a session lease on key in the fleet lease root.
func acquireLease(owner, key string) (*lease.Handle, error) {
	root, err := leaseRoot()
	if err != nil {
		return nil, fmt.Errorf("resolve the session lease root: %w", err)
	}
	h, err := lease.Acquire(root, key, owner)
	if err != nil {
		return nil, fmt.Errorf("take a session lease on %s: %w", key, err)
	}
	return h, nil
}

// acquireSessionLease takes the shared session lease on account a keyed on a's
// current-shape lease dir — byte-identical to the dir the holder fences, so the
// lease gates the holder's teardown of a's mount. The handle's fd is non-CLOEXEC
// by lease design, so it rides through syscall.Exec and every fork+exec child,
// pinning the lease for the whole session tree; it releases only when the last
// descriptor closes.
func acquireSessionLease(a store.Account) (*lease.Handle, error) {
	return acquireLease(pool.HolderOwner, pool.SessionLeaseDir(a))
}

// probeSessionLease runs the post-Acquire probe on the dir claude reads (a's
// ConfigDir), catching a dead, absent, or partially-wedged mirror before exec.
func probeSessionLease(a store.Account) error {
	return probeLeasedDir(a.ConfigDir, isFuseRow(a.OverlayKind))
}

// acquireAndProbeSessionLease is the one shared acquire+probe helper every
// synchronous launch/login path uses (run, login, relogin, TUI relogin): it takes
// a's session lease then proves the mount answers, closing the handle on a probe
// failure so a wedged mount never keeps a dangling lease.
func acquireAndProbeSessionLease(a store.Account) (*lease.Handle, error) {
	return acquireAndProbe(pool.SessionLeaseDir(a), a.ConfigDir, isFuseRow(a.OverlayKind))
}

// acquireAndProbePendingLease is acquireAndProbeSessionLease for a fresh add,
// whose account is not in the store yet: the key derives from the pending row's
// index, dir, and backend.
func acquireAndProbePendingLease(p *pool.PendingAdd) (*lease.Handle, error) {
	return acquireAndProbe(pool.SessionLeaseDirFor(p.Index, p.ConfigDir, string(p.OverlayKind)), p.ConfigDir, p.OverlayKind.IsFuse())
}

func acquireAndProbe(key, probeDir string, fuseRow bool) (*lease.Handle, error) {
	h, err := acquireLease(pool.HolderOwner, key)
	if err != nil {
		return nil, err
	}
	if err := probeLeasedDir(probeDir, fuseRow); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

// keepLeaseAlive extends h's lifetime across a syscall.Exec: the lease fd must
// stay open at exec time (it is inherited, non-CLOEXEC) for the lock to survive.
func keepLeaseAlive(h *lease.Handle) { runtime.KeepAlive(h) }

// spawnLeaseAgent forks a detached `ccp lease-agent` that holds account a's session
// lease until the terminal's session leader exits. select/env are launchers whose
// caller (a shell) runs claude after them, so the lease must outlive the ccp
// process — it lives in the agent instead. The agent watches the SESSION LEADER
// (getsid), not the immediate parent: for `$(ccp select)` / `eval "$(ccp env)"`
// the parent is an ephemeral command-substitution subshell whose getppid dies at
// once, whereas the leader is the terminal tab's shell in every invocation shape.
// A readiness handshake makes the caller block until the agent has acquired AND
// probed the lease, so no dir is handed out unprotected; a live, ready agent already
// covering (leader, key) skips the fork (an advisory-slot optimization — a redundant
// agent would merely be one idle process, so a missed skip is harmless). A var seam so
// command tests stub the real fork+handshake.
var spawnLeaseAgent = func(a store.Account) error {
	return spawnLeaseAgentKey(a.ID, pool.SessionLeaseDir(a), a.ConfigDir, isFuseRow(a.OverlayKind))
}

// spawnPendingLeaseAgent hands a fresh add's PENDING dir to a detached lease agent
// tied to the terminal's session leader — the select/env shape — so the printed
// external login stays protected from holder teardown until the terminal session ends.
// LIMITATION: the lease watches the ORIGINATING terminal's session leader, so a login
// the user completes in a DIFFERENT terminal loses lease coverage the moment they close
// the originating terminal. The session leader is the only anchor available at print
// time, and an anchored lease beats none; the add command's help names this. A var so
// add tests stub the fork.
var spawnPendingLeaseAgent = func(p *pool.PendingAdd) error {
	return spawnLeaseAgentKey(p.Index, pool.SessionLeaseDirFor(p.Index, p.ConfigDir, string(p.OverlayKind)), p.ConfigDir, p.OverlayKind.IsFuse())
}

// leaseAgentArgs is the detached lease-agent argv. --ready-fd names the readiness pipe
// (readyPipeFD, ExtraFiles[0]); it must round-trip through SpawnedLeaseAgentReadyFD, the
// single source the child adopts and main's sweep preserves.
func leaseAgentArgs(leader int, start int64, id int, key, probeDir string, fuseRow bool) []string {
	args := []string{leaseAgentSubcommand,
		"--pid", strconv.Itoa(leader),
		"--start", strconv.FormatInt(start, 10),
		"--id", strconv.Itoa(id),
		"--dir", key,
		"--probe", probeDir,
		"--ready-fd", strconv.Itoa(readyPipeFD)}
	if fuseRow {
		args = append(args, "--fuse")
	}
	return args
}

func spawnLeaseAgentKey(id int, key, probeDir string, fuseRow bool) error {
	leader, err := procSessionLeader()
	if err != nil {
		return fmt.Errorf("resolve the terminal session leader: %w", err)
	}
	// A degenerate or dead session leader means there is nothing to tie the lease to,
	// so fail closed rather than hand out an unprotected dir — the user reruns from a
	// live interactive shell.
	if leader <= 1 {
		return fmt.Errorf("cannot hold a session lease: no terminal session leader (getsid returned %d); rerun from an interactive shell", leader)
	}
	start, err := procStartTime(leader)
	if err != nil {
		if errors.Is(err, ErrNoProc) {
			return fmt.Errorf("cannot hold a session lease: session leader %d is gone; rerun from a live shell", leader)
		}
		return fmt.Errorf("read session leader %d start time: %w", leader, err)
	}
	if slotCovered(leader, start, key) {
		return nil // a live, ready agent already holds this session's lease
	}
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable for the lease agent: %w", err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create the lease-agent readiness pipe: %w", err)
	}
	defer func() { _ = r.Close() }()
	args := leaseAgentArgs(leader, start, id, key, probeDir, fuseRow)
	//nolint:gosec // G204: self is this CLI; args are fixed flags with numeric/path values.
	c := exec.Command(self, args...)
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach into a new session so it outlives ccp
	c.Stdin, c.Stdout, c.Stderr = nil, nil, nil
	c.ExtraFiles = []*os.File{w} // fd 3 in the child: the readiness pipe
	if err := c.Start(); err != nil {
		_ = w.Close()
		return fmt.Errorf("spawn lease agent: %w", err)
	}
	_ = w.Close() // the child holds the write end; our copy must close so EOF signals its death
	return awaitAgentReady(r, c)
}

// awaitAgentReady blocks until the agent signals acquired+probed ("ok"), reports a
// failure ("err: …"), or dies (EOF before "ok") — or the readiness deadline elapses.
// Any non-ok outcome is an error so the caller hands out nothing. On the deadline it
// kills and reaps the child; the child's advisory slot (if any) is best-effort and
// needs no cleanup here — a dead child's flock releases, so a later spawner's
// slotCovered check reads it stale and simply spawns anew.
func awaitAgentReady(r *os.File, child *exec.Cmd) error {
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		b, err := io.ReadAll(r)
		ch <- result{string(b), err}
	}()
	select {
	case <-time.After(leaseReadyTimeout):
		if child != nil && child.Process != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
		return fmt.Errorf("the session lease agent did not become ready within %s", leaseReadyTimeout)
	case res := <-ch:
		if res.err != nil {
			return fmt.Errorf("read the lease-agent readiness signal: %w", res.err)
		}
		line := strings.TrimSpace(res.line)
		switch {
		case line == "ok":
			return nil
		case strings.HasPrefix(line, "err:"):
			return fmt.Errorf("session lease: %s", strings.TrimSpace(strings.TrimPrefix(line, "err:")))
		default:
			return errors.New("the session lease agent exited before acquiring the lease")
		}
	}
}

// leaseAgentSubcommand is the internal detached-agent verb; main runs the
// fd-3-preserving sweep for it instead of the full inherited-fd sweep.
const leaseAgentSubcommand = "lease-agent"

// readyPipeFD is the descriptor the launcher hands a detached lease agent its readiness
// pipe on (ExtraFiles[0]); the launcher marks its presence with --ready-fd and the agent
// keeps it across the inherited-fd sweep. A manual `ccp lease-agent` passes no --ready-fd,
// adopts no pipe, and runs the full sweep.
const readyPipeFD = 3

// LeaseAgentInvocation reports whether args launches the internal detached lease agent.
func LeaseAgentInvocation(args []string) bool {
	return len(args) > 0 && args[0] == leaseAgentSubcommand
}

// SpawnedLeaseAgentReadyFD reports the readiness-pipe fd a detached lease agent was
// launched with (--ready-fd N) and whether the flag is present. main inspects os.Args
// BEFORE cobra parses so the inherited-fd sweep preserves EXACTLY that fd; a manual
// `ccp lease-agent` with no --ready-fd gets the full proc.CloseInheritedFDs sweep and
// keeps no fd, so it can neither pin nor later clobber an unrelated inherited descriptor.
func SpawnedLeaseAgentReadyFD(args []string) (int, bool) {
	if !LeaseAgentInvocation(args) {
		return 0, false
	}
	for i, a := range args {
		var v string
		switch {
		case a == "--ready-fd" && i+1 < len(args):
			v = args[i+1]
		case strings.HasPrefix(a, "--ready-fd="):
			v = strings.TrimPrefix(a, "--ready-fd=")
		default:
			continue
		}
		if fd, err := strconv.Atoi(v); err == nil && fd > 0 {
			return fd, true
		}
		return 0, false
	}
	return 0, false
}

// SweepInheritedFDsExcept closes every inherited non-CLOEXEC descriptor except stdio
// (0-2) and keep — the launcher's readiness pipe (--ready-fd). The lease agent runs this
// instead of proc.CloseInheritedFDs (which would close the pipe too): a manually- or
// claude-spawned agent can inherit an unrelated non-CLOEXEC lease fd, and keeping it
// would pin that lease for the whole watch, so every non-CLOEXEC fd above stdio except
// the readiness pipe is dropped.
func SweepInheritedFDsExcept(keep int) error {
	dir := "/dev/fd"
	if runtime.GOOS == "linux" {
		dir = "/proc/self/fd"
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list open fds: %w", err)
	}
	for _, e := range entries {
		fd, err := strconv.Atoi(e.Name())
		if err != nil || fd <= 2 || fd == keep {
			continue
		}
		// ReadDir's own transient fd reads EBADF here and is skipped.
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		_ = unix.Close(fd)
	}
	return nil
}

func newLeaseAgentCmd() *cobra.Command {
	var pid, id, readyFd int
	var start int64
	var dir, probe string
	var fuse bool
	cmd := &cobra.Command{
		Use:    leaseAgentSubcommand,
		Short:  "Hold a session lease until a watched shell exits (internal)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// INVARIANT: adopt the fd from the SAME parse main's sweep used, never cobra's
			// --ready-fd (pflag is base-0/last-occurrence, the sweep base-10/first) — the
			// handshake must reuse the fd the sweep preserved, so the two cannot diverge.
			var ready *os.File
			if fd, ok := SpawnedLeaseAgentReadyFD(os.Args[1:]); ok {
				ready = os.NewFile(uintptr(fd), "lease-ready")
			}
			return runLeaseAgent(pid, start, id, dir, probe, fuse, ready)
		},
	}
	cmd.Flags().IntVar(&pid, "pid", 0, "session-leader pid to watch")
	cmd.Flags().Int64Var(&start, "start", 0, "session-leader start-time stamp captured by the launcher")
	cmd.Flags().IntVar(&id, "id", 0, "account index, for re-deriving the lease key from current shape")
	cmd.Flags().StringVar(&dir, "dir", "", "lease dir to hold")
	cmd.Flags().StringVar(&probe, "probe", "", "config dir to probe after acquiring")
	cmd.Flags().BoolVar(&fuse, "fuse", false, "the probe dir is a fuse mount (a missing probe file is fatal)")
	cmd.Flags().IntVar(&readyFd, "ready-fd", 0, "readiness-pipe fd the launcher passes (ExtraFiles[0]); unset skips the handshake")
	return cmd
}

// runLeaseAgent acquires the session lease on dir and holds it until the session
// leader exits. It acquires the lease, PROBES the mount, installs the leader
// exit-watch and revalidates the leader's start-time, and only THEN writes its
// advisory slot and signals the launcher it is safe to hand out the dir — a leader
// that exits or is pid-reused during acquire+probe never gets an ok whose lease
// releases the instant we begin watching. It takes NO slot before acquiring: a
// concurrent duplicate agent is harmless (leases are shared, each watches the same
// leader and exits with it), so there is no cross-agent coordination — the advisory
// slot is written after readiness purely to let a LATER spawner skip a redundant fork.
//
// KNOWABLE-DEATH residual: a leader that dies within the acquire+watch→ok window is
// caught by revalidateLeader (kqueue) or the pre-ok drain of exited (poll). The
// UNKNOWABLE residual — a leader that dies microseconds AFTER ok — is inherent to both
// watch paths (the lease simply releases when its watcher fires) and is not closed.
func runLeaseAgent(leader int, wantStart int64, id int, dir, probe string, fuseRow bool, ready *os.File) error {
	if leader <= 1 || dir == "" {
		err := fmt.Errorf("lease-agent: need a leader pid > 1 and a dir (got pid=%d dir=%q)", leader, dir)
		signalReady(ready, err)
		return err
	}

	h, err := acquireLease(pool.HolderOwner, pool.SessionLeaseDirForShape(id, dir, fuseRow))
	if err != nil {
		return signalFail(ready, err)
	}
	defer func() { _ = h.Close() }()

	if probe != "" {
		if err := probeLeasedDir(probe, fuseRow); err != nil {
			return signalFail(ready, err)
		}
	}

	waiter, werr := registerProcExit(leader)
	switch {
	case errors.Is(werr, ErrNoProc):
		return signalFail(ready, fmt.Errorf("session leader %d exited before the lease was ready", leader))
	case werr != nil:
		// kqueue unavailable (EMFILE/ENFILE): a poll goroutine stands in for the exit
		// watch. It must be RUNNING with its first aliveness check passed BEFORE we signal
		// ok — the same watch-before-ok contract as the kqueue path.
		started := make(chan error, 1)
		exited := make(chan error, 1)
		go func() { exited <- pollProcExitWatch(leader, wantStart, started) }()
		if err := <-started; err != nil {
			return signalFail(ready, err)
		}
		if pollDrainHook != nil {
			pollDrainHook(exited)
		}
		// The poll watcher may have ALREADY observed the leader's death between its
		// started signal and here; a queued exit means the leader is gone, so fail loud
		// rather than hand out a dir the lease no longer meaningfully protects.
		select {
		case <-exited:
			return signalFail(ready, fmt.Errorf("session leader %d exited before the lease was ready", leader))
		default:
		}
		slot := writeAdvisorySlot(leader, wantStart, dir)
		defer removeOwnSlot(slot)
		signalReady(ready, nil)
		return <-exited
	}
	defer waiter.Close()
	if err := revalidateLeader(leader, wantStart); err != nil {
		return signalFail(ready, err)
	}
	// A leader that died AFTER the watch was installed is a still-observable zombie that
	// passes revalidateLeader, but its NOTE_EXIT is already queued — Wait would return
	// the instant we begin watching. Drain the queued death before ok, mirroring the poll
	// fallback's pre-ok drain of exited: a knowable death fails loud, never handed out.
	switch queued, derr := waiter.Drain(); {
	case derr != nil:
		return signalFail(ready, fmt.Errorf("poll session leader %d exit watch: %w", leader, derr))
	case queued:
		return signalFail(ready, fmt.Errorf("session leader %d exited before the lease was ready", leader))
	}
	slot := writeAdvisorySlot(leader, wantStart, dir)
	defer removeOwnSlot(slot)
	signalReady(ready, nil) // acquired + probed + watching: the launcher may hand out the dir
	return waiter.Wait()
}

// revalidateLeader re-reads leader's start-time after the exit watch is installed; a
// mismatch or ESRCH means the leader exited or its pid was reused during acquire+probe.
func revalidateLeader(leader int, wantStart int64) error {
	switch got, err := procStartTime(leader); {
	case errors.Is(err, ErrNoProc):
		return fmt.Errorf("session leader %d exited before the lease was ready", leader)
	case err != nil:
		return fmt.Errorf("re-read session leader %d start time: %w", leader, err)
	case got != wantStart:
		return fmt.Errorf("session leader %d pid was reused before the lease was ready", leader)
	default:
		return nil
	}
}

// signalReady writes the agent's readiness verdict to the launcher's pipe and
// closes it. Nil cause is "ok" (acquired + probed); a cause is "err: …". A nil
// pipe (manual invocation) is a no-op.
func signalReady(ready *os.File, cause error) {
	if ready == nil {
		return
	}
	if cause == nil {
		_, _ = io.WriteString(ready, "ok\n")
	} else {
		_, _ = fmt.Fprintf(ready, "err: %v\n", cause)
	}
	_ = ready.Close()
}

// signalFail reports err over the readiness pipe and returns it.
func signalFail(ready *os.File, err error) error {
	signalReady(ready, err)
	return err
}

// leaseAgentSlotKey is the registry slot filename for one (leader, start, key)
// tuple. The leader start-time distinguishes a reused leader pid; the key hash
// scopes it to the account's lease dir.
func leaseAgentSlotKey(leader int, start int64, key string) string {
	sum := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%d-%d-%s.slot", leader, start, hex.EncodeToString(sum[:])[:16])
}

// The lease-agent registry slot is an ADVISORY OPTIMIZATION, never a correctness
// mechanism. Coverage is correct BY CONSTRUCTION regardless of the slot: session
// leases are shared (LOCK_SH), each lease agent watches its terminal's session leader
// and exits with it, and the launcher's own fd-3 readiness handshake
// (acquired+probed+watching ⇒ ok) is what gates every dir handout. A duplicate agent
// therefore costs one idle process and nothing more.
//
// The slot exists only to let a LATER spawner skip forking a redundant agent. It is
// written AFTER an agent is ready, under that agent's own flock, and removed only by
// that agent (fd-identity-checked, under the held lock). slotCovered does ONE cheap
// check and SPAWNS ON ANY DOUBT: only a slot that is present, flock-held by a live
// agent, identity-matched, and marked ready skips the fork. A wrong skip is impossible
// (only a verified live+ready+identity-matched slot skips); a wrong spawn is harmless.
// This fail-closed-toward-spawning direction is why there is no cross-agent unlink, no
// blocking wait, and no slot state machine.

// slotCovered reports whether a live, ready lease agent already advertises the slot for
// (leader, start, key) — the spawner's fork-skip fast path. FAIL-CLOSED TOWARD
// SPAWNING: it returns true ONLY when, in a single open, the slot is present, its flock
// is held by another live process, the fd still names the live slot path, and its
// marker reads ready. Absent, unlocked (stale), unreadable, identity-mismatched,
// still-initializing, or any error ⇒ false ⇒ the caller spawns a (harmless) duplicate.
func slotCovered(leader int, start int64, key string) bool {
	agentDir, err := leaseAgentDir()
	if err != nil {
		return false
	}
	p := filepath.Join(agentDir, leaseAgentSlotKey(leader, start, key))
	f, err := os.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	fd := int(f.Fd())
	switch lerr := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); {
	case errors.Is(lerr, syscall.EWOULDBLOCK):
		// Held by a live agent — the only case that can skip. Never unlink it here.
	case lerr == nil:
		_ = syscall.Flock(fd, syscall.LOCK_UN) // we took the lock ⇒ no live holder ⇒ stale
		return false
	default:
		return false
	}
	if same, err := fdIsSlotPath(fd, p); err != nil || !same {
		return false
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(string(b)), slotReadyMarker)
}

// writeAdvisorySlot records the (leader, start, key) coverage hint AFTER the agent is
// ready, holding the slot's flock for the agent's lifetime so a spawner's slotCovered
// check sees a live holder. BEST-EFFORT: any failure (an unwritable dir, or a duplicate
// agent already holding the slot flock) returns nil — the slot is an optimization, so a
// missing hint only costs a future duplicate, never a failed launch. removeOwnSlot
// releases it.
func writeAdvisorySlot(leader int, start int64, key string) *os.File {
	agentDir, err := leaseAgentDir()
	if err != nil {
		return nil
	}
	if err := os.MkdirAll(agentDir, 0o700); err != nil {
		return nil
	}
	p := filepath.Join(agentDir, leaseAgentSlotKey(leader, start, key))
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close() // EWOULDBLOCK: a concurrent duplicate already advertises this slot
		return nil
	}
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return nil
	}
	if _, err := f.WriteAt([]byte(fmt.Sprintf("%s %d %d %s", slotReadyMarker, os.Getpid(), start, key)), 0); err != nil {
		_ = f.Close()
		return nil
	}
	return f
}

// removeOwnSlot removes the agent's OWN advisory slot, then closes it. It removes only a
// slot whose fd still names the live path (under the held lock) — never another agent's
// slot. Nil-safe: an agent that never wrote a slot (contention or an unwritable dir) has
// nothing to remove.
func removeOwnSlot(f *os.File) {
	if f == nil {
		return
	}
	if same, err := fdIsSlotPath(int(f.Fd()), f.Name()); err == nil && same {
		_ = os.Remove(f.Name())
	}
	_ = f.Close()
}

// fdIsSlotPath reports whether fd still refers to the live file at p — the unlink-race
// guard: a flock on an inode an agent already unlinked (release) or that a successor
// replaced is invisible to the live slot and must not be trusted as coverage.
func fdIsSlotPath(fd int, p string) (bool, error) {
	var fst syscall.Stat_t
	if err := syscall.Fstat(fd, &fst); err != nil {
		return false, fmt.Errorf("fstat lease slot %s: %w", p, err)
	}
	if fst.Nlink == 0 {
		return false, nil
	}
	var pst syscall.Stat_t
	switch err := syscall.Stat(p, &pst); {
	case errors.Is(err, syscall.ENOENT):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("stat lease slot %s: %w", p, err)
	}
	return fst.Dev == pst.Dev && fst.Ino == pst.Ino, nil
}

// pollProcExitWatch is the kqueue fallback's watch-before-ok wrapper: it does the
// first aliveness check (revalidateLeader) and reports it on started (nil = alive
// and now watching; an error = already gone/reused at watch start), then polls to
// the leader's exit. The caller signals the launcher ok only AFTER started reports
// the watcher live — mirroring the kqueue path's watch-installed-before-ok.
func pollProcExitWatch(pid int, wantStart int64, started chan<- error) error {
	if err := revalidateLeader(pid, wantStart); err != nil {
		started <- err
		return err
	}
	started <- nil
	return pollProcExit(pid, wantStart)
}

// pollProcExit is the kqueue-unavailable fallback: kill(pid,0) every second until
// the pid is gone or its start-time stamp changes (pid reuse). procKill is a seam.
var procKill = syscall.Kill

func pollProcExit(pid int, wantStart int64) error {
	for {
		switch got, err := procStartTime(pid); {
		case errors.Is(err, ErrNoProc):
			return nil
		case err != nil:
			return err
		case got != wantStart:
			return nil
		}
		if err := procKill(pid, 0); err != nil {
			return nil // ESRCH or EPERM: treat as gone
		}
		time.Sleep(time.Second)
	}
}
