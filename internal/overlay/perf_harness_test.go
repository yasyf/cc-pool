//go:build fuse && cgo && darwin

package overlay

// On-demand fuse-t serving-path load harness (gated by CCP_PERF=1; never runs in
// the normal suite). It mounts a synthetic ~/.claude mirror, drives a concurrent
// stat-storm + .claude.json commit load resembling ~20 claude sessions, and
// reports client throughput plus the CPU of the fuse-t NFS backend (go-nfsv4)
// and the in-process libfuse-t handler — once with the production noattrcache
// mount opts and once with noattrcache dropped, so the GETATTR-cache lever
// (Phase 3a) is measured directly against the current code.
//
//	CCP_PERF=1 /opt/homebrew/bin/go test -tags fuse ./internal/overlay/ \
//	    -run TestPerfHarness -v -count=1 -timeout 8m

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/sys/unix"
)

// waitMirrorUp polls until base's contents show through mnt, the timeout
// elapses, or the serve goroutine exits first (done closed → a hard mount(2)
// failure that will never come live, after which one final probe keeps a mount
// that landed in the same instant). It is a harness-local stand-in for the
// deleted overlay.waitMounted: the perf arm mounts cgofuse by hand with custom
// A/B opts (so it cannot route through the FuseProvider/MountSet lifecycle),
// and only needs a bounded readiness wait, not fusekit's full mount machinery.
func waitMirrorUp(base, mnt string, timeout time.Duration, done <-chan struct{}) bool {
	deadline := time.Now().Add(timeout)
	for {
		if MountAlive(base, mnt) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-done:
			return MountAlive(base, mnt)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// BenchmarkGetattrSharedLink quantifies win B: a carve-out symlink getattr is now
// syscall-free (serves the precomputed synthetic stat) vs the pre-change cost of
// an Lstat(target)+copyStat+override. Run:
//
//	go test -tags fuse ./internal/overlay/ -run x -bench BenchmarkGetattrSharedLink -benchmem
func BenchmarkGetattrSharedLink(b *testing.B) {
	home := b.TempDir()
	base := filepath.Join(home, ".claude")
	priv := filepath.Join(home, "acct.private")
	_ = os.MkdirAll(filepath.Join(base, "projects"), 0o755) //nolint:gosec // G301: dir perms are intentional for this test's fixture layout
	_ = os.MkdirAll(priv, 0o700)
	fs := newMirrorFS(base, priv, filepath.Join(home, ".claude.json"))
	fs.snapshotShared()
	target := filepath.Join(base, "projects")

	b.Run("synthetic_current", func(b *testing.B) {
		var stat fuse.Stat_t
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if st := fs.Getattr("/projects", &stat, ^uint64(0)); st != 0 {
				b.Fatalf("getattr = %d", st)
			}
		}
	})
	b.Run("lstat_prechange", func(b *testing.B) {
		var st syscall.Stat_t
		var stat fuse.Stat_t
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := syscall.Lstat(target, &st); err != nil {
				b.Fatal(err)
			}
			copyStat(&stat, &st)
			stat.Mode = fuse.S_IFLNK | 0o777
			stat.Size = int64(len(target))
		}
	})
}

// BenchmarkMergedHerd quantifies win A: under a continuous .claude.json
// invalidation stream, single-flight collapses N concurrent merges to 1 (the
// post-commit thundering herd). "singleflight" is the current merged(); "per_caller"
// is the pre-change behavior (every caller runs a full recompute).
func BenchmarkMergedHerd(b *testing.B) {
	dir := b.TempDir()
	privPath := filepath.Join(dir, "priv.claude.json")
	basePath := filepath.Join(dir, "base.claude.json")
	if err := os.WriteFile(privPath, buildClaudeJSON(80, "acct"), 0o600); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(basePath, buildClaudeJSON(80, "base"), 0o600); err != nil {
		b.Fatal(err)
	}
	v := newClaudeJSONView(privPath, basePath)

	run := func(b *testing.B, call func()) {
		stop := make(chan struct{})
		go func() {
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
					n++
					_ = os.WriteFile(privPath, buildClaudeJSON(80, "rev-"+strconv.Itoa(n)), 0o600)
					time.Sleep(time.Millisecond)
				}
			}
		}()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				call()
			}
		})
		b.StopTimer()
		close(stop)
	}

	b.Run("singleflight_current", func(b *testing.B) {
		b.ReportAllocs()
		run(b, func() { v.merged() })
	})
	b.Run("per_caller_prechange", func(b *testing.B) {
		b.ReportAllocs()
		run(b, func() {
			k, ok := v.statKey()
			v.recompute(k, ok)
		})
	})
}

func TestPerfHarness(t *testing.T) {
	if os.Getenv("CCP_PERF") == "" {
		t.Skip("perf harness: set CCP_PERF=1 to run")
	}
	const (
		clients  = 20
		warmup   = 2 * time.Second
		duration = 14 * time.Second
		nProj    = 80
	)
	base := []string{"-o", "nobrowse", "-o", "namedattr", "-o", "rwsize=1048576"}
	arms := []struct {
		name string
		opts []string
	}{
		{"A_noattrcache_CURRENT", append([]string{"-o", "noattrcache"}, base...)},
		{"B_attrcache_PHASE3a", append([]string{}, base...)},
	}
	for i, arm := range arms {
		vol := "cc-pool-perf" + strconv.Itoa(i)
		opts := append([]string{"-o", "volname=" + vol}, arm.opts...)
		t.Run(arm.name, func(t *testing.T) {
			r := runPerfArm(t, vol, opts, clients, warmup, duration, nProj)
			t.Logf("RESULT[%s] ops=%d ops/s=%.0f  nfsv4_cpu=%.0f%%  handler_cpu=%.0f%%  merges=%d",
				arm.name, r.ops, r.opsPerSec, r.nfsCPU, r.selfCPU, r.merges)
			t.Logf("RESULT[%s] go-nfsv4 hot leaves:\n%s", arm.name, r.nfsTop)
		})
	}
}

type armResult struct {
	ops       int64
	opsPerSec float64
	nfsCPU    float64
	selfCPU   float64
	merges    int64
	nfsTop    string
}

func runPerfArm(t *testing.T, vol string, opts []string, clients int, warmup, dur time.Duration, nProj int) armResult {
	base, baseCJ := buildSyntheticClaude(t, nProj)
	mnt := t.TempDir()
	priv := FusePrivateRoot(mnt)
	for name := range ExcludedEntries {
		_ = os.MkdirAll(filepath.Join(priv, name), 0o700)
	}
	// Seed the private .claude.json (a near-copy of base so the merge does real
	// overlay work each invalidation).
	if err := os.WriteFile(filepath.Join(priv, ".claude.json"), buildClaudeJSON(nProj, "acct"), 0o600); err != nil {
		t.Fatal(err)
	}

	fs := newMirrorFS(base, priv, baseCJ)
	fs.snapshotShared()
	host := fuse.NewFileSystemHost(fs)
	host.SetCapReaddirPlus(true)
	done := make(chan struct{})
	go func() { defer close(done); host.Mount(mnt, opts) }()
	if !waitMirrorUp(base, mnt, 15*time.Second, done) {
		host.Unmount()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		t.Skipf("mount %s did not come up", mnt)
	}
	defer func() {
		go host.Unmount()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = unix.Unmount(mnt, unix.MNT_FORCE)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
		}
	}()

	var ops, merges int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	statTargets := []string{"projects", "history", "todos", "settings.json", "statsig"}

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			j := id
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = os.Lstat(filepath.Join(mnt, statTargets[j%len(statTargets)]))
				if j%5 == 0 {
					_, _ = os.Stat(filepath.Join(mnt, ".claude.json")) // merged-view getattr path
					atomic.AddInt64(&merges, 1)
				}
				if j%9 == 0 {
					_, _ = os.Readlink(filepath.Join(mnt, statTargets[j%len(statTargets)]))
				}
				atomic.AddInt64(&ops, 1)
				j++
			}
		}(i)
	}
	// Committer: claude-style atomic save THROUGH THE MOUNT (write tmp + rename),
	// so the kernel's .claude.json vnode cache is invalidated each cycle and the
	// concurrent .claude.json stats are forced to revalidate (the noattrcache vs
	// attrcache difference shows up on that revalidation), while also exercising
	// the single-flight merge + write-through under a live invalidation stream.
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for {
			select {
			case <-stop:
				return
			case <-time.After(120 * time.Millisecond):
				n++
				tmp := filepath.Join(mnt, ".claude.json.tmp.ab12cd"+strconv.Itoa(n%90))
				if err := os.WriteFile(tmp, buildClaudeJSON(nProj, "acct-rev-"+strconv.Itoa(n)), 0o600); err != nil {
					continue
				}
				_ = os.Rename(tmp, filepath.Join(mnt, ".claude.json"))
			}
		}
	}()

	time.Sleep(warmup)
	atomic.StoreInt64(&ops, 0)
	atomic.StoreInt64(&merges, 0)
	t0 := time.Now()

	nfsCPU, selfCPU := sampleCPU(vol, os.Getpid(), dur-2*time.Second)
	nfsTop := sampleNFSStacks(vol)

	elapsed := time.Since(t0)
	close(stop)
	wg.Wait()

	got := atomic.LoadInt64(&ops)
	return armResult{
		ops:       got,
		opsPerSec: float64(got) / elapsed.Seconds(),
		nfsCPU:    nfsCPU,
		selfCPU:   selfCPU,
		merges:    atomic.LoadInt64(&merges),
		nfsTop:    nfsTop,
	}
}

// sampleCPU averages the %CPU of the mount's go-nfsv4 backend and the handler
// process over the window, using top's 1s-delta sample (one top call per pid —
// macOS top's -pid takes a single id, not a comma list).
func sampleCPU(vol string, selfPid int, window time.Duration) (nfs, self float64) {
	nfsPid := nfsv4Pid(vol)
	deadline := time.Now().Add(window)
	var nSum, sSum float64
	var n int
	for time.Now().Before(deadline) {
		sSum += topCPU(strconv.Itoa(selfPid))
		if nfsPid != "" {
			nSum += topCPU(nfsPid)
		}
		n++
	}
	if n == 0 {
		return 0, 0
	}
	return nSum / float64(n), sSum / float64(n)
}

// topCPU returns one pid's %CPU from the SECOND (1s-delta) sample of `top -l 2`.
func topCPU(pid string) float64 {
	out, _ := exec.Command("top", "-l", "2", "-s", "1", "-stats", "pid,cpu", "-pid", pid).CombinedOutput() //nolint:gosec // G204: a fixed diagnostic subprocess in a perf harness test, not user input
	var cpu float64
	for _, ln := range strings.Split(string(out), "\n") {
		f := strings.Fields(ln)
		if len(f) >= 2 && f[0] == pid {
			if v, err := strconv.ParseFloat(f[1], 64); err == nil {
				cpu = v // last match wins → the 2nd (delta) sample
			}
		}
	}
	return cpu
}

func nfsv4Pid(vol string) string {
	out, _ := exec.Command("pgrep", "-f", "go-nfsv4.*"+vol).CombinedOutput() //nolint:gosec // G204: a fixed diagnostic subprocess in a perf harness test, not user input
	return strings.TrimSpace(strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0])
}

// sampleNFSStacks captures a short profile of the go-nfsv4 backend and returns
// its heaviest leaf frames (the "Sort by top of stack" tail).
func sampleNFSStacks(vol string) string {
	pid := nfsv4Pid(vol)
	if pid == "" {
		return "(go-nfsv4 not found)"
	}
	f := filepath.Join(os.TempDir(), "ccp_perf_nfs_"+vol+".txt")
	if err := exec.Command("sample", pid, "5", "-file", f).Run(); err != nil { //nolint:gosec // G204: a fixed diagnostic subprocess in a perf harness test, not user input
		return "(sample failed: " + err.Error() + ")"
	}
	b, err := os.ReadFile(f) //nolint:gosec // G304: path is under the test's own t.TempDir(), not external input
	if err != nil {
		return "(read sample failed)"
	}
	s := string(b)
	if i := strings.Index(s, "Sort by top of stack"); i >= 0 {
		tail := s[i:]
		if len(tail) > 1400 {
			tail = tail[:1400]
		}
		return tail
	}
	return "(no top-of-stack section)"
}

// buildSyntheticClaude lays a ~/.claude with the carve-out dirs/files and a
// sizable base ~/.claude.json sibling, returning base and the base-sibling path.
func buildSyntheticClaude(t *testing.T, nProj int) (base, baseCJ string) {
	home := t.TempDir()
	base = filepath.Join(home, ".claude")
	baseCJ = filepath.Join(home, ".claude.json")
	for _, name := range []string{"projects", "history", "todos", "shell-snapshots", "statsig"} {
		d := filepath.Join(base, name)
		if err := os.MkdirAll(d, 0o755); err != nil { //nolint:gosec // G301: dir perms are intentional for this test's fixture layout
			t.Fatal(err)
		}
		for k := 0; k < 5; k++ {
			_ = os.WriteFile(filepath.Join(d, "f"+strconv.Itoa(k)+".jsonl"), []byte("{}\n"), 0o644) //nolint:gosec // G306: perms are intentional for this test fixture file
		}
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte(`{}`), 0o644); err != nil { //nolint:gosec // G306: perms are intentional for this test fixture file
		t.Fatal(err)
	}
	if err := os.WriteFile(baseCJ, buildClaudeJSON(nProj, "base"), 0o600); err != nil {
		t.Fatal(err)
	}
	return base, baseCJ
}

// buildClaudeJSON renders a non-trivial .claude.json with nProj project entries
// so each merge does real Unmarshal/overlay/Marshal work.
func buildClaudeJSON(nProj int, tag string) []byte {
	doc := map[string]any{
		"theme":        "dark",
		"numStartups":  42,
		"oauthAccount": map[string]any{"accountUuid": tag},
	}
	projects := map[string]any{}
	for i := 0; i < nProj; i++ {
		projects["/Users/x/repo-"+strconv.Itoa(i)] = map[string]any{
			"history":                             []string{tag + "-h1", tag + "-h2", tag + "-h3", tag + "-h4"},
			"hasTrustDialogAccepted":              true,
			"hasClaudeMdExternalIncludesApproved": true,
			"lastSessionId":                       tag + "-sess-" + strconv.Itoa(i),
			"projectOnboardingSeenCount":          i,
		}
	}
	doc["projects"] = projects
	b, _ := json.Marshal(doc)
	return append(b, '\n')
}
