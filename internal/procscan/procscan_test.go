package procscan

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// procargs2 builds a KERN_PROCARGS2 buffer: uint32 argc, the NUL-terminated
// executable path, alignment padding, argc NUL-terminated argv strings, then
// NUL-terminated env strings ended by an empty string — the exact shape the
// kernel hands back and parseProcArgs decodes.
func procargs2(argc uint32, execPath string, argv, env []string) []byte {
	b := make([]byte, 4)
	binary.NativeEndian.PutUint32(b, argc)
	b = append(b, execPath...)
	b = append(b, 0)
	b = append(b, 0, 0) // alignment padding
	for _, a := range argv {
		b = append(b, a...)
		b = append(b, 0)
	}
	for _, e := range env {
		b = append(b, e...)
		b = append(b, 0)
	}
	b = append(b, 0) // empty string terminates the environment
	return b
}

func TestParseProcArgs(t *testing.T) {
	cases := map[string]struct {
		buf        []byte
		wantArgv0  string
		wantCfgDir string
	}{
		"pool session": {
			buf: procargs2(2,
				"/Users/me/.local/bin/claude",
				[]string{"/Users/me/.local/bin/claude", "--session-id=abc"},
				[]string{"FOO=bar", "CLAUDE_CONFIG_DIR=/Users/me/Library/CloudStorage/CCPoolStatus-acct-01", "PATH=/usr/bin"}),
			wantArgv0:  "/Users/me/.local/bin/claude",
			wantCfgDir: "/Users/me/Library/CloudStorage/CCPoolStatus-acct-01",
		},
		"plain claude (no CLAUDE_CONFIG_DIR)": {
			buf: procargs2(1,
				"/opt/homebrew/bin/claude",
				[]string{"claude"},
				[]string{"PATH=/usr/bin", "LANG=en_US.UTF-8"}),
			wantArgv0:  "claude",
			wantCfgDir: "",
		},
		"explicit ~/.claude": {
			buf: procargs2(1,
				"/Users/me/.local/bin/claude",
				[]string{"/Users/me/.local/bin/claude"},
				[]string{"CLAUDE_CONFIG_DIR=/Users/me/.claude"}),
			wantArgv0:  "/Users/me/.local/bin/claude",
			wantCfgDir: "/Users/me/.claude",
		},
		"non-claude node child carries CLAUDE_CONFIG_DIR": {
			buf: procargs2(3,
				"/usr/bin/node",
				[]string{"/usr/bin/node", "/some/script.js", "--flag"},
				[]string{"CLAUDE_CONFIG_DIR=/Users/me/Library/CloudStorage/CCPoolStatus-acct-03"}),
			wantArgv0:  "/usr/bin/node",
			wantCfgDir: "/Users/me/Library/CloudStorage/CCPoolStatus-acct-03",
		},
		"no env block": {
			buf:        procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, nil),
			wantArgv0:  "claude",
			wantCfgDir: "",
		},
		"buffer too short for argc": {
			buf:        []byte{0x01, 0x00},
			wantArgv0:  "",
			wantCfgDir: "",
		},
		"exec path never terminates": {
			buf: append(
				binary.NativeEndian.AppendUint32(nil, 1),
				[]byte("/opt/homebrew/bin/claude")...), // no trailing NUL
			wantArgv0:  "",
			wantCfgDir: "",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			argv0, cfgDir := parseProcArgs(tc.buf)
			if argv0 != tc.wantArgv0 {
				t.Errorf("argv0 = %q, want %q", argv0, tc.wantArgv0)
			}
			if cfgDir != tc.wantCfgDir {
				t.Errorf("configDir = %q, want %q", cfgDir, tc.wantCfgDir)
			}
		})
	}
}

// TestParseProcArgsTruncatedEnv proves a buffer cut mid-environment keeps a
// recoverable argv0 and the CLAUDE_CONFIG_DIR seen before the cut, without
// panicking on the missing terminator.
func TestParseProcArgsTruncatedEnv(t *testing.T) {
	full := procargs2(1,
		"/Users/me/.local/bin/claude",
		[]string{"/Users/me/.local/bin/claude"},
		[]string{"CLAUDE_CONFIG_DIR=/pool/acct-07", "PATH=/usr/bin/very/long/tail"})
	// Drop the trailing bytes so the final env string loses its NUL terminator.
	truncated := full[:len(full)-8]

	argv0, cfgDir := parseProcArgs(truncated)
	if argv0 != "/Users/me/.local/bin/claude" {
		t.Errorf("argv0 = %q, want the claude path", argv0)
	}
	if cfgDir != "/pool/acct-07" {
		t.Errorf("configDir = %q, want /pool/acct-07 (seen before the cut)", cfgDir)
	}

	// Cut before argv[0]'s terminator: argv0 is unrecoverable, no panic.
	early := full[:6]
	if argv0, cfgDir := parseProcArgs(early); argv0 != "" || cfgDir != "" {
		t.Errorf("early cut = (%q, %q), want empty", argv0, cfgDir)
	}
}

// canScan swaps both process-table seams for canned data and restores them.
func canScan(t *testing.T, procs []proc, args map[int][]byte, argErr map[int]error) {
	t.Helper()
	origList, origArgs := listProcs, procArgs
	t.Cleanup(func() { listProcs, procArgs = origList, origArgs })
	listProcs = func(context.Context) ([]proc, error) { return procs, nil }
	procArgs = func(_ context.Context, pid int) ([]byte, error) {
		if err := argErr[pid]; err != nil {
			return nil, err
		}
		return args[pid], nil
	}
}

func TestScan(t *testing.T) {
	start := time.Date(2026, 6, 12, 9, 30, 0, 0, time.UTC)
	procs := []proc{
		{pid: 501, startedAt: start},
		{pid: 777, startedAt: start.Add(-5 * time.Minute)},
		{pid: 888, startedAt: start.Add(-time.Hour)},
		{pid: 999, startedAt: start.Add(-2 * time.Hour)},
		{pid: 1010, startedAt: start.Add(-3 * time.Hour)},
	}
	args := map[int][]byte{
		501: procargs2(1, "/Users/me/.local/bin/claude", []string{"/Users/me/.local/bin/claude"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/Library/CloudStorage/CCPoolStatus-acct-01"}),
		777: procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, []string{"PATH=/usr/bin"}),
		888: procargs2(3, "/opt/homebrew/bin/fish", []string{"fish", "-c", "claude"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/Library/CloudStorage/CCPoolStatus-acct-02"}),
		999: procargs2(2, "/usr/bin/node", []string{"/usr/bin/node", "/some/script.js"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/Library/CloudStorage/CCPoolStatus-acct-03"}),
		1010: procargs2(1, "/Users/me/.local/bin/claude", []string{"/Users/me/.local/bin/claude"},
			[]string{"CLAUDE_CONFIG_DIR=/Users/me/.claude"}),
	}
	canScan(t, procs, args, nil)

	got, err := Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d sessions, want 3: %+v", len(got), got)
	}
	byPID := map[int]Session{}
	for _, s := range got {
		byPID[s.PID] = s
	}
	if byPID[501].ConfigDir != "/Users/me/Library/CloudStorage/CCPoolStatus-acct-01" {
		t.Errorf("pid 501 dir = %q", byPID[501].ConfigDir)
	}
	if !byPID[501].StartedAt.Equal(start) {
		t.Errorf("pid 501 StartedAt = %v, want %v", byPID[501].StartedAt, start)
	}
	if s, ok := byPID[777]; !ok || s.ConfigDir != "" {
		t.Errorf("pid 777 should be present with empty config dir, got %q ok=%v", s.ConfigDir, ok)
	}
	if byPID[1010].ConfigDir != "/Users/me/.claude" {
		t.Errorf("pid 1010 dir = %q", byPID[1010].ConfigDir)
	}
	if _, ok := byPID[888]; ok {
		t.Error("fish wrapper should be excluded")
	}
	if _, ok := byPID[999]; ok {
		t.Error("node child should be excluded even though it carries CLAUDE_CONFIG_DIR")
	}
}

func TestScanSnapshotUsesOneEnumerationForSessionsAndAllProcesses(t *testing.T) {
	start := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	origList, origArgs := listProcs, procArgs
	t.Cleanup(func() { listProcs, procArgs = origList, origArgs })
	lists := 0
	listProcs = func(context.Context) ([]proc, error) {
		lists++
		return []proc{{pid: 101, startedAt: start}, {pid: 202, startedAt: start.Add(time.Second)}}, nil
	}
	procArgs = func(_ context.Context, pid int) ([]byte, error) {
		if pid == 101 {
			return procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, []string{"CLAUDE_CONFIG_DIR=/acct-01"}), nil
		}
		return procargs2(1, "/opt/homebrew/bin/ccp", []string{"ccp"}, nil), nil
	}

	snapshot, err := ScanSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if lists != 1 {
		t.Fatalf("process enumerations = %d, want 1", lists)
	}
	if len(snapshot.Sessions) != 1 || snapshot.Sessions[0].PID != 101 {
		t.Fatalf("Claude sessions = %+v", snapshot.Sessions)
	}
	if len(snapshot.Processes) != 2 || !snapshot.Processes[101].Equal(start) ||
		!snapshot.Processes[202].Equal(start.Add(time.Second)) {
		t.Fatalf("all process identities = %+v", snapshot.Processes)
	}
}

// TestScanFailsClosedOnListError pins the load-bearing fail-closed: a failed
// process enumeration surfaces as Scan's error, never a silent empty slice.
func TestScanFailsClosedOnListError(t *testing.T) {
	origList := listProcs
	t.Cleanup(func() { listProcs = origList })
	listProcs = func(context.Context) ([]proc, error) { return nil, errors.New("sysctl exploded") }

	if _, err := Scan(context.Background()); err == nil {
		t.Fatal("Scan must propagate a process-list failure (fail closed)")
	}
}

// TestScanSkipsGoneProcs proves a process that vanished (ESRCH), was reused by
// another uid mid-scan (EPERM), has no readable args (EINVAL), or exits during
// the kernel copyout (EIO) is skipped
// without failing the scan, while a live claude alongside it is still reported.
func TestScanSkipsGoneProcs(t *testing.T) {
	procs := []proc{{pid: 501}, {pid: 777}, {pid: 888}, {pid: 999}, {pid: 1000}}
	args := map[int][]byte{
		501: procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, nil),
	}
	argErr := map[int]error{777: unix.ESRCH, 888: unix.EPERM, 999: unix.EINVAL, 1000: unix.EIO}
	canScan(t, procs, args, argErr)

	got, err := Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 501 {
		t.Fatalf("got %+v, want only pid 501", got)
	}
}

// TestScanFailsClosedOnUnexpectedArgError proves an unexpected per-PID read
// error (not ESRCH/EINVAL/EPERM/EIO) fails the whole scan closed rather than
// silently dropping a process we could not classify.
func TestScanFailsClosedOnUnexpectedArgError(t *testing.T) {
	procs := []proc{{pid: 501}}
	argErr := map[int]error{501: errors.New("unexpected procargs failure")}
	canScan(t, procs, nil, argErr)

	if _, err := Scan(context.Background()); err == nil {
		t.Fatal("an unexpected procargs error must fail the scan closed")
	}
}

func TestCountByConfigDir(t *testing.T) {
	sessions := []Session{
		{PID: 501, ConfigDir: "/Users/me/Library/CloudStorage/CCPoolStatus-acct-01"},
		{PID: 777, ConfigDir: ""},
		{PID: 1010, ConfigDir: "/Users/me/.claude"},
	}
	cases := map[string]struct {
		configDir string
		want      int
	}{
		"exact match":           {"/Users/me/Library/CloudStorage/CCPoolStatus-acct-01", 1},
		"no sessions for dir":   {"/Users/me/Library/CloudStorage/CCPoolStatus-acct-99", 0},
		"explicit ~/.claude":    {"/Users/me/.claude", 1},
		"empty matches nothing": {"", 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if n := CountByConfigDir(sessions, tc.configDir); n != tc.want {
				t.Errorf("CountByConfigDir(%q) = %d, want %d", tc.configDir, n, tc.want)
			}
		})
	}
}

func TestClaudeProcesses(t *testing.T) {
	a := time.Unix(10, 0)
	b := time.Unix(20, 0)
	sessions := []Session{{PID: 501, StartedAt: a}, {PID: 777, StartedAt: b}}
	alive := ClaudeProcesses(sessions)
	if !alive[501].Equal(a) || !alive[777].Equal(b) {
		t.Errorf("ClaudeProcesses = %v", alive)
	}
	if _, ok := alive[123]; ok {
		t.Error("ClaudeProcesses must not report an absent pid")
	}
}

func TestScanReturnsContextCancellationWithoutAbandoningWork(t *testing.T) {
	origList, origArgs, origTimeout := listProcs, procArgs, scanTimeout
	t.Cleanup(func() {
		listProcs, procArgs, scanTimeout = origList, origArgs, origTimeout
	})

	scanTimeout = 20 * time.Millisecond
	listProcs = func(context.Context) ([]proc, error) { return []proc{{pid: 4242}}, nil }
	procArgs = func(ctx context.Context, _ int) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	_, err := Scan(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Scan err = %v, want context.DeadlineExceeded", err)
	}
}

func TestParseExecPath(t *testing.T) {
	cases := map[string]struct {
		buf  []byte
		want string
	}{
		"normal buffer": {
			buf:  procargs2(1, "/Applications/A.app/Contents/MacOS/A", []string{"A"}, nil),
			want: "/Applications/A.app/Contents/MacOS/A",
		},
		"too short for argc":         {buf: []byte{1, 0}, want: ""},
		"exec path never terminates": {buf: append([]byte{1, 0, 0, 0}, []byte("/no/nul")...), want: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := parseExecPath(tc.buf); got != tc.want {
				t.Fatalf("parseExecPath = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProcsByExecutable(t *testing.T) {
	start := time.Date(2026, 7, 4, 16, 18, 9, 0, time.UTC)
	widget := "/Users/test/Applications/CCPoolStatus.app/Contents/PlugIns/CCPoolStatusWidget.appex/Contents/MacOS/CCPoolStatusWidget"
	procs := []proc{{pid: 100, startedAt: start}, {pid: 200, startedAt: start.Add(time.Hour)}}
	args := map[int][]byte{
		100: procargs2(1, widget, []string{"CCPoolStatusWidget"}, nil),
		200: procargs2(1, "/opt/homebrew/bin/claude", []string{"claude"}, nil),
	}
	canScan(t, procs, args, nil)

	got, err := ProcsByExecutable(context.Background(), widget)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 100 || !got[0].StartedAt.Equal(start) {
		t.Fatalf("got %+v, want only pid 100 @ %s", got, start)
	}
}

// TestProcsByExecutableEmptyPath proves an empty path matches nothing — not
// every process whose PROCARGS2 buffer fails to parse into an exec path.
func TestProcsByExecutableEmptyPath(t *testing.T) {
	canScan(t, []proc{{pid: 100}}, map[int][]byte{100: {1, 0}}, nil)

	got, err := ProcsByExecutable(context.Background(), "")
	if err != nil || len(got) != 0 {
		t.Fatalf("got %+v, %v; want none, nil", got, err)
	}
}

func TestProcsByExecutableSkipsGoneProcs(t *testing.T) {
	widget := "/w"
	procs := []proc{{pid: 100}, {pid: 200}, {pid: 300}, {pid: 400}, {pid: 500}}
	args := map[int][]byte{100: procargs2(1, widget, []string{"w"}, nil)}
	argErr := map[int]error{200: unix.ESRCH, 300: unix.EINVAL, 400: unix.EPERM, 500: unix.EIO}
	canScan(t, procs, args, argErr)

	got, err := ProcsByExecutable(context.Background(), widget)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 100 {
		t.Fatalf("got %+v, want only pid 100", got)
	}
}

func TestProcsByExecutableFailsClosed(t *testing.T) {
	t.Run("list error", func(t *testing.T) {
		origList := listProcs
		t.Cleanup(func() { listProcs = origList })
		listProcs = func(context.Context) ([]proc, error) { return nil, errors.New("sysctl exploded") }

		if _, err := ProcsByExecutable(context.Background(), "/w"); err == nil {
			t.Fatal("ProcsByExecutable must propagate a process-list failure (fail closed)")
		}
	})
	t.Run("unexpected arg error", func(t *testing.T) {
		canScan(t, []proc{{pid: 100}}, nil, map[int]error{100: errors.New("unexpected procargs failure")})

		if _, err := ProcsByExecutable(context.Background(), "/w"); err == nil {
			t.Fatal("an unexpected procargs error must fail the walk closed")
		}
	})
}
