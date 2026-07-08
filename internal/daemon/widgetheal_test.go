package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/procscan"
)

// widgetBinary writes a fake appex binary and returns its path and ctime.
func widgetBinary(t *testing.T) (string, time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "CCPoolStatusWidget")
	if err := os.WriteFile(path, []byte("mach-o"), 0o600); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	ct := fi.Sys().(*syscall.Stat_t).Ctimespec
	return path, time.Unix(ct.Sec, ct.Nsec)
}

// swapWidgetSeams points the reconcile seams at canned scans and a recorded
// kill; scans are consumed one per call, the last repeating.
func swapWidgetSeams(t *testing.T, binPath string, scans [][]procscan.Proc) (killed *[]int, scanCalls *int) {
	t.Helper()
	origList, origPath, origKill := listWidgetProcs, widgetBinaryPath, killPID
	t.Cleanup(func() { listWidgetProcs, widgetBinaryPath, killPID = origList, origPath, origKill })
	widgetBinaryPath = func() string { return binPath }
	killed, scanCalls = &[]int{}, new(int)
	listWidgetProcs = func(context.Context, string) ([]procscan.Proc, error) {
		i := *scanCalls
		if i >= len(scans) {
			i = len(scans) - 1
		}
		*scanCalls++
		return scans[i], nil
	}
	killPID = func(pid int) error {
		*killed = append(*killed, pid)
		return nil
	}
	return killed, scanCalls
}

func TestStaleWidgetAppexes(t *testing.T) {
	bin, installedAt := widgetBinary(t)
	cases := map[string]struct {
		startedAt time.Time
		stale     bool
	}{
		"appex older than installed binary is flagged": {installedAt.Add(-time.Hour), true},
		"current-binary appex not flagged":             {installedAt.Add(time.Second), false},
		"start within slack not flagged":               {installedAt.Add(-2 * time.Second), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			swapWidgetSeams(t, bin, [][]procscan.Proc{{{PID: 42, StartedAt: tc.startedAt}}})

			got, err := StaleWidgetAppexes(context.Background(), bin)
			if err != nil {
				t.Fatal(err)
			}
			if tc.stale && (len(got) != 1 || got[0].PID != 42 || !got[0].StartedAt.Equal(tc.startedAt)) {
				t.Fatalf("got %+v, want pid 42 @ %s", got, tc.startedAt)
			}
			if !tc.stale && len(got) != 0 {
				t.Fatalf("got %+v, want none", got)
			}
		})
	}
}

// TestStaleWidgetAppexesMissingBinary proves an absent widget install is
// normal — no appexes, no error, no process scan.
func TestStaleWidgetAppexesMissingBinary(t *testing.T) {
	got, err := StaleWidgetAppexes(context.Background(), filepath.Join(t.TempDir(), "gone"))
	if err != nil || got != nil {
		t.Fatalf("got %+v, %v; want nil, nil", got, err)
	}
}

func TestStaleWidgetAppexesScanError(t *testing.T) {
	bin, _ := widgetBinary(t)
	orig := listWidgetProcs
	t.Cleanup(func() { listWidgetProcs = orig })
	listWidgetProcs = func(context.Context, string) ([]procscan.Proc, error) { return nil, errors.New("boom") }

	if _, err := StaleWidgetAppexes(context.Background(), bin); err == nil {
		t.Fatal("a failed process scan must propagate, not read as no stale appexes")
	}
}

func TestReconcileStaleWidgetKills(t *testing.T) {
	bin, installedAt := widgetBinary(t)
	stale := []procscan.Proc{{PID: 42, StartedAt: installedAt.Add(-time.Hour)}}
	killed, _ := swapWidgetSeams(t, bin, [][]procscan.Proc{stale, stale})
	var buf bytes.Buffer
	s := &Server{cl: newClaims(), log: log.New(&buf, "", 0)}

	s.reconcileStaleWidget(context.Background())

	if len(*killed) != 1 || (*killed)[0] != 42 {
		t.Fatalf("killed %v, want exactly [42]", *killed)
	}
	if !strings.Contains(buf.String(), "killed stale widget appex pid 42") {
		t.Fatalf("log %q missing the kill line", buf.String())
	}
}

// TestReconcileStaleWidgetPIDReuseGuard proves a pid whose start time changed
// between scan and reconfirm — a reused pid, even one that itself looks stale
// — is spared.
func TestReconcileStaleWidgetPIDReuseGuard(t *testing.T) {
	bin, installedAt := widgetBinary(t)
	killed, _ := swapWidgetSeams(t, bin, [][]procscan.Proc{
		{{PID: 42, StartedAt: installedAt.Add(-time.Hour)}},
		{{PID: 42, StartedAt: installedAt.Add(-30 * time.Minute)}},
	})
	s := &Server{cl: newClaims(), log: log.New(io.Discard, "", 0)}

	s.reconcileStaleWidget(context.Background())

	if len(*killed) != 0 {
		t.Fatalf("killed %v, want none: the pid was reused between scan and reconfirm", *killed)
	}
}

func TestReconcileStaleWidgetGoneOnReconfirm(t *testing.T) {
	bin, installedAt := widgetBinary(t)
	killed, _ := swapWidgetSeams(t, bin, [][]procscan.Proc{
		{{PID: 42, StartedAt: installedAt.Add(-time.Hour)}},
		{},
	})
	s := &Server{cl: newClaims(), log: log.New(io.Discard, "", 0)}

	s.reconcileStaleWidget(context.Background())

	if len(*killed) != 0 {
		t.Fatalf("killed %v, want none: the appex exited before the reconfirm", *killed)
	}
}

func TestReconcileStaleWidgetNoStale(t *testing.T) {
	bin, installedAt := widgetBinary(t)
	killed, scanCalls := swapWidgetSeams(t, bin, [][]procscan.Proc{
		{{PID: 7, StartedAt: installedAt.Add(time.Second)}},
	})
	s := &Server{cl: newClaims(), log: log.New(io.Discard, "", 0)}

	s.reconcileStaleWidget(context.Background())

	if len(*killed) != 0 {
		t.Fatalf("killed %v, want none", *killed)
	}
	if *scanCalls != 1 {
		t.Fatalf("scanned %d times, want 1: no reconfirm when nothing is stale", *scanCalls)
	}
}
