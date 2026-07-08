package daemon

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/procscan"
)

// TestMaintainerTableOrder pins each table's declared row order (and per-row claim
// scope) by name. The tick runners execute rows in table order, so a reorder is a
// behavior change a reviewer must confront — this test makes it a diff.
func TestMaintainerTableOrder(t *testing.T) {
	type row struct {
		name  string
		scope claimScope
	}
	cases := []struct {
		table string
		got   []maintainer
		want  []row
	}{
		{"poll", pollTable, []row{
			{"holder.refresh", claimNone},
			{"session.reconcile", claimNone},
			{"widget.stale", claimNone},
			{"sticky.prune", claimNone},
			{"account.poll", claimPerAccount},
			{"status.snapshot", claimNone},
		}},
		{"heal", healTable, []row{
			{"holder.refresh", claimNone},
			{"fp.app.ensure", claimNone},
			{"fuse.remount", claimPerAccount},
			{"fp.heal", claimPerAccount},
			{"fp.orphan.reap", claimNone},
			{"strand.heal", claimPerAccount},
			{"content.health", claimNone},
		}},
		{"startup", startupTable, []row{
			{"bridge.content", claimNone},
			{"bridge.fp", claimNone},
			{"holder.refresh", claimNone},
			{"ua.detect", claimNone},
			{"fp.app.ensure", claimNone},
			{"overlays.reconcile", claimPerAccount},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.table, func(t *testing.T) {
			if len(tc.got) != len(tc.want) {
				t.Fatalf("%s table has %d rows, want %d: %v", tc.table, len(tc.got), len(tc.want), rowNames(tc.got))
			}
			for i, want := range tc.want {
				if tc.got[i].name != want.name {
					t.Errorf("%s row %d = %q, want %q (full order: %v)", tc.table, i, tc.got[i].name, want.name, rowNames(tc.got))
				}
				if tc.got[i].claimScope != want.scope {
					t.Errorf("%s row %q claimScope = %d, want %d", tc.table, want.name, tc.got[i].claimScope, want.scope)
				}
				if tc.got[i].run == nil {
					t.Errorf("%s row %q has a nil run", tc.table, want.name)
				}
			}
		})
	}
}

// TestPollTableHasNoFPRow pins the structural exclusion: File Provider recovery is
// owned by the backoff-gated heal ticker, never the poll pass (an inline FP
// reconcile on every poll is the reconcile storm the exclusion prevents).
func TestPollTableHasNoFPRow(t *testing.T) {
	for _, m := range pollTable {
		if strings.HasPrefix(m.name, "fp.") {
			t.Errorf("poll table must contain no FP row (FP heal is heal-ticker-only): found %q", m.name)
		}
	}
}

// TestTickScanFailAllBusy pins the single home of the scan-fail-means-all-busy
// rule: a failed scan makes idle false for EVERY dir (and a clean scan reports
// per-dir liveness).
func TestTickScanFailAllBusy(t *testing.T) {
	dirs := []string{"/pool/acct-01", "/pool/acct-02", "/pool/acct-03"}

	t.Run("scan failure fails closed to busy everywhere", func(t *testing.T) {
		var buf strings.Builder
		s := &Server{log: log.New(&buf, "", 0), scanSessions: func(context.Context) ([]procscan.Session, error) {
			return nil, errors.New("ps exploded")
		}}
		tk := s.newTick(context.Background())
		if tk.scanOK() {
			t.Fatal("a failed scan must report scanOK false")
		}
		for _, dir := range dirs {
			if tk.idle(dir) {
				t.Errorf("scan failure must make idle(%q) false (fail closed)", dir)
			}
			if n := tk.sessionCount(dir); n != 0 {
				t.Errorf("scan failure sessionCount(%q) = %d, want 0", dir, n)
			}
		}
		if !strings.Contains(buf.String(), "procscan failed") {
			t.Errorf("scan failure must log once; got %q", buf.String())
		}
	})

	t.Run("clean empty scan is idle everywhere", func(t *testing.T) {
		s := &Server{log: log.New(io.Discard, "", 0), scanSessions: func(context.Context) ([]procscan.Session, error) {
			return nil, nil
		}}
		tk := s.newTick(context.Background())
		if !tk.scanOK() {
			t.Fatal("a clean scan must report scanOK true")
		}
		for _, dir := range dirs {
			if !tk.idle(dir) {
				t.Errorf("empty clean scan must make idle(%q) true", dir)
			}
		}
	})

	t.Run("clean scan reports per-dir liveness", func(t *testing.T) {
		s := &Server{log: log.New(io.Discard, "", 0), scanSessions: func(context.Context) ([]procscan.Session, error) {
			return []procscan.Session{{PID: 4242, ConfigDir: dirs[0]}}, nil
		}}
		tk := s.newTick(context.Background())
		if tk.idle(dirs[0]) {
			t.Errorf("idle(%q) with a live session must be false", dirs[0])
		}
		if n := tk.sessionCount(dirs[0]); n != 1 {
			t.Errorf("sessionCount(%q) = %d, want 1", dirs[0], n)
		}
		if !tk.idle(dirs[1]) {
			t.Errorf("idle(%q) with no session must be true", dirs[1])
		}
	})
}

func rowNames(table []maintainer) []string {
	out := make([]string, len(table))
	for i, m := range table {
		out[i] = m.name
	}
	return out
}
