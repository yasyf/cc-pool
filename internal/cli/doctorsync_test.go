package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/hostsync"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/store"
	"github.com/yasyf/synckit/cregistry"
	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/syncservice"
)

type warnCall struct {
	label  string
	detail string
}

func captureWarns() (func(string, string), *[]warnCall) {
	var calls []warnCall
	return func(label, detail string) {
		calls = append(calls, warnCall{label, detail})
	}, &calls
}

// TestReportSyncGatedOnEnabled pins the composite gate: the sync section is
// silent (and probes nothing) unless the sync_enabled meta is "1", and a
// meta read failure is loud, never a silent skip.
func TestReportSyncGatedOnEnabled(t *testing.T) {
	cases := map[string]struct {
		meta    string // "" = unset
		wantRun bool
	}{
		"meta unset is silent and probes nothing": {meta: ""},
		"meta 0 is silent and probes nothing":     {meta: "0"},
		"meta 1 runs the sync section":            {meta: "1", wantRun: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if tc.meta != "" {
				if err := st.SetMeta(syncMetaKey, tc.meta); err != nil {
					t.Fatal(err)
				}
			}
			m := &pool.Manager{Store: st}
			probed := false
			swapVar(t, &syncSockCapabilities, func(context.Context, string) (syncservice.Capabilities, error) {
				probed = true
				return syncservice.Capabilities{}, errors.New("unexpected probe")
			})
			swapVar(t, &synckitdLookPath, func() (string, error) { return "", errors.New("not on PATH") })
			report, calls := captureReports()
			warnf, warns := captureWarns()

			if err := reportSync(t.Context(), m, nil, report, warnf); err != nil {
				t.Fatal(err)
			}

			if !tc.wantRun {
				if len(*calls) != 0 || len(*warns) != 0 || probed {
					t.Fatalf("disabled sync produced reports=%+v warns=%+v probed=%v, want total silence", *calls, *warns, probed)
				}
				return
			}
			// Enabled under an isolated HOME: socket missing (✗, stat gates the
			// probe), mesh warn (synckitd absent), fresh empty registry (✓).
			if probed {
				t.Error("capabilities probed despite the socket file missing; the stat must gate the probe")
			}
			if len(*calls) != 2 {
				t.Fatalf("got %d reports %+v, want socket + registry", len(*calls), *calls)
			}
			if got := (*calls)[0]; got.label != "sync socket" || got.healthy || !strings.Contains(got.detail, "missing") {
				t.Errorf("report[0] = %+v, want unhealthy sync socket 'missing'", got)
			}
			if got := (*calls)[1]; got.label != "sync registry" || !got.healthy || got.detail != "0 accounts" {
				t.Errorf("report[1] = %+v, want healthy sync registry with 0 accounts", got)
			}
			if len(*warns) != 1 || (*warns)[0].label != "sync mesh" || !strings.Contains((*warns)[0].detail, "synckitd is not installed") {
				t.Errorf("warns = %+v, want exactly the absent-synckitd mesh warning", *warns)
			}
		})
	}

	t.Run("meta read failure is loud", func(t *testing.T) {
		st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatal(err)
		}
		_ = st.Close()
		report, calls := captureReports()
		warnf, _ := captureWarns()
		if err := reportSync(t.Context(), &pool.Manager{Store: st}, nil, report, warnf); err == nil {
			t.Fatalf("reportSync on a closed store returned nil, want an error (reports=%+v)", *calls)
		}
	})
}

// TestReportSyncUUIDDupes pins the ambiguous-uuid scan: one AccountsByUUID
// query per distinct non-empty uuid, a warning naming every duplicate row,
// and a loud failure on a store error.
func TestReportSyncUUIDDupes(t *testing.T) {
	acct := func(id int, uuid string) store.Account {
		return store.Account{ID: id, AccountUUID: uuid}
	}
	cases := map[string]struct {
		accts     []store.Account
		rows      map[string][]store.Account
		errs      map[string]error
		wantCalls []string
		wantWarns [][]string // per warn, fragments the detail must contain
		wantBad   []string   // per unhealthy report, a fragment
	}{
		"unique uuids are silent": {
			accts:     []store.Account{acct(1, "u1"), acct(2, "u2")},
			rows:      map[string][]store.Account{"u1": {acct(1, "u1")}, "u2": {acct(2, "u2")}},
			wantCalls: []string{"u1", "u2"},
		},
		"empty uuids are never queried": {
			accts: []store.Account{acct(1, ""), acct(2, "")},
		},
		"duplicate rows warn once naming every account": {
			accts: []store.Account{acct(1, "u1"), acct(2, "u1"), acct(3, "u2")},
			rows: map[string][]store.Account{
				"u1": {acct(1, "u1"), acct(2, "u1")},
				"u2": {acct(3, "u2")},
			},
			wantCalls: []string{"u1", "u2"},
			wantWarns: [][]string{{"acct-01, acct-02", "u1", "refuses ambiguous uuids", "wedges", "ccp remove"}},
		},
		"two duplicate groups warn twice": {
			accts: []store.Account{acct(1, "u1"), acct(2, "u1"), acct(3, "u2"), acct(4, "u2")},
			rows: map[string][]store.Account{
				"u1": {acct(1, "u1"), acct(2, "u1")},
				"u2": {acct(3, "u2"), acct(4, "u2")},
			},
			wantCalls: []string{"u1", "u2"},
			wantWarns: [][]string{{"acct-01, acct-02", "u1"}, {"acct-03, acct-04", "u2"}},
		},
		"store error fails the check loud": {
			accts:     []store.Account{acct(1, "u1")},
			errs:      map[string]error{"u1": errors.New("db exploded")},
			wantCalls: []string{"u1"},
			wantBad:   []string{"db exploded"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var got []string
			byUUID := func(uuid string) ([]store.Account, error) {
				got = append(got, uuid)
				if err := tc.errs[uuid]; err != nil {
					return nil, err
				}
				return tc.rows[uuid], nil
			}
			report, calls := captureReports()
			warnf, warns := captureWarns()

			reportSyncUUIDDupes(tc.accts, byUUID, report, warnf)

			if len(got) != len(tc.wantCalls) {
				t.Fatalf("byUUID called with %v, want %v", got, tc.wantCalls)
			}
			for i, uuid := range tc.wantCalls {
				if got[i] != uuid {
					t.Errorf("byUUID call[%d] = %q, want %q", i, got[i], uuid)
				}
			}
			if len(*warns) != len(tc.wantWarns) {
				t.Fatalf("got %d warns %+v, want %d", len(*warns), *warns, len(tc.wantWarns))
			}
			for i, frags := range tc.wantWarns {
				w := (*warns)[i]
				if w.label != "sync uuids" {
					t.Errorf("warn[%d] label = %q, want %q", i, w.label, "sync uuids")
				}
				for _, frag := range frags {
					if !strings.Contains(w.detail, frag) {
						t.Errorf("warn[%d] detail %q missing %q", i, w.detail, frag)
					}
				}
			}
			if len(*calls) != len(tc.wantBad) {
				t.Fatalf("got %d reports %+v, want %d", len(*calls), *calls, len(tc.wantBad))
			}
			for i, frag := range tc.wantBad {
				c := (*calls)[i]
				if c.label != "sync uuids" || c.healthy || !strings.Contains(c.detail, frag) {
					t.Errorf("report[%d] = %+v, want unhealthy sync uuids containing %q", i, c, frag)
				}
			}
		})
	}
}

// TestReportSyncSocket pins the socket check: a missing socket fails without
// probing, an answering one is healthy naming the consumer, and a probe
// error fails with the restart hint.
func TestReportSyncSocket(t *testing.T) {
	t.Run("missing socket fails without probing", func(t *testing.T) {
		sock := filepath.Join(t.TempDir(), "sync.sock")
		swapVar(t, &syncSockCapabilities, func(context.Context, string) (syncservice.Capabilities, error) {
			t.Error("probed a missing socket")
			return syncservice.Capabilities{}, nil
		})
		report, calls := captureReports()
		reportSyncSocket(t.Context(), sock, report)
		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.label != "sync socket" || got.healthy {
			t.Fatalf("report = %+v, want an unhealthy sync socket line", got)
		}
		for _, frag := range []string{"missing", "ccp service status"} {
			if !strings.Contains(got.detail, frag) {
				t.Errorf("detail %q missing %q", got.detail, frag)
			}
		}
	})

	t.Run("answering socket is healthy naming consumer and protocol", func(t *testing.T) {
		sock := filepath.Join(t.TempDir(), "sync.sock")
		if err := os.WriteFile(sock, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		var probedSock string
		swapVar(t, &syncSockCapabilities, func(_ context.Context, s string) (syncservice.Capabilities, error) {
			probedSock = s
			return syncservice.DefaultCapabilities("cc-pool"), nil
		})
		report, calls := captureReports()
		reportSyncSocket(t.Context(), sock, report)
		if probedSock != sock {
			t.Errorf("probed %q, want %q", probedSock, sock)
		}
		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.label != "sync socket" || !got.healthy || got.detail != "cc-pool protocol v1" {
			t.Errorf("report = %+v, want healthy 'cc-pool protocol v1'", got)
		}
	})

	t.Run("probe error fails with the restart hint", func(t *testing.T) {
		sock := filepath.Join(t.TempDir(), "sync.sock")
		if err := os.WriteFile(sock, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		swapVar(t, &syncSockCapabilities, func(context.Context, string) (syncservice.Capabilities, error) {
			return syncservice.Capabilities{}, errors.New("connection refused")
		})
		report, calls := captureReports()
		reportSyncSocket(t.Context(), sock, report)
		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.label != "sync socket" || got.healthy {
			t.Fatalf("report = %+v, want an unhealthy sync socket line", got)
		}
		for _, frag := range []string{"svc.capabilities", "connection refused", "restart the daemon"} {
			if !strings.Contains(got.detail, frag) {
				t.Errorf("detail %q missing %q", got.detail, frag)
			}
		}
	})
}

// TestReportSyncMesh pins the best-effort mesh check: every degraded state is
// a warning — never a doctor failure — and only a live synckitd with at least
// one peer reads healthy.
func TestReportSyncMesh(t *testing.T) {
	cases := map[string]struct {
		onPath        bool
		mesh          *hostregistry.Registry
		meshErr       error
		live          bool
		wantLiveProbe bool
		wantWarn      []string // fragments; nil = no warn
		wantOK        string   // healthy detail; "" = no healthy report
	}{
		"absent synckitd warns with the install hint": {
			wantWarn: []string{"synckitd is not installed", "brew install yasyf/tap/synckit"},
		},
		"unreadable mesh state warns with the error": {
			onPath:   true,
			meshErr:  errors.New("corrupt state.json"),
			wantWarn: []string{"corrupt state.json"},
		},
		"peerless mesh warns with the host add hint": {
			onPath:   true,
			mesh:     &hostregistry.Registry{Self: "mac-a"},
			wantWarn: []string{"no peers", "synckitd host add"},
		},
		"peers but dead synckitd warns stalled": {
			onPath:        true,
			mesh:          &hostregistry.Registry{Self: "mac-a", Hosts: []string{"mac-b"}},
			wantLiveProbe: true,
			wantWarn:      []string{"1 peer", "not running", "synckitd install"},
		},
		"live synckitd with peers is healthy": {
			onPath:        true,
			mesh:          &hostregistry.Registry{Self: "mac-a", Hosts: []string{"mac-b", "mac-c"}},
			live:          true,
			wantLiveProbe: true,
			wantOK:        "self mac-a; 2 peers",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			meshLoads, liveProbes := 0, 0
			swapVar(t, &synckitdLookPath, func() (string, error) {
				if tc.onPath {
					return "/opt/homebrew/bin/synckitd", nil
				}
				return "", errors.New("not on PATH")
			})
			swapVar(t, &loadMeshRegistry, func() (*hostregistry.Registry, error) {
				meshLoads++
				return tc.mesh, tc.meshErr
			})
			swapVar(t, &synckitdLive, func(context.Context) bool {
				liveProbes++
				return tc.live
			})
			report, calls := captureReports()
			warnf, warns := captureWarns()

			reportSyncMesh(t.Context(), report, warnf)

			// The WARN-not-FAIL pin: the mesh check never reports unhealthy.
			for _, c := range *calls {
				if !c.healthy {
					t.Errorf("mesh check reported unhealthy %+v; degraded mesh states must warn, never fail", c)
				}
			}
			wantLoads := 0
			if tc.onPath {
				wantLoads = 1
			}
			wantProbes := 0
			if tc.wantLiveProbe {
				wantProbes = 1
			}
			if meshLoads != wantLoads || liveProbes != wantProbes {
				t.Errorf("mesh loaded %d times (want %d), liveness probed %d (want %d)", meshLoads, wantLoads, liveProbes, wantProbes)
			}
			if tc.wantOK != "" {
				if len(*warns) != 0 {
					t.Errorf("healthy mesh produced warns %+v", *warns)
				}
				if len(*calls) != 1 || (*calls)[0].label != "sync mesh" || !(*calls)[0].healthy || (*calls)[0].detail != tc.wantOK {
					t.Fatalf("got %+v, want healthy sync mesh %q", *calls, tc.wantOK)
				}
				return
			}
			if len(*calls) != 0 {
				t.Errorf("degraded mesh produced reports %+v, want warnings only", *calls)
			}
			if len(*warns) != 1 || (*warns)[0].label != "sync mesh" {
				t.Fatalf("got %d warns %+v, want exactly one sync mesh warning", len(*warns), *warns)
			}
			for _, frag := range tc.wantWarn {
				if !strings.Contains((*warns)[0].detail, frag) {
					t.Errorf("warn detail %q missing %q", (*warns)[0].detail, frag)
				}
			}
		})
	}
}

// TestReportSyncRegistry pins the registry load-health check: absent reads as
// a fresh empty pool, tombstones are excluded from the healthy count, and a
// corrupt file is a loud failure explaining the fail-open refresh gate.
func TestReportSyncRegistry(t *testing.T) {
	tempRF := func(t *testing.T) hostsync.RegistryFile {
		t.Helper()
		dir := t.TempDir()
		return hostsync.RegistryFile{
			Path:     filepath.Join(dir, "registry.json"),
			LockPath: filepath.Join(dir, "registry.lock"),
		}
	}

	t.Run("absent registry is a fresh empty pool", func(t *testing.T) {
		report, calls := captureReports()
		reportSyncRegistry(tempRF(t), report)
		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.label != "sync registry" || !got.healthy || got.detail != "0 accounts" {
			t.Errorf("report = %+v, want healthy '0 accounts'", got)
		}
	})

	t.Run("counts only present accounts", func(t *testing.T) {
		rf := tempRF(t)
		reg := cregistry.New[hostsync.AccountValue]()
		reg.Add("u1", hostsync.AccountValue{UUID: "u1"}, 100)
		reg.Add("u2", hostsync.AccountValue{UUID: "u2"}, 100)
		reg.Remove("u2", 200) // tombstoned: not a live account
		if err := rf.Save(reg); err != nil {
			t.Fatal(err)
		}
		report, calls := captureReports()
		reportSyncRegistry(rf, report)
		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.label != "sync registry" || !got.healthy || got.detail != "1 account" {
			t.Errorf("report = %+v, want healthy '1 account' (tombstone excluded)", got)
		}
	})

	t.Run("corrupt registry fails loud with the fail-open explanation", func(t *testing.T) {
		rf := tempRF(t)
		if err := os.WriteFile(rf.Path, []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		report, calls := captureReports()
		reportSyncRegistry(rf, report)
		if len(*calls) != 1 {
			t.Fatalf("got %d reports %+v, want exactly one", len(*calls), *calls)
		}
		got := (*calls)[0]
		if got.label != "sync registry" || got.healthy {
			t.Fatalf("report = %+v, want an unhealthy sync registry line", got)
		}
		for _, frag := range []string{"parse registry", "FAILS OPEN", "double-spending", "ccp sync enable"} {
			if !strings.Contains(got.detail, frag) {
				t.Errorf("detail %q missing %q", got.detail, frag)
			}
		}
	})
}
