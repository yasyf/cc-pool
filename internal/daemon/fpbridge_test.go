package daemon

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/pool"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/fusekit/content"
)

// deadBridge binds the FP bridge socket with a listener that closes every
// connection unanswered — the bound-but-dead shape (dial succeeds, the content
// round-trip fails), distinct from an unbound socket (dial refused).
func deadBridge(t *testing.T, sock string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", sock)
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
			_ = conn.Close()
		}
	}()
}

func boundDeadStatus() FPBridgeStatus {
	return FPBridgeStatus{Verdict: FPBridgeBoundDead, Detail: fpBridgeBoundDeadLever}
}

func newBridgeLedgerServer() *Server {
	return &Server{led: newLedgers(), log: log.New(io.Discard, "", 0)}
}

// TestFPBridgeCheckClassify pins fpBridgeCheck's four verdicts through the real
// dial + SelfTest path (seam nil): a consent-pending bind parks WITHOUT dialing,
// an unbound socket is down, a bound-but-unanswering socket is bound-dead, and a
// live bridge over the real content source serves.
func TestFPBridgeCheckClassify(t *testing.T) {
	t.Run("consent-parked without dialing", func(t *testing.T) {
		shortHome(t)
		s := &Server{log: log.New(io.Discard, "", 0)}
		s.fpConsentPending.Store(true)
		// No socket is bound, so a dial would classify Down. ConsentParked proves
		// fpBridgeCheck short-circuits without touching the socket.
		st := s.fpBridgeCheck(context.Background())
		if st.Verdict != FPBridgeConsentParked {
			t.Fatalf("verdict = %q, want consent-parked (no dial while consent pending)", st.Verdict)
		}
		if st.Detail != fpBridgeConsentLever {
			t.Fatalf("detail = %q, want the consent lever", st.Detail)
		}
	})

	t.Run("down when the socket is unbound", func(t *testing.T) {
		shortHome(t)
		s := &Server{log: log.New(io.Discard, "", 0)}
		st := s.fpBridgeCheck(context.Background())
		if st.Verdict != FPBridgeDown || st.Detail != fpBridgeDownLever {
			t.Fatalf("verdict/detail = %q/%q, want down + the down lever", st.Verdict, st.Detail)
		}
	})

	t.Run("bound-dead when the socket answers nothing", func(t *testing.T) {
		shortHome(t)
		deadBridge(t, pool.FPBridgeSocketPath())
		s := &Server{log: log.New(io.Discard, "", 0)}
		st := s.fpBridgeCheck(context.Background())
		if st.Verdict != FPBridgeBoundDead || st.Detail != fpBridgeBoundDeadLever {
			t.Fatalf("verdict/detail = %q/%q, want bound-dead + the bound-dead lever", st.Verdict, st.Detail)
		}
	})

	t.Run("serving over a live bridge", func(t *testing.T) {
		shortHome(t)
		if err := os.MkdirAll(pool.ClaudeDir(), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(pool.ClaudeDir(), "settings.json"), []byte(`{}`), 0o600); err != nil {
			t.Fatal(err)
		}
		s := &Server{
			log:           log.New(io.Discard, "", 0),
			contentSource: overlay.NewPoolContentSource(pool.ClaudeDir(), pool.ClaudeJSONPath(), filepath.Join(pool.StateDir(), "content-stamps")),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.startFPBridge(ctx)
		waitFor(t, 2*time.Second, "the FP bridge to bind", func() bool {
			return content.NewBridgeClient(pool.FPBridgeSocketPath()).Available()
		})
		st := s.fpBridgeCheck(ctx)
		if st.Verdict != FPBridgeServing {
			t.Fatalf("verdict = %q (detail %q), want serving", st.Verdict, st.Detail)
		}
		if st.Detail != "" {
			t.Fatalf("serving verdict carried a lever %q, want none", st.Detail)
		}
		cancel()
		s.wg.Wait()
	})
}

// TestRecordFPBridgeVerdict pins the 2-strike debounce: one not-serving verdict
// never faults, the second latches, and a serving verdict clears the row. Every
// not-serving verdict strikes.
func TestRecordFPBridgeVerdict(t *testing.T) {
	for _, verdict := range []FPBridgeVerdict{FPBridgeBoundDead, FPBridgeDown, FPBridgeConsentParked} {
		t.Run(string(verdict), func(t *testing.T) {
			s := newBridgeLedgerServer()
			st := FPBridgeStatus{Verdict: verdict, Detail: "x"}
			s.recordFPBridgeVerdict(st)
			if s.fpBridgeFaulted() {
				t.Fatal("one strike faulted (debounce is 2)")
			}
			s.recordFPBridgeVerdict(st)
			if !s.fpBridgeFaulted() {
				t.Fatal("two strikes did not fault")
			}
			s.recordFPBridgeVerdict(FPBridgeStatus{Verdict: FPBridgeServing})
			if s.fpBridgeFaulted() {
				t.Fatal("a serving verdict did not clear the fault")
			}
		})
	}
}

// TestRecordFPBridgeHealthNonFPMachine is the non-FP-machine gate negative: with no
// File-Provider-backed account, recordFPBridgeHealth never faults the fp.bridge row
// (and clears a stale one) even against a bound-dead bridge — a parked bridge on a
// non-FP machine must not alert.
func TestRecordFPBridgeHealthNonFPMachine(t *testing.T) {
	s, _ := newTestServer(t) // two symlink accounts, zero FP rows
	s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus { return boundDeadStatus() }
	// Seed a stale fault: the check pass must clear it, not leave it alerting.
	s.recordFPBridgeVerdict(boundDeadStatus())
	s.recordFPBridgeVerdict(boundDeadStatus())
	if !s.fpBridgeFaulted() {
		t.Fatal("precondition: two strikes should have faulted the row")
	}
	s.recordFPBridgeHealth(context.Background())
	s.ledMu.Lock()
	row := s.led.peek(fpBridgePolicy, poolResource)
	s.ledMu.Unlock()
	if row != nil {
		t.Fatalf("fp.bridge row = %+v on a non-FP machine, want cleared (no alert)", row)
	}
}

// TestFPBridgeReadyFaultedGate pins UNIT 2c: a bound bridge with a clean row reads
// ready, but once the fp.bridge row faults, fpBridgeReady gates healFPRows off even
// though the socket still accepts — a bound-but-dead bridge is not ready to probe
// through.
func TestFPBridgeReadyFaultedGate(t *testing.T) {
	shortHome(t)
	deadBridge(t, pool.FPBridgeSocketPath()) // socket bound (dial succeeds), never serves
	s := newBridgeLedgerServer()
	if !s.fpBridgeReady() {
		t.Fatal("a bound bridge with a clean row should read ready (the row faults over strikes, not the dial)")
	}
	s.recordFPBridgeVerdict(boundDeadStatus())
	s.recordFPBridgeVerdict(boundDeadStatus()) // faults the row
	if s.fpBridgeReady() {
		t.Fatal("a faulted fp.bridge row must gate healFPRows off (bound-but-dead is not ready)")
	}
}

// TestHandleFPBridgeCheckRefreshesLedger pins the on-demand op: it returns the
// verdict payload AND folds it onto the fp.bridge row (two strikes fault it).
func TestHandleFPBridgeCheckRefreshesLedger(t *testing.T) {
	s, _ := newTestServer(t)
	s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus { return boundDeadStatus() }
	resp := s.handleFPBridgeCheck(t.Context())
	if !resp.OK || resp.FPBridge == nil || resp.FPBridge.Verdict != FPBridgeBoundDead {
		t.Fatalf("resp = %+v, want OK with a bound-dead FPBridge payload", resp)
	}
	if resp.FPBridge.Detail != fpBridgeBoundDeadLever {
		t.Fatalf("detail = %q, want the bound-dead lever", resp.FPBridge.Detail)
	}
	if s.fpBridgeFaulted() {
		t.Fatal("one on-demand strike must not fault (debounce 2)")
	}
	if resp := s.handleFPBridgeCheck(t.Context()); resp.FPBridge.Verdict != FPBridgeBoundDead {
		t.Fatalf("second check payload = %+v, want bound-dead", resp.FPBridge)
	}
	if !s.fpBridgeFaulted() {
		t.Fatal("second on-demand strike should fault the fp.bridge row")
	}
}

// TestHandleFPRepairBridgeGate pins UNIT 2g: a non-retreat repair on a bound-dead
// bridge makes no domain claims and performs ZERO Teardown, while an explicit
// retreat stays un-gated (it needs no bridge).
func TestHandleFPRepairBridgeGate(t *testing.T) {
	t.Run("non-retreat repair on a bound-dead bridge tears down nothing", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus { return boundDeadStatus() }
		wedgeIt(t, s, dirs[1])
		one := a.ID
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &one})
		if resp.OK {
			t.Fatalf("resp.OK = true, want a refusal on a bound-dead bridge")
		}
		if resp.Error != "cannot repair: "+fpBridgeBoundDeadLever {
			t.Fatalf("error = %q, want the cannot-repair bound-dead lever", resp.Error)
		}
		if len(resp.FPRepairs) != 0 {
			t.Fatalf("FPRepairs = %+v, want no domain claims on a gated repair", resp.FPRepairs)
		}
		if _, registrations, _, teardowns := fake.counts(); registrations != 0 || teardowns != 0 {
			t.Fatalf("registrations=%d teardowns=%d, want 0/0 — a gated repair tears down nothing", registrations, teardowns)
		}
	})

	t.Run("retreat stays un-gated on a bound-dead bridge", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		s.fpBridgeCheckFn = func(context.Context) FPBridgeStatus { return boundDeadStatus() }
		s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
		wedgeIt(t, s, dirs[1])
		one := a.ID
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &one, Retreat: true})
		if !resp.OK || len(resp.FPRepairs) != 1 || resp.FPRepairs[0].Outcome != FPRepairRetreated {
			t.Fatalf("resp = %+v, want a retreat despite the bound-dead bridge (retreat is un-gated)", resp)
		}
		if _, registrations, _, _ := fake.counts(); registrations != 0 {
			t.Fatalf("retreat re-registered (registrations=%d), want 0", registrations)
		}
	})
}
