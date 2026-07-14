package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-pool/internal/overlay"
	"github.com/yasyf/cc-pool/internal/procscan"
	"github.com/yasyf/fusekit/fileproviderd"
)

// TestHandleFPRepairTargetSelection pins which accounts a repair touches: an
// explicit account is repaired regardless of its verdict, a non-File-Provider or
// unknown account is an op-level error, and a bulk repair (no account) touches
// only currently-wedged domains.
func TestHandleFPRepairTargetSelection(t *testing.T) {
	t.Run("explicit fileprovider account re-registers regardless of verdict", func(t *testing.T) {
		s, a, _, fake := newFPHealServer(t)
		one := a.ID
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &one})
		if !resp.OK || len(resp.FPRepairs) != 1 {
			t.Fatalf("resp = %+v, want OK with one repair result", resp)
		}
		if resp.FPRepairs[0].Outcome != FPRepairRepaired {
			t.Fatalf("outcome = %q, want repaired", resp.FPRepairs[0].Outcome)
		}
		if _, setups, _, teardowns := fake.counts(); setups != 1 || teardowns != 1 {
			t.Fatalf("setups=%d teardowns=%d, want 1/1 (one re-register)", setups, teardowns)
		}
		if kindOf(t, s, 1) != "fileprovider" {
			t.Fatal("a re-register must not change the row")
		}
	})

	t.Run("explicit non-fileprovider account is an op error", func(t *testing.T) {
		s, _, _, _ := newFPHealServer(t)
		two := 2 // acct-2 is a symlink row
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &two})
		if resp.OK || !strings.Contains(resp.Error, "not a file provider account") {
			t.Fatalf("resp = %+v, want an op error naming the wrong backend", resp)
		}
	})

	t.Run("explicit unknown account is an op error", func(t *testing.T) {
		s, _, _, _ := newFPHealServer(t)
		missing := 99
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &missing})
		if resp.OK || !strings.Contains(resp.Error, "not found") {
			t.Fatalf("resp = %+v, want an account-not-found op error", resp)
		}
	})

	t.Run("bulk repair touches only wedged domains", func(t *testing.T) {
		s, _, dirs, fake := newFPHealServer(t)
		wedgeIt(t, s, dirs[1])
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair})
		if !resp.OK || len(resp.FPRepairs) != 1 || resp.FPRepairs[0].ID != 1 {
			t.Fatalf("resp = %+v, want exactly the wedged acct-1 repaired", resp)
		}
		if resp.FPRepairs[0].Outcome != FPRepairRepaired {
			t.Fatalf("outcome = %q, want repaired", resp.FPRepairs[0].Outcome)
		}
		if _, setups, _, _ := fake.counts(); setups != 1 {
			t.Fatalf("setups=%d, want 1 (only the wedged domain)", setups)
		}
		if s.fpWedged(dirs[1]) {
			t.Fatal("a clean re-register must reset the wedge state")
		}
	})

	t.Run("bulk repair with no wedged domains is a no-op", func(t *testing.T) {
		s, _, _, fake := newFPHealServer(t)
		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair})
		if !resp.OK || len(resp.FPRepairs) != 0 {
			t.Fatalf("resp = %+v, want OK with no repairs", resp)
		}
		if _, setups, _, teardowns := fake.counts(); setups != 0 || teardowns != 0 {
			t.Fatalf("setups=%d teardowns=%d, want 0/0 (nothing wedged)", setups, teardowns)
		}
	})
}

// TestRepairFPDomainOutcomes pins the per-account outcome classification: a clean
// Setup repairs, a claimed account is busy, ErrCannotControl retreats to symlink,
// and a transient Setup failure is reported as failed (never silently repaired).
func TestRepairFPDomainOutcomes(t *testing.T) {
	t.Run("clean re-register repairs and resets state", func(t *testing.T) {
		s, a, dirs, _ := newFPHealServer(t)
		wedgeIt(t, s, dirs[1])
		res := s.repairFPDomain(t.Context(), a, false)
		if res.Outcome != FPRepairRepaired {
			t.Fatalf("outcome = %q, want repaired", res.Outcome)
		}
		if s.fpWedged(dirs[1]) {
			t.Fatal("repair must reset the wedge verdict")
		}
	})

	t.Run("a claimed account is busy", func(t *testing.T) {
		s, a, _, fake := newFPHealServer(t)
		if !s.cl.reserve(a.ID) {
			t.Fatal("could not reserve acct-1")
		}
		res := s.repairFPDomain(t.Context(), a, false)
		if res.Outcome != FPRepairBusy {
			t.Fatalf("outcome = %q, want busy under a reservation", res.Outcome)
		}
		if _, setups, _, teardowns := fake.counts(); setups != 0 || teardowns != 0 {
			t.Fatalf("setups=%d teardowns=%d, want 0/0 (never re-registered a claimed dir)", setups, teardowns)
		}
	})

	t.Run("ErrCannotControl retreats to symlink", func(t *testing.T) {
		s, a, _, fake := newFPHealServer(t)
		fake.setupErr = fmt.Errorf("file provider setup: %w", fileproviderd.ErrCannotControl)
		res := s.repairFPDomain(t.Context(), a, false)
		if res.Outcome != FPRepairRetreated {
			t.Fatalf("outcome = %q, want retreated", res.Outcome)
		}
		if kindOf(t, s, 1) != "symlink" {
			t.Fatal("ErrCannotControl repair must retreat the row to symlink")
		}
	})

	t.Run("a transient Setup failure is reported failed, not repaired", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		s.fpForceWedge(dirs[1], overlay.ErrFPProbeWedged)
		fake.setupErr = fmt.Errorf("register domain: %w", fileproviderd.ErrBusy)
		res := s.repairFPDomain(t.Context(), a, false)
		if res.Outcome != FPRepairFailed {
			t.Fatalf("outcome = %q, want failed on a transient Setup error", res.Outcome)
		}
		if !s.fpWedged(dirs[1]) {
			t.Fatal("a failed repair must not clear the wedge verdict")
		}
		if kindOf(t, s, 1) != "fileprovider" {
			t.Fatal("a failed (non-cannot-control) repair must not change the row")
		}
	})
}

// TestFPRepairRetreatWire pins the operator-only retreat path (`ccp fp repair
// --retreat`): Request.Retreat routes to convertFPToSymlinkHeld instead of a
// re-register — the ONLY caller left now that the heal breaker parks. Idle it
// retreats and forgets the wedge; under a live session it defers loudly rather than
// tear a domain out from under it. Neither path ever calls the FP provider's Setup
// (a retreat is not a re-register).
func TestFPRepairRetreatWire(t *testing.T) {
	t.Run("explicit retreat converts to symlink without re-registering", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
		wedgeIt(t, s, dirs[1])
		one := a.ID

		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &one, Retreat: true})
		if !resp.OK || len(resp.FPRepairs) != 1 {
			t.Fatalf("resp = %+v, want OK with one result", resp)
		}
		if resp.FPRepairs[0].Outcome != FPRepairRetreated {
			t.Fatalf("outcome = %q, want retreated", resp.FPRepairs[0].Outcome)
		}
		if kindOf(t, s, 1) != "symlink" {
			t.Fatal("explicit retreat must convert the row to symlink")
		}
		if s.fpWedged(dirs[1]) {
			t.Fatal("a retreat must forget the wedge state")
		}
		if _, setups, _, _ := fake.counts(); setups != 0 {
			t.Fatalf("retreat re-registered the domain (setups=%d), want 0 — retreat is not a re-register", setups)
		}
	})

	t.Run("retreat under a live session defers, row stays fileprovider", func(t *testing.T) {
		s, a, dirs, _ := newFPHealServer(t)
		// A held session lease (a live session or a select handout — its open fds
		// break on the domain removal) makes the retreat's exclusive seize bounce.
		holdSessionLease(t, s, a)
		wedgeIt(t, s, dirs[1])
		one := a.ID

		resp := s.handleFPRepair(t.Context(), Request{Op: OpFPRepair, Account: &one, Retreat: true})
		if !resp.OK || len(resp.FPRepairs) != 1 {
			t.Fatalf("resp = %+v, want OK with one result", resp)
		}
		if resp.FPRepairs[0].Outcome != FPRepairFailed {
			t.Fatalf("outcome = %q, want failed (retreat blocked by a live session)", resp.FPRepairs[0].Outcome)
		}
		if !strings.Contains(resp.FPRepairs[0].Detail, "live session") {
			t.Fatalf("detail = %q, want the live-session blocked guidance", resp.FPRepairs[0].Detail)
		}
		if kindOf(t, s, 1) != "fileprovider" {
			t.Fatal("a blocked retreat must not change the row")
		}
	})

	t.Run("retreat rides the socket wire", func(t *testing.T) {
		s, a, dirs, fake := newFPHealServer(t)
		s.scanSessions = func(context.Context) ([]procscan.Session, error) { return nil, nil }
		wedgeIt(t, s, dirs[1])
		cl := &Client{socket: serveHandlerOnSocket(t, s)}
		one := a.ID

		resp, err := cl.FPRepair(&one, true)
		if err != nil {
			t.Fatalf("FPRepair --retreat: %v", err)
		}
		if len(resp.FPRepairs) != 1 || resp.FPRepairs[0].Outcome != FPRepairRetreated {
			t.Fatalf("FPRepairs = %+v, want one retreated result", resp.FPRepairs)
		}
		if kindOf(t, s, 1) != "symlink" {
			t.Fatal("retreat over the wire must convert the row to symlink")
		}
		if _, setups, _, _ := fake.counts(); setups != 0 {
			t.Fatalf("retreat over the wire re-registered (setups=%d), want 0", setups)
		}
	})
}

// TestFPRepairEndToEndOverSocket drives OpFPRepair through the real wire:
// Client.FPRepair -> unix socket -> handle -> dispatch -> handleFPRepair. A
// missing dispatch case or client op would fail here, not in the handler unit.
func TestFPRepairEndToEndOverSocket(t *testing.T) {
	s, a, _, fake := newFPHealServer(t)
	cl := &Client{socket: serveHandlerOnSocket(t, s)}
	one := a.ID
	resp, err := cl.FPRepair(&one, false)
	if err != nil {
		t.Fatalf("FPRepair: %v", err)
	}
	if !resp.OK || resp.Proto != ProtocolVersion {
		t.Fatalf("resp = %+v, want OK at proto %d", resp, ProtocolVersion)
	}
	if len(resp.FPRepairs) != 1 || resp.FPRepairs[0].Outcome != FPRepairRepaired {
		t.Fatalf("FPRepairs = %+v, want one repaired result", resp.FPRepairs)
	}
	if _, setups, _, _ := fake.counts(); setups != 1 {
		t.Fatalf("setups = %d over the wire, want 1", setups)
	}
}

// TestHandleStatusSurfacesFPLedger pins the status wire: a wedged domain appears
// as a faulted fp.domain ledger with recovery progress, and a healthy pool
// reports no rows.
func TestHandleStatusSurfacesFPLedger(t *testing.T) {
	s, _, dirs, _ := newFPHealServer(t)

	if resp := s.handleStatus(t.Context()); len(resp.Ledgers) != 0 {
		t.Fatalf("Ledgers = %+v on a healthy pool, want none", resp.Ledgers)
	}

	wedgeIt(t, s, dirs[1])
	s.fpRecordAttempt(dirs[1], time.Unix(0, 0))
	resp := s.handleStatus(t.Context())
	if len(resp.Ledgers) != 1 {
		t.Fatalf("Ledgers = %+v, want exactly the wedged acct-1 row", resp.Ledgers)
	}
	l := resp.Ledgers[0]
	if l.Policy != "fp.domain" || l.Resource != dirs[1] || !l.Faulted {
		t.Fatalf("ledger state = %+v, want faulted fp.domain at %s", l, dirs[1])
	}
	if l.Attempts != 1 || l.Parked {
		t.Fatalf("recovery = (attempts %d, parked %v), want (1, false)", l.Attempts, l.Parked)
	}
}

// TestFPLedgerReportsBreaker pins that the wire ledger flags a breaker-parked
// domain and drops a recovered domain entirely.
func TestFPLedgerReportsBreaker(t *testing.T) {
	s := newFPLedgerServer(alwaysNonEmpty)
	wedgeIt(t, s, fpTestDir)
	for i := 0; i < fpRecoveryBreaker; i++ {
		s.fpRecordAttempt(fpTestDir, time.Unix(0, 0))
	}
	snap := s.ledgersWire()
	if len(snap) != 1 {
		t.Fatalf("ledger snapshot = %+v, want one wedged domain", snap)
	}
	if !snap[0].Parked || snap[0].Attempts != fpRecoveryBreaker {
		t.Fatalf("snapshot[0] = %+v, want parked with %d attempts", snap[0], fpRecoveryBreaker)
	}

	if msg := s.recordFPProbe(fpTestDir, nil); !strings.Contains(msg, "recovered") {
		t.Fatalf("recovery log = %q, want a recovered line", msg)
	}
	if got := s.ledgersWire(); len(got) != 0 {
		t.Fatalf("ledger snapshot after recovery = %+v, want empty", got)
	}
}
