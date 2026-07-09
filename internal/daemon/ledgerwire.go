package daemon

import "sort"

// ledgersWire assembles the composed Ledgers block for the on-disk snapshot: the
// Server-owned store's rows under ledMu plus the holder cache's fuse verdict rows
// under the holder's own mu — the two locks taken and released separately, never
// nested; bookkeeping only, no I/O under either.
func (s *Server) ledgersWire() []LedgerState {
	s.ledMu.Lock()
	rows := s.led.snapshot()
	s.ledMu.Unlock()
	return composeLedgersWire(rows, s.holder.ledgersSnapshot())
}

// statusLedgers derives the wedged-FP-domain view AND the composed Ledgers block from
// a SINGLE s.led snapshot, so one status response can't contradict itself about the
// same fp.domain row (FPWedged and Ledgers previously took ledMu in separate epochs).
// The holder cache composes under its own mu afterward — cross-STORE atomicity is not
// a goal (two stores by design; locks never nested), only the double-read of s.led is.
func (s *Server) statusLedgers(accts []AccountStatus) ([]FPDomainState, []LedgerState) {
	s.ledMu.Lock()
	rows := s.led.snapshot()
	s.ledMu.Unlock()
	var fpWedged []FPDomainState
	if s.fpEnabled() {
		fpWedged = fpDomainStates(accts, fpWedgesFrom(rows))
	}
	return fpWedged, composeLedgersWire(rows, s.holder.ledgersSnapshot())
}

// composeLedgersWire folds the Server-owned store's snapshot and the holder cache's
// fuse-verdict rows into the sorted wire block — the pure half of ledgersWire, shared
// with statusLedgers so the s.led snapshot is taken once. The stores stay separate by
// design (the holder rows reset in lockstep with its mount cache); only this composes
// them. Rows sort by policy then resource so the wire view is deterministic.
func composeLedgersWire(ledRows, holderRows []ledgerSnapshot) []LedgerState {
	rows := append(ledRows, holderRows...)
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Policy != rows[j].Policy {
			return rows[i].Policy < rows[j].Policy
		}
		return rows[i].Resource < rows[j].Resource
	})
	out := make([]LedgerState, len(rows))
	for i, r := range rows {
		out[i] = LedgerState(r)
	}
	return out
}
